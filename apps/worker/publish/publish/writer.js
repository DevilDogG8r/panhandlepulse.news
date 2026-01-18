// apps/worker/writer.js
import pg from "pg";

const { Client } = pg;

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is missing`);
  return v;
}

async function getDbClient() {
  const url = requireEnv("DATABASE_URL");
  const client = new Client({
    connectionString: url,
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();
  return client;
}

// Provider-agnostic AI call.
// You provide an endpoint that accepts { messages: [...] } and returns { text: "..." , json?: {...} }
async function callAI({ messages }) {
  const endpoint = requireEnv("AI_ENDPOINT");
  const key = requireEnv("AI_API_KEY");

  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "authorization": `Bearer ${key}`,
    },
    body: JSON.stringify({ messages }),
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`AI HTTP ${res.status}: ${t.slice(0, 500)}`);
  }

  const data = await res.json();
  if (!data?.text) throw new Error("AI response missing `text`");
  return data;
}

function iso(d) {
  return d.toISOString();
}

function hoursAgo(n) {
  const d = new Date();
  d.setHours(d.getHours() - n);
  return d;
}

async function loadRecentFeedItems(client, { windowStart, windowEnd, limitPerCounty = 40 }) {
  // Pull recent items grouped by county/state via sources join
  const res = await client.query(
    `
    SELECT
      fi.id,
      fi.title,
      fi.link,
      fi.published_at,
      fi.summary,
      s.state,
      s.county,
      s.source_name
    FROM feed_items fi
    JOIN sources s ON s.id = fi.source_id
    WHERE (fi.published_at IS NULL OR fi.published_at BETWEEN $1 AND $2)
      AND s.enabled = TRUE
    ORDER BY fi.published_at DESC NULLS LAST, fi.id DESC
    LIMIT 1000
    `,
    [windowStart, windowEnd]
  );

  // group by state/county
  const groups = new Map();
  for (const row of res.rows) {
    const key = `${row.state}||${row.county}`;
    if (!groups.has(key)) groups.set(key, []);
    const arr = groups.get(key);
    if (arr.length < limitPerCounty) arr.push(row);
  }
  return groups;
}

function buildPrompt({ state, county, items, windowStart, windowEnd }) {
  // Keep it safe: summary + citations. No copying full text. No invention.
  const sourcesBlock = items
    .slice(0, 20)
    .map((it, idx) => {
      const when = it.published_at ? new Date(it.published_at).toLocaleString() : "unknown time";
      return [
        `[${idx + 1}] ${it.title}`,
        `Source: ${it.source_name} (${state}/${county})`,
        `Published: ${when}`,
        `Link: ${it.link}`,
        `Snippet: ${String(it.summary || "").replace(/\s+/g, " ").slice(0, 240)}`,
      ].join("\n");
    })
    .join("\n\n");

  const system = `
You are the editorial writer for Panhandle Pulse, a local news site.
Write ORIGINAL text based ONLY on the provided source list.
Do NOT invent facts. If details are unclear, say so.
Do NOT copy long passages. Use short paraphrases only.
Output MUST be valid JSON with keys:
- title (string)
- dek (string, 1 sentence)
- bullets (array of 4-8 short bullet strings)
- body_markdown (string, 4-10 short paragraphs max)
- used_source_indexes (array of integers referencing the [1..N] items you used)
Rules:
- Include citations inline in body like: (Sources: [1], [3])
- Keep it local and practical.
- Neutral, non-clickbait tone.
`.trim();

  const user = `
Write a county roundup story for ${state}/${county}.
Time window: ${iso(windowStart)} to ${iso(windowEnd)}.

SOURCE ITEMS:
${sourcesBlock}
`.trim();

  return { system, user };
}

async function storyAlreadyExists(client, { state, county, windowStart, windowEnd }) {
  const res = await client.query(
    `
    SELECT id
    FROM stories
    WHERE state = $1 AND county = $2
      AND story_type = 'roundup'
      AND time_window_start = $3
      AND time_window_end = $4
    LIMIT 1
    `,
    [state, county, windowStart, windowEnd]
  );
  return res.rowCount > 0;
}

async function insertStory(client, { state, county, windowStart, windowEnd, title, dek, bullets, body_markdown, model_name }) {
  const res = await client.query(
    `
    INSERT INTO stories
      (state, county, story_type, title, dek, bullets_json, body_markdown, time_window_start, time_window_end, model_name, prompt_version, status)
    VALUES
      ($1, $2, 'roundup', $3, $4, $5::jsonb, $6, $7, $8, $9, 'v1', 'published')
    RETURNING id
    `,
    [state, county, title, dek || "", JSON.stringify(bullets || []), body_markdown, windowStart, windowEnd, model_name || ""]
  );
  return res.rows[0].id;
}

async function insertStorySources(client, storyId, items, usedIndexes) {
  const usedSet = new Set((usedIndexes || []).map((n) => Number(n)));
  const chosen = [];

  // If model didn't specify, fallback to first 6 items
  if (usedSet.size === 0) {
    for (let i = 0; i < Math.min(6, items.length); i++) chosen.push(items[i]);
  } else {
    for (const idx of usedSet) {
      const it = items[idx - 1];
      if (it) chosen.push(it);
    }
  }

  for (const it of chosen) {
    await client.query(
      `
      INSERT INTO story_sources (story_id, feed_item_id, source_link, source_title, source_published_at)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT DO NOTHING
      `,
      [storyId, it.id, it.link, it.title, it.published_at]
    );
  }
}

async function main() {
  console.log("Starting Story Writer…");

  // default: last 24 hours
  const windowHours = Number(process.env.STORY_WINDOW_HOURS || "24");
  const windowEnd = new Date();
  const windowStart = hoursAgo(windowHours);

  const client = await getDbClient();
  try {
    // Ensure schema exists (db.sql already handles it via worker ensureSchema)
    const groups = await loadRecentFeedItems(client, { windowStart, windowEnd });

    console.log(`Found ${groups.size} county groups with recent items`);

    let created = 0;
    let skipped = 0;
    let failed = 0;

    for (const [key, items] of groups.entries()) {
      const [state, county] = key.split("||");

      // skip tiny groups (not enough to make a roundup)
      if (items.length < 3) {
        skipped++;
        continue;
      }

      const exists = await storyAlreadyExists(client, { state, county, windowStart, windowEnd });
      if (exists) {
        skipped++;
        continue;
      }

      const { system, user } = buildPrompt({ state, county, items, windowStart, windowEnd });

      try {
        const ai = await callAI({
          messages: [
            { role: "system", content: system },
            { role: "user", content: user },
          ],
        });

        let parsed;
        try {
          parsed = JSON.parse(ai.text);
        } catch {
          throw new Error("AI did not return valid JSON");
        }

        const storyId = await insertStory(client, {
          state,
          county,
          windowStart,
          windowEnd,
          title: parsed.title || `${county} County Roundup`,
          dek: parsed.dek || "",
          bullets: parsed.bullets || [],
          body_markdown: parsed.body_markdown || "",
          model_name: ai.model || "",
        });

        await insertStorySources(client, storyId, items, parsed.used_source_indexes || []);

        console.log(`Created story ${storyId} for ${state}/${county} from ${items.length} items`);
        created++;
      } catch (e) {
        console.error(`FAILED ${state}/${county}: ${e.message}`);
        failed++;
      }
    }

    console.log(`Writer done. Created=${created}, Skipped=${skipped}, Failed=${failed}`);
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  console.error("Fatal writer error:", err);
  process.exit(1);
});
