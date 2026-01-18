import crypto from "node:crypto";
import pg from "pg";

const { Client } = pg;

function envInt(name, fallback) {
  const v = process.env[name];
  if (!v) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function hashItem(title, link) {
  return crypto.createHash("sha256").update(`${title}||${link}`).digest("hex");
}

async function getDbClient() {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error("DATABASE_URL is missing.");
  const client = new Client({
    connectionString: url,
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();
  return client;
}

async function upsertSearchSource(client, state, county) {
  const source_name = `GDELT Search (${state}/${county})`;

  const res = await client.query(
    `
    INSERT INTO sources (state, county, source_name, source_type, tier, website_url, rss_url, facebook_url, x_url, enabled)
    VALUES ($1,$2,$3,'search','1A','https://www.gdeltproject.org','', '', '', TRUE)
    ON CONFLICT (state, county, source_name)
    DO UPDATE SET enabled = TRUE
    RETURNING id
    `,
    [state, county, source_name]
  );
  return res.rows[0].id;
}

async function insertFeedItem(client, sourceId, title, link, publishedAtIso, summary) {
  const contentHash = hashItem(title, link);
  await client.query(
    `
    INSERT INTO feed_items (source_id, title, link, published_at, summary, content_hash)
    VALUES ($1,$2,$3,$4,$5,$6)
    ON CONFLICT (source_id, content_hash) DO NOTHING
    `,
    [sourceId, title, link, publishedAtIso, summary || "", contentHash]
  );
}

function buildCountyQueries(state, county) {
  const niceCounty = county.replace(/_/g, " ");
  // Keep queries short + consistent
  return [`"${niceCounty} County" ${state}`];
}

async function fetchText(url, timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        "user-agent": "PanhandlePulseBot/0.1 (+https://panhandlepulse.news)",
        accept: "application/json,text/plain,*/*",
      },
    });

    const text = await res.text();
    if (!res.ok) {
      // include some body for debugging
      const snippet = text.slice(0, 180).replace(/\s+/g, " ").trim();
      throw new Error(`HTTP ${res.status}. Body: ${snippet}`);
    }
    return text;
  } catch (e) {
    if (e?.name === "AbortError") throw new Error(`Timeout after ${timeoutMs}ms`);
    throw e;
  } finally {
    clearTimeout(t);
  }
}

function tryParseJson(text) {
  const trimmed = (text || "").trim();
  if (!trimmed) return { ok: false, value: null, reason: "empty response" };
  if (!(trimmed.startsWith("{") || trimmed.startsWith("["))) {
    return { ok: false, value: null, reason: trimmed.slice(0, 180).replace(/\s+/g, " ").trim() };
  }
  try {
    return { ok: true, value: JSON.parse(trimmed), reason: "" };
  } catch (e) {
    return { ok: false, value: null, reason: `JSON parse failed: ${e.message}. Head: ${trimmed.slice(0, 180)}` };
  }
}

// GDELT 2.0/2.1 DOC API endpoint
async function gdeltSearch(query, timespan, timeoutMs) {
  const base = "https://api.gdeltproject.org/api/v2/doc/doc";
  const url =
    `${base}?query=${encodeURIComponent(query)}` +
    `&mode=ArtList&format=json&sort=HybridRel&maxrecords=50` +
    `&timespan=${encodeURIComponent(timespan)}`;

  const text = await fetchText(url, timeoutMs);
  const parsed = tryParseJson(text);

  if (!parsed.ok) {
    // This is the exact message you were getting (starts with "Your searc...")
    throw new Error(`Non-JSON response: ${parsed.reason}`);
  }
  return parsed.value;
}

function toIsoFromGdelt(seenStr) {
  // Often 20260118103000
  if (!seenStr || typeof seenStr !== "string" || seenStr.length < 14) return null;
  const yyyy = seenStr.slice(0, 4);
  const mm = seenStr.slice(4, 6);
  const dd = seenStr.slice(6, 8);
  const hh = seenStr.slice(8, 10);
  const mi = seenStr.slice(10, 12);
  const ss = seenStr.slice(12, 14);
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}Z`;
}

async function main() {
  const timeoutMs = envInt("FETCH_TIMEOUT_MS", 20000);

  // IMPORTANT: slow down so GDELT doesn't throw the "Your search..." text response
  const pauseMs = envInt("PAUSE_BETWEEN_QUERIES_MS", 2000);

  const timespan = process.env.GDELT_TIMESPAN || "1day";

  const STATES = (process.env.SEARCH_STATES || "FL,AL")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const COVERAGE = {
    FL: ["Escambia", "Santa_Rosa", "Okaloosa", "Walton", "Bay", "Gulf", "Franklin", "Holmes", "Washington", "Jackson", "Calhoun"],
    AL: ["Mobile", "Baldwin", "Escambia_AL", "Covington", "Geneva", "Houston", "Coffee", "Dale", "Henry"],
  };

  const client = await getDbClient();
  try {
    let ok = 0;
    let failed = 0;
    let attempted = 0;

    for (const state of STATES) {
      const counties = COVERAGE[state] || [];
      for (const county of counties) {
        const sourceId = await upsertSearchSource(client, state, county);
        const queries = buildCountyQueries(state, county);

        for (const q of queries) {
          try {
            const data = await gdeltSearch(q, timespan, timeoutMs);
            const arts = data?.articles || [];

            let localAttempts = 0;
            for (const a of arts) {
              const title = a?.title || "";
              const link = a?.url || "";
              if (!title || !link) continue;

              const publishedAt = toIsoFromGdelt(a?.seendate) || null;
              const summary = a?.seendescription || "";

              await insertFeedItem(client, sourceId, title, link, publishedAt, summary);
              localAttempts += 1;
            }

            attempted += localAttempts;
            ok += 1;
            console.log(`[GDELT ${state}/${county}] ${q} -> results=${arts.length}, insertAttempts=${localAttempts}`);
          } catch (e) {
            failed += 1;
            console.log(`[GDELT ${state}/${county}] ERROR: ${e.message}`);
          }

          await sleep(pauseMs);
        }
      }
    }

    console.log(`GDELT Summary: OK=${ok}, Failed=${failed}, InsertAttempts=${attempted}`);
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  console.error("Fatal search ingest error:", err);
  process.exit(1);
});
