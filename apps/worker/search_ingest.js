import crypto from "node:crypto";
import pg from "pg";

const { Client } = pg;

function envInt(name, fallback) {
  const v = process.env[name];
  if (!v) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
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

// This creates (or reuses) a single "national search" source per county
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
  // Keep it simple and high-signal.
  // You can expand later with city names, agencies, etc.
  const q = `${county.replace(/_/g, " ")} county ${state}`;
  return [q];
}

async function fetchJson(url, timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: { "user-agent": "PanhandlePulseBot/0.1 (+https://panhandlepulse.news)" },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return await res.json();
  } catch (e) {
    if (e?.name === "AbortError") throw new Error(`Timeout after ${timeoutMs}ms for ${url}`);
    throw e;
  } finally {
    clearTimeout(t);
  }
}

// GDELT 2.1 DOC API
// We’ll use "mode=ArtList&format=json" and a recent time window.
async function gdeltSearch(query, timespan, timeoutMs) {
  const base = "https://api.gdeltproject.org/api/v2/doc/doc";
  const url =
    `${base}?query=${encodeURIComponent(query)}` +
    `&mode=ArtList&format=json&sort=HybridRel&maxrecords=50&format=json` +
    `&timespan=${encodeURIComponent(timespan)}`;
  return fetchJson(url, timeoutMs);
}

function toIsoFromGdelt(seenStr) {
  // GDELT "seendate" often like 20260118103000
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
  const timeoutMs = envInt("FETCH_TIMEOUT_MS", 15000);
  const pauseMs = envInt("PAUSE_BETWEEN_SOURCES_MS", 250);

  // How far back each run searches (kept small since you run every 15 min)
  const timespan = process.env.GDELT_TIMESPAN || "1day"; // options: 15min, 1h, 6h, 1day, etc.

  // Limit which counties are searched (optional)
  const STATES = (process.env.SEARCH_STATES || "FL,AL")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  // These should match your coverage list
  const COVERAGE = {
    FL: ["Escambia", "Santa_Rosa", "Okaloosa", "Walton", "Bay", "Gulf", "Franklin", "Holmes", "Washington", "Jackson", "Calhoun"],
    AL: ["Mobile", "Baldwin", "Escambia_AL", "Covington", "Geneva", "Houston", "Coffee", "Dale", "Henry"],
  };

  const client = await getDbClient();
  try {
    let inserted = 0;
    let ok = 0;
    let failed = 0;

    for (const state of STATES) {
      const counties = COVERAGE[state] || [];
      for (const county of counties) {
        const sourceId = await upsertSearchSource(client, state, county);
        const queries = buildCountyQueries(state, county);

        for (const q of queries) {
          try {
            const data = await gdeltSearch(q, timespan, timeoutMs);
            const arts = data?.articles || [];

            let localInserted = 0;
            for (const a of arts) {
              const title = a?.title || "";
              const link = a?.url || "";
              if (!title || !link) continue;

              const publishedAt = toIsoFromGdelt(a?.seendate) || null;
              const summary = a?.seendescription || a?.sourceCountry || "";

              await insertFeedItem(client, sourceId, title, link, publishedAt, summary);
              localInserted += 1;
            }

            inserted += localInserted;
            ok += 1;
            console.log(`[GDELT ${state}/${county}] "${q}" -> ${arts.length} results, inserted attempts ${localInserted}`);
          } catch (e) {
            failed += 1;
            console.log(`[GDELT ${state}/${county}] ERROR: ${e.message}`);
          }

          await new Promise((r) => setTimeout(r, pauseMs));
        }
      }
    }

    console.log(`GDELT Summary: OK=${ok}, Failed=${failed}, InsertAttempts=${inserted}`);
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  console.error("Fatal search ingest error:", err);
  process.exit(1);
});
