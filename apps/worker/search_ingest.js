/**
 * Panhandle Pulse — GDELT Search Ingest Worker (ESM) — FINAL
 *
 * Writes GDELT results into a dedicated ingest table that matches what GDELT can provide.
 * DOES NOT write to `stories` (because `stories` requires body_markdown NOT NULL).
 *
 * Required ENV:
 * - DATABASE_URL
 *
 * Optional ENV:
 * - TARGET_TABLE (default: panhandle_search_articles)
 * - GDELT_DOC_API (default: https://api.gdeltproject.org/api/v2/doc/doc)
 * - LOOKBACK_HOURS (default: 12)
 * - MAX_RECORDS (default: 50)
 * - MIN_KEYWORD_LEN (default: 4)
 */

import { Pool } from 'pg';

const TARGET_TABLE = process.env.TARGET_TABLE || 'panhandle_search_articles';
const GDELT_DOC_API = process.env.GDELT_DOC_API || 'https://api.gdeltproject.org/api/v2/doc/doc';
const LOOKBACK_HOURS = Number(process.env.LOOKBACK_HOURS || 12);
const MAX_RECORDS = Number(process.env.MAX_RECORDS || 50);
const MIN_KEYWORD_LEN = Number(process.env.MIN_KEYWORD_LEN || 4);

if (!process.env.DATABASE_URL) {
  console.error('FATAL: DATABASE_URL is not set');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'disable' ? false : { rejectUnauthorized: false }
});

// ------------------------------
// Regions / Queries
// ------------------------------
const REGIONS = [
  { tag: 'GDELT FL/Escambia', state: 'FL', county: 'Escambia', queries: ['"Escambia County" "Florida"'] },
  { tag: 'GDELT FL/Santa_Rosa', state: 'FL', county: 'Santa Rosa', queries: ['"Santa Rosa County" "Florida"'] },
  { tag: 'GDELT FL/Okaloosa', state: 'FL', county: 'Okaloosa', queries: ['"Okaloosa County" "Florida"'] },
  { tag: 'GDELT FL/Walton', state: 'FL', county: 'Walton', queries: ['"Walton County" "Florida"'] },
  { tag: 'GDELT FL/Bay', state: 'FL', county: 'Bay', queries: ['"Bay County" "Florida"'] },
  { tag: 'GDELT FL/Gulf', state: 'FL', county: 'Gulf', queries: ['"Gulf County" "Florida"'] },
  { tag: 'GDELT FL/Franklin', state: 'FL', county: 'Franklin', queries: ['"Franklin County" "Florida"'] },
  { tag: 'GDELT FL/Holmes', state: 'FL', county: 'Holmes', queries: ['"Holmes County" "Florida"'] },
  { tag: 'GDELT FL/Washington', state: 'FL', county: 'Washington', queries: ['"Washington County" "Florida"'] },
  { tag: 'GDELT FL/Jackson', state: 'FL', county: 'Jackson', queries: ['"Jackson County" "Florida"'] },
  { tag: 'GDELT FL/Calhoun', state: 'FL', county: 'Calhoun', queries: ['"Calhoun County" "Florida"'] },

  { tag: 'GDELT AL/Mobile', state: 'AL', county: 'Mobile', queries: ['"Mobile County" "Alabama"'] },
  { tag: 'GDELT AL/Baldwin', state: 'AL', county: 'Baldwin', queries: ['"Baldwin County" "Alabama"'] },
  { tag: 'GDELT AL/Escambia', state: 'AL', county: 'Escambia', queries: ['"Escambia County" "Alabama"'] },
  { tag: 'GDELT AL/Covington', state: 'AL', county: 'Covington', queries: ['"Covington County" "Alabama"'] },
  { tag: 'GDELT AL/Geneva', state: 'AL', county: 'Geneva', queries: ['"Geneva County" "Alabama"'] },
  { tag: 'GDELT AL/Houston', state: 'AL', county: 'Houston', queries: ['"Houston County" "Alabama"'] }
];

// ------------------------------
// Helpers
// ------------------------------
function pad2(n) {
  return String(n).padStart(2, '0');
}

function gdeltUtcStamp(date = new Date()) {
  const d = new Date(date);
  return (
    d.getUTCFullYear() +
    pad2(d.getUTCMonth() + 1) +
    pad2(d.getUTCDate()) +
    pad2(d.getUTCHours()) +
    pad2(d.getUTCMinutes()) +
    pad2(d.getUTCSeconds())
  );
}

function parseGdeltSeenDate(seendate) {
  if (!seendate || typeof seendate !== 'string' || seendate.length < 14) return null;
  const y = Number(seendate.slice(0, 4));
  const mo = Number(seendate.slice(4, 6));
  const da = Number(seendate.slice(6, 8));
  const hh = Number(seendate.slice(8, 10));
  const mm = Number(seendate.slice(10, 12));
  const ss = Number(seendate.slice(12, 14));
  if (![y, mo, da, hh, mm, ss].every(Number.isFinite)) return null;
  return new Date(Date.UTC(y, mo - 1, da, hh, mm, ss));
}

function isValidKeyword(q) {
  return q && String(q).trim().length >= MIN_KEYWORD_LEN;
}

function buildGdeltUrl({ query, startdatetime, enddatetime, maxrecords }) {
  const params = new URLSearchParams();
  params.set('query', query);
  params.set('mode', 'ArtList');
  params.set('format', 'json');
  params.set('sort', 'datedesc');
  params.set('maxrecords', String(maxrecords));
  params.set('startdatetime', startdatetime);
  params.set('enddatetime', enddatetime);
  return `${GDELT_DOC_API}?${params.toString()}`;
}

async function fetchJson(url) {
  const res = await fetch(url, { method: 'GET' });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status} ${res.statusText} :: ${text.slice(0, 300)}`);
  }
  return res.json();
}

// ------------------------------
// DB: ensure ingest table exists
// ------------------------------
async function ensureIngestTable(tableName) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${tableName} (
      id BIGSERIAL PRIMARY KEY,
      source TEXT NOT NULL DEFAULT 'GDELT',
      state TEXT NOT NULL,
      county TEXT NOT NULL,
      region_tag TEXT,
      query TEXT,
      title TEXT,
      url TEXT NOT NULL UNIQUE,
      domain TEXT,
      published_at TIMESTAMPTZ,
      fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      image_url TEXT,
      summary TEXT
    );
  `);

  // Helpful indexes (safe if they already exist)
  await pool.query(`CREATE INDEX IF NOT EXISTS ${tableName}_state_idx ON ${tableName}(state);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS ${tableName}_county_idx ON ${tableName}(county);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS ${tableName}_published_idx ON ${tableName}(published_at DESC);`);

  console.log(`[DB] ensured table=${tableName}`);
}

const UPSERT_SQL = (tableName) => `
  INSERT INTO ${tableName}
    (source, state, county, region_tag, query, title, url, domain, published_at, fetched_at, image_url, summary)
  VALUES
    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
  ON CONFLICT (url) DO UPDATE SET
    title = EXCLUDED.title,
    domain = EXCLUDED.domain,
    published_at = EXCLUDED.published_at,
    fetched_at = EXCLUDED.fetched_at,
    image_url = COALESCE(EXCLUDED.image_url, ${tableName}.image_url),
    summary = COALESCE(EXCLUDED.summary, ${tableName}.summary),
    state = EXCLUDED.state,
    county = EXCLUDED.county,
    region_tag = EXCLUDED.region_tag,
    query = EXCLUDED.query
`;

// ------------------------------
// Main
// ------------------------------
async function run() {
  const started = Date.now();

  console.log(`[CONFIG] gdeltDocApi=${GDELT_DOC_API}`);
  console.log(`[CONFIG] targetTable=${TARGET_TABLE}`);
  console.log(`[SEARCH_INGEST_START] ts=${new Date().toISOString()} lookbackHours=${LOOKBACK_HOURS} maxRecords=${MAX_RECORDS} minKeywordLen=${MIN_KEYWORD_LEN}`);

  const end = gdeltUtcStamp(new Date());
  const start = gdeltUtcStamp(new Date(Date.now() - LOOKBACK_HOURS * 60 * 60 * 1000));
  console.log(`[TIME] start=${start} end=${end}`);

  await ensureIngestTable(TARGET_TABLE);

  let totalResults = 0;
  let totalWrites = 0;

  for (const region of REGIONS) {
    for (const rawQuery of region.queries) {
      const query = String(rawQuery || '').trim();
      if (!isValidKeyword(query)) continue;

      const url = buildGdeltUrl({
        query,
        startdatetime: start,
        enddatetime: end,
        maxrecords: MAX_RECORDS
      });

      try {
        const json = await fetchJson(url);
        const articles = Array.isArray(json?.articles) ? json.articles : [];
        const results = articles.length;

        let writeAttempts = 0;

        for (const a of articles) {
          const link = a?.url || a?.urlsource || a?.sourceurl || null;
          const title = a?.title || null;
          if (!link || !title) continue;

          let domain = null;
          try { domain = new URL(link).hostname; } catch {}

          const publishedAt = parseGdeltSeenDate(a?.seendate) || null;
          const fetchedAt = new Date();
          const imageUrl = a?.socialimage || a?.image || null;
          const summary = a?.summary || a?.description || null;

          try {
            await pool.query(UPSERT_SQL(TARGET_TABLE), [
              'GDELT',
              region.state,
              region.county,
              region.tag,
              query,
              title,
              link,
              domain,
              publishedAt,
              fetchedAt,
              imageUrl,
              summary
            ]);
            totalWrites++;
            writeAttempts++;
          } catch (err) {
            console.error(`[${region.tag}] DB_ERROR url=${link} msg=${err?.message || err}`);
          }
        }

        totalResults += results;
        console.log(`[${region.tag}] ${query} -> results=${results}, writeAttempts=${writeAttempts}`);
      } catch (err) {
        console.error(`[${region.tag}] ERROR: ${err?.message || err}`);
      }
    }
  }

  const durationMs = Date.now() - started;
  console.log(`[SEARCH_INGEST_DONE] table=${TARGET_TABLE} totalResults=${totalResults} totalWrites=${totalWrites} durationMs=${durationMs}`);
}

run()
  .then(async () => {
    await pool.end().catch(() => {});
    process.exit(0);
  })
  .catch(async (err) => {
    console.error(`FATAL_RUN_ERROR: ${err?.message || err}`);
    await pool.end().catch(() => {});
    process.exit(1);
  });
