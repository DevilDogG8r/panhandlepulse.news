/**
 * Panhandle Pulse — GDELT Search Ingest Worker
 * Replacement file: apps/worker/search_ingest.js
 *
 * Key goals:
 * - No "keyword too short" errors (we never send short keywords)
 * - No Postgres timezone errors (no AT TIME ZONE, no timezone strings)
 * - Clean cron-friendly: run once, log, exit 0
 *
 * Required ENV:
 * - DATABASE_URL
 *
 * Optional ENV:
 * - GDELT_DOC_API (default: https://api.gdeltproject.org/api/v2/doc/doc)
 * - LOOKBACK_HOURS (default: 12)
 * - MAX_RECORDS (default: 50)   // per query
 * - MIN_KEYWORD_LEN (default: 4)
 */

'use strict';

const { Pool } = require('pg');

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
  // Railway typically uses SSL; pg auto-handles in many cases, but this keeps it safe.
  ssl: process.env.PGSSLMODE === 'disable' ? false : { rejectUnauthorized: false }
});

// ------------------------------------
// Regions / Queries (edit here)
// ------------------------------------
const REGIONS = [
  // Florida Panhandle + nearby
  { tag: 'GDELT FL/Escambia', queries: ['Escambia County Florida', '"Escambia County" "Florida"'] },
  { tag: 'GDELT FL/Santa_Rosa', queries: ['Santa Rosa County Florida', '"Santa Rosa County" "Florida"'] },
  { tag: 'GDELT FL/Okaloosa', queries: ['Okaloosa County Florida', '"Okaloosa County" "Florida"'] },
  { tag: 'GDELT FL/Walton', queries: ['Walton County Florida', '"Walton County" "Florida"'] },
  { tag: 'GDELT FL/Bay', queries: ['Bay County Florida', '"Bay County" "Florida"'] },
  { tag: 'GDELT FL/Gulf', queries: ['Gulf County Florida', '"Gulf County" "Florida"'] },
  { tag: 'GDELT FL/Franklin', queries: ['Franklin County Florida', '"Franklin County" "Florida"'] },
  { tag: 'GDELT FL/Holmes', queries: ['Holmes County Florida', '"Holmes County" "Florida"'] },
  { tag: 'GDELT FL/Washington', queries: ['Washington County Florida', '"Washington County" "Florida"'] },
  { tag: 'GDELT FL/Jackson', queries: ['Jackson County Florida', '"Jackson County" "Florida"'] },
  { tag: 'GDELT FL/Calhoun', queries: ['Calhoun County Florida', '"Calhoun County" "Florida"'] },

  // South Alabama
  { tag: 'GDELT AL/Mobile', queries: ['Mobile County Alabama', '"Mobile County" "Alabama"'] },
  { tag: 'GDELT AL/Baldwin', queries: ['Baldwin County Alabama', '"Baldwin County" "Alabama"'] },
  { tag: 'GDELT AL/Escambia_AL', queries: ['Escambia County Alabama', '"Escambia County" "Alabama"'] },
  { tag: 'GDELT AL/Covington', queries: ['Covington County Alabama', '"Covington County" "Alabama"'] },
  { tag: 'GDELT AL/Geneva', queries: ['Geneva County Alabama', '"Geneva County" "Alabama"'] },
  { tag: 'GDELT AL/Houston', queries: ['Houston County Alabama', '"Houston County" "Alabama"'] }
];

// ------------------------------------
// Helpers
// ------------------------------------
function pad2(n) {
  return String(n).padStart(2, '0');
}

// GDELT wants UTC timestamps like YYYYMMDDHHMMSS
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

// Parse GDELT seendate format "YYYYMMDDHHMMSS" into JS Date (UTC)
// Returns null if missing/invalid.
function parseGdeltSeenDate(seendate) {
  if (!seendate || typeof seendate !== 'string' || seendate.length < 14) return null;
  const y = Number(seendate.slice(0, 4));
  const mo = Number(seendate.slice(4, 6));
  const da = Number(seendate.slice(6, 8));
  const hh = Number(seendate.slice(8, 10));
  const mm = Number(seendate.slice(10, 12));
  const ss = Number(seendate.slice(12, 14));
  if (![y, mo, da, hh, mm, ss].every(Number.isFinite)) return null;

  // Date.UTC expects month 0-11
  return new Date(Date.UTC(y, mo - 1, da, hh, mm, ss));
}

// Safe keyword validation
function isValidKeyword(q) {
  if (!q) return false;
  const cleaned = String(q).trim();
  if (cleaned.length < MIN_KEYWORD_LEN) return false;
  return true;
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

// ------------------------------------
// DB upsert (ONLY place you should need to edit if schema differs)
// ------------------------------------
/**
 * Expected table (example):
 *
 * CREATE TABLE IF NOT EXISTS panhandle_articles (
 *   id BIGSERIAL PRIMARY KEY,
 *   source TEXT,
 *   region_tag TEXT,
 *   query TEXT,
 *   title TEXT,
 *   url TEXT UNIQUE,
 *   domain TEXT,
 *   published_at TIMESTAMPTZ,
 *   fetched_at TIMESTAMPTZ,
 *   image TEXT,
 *   summary TEXT
 * );
 */

const UPSERT_SQL = `
  INSERT INTO panhandle_articles
    (source, region_tag, query, title, url, domain, published_at, fetched_at, image, summary)
  VALUES
    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
  ON CONFLICT (url) DO UPDATE SET
    title = EXCLUDED.title,
    domain = EXCLUDED.domain,
    published_at = EXCLUDED.published_at,
    fetched_at = EXCLUDED.fetched_at,
    image = COALESCE(EXCLUDED.image, panhandle_articles.image),
    summary = COALESCE(EXCLUDED.summary, panhandle_articles.summary)
`;

async function upsertArticles({ regionTag, query, articles }) {
  let insertAttempts = 0;
  const fetchedAt = new Date(); // stored as timestamptz safely (no timezone strings)

  for (const a of articles) {
    const url = a?.url || a?.urlsource || a?.sourceurl;
    const title = a?.title || null;
    if (!url || !title) continue;

    // domain extraction (best effort)
    let domain = null;
    try {
      domain = new URL(url).hostname;
    } catch (_) {}

    const publishedAt = parseGdeltSeenDate(a?.seendate) || null;
    const image = a?.socialimage || a?.image || null;
    const summary = a?.summary || a?.description || null;

    try {
      insertAttempts++;
      await pool.query(UPSERT_SQL, [
        'GDELT',
        regionTag,
        query,
        title,
        url,
        domain,
        publishedAt,     // Date or null
        fetchedAt,       // Date
        image,
        summary
      ]);
    } catch (err) {
      // Keep going; log and continue
      console.error(`[${regionTag}] DB_ERROR url=${url} msg=${err?.message || err}`);
    }
  }

  return insertAttempts;
}

// ------------------------------------
// Main run
// ------------------------------------
async function run() {
  const started = Date.now();

  const end = gdeltUtcStamp(new Date());
  const start = gdeltUtcStamp(new Date(Date.now() - LOOKBACK_HOURS * 60 * 60 * 1000));

  console.log(`[SEARCH_INGEST_START] ts=${new Date().toISOString()} lookbackHours=${LOOKBACK_HOURS} maxRecords=${MAX_RECORDS} minKeywordLen=${MIN_KEYWORD_LEN}`);
  console.log(`[TIME] start=${start} end=${end}`);
  console.log(`[CONFIG] gdeltDocApi=${GDELT_DOC_API}`);

  let totalResults = 0;
  let totalInsertAttempts = 0;

  for (const region of REGIONS) {
    const regionTag = region.tag;

    for (const rawQuery of region.queries) {
      const query = String(rawQuery || '').trim();

      if (!isValidKeyword(query)) {
        console.log(`[${regionTag}] SKIP_KEYWORD_TOO_SHORT keyword="${query}" len=${query.length}`);
        continue;
      }

      const url = buildGdeltUrl({
        query,
        startdatetime: start,
        enddatetime: end,
        maxrecords: MAX_RECORDS
      });

      try {
        const json = await fetchJson(url);

        // GDELT doc API typically returns { articles: [...] } for ArtList
        const articles = Array.isArray(json?.articles) ? json.articles : [];
        const results = articles.length;

        let insertAttempts = 0;
        if (results > 0) {
          insertAttempts = await upsertArticles({ regionTag, query, articles });
        }

        totalResults += results;
        totalInsertAttempts += insertAttempts;

        // Log format matching your current output style
        console.log(`[${regionTag}] ${query} -> results=${results}, insertAttempts=${insertAttempts}`);
      } catch (err) {
        console.error(`[${regionTag}] ERROR: ${err?.message || err}`);
      }
    }
  }

  const durationMs = Date.now() - started;
  console.log(`[SEARCH_INGEST_DONE] totalResults=${totalResults} totalInsertAttempts=${totalInsertAttempts} durationMs=${durationMs}`);
}

// Run once for cron, then exit
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
