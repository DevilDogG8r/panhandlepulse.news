/**
 * Panhandle Pulse — GDELT Search Ingest Worker (ESM)
 * File: apps/worker/search_ingest.js
 *
 * Fixes:
 * - Fills NOT NULL `state` and `county` columns in `stories`
 * - Schema-aware: only writes columns that exist
 * - Cron-safe: run once, exit
 *
 * Required ENV:
 * - DATABASE_URL
 *
 * Optional ENV:
 * - TARGET_TABLE (default: stories)
 * - GDELT_DOC_API (default: https://api.gdeltproject.org/api/v2/doc/doc)
 * - LOOKBACK_HOURS (default: 12)
 * - MAX_RECORDS (default: 50)
 * - MIN_KEYWORD_LEN (default: 4)
 */

import { Pool } from 'pg';

const TARGET_TABLE = process.env.TARGET_TABLE || 'stories';
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
// IMPORTANT: county here should match what your app expects (usually plain county name without "County")
const REGIONS = [
  { tag: 'GDELT FL/Escambia', state: 'FL', county: 'Escambia', queries: ['Escambia County Florida', '"Escambia County" "Florida"'] },
  { tag: 'GDELT FL/Santa_Rosa', state: 'FL', county: 'Santa Rosa', queries: ['Santa Rosa County Florida', '"Santa Rosa County" "Florida"'] },
  { tag: 'GDELT FL/Okaloosa', state: 'FL', county: 'Okaloosa', queries: ['Okaloosa County Florida', '"Okaloosa County" "Florida"'] },
  { tag: 'GDELT FL/Walton', state: 'FL', county: 'Walton', queries: ['Walton County Florida', '"Walton County" "Florida"'] },
  { tag: 'GDELT FL/Bay', state: 'FL', county: 'Bay', queries: ['Bay County Florida', '"Bay County" "Florida"'] },
  { tag: 'GDELT FL/Gulf', state: 'FL', county: 'Gulf', queries: ['Gulf County Florida', '"Gulf County" "Florida"'] },
  { tag: 'GDELT FL/Franklin', state: 'FL', county: 'Franklin', queries: ['Franklin County Florida', '"Franklin County" "Florida"'] },
  { tag: 'GDELT FL/Holmes', state: 'FL', county: 'Holmes', queries: ['Holmes County Florida', '"Holmes County" "Florida"'] },
  { tag: 'GDELT FL/Washington', state: 'FL', county: 'Washington', queries: ['Washington County Florida', '"Washington County" "Florida"'] },
  { tag: 'GDELT FL/Jackson', state: 'FL', county: 'Jackson', queries: ['Jackson County Florida', '"Jackson County" "Florida"'] },
  { tag: 'GDELT FL/Calhoun', state: 'FL', county: 'Calhoun', queries: ['Calhoun County Florida', '"Calhoun County" "Florida"'] },

  { tag: 'GDELT AL/Mobile', state: 'AL', county: 'Mobile', queries: ['Mobile County Alabama', '"Mobile County" "Alabama"'] },
  { tag: 'GDELT AL/Baldwin', state: 'AL', county: 'Baldwin', queries: ['Baldwin County Alabama', '"Baldwin County" "Alabama"'] },
  { tag: 'GDELT AL/Escambia_AL', state: 'AL', county: 'Escambia', queries: ['Escambia County Alabama', '"Escambia County" "Alabama"'] },
  { tag: 'GDELT AL/Covington', state: 'AL', county: 'Covington', queries: ['Covington County Alabama', '"Covington County" "Alabama"'] },
  { tag: 'GDELT AL/Geneva', state: 'AL', county: 'Geneva', queries: ['Geneva County Alabama', '"Geneva County" "Alabama"'] },
  { tag: 'GDELT AL/Houston', state: 'AL', county: 'Houston', queries: ['Houston County Alabama', '"Houston County" "Alabama"'] }
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

async function getTableColumns(tableName) {
  const [schema, table] = tableName.includes('.')
    ? tableName.split('.', 2)
    : ['public', tableName];

  const { rows } = await pool.query(
    `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    ORDER BY ordinal_position
    `,
    [schema, table]
  );

  if (!rows || rows.length === 0) throw new Error(`Table not found or no columns: ${tableName}`);
  return new Set(rows.map(r => r.column_name));
}

// If no unique key exists, insert-only is fine for now (but duplicates will accumulate).
function pickUniqueKeyColumn(columns) {
  const candidates = ['url', 'link', 'canonical_url', 'guid', 'external_id', 'source_url'];
  for (const c of candidates) if (columns.has(c)) return c;
  return null;
}

function buildRowForTable(columns, { regionTag, state, county, query, article }) {
  const url = article?.url || article?.urlsource || article?.sourceurl || null;
  const title = article?.title || null;
  const publishedAt = parseGdeltSeenDate(article?.seendate) || null;
  const fetchedAt = new Date();
  const image = article?.socialimage || article?.image || null;
  const summary = article?.summary || article?.description || null;

  let domain = null;
  if (url) {
    try { domain = new URL(url).hostname; } catch {}
  }

  const out = {};

  // REQUIRED FIELDS in your table
  if (columns.has('state')) out.state = state;                 // FL/AL
  if (columns.has('county')) out.county = county;              // e.g. "Escambia" (no "County" suffix)

  // URL-ish
  if (columns.has('url')) out.url = url;
  if (columns.has('link')) out.link = url;
  if (columns.has('canonical_url')) out.canonical_url = url;
  if (columns.has('source_url')) out.source_url = url;

  // Title-ish
  if (columns.has('title')) out.title = title;
  if (columns.has('headline')) out.headline = title;

  // Provider/source-ish
  if (columns.has('provider')) out.provider = 'GDELT';
  if (columns.has('source_name')) out.source_name = 'GDELT';
  if (columns.has('origin')) out.origin = 'GDELT';

  // Region/query metadata
  if (columns.has('region_tag')) out.region_tag = regionTag;
  if (columns.has('region')) out.region = regionTag;
  if (columns.has('query')) out.query = query;

  // Domain
  if (columns.has('domain')) out.domain = domain;
  if (columns.has('host')) out.host = domain;

  // Times
  if (columns.has('published_at')) out.published_at = publishedAt;
  if (columns.has('published')) out.published = publishedAt;
  if (columns.has('pub_date')) out.pub_date = publishedAt;
  if (columns.has('fetched_at')) out.fetched_at = fetchedAt;
  if (columns.has('ingested_at')) out.ingested_at = fetchedAt;
  if (columns.has('created_at')) out.created_at = fetchedAt;

  // Content
  if (columns.has('summary')) out.summary = summary;
  if (columns.has('description')) out.description = summary;
  if (columns.has('excerpt')) out.excerpt = summary;

  // Image
  if (columns.has('image')) out.image = image;
  if (columns.has('image_url')) out.image_url = image;
  if (columns.has('thumbnail')) out.thumbnail = image;

  return out;
}

function buildInsertSql({ tableName, columnsToInsert, conflictColumn, updateColumns }) {
  const cols = columnsToInsert.map(c => `"${c}"`).join(', ');
  const vals = columnsToInsert.map((_, i) => `$${i + 1}`).join(', ');

  let sql = `INSERT INTO ${tableName} (${cols}) VALUES (${vals})`;

  if (conflictColumn && updateColumns.length > 0) {
    const sets = updateColumns.map(c => `"${c}" = EXCLUDED."${c}"`).join(', ');
    sql += ` ON CONFLICT ("${conflictColumn}") DO UPDATE SET ${sets}`;
  }

  return sql;
}

async function writeOne({ tableName, tableColumns, conflictColumn, regionTag, state, county, query, article }) {
  const row = buildRowForTable(tableColumns, { regionTag, state, county, query, article });
  const keys = Object.keys(row).filter(k => row[k] !== undefined);

  if (keys.length === 0) return { didWrite: false };

  // Hard guarantee: if these columns exist, never allow null
  if (tableColumns.has('state') && !row.state) throw new Error('missing required state');
  if (tableColumns.has('county') && !row.county) throw new Error('missing required county');

  if (conflictColumn && !row[conflictColumn]) return { didWrite: false };

  const updateColumns = conflictColumn ? keys.filter(k => k !== conflictColumn) : [];
  const sql = buildInsertSql({
    tableName,
    columnsToInsert: keys,
    conflictColumn,
    updateColumns
  });

  const values = keys.map(k => row[k]);
  await pool.query(sql, values);
  return { didWrite: true };
}

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

  const tableColumns = await getTableColumns(TARGET_TABLE);
  console.log(`[DB] detected columns (${tableColumns.size}) on ${TARGET_TABLE}`);

  const conflictColumn = pickUniqueKeyColumn(tableColumns);
  console.log(`[DB] upsert key column=${conflictColumn || 'NONE (insert-only)'}`);

  if (tableColumns.has('state')) console.log('[DB] will fill state=FL/AL');
  if (tableColumns.has('county')) console.log('[DB] will fill county from region mapping');

  let totalResults = 0;
  let totalWrites = 0;
  let totalRowErrors = 0;

  for (const region of REGIONS) {
    const regionTag = region.tag;
    const state = region.state;
    const county = region.county;

    for (const rawQuery of region.queries) {
      const query = String(rawQuery || '').trim();

      if (!isValidKeyword(query)) {
        console.log(`[${regionTag}] SKIP_KEYWORD_TOO_SHORT keyword="${query}" len=${query.length}`);
        continue;
      }

      const url = buildGdeltUrl({ query, startdatetime: start, enddatetime: end, maxrecords: MAX_RECORDS });

      try {
        const json = await fetchJson(url);
        const articles = Array.isArray(json?.articles) ? json.articles : [];
        const results = articles.length;

        let writeAttempts = 0;

        for (const a of articles) {
          try {
            const r = await writeOne({
              tableName: TARGET_TABLE,
              tableColumns,
              conflictColumn,
              regionTag,
              state,
              county,
              query,
              article: a
            });
            if (r.didWrite) {
              totalWrites++;
              writeAttempts++;
            }
          } catch (err) {
            totalRowErrors++;
            const au = a?.url || a?.urlsource || a?.sourceurl || '';
            console.error(`[${regionTag}] DB_ERROR url=${au} msg=${err?.message || err}`);
          }
        }

        totalResults += results;
        console.log(`[${regionTag}] ${query} -> results=${results}, writeAttempts=${writeAttempts}`);
      } catch (err) {
        console.error(`[${regionTag}] ERROR: ${err?.message || err}`);
      }
    }
  }

  const durationMs = Date.now() - started;
  console.log(`[SEARCH_INGEST_DONE] table=${TARGET_TABLE} totalResults=${totalResults} totalWrites=${totalWrites} totalRowErrors=${totalRowErrors} durationMs=${durationMs}`);
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
