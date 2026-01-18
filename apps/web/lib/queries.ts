// apps/web/lib/queries.ts
import { Pool } from "pg";

let pool: Pool | null = null;

function getPool() {
  if (!pool) {
    if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL not set");
    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.PGSSLMODE === "disable" ? false : { rejectUnauthorized: false },
    });
  }
  return pool;
}

export type FeedItem = {
  id: number;
  title: string;
  url: string;
  published_at: string | null;
  fetched_at: string | null;
  source: string | null;
  county: string | null;
  state: string | null;
  image_url: string | null;
  summary: string | null;
};

function toFeedItem(row: any): FeedItem {
  return {
    id: Number(row.id),
    title: row.title,
    url: row.url,
    published_at: row.published_at,
    fetched_at: row.fetched_at,
    source: row.source ?? row.domain ?? null,
    county: row.county ?? null,
    state: row.state ?? null,
    image_url: row.image_url ?? row.socialimage ?? null,
    summary: row.summary ?? row.description ?? row.excerpt ?? null,
  };
}

/**
 * Latest RSS/Stories (dedup by URL)
 * Assumes your RSS pipeline writes into `stories`.
 */
export async function getLatestStories(limit = 50): Promise<FeedItem[]> {
  const db = getPool();

  // Dedup: keep the newest row per URL
  const sql = `
    SELECT DISTINCT ON (url)
      id,
      title,
      url,
      published_at,
      fetched_at,
      source,
      county,
      state,
      image_url,
      summary
    FROM stories
    WHERE url IS NOT NULL AND title IS NOT NULL
    ORDER BY url, COALESCE(published_at, fetched_at) DESC
    LIMIT $1
  `;

  const { rows } = await db.query(sql, [limit]);
  return rows.map(toFeedItem);
}

/**
 * Latest GDELT search ingest (dedup by URL)
 */
export async function getLatestSearch(limit = 50): Promise<FeedItem[]> {
  const db = getPool();

  const sql = `
    SELECT DISTINCT ON (url)
      id,
      title,
      url,
      published_at,
      fetched_at,
      domain as source,
      county,
      state,
      image_url,
      summary
    FROM panhandle_search_articles
    WHERE url IS NOT NULL AND title IS NOT NULL
    ORDER BY url, COALESCE(published_at, fetched_at) DESC
    LIMIT $1
  `;

  const { rows } = await db.query(sql, [limit]);
  return rows.map(toFeedItem);
}

/**
 * Combined feed (RSS + GDELT), dedup across both by URL.
 * If the same URL appears in both, newest wins.
 */
export async function getCombinedFeed(limit = 50): Promise<FeedItem[]> {
  const db = getPool();

  const sql = `
    WITH combined AS (
      SELECT
        id,
        title,
        url,
        published_at,
        fetched_at,
        source,
        county,
        state,
        image_url,
        summary
      FROM stories
      WHERE url IS NOT NULL AND title IS NOT NULL

      UNION ALL

      SELECT
        id,
        title,
        url,
        published_at,
        fetched_at,
        domain as source,
        county,
        state,
        image_url,
        summary
      FROM panhandle_search_articles
      WHERE url IS NOT NULL AND title IS NOT NULL
    )
    SELECT DISTINCT ON (url)
      *
    FROM combined
    ORDER BY url, COALESCE(published_at, fetched_at) DESC
    LIMIT $1
  `;

  const { rows } = await db.query(sql, [limit]);
  return rows.map(toFeedItem);
}
