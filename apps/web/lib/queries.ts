import { getPool } from "./db";

export type FeedItem = {
  id: string | number;
  title: string;
  url: string;
  published_at: string | null;
  summary: string | null;
  source_id: string | number | null;
};

export async function getLatestFeedItems(limit = 50): Promise<FeedItem[]> {
  const pool = getPool();

  // IMPORTANT: Your worker inserts into feed_items(link, published_at, summary, ...)
  const sql = `
    SELECT
      id,
      COALESCE(title, '') AS title,
      link AS url,
      CASE WHEN published_at IS NULL THEN NULL ELSE published_at::text END AS published_at,
      summary,
      source_id
    FROM feed_items
    ORDER BY
      published_at DESC NULLS LAST,
      id DESC
    LIMIT $1
  `;

  const res = await pool.query(sql, [limit]);

  return res.rows.map((r) => ({
    id: r.id,
    title: r.title,
    url: r.url,
    published_at: r.published_at,
    summary: r.summary ?? null,
    source_id: r.source_id ?? null
  }));
}
