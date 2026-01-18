// apps/web/src/lib/storyQueries.ts
import { Pool } from "pg";

declare global {
  // eslint-disable-next-line no-var
  var __pgPoolStories: Pool | undefined;
}

function getPool() {
  if (!global.__pgPoolStories) {
    global.__pgPoolStories = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    });
  }
  return global.__pgPoolStories;
}

export type StoryRow = {
  id: string;
  state: string;
  county: string;
  title: string;
  dek: string;
  body_markdown: string;
  bullets_json: any;
  created_at: string;
  time_window_start: string;
  time_window_end: string;
};

export async function getLatestStories(limit = 20): Promise<StoryRow[]> {
  const pool = getPool();
  const res = await pool.query(
    `
    SELECT id, state, county, title, dek, body_markdown, bullets_json, created_at, time_window_start, time_window_end
    FROM stories
    WHERE status = 'published'
    ORDER BY created_at DESC
    LIMIT $1
    `,
    [limit]
  );
  return res.rows;
}

export async function getCountyStories(state: string, county: string, limit = 30): Promise<StoryRow[]> {
  const pool = getPool();
  const res = await pool.query(
    `
    SELECT id, state, county, title, dek, body_markdown, bullets_json, created_at, time_window_start, time_window_end
    FROM stories
    WHERE status = 'published'
      AND state = $1
      AND county = $2
    ORDER BY created_at DESC
    LIMIT $3
    `,
    [state, county, limit]
  );
  return res.rows;
}

export async function getStoryWithSources(id: string) {
  const pool = getPool();
  const storyRes = await pool.query(
    `
    SELECT id, state, county, title, dek, body_markdown, bullets_json, created_at, time_window_start, time_window_end
    FROM stories
    WHERE id = $1
    LIMIT 1
    `,
    [id]
  );
  const story = storyRes.rows[0];
  if (!story) return null;

  const sourcesRes = await pool.query(
    `
    SELECT source_title, source_link, source_published_at
    FROM story_sources
    WHERE story_id = $1
    ORDER BY source_published_at DESC NULLS LAST
    `,
    [id]
  );

  return { story, sources: sourcesRes.rows };
}
