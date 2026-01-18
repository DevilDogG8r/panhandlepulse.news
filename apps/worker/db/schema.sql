-- apps/worker/db.sql

-- Enable UUID generation (Postgres 13+ typically has pgcrypto available)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ----------------------------
-- SOURCES
-- ----------------------------
CREATE TABLE IF NOT EXISTS sources (
  id SERIAL PRIMARY KEY,
  state TEXT NOT NULL,
  county TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_type TEXT NOT NULL DEFAULT 'rss',
  tier TEXT NOT NULL DEFAULT 'secondary',
  website_url TEXT NOT NULL DEFAULT '',
  rss_url TEXT NOT NULL DEFAULT '',
  facebook_url TEXT NOT NULL DEFAULT '',
  x_url TEXT NOT NULL DEFAULT '',
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Unique key for upserts
CREATE UNIQUE INDEX IF NOT EXISTS sources_state_county_name_unique
ON sources(state, county, source_name);

-- ----------------------------
-- FEED ITEMS
-- ----------------------------
CREATE TABLE IF NOT EXISTS feed_items (
  id BIGSERIAL PRIMARY KEY,
  source_id INT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  title TEXT NOT NULL,
  link TEXT NOT NULL,
  published_at TIMESTAMPTZ NULL,
  summary TEXT NOT NULL DEFAULT '',
  content_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dedup within source
CREATE UNIQUE INDEX IF NOT EXISTS feed_items_source_hash_unique
ON feed_items(source_id, content_hash);

CREATE INDEX IF NOT EXISTS feed_items_published_at_idx
ON feed_items(published_at DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS feed_items_source_id_idx
ON feed_items(source_id);

-- ----------------------------
-- AI STORIES (Level 2)
-- ----------------------------
CREATE TABLE IF NOT EXISTS stories (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  state TEXT NOT NULL,
  county TEXT NOT NULL,
  region TEXT NOT NULL DEFAULT '',

  -- What kind of story is it?
  story_type TEXT NOT NULL DEFAULT 'roundup', -- roundup | breaking | public_record | etc.

  title TEXT NOT NULL,
  dek TEXT NOT NULL DEFAULT '',              -- short subtitle
  body_markdown TEXT NOT NULL,               -- the actual story (markdown)
  bullets_json JSONB NOT NULL DEFAULT '[]'::jsonb,

  time_window_start TIMESTAMPTZ NOT NULL,
  time_window_end TIMESTAMPTZ NOT NULL,

  model_name TEXT NOT NULL DEFAULT '',
  prompt_version TEXT NOT NULL DEFAULT 'v1',
  status TEXT NOT NULL DEFAULT 'published',  -- draft | published | failed

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS stories_state_county_created_at_idx
ON stories(state, county, created_at DESC);

-- Which feed_items were used
CREATE TABLE IF NOT EXISTS story_sources (
  story_id UUID NOT NULL REFERENCES stories(id) ON DELETE CASCADE,
  feed_item_id BIGINT NOT NULL REFERENCES feed_items(id) ON DELETE CASCADE,
  source_link TEXT NOT NULL,
  source_title TEXT NOT NULL,
  source_published_at TIMESTAMPTZ NULL,
  PRIMARY KEY (story_id, feed_item_id)
);

CREATE INDEX IF NOT EXISTS story_sources_story_id_idx
ON story_sources(story_id);

CREATE INDEX IF NOT EXISTS story_sources_feed_item_id_idx
ON story_sources(feed_item_id);
