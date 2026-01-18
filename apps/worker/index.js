import fs from "node:fs";
import path from "node:path";
import yaml from "js-yaml";
import crypto from "node:crypto";
import { XMLParser } from "fast-xml-parser";
import pg from "pg";

const { Client } = pg;

const ROOT = process.cwd();

const CONFIG_PATH_CANDIDATES = [
  path.join(ROOT, "config", "sources.yaml"),
  path.join(ROOT, "..", "config", "sources.yaml"),
  path.join(ROOT, "..", "..", "config", "sources.yaml"),
  path.join(ROOT, "apps", "worker", "config", "sources.yaml"),
];

function loadSourcesYaml() {
  for (const p of CONFIG_PATH_CANDIDATES) {
    if (fs.existsSync(p)) {
      console.log(`Using sources.yaml at: ${p}`);
      const raw = fs.readFileSync(p, "utf8");
      const parsed = yaml.load(raw);
      const keys = parsed ? Object.keys(parsed) : [];
      console.log(
        `Top-level keys in sources.yaml: ${keys.length ? keys.join(", ") : "(none)"}`
      );
      return parsed;
    }
  }
  throw new Error(
    `sources.yaml not found. Tried:\n${CONFIG_PATH_CANDIDATES.join("\n")}`
  );
}

function flattenSources(config) {
  const out = [];
  if (!config || !config.states) return out;

  for (const [stateCode, counties] of Object.entries(config.states)) {
    for (const [countyName, countyData] of Object.entries(counties)) {
      if (!countyData?.enabled) continue;

      for (const src of countyData?.sources || []) {
        out.push({ state: stateCode, county: countyName, ...src });
      }
    }
  }
  return out;
}

function pickRssSources(sources) {
  return sources.filter(
    (s) => typeof s.rss_url === "string" && s.rss_url.trim() !== ""
  );
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "PanhandlePulseBot/0.1 (+https://panhandlepulse.news)",
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.text();
}

function parseRss(xml) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "@_",
    removeNSPrefix: true,
  });

  const doc = parser.parse(xml);

  // RSS 2.0
  if (doc?.rss?.channel?.item) {
    const items = Array.isArray(doc.rss.channel.item)
      ? doc.rss.channel.item
      : [doc.rss.channel.item];

    return items.map((it) => ({
      title: it.title || "",
      link: it.link || "",
      pubDate: it.pubDate || "",
      summary: it.description || "",
    }));
  }

  // Atom
  if (doc?.feed?.entry) {
    const entries = Array.isArray(doc.feed.entry)
      ? doc.feed.entry
      : [doc.feed.entry];

    return entries.map((e) => ({
      title: e.title?.["#text"] || e.title || "",
      link:
        Array.isArray(e.link)
          ? e.link.find((l) => l["@_rel"] === "alternate")?.["@_href"]
          : e.link?.["@_href"] || "",
      pubDate: e.updated || e.published || "",
      summary: e.summary?.["#text"] || "",
    }));
  }

  return [];
}

function toTimestamp(pubDate) {
  if (!pubDate) return null;
  const d = new Date(pubDate);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function hashItem(title, link) {
  return crypto
    .createHash("sha256")
    .update(`${title}||${link}`)
    .digest("hex");
}

async function getDbClient() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    throw new Error(
      "DATABASE_URL is missing. Add a Railway Postgres and connect it to this service."
    );
  }
  const client = new Client({ connectionString: url, ssl: { rejectUnauthorized: false } });
  await client.connect();
  return client;
}

async function ensureSchema(client) {
  const sqlPath = path.join(ROOT, "db.sql");
  if (!fs.existsSync(sqlPath)) {
    // If running from repo root
    const alt = path.join(ROOT, "apps", "worker", "db.sql");
    if (fs.existsSync(alt)) {
      const sql = fs.readFileSync(alt, "utf8");
      await client.query(sql);
      return;
    }
    throw new Error("db.sql not found near worker. Ensure apps/worker/db.sql exists.");
  }
  const sql = fs.readFileSync(sqlPath, "utf8");
  await client.query(sql);
}

async function upsertSource(client, s) {
  const facebook = s?.social_urls?.facebook || "";
  const x = s?.social_urls?.x || "";

  const res = await client.query(
    `
    INSERT INTO sources (state, county, source_name, source_type, tier, website_url, rss_url, facebook_url, x_url, enabled)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,TRUE)
    ON CONFLICT (state, county, source_name)
    DO UPDATE SET
      source_type = EXCLUDED.source_type,
      tier = EXCLUDED.tier,
      website_url = EXCLUDED.website_url,
      rss_url = EXCLUDED.rss_url,
      facebook_url = EXCLUDED.facebook_url,
      x_url = EXCLUDED.x_url,
      enabled = TRUE
    RETURNING id
    `,
    [s.state, s.county, s.source_name, s.source_type, s.tier, s.website_url || "", s.rss_url || "", facebook, x]
  );

  return res.rows[0].id;
}

// Add unique constraint for upsertSource if missing
async function ensureSourceUniq(client) {
  await client.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_indexes WHERE indexname = 'sources_state_county_name_unique'
      ) THEN
        CREATE UNIQUE INDEX sources_state_county_name_unique
        ON sources(state, county, source_name);
      END IF;
    END $$;
  `);
}

async function insertFeedItem(client, sourceId, item) {
  const contentHash = hashItem(item.title, item.link);
  const publishedAt = toTimestamp(item.pubDate);

  await client.query(
    `
    INSERT INTO feed_items (source_id, title, link, published_at, summary, content_hash)
    VALUES ($1,$2,$3,$4,$5,$6)
    ON CONFLICT (source_id, content_hash) DO NOTHING
    `,
    [sourceId, item.title, item.link, publishedAt, item.summary || "", contentHash]
  );
}

async function main() {
  console.log("Starting Panhandle Pulse Worker…");

  const config = loadSourcesYaml();
  const sources = flattenSources(config);
  const rssSources = pickRssSources(sources);

  console.log(`Loaded sources: ${sources.length}`);
  console.log(`RSS sources: ${rssSources.length}`);

  const client = await getDbClient();
  try {
    await ensureSchema(client);
    await ensureSourceUniq(client);

    let ok = 0;
    let failed = 0;
    let stored = 0;

    for (const src of rssSources) {
      console.log(`\n[${src.state} / ${src.county}] ${src.source_name}`);
      console.log(`RSS: ${src.rss_url}`);

      try {
        const sourceId = await upsertSource(client, src);

        const xml = await fetchText(src.rss_url);
        const items = parseRss(xml);

        let insertedForSource = 0;
        for (const it of items) {
          if (!it.title || !it.link) continue;
          await insertFeedItem(client, sourceId, it);
          insertedForSource += 1;
        }

        console.log(`Parsed ${items.length} items, attempted insert ${insertedForSource}`);
        stored += insertedForSource;
        ok += 1;
      } catch (err) {
        console.error(`ERROR: ${err.message}`);
        failed += 1;
      }
    }

    console.log(`\nSummary: RSS OK=${ok}, Failed=${failed}, InsertAttempts=${stored}`);
    console.log("Worker run complete.");
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  console.error("Fatal worker error:", err);
  process.exit(1);
});
