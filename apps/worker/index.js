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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function envInt(name, fallback) {
  const v = process.env[name];
  if (!v) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

async function fetchText(url, timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        "user-agent": "PanhandlePulseBot/0.1 (+https://panhandlepulse.news)",
      },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return res.text();
  } catch (e) {
    if (e?.name === "AbortError") throw new Error(`Timeout after ${timeoutMs}ms for ${url}`);
    throw e;
  } finally {
    clearTimeout(t);
  }
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
      link: Array.isArray(e.link)
        ? e.link.find((l) => l["@_rel"] === "alternate")?.["@_href"] || ""
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
  return crypto.createHash("sha256").update(`${title}||${link}`).digest("hex");
}

function safeUrlHost(websiteUrl) {
  try {
    return new URL(websiteUrl).host;
  } catch {
    return "";
  }
}

function buildCandidateFeeds(src) {
  const candidates = [];
  const rss = (src.rss_url || "").trim();
  if (rss) candidates.push(rss);

  const website = (src.website_url || "").trim();
  const host = safeUrlHost(website);
  if (!host) return candidates;

  // WordPress common feeds
  candidates.push(`https://${host}/feed/`);
  candidates.push(`https://${host}/feed`);
  candidates.push(`https://${host}/rss`);
  candidates.push(`https://${host}/rss.xml`);
  candidates.push(`https://${host}/atom.xml`);

  // CivicPlus common RSSFeed patterns (News Flash / Alerts)
  // Not guaranteed, but often works if the site uses CivicPlus.
  candidates.push(`https://${host}/RSSFeed.aspx?CID=All-newsflash.xml&ModID=1`);
  candidates.push(`https://${host}/RSSFeed.aspx?CID=All-0&ModID=63`);
  candidates.push(`https://${host}/RSS.aspx`);

  // Some sites host "CivicAlerts" and still have RSSFeed.aspx
  candidates.push(`https://${host}/CivicAlerts.aspx?rss=true`);

  // De-dupe
  return [...new Set(candidates)];
}

async function resolveFeedUrl(src, timeoutMs) {
  const candidates = buildCandidateFeeds(src);

  for (const url of candidates) {
    try {
      const xml = await fetchText(url, timeoutMs);
      const items = parseRss(xml);
      if (items.length > 0) {
        return { feedUrl: url, xml, items };
      }
    } catch {
      // ignore, try next candidate
    }
  }

  return { feedUrl: "", xml: "", items: [] };
}

async function getDbClient() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    throw new Error("DATABASE_URL is missing. Add a Railway Postgres and connect it to this service.");
  }
  const client = new Client({
    connectionString: url,
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();
  return client;
}

async function ensureSchema(client) {
  const candidates = [
    path.join(ROOT, "db", "schema.sql"),
    path.join(ROOT, "apps", "worker", "db", "schema.sql"),
    path.join(ROOT, "..", "db", "schema.sql"),
    path.join(ROOT, "..", "..", "apps", "worker", "db", "schema.sql"),
  ];

  for (const p of candidates) {
    if (fs.existsSync(p)) {
      console.log(`Using schema at: ${p}`);
      const sql = fs.readFileSync(p, "utf8");
      if (!sql.trim()) throw new Error(`schema.sql is empty at: ${p}`);
      await client.query(sql);
      return;
    }
  }

  throw new Error(`schema.sql not found. Tried:\n${candidates.join("\n")}`);
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
    [
      s.state,
      s.county,
      s.source_name,
      s.source_type || "rss",
      s.tier || "2",
      s.website_url || "",
      s.rss_url || "",
      facebook,
      x,
    ]
  );

  return res.rows[0].id;
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

async function runOnce() {
  console.log("Starting Panhandle Pulse Worker…");

  const config = loadSourcesYaml();
  const sources = flattenSources(config);

  console.log(`Loaded sources: ${sources.length}`);

  const timeoutMs = envInt("FETCH_TIMEOUT_MS", 15000);
  const pauseBetweenSourcesMs = envInt("PAUSE_BETWEEN_SOURCES_MS", 250);

  const client = await getDbClient();
  try {
    await ensureSchema(client);

    let ok = 0;
    let failed = 0;
    let attempted = 0;
    let ingestible = 0;

    for (const src of sources) {
      if (!src?.enabled && src?.enabled !== undefined) continue;

      // Attempt to resolve an RSS/Atom feed even if rss_url is blank.
      console.log(`\n[${src.state} / ${src.county}] ${src.source_name}`);

      try {
        const resolved = await resolveFeedUrl(src, timeoutMs);
        if (!resolved.feedUrl) {
          console.log("No feed found (rss_url blank + auto-discovery failed). Skipping.");
          continue;
        }

        ingestible += 1;

        // Store resolved feed back into DB record (so you can see what was used)
        const sourceId = await upsertSource(client, { ...src, rss_url: resolved.feedUrl });

        let insertedForSource = 0;
        for (const it of resolved.items) {
          if (!it.title || !it.link) continue;
          await insertFeedItem(client, sourceId, it);
          insertedForSource += 1;
          attempted += 1;
        }

        console.log(`Feed: ${resolved.feedUrl}`);
        console.log(`Parsed ${resolved.items.length} items, attempted insert ${insertedForSource}`);
        ok += 1;
      } catch (err) {
        console.error(`ERROR: ${err.message}`);
        failed += 1;
      }

      await sleep(pauseBetweenSourcesMs);
    }

    console.log(`\nIngestible feeds found: ${ingestible}`);
    console.log(`Summary: OK=${ok}, Failed=${failed}, InsertAttempts=${attempted}`);
    console.log("Worker run complete.");
  } finally {
    await client.end();
  }
}

runOnce().catch((err) => {
  console.error("Fatal worker error:", err);
  process.exit(1);
});
