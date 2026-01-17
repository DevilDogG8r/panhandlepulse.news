import fs from "node:fs";
import path from "node:path";
import yaml from "js-yaml";
import { XMLParser } from "fast-xml-parser";

/**
 * Determine where the worker is running from and
 * try all reasonable locations for config/sources.yaml
 */
const ROOT = process.cwd();

const CONFIG_PATH_CANDIDATES = [
  path.join(ROOT, "config", "sources.yaml"),                 // repo root deploy
  path.join(ROOT, "..", "config", "sources.yaml"),          // if cwd is apps/worker
  path.join(ROOT, "..", "..", "config", "sources.yaml"),    // deep fallback
];

function loadSourcesYaml() {
  for (const p of CONFIG_PATH_CANDIDATES) {
    if (fs.existsSync(p)) {
      console.log(`Using sources.yaml at: ${p}`);
      const raw = fs.readFileSync(p, "utf8");
      const parsed = yaml.load(raw);

      console.log(
        `Top-level keys in sources.yaml: ${
          parsed ? Object.keys(parsed).join(", ") : "(none)"
        }`
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

  if (!config || !config.states) {
    console.log("No states found in sources.yaml");
    return out;
  }

  for (const [stateCode, counties] of Object.entries(config.states)) {
    for (const [countyName, countyData] of Object.entries(counties)) {
      if (!countyData.enabled) continue;

      for (const src of countyData.sources || []) {
        out.push({
          state: stateCode,
          county: countyName,
          ...src,
        });
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

  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }

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

async function main() {
  console.log("Starting Panhandle Pulse Worker…");

  const config = loadSourcesYaml();
  const sources = flattenSources(config);
  const rssSources = pickRssSources(sources);

  console.log(`Loaded sources: ${sources.length}`);
  console.log(`RSS sources: ${rssSources.length}`);

  for (const src of rssSources) {
    console.log(
      `\n[${src.state} / ${src.county}] ${src.source_name}\n${src.rss_url}`
    );

    try {
      const xml = await fetchText(src.rss_url);
      const items = parseRss(xml).slice(0, 5);

      console.log(`Found ${items.length} items`);
      for (const it of items) {
        console.log(`- ${it.title}`);
        console.log(`  ${it.link}`);
      }
    } catch (err) {
      console.error(`ERROR fetching ${src.rss_url}: ${err.message}`);
    }
  }

  console.log("\nWorker run complete.");
}

main().catch((err) => {
  console.error("Fatal worker error:", err);
  process.exit(1);
});
