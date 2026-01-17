import fs from "node:fs";
import path from "node:path";
import { XMLParser } from "fast-xml-parser";
import yaml from "js-yaml";

const ROOT = process.cwd(); // Railway will run from service root if configured; we’ll handle both cases.
const CONFIG_PATH_CANDIDATES = [
  path.join(ROOT, "config", "sources.yaml"),
  path.join(ROOT, "..", "..", "config", "sources.yaml"), // if cwd is apps/worker
];

function loadSourcesYaml() {
  for (const p of CONFIG_PATH_CANDIDATES) {
    if (fs.existsSync(p)) {
      const raw = fs.readFileSync(p, "utf8");
      return yaml.load(raw);
    }
  }
  throw new Error(
    `sources.yaml not found. Tried:\n${CONFIG_PATH_CANDIDATES.join("\n")}`
  );
}

function flattenSources(config) {
  const out = [];
  const states = config?.states || {};
  for (const [stateCode, counties] of Object.entries(states)) {
    for (const [countyKey, countyObj] of Object.entries(counties)) {
      if (!countyObj?.enabled) continue;
      for (const s of countyObj?.sources || []) {
        out.push({
          state: stateCode,
          county: countyKey,
          ...s,
        });
      }
    }
  }
  return out;
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: { "user-agent": "PanhandlePulseBot/0.1 (+https://panhandlepulse.news)" },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return await res.text();
}

function normalizeRssItems(xml) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "@_",
    // RSS/Atom are messy; this helps
    removeNSPrefix: true,
  });

  const doc = parser.parse(xml);

  // RSS 2.0
  const rssItems = doc?.rss?.channel?.item;
  if (rssItems) {
    const arr = Array.isArray(rssItems) ? rssItems : [rssItems];
    return arr.map((it) => ({
      title: it?.title ?? "",
      link: it?.link ?? "",
      pubDate: it?.pubDate ?? it?.date ?? "",
      guid: typeof it?.guid === "string" ? it.guid : it?.guid?.["#text"] ?? "",
      summary: it?.description ?? "",
    }));
  }

  // Atom
  const atomEntries = doc?.feed?.entry;
  if (atomEntries) {
    const arr = Array.isArray(atomEntries) ? atomEntries : [atomEntries];
    return arr.map((e) => ({
      title: e?.title?.["#text"] ?? e?.title ?? "",
      link:
        (Array.isArray(e?.link) ? e.link.find((l) => l?.["@_rel"] === "alternate") : e?.link)?.["@_href"] ??
        e?.link?.["@_href"] ??
        "",
      pubDate: e?.updated ?? e?.published ?? "",
      guid: e?.id ?? "",
      summary: e?.summary?.["#text"] ?? e?.summary ?? "",
    }));
  }

  return [];
}

function pickRssSources(sources) {
  return sources
    .filter((s) => typeof s.rss_url === "string" && s.rss_url && s.rss_url !== "null")
    .map((s) => ({
      state: s.state,
      county: s.county,
      source_name: s.source_name,
      tier: s.tier,
      rss_url: s.rss_url,
    }));
}

async function main() {
  const cfg = loadSourcesYaml();
  const sources = flattenSources(cfg);
  const rssSources = pickRssSources(sources);

  console.log(`Loaded sources: ${sources.length}`);
  console.log(`RSS sources: ${rssSources.length}`);

  for (const src of rssSources) {
    console.log("\n---");
    console.log(`[${src.state} / ${src.county}] ${src.source_name}`);
    console.log(`RSS: ${src.rss_url}`);

    try {
      const xml = await fetchText(src.rss_url);
      const items = normalizeRssItems(xml).slice(0, 5);
      console.log(`Found ${items.length} items (showing up to 5):`);
      for (const it of items) {
        console.log(`- ${it.title}`);
        console.log(`  ${it.link}`);
      }
    } catch (err) {
      console.log(`ERROR: ${err.message}`);
    }
  }

  console.log("\nDone.");
}

main().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});
