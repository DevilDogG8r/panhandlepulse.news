import { FeedItem } from "../lib/queries";

function formatDate(dateText: string | null) {
  if (!dateText) return "Unknown time";
  const d = new Date(dateText);
  if (Number.isNaN(d.getTime())) return "Unknown time";
  return d.toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "numeric",
    minute: "2-digit"
  });
}

function snippet(text: string | null, max = 220) {
  if (!text) return null;
  const cleaned = text.replace(/\s+/g, " ").trim();
  if (!cleaned) return null;
  return cleaned.length <= max ? cleaned : cleaned.slice(0, max).trimEnd() + "…";
}

export function ArticleCard({ item }: { item: FeedItem }) {
  const s = snippet(item.summary);

  return (
    <article className="card">
      <h3 className="article-title">
        <a href={item.url} target="_blank" rel="noreferrer">
          {item.title || "Untitled"}
        </a>
      </h3>

      <div className="meta">{formatDate(item.published_at)}</div>

      {s ? <p className="summary">{s}</p> : null}

      <div className="meta" style={{ marginTop: 10 }}>
        <a href={item.url} target="_blank" rel="noreferrer">
          Read →
        </a>
      </div>
    </article>
  );
}
