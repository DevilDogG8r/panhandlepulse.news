import { FeedItem } from "../lib/queries";
import { ArticleCard } from "./ArticleCard";

export function ArticleList({ items }: { items: FeedItem[] }) {
  if (!items.length) {
    return (
      <div className="card">
        <strong>No articles yet.</strong>
        <div className="meta">
          If ingestion is running, make sure feed_items has rows and DATABASE_URL is set on the web service.
        </div>
      </div>
    );
  }

  return (
    <div className="grid">
      {items.map((item) => (
        <ArticleCard key={`${item.id}`} item={item} />
      ))}
    </div>
  );
}
