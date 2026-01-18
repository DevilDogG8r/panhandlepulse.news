import { ArticleList } from "../components/ArticleList";
import { getLatestFeedItems } from "../lib/queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function HomePage() {
  const items = await getLatestFeedItems(50);

  return (
    <div className="grid">
      <div className="card">
        <strong>Latest</strong>
        <div className="meta">{items.length} shown</div>
      </div>

      <ArticleList items={items} />
    </div>
  );
}
