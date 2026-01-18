// apps/web/src/app/stories/page.tsx
import Link from "next/link";
import { getLatestStories } from "../../lib/storyQueries";

export const dynamic = "force-dynamic";

export default async function StoriesPage() {
  const stories = await getLatestStories(30);

  return (
    <main style={{ maxWidth: 980, margin: "0 auto", padding: "32px 16px" }}>
      <h1 style={{ fontSize: 34, fontWeight: 800 }}>Stories</h1>
      <p style={{ opacity: 0.8, marginTop: 8 }}>
        AI-written roundups based on multiple local sources, with citations.
      </p>

      <div style={{ marginTop: 24, display: "grid", gap: 14 }}>
        {stories.map((s) => (
          <div
            key={s.id}
            style={{ border: "1px solid rgba(255,255,255,0.08)", borderRadius: 14, padding: 16 }}
          >
            <div style={{ display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
              <span style={{ fontSize: 12, opacity: 0.75 }}>
                {s.state} / {s.county}
              </span>
              <span style={{ fontSize: 12, opacity: 0.55 }}>{new Date(s.created_at).toLocaleString()}</span>
            </div>

            <h2 style={{ fontSize: 20, fontWeight: 800, marginTop: 10 }}>
              <Link href={`/stories/${s.id}`} style={{ textDecoration: "none" }}>
                {s.title}
              </Link>
            </h2>

            {s.dek ? <p style={{ marginTop: 8, opacity: 0.85 }}>{s.dek}</p> : null}

            <div style={{ marginTop: 10 }}>
              <Link href={`/stories/${s.id}`} style={{ fontWeight: 700 }}>
                Read →
              </Link>
            </div>
          </div>
        ))}
      </div>
    </main>
  );
}
