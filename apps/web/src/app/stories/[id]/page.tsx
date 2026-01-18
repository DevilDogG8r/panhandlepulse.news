// apps/web/src/app/stories/[id]/page.tsx
import Link from "next/link";
import { getStoryWithSources } from "@/lib/storyQueries";

export const dynamic = "force-dynamic";

function escapeHtml(s: string) {
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

// Minimal markdown-ish rendering (safe + simple).
// Later we can add a real markdown renderer.
function renderMarkdown(md: string) {
  const safe = escapeHtml(md || "");
  const paragraphs = safe.split(/\n\s*\n/).filter(Boolean);
  return paragraphs.map((p, idx) => <p key={idx} style={{ marginTop: 12, lineHeight: 1.7, opacity: 0.92 }} dangerouslySetInnerHTML={{ __html: p.replace(/\n/g, "<br/>") }} />);
}

export default async function StoryDetail({ params }: { params: { id: string } }) {
  const data = await getStoryWithSources(params.id);
  if (!data) {
    return (
      <main style={{ maxWidth: 900, margin: "0 auto", padding: "32px 16px" }}>
        <h1>Not found</h1>
        <Link href="/stories">Back</Link>
      </main>
    );
  }

  const { story, sources } = data;

  return (
    <main style={{ maxWidth: 900, margin: "0 auto", padding: "32px 16px" }}>
      <Link href="/stories" style={{ opacity: 0.8 }}>
        ← Stories
      </Link>

      <div style={{ marginTop: 14, display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
        <span style={{ fontSize: 12, opacity: 0.75 }}>
          {story.state} / {story.county}
        </span>
        <span style={{ fontSize: 12, opacity: 0.55 }}>
          {new Date(story.created_at).toLocaleString()}
        </span>
      </div>

      <h1 style={{ fontSize: 34, fontWeight: 900, marginTop: 10 }}>{story.title}</h1>
      {story.dek ? <p style={{ marginTop: 10, fontSize: 16, opacity: 0.85 }}>{story.dek}</p> : null}

      <section style={{ marginTop: 18 }}>
        {renderMarkdown(story.body_markdown)}
      </section>

      <section style={{ marginTop: 28, borderTop: "1px solid rgba(255,255,255,0.08)", paddingTop: 18 }}>
        <h2 style={{ fontSize: 18, fontWeight: 800 }}>Sources used</h2>
        <ul style={{ marginTop: 10 }}>
          {sources.map((s: any, idx: number) => (
            <li key={idx} style={{ marginTop: 8 }}>
              <a href={s.source_link} target="_blank" rel="noreferrer" style={{ fontWeight: 700 }}>
                {s.source_title}
              </a>
              {s.source_published_at ? (
                <span style={{ marginLeft: 8, fontSize: 12, opacity: 0.65 }}>
                  {new Date(s.source_published_at).toLocaleString()}
                </span>
              ) : null}
            </li>
          ))}
        </ul>

        <p style={{ marginTop: 14, fontSize: 12, opacity: 0.6 }}>
          Note: Stories are AI-written summaries based on linked sources. Read the originals for full context.
        </p>
      </section>
    </main>
  );
}
