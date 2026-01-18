// apps/web/app/page.tsx
import Link from "next/link";
import { getCombinedFeed } from "../lib/queries";

function timeAgo(dateStr?: string | null) {
  if (!dateStr) return "";
  const d = new Date(dateStr);
  const diff = Date.now() - d.getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

function cleanSource(s?: string | null) {
  if (!s) return "";
  return s.replace(/^www\./, "");
}

export default async function HomePage() {
  const items = await getCombinedFeed(60);

  const hero = items[0];
  const topGrid = items.slice(1, 7);
  const latest = items.slice(7, 35);

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-50">
      <header className="sticky top-0 z-20 border-b border-zinc-800 bg-zinc-950/85 backdrop-blur">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-4 py-3">
          <Link href="/" className="flex items-center gap-2">
            <div className="h-8 w-8 rounded-lg bg-zinc-100" />
            <div className="leading-tight">
              <div className="text-lg font-semibold">Panhandle Pulse</div>
              <div className="text-xs text-zinc-400">Local news • FL Panhandle • SE Alabama</div>
            </div>
          </Link>

          <nav className="hidden gap-6 text-sm text-zinc-200 md:flex">
            <Link className="hover:text-white" href="/">Home</Link>
            <Link className="hover:text-white" href="/local">Local</Link>
            <Link className="hover:text-white" href="/topics">Topics</Link>
            <Link className="hover:text-white" href="/search">Search</Link>
            <Link className="hover:text-white" href="/about">About</Link>
          </nav>
        </div>
      </header>

      <main className="mx-auto max-w-6xl px-4 py-6">
        {hero && (
          <section className="grid gap-6 md:grid-cols-12">
            <a href={hero.url} target="_blank" rel="noreferrer" className="group md:col-span-8">
              <div className="overflow-hidden rounded-2xl border border-zinc-800 bg-zinc-900">
                <div className="relative aspect-[16/9] w-full bg-zinc-800">
                  {hero.image_url ? (
                    // eslint-disable-next-line @next/next/no-img-element
                    <img
                      src={hero.image_url}
                      alt={hero.title}
                      className="h-full w-full object-cover transition-transform duration-300 group-hover:scale-[1.02]"
                    />
                  ) : null}
                </div>
                <div className="p-5">
                  <div className="flex flex-wrap items-center gap-2 text-xs text-zinc-400">
                    <span className="rounded-full border border-zinc-700 px-2 py-0.5">Top Story</span>
                    {hero.state && hero.county ? (
                      <span className="rounded-full border border-zinc-700 px-2 py-0.5">
                        {hero.county} • {hero.state}
                      </span>
                    ) : null}
                    <span className="ml-auto">{timeAgo(hero.published_at ?? hero.fetched_at)}</span>
                  </div>

                  <h1 className="mt-3 text-2xl font-semibold leading-snug md:text-3xl">
                    {hero.title}
                  </h1>

                  {hero.summary ? (
                    <p className="mt-3 line-clamp-3 text-sm text-zinc-300">{hero.summary}</p>
                  ) : null}

                  <div className="mt-4 text-sm text-zinc-400">
                    Source: <span className="text-zinc-200">{cleanSource(hero.source)}</span>
                  </div>
                </div>
              </div>
            </a>

            <aside className="md:col-span-4">
              <div className="rounded-2xl border border-zinc-800 bg-zinc-900 p-5">
                <div className="text-sm font-semibold">Counties</div>
                <div className="mt-3 grid grid-cols-2 gap-2 text-sm">
                  {["Escambia","Santa Rosa","Okaloosa","Walton","Bay","Gulf","Washington","Jackson","Mobile","Baldwin","Covington"].map((c) => (
                    <Link
                      key={c}
                      href={`/local?county=${encodeURIComponent(c)}`}
                      className="rounded-lg border border-zinc-800 bg-zinc-950 px-3 py-2 text-zinc-200 hover:border-zinc-600"
                    >
                      {c}
                    </Link>
                  ))}
                </div>

                <div className="mt-6 text-sm font-semibold">Quick filters</div>
                <div className="mt-3 flex gap-2">
                  <Link
                    href="/local?state=FL"
                    className="rounded-lg border border-zinc-800 bg-zinc-950 px-3 py-2 text-sm text-zinc-200 hover:border-zinc-600"
                  >
                    Florida
                  </Link>
                  <Link
                    href="/local?state=AL"
                    className="rounded-lg border border-zinc-800 bg-zinc-950 px-3 py-2 text-sm text-zinc-200 hover:border-zinc-600"
                  >
                    Alabama
                  </Link>
                </div>

                <div className="mt-6 text-xs text-zinc-500">
                  Read-only beta • Powered by your ingestion pipeline
                </div>
              </div>
            </aside>
          </section>
        )}

        <section className="mt-8">
          <div className="mb-3 flex items-end justify-between">
            <h2 className="text-lg font-semibold">Top Stories</h2>
            <Link href="/latest" className="text-sm text-zinc-400 hover:text-white">
              View all →
            </Link>
          </div>

          <div className="grid gap-4 md:grid-cols-3">
            {topGrid.map((it) => (
              <a
                key={it.url}
                href={it.url}
                target="_blank"
                rel="noreferrer"
                className="group rounded-2xl border border-zinc-800 bg-zinc-900 p-4 hover:border-zinc-600"
              >
                <div className="flex items-center justify-between text-xs text-zinc-400">
                  <span>{it.county && it.state ? `${it.county} • ${it.state}` : "Region"}</span>
                  <span>{timeAgo(it.published_at ?? it.fetched_at)}</span>
                </div>
                <div className="mt-2 font-semibold leading-snug group-hover:underline">{it.title}</div>
                <div className="mt-2 text-xs text-zinc-400">Source: {cleanSource(it.source)}</div>
              </a>
            ))}
          </div>
        </section>

        <section className="mt-10">
          <h2 className="text-lg font-semibold">Latest</h2>
          <div className="mt-3 divide-y divide-zinc-800 rounded-2xl border border-zinc-800 bg-zinc-900">
            {latest.map((it) => (
              <a
                key={it.url}
                href={it.url}
                target="_blank"
                rel="noreferrer"
                className="flex items-start justify-between gap-4 p-4 hover:bg-zinc-900/60"
              >
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2 text-xs text-zinc-400">
                    {it.county && it.state ? (
                      <span className="rounded-full border border-zinc-800 bg-zinc-950 px-2 py-0.5">
                        {it.county} • {it.state}
                      </span>
                    ) : null}
                    <span>{cleanSource(it.source)}</span>
                    <span>•</span>
                    <span>{timeAgo(it.published_at ?? it.fetched_at)}</span>
                  </div>
                  <div className="mt-1 font-medium leading-snug">{it.title}</div>
                  {it.summary ? (
                    <div className="mt-1 line-clamp-2 text-sm text-zinc-300">{it.summary}</div>
                  ) : null}
                </div>

                {it.image_url ? (
                  // eslint-disable-next-line @next/next/no-img-element
                  <img
                    src={it.image_url}
                    alt=""
                    className="hidden h-20 w-28 flex-none rounded-lg object-cover md:block"
                  />
                ) : null}
              </a>
            ))}
          </div>
        </section>
      </main>

      <footer className="border-t border-zinc-800 py-8 text-center text-xs text-zinc-500">
        © {new Date().getFullYear()} Panhandle Pulse • Sources link to original publishers
      </footer>
    </div>
  );
}
