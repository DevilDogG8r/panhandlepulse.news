// apps/web/next.config.mjs

/** @type {import('next').NextConfig} */
const nextConfig = {
  // IMPORTANT:
  // Railway (Railpack) is running `npm run start` -> `next start -p $PORT`.
  // That does NOT support `output: "standalone"`, so we remove it.
  //
  // If later you truly want standalone, we can switch your deploy to run:
  // node .next/standalone/server.js
  // BUT Railway must actually honor that start command first.

  reactStrictMode: true,

  images: {
    remotePatterns: [
      { protocol: "https", hostname: "**" },
      { protocol: "http", hostname: "**" }
    ]
  }
};

export default nextConfig;
