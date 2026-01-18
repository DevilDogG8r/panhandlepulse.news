/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  experimental: {
    // keep defaults; no edge runtime for DB access
  }
};

export default nextConfig;
