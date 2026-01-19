/** @type {import('next').NextConfig} */
const nextConfig = {
  // IMPORTANT:
  // Railway is running `next start -p $PORT`
  // so we MUST NOT use `output: "standalone"`.

  reactStrictMode: true,

  images: {
    remotePatterns: [
      { protocol: "https", hostname: "**" },
      { protocol: "http", hostname: "**" }
    ]
  }
};

export default nextConfig;
