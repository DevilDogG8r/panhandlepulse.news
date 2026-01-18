import { Pool } from "pg";

declare global {
  // eslint-disable-next-line no-var
  var __pp_pool: Pool | undefined;
}

function requireDatabaseUrl(): string {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error("DATABASE_URL is not set for the web service.");
  return url;
}

export function getPool(): Pool {
  if (!global.__pp_pool) {
    global.__pp_pool = new Pool({
      connectionString: requireDatabaseUrl(),
      max: 10,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 10_000,
      ssl: { rejectUnauthorized: false }
    });
  }
  return global.__pp_pool;
}
