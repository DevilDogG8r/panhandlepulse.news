async function ensureSchema(client) {
  const sqlPath = path.join(ROOT, "db", "schema.sql");

  // If running from repo root (or different CWD), try common alternatives
  const altCandidates = [
    sqlPath,
    path.join(ROOT, "apps", "worker", "db", "schema.sql"),
    path.join(ROOT, "..", "db", "schema.sql"),
    path.join(ROOT, "..", "..", "apps", "worker", "db", "schema.sql"),
  ];

  for (const p of altCandidates) {
    if (fs.existsSync(p)) {
      console.log(`Using schema at: ${p}`);
      const sql = fs.readFileSync(p, "utf8");
      await client.query(sql);
      return;
    }
  }

  throw new Error(
    `schema.sql not found. Tried:\n${altCandidates.join("\n")}`
  );
}
