// apps/web/start-railway.cjs
const { spawn } = require("node:child_process");
const fs = require("node:fs");

const port = process.env.PORT || "8080";

// Railway sometimes doesn't provide HOSTNAME; set a safe default
process.env.HOSTNAME = process.env.HOSTNAME || "0.0.0.0";
process.env.PORT = port;

// Prefer standalone server if it exists
const standalonePath = ".next/standalone/server.js";

const cmd = "node";
const args = fs.existsSync(standalonePath)
  ? [standalonePath]
  : ["node_modules/next/dist/bin/next", "start", "-p", port];

console.log(`[START] cmd=${cmd} ${args.join(" ")}`);

const child = spawn(cmd, args, {
  stdio: "inherit",
  env: process.env
});

child.on("exit", (code) => process.exit(code ?? 0));
