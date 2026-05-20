import { Elysia } from "elysia";
import { Database } from "bun:sqlite";
import { spawn } from "bun";
import { cpus } from "os";

const port = 3005;

// BUN_WORKERS controls multi-process mode via SO_REUSEPORT.
// Default 1 (single-process). BUN_WORKERS=0 means NumCPU.
let workers = parseInt(Bun.env.BUN_WORKERS || "1", 10);
if (!Number.isFinite(workers) || workers < 0) workers = 1;
if (workers === 0) workers = cpus().length;

const isChild = Bun.env.BUN_WORKER_CHILD === "1";

if (workers > 1 && !isChild) {
  console.log(`bun+elysia spawning ${workers} workers on port ${port}`);
  const procs = [];
  for (let i = 0; i < workers; i++) {
    procs.push(
      spawn({
        cmd: ["bun", "run", import.meta.path],
        env: { ...process.env, BUN_WORKER_CHILD: "1" },
        stdout: "inherit",
        stderr: "inherit",
      })
    );
  }
  const shutdown = () => {
    for (const p of procs) p.kill();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
  await Promise.all(procs.map((p) => p.exited));
  process.exit(0);
}

const dbPath = Bun.env.BENCH_DB || "/tmp/uwsbench/sample.db";
const db = new Database(dbPath);
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA synchronous = NORMAL");
db.exec(`CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  name TEXT,
  email TEXT,
  role TEXT
)`);
const count = (db.query("SELECT COUNT(*) AS c FROM users").get() as { c: number }).c;
if (count < 1000) {
  const ins = db.prepare("INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)");
  const tx = db.transaction(() => {
    for (let i = 1; i <= 1000; i++) {
      ins.run(i, `User ${i}`, `user${i}@example.com`, "user");
    }
  });
  tx();
}
const dbStmt = db.query("SELECT name, email, role FROM users WHERE id = ?");

const app = new Elysia()
  .get("/plain", () => new Response("hello world\n", {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  }))
  .get("/hello", () => new Response("hello world\n", {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  }))
  .get("/json", () => new Response('{"message":"hello world","ok":true}\n', {
    headers: { "Content-Type": "application/json" },
  }))
  .get("/hello/:name", ({ params }) => new Response(`hello ${params.name}\n`, {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  }))
  .get("/db", () => {
    const id = 1 + Math.floor(Math.random() * 1000);
    const row = dbStmt.get(id) as { name: string; email: string; role: string };
    return new Response(
      `{"id":${id},"name":${JSON.stringify(row.name)},"email":${JSON.stringify(row.email)},"role":${JSON.stringify(row.role)}}\n`,
      { headers: { "Content-Type": "application/json" } }
    );
  })
  .listen({ port, reusePort: workers > 1 });

const tag = workers > 1 ? ` (worker ${process.pid})` : "";
console.log(`bun+elysia listening on http://localhost:${port}${tag}`);
