const uWS = require("uWebSockets.js");
const cluster = require("cluster");
const os = require("os");
const path = require("path");

const port = 3003;

// NODE_WORKERS controls multi-process mode via cluster + SO_REUSEPORT.
// Default 1 (single-process). NODE_WORKERS=0 means NumCPU.
let workers = parseInt(process.env.NODE_WORKERS || "1", 10);
if (!Number.isFinite(workers) || workers < 0) workers = 1;
if (workers === 0) workers = os.cpus().length;

if (workers > 1 && cluster.isPrimary) {
  console.log(`uWebSockets.js spawning ${workers} workers on port ${port}`);
  for (let i = 0; i < workers; i++) cluster.fork();
  cluster.on("exit", (worker, code, sig) => {
    console.error(`worker ${worker.process.pid} died: ${sig || code}`);
  });
  return;
}

// /db uses better-sqlite3 if available; falls back to a stub when missing
// so the rest of the benchmarks still run.
let dbStmt = null;
try {
  const Database = require("better-sqlite3");
  const dbPath = process.env.BENCH_DB || "/tmp/uwsbench/sample.db";
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.exec(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT
  )`);
  const row = db.prepare("SELECT COUNT(*) AS c FROM users").get();
  if (row.c < 1000) {
    const ins = db.prepare(
      "INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)"
    );
    const tx = db.transaction(() => {
      for (let i = 1; i <= 1000; i++) {
        ins.run(i, `User ${i}`, `user${i}@example.com`, "user");
      }
    });
    tx();
  }
  dbStmt = db.prepare("SELECT name, email, role FROM users WHERE id = ?");
} catch (e) {
  console.warn(`/db disabled: better-sqlite3 not installed (${e.code || e.message})`);
}

uWS
  .App()
  .get("/plain", (res) => {
    res
      .writeStatus("200 OK")
      .writeHeader("Content-Type", "text/plain; charset=utf-8")
      .end("hello world\n");
  })
  .get("/hello", (res) => {
    res
      .writeStatus("200 OK")
      .writeHeader("Content-Type", "text/plain; charset=utf-8")
      .end("hello world\n");
  })
  .get("/json", (res) => {
    res
      .writeStatus("200 OK")
      .writeHeader("Content-Type", "application/json")
      .end('{"message":"hello world","ok":true}\n');
  })
  .get("/hello/:name", (res, req) => {
    res
      .writeStatus("200 OK")
      .writeHeader("Content-Type", "text/plain; charset=utf-8")
      .end(`hello ${req.getParameter(0)}\n`);
  })
  .get("/db", (res) => {
    if (!dbStmt) {
      res.writeStatus("500 Internal Server Error").end("db not available\n");
      return;
    }
    const id = 1 + Math.floor(Math.random() * 1000);
    const row = dbStmt.get(id);
    res
      .writeStatus("200 OK")
      .writeHeader("Content-Type", "application/json")
      .end(
        `{"id":${id},"name":${JSON.stringify(row.name)},"email":${JSON.stringify(
          row.email
        )},"role":${JSON.stringify(row.role)}}\n`
      );
  })
  .get("/sleep", (res) => {
    res.onAborted(() => {
      res.aborted = true;
    });
    setTimeout(() => {
      if (res.aborted) return;
      res.cork(() => {
        res
          .writeStatus("200 OK")
          .writeHeader("Content-Type", "text/plain; charset=utf-8")
          .end("slept\n");
      });
    }, 2);
  })
  // POST /echo: read the body, write it back unchanged.
  // uWS streams body chunks via onData; accumulate then reply on isLast.
  .post("/echo", (res) => {
    res.onAborted(() => {
      res.aborted = true;
    });
    let buf = Buffer.alloc(0);
    res.onData((chunk, isLast) => {
      buf = Buffer.concat([buf, Buffer.from(chunk)]);
      if (isLast && !res.aborted) {
        res.cork(() => {
          res
            .writeStatus("200 OK")
            .writeHeader("Content-Type", "application/json")
            .end(buf);
        });
      }
    });
  })
  .listen(port, workers > 1 ? uWS.LIBUS_LISTEN_EXCLUSIVE_PORT : 0, (token) => {
    if (!token) {
      console.error(`failed to listen on :${port}`);
      process.exit(1);
    }

    const tag = workers > 1 ? ` (worker ${process.pid})` : "";
    console.log(`uWebSockets.js listening on http://localhost:${port}${tag}`);
  });
