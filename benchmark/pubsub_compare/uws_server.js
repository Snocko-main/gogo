// uWS.js server — minimal pub/sub surface to compare against gogo.
//
// Topology:
// - GET /ws upgrades to a WebSocket
// - Every connection subscribes to "bench/topic" in Open
// - GET /publish?n=N broadcasts N messages via app.publish (from
//   inside an HTTP handler; uWS.js is single-threaded so this is
//   loop-thread context)
// - GET /publishbatch?n=N does the same via a tight loop (uWS.js has
//   no batch API — it's already single-threaded, so each publish
//   already runs on the loop with no defer cost)
// - GET /stat returns process.memoryUsage() as JSON for the harness

const uWS = require('uWebSockets.js');

const PORT = parseInt(process.env.PORT || '7000', 10);
const PAYLOAD = Buffer.alloc(128, 0x41);

let connCount = 0;

const app = uWS.App()
  .ws('/ws', {
    maxPayloadLength: 16 * 1024,
    idleTimeout: 120,
    maxBackpressure: 64 * 1024 * 1024,
    open: (ws) => {
      ws.subscribe('bench/topic');
      connCount++;
      if (process.env.LOG_EVT) console.log(Date.now(), 'OPEN  count=', connCount);
    },
    close: (_ws, code, _msg) => {
      connCount--;
      if (process.env.LOG_EVT) console.log(Date.now(), 'CLOSE code=', code, 'count=', connCount);
    },
  })
  .get('/publish', (res, req) => {
    const n = parseInt(req.getQuery('n') || '1', 10);
    for (let i = 0; i < n; i++) {
      app.publish('bench/topic', PAYLOAD, false, false);
    }
    res.end(`${n}`);
  })
  // uWS.js has no batch API — its publish is already loop-thread
  // because the whole runtime is single-threaded. The best-case
  // fan-out is a tight loop of app.publish, which is what we do
  // here. /publishbatch exists so the load-gen can use the same
  // endpoint path against uWS.js and gogo for a side-by-side run.
  .get('/publishbatch', (res, req) => {
    const n = parseInt(req.getQuery('n') || '1', 10);
    for (let i = 0; i < n; i++) {
      app.publish('bench/topic', PAYLOAD, false, false);
    }
    res.end(`${n}`);
  })
  .get('/stat', (res) => {
    const m = process.memoryUsage();
    res.writeHeader('content-type', 'application/json')
      .end(JSON.stringify({ rss: m.rss, heapUsed: m.heapUsed, conns: connCount }));
  })
  .listen(PORT, (token) => {
    if (token) {
      console.log(`uWS.js listening on :${PORT}`);
    } else {
      console.error('listen failed');
      process.exit(1);
    }
  });
