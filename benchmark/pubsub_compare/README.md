# pub/sub compare — gogo vs uWebSockets.js

Side-by-side WebSocket pub/sub benchmark. Both servers expose the
same surface (`/ws` subscribes to `bench/topic`, `/publish?n=N`
broadcasts N messages, `/publishbatch?n=N` likewise, `/stat`
returns RSS + conn count). A single Go load generator drives them
over `WebSocket` + plain HTTP and prints throughput, RSS, and
per-connection memory.

## Files

- `main.go` — gogo server (build with the `gogo` tag)
- `uws_server.js` — uWebSockets.js server
- `loadgen/` — Go load generator

## Run

```
# 1. Install uWS.js (one-time)
mkdir -p /tmp/wsbench && cd /tmp/wsbench && npm init -y && \
  npm install uWebSockets.js@uNetworking/uWebSockets.js#v20.51.0

# 2. Build both servers + loadgen
go build -tags gogo -o /tmp/wsbench/gogo_server ./benchmark/pubsub_compare/
go build           -o /tmp/wsbench/loadgen     ./benchmark/pubsub_compare/loadgen/

# 3. gogo throughput run
/tmp/wsbench/gogo_server &
sleep 1
/tmp/wsbench/loadgen -target 127.0.0.1:7100 -conns 1000 -duration 5s \
  -batch 100 -mode publishbatch -label "gogo"
kill %1; wait

# 4. uWS.js throughput run
PORT=7000 node ./benchmark/pubsub_compare/uws_server.js &
sleep 1
/tmp/wsbench/loadgen -target 127.0.0.1:7000 -conns 1000 -duration 5s \
  -batch 100 -mode publishbatch -label "uWS.js"
kill %1; wait
```

## Reference numbers (4-vCPU VM, ulimit -n = 4096, single-machine setup)

These are the numbers from one run on the dev VM. They are
indicative, not authoritative — rerun on your own hardware before
quoting them.

| Metric                                  | uWebSockets.js v20.51 | gogo (this repo) |
| --------------------------------------- | --------------------- | ---------------- |
| Baseline RSS (no conns)                 | 48 MB                 | **7.5 MB**       |
| RSS @ 1000 idle conns                   | 48 MB                 | **8.2 MB**       |
| RSS @ 3000 idle conns                   | 57 MB                 | **9.3 MB**       |
| Mem / conn (idle, N=3000)               | 830 bytes             | 262 bytes        |
| Single-publish throughput, 1000 subs    | 659k msgs/s           | **715k msgs/s**  |
| Batch=100 throughput, 1000 subs         | 280k msgs/s           | **5.1M msgs/s**  |
| Batch HTTP-call rate, 4 workers         | 3 calls/s             | **51 calls/s**   |

### Reading the batch result

gogo's `App.PublishBatch` defers onto the loop and returns from the
HTTP handler immediately, so the next /publishbatch call can be in
flight while the previous batch's WS sends drain. uWS.js's
`app.publish` runs synchronously on the loop, so the HTTP handler
holds the response until 100 publishes × 1000 subscribers = 100 k
frame writes are queued. The HTTP rate collapses (3 calls/s) and so
does the effective broadcast throughput.

This is what the PublishBatch API is for — taking a fan-out off the
loop thread's critical path. The single-publish row above shows
that when each request is just one publish, gogo and uWS.js are
within 10 % of each other, since both are bound by the same uWS
TopicTree fan-out cost.

### Caveats

- 4 vCPU, 15 GiB RAM. Plenty of headroom; not a CPU-saturation
  contest. Numbers will scale with cores up to uWS's loop-thread
  limit (one loop = one core in both binding flavours).
- `ulimit -n = 4096`. Caps the loadgen at ~3000 conns (plus
  internal fds). Real production servers go far higher with `prlimit`.
- The loadgen and both servers run on the same machine — no
  network latency, but they share CPU. Inter-process publish under
  realistic NIC latency would shift the absolute numbers, not the
  relative ones.
- RSS deltas at small N are page-granular (4 KiB) noise. Trust the
  per-conn figure at N=3000, not N=500.

### Load-gen flags

```
-target host:port        which server to hit
-conns N                 open N WebSocket subscribers
-duration 5s             publish phase duration
-batch K                 messages per /publish or /publishbatch call
-mode publish|publishbatch|idle
-rate K                  publish calls/sec (0 = saturate)
-label "name"            shown in the summary line
```
