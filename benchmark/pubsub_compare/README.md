# WebSocket Pub/Sub Baselines

This directory contains the v0.7 WebSocket publish-batch baseline harness.
It compares the same subscriber fan-out path in three shapes:

| case | endpoint | shape |
|---|---|---|
| `publish-single` | `/publish?n=1` | one `App.Publish` per HTTP request |
| `publish-loop` | `/publish?n=$BATCH` | pre-batch shape: loop over `App.Publish` |
| `publishbatch` | `/publishbatch?n=$BATCH` | batch shape: one `App.PublishBatch` call |

The load generator opens WebSocket subscribers, reads every delivered frame,
drives the HTTP publish endpoint for a fixed duration, and prints throughput,
delivery ratio, RSS, and idle memory per connection.

## Run

From the repository root:

```sh
CONNS=1000 DURATION=5s BATCH=100 benchmark/pubsub_compare/run_baseline.sh
```

For a quick smoke run:

```sh
CONNS=50 DURATION=1s BATCH=10 benchmark/pubsub_compare/run_baseline.sh
```

Useful knobs:

| variable | default | meaning |
|---|---:|---|
| `PORT` | `17100` | first local gogo server port; later cases use the next ports |
| `CONNS` | `1000` | WebSocket subscribers to open per case |
| `DURATION` | `5s` | publish phase duration per case |
| `BATCH` | `100` | messages per `publish-loop` or `publishbatch` HTTP call |
| `BENCH_BIN_DIR` | `${TMPDIR:-/tmp}/gogo-pubsub-baseline` | temporary build/output directory |

The script refuses to run when the target port is already listening if `lsof`
is available. This avoids accidentally measuring a stale server process on
systems where the socket allows port reuse.

## Manual Commands

The script is only a convenience wrapper. These are the equivalent commands
for direct runs:

```sh
(cd benchmark && CGO_ENABLED=1 go build -tags gogo -o /tmp/gogo_pubsub_server ./pubsub_compare)
(cd benchmark && go build -o /tmp/pubsub_loadgen ./pubsub_compare/loadgen)

PORT=17100 /tmp/gogo_pubsub_server

/tmp/pubsub_loadgen -target 127.0.0.1:17100 -conns 1000 -duration 5s \
  -batch 1 -mode publish -label publish-single

/tmp/pubsub_loadgen -target 127.0.0.1:17100 -conns 1000 -duration 5s \
  -batch 100 -mode publish -label publish-loop-100

/tmp/pubsub_loadgen -target 127.0.0.1:17100 -conns 1000 -duration 5s \
  -batch 100 -mode publishbatch -label publishbatch-100
```

To compare with uWebSockets.js, install `uWebSockets.js` in a temporary npm
workspace and run `uws_server.js`, then point the same `pubsub_loadgen`
commands at that port. `uWS.js` has no publish-batch API, so its
`/publishbatch` endpoint is intentionally a tight `app.publish` loop.

## Baseline Notes

Treat these numbers as local baselines, not portable capacity claims. The
setup runs client and server on the same machine, so CPU sharing, file-descriptor
limits, and WebSocket scheduling dominate absolute throughput.

The before/after comparison for v0.7 is:

- before: `publish-loop`, which sends `$BATCH` messages with repeated
  `App.Publish` calls from the HTTP handler.
- after: `publishbatch`, which sends the same `$BATCH` messages through one
  `App.PublishBatch` call.

Keep raw run output small when committing updates. Prefer a short table or a
small excerpt with the exact command, date, host, Go version, connection count,
duration, and batch size.

### Local Smoke

Initial v0.7 baseline data was collected on 2026-06-08 on Darwin arm64
with Go 1.26.3:

```sh
PORT=17800 CONNS=1000 DURATION=5s BATCH=100 benchmark/pubsub_compare/run_baseline.sh
```

| case | publishes/sec | msgs delivered/sec | delivered/published | RSS baseline -> load |
|---|---:|---:|---:|---:|
| `publish-single` | 1,278 | 1,278,135 | 1.00 | 11.3 -> 12.5 MB |
| `publish-loop-100` | 62 | 6,233,491 | 1.00 | 11.3 -> 12.7 MB |
| `publishbatch-100` | 64 | 6,355,226 | 1.00 | 11.4 -> 17.5 MB |

On this same-machine Darwin smoke run, `PublishBatch` was slightly ahead of
the publish-loop shape. Treat that as the baseline to compare against, not as
a portable conclusion. Record fresh numbers here when this lane is rerun on
the target release machine.
