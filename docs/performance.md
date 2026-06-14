# Performance Benchmarks

`ROAD_TO_V1.md` tracks performance work. This page keeps the HTTP benchmark
commands, route coverage, raw result location, and cgo hot-path budget in one
place so benchmark numbers can be reproduced before they are summarized in the
README.

For the v1 reproducibility gate, see
[`docs/v1-benchmark-baseline.md`](v1-benchmark-baseline.md). That page records
the exact baseline and smoke commands, environment notes, pass/fail smoke
evidence from 2026-06-10, and how to capture fresh raw logs.

## v0.7 HTTP Baseline

Run the v0.7 route set against the local Go baselines:

```sh
export CGO_ENABLED=1
RESULTS_DIR="benchmark/results/v07-http-$(date -u +%Y%m%dT%H%M%SZ)" \
  BENCH_ROUTE_SET=v07-http \
  DURATION=10 \
  THREADS="1 2 4 8" \
  CONN=500 \
  ./scripts/bench_wrk.sh
```

The `v07-http` route set runs `gogo`, `fiber`, and `nethttp` only. That keeps
the new middleware route comparable without changing the Node, Bun, or Actix
benchmark servers.

For gogo, `GOGO_WORKERS` can override the shared-dispatch worker count. The
default value `0` keeps gogo's runtime default of `ceil(1.5 × loop count)` —
it scales with the number of uWS loops, not `NumCPU`, so a single-loop run on
a many-core box does not over-subscribe the loop thread. Raise it for
heavily blocking async work.

Covered GET workloads:

| workload | purpose |
|---|---|
| `/plain` | minimal dynamic text response |
| `/hello/inon` | parameter extraction |
| `/json` | fixed JSON response |
| `/middleware` | cheap middleware that stamps benchmark headers |
| `/async` | deterministic SQLite lookup from an async-capable route |

Raw `wrk` output is written to
`$RESULTS_DIR/wrk-<framework>-<mode>.log`. Use a fresh `RESULTS_DIR` for every
run; checked-in snapshot logs live under `benchmark/results/wrk-*.log`.

## Legacy Six-Framework Snapshot

The README tables and SVG snapshot are tied to the checked-in raw logs from
the legacy route set:

```sh
export CGO_ENABLED=1
cargo build --release --manifest-path benchmark/actix/Cargo.toml
BENCH_ROUTE_SET=legacy ./scripts/bench_wrk.sh
```

The legacy route set covers `/hello`, `/hello/inon`, `/db`, `POST /echo`, and
`POST /query` across gogo, Fiber, net/http, uWebSockets.js, Bun/Elysia, and
Actix.

## cgo Crossing Budget

The HTTP hot-path budget for gogo is:

| path shape | per-request cgo budget |
|---|---|
| static `Reply`, string, or byte routes without sync middleware | 0 callbacks/calls |
| dynamic sync routes such as `/plain`, `/json`, and `/hello/:name` | at most 1 C++ to Go handler callback plus 1 Go to C `Send` call |
| sync route with middleware-set headers such as `/middleware` | at most 1 C++ to Go handler callback plus 3 Go to C response calls: status, batched headers, end |
| shared async route such as `/async` | 0 framework callbacks/calls while no sync middleware matches and the response fits shared-send limits |

The `/async` workload intentionally runs a SQLite lookup inside the handler.
That database driver may use cgo internally; the budget above describes gogo's
HTTP dispatch/response path only, not application or driver work.

If a benchmark route changes those budgets, record why in the script or PR and
capture fresh raw results with the exact command used.

## Before and After Notes

This v0.7 baseline lane adds benchmark coverage and documentation only; it does
not change runtime HTTP request handling. No before/after throughput delta is
expected from this PR. Treat the first `BENCH_ROUTE_SET=v07-http` run as the new
baseline, then compare future performance patches by running the same command
with separate `RESULTS_DIR` values before and after the code change.
