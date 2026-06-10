# v1 Benchmark Baseline

This page is the v1 evidence target for the roadmap criterion "Benchmark
baseline is reproducible." It documents the benchmark commands, environment
notes, result locations, and short local smoke commands that prove the HTTP
baseline harness can be rerun from a clean checkout.

The short smoke commands below are pass/fail checks for the benchmark harness.
They intentionally do not publish requests/sec values because one-second
developer-laptop output is noisy enough to invert framework ordering. For
performance claims, rerun the full command in a quiet release environment and
keep the raw logs.

## Baseline Command

Use this command for the v1 HTTP baseline. It runs the v0.7 HTTP route set
against the Go-local baselines: gogo, Fiber, and net/http.

```sh
export CGO_ENABLED=1
RESULTS_DIR="benchmark/results/v1-http-$(date -u +%Y%m%dT%H%M%SZ)" \
  BENCH_ROUTE_SET=v07-http \
  DURATION=10 \
  THREADS="1 2 4 8" \
  CONN=500 \
  ./scripts/bench_wrk.sh
```

The harness writes raw `wrk` output to:

```text
$RESULTS_DIR/wrk-gogo-single.log
$RESULTS_DIR/wrk-gogo-multi.log
$RESULTS_DIR/wrk-fiber-single.log
$RESULTS_DIR/wrk-fiber-multi.log
$RESULTS_DIR/wrk-nethttp-single.log
$RESULTS_DIR/wrk-nethttp-multi.log
```

Use a fresh timestamped `RESULTS_DIR` for each run. Do not overwrite previous
logs when comparing before and after a performance change.

## Routes And Knobs

`BENCH_ROUTE_SET=v07-http` covers these GET routes:

| route | purpose |
|---|---|
| `/plain` | minimal dynamic text response |
| `/hello/inon` | route parameter extraction |
| `/json` | fixed JSON response |
| `/middleware` | cheap middleware that stamps benchmark headers |
| `/async` | deterministic SQLite lookup from an async-capable route |

Useful knobs:

| variable | default in full command | meaning |
|---|---:|---|
| `RESULTS_DIR` | timestamped directory | raw `wrk` log directory |
| `BENCH_ROUTE_SET` | `v07-http` | route/framework matrix |
| `DURATION` | `10` | seconds per `wrk` measurement |
| `THREADS` | `1 2 4 8` | thread counts passed to `wrk -t` |
| `CONN` | `500` | connections passed to `wrk -c` |
| `MODES` | `single multi` | single worker and multi-worker server modes |
| `MULTI_WORKERS` | min(`NumCPU`, 4) | workers/processes for multi mode |
| `GOGO_WORKERS` | `0` | gogo shared-dispatch workers; `0` means runtime default |
| `WARMUP` | `2` | seconds of warmup before each measured route |

## Short Smoke Evidence

The following check ran from branch `doc/v1-benchmark-baseline` at commit
`8c36f0b21ac22569879072200184839c1bb30adf`.

Environment:

| field | value |
|---|---|
| date | 2026-06-10T11:27:00Z |
| OS | Darwin 25.5.0 arm64 |
| CPU | Apple M3 |
| CPUs | 8 |
| memory | 16 GB |
| Go | go1.26.3 darwin/arm64 |
| wrk | 4.2.0 kqueue |

Command:

```sh
RESULTS_DIR="benchmark/results/v1-baseline-smoke-20260610T112700Z" \
  BENCH_ROUTE_SET=v07-http \
  FRAMEWORKS="gogo fiber nethttp" \
  MODES=single \
  THREADS=1 \
  CONN=50 \
  DURATION=1 \
  WARMUP=0 \
  CGO_ENABLED=1 \
  ./scripts/bench_wrk.sh
```

During this run, raw logs were written locally to:

```text
benchmark/results/v1-baseline-smoke-20260610T112700Z/wrk-gogo-single.log
benchmark/results/v1-baseline-smoke-20260610T112700Z/wrk-fiber-single.log
benchmark/results/v1-baseline-smoke-20260610T112700Z/wrk-nethttp-single.log
```

The smoke passed for gogo, Fiber, and net/http. The local logs are not
committed, and their requests/sec values are intentionally omitted because a
one-second smoke run is not a valid relative-performance comparison. Future
release runs should use a fresh timestamped directory.

## Rerun Check

A second narrow rerun checked that the same harness can be invoked again with a
new result directory:

```sh
RESULTS_DIR="benchmark/results/v1-baseline-smoke-rerun-20260610T112900Z" \
  BENCH_ROUTE_SET=v07-http \
  FRAMEWORKS="gogo" \
  MODES=single \
  THREADS=1 \
  CONN=50 \
  DURATION=1 \
  WARMUP=0 \
  CGO_ENABLED=1 \
  ./scripts/bench_wrk.sh
```

During this run, raw logs were written locally to:

```text
benchmark/results/v1-baseline-smoke-rerun-20260610T112900Z/wrk-gogo-single.log
```

The rerun passed. It intentionally stays small and omits throughput numbers for
the same reason: developer-laptop load and warmup effects can dominate tiny
benchmark windows. Use the full baseline command for comparisons.

## How To Refresh The Evidence

1. Start from a clean checkout of the commit being evaluated.
2. Record `git rev-parse HEAD`, `go version`, the first line from `wrk -v`,
   `uname -a`, CPU count, and memory.
3. Run the full baseline command with a fresh `RESULTS_DIR`.
4. Keep the raw `wrk` logs in that result directory for review.
5. For full baseline runs only, summarize requests/sec and tail latency in this
   page or in the release PR.
6. Compare future performance work against a separate before/after result
   directory created with the same command and environment.

## Current Limitations

- The committed smoke evidence is a pass/fail reproducibility check, not a
  release-capacity baseline or a framework ranking.
- The 2026-06-10 smoke ran on a laptop that was not isolated from background
  work.
- Only the single-worker Go-local matrix was smoked. The full command still
  needs to be run on the release machine before using numbers in release notes
  or README performance claims.
