# Stream and SSE Backpressure Benchmark

This benchmark is an opt-in Go benchmark for the native `Response.SSE` path,
which delegates to `Response.Stream` and uses `Response.AwaitDrain` for
backpressure. It does not require external services.

Run from the repository root:

```sh
go test -tags gogo -run '^$' -bench '^BenchmarkSSEBackpressureMemory$' -benchmem -benchtime=5x -count=1 .
```

The benchmark opens a local no-read TCP client, writes SSE frames until
`Response.BufferedAmount` crosses a 4 MiB trigger, then calls `AwaitDrain(0)`
and closes the client. It reports:

- `peak_buffered_B`: largest sampled `Response.BufferedAmount`.
- `trigger_B`: backlog trigger before calling `AwaitDrain`.
- `await_below_B`: `AwaitDrain` threshold.
- `aborted/op`: share of iterations that returned `ErrStreamAborted`.
- `drained/op`: share of iterations that drained before the abort was observed.
- `heap_live_after_B`: live Go heap after the benchmark and a forced GC.

Raw result from 2026-06-08 on darwin/arm64, Go 1.26.3:

```text
goos: darwin
goarch: arm64
pkg: github.com/Snocko-main/gogo
cpu: Apple M3
BenchmarkSSEBackpressureMemory-8   	       5	   5344667 ns/op	         0 aborted/op	         0 await_below_B	         1.000 drained/op	    371344 heap_live_after_B	   4289875 peak_buffered_B	   4194304 trigger_B	18686459 B/op	    1173 allocs/op
PASS
ok  	github.com/Snocko-main/gogo	0.499s
```

On this local loopback run, Darwin drained the queued SSE bytes to the kernel
before the client close surfaced as `ErrStreamAborted`, so `drained/op` is 1.000
and `aborted/op` is 0. A slower client or smaller socket buffers may flip that
outcome while keeping `peak_buffered_B` comparable to the trigger.
