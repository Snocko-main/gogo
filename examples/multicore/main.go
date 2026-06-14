// multicore demonstrates the full RunMultiCore production setup:
//
//   - N independent uWS event loops, one per vCPU (NumCPU by default),
//     with accepted sockets round-robined across loops
//   - Shared state (here: a counter; in real apps a *sql.DB pool)
//     created ONCE before RunMultiCore and captured into the handlers
//     so per-worker initialization stays cheap
//   - Per-worker request counters aggregated through a /metrics
//     endpoint formatted as Prometheus text exposition
//   - Signal-driven immediate group shutdown via SIGINT / SIGTERM
//
// Run with:
//
//	CGO_ENABLED=1 go run -tags gogo ./examples/multicore
//	wrk -t 4 -c 500 -d 10s http://localhost:3000/plain
//	curl http://localhost:3000/metrics
//
// Tuning knobs
// ============
//
// GOMAXPROCS — defaults to NumCPU. For RunMultiCore workloads pin it
// to the same number you pass as `n` so the runtime scheduler has
// exactly as many Ps as event loops; pinning beyond that wastes
// scheduling cycles, pinning below it starves loops.
//
// SetWorkerCount — controls the GetAsync worker-goroutine pool size.
// Defaults to ceil(1.5 × loop count): it scales with the number of
// loops, not NumCPU, so it doesn't over-subscribe the loop threads.
// Trust the default for short async handlers; raise it for IO-bound
// handlers that keep many requests blocked at once.
//
// Per-worker resources — wrap shared resources in plain Go state
// captured into setup. The example below uses an atomic.Int64 for
// counts; a production app would create a *sql.DB once at startup
// and pass it into every handler closure.
//
// Config boundary — RunMultiCore currently creates each worker App
// with the zero-value gogo.Config. App-scoped Config fields are not
// configurable through this helper; use process-wide knobs before
// RunMultiCore and per-route/per-middleware options inside setup.
//
// Shutdown behavior — MultiCoreHandle.Shutdown calls each worker's
// immediate Shutdown. It stops the group quickly, but it is not the
// same as App.ShutdownContext's single-app graceful drain. See
// examples/graceful when in-flight requests must finish before exit.
//
// Pinning to CPUs — gogo doesn't pin loops to specific cores
// today. With a `RunMultiCore(N=NumCPU)` config the kernel typically
// keeps each loop on its initial CPU; if you need stricter pinning
// run the server under `taskset -c 0-(N-1)` or wrap the
// LockOSThread inside a sched_setaffinity call (Linux only).

package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	gogo "github.com/Snocko-main/gogo"
)

func main() {
	cores := runtime.NumCPU()
	if env := os.Getenv("GOGO_CORES"); env != "" {
		if n, err := fmt.Sscanf(env, "%d", &cores); err != nil || n != 1 || cores <= 0 {
			log.Fatalf("GOGO_CORES must be a positive integer, got %q", env)
		}
	}
	runtime.GOMAXPROCS(cores)

	// Shared state lives outside setup so every worker captures the
	// same pointers. Counters are atomic so the loop goroutines can
	// stamp them concurrently without synchronization on the hot
	// path.
	stats := &stats{startedAt: time.Now()}

	startedAt := time.Now()
	handle, err := gogo.RunMultiCore(cores, 3000, func(app *gogo.App) {
		registerRoutes(app, stats)
	})
	if err != nil {
		log.Fatalf("RunMultiCore: %v", err)
	}

	log.Printf("gogo~ multicore listening on :3000 (%d cores, GOMAXPROCS=%d, started in %s)",
		cores, runtime.GOMAXPROCS(0), time.Since(startedAt))

	// Signal-driven immediate shutdown: SIGINT (Ctrl+C) or SIGTERM
	// (container stop) triggers Shutdown on every worker, then Wait
	// blocks until all loops have exited. MultiCoreHandle does not
	// currently expose a graceful drain API; active connections may be
	// closed before in-flight responses complete.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("shutdown signal received — stopping workers")
	handle.Shutdown()
	handle.Wait()
	log.Printf("stopped after %s, served %d requests",
		time.Since(startedAt), stats.totalRequests.Load())
}

// stats holds the per-process metrics surfaced by /metrics.
// Counters are split per route so a Prometheus scraper can graph
// the breakdown; bytesOut is summed across all responses.
type stats struct {
	startedAt time.Time

	totalRequests atomic.Int64

	plainRequests atomic.Int64
	jsonRequests  atomic.Int64
	bytesOut      atomic.Int64
}

func registerRoutes(app *gogo.App, s *stats) {
	// /plain — bare baseline, perfect for wrk throughput tests.
	app.Get("/plain", func(res *gogo.Response, req *gogo.Request) {
		s.totalRequests.Add(1)
		s.plainRequests.Add(1)
		s.bytesOut.Add(2)
		res.Send(200, "text/plain", "ok")
	})

	// /json — small JSON response. Demonstrates Content-Type
	// negotiation without middleware.
	app.Get("/json", func(res *gogo.Response, req *gogo.Request) {
		s.totalRequests.Add(1)
		s.jsonRequests.Add(1)
		const body = `{"ok":true,"msg":"hello"}`
		s.bytesOut.Add(int64(len(body)))
		res.Send(200, "application/json", body)
	})

	// /metrics — Prometheus text exposition format. Operators can
	// scrape this directly; no extra dependencies. Uses the stats
	// snapshot captured at the moment of the request so the numbers
	// are coherent across the four lines.
	app.Get("/metrics", func(res *gogo.Response, req *gogo.Request) {
		total := s.totalRequests.Load()
		plain := s.plainRequests.Load()
		jsonReq := s.jsonRequests.Load()
		bytes := s.bytesOut.Load()
		uptime := time.Since(s.startedAt).Seconds()

		body := fmt.Sprintf(`# HELP gogo_requests_total Total HTTP requests served.
# TYPE gogo_requests_total counter
gogo_requests_total %d
# HELP gogo_requests_route_total Total HTTP requests per route.
# TYPE gogo_requests_route_total counter
gogo_requests_route_total{route="/plain"} %d
gogo_requests_route_total{route="/json"} %d
# HELP gogo_bytes_out_total Bytes written to clients across all responses.
# TYPE gogo_bytes_out_total counter
gogo_bytes_out_total %d
# HELP gogo_uptime_seconds Seconds since the server started.
# TYPE gogo_uptime_seconds gauge
gogo_uptime_seconds %.3f
`, total, plain, jsonReq, bytes, uptime)
		res.Send(200, "text/plain; version=0.0.4", body)
	})
}
