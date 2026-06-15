// multicore demonstrates the production multicore setup:
//
//   - Automatic uWS loop and async-worker sizing from GOMAXPROCS by
//     default, with explicit GOGO_CORES / GOGO_WORKERS overrides when
//     benchmarking a deployment
//   - Shared state (here: a counter; in real apps a *sql.DB pool)
//     created ONCE before gogo.Run and captured into the handlers so
//     per-worker initialization stays cheap
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
// GOMAXPROCS — the primary CPU budget. Set it with the standard Go
// environment variable or runtime.GOMAXPROCS before calling gogo.Run.
// gogo derives default loop and worker counts from that value.
//
// GOGO_CORES / GOGO_WORKERS — example-local overrides for tuning.
// Leave them unset first; set them only when comparing 1 / 2 / 4 loops
// or raising async workers for IO-bound handlers.
//
// Per-worker resources — wrap shared resources in plain Go state
// captured into setup. The example below uses an atomic.Int64 for
// counts; a production app would create a *sql.DB once at startup
// and pass it into every handler closure.
//
// Config boundary — gogo.RunWithOptions can apply one gogo.Config to
// every worker. Create shared resources outside setup; do not allocate
// a new DB pool per worker.
//
// Shutdown behavior — MultiCoreHandle.Shutdown calls each worker's
// immediate Shutdown. It stops the group quickly, but it is not the
// same as App.ShutdownContext's single-app graceful drain. See
// examples/graceful when in-flight requests must finish before exit.
//
// Pinning to CPUs — gogo doesn't pin loops to specific cores today.
// If you need stricter pinning, run the server under
// `taskset -c 0-(N-1)` or wrap LockOSThread inside a
// sched_setaffinity call (Linux only).

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
	opts := gogo.RunOptions{}
	if env := os.Getenv("GOGO_CORES"); env != "" {
		opts.Cores = mustPositiveInt("GOGO_CORES", env)
	}
	if env := os.Getenv("GOGO_WORKERS"); env != "" {
		opts.Workers = mustPositiveInt("GOGO_WORKERS", env)
	}

	// Shared state lives outside setup so every worker captures the
	// same pointers. Counters are atomic so the loop goroutines can
	// stamp them concurrently without synchronization on the hot
	// path.
	stats := &stats{startedAt: time.Now()}

	startedAt := time.Now()
	handle, err := gogo.RunWithOptions(3000, func(app *gogo.App) {
		registerRoutes(app, stats)
	}, opts)
	if err != nil {
		log.Fatalf("gogo.RunWithOptions: %v", err)
	}

	log.Printf("gogo~ multicore listening on :3000 (GOMAXPROCS=%d, cores=%s, workers=%s, started in %s)",
		runtime.GOMAXPROCS(0), optionLabel(opts.Cores), optionLabel(opts.Workers), time.Since(startedAt))

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

func mustPositiveInt(name, raw string) int {
	var value int
	if n, err := fmt.Sscanf(raw, "%d", &value); err != nil || n != 1 || value <= 0 {
		log.Fatalf("%s must be a positive integer, got %q", name, raw)
	}
	return value
}

func optionLabel(value int) string {
	if value == 0 {
		return "auto"
	}
	return fmt.Sprintf("%d", value)
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
