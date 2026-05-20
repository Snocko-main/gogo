// Benchmark server for the T-1 / T-2 / T-4 perf-quickwins PR.
//
// Three routes are registered, each isolating one of the optimized
// paths:
//
//	/plain  — baseline. Sync route, no middleware, no buffered
//	          headers. Catches regressions in the framework's
//	          unchanged hot path; same number should appear before
//	          and after the optimization.
//
//	/cors   — sync route with the CORS middleware enabled (2
//	          configured origins, including a wildcard pattern, plus
//	          ExposeHeaders so the request emits 3 buffered headers
//	          per response). Exercises T-1 (batch flushPendingHeaders
//	          for N=3) and T-2 (pre-lowercased pattern match)
//	          together.
//
//	/heavy  — sync route with a tiny custom middleware that stamps
//	          8 response headers. The most cgo-heavy route in the
//	          test set — the optimization saves N-1 = 7 cgo
//	          crossings per response.
//
// Run with:
//
//	go run -tags gogo ./benchmark/perfquickwins :8080
//
// Multi-core (matches fiber's default GOMAXPROCS-parallel model):
//
//	GOGO_CORES=4 go run -tags gogo ./benchmark/perfquickwins :8080
//
// Drive via wrk:
//
//	wrk -t 4 -c 64 -d 30s --latency http://127.0.0.1:8080/heavy/x
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"

	gogo "uwebsockets-go/gogo"
	"uwebsockets-go/gogo/middleware"
)

func main() {
	addr := ":8080"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	port := 0
	if _, err := fmt.Sscanf(addr, ":%d", &port); err != nil || port == 0 {
		log.Fatalf("usage: %s :PORT", os.Args[0])
	}

	cores := 1
	if env := os.Getenv("GOGO_CORES"); env != "" {
		if n, err := strconv.Atoi(env); err == nil && n > 0 {
			cores = n
		}
	}

	// Silence any panic noise from the framework so a misconfigured
	// wrk run doesn't spam stdout.
	gogo.SetPanicHandler(func(r any) {})

	// Shared state created ONCE — every worker's setup closure
	// captures the same metrics instance, so counters aggregate
	// across loops instead of each worker maintaining its own
	// disjoint copy.
	var sharedMetrics *middleware.Metrics
	if os.Getenv("GOGO_METRICS") == "1" {
		sharedMetrics = middleware.NewMetrics()
	}

	setupFn := func(app *gogo.App) {
		setupShared(app, sharedMetrics)
	}

	if cores > 1 {
		handle, err := gogo.RunMultiCore(cores, port, setupFn)
		if err != nil {
			log.Fatalf("RunMultiCore: %v", err)
		}
		fmt.Fprintf(io.Discard, "gogo listening on %s (%d cores)\n", addr, cores)
		handle.Wait()
		return
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	app, err := gogo.NewApp()
	if err != nil {
		log.Fatalf("NewApp: %v", err)
	}
	setupFn(app)
	if !app.Listen(port) {
		log.Fatalf("Listen %s failed", addr)
	}
	fmt.Fprintf(io.Discard, "gogo listening on %s\n", addr)
	app.Run()
}

func setupShared(app *gogo.App, metrics *middleware.Metrics) {
	// When GOGO_METRICS=1 is set, the caller threads the same
	// *Metrics instance into every worker — multi-core RunMultiCore
	// then aggregates counters across workers instead of each
	// worker maintaining its own disjoint copy.
	if metrics != nil {
		app.Use(metrics.Middleware())
		app.Get("/metrics", metrics.Handler())
	}

	// /plain — baseline, no middleware, no buffered headers.
	app.Get("/plain", func(res *gogo.Response, req *gogo.Request) {
		res.Send(200, "text/plain", "ok")
	})

	// /cors-async/x — async CORS route. Exercises T-5's actual
	// payoff: the shared-dispatch snapshot now carries headers
	// at 8 KB (matching the sync scratch buffer), so middleware
	// that reads Origin / Authorization / etc. on async routes
	// has zero cgo on the lookup path.
	app.Use("/cors-async", middleware.CORS(middleware.CORSOptions{
		AllowOrigins:  []string{"https://app.example.com", "https://*.tenant.example.com"},
		ExposeHeaders: []string{"X-Request-Id", "X-Rate-Limit"},
	}))
	app.GetAsync("/cors-async/x", func(res *gogo.Response, req *gogo.Request) {
		res.Send(200, "text/plain", "ok")
	})

	// /headers/x — handler reads 5 typical request headers in
	// sequence (User-Agent, Accept-Encoding, Cookie, Authorization,
	// X-Forwarded-For). Pre-D-1 each lookup costs one cgo crossing
	// (~80 ns × 5 = ~400 ns/req); post-D-1 the dispatcher hands
	// Go a single packed blob and the lookups stay Go-only.
	// The handler echoes back the User-Agent so the bench client
	// can verify the read actually happened.
	app.Get("/headers/x", func(res *gogo.Response, req *gogo.Request) {
		ua := req.Header("user-agent")
		_ = req.Header("accept-encoding")
		_ = req.Header("cookie")
		_ = req.Header("authorization")
		_ = req.Header("x-forwarded-for")
		res.Send(200, "text/plain", ua)
	})

	// /cors — CORS middleware scoped to the /cors prefix via the
	// app-level Use so we can pass the bundled mwhint.Hinted value
	// (Group.Use is typed to gogo.Middleware only).
	app.Use("/cors", middleware.CORS(middleware.CORSOptions{
		AllowOrigins:  []string{"https://app.example.com", "https://*.tenant.example.com"},
		ExposeHeaders: []string{"X-Request-Id", "X-Rate-Limit"},
	}))
	app.Get("/cors/x", func(res *gogo.Response, req *gogo.Request) {
		res.Send(200, "text/plain", "ok")
	})

	// /heavy — 8 stamped headers. Pure stress for the
	// flushPendingHeaders batching path. Scoped via app.Use("/heavy",
	// ...) so a separate route group isn't required.
	app.Use("/heavy", func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			res.Header("X-Service", "gogo")
			res.Header("X-Region", "us-west-2")
			res.Header("X-Build", "abc123")
			res.Header("X-Trace-Id", "trace-1")
			res.Header("Cache-Control", "no-store")
			res.Header("Vary", "Accept-Encoding")
			res.Header("X-Frame-Options", "DENY")
			res.Header("X-Content-Type-Options", "nosniff")
			next(res, req)
		}
	})
	app.Get("/heavy/x", func(res *gogo.Response, req *gogo.Request) {
		res.Send(200, "text/plain", "ok")
	})
}
