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
// Drive via wrk:
//
//	wrk -t 4 -c 64 -d 30s --latency http://127.0.0.1:8080/heavy
//
// For the /cors-match path, send the Origin header:
//
//	wrk -t 4 -c 64 -d 30s --latency \
//	    -H 'Origin: https://admin.example.com' \
//	    http://127.0.0.1:8080/cors
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"

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

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	app, err := gogo.NewApp()
	if err != nil {
		log.Fatalf("NewApp: %v", err)
	}

	// /plain — baseline, no middleware, no buffered headers.
	app.Get("/plain", func(res *gogo.Response, req *gogo.Request) {
		res.Send(200, "text/plain", "ok")
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

	// Silence the framework's default panic / 500 path so a
	// misconfigured wrk run doesn't spam stdout.
	gogo.SetPanicHandler(func(r any) {})

	if !app.Listen(port) {
		log.Fatalf("Listen %s failed", addr)
	}
	fmt.Fprintf(io.Discard, "listening on %s\n", addr)
	app.Run()
}
