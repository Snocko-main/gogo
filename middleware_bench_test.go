//go:build cgo && gogo

package gogo_test

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

// runHTTPBench drives a single HTTP route at sustained load via a
// keep-alive client. Reports ns/op based on wall-clock per request.
// Used to compare middleware-chain overhead across configurations,
// not to measure absolute peak throughput (which would need parallel
// loaders and OS tuning).
func runHTTPBench(b *testing.B, configure func(app *gogo.App), path string) {
	b.Helper()
	port, teardown := startApp(b, configure)
	defer teardown()

	tr := &http.Transport{
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	url := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)

	// Warm up the keep-alive connection and JIT path.
	for i := 0; i < 100; i++ {
		resp, err := client.Get(url)
		if err != nil {
			b.Fatalf("warmup: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(url)
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// runHTTPBenchWithRequest is the runHTTPBench twin that builds each
// request via http.NewRequest so the caller can stamp headers
// (Origin, Authorization, etc.) — used by benches that exercise
// header-conditional code paths like CORS origin matching.
func runHTTPBenchWithRequest(b *testing.B, configure func(app *gogo.App), path string, stamp func(req *http.Request)) {
	b.Helper()
	port, teardown := startApp(b, configure)
	defer teardown()

	tr := &http.Transport{
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	url := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)

	makeReq := func() *http.Request {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			b.Fatalf("new request: %v", err)
		}
		if stamp != nil {
			stamp(req)
		}
		return req
	}

	// Warm up the keep-alive connection and JIT path.
	for i := 0; i < 100; i++ {
		resp, err := client.Do(makeReq())
		if err != nil {
			b.Fatalf("warmup: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Do(makeReq())
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// startApp's signature uses *testing.T. Provide a thin shim that lets
// the benchmark reuse it via the testing.TB interface that both T and
// B satisfy. Avoids duplicating the listener boilerplate.
//
// (startApp is defined in integration_test.go; we cast b to its
// expected type via a small adapter.)
//
// In Go, *testing.B can't masquerade as *testing.T directly, so we
// build a parallel implementation here that takes *testing.B.

// BenchmarkRequest_NoMiddleware is the baseline — sync route, no mw.
// The framework dispatches directly through the uWS C-callback with
// no wrapper layer. Any middleware-related changes should not move
// this number.
func BenchmarkRequest_NoMiddleware(b *testing.B) {
	runHTTPBench(b, func(app *gogo.App) {
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	}, "/x")
}

// BenchmarkRequest_SyncRoute_Logger exercises a sync route with the
// bundled Logger middleware (PlaceBoth — runs on the loop thread for
// sync routes).
func BenchmarkRequest_SyncRoute_Logger(b *testing.B) {
	runHTTPBench(b, func(app *gogo.App) {
		// Discard log output so the benchmark isn't I/O-bound on stdout.
		app.Use(middleware.Logger(middleware.LoggerOptions{Output: io.Discard}))
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	}, "/x")
}

// BenchmarkRequest_AsyncRoute_Logger exercises an async route with
// the bundled Logger middleware. Logger is PlaceBoth — async route
// uses the worker-goroutine async chain, keeping the zero-cgo
// shared-memory dispatch path alive.
func BenchmarkRequest_AsyncRoute_Logger(b *testing.B) {
	runHTTPBench(b, func(app *gogo.App) {
		app.Use(middleware.Logger(middleware.LoggerOptions{Output: io.Discard}))
		app.GetAsync("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	}, "/x")
}

// BenchmarkRequest_SyncRoute_RateLimit exercises a sync route with
// the bundled RateLimit middleware (PlaceSync). The middleware reads
// the peer IP and writes three rate-limit headers — measures the
// "cheap rejecter on loop thread" path.
func BenchmarkRequest_SyncRoute_RateLimit(b *testing.B) {
	runHTTPBench(b, func(app *gogo.App) {
		app.Use(middleware.RateLimit(middleware.RateLimitOptions{
			Max:    1000000,
			Window: time.Hour,
		}))
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	}, "/x")
}

// BenchmarkRequest_AsyncRoute_RateLimit exercises an async route
// with RateLimit. PlaceSync mw on async route triggers the slow path
// (sync wrap → snapshot → async dispatch), which is intentionally
// slower than the zero-cgo path — but the trade-off is the rejecter
// fires before any goroutine spawns.
func BenchmarkRequest_AsyncRoute_RateLimit(b *testing.B) {
	runHTTPBench(b, func(app *gogo.App) {
		app.Use(middleware.RateLimit(middleware.RateLimitOptions{
			Max:    1000000,
			Window: time.Hour,
		}))
		app.GetAsync("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	}, "/x")
}

// BenchmarkRequest_AsyncRoute_NoMW is the async-route baseline. It
// uses the zero-cgo shared-memory dispatch path with no middleware
// at all — the upper bound for async-route throughput in this
// framework.
func BenchmarkRequest_AsyncRoute_NoMW(b *testing.B) {
	runHTTPBench(b, func(app *gogo.App) {
		app.GetAsync("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	}, "/x")
}

// BenchmarkRequest_AsyncRoute_Stack exercises a realistic stack
// (Logger + Helmet + RequestID + CORS) on an async route to measure
// the cumulative cost of multiple PlaceBoth middleware running in
// the worker.
func BenchmarkRequest_AsyncRoute_Stack(b *testing.B) {
	runHTTPBench(b, func(app *gogo.App) {
		app.Use(middleware.Logger(middleware.LoggerOptions{Output: io.Discard}))
		app.Use(middleware.Helmet())
		app.Use(middleware.RequestID())
		app.Use(middleware.CORS())
		app.GetAsync("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	}, "/x")
}

// BenchmarkRequest_SyncRoute_CORS isolates the CORS middleware on a
// sync route. CORS sets 2-3 response headers per request (Allow-Origin,
// Vary, optionally Expose-Headers) — the exact scenario T-1's batched
// flushPendingHeaders targets, and T-2's pre-lowercased origins
// shaves the per-request EqualFold loop.
func BenchmarkRequest_SyncRoute_CORS(b *testing.B) {
	runHTTPBench(b, func(app *gogo.App) {
		app.Use(middleware.CORS(middleware.CORSOptions{
			AllowOrigins:  []string{"https://app.example.com", "https://*.example.com"},
			ExposeHeaders: []string{"X-Request-Id", "X-Rate-Limit"},
		}))
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	}, "/x")
}

// BenchmarkRequest_SyncRoute_HeavyHeaders exercises the many-headers
// path that T-1 (batch flushPendingHeaders) optimizes most. The
// middleware stamps 8 response headers; without batching this
// would cost ~8 cgo crossings per response on top of the status +
// body crossings.
func BenchmarkRequest_SyncRoute_HeavyHeaders(b *testing.B) {
	runHTTPBench(b, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
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
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	}, "/x")
}

// BenchmarkRequest_SyncRoute_CORS_OriginMatch sends an Origin header
// matching one of two configured allow-list entries — this is the
// path T-2 affects (origin lowercased once per request + direct ==
// rather than EqualFold per pattern).
func BenchmarkRequest_SyncRoute_CORS_OriginMatch(b *testing.B) {
	runHTTPBenchWithRequest(b, func(app *gogo.App) {
		app.Use(middleware.CORS(middleware.CORSOptions{
			AllowOrigins: []string{
				"https://app.example.com",
				"https://admin.example.com",
				"https://*.tenant.example.com",
			},
		}))
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	}, "/x", func(req *http.Request) {
		req.Header.Set("Origin", "https://admin.example.com")
	})
}
