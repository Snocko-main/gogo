// Package gogo is the "gogo~" HTTP framework: a Go binding for the
// uWebSockets C++ HTTP server tuned for low cgo overhead.
//
// # Build
//
// The native binding is opt-in because it links a vendored uWebSockets /
// uSockets and requires cgo. Build with:
//
//	CGO_ENABLED=1 go build -tags gogo ./...
//
// Without the gogo build tag the package compiles a stub that returns an
// error from NewApp — useful for tools that import the package but won't
// actually run a server.
//
// # Hello world
//
//	import gogo "github.com/Snocko-main/gogo"
//
//	func main() {
//	    app, err := gogo.NewApp()
//	    if err != nil { panic(err) }
//	    defer app.Close()
//
//	    app.Get("/hello/:name", func(res *gogo.Response, req *gogo.Request) {
//	        res.Send(200, "text/plain", "hi " + req.Parameter(0))
//	    })
//	    if !app.Listen(3000) { panic("listen") }
//	    app.Run()
//	}
//
// # Handler styles
//
// gogo offers three handler styles, picking the lowest-overhead dispatch
// path that still satisfies the constraints of the route:
//
//   - app.Get / app.Post / app.Any (Handler):
//     synchronous handler invoked from the uWS loop thread. Must not block.
//     One cgo callback per request. Use for fast in-memory replies and
//     middleware that doesn't fan out to async work.
//
//   - app.GetAsync (AsyncHandler):
//     handler runs on a goroutine and is free to block. Without middleware,
//     gogo dispatches through a shared-memory ring with ZERO cgo crossings
//     per request — the C++ side snapshots the request into the AsyncCtx
//     and pushes a pointer onto the ring; a pool of long-lived Go workers
//     drains it. With middleware, the framework falls back to a sync
//     callback that runs the chain with the live request, then captures a
//     snapshot and spawns the user goroutine.
//
//   - app.PostAsync (PostAsyncHandler):
//     async handler that receives a fully-collected body up to maxBodyBytes.
//     Oversize bodies return 413 automatically. Uses res.OnData internally
//     and switches to async mode once the body is complete.
//
// All async handlers receive a *Request snapshot (URL/method/query/params/
// headers all captured before uWS freed the live request). Snapshot caps in
// the zero-cgo shared path: URL 256, query 512, params 8x64, headers 4 KB
// total; requests that exceed those caps are rejected with 431 rather than
// being silently truncated. The middleware/PostAsync paths copy headers exactly
// via cgo so they have no cap.
//
// # Responses
//
// Use res.Send for one-shot replies with status, content-type, and body.
// The framework auto-picks the fastest path:
//   - Sync handler: one cgo crossing into uWS.
//   - Async handler with body ≤ 8 KB: ZERO cgo — written into shared-memory
//     inline buffers and pushed onto the App's response ring; the loop
//     drains it.
//   - Async handler with body > 8 KB: cgo Loop::defer falls back.
//
// res.JSON wraps Send with json.Marshal and Content-Type: application/json.
//
// # Middleware
//
// Sync middleware runs on the uWS loop thread and must NOT block. Use it
// for cheap cross-cutting work (auth header check, logging, CORS).
//
//	app.Use(loggerMW)                    // every route registered later
//	app.Use("/api/*", authMW, corsMW)    // only routes under /api/
//	app.Get("/api/x", handleX)
//
//	authMW := func(next gogo.Handler) gogo.Handler {
//	    return func(res *gogo.Response, req *gogo.Request) {
//	        if req.Header("authorization") == "" {
//	            res.Send(401, "text/plain", "no")
//	            return
//	        }
//	        next(res, req)
//	    }
//	}
//
// The first argument to Use may optionally be a path pattern that scopes the
// middleware to routes whose pattern starts with that prefix. "/api/*" and
// "/api" mean the same thing — trailing "/*" or "/**" is stripped. Without a
// pattern, middleware applies to every later-registered route.
//
// Path matching happens at route registration, so per-request overhead is
// just a function call through the matched chain — no string comparison per
// request. Static replies (Reply, string, []byte targets of app.Get) bypass
// middleware.
//
// # Async middleware
//
// AsyncMiddleware wraps GetAsync / PostAsync handlers and runs on the same
// goroutine as the user handler, so it IS free to block — typical use:
// resolve a user from a session token via a DB lookup, then hand the
// loaded user to the handler.
//
//	app.Use("/api/*", func(next gogo.AsyncHandler) gogo.AsyncHandler {
//	    return func(res *gogo.Response, req *gogo.Request) {
//	        token := req.Header("authorization")
//	        user, err := db.LoadUserByToken(token)   // blocking — ok
//	        if err != nil {
//	            res.Send(401, "text/plain", "unauthorized\n")
//	            return
//	        }
//	        req.SetLocal("user", user)
//	        next(res, req)
//	    }
//	})
//
//	app.GetAsync("/api/me", func(res *gogo.Response, req *gogo.Request) {
//	    user := req.Local("user").(*User)
//	    res.JSON(200, user)
//	})
//
// Async middleware applies only to GetAsync / PostAsync. If only async
// middleware matches a GetAsync route (no sync mw), the framework still
// uses the zero-cgo shared-memory dispatch path; the async chain composes
// inside the worker goroutine alongside the user handler. Sync routes
// (Get, Post, Any) never see async middleware.
//
// req.SetLocal / req.Local pass values from middleware to the handler;
// req.Body() returns the collected body for PostAsync routes (nil
// otherwise), so async middleware can inspect the body before the handler.
//
// # Cookies, JSON
//
//	req.Cookie("session")
//	res.SetCookie(gogo.Cookie{Name: "x", Value: "y", HttpOnly: true})
//	res.JSON(200, map[string]any{"ok": true})
//
// # Multi-core
//
// Single-loop mode (NewApp + Run) caps throughput at one OS thread —
// uWebSockets is event-loop driven, not goroutine-per-request. To
// saturate every vCPU, use RunMultiCore:
//
//	handle, err := gogo.RunMultiCore(runtime.NumCPU(), 3000, func(app *gogo.App) {
//	    app.Get("/plain", plainHandler)
//	    // … same routes / middleware as a single-loop app …
//	})
//	if err != nil { log.Fatal(err) }
//	// signal-driven graceful shutdown:
//	<-sigCh
//	handle.Shutdown()
//	handle.Wait()
//
// RunMultiCore spawns N independent App instances, each bound to the
// same port. Accepted sockets are round-robined across the App loops,
// so scaling does not depend on the kernel's SO_REUSEPORT hash
// distributing connections evenly. setup runs once per instance on
// the OS thread that instance will own.
//
// Tuning knobs that actually matter:
//
//   - GOMAXPROCS — pin to the same N you passed to RunMultiCore. The
//     scheduler then has exactly one P per loop; oversubscribing
//     wastes context-switch budget, undersubscribing starves loops.
//   - SetWorkerCount — controls the GetAsync worker-goroutine pool.
//     Default = NumCPU. With RunMultiCore each loop already owns one
//     core; the workers compete for the same CPUs, so consider
//     halving this if your GetAsync handlers are short and your
//     workload is sync-route-heavy.
//   - Shared resources (DB pools, caches) — create ONCE outside
//     RunMultiCore and capture the pointers into the handler
//     closures. setup runs once per loop; allocating fresh DB pools
//     per loop wastes RAM and connection slots.
//   - Per-loop CPU pinning — gogo does not pin to specific cores.
//     Linux's scheduler typically keeps each loop on its initial CPU
//     for cache locality. If you need stricter pinning run the
//     server under `taskset -c 0-(N-1)` or wrap LockOSThread with a
//     sched_setaffinity call.
//
// On a 4 vCPU host the gogo bench /plain route scales from ~112 k RPS
// at 1 core to ~230 k RPS at 2 cores (~2.05× linear). Past 2 cores
// the same-host wrk client starts competing with the server for CPU,
// so the apparent 4-core number drops back to ~200 k — a true 4-core
// measurement needs a separate load-generator host. Either way, gogo
// per core consistently outruns fiber per core on this hardware
// (+86 % at 1 core, +48 % at 4 cores, both routes saturated).
//
// See examples/multicore for a full setup with graceful shutdown
// + a /metrics endpoint formatted as Prometheus text exposition.
//
// # Graceful shutdown
//
//	app.Shutdown() // close listen socket + drain timer; returns immediately
//	// when Run() returns, call:
//	app.Close()    // free native resources
//
// Shutdown is safe from any goroutine.
//
// # Performance characteristics
//
//   - Per-request alloc on the GET fast path: dominated by Go runtime work,
//     not framework.
//   - statusLine for common HTTP codes (200/201/204/3xx/4xx/5xx) is
//     precomputed and zero-alloc.
//   - PanicHandler (see SetPanicHandler) catches handler panics across HTTP,
//     async, WebSocket, defer, and body callbacks, emits a best-effort 500
//     where an HTTP response is still available, and keeps the server alive.
package gogo
