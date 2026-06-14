// Package gogo is the "gogo~" HTTP framework: a Go binding for the
// uWebSockets C++ HTTP server tuned for low cgo overhead.
//
// # Build
//
// The native binding is opt-in because it compiles the vendored uWebSockets /
// uSockets sources and requires cgo. Native builds need Go 1.24+, CGO_ENABLED=1,
// a C compiler, a C++20-capable compiler, and the host zlib library/headers.
// Build with:
//
//	CGO_ENABLED=1 go build -tags gogo ./...
//	CGO_ENABLED=1 go run -tags gogo .
//	CGO_ENABLED=1 go test -tags gogo ./...
//
// Without the gogo build tag the package compiles a stub that returns an
// error from NewApp — useful for tools that import the package but won't
// actually run a server.
// See docs/install-build.md for OS package prerequisites and downstream smoke
// build validation.
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
// # Routing
//
// Route patterns are path-only patterns that must start with "/". gogo uses
// uWS route syntax for literals, named params such as "/users/:id", and
// trailing wildcards such as "/files/*", then adds typed annotations such as
// "/users/:id<int>". Typed annotations are stripped before registration and
// checked before middleware or handlers run; invalid values receive 404.
//
// For a concrete HTTP method, literal segments take precedence over named
// params, and named params take precedence over wildcard catch-alls.
// Method-specific routes run before Any routes. Custom NotFound and
// MethodNotAllowed handlers are installed as a catch-all at Listen time after
// user routes, so explicit routes retain precedence. MethodNotAllowed
// distinguishes wrong-method requests for literal, named-param, typed-param,
// and terminal wildcard routes; typed-param constraints must match before the
// route contributes to the Allow header.
//
// Use Group or Mount to bind a prefix and middleware to a router identity:
//
//	api := app.Group("/api", authMW)
//	api.Get("/users/:id", showUser)
//
// Group prefixes may include named or typed params, reject wildcards, strip
// trailing slashes, and concatenate with child patterns that also start with
// "/". Router middleware composes at route registration with no per-request
// prefix check; parent middleware wraps child middleware.
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
//   - app.PostAsync / app.PutAsync / app.PatchAsync / app.DeleteAsync
//     (BodyAsyncHandler):
//     async handler that receives a fully-collected body up to maxBodyBytes.
//     Oversize bodies return 413 automatically. Uses res.OnData internally
//     and switches to async mode once the body is complete.
//
// All async handlers receive a *Request snapshot (URL/method/query/params/
// headers all captured before uWS freed the live request). Snapshot caps in
// the zero-cgo shared path: URL 256, query 512, params 8x64, headers 8 KB
// total; requests that exceed those caps are rejected with 431 rather than
// being silently truncated. The middleware/body-async paths copy headers
// exactly via cgo so they have no cap.
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
// res.JSON wraps Send with Config.JSONEncoder (encoding/json.Marshal by
// default) and Content-Type: application/json.
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
// The first argument to Use may optionally be a path prefix that scopes the
// middleware to request URLs under that prefix. "/api/*", "/api/**", and
// "/api" mean exact "/api" plus children under "/api/"; "/", "/*", and
// "/**" mean global. Without a pattern, middleware applies to every
// later-registered route.
//
// Global middleware composes at route registration with no per-request string
// comparison. Scoped middleware matches the live request URL so dynamic routes
// cannot bypass a scoped guard. Static replies (Reply, string, []byte targets
// of app.Get) use the zero-cgo fast path only when no matching sync middleware
// or typed-param constraint needs to run.
//
// # Async middleware
//
// AsyncMiddleware wraps GetAsync and body-async handlers and runs on the same
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
// Async middleware applies only to GetAsync and body-async routes. If only
// async middleware matches a GetAsync route (no sync mw), the framework still
// uses the zero-cgo shared-memory dispatch path; the async chain composes
// inside the worker goroutine alongside the user handler. Sync routes
// (Get, Post, Put, Patch, Delete, Any) never see async middleware.
//
// req.SetLocal / req.Local pass values from middleware to the handler;
// req.Body() returns the collected body for body-async routes (nil
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
// run multiple event loops, use RunMultiCore:
//
//	handle, err := gogo.RunMultiCore(runtime.NumCPU(), 3000, func(app *gogo.App) {
//	    app.Get("/plain", plainHandler)
//	    // … same routes / middleware as a single-loop app …
//	})
//	if err != nil { log.Fatal(err) }
//	// signal-driven immediate shutdown:
//	<-sigCh
//	handle.Shutdown()
//	handle.Wait()
//
// RunMultiCore spawns N independent App instances, each bound to the
// same port. The default mode lets the kernel distribute accepted
// sockets with SO_REUSEPORT, which avoids cross-loop socket handoff
// overhead. If you need predictable per-loop connection placement,
// use RunMultiCoreWithOptions with MultiCoreBalanced; that mode
// round-robins accepted sockets across App loops at extra accept-path
// cost. setup runs once per instance on the OS thread that instance
// will own. setup has no error return; do fallible shared
// initialization before RunMultiCore. A setup panic is recovered,
// converted to an error, and any created Apps are closed.
// RunMultiCore currently creates each worker with the zero-value Config; there
// is no Config parameter for app-scoped settings such as BodyLimit,
// BodyReadTimeout, BindAddr, CapturePeerIP, TrustProxy, or custom JSON codecs.
// Use process-wide knobs before RunMultiCore and per-route/per-middleware
// options inside setup.
//
// MultiCoreHandle.Shutdown calls Shutdown on every worker App, so multicore
// shutdown is immediate and active connections are closed. There is no
// multicore equivalent of App.ShutdownGracefully yet.
//
// Tuning knobs that actually matter:
//
//   - GOMAXPROCS — pin to the same N you passed to RunMultiCore. The
//     scheduler then has exactly one P per loop; oversubscribing
//     wastes context-switch budget, undersubscribing starves loops.
//   - SetWorkerCount — controls the GetAsync worker-goroutine pool.
//     Default = ceil(1.5 × worker-hint loops). A single App and
//     default RunMultiCore reuseport mode use the one-loop default so
//     async workers do not steal CPU when the kernel places many
//     connections on one listener. MultiCoreBalanced uses the full
//     loop count. Raise it for IO-bound handlers that keep many
//     requests blocked at once.
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
// See examples/multicore for a full setup with signal handling
// + a /metrics endpoint formatted as Prometheus text exposition.
// See docs/production-examples.md for the v1 production example coverage map.
//
// # Graceful shutdown
//
//	runDone := make(chan struct{})
//	go func() {
//	    app.Run()
//	    close(runDone)
//	}()
//
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	if err := app.ShutdownContext(ctx); err != nil {
//	    log.Printf("forced shutdown: %v", err)
//	}
//	<-runDone
//	app.Close() // free native resources after Run returns
//
// ShutdownContext returns nil only after Run exits. If the context expires
// first, it force-closes active connections and returns the context error;
// wait for Run to return before calling Close.
//
// Shutdown, ShutdownGracefully, and ShutdownContext are safe from any goroutine.
// Native builds own the uWS loop on an internal locked goroutine, so normal
// single-app code does not need runtime.LockOSThread. Register routes,
// middleware, and WebSocket behavior before Listen / Run; shutdown and publish
// APIs are the supported cross-goroutine entry points once the loop is running.
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
