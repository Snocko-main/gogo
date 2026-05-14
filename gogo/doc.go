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
//	import gogo "uwebsockets-go/gogo"
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
// total. The middleware/PostAsync paths copy headers exactly via cgo so
// they have no cap.
//
// # Responses
//
// Use res.Send for one-shot replies with status, content-type, and body in
// a single cgo call. Inside an async handler, res.SendShared writes into
// the AsyncCtx's inline buffers and pushes onto a response ring drained by
// the loop — zero cgo per response when the body fits 8 KB. res.JSON wraps
// Send/SendShared with json.Marshal and the correct Content-Type.
//
// # Middleware
//
//	app.Use(authMW, corsMW)
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
// Middleware composes at registration time, so per-request overhead is
// just a function call. Static replies (Reply, string, []byte targets of
// app.Get) bypass middleware.
//
// # Cookies, JSON
//
//	req.Cookie("session")
//	res.SetCookie(gogo.Cookie{Name: "x", Value: "y", HttpOnly: true})
//	res.JSON(200, map[string]any{"ok": true})
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
//   - PanicHandler (see SetPanicHandler) catches handler panics in the
//     shared-dispatch worker, emits a best-effort 500, and keeps the worker
//     alive.
package gogo
