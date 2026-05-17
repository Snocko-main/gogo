package gogo

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// commonStatusLines caches the formatted status line for codes likely to
// appear on the hot path so Send/JSON don't pay 2 allocs per response.
// Anything not in this table falls back to the slow strconv+concat path.
var commonStatusLines = map[int]string{
	200: "200 OK",
	201: "201 Created",
	202: "202 Accepted",
	204: "204 No Content",
	301: "301 Moved Permanently",
	302: "302 Found",
	304: "304 Not Modified",
	400: "400 Bad Request",
	401: "401 Unauthorized",
	403: "403 Forbidden",
	404: "404 Not Found",
	405: "405 Method Not Allowed",
	409: "409 Conflict",
	413: "413 Request Entity Too Large",
	429: "429 Too Many Requests",
	500: "500 Internal Server Error",
	502: "502 Bad Gateway",
	503: "503 Service Unavailable",
	504: "504 Gateway Timeout",
}

// statusLine formats an HTTP status code into the "<code> <reason>" string
// uWS writes verbatim onto the wire. Standard codes get their canonical reason
// phrase via net/http; unknown codes fall back to just the number. Common
// codes hit the cache and avoid all allocation.
func statusLine(code int) string {
	if cached, ok := commonStatusLines[code]; ok {
		return cached
	}
	text := http.StatusText(code)
	if text == "" {
		return strconv.Itoa(code)
	}
	return strconv.Itoa(code) + " " + text
}

// validatePattern rejects registration-time patterns that would crash uWS or
// produce surprising routes. Patterns must be non-empty, start with '/', and
// contain no control characters (NUL/CR/LF).
func validatePattern(pattern string) {
	if pattern == "" {
		panic("gogo: route pattern must not be empty")
	}
	if pattern[0] != '/' {
		panic(fmt.Sprintf("gogo: route pattern %q must start with '/'", pattern))
	}
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if c == 0 || c == '\r' || c == '\n' {
			panic(fmt.Sprintf("gogo: route pattern %q contains a control character", pattern))
		}
	}
}

// validateHeaderValue rejects header values that contain CR/LF/NUL, which
// would otherwise allow HTTP response splitting if user-controlled input is
// passed straight through.
func validateHeaderValue(key, value string) {
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '\r' || c == '\n' || c == 0 {
			panic(fmt.Sprintf("gogo: header %q value contains a control character (CR/LF/NUL not allowed)", key))
		}
	}
}

var (
	responsePool   = sync.Pool{New: func() any { return &Response{} }}
	requestPool    = sync.Pool{New: func() any { return &Request{} }}
	asyncStatePool = sync.Pool{New: func() any { return &asyncState{} }}
)

// PanicHandler is invoked when user code panics inside an HTTP, async,
// WebSocket, defer, or body callback. HTTP paths emit a best-effort 500 when a
// response is still available. The argument is the recovered value (the panic
// payload).
//
// The framework ships a default handler that prints the panic value plus a
// goroutine stack trace to stderr, so production deployments never have a
// panic disappear silently. Call SetPanicHandler with a custom function to
// route panics elsewhere (structured logger, error tracker), or pass nil
// to restore the default.
type PanicHandler func(recovered any)

var (
	panicHandlerMu sync.RWMutex
	panicHandlerFn PanicHandler = defaultPanicHandler
)

// defaultPanicHandler writes the recovered value and a goroutine stack
// trace to stderr. Matches the format Go's runtime uses for unrecovered
// panics so operators have something familiar to grep for.
func defaultPanicHandler(recovered any) {
	fmt.Fprintf(os.Stderr, "gogo: recovered panic: %v\n%s\n",
		recovered, debug.Stack())
}

// SetPanicHandler registers fn as the global panic handler. Pass nil to
// restore the default stderr logger. The handler must not panic itself
// (any panic inside it is recovered silently).
func SetPanicHandler(fn PanicHandler) {
	panicHandlerMu.Lock()
	if fn == nil {
		panicHandlerFn = defaultPanicHandler
	} else {
		panicHandlerFn = fn
	}
	panicHandlerMu.Unlock()
}

func getPanicHandler() PanicHandler {
	panicHandlerMu.RLock()
	defer panicHandlerMu.RUnlock()
	return panicHandlerFn
}

func reportPanic(recovered any) {
	panicHandler := getPanicHandler()
	if panicHandler == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	panicHandler(recovered)
}

// Handler handles a single HTTP request.
//
// The Request and Response values are only valid for the duration of the
// callback. Do not store them or use them from another goroutine.
type Handler func(*Response, *Request)

// AsyncHandler handles a request on a goroutine that is free to block.
// The Response arrives in async mode with the abort context pre-attached.
// The Request is a snapshot copied from the live uWS request before it was
// freed; URL/method/query/params/headers are all readable, with fixed caps
// (URL 256, query 512, each param 64, headers buffer 4 KB).
type AsyncHandler func(*Response, *Request)

// OpCode identifies a WebSocket frame type.
type OpCode int

const (
	// Text is a UTF-8 WebSocket message.
	Text OpCode = 1

	// Binary is a binary WebSocket message.
	Binary OpCode = 2
)

// WebSocketBehavior contains callbacks and per-route limits for a
// WebSocket endpoint. Limit fields default to safe production values
// when zero — pick explicit numbers when you need different limits, do
// not leave them at zero hoping for "unlimited".
type WebSocketBehavior struct {
	Open    func(*WebSocket)
	Message func(*WebSocket, []byte, OpCode)
	Close   func(*WebSocket, int, []byte)

	// MaxPayloadLength is the largest single incoming message the
	// server will accept. Frames over this cap cause uWS to close the
	// connection. Default 16 MiB.
	MaxPayloadLength int

	// IdleTimeout is the maximum time a WebSocket may sit idle (no
	// frames in either direction) before uWS closes it. Default 120s.
	IdleTimeout time.Duration

	// MaxBackpressure is the bytes uWS will queue per-socket for a
	// slow consumer before closing the connection. Protects the
	// loop from being held hostage by a single non-draining client.
	// Default 64 KiB.
	MaxBackpressure int

	// DisablePings turns off uWS's built-in ping/pong keepalive.
	// Default (zero) leaves automatic pings ON so an idle connection
	// doesn't get reaped by NAT boxes; set true only if your client
	// drives its own ping protocol.
	DisablePings bool
}

// Middleware wraps a Handler with cross-cutting behavior (auth, logging,
// CORS, etc.). The returned Handler is invoked per request; call next(res,
// req) to continue the chain or write a response and return to short-circuit.
//
// Middleware composes at registration time, so there is no per-request
// allocation. Middleware applies to routes registered AFTER the call to
// App.Use that introduced it; reordering Use and route registration changes
// the effective chain for those routes.
//
// Middleware runs on the uWS loop thread and MUST NOT block — no DB queries,
// no remote calls. For middleware that needs to block (e.g. resolving a user
// from a session token via a DB lookup), use AsyncMiddleware with UseAsync;
// it runs on a goroutine and is free to block.
//
// Static replies (Reply, string, []byte targets of App.Get) and GetShared
// routes bypass middleware because they have no Go-side handler to wrap.
type Middleware func(next Handler) Handler

// AsyncMiddleware wraps an AsyncHandler the same way Middleware wraps a
// Handler, but executes on the goroutine that runs the user's async handler
// so it is free to block (DB queries, downstream HTTP calls, etc.).
//
// Async middleware applies only to GetAsync and PostAsync routes. Use it
// when the cross-cutting work itself needs to block; for cheap header /
// query inspection prefer sync Middleware (smaller per-request overhead and
// also applicable to sync routes).
//
// Pass data through to the user handler via Request.SetLocal / Request.Local.
type AsyncMiddleware func(next AsyncHandler) AsyncHandler

// middlewareEntry is a single Use call's middleware bound to an optional
// path prefix. prefix == "" means "global" (apply to every later route);
// otherwise the middleware applies only to routes whose pattern is the
// prefix itself or sits under "prefix/".
type middlewareEntry struct {
	prefix string
	mw     Middleware
}

// asyncMiddlewareEntry mirrors middlewareEntry for the async chain.
type asyncMiddlewareEntry struct {
	prefix string
	mw     AsyncMiddleware
}

// Config tunes per-App behavior. All fields are optional; the zero value
// is a safe production default. Pass to NewApp; values are applied at
// app creation and bind time. The struct is intentionally narrow — knobs
// only get added here when they need a single, app-wide value.
type Config struct {
	// BodyLimit caps the request-body bytes a Post / Any route will
	// accept. The framework rejects oversized requests with 413 at
	// arrival by checking the Content-Length header on the C++ side
	// before dispatching to Go — zero per-request cost beyond the
	// existing header lookup. Chunked transfer-encoded requests with
	// no Content-Length bypass this check; handlers that accept those
	// must call res.Body(maxN, ...) for protection. Set to 0 to
	// disable. Default 4 MiB.
	BodyLimit int

	// BindAddr is the local interface to bind on. Empty string means
	// "all interfaces" (uWS default 0.0.0.0). Use "127.0.0.1" for a
	// localhost-only service. Applied at Listen time.
	BindAddr string
}

// App is a uWebSockets HTTP application.
type App struct {
	inner            appNative
	middlewares      []middlewareEntry
	asyncMiddlewares []asyncMiddlewareEntry
	cfg              Config
}

// defaultConfig fills in safe production defaults for any zero Config
// fields. Mutates and returns the input.
func defaultConfig(c Config) Config {
	if c.BodyLimit == 0 {
		c.BodyLimit = 4 << 20 // 4 MiB
	}
	return c
}

// NewApp creates a non-TLS uWebSockets app. With no Config the app uses
// safe production defaults; pass one Config to override (extra Configs
// are ignored — variadic only for backward compat with the old zero-arg
// signature).
func NewApp(cfg ...Config) (*App, error) {
	inner, err := newAppNative()
	if err != nil {
		return nil, err
	}
	initSharedLayout()
	var c Config
	if len(cfg) > 0 {
		c = cfg[0]
	}
	c = defaultConfig(c)
	inner.setBodyLimit(c.BodyLimit)
	return &App{inner: inner, cfg: c}, nil
}

// Reply is a static response captured once at registration time. Routes
// registered with this target are served entirely by the C++ event loop with
// no cgo callback per request — use it for /health, /version, cached config,
// or any constant response.
type Reply struct {
	Status      int    // defaults to 200 when zero
	ContentType string // omits Content-Type header when empty
	Body        string
}

// Use appends middleware to the chain. Each registered route that follows
// this call wraps its handler in the current chain.
//
// The first argument may optionally be a path pattern (string), scoping the
// middleware to URLs that fall under that prefix at request time:
//
//	app.Use(authMW)                       // applies to every later route
//	app.Use("/api/*", authMW)             // applies to URLs under /api/
//	app.Use("/admin", auditMW, rateMW)    // /admin and URLs under /admin/
//
// Trailing "/*" or "/**" on the prefix is stripped — "/api/*" and "/api"
// mean the same thing (prefix = "/api"). A pattern of "/*" or "/" means
// "every route" (equivalent to no pattern).
//
// Scoped Use matches the live request URL via req.URL(), not the route
// pattern string. This makes it safe against parametric / wildcard routes
// (e.g. Get("/api/:section") serving /api/admin will go through a
// Use("/api/admin", auth) middleware). Cost: one URL string compare per
// scoped entry per request — global Use (no prefix) still composes at
// registration with zero per-request cost.
//
// Prefer App.Group(prefix, mws...) for scoping new code: Group binds
// middleware by Router identity, so the chain composes at registration
// time and matching is unambiguous without the per-request URL check.
//
// Middlewares run left-to-right — the first argument runs first (outermost).
// Safe to call multiple times.
func (a *App) Use(args ...any) {
	if len(args) == 0 {
		return
	}

	var (
		prefix     string
		hasPrefix  bool
		startIndex int
	)
	if s, ok := args[0].(string); ok {
		prefix = normalizeMWPrefix(s)
		hasPrefix = true
		startIndex = 1
	}

	if startIndex == len(args) {
		if hasPrefix {
			panic("gogo: Use: path pattern given but no middleware passed")
		}
		return
	}

	for i := startIndex; i < len(args); i++ {
		var m Middleware
		switch v := args[i].(type) {
		case Middleware:
			m = v
		case func(next Handler) Handler:
			m = Middleware(v)
		case string:
			panic("gogo: Use: only the first argument may be a path pattern")
		default:
			panic(fmt.Sprintf("gogo: Use: unsupported argument type %T at index %d", v, i))
		}
		a.middlewares = append(a.middlewares, middlewareEntry{prefix: prefix, mw: m})
	}
}

// normalizeMWPrefix turns a user-facing Use pattern into the stored prefix
// form. "/api/*" → "/api"; "/api/**" → "/api"; "/" or "/*" → "" (global).
func normalizeMWPrefix(p string) string {
	validatePattern(p)
	for strings.HasSuffix(p, "/**") {
		p = p[:len(p)-3]
	}
	for strings.HasSuffix(p, "/*") {
		p = p[:len(p)-2]
	}
	if p == "" || p == "/" {
		return ""
	}
	return p
}

// mwMatches reports whether a middleware bound to prefix should apply to a
// route registered with routePattern.
func mwMatches(prefix, routePattern string) bool {
	if prefix == "" {
		return true
	}
	if routePattern == prefix {
		return true
	}
	return strings.HasPrefix(routePattern, prefix+"/")
}

// wrap composes registered middleware around h. Global middleware (prefix
// == "") is wrapped at registration time with zero per-request cost. When
// any path-scoped middleware is present, composition is deferred to request
// time and matched against the live URL via req.URL() rather than the route
// pattern string — this closes the bypass that pattern-string matching had
// for dynamic / wildcard routes (e.g. Get("/api/:section") serving
// /api/admin would not have triggered Use("/api/admin", auth) under the
// old scheme because the literal strings "/api/:section" and "/api/admin"
// do not share a prefix).
func (a *App) wrap(routePattern string, h Handler) Handler {
	if len(a.middlewares) == 0 {
		return h
	}
	hasScoped := false
	for _, e := range a.middlewares {
		if e.prefix != "" {
			hasScoped = true
			break
		}
	}
	if !hasScoped {
		for i := len(a.middlewares) - 1; i >= 0; i-- {
			h = a.middlewares[i].mw(h)
		}
		return h
	}
	entries := make([]middlewareEntry, len(a.middlewares))
	copy(entries, a.middlewares)
	inner := h
	return func(res *Response, req *Request) {
		url := req.URL()
		chain := inner
		for i := len(entries) - 1; i >= 0; i-- {
			e := entries[i]
			if e.prefix == "" || urlUnderPrefix(url, e.prefix) {
				chain = e.mw(chain)
			}
		}
		chain(res, req)
	}
}

// urlUnderPrefix reports whether the request URL falls within the path
// prefix scope: exact match or any path under "prefix/".
func urlUnderPrefix(url, prefix string) bool {
	if url == prefix {
		return true
	}
	return strings.HasPrefix(url, prefix+"/")
}

// hasMatchingMiddleware reports whether any registered sync middleware could
// apply to a request that uWS will route to routePattern. Used by GetAsync
// to choose between the zero-cgo shared path and the sync wrapper fallback.
//
// Conservatively reports true when the pattern is dynamic (contains : or *)
// and any scoped middleware exists, because the pattern string alone does
// not tell us which URLs the route will actually serve.
func (a *App) hasMatchingMiddleware(routePattern string) bool {
	if len(a.middlewares) == 0 {
		return false
	}
	dynamic := strings.ContainsAny(routePattern, ":*")
	for _, e := range a.middlewares {
		if e.prefix == "" {
			return true
		}
		if dynamic {
			return true
		}
		if mwMatches(e.prefix, routePattern) {
			return true
		}
	}
	return false
}

// UseAsync appends an async middleware to the chain that wraps GetAsync /
// PostAsync handlers. The middleware runs on the goroutine that runs the
// user handler, so it is free to block (DB queries, downstream HTTP).
//
// Like Use, the first argument may optionally be a path pattern:
//
//	app.UseAsync(loadUserMW)                         // every async route
//	app.UseAsync("/api/*", loadUserMW)               // URLs under /api/
//	app.UseAsync("/api/v2", rateLimitMW, loadMW)     // /api/v2 and children
//
// Trailing "/*" or "/**" is stripped. A pattern of "/", "/*", or no pattern
// at all means "every async route".
//
// Scoped UseAsync matches the live request URL at request time (same fix as
// scoped Use). Prefer App.Group(...).UseAsync(...) for new code to avoid the
// per-request URL check.
//
// To pass values from middleware down to the handler (e.g. the loaded user),
// store them on the Request via req.SetLocal; the handler reads them with
// req.Local.
//
// Async middleware applies only to GetAsync / PostAsync — sync routes
// (Get / Post / Any) never see it. If only async middleware matches a
// GetAsync route, the framework still uses the zero-cgo shared-memory
// dispatch path; the async chain composes inside the worker goroutine.
func (a *App) UseAsync(args ...any) {
	if len(args) == 0 {
		return
	}

	var (
		prefix     string
		hasPrefix  bool
		startIndex int
	)
	if s, ok := args[0].(string); ok {
		prefix = normalizeMWPrefix(s)
		hasPrefix = true
		startIndex = 1
	}

	if startIndex == len(args) {
		if hasPrefix {
			panic("gogo: UseAsync: path pattern given but no middleware passed")
		}
		return
	}

	for i := startIndex; i < len(args); i++ {
		var m AsyncMiddleware
		switch v := args[i].(type) {
		case AsyncMiddleware:
			m = v
		case func(next AsyncHandler) AsyncHandler:
			m = AsyncMiddleware(v)
		case string:
			panic("gogo: UseAsync: only the first argument may be a path pattern")
		default:
			panic(fmt.Sprintf("gogo: UseAsync: unsupported argument type %T at index %d", v, i))
		}
		a.asyncMiddlewares = append(a.asyncMiddlewares, asyncMiddlewareEntry{prefix: prefix, mw: m})
	}
}

// wrapAsync composes registered async middleware around h. Mirrors wrap:
// global async MW wraps at registration; path-scoped async MW composes at
// request time against the live URL so dynamic routes can't bypass scoped
// middleware via pattern/URL mismatch.
func (a *App) wrapAsync(routePattern string, h AsyncHandler) AsyncHandler {
	if len(a.asyncMiddlewares) == 0 {
		return h
	}
	hasScoped := false
	for _, e := range a.asyncMiddlewares {
		if e.prefix != "" {
			hasScoped = true
			break
		}
	}
	if !hasScoped {
		for i := len(a.asyncMiddlewares) - 1; i >= 0; i-- {
			h = a.asyncMiddlewares[i].mw(h)
		}
		return h
	}
	entries := make([]asyncMiddlewareEntry, len(a.asyncMiddlewares))
	copy(entries, a.asyncMiddlewares)
	inner := h
	return func(res *Response, req *Request) {
		url := req.URL()
		chain := inner
		for i := len(entries) - 1; i >= 0; i-- {
			e := entries[i]
			if e.prefix == "" || urlUnderPrefix(url, e.prefix) {
				chain = e.mw(chain)
			}
		}
		chain(res, req)
	}
}

// Get registers a GET route. The target may be:
//
//   - Handler / func(*Response, *Request) — dynamic, invoked per request via cgo
//   - string                              — static body served by C++ (no cgo per request)
//   - []byte                              — same as string
//   - Reply                               — static body with explicit status and Content-Type
//
// Static targets are served entirely in C++ with no Go work per request.
func (a *App) Get(pattern string, target any) {
	validatePattern(pattern)
	switch v := target.(type) {
	case Handler:
		a.inner.get(pattern, a.wrap(pattern, v))
	case func(*Response, *Request):
		a.inner.get(pattern, a.wrap(pattern, Handler(v)))
	case Reply:
		code := v.Status
		if code == 0 {
			code = 200
		}
		if v.ContentType != "" {
			validateHeaderValue("Content-Type", v.ContentType)
		}
		a.inner.getStatic(pattern, statusLine(code), v.ContentType, v.Body)
	case string:
		a.inner.getStatic(pattern, statusLine(200), "", v)
	case []byte:
		a.inner.getStatic(pattern, statusLine(200), "", string(v))
	default:
		panic(fmt.Sprintf("gogo: unsupported Get target type %T for %q", target, pattern))
	}
}

// GetAsync registers a GET route whose handler runs on a goroutine and
// receives a Response in async mode plus a snapshot Request (URL/method/
// query/params/headers captured from the live uWS request before it was
// freed).
//
// Without middleware, GetAsync uses the zero-cgo shared-memory dispatch path:
// C++ snapshots the request, pushes onto a lock-free ring, and a long-lived
// Go worker pool drains it. SendShared in the handler completes the response
// without any cgo crossing per request.
//
// With middleware registered, GetAsync falls back to a sync cgo handler that
// runs the middleware chain with the live request, then captures a snapshot
// and switches to async mode for the user handler. One extra cgo callback
// per request only when middleware is in use.
func (a *App) GetAsync(pattern string, handler AsyncHandler) {
	validatePattern(pattern)

	// Compose the async middleware chain once at registration. wrappedAsync
	// runs the user handler last; matching AsyncMiddleware wraps it
	// outermost-first.
	wrappedAsync := a.wrapAsync(pattern, handler)

	if !a.hasMatchingMiddleware(pattern) {
		// No sync middleware matches → keep the zero-cgo shared-memory
		// dispatch path. The async chain composes inside the worker
		// goroutine alongside the user handler.
		a.inner.getShared(pattern, wrappedAsync)
		return
	}

	a.inner.get(pattern, a.wrap(pattern, func(res *Response, req *Request) {
		// Capture req fields before the sync wrapper returns — uWS frees the
		// underlying HttpRequest the moment we return from this cgo callback.
		snap := req.snapshotFromSync()
		res.Async(func() {
			snapReq := requestPool.Get().(*Request)
			snapReq.snap = snap
			wrappedAsync(res, snapReq)
			snapReq.resetForPool()
			requestPool.Put(snapReq)
		})
	}))
}

// Post registers a POST route.
func (a *App) Post(pattern string, handler Handler) {
	validatePattern(pattern)
	a.inner.post(pattern, a.wrap(pattern, handler))
}

// PostAsyncHandler is the handler signature for PostAsync routes. It receives
// the response, a snapshot of the request (URL/query/params/headers all
// captured before uWS freed the live request), and the fully-collected body.
// Runs on a goroutine so it is free to block.
type PostAsyncHandler func(res *Response, req *Request, body []byte)

// PostAsync registers a POST route that collects the full request body up to
// maxBodyBytes, then invokes handler on a goroutine with the collected bytes
// plus a request snapshot. On bodies that exceed maxBodyBytes the framework
// sends 413 Payload Too Large automatically and the handler is not called.
func (a *App) PostAsync(pattern string, maxBodyBytes int, handler PostAsyncHandler) {
	validatePattern(pattern)

	// Adapt the body-receiving handler into the AsyncHandler shape that
	// AsyncMiddleware expects. The body is stashed on req.body in the
	// runtime wrapper below; middleware can read it via req.Body() and the
	// final user handler still receives it as a parameter.
	finalAsync := AsyncHandler(func(res *Response, req *Request) {
		handler(res, req, req.body)
	})
	wrappedAsync := a.wrapAsync(pattern, finalAsync)

	a.inner.post(pattern, a.wrap(pattern, func(res *Response, req *Request) {
		// Snapshot the request before its lifetime ends. Body collection
		// happens via onData callbacks fired after we return, by which time
		// req would be invalid; the snapshot survives.
		snap := req.snapshotFromSync()
		res.Body(maxBodyBytes, func(body []byte, err error) {
			if err == ErrBodyTooLarge {
				res.Send(413, "text/plain; charset=utf-8", "payload too large\n")
				return
			}
			// Async spawns a goroutine and re-arms onAborted with the async
			// ctx hookup. Body's onAborted has fulfilled its purpose by now.
			res.Async(func() {
				snapReq := requestPool.Get().(*Request)
				snapReq.snap = snap
				snapReq.body = body
				wrappedAsync(res, snapReq)
				snapReq.resetForPool()
				requestPool.Put(snapReq)
			})
		})
	}))
}

// Any registers a route for every HTTP method.
func (a *App) Any(pattern string, handler Handler) {
	validatePattern(pattern)
	a.inner.any(pattern, a.wrap(pattern, handler))
}

// WebSocket registers a WebSocket route.
func (a *App) WebSocket(pattern string, behavior WebSocketBehavior) {
	validatePattern(pattern)
	a.inner.websocket(pattern, behavior)
}

// Router scopes middleware and a path prefix to a subtree of routes. Created
// by App.Group or Router.Group. Routes registered through a Router have the
// Router's prefix prepended and inherit the Router's middleware stack on top
// of the App's global / scoped middleware.
//
// Group scope is identity-based — a route is wrapped because it is registered
// through this Router, not because its pattern string starts with some
// prefix. That avoids the pattern/URL mismatch that App.Use(prefix, mw) has
// to guard against at request time, so Group middleware composes at
// registration with zero per-request cost.
//
// Prefer Group over App.Use(prefix, mw) for scoping new code.
type Router struct {
	app     *App
	prefix  string
	syncMW  []Middleware
	asyncMW []AsyncMiddleware
}

// Group returns a Router scoped to prefix with mws applied to every route
// subsequently registered through it. Prefix must start with '/' and contain
// no wildcards; trailing slash is stripped so Group("/api") and Group("/api/")
// behave identically. Group("/") is equivalent to no prefix.
func (a *App) Group(prefix string, mws ...Middleware) *Router {
	return &Router{
		app:    a,
		prefix: normalizeGroupPrefix(prefix),
		syncMW: append([]Middleware(nil), mws...),
	}
}

// Group creates a nested Router. The child prefix is appended to the parent's
// prefix and middleware is inherited then extended — mws here run inside the
// parent group's middleware.
func (r *Router) Group(prefix string, mws ...Middleware) *Router {
	return &Router{
		app:     r.app,
		prefix:  r.prefix + normalizeGroupPrefix(prefix),
		syncMW:  append(append([]Middleware(nil), r.syncMW...), mws...),
		asyncMW: append([]AsyncMiddleware(nil), r.asyncMW...),
	}
}

// Use appends sync middleware to this Router. Applies to every route
// subsequently registered through this Router (or any child Group created
// after this call).
func (r *Router) Use(mws ...Middleware) {
	r.syncMW = append(r.syncMW, mws...)
}

// UseAsync appends async middleware to this Router. Applies only to GetAsync
// and PostAsync routes registered through this Router.
func (r *Router) UseAsync(mws ...AsyncMiddleware) {
	r.asyncMW = append(r.asyncMW, mws...)
}

// normalizeGroupPrefix validates a Group prefix and trims trailing slashes.
// Wildcards (* / **) are rejected because the prefix is concatenated literally
// onto child route patterns; expressing "everything under here" is the job of
// Group itself, not its prefix.
func normalizeGroupPrefix(p string) string {
	validatePattern(p)
	if strings.ContainsAny(p, "*") {
		panic(fmt.Sprintf("gogo: Group prefix %q must not contain wildcards", p))
	}
	for len(p) > 1 && p[len(p)-1] == '/' {
		p = p[:len(p)-1]
	}
	if p == "/" {
		return ""
	}
	return p
}

func (r *Router) wrapGroupSync(h Handler) Handler {
	for i := len(r.syncMW) - 1; i >= 0; i-- {
		h = r.syncMW[i](h)
	}
	return h
}

func (r *Router) wrapGroupAsync(h AsyncHandler) AsyncHandler {
	for i := len(r.asyncMW) - 1; i >= 0; i-- {
		h = r.asyncMW[i](h)
	}
	return h
}

// hasGroupOrAppMW reports whether any sync middleware (group or app, global
// or scoped) could touch a route registered through this Router. Used to
// decide whether a static target (Reply / string / []byte) can take the
// zero-cgo static path or must fall back to a dynamic handler so middleware
// can intercept.
func (r *Router) hasGroupOrAppMW(fullPattern string) bool {
	return len(r.syncMW) > 0 || r.app.hasMatchingMiddleware(fullPattern)
}

// Get registers a GET route under this Router. Target follows the same rules
// as App.Get: Handler, func, Reply, string, []byte. Static targets bypass
// middleware only when neither the Router nor the App has any middleware
// touching this route; otherwise the static body is served by a synthetic
// dynamic handler so middleware can intercept.
func (r *Router) Get(pattern string, target any) {
	validatePattern(pattern)
	full := r.prefix + pattern
	switch v := target.(type) {
	case Handler:
		h := r.app.wrap(full, r.wrapGroupSync(v))
		r.app.inner.get(full, h)
	case func(*Response, *Request):
		h := r.app.wrap(full, r.wrapGroupSync(Handler(v)))
		r.app.inner.get(full, h)
	case Reply:
		code := v.Status
		if code == 0 {
			code = 200
		}
		if v.ContentType != "" {
			validateHeaderValue("Content-Type", v.ContentType)
		}
		if !r.hasGroupOrAppMW(full) {
			r.app.inner.getStatic(full, statusLine(code), v.ContentType, v.Body)
			return
		}
		cType, body := v.ContentType, v.Body
		h := r.app.wrap(full, r.wrapGroupSync(func(res *Response, req *Request) {
			res.Send(code, cType, body)
		}))
		r.app.inner.get(full, h)
	case string:
		if !r.hasGroupOrAppMW(full) {
			r.app.inner.getStatic(full, statusLine(200), "", v)
			return
		}
		body := v
		h := r.app.wrap(full, r.wrapGroupSync(func(res *Response, req *Request) {
			res.Send(200, "", body)
		}))
		r.app.inner.get(full, h)
	case []byte:
		body := string(v)
		if !r.hasGroupOrAppMW(full) {
			r.app.inner.getStatic(full, statusLine(200), "", body)
			return
		}
		h := r.app.wrap(full, r.wrapGroupSync(func(res *Response, req *Request) {
			res.Send(200, "", body)
		}))
		r.app.inner.get(full, h)
	default:
		panic(fmt.Sprintf("gogo: unsupported Get target type %T for %q", target, full))
	}
}

// Post registers a POST route under this Router.
func (r *Router) Post(pattern string, handler Handler) {
	validatePattern(pattern)
	full := r.prefix + pattern
	h := r.app.wrap(full, r.wrapGroupSync(handler))
	r.app.inner.post(full, h)
}

// Any registers a route for every HTTP method under this Router.
func (r *Router) Any(pattern string, handler Handler) {
	validatePattern(pattern)
	full := r.prefix + pattern
	h := r.app.wrap(full, r.wrapGroupSync(handler))
	r.app.inner.any(full, h)
}

// GetAsync registers a GET route under this Router that runs on a goroutine.
// Uses the zero-cgo shared-memory dispatch path only when no sync middleware
// (group or app) touches this route.
func (r *Router) GetAsync(pattern string, handler AsyncHandler) {
	validatePattern(pattern)
	full := r.prefix + pattern

	wrappedAsync := r.app.wrapAsync(full, r.wrapGroupAsync(handler))

	if len(r.syncMW) == 0 && !r.app.hasMatchingMiddleware(full) {
		r.app.inner.getShared(full, wrappedAsync)
		return
	}

	syncEntry := func(res *Response, req *Request) {
		snap := req.snapshotFromSync()
		res.Async(func() {
			snapReq := requestPool.Get().(*Request)
			snapReq.snap = snap
			wrappedAsync(res, snapReq)
			snapReq.resetForPool()
			requestPool.Put(snapReq)
		})
	}
	h := r.app.wrap(full, r.wrapGroupSync(Handler(syncEntry)))
	r.app.inner.get(full, h)
}

// PostAsync registers a POST route under this Router that collects the body
// up to maxBodyBytes then runs handler on a goroutine. On bodies over the cap
// the framework sends 413 and the handler is not called.
func (r *Router) PostAsync(pattern string, maxBodyBytes int, handler PostAsyncHandler) {
	validatePattern(pattern)
	full := r.prefix + pattern

	finalAsync := AsyncHandler(func(res *Response, req *Request) {
		handler(res, req, req.body)
	})
	wrappedAsync := r.app.wrapAsync(full, r.wrapGroupAsync(finalAsync))

	syncEntry := func(res *Response, req *Request) {
		snap := req.snapshotFromSync()
		res.Body(maxBodyBytes, func(body []byte, err error) {
			if err == ErrBodyTooLarge {
				res.Send(413, "text/plain; charset=utf-8", "payload too large\n")
				return
			}
			res.Async(func() {
				snapReq := requestPool.Get().(*Request)
				snapReq.snap = snap
				snapReq.body = body
				wrappedAsync(res, snapReq)
				snapReq.resetForPool()
				requestPool.Put(snapReq)
			})
		})
	}
	h := r.app.wrap(full, r.wrapGroupSync(Handler(syncEntry)))
	r.app.inner.post(full, h)
}

// WebSocket registers a WebSocket route under this Router. Middleware does
// not run around WebSocket upgrade — uWS does not expose a chain at the
// upgrade boundary.
func (r *Router) WebSocket(pattern string, behavior WebSocketBehavior) {
	validatePattern(pattern)
	full := r.prefix + pattern
	r.app.inner.websocket(full, behavior)
}

// Listen binds the app to the given port and reports whether binding
// succeeded. The bind interface comes from Config.BindAddr; an empty
// BindAddr keeps the uWS default of all interfaces (0.0.0.0).
func (a *App) Listen(port int) bool {
	return a.inner.listen(a.cfg.BindAddr, port)
}

// Run starts the uWebSockets event loop and blocks. Before running, installs
// the shared-memory drain timer on this loop so SendShared responses can be
// flushed by the loop thread.
func (a *App) Run() {
	a.inner.startSharedDrain(200) // 200μs drain interval
	a.inner.run()
}

// Shutdown initiates a graceful stop: the listen socket is closed so the
// loop stops accepting new connections, and worker goroutines that drain
// the shared request ring see the shutdown flag and exit once the queue
// drains. In-flight requests already accepted finish normally. Safe to
// call from any goroutine; idempotent.
//
// Shutdown does NOT block — call Close after Run returns to free native
// resources.
func (a *App) Shutdown() {
	a.inner.stop()
}

// Close frees native resources. Call it only after Run has returned, or before
// Run if the app was never started.
func (a *App) Close() {
	a.inner.close()
}

// MultiCoreHandle controls a group of App instances started by RunMultiCore.
// Shutdown stops all of them; Wait blocks until every Run loop has exited.
type MultiCoreHandle struct {
	apps []*App
	done chan struct{}
}

// Shutdown initiates graceful stop on every App in the group. Idempotent;
// safe to call from any goroutine.
func (h *MultiCoreHandle) Shutdown() {
	for _, a := range h.apps {
		a.Shutdown()
	}
}

// Wait blocks until every App in the group has exited its Run loop and
// freed native resources. Returns immediately once all loops have finished.
func (h *MultiCoreHandle) Wait() {
	<-h.done
}

// RunMultiCore spawns n independent App instances on dedicated OS threads.
// Each instance binds to the given port — uWS listen sockets enable
// SO_REUSEPORT, so the kernel load-balances incoming connections across
// the App instances. setup is called once per App, on the thread that
// instance will run on, to register routes / middleware / etc.
//
// setup MUST register the same routes on every App for consistent behavior;
// the framework just calls setup(app) and trusts user code to be
// deterministic. Heavy shared state (DB pools, caches) should be created
// ONCE outside RunMultiCore and captured into the handler closures so
// per-App initialization stays cheap.
//
// Returns a MultiCoreHandle that can Shutdown or Wait. Returns an error if
// any App fails to start; in that case already-started Apps are shut down
// before returning.
func RunMultiCore(n int, port int, setup func(app *App)) (*MultiCoreHandle, error) {
	if n <= 0 {
		return nil, fmt.Errorf("gogo: RunMultiCore needs n>0, got %d", n)
	}
	if setup == nil {
		return nil, fmt.Errorf("gogo: RunMultiCore requires a setup function")
	}

	type startResult struct {
		app *App
		err error
	}
	starts := make(chan startResult, n)
	done := make(chan struct{})
	apps := make([]*App, 0, n)

	var runWg sync.WaitGroup
	for i := 0; i < n; i++ {
		runWg.Add(1)
		go func() {
			defer runWg.Done()
			// uWS::Loop is bound to the OS thread that created the App, so
			// every API call against this App must happen on this thread.
			// LockOSThread keeps us pinned for the lifetime of Run.
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			app, err := NewApp()
			if err != nil {
				starts <- startResult{nil, err}
				return
			}
			setup(app)
			if !app.Listen(port) {
				app.Close()
				starts <- startResult{nil, fmt.Errorf("gogo: failed to Listen on :%d", port)}
				return
			}
			starts <- startResult{app, nil}
			app.Run()
			app.Close()
		}()
	}

	// Collect start results.
	for i := 0; i < n; i++ {
		r := <-starts
		if r.err != nil {
			// Shut down any apps that already started, then surface the error.
			for _, a := range apps {
				a.Shutdown()
			}
			runWg.Wait()
			return nil, r.err
		}
		apps = append(apps, r.app)
	}

	// Signal Wait() once every Run loop has exited.
	go func() {
		runWg.Wait()
		close(done)
	}()

	return &MultiCoreHandle{apps: apps, done: done}, nil
}

// Response wraps a uWebSockets response.
type Response struct {
	inner responseNative

	// async is non-nil after Async has been called. Subsequent Status/Header/
	// Write/End calls buffer into it instead of touching the C++ response;
	// End flushes the buffer back onto the loop with a single cork.
	async *asyncState

	// refs is the wrapper's reference count. The wrapper is alive (in use,
	// safe to dereference) while refs > 0. Each holder that may outlive
	// the main HTTP handler — the body-collection callback chain, the
	// Async goroutine, the shared-dispatch worker, etc. — increments refs
	// when it takes ownership and decrements (via releaseRef) when it is
	// done. The decrement that drops refs to zero returns the wrapper to
	// its sync.Pool. This makes the wrapper lifecycle CAS-correct: a late-
	// firing OnData callback and a fast Async goroutine can no longer race
	// each other into double-recycling the same wrapper.
	refs atomic.Int32
}

type asyncState struct {
	loopPtr     uintptr
	ctxHandle   uintptr
	status      string
	contentType string
	body        strings.Builder
	sent        bool
}

// Status sets the HTTP status code. The standard reason phrase from
// net/http is appended automatically (e.g. 200 → "200 OK", 404 → "404 Not
// Found"); unknown codes are written as the bare number.
func (r *Response) Status(code int) *Response {
	line := statusLine(code)
	if r.async != nil {
		r.async.status = line
		return r
	}
	r.inner.status(line)
	return r
}

// Header writes a response header.
//
// In async mode only Content-Type is supported via the fast path. Setting any
// other header from inside Async panics; use Loop.Defer + Cork directly to
// build multi-header responses asynchronously.
func (r *Response) Header(key, value string) *Response {
	validateHeaderValue(key, value)
	if r.async != nil {
		if key != "Content-Type" {
			panic("gogo: Header in async mode only supports Content-Type; use Loop.Defer/Cork for multi-header async responses")
		}
		r.async.contentType = value
		return r
	}
	r.inner.header(key, value)
	return r
}

// Write appends a response chunk without ending the response.
func (r *Response) Write(body string) *Response {
	if r.async != nil {
		r.async.body.WriteString(body)
		return r
	}
	r.inner.write(body)
	return r
}

// End finishes the response.
func (r *Response) End(body string) {
	if r.async != nil {
		r.async.body.WriteString(body)
		r.flushAsync()
		return
	}
	r.inner.end(body)
}

// Send writes status code, an optional Content-Type header, and body in
// one call. The framework picks the lowest-overhead path based on the
// handler's mode:
//
//   - Sync handler (Get / Post / Any): one cgo crossing into uWS.
//   - Async handler (GetAsync / PostAsync) with body up to 8 KB:
//     ZERO cgo per request — written to shared-memory inline buffers and
//     pushed onto the App's response ring; the loop drains it.
//   - Async handler with body > 8 KB: cgo Loop::defer with the full
//     payload, status, and Content-Type.
//
// Pass an empty contentType to omit the header.
func (r *Response) Send(code int, contentType, body string) {
	if contentType != "" {
		validateHeaderValue("Content-Type", contentType)
	}
	line := statusLine(code)

	// Async mode: try the zero-cgo shared-memory path first. Falls back
	// to the cgo defer path when the body exceeds inline capacity.
	if r.async != nil && !r.async.sent {
		if asyncSendShared(r.async.ctxHandle, line, contentType, body) {
			r.async.sent = true
			return
		}
		r.async.status = line
		r.async.contentType = contentType
		r.async.body.WriteString(body)
		r.flushAsync()
		return
	}

	// Sync mode: direct cgo call into uWS.
	r.inner.send(line, contentType, body)
}

// JSON marshals v and sends it with Content-Type: application/json. If
// marshalling fails the response is replaced with a generic 500 and the
// underlying marshal error is reported through the panic handler so the
// programmer sees it server-side without leaking type / package names to
// the network. json.Marshal only fails for unsupported value shapes
// (channels, functions, cyclic structures), so failures here always
// indicate a bug in caller code.
func (r *Response) JSON(code int, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		reportPanic(fmt.Errorf("gogo: JSON marshal: %w", err))
		r.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
		return
	}
	r.Send(code, "application/json", string(data))
}

// Redirect sends an HTTP redirect to location with the given status code.
// Standard codes: 301 (moved permanently), 302 (found / temporary, common
// default), 303 (see other — POST → GET), 307 (temp, preserves method),
// 308 (permanent, preserves method). Status 0 defaults to 302.
//
// The location string is validated against CRLF / NUL injection before
// being written into the Location header.
//
// In async mode this schedules a Cork on the loop so the status, Location
// header, and empty body go out as a single packet; do not call Send /
// End on the same response afterwards.
func (r *Response) Redirect(location string, code int) {
	if code == 0 {
		code = 302
	}
	validateHeaderValue("Location", location)
	line := statusLine(code)
	if r.async != nil && !r.async.sent {
		r.async.sent = true
		inner := r.inner
		loop := r.Loop()
		ctx := r.async.ctxHandle
		loc := location
		loop.Defer(func() {
			defer asyncCtxRelease(ctx)
			inner.cork(func() {
				inner.status(line)
				inner.header("Location", loc)
				inner.end("")
			})
		})
		return
	}
	r.inner.status(line)
	r.inner.header("Location", location)
	r.inner.end("")
}

// Async marks the response for asynchronous handling and runs fn on a new
// goroutine. After calling Async, subsequent Status/Header/Write calls buffer
// Go-side and End flushes the buffered response back onto the event loop with
// a single cork. fn may block freely.
//
// Call Async at most once per response, and before any synchronous
// Status/Header/Write/End calls. The outer handler should return immediately
// after calling Async.
func (r *Response) Async(fn func()) {
	if r.async != nil {
		return
	}
	a := asyncStatePool.Get().(*asyncState)
	a.loopPtr, a.ctxHandle = r.inner.beginAsync()
	a.status = "200 OK"
	a.sent = false
	r.async = a
	r.acquireRef()

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				reportPanic(recovered)
				if !a.sent {
					r.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
				}
			}
			if !a.sent {
				// fn returned without invoking Send/End, or a panic prevented
				// the best-effort 500 from being sent. Drop the response.
				asyncCtxRelease(a.ctxHandle)
			}
			r.finishAsync(a)
		}()
		fn()
	}()
}

func (r *Response) flushAsync() {
	a := r.async
	if a.sent {
		return
	}
	asyncDeferSend(a.loopPtr, a.ctxHandle, a.status, a.contentType, a.body.String())
	a.sent = true
}

// acquireRef adds one to the wrapper's refcount. Callers must pair every
// acquireRef with exactly one releaseRef. Safe to call from any goroutine.
func (r *Response) acquireRef() {
	r.refs.Add(1)
}

// releaseRef drops one ref. The decrement that takes the count to zero is
// the unique recycle point: it returns the wrapper's inner pointer to nil
// and Puts the wrapper back into responsePool. Any later access via a stale
// pointer is the caller's bug (they kept a ref past releaseRef).
func (r *Response) releaseRef() {
	if r.refs.Add(-1) != 0 {
		return
	}
	r.inner = responseNative{}
	r.async = nil
	responsePool.Put(r)
}

// finishAsync returns the asyncState to its pool, then drops one wrapper
// ref. Used by the Async goroutine after fn returns and by runSharedHandler
// once the user-supplied AsyncHandler is done.
func (r *Response) finishAsync(a *asyncState) {
	a.loopPtr = 0
	a.ctxHandle = 0
	a.status = ""
	a.contentType = ""
	a.body.Reset()
	a.sent = false
	asyncStatePool.Put(a)
	r.releaseRef()
}

// Loop returns the event loop that owns this response. Capture it inside the
// handler before spawning a goroutine. The returned Loop is safe to use from
// any goroutine; the Response itself is not.
func (r *Response) Loop() *Loop {
	return &Loop{inner: r.inner.loop()}
}

// OnAborted registers an abort callback and returns an atomic flag that is set
// to true if the client disconnects before the response is sent. Must be called
// synchronously inside the route handler when responding asynchronously.
func (r *Response) OnAborted() *Aborted {
	state := &Aborted{}
	r.inner.onAborted(state)
	return state
}

// Cork batches all response writes inside fn into a single packet. Required
// when sending a response from a deferred callback to avoid uWebSockets warning
// about uncorked writes.
func (r *Response) Cork(fn func()) {
	r.inner.cork(fn)
}

// OnData registers a body-chunk callback. fn is invoked once per chunk uWS
// receives, with isLast == true on the final chunk. Must be called inside
// the route handler, before it returns. The callback runs on the loop thread
// — spawn a goroutine inside it if the work can block.
//
// Each chunk slice is freshly allocated; the caller owns it and may retain
// references after fn returns.
// OnData takes one wrapper ref at registration. The ref is released on
// isLast — at which point the cgo handle for the lambda is also freed.
// Body's onAborted handler releases this ref on early abort (when the last
// chunk would never fire).
// OnData registers a body-chunk callback. Each chunk is freshly allocated;
// the caller owns it. fn runs on the loop thread.
//
// OnData takes one wrapper ref at registration and releases it on the
// final chunk (isLast == true). When the connection aborts before isLast
// fires the ref is held until the response is destroyed; for the
// Body() collector that case is covered explicitly via its own onAborted.
func (r *Response) OnData(fn func(chunk []byte, isLast bool)) {
	r.acquireRef()
	r.inner.onData(func(chunk []byte, isLast bool) {
		fn(chunk, isLast)
		if isLast {
			r.releaseRef()
		}
	})
}

// ErrBodyTooLarge is reported by Body when the request body exceeds the
// caller-supplied max size.
var ErrBodyTooLarge = errFramework("body exceeds max size")

type errFramework string

func (e errFramework) Error() string { return string(e) }

// Body collects the full request body and invokes done once it has arrived.
// If the body exceeds maxBytes, done is called with err = ErrBodyTooLarge and
// the response is closed without sending. Call inside the route handler
// before it returns; done runs on the loop thread (spawn a goroutine for
// blocking work).
func (r *Response) Body(maxBytes int, done func(body []byte, err error)) {
	var buf []byte
	var finished bool
	aborted := &Aborted{}
	// Body takes one wrapper ref that's released exactly once on whichever
	// of these fires first: the final chunk, body-too-large, or abort.
	// Going through r.inner.onData directly (instead of Response.OnData)
	// keeps that release symmetric — Response.OnData would release on
	// isLast on its own and double-release with us on bodyTooLarge/abort.
	r.acquireRef()
	release := func() {
		if finished {
			return
		}
		finished = true
		r.releaseRef()
	}
	r.inner.onAborted(func() {
		aborted.state.Store(true)
		release()
	})
	r.inner.onData(func(chunk []byte, isLast bool) {
		if finished || aborted.Load() {
			return
		}
		if len(buf)+len(chunk) > maxBytes {
			done(nil, ErrBodyTooLarge)
			release()
			return
		}
		if buf == nil && len(chunk) > 0 {
			buf = make([]byte, 0, len(chunk))
		}
		buf = append(buf, chunk...)
		if isLast {
			done(buf, nil)
			release()
		}
	})
}

// Loop is a uWebSockets event loop. Use Defer to schedule work back onto the
// loop thread from any goroutine.
type Loop struct {
	inner loopNative
}

// Defer schedules fn to run on the loop thread. Safe to call from any
// goroutine. fn runs once, in FIFO order with other deferred callbacks.
func (l *Loop) Defer(fn func()) {
	l.inner.defer_(fn)
}

// Aborted is a thread-safe flag set when a client aborts before the response is
// fully sent. Check Load before touching the Response from a deferred callback.
type Aborted struct {
	state atomic.Bool
}

// Load reports whether the response was aborted.
func (a *Aborted) Load() bool {
	return a.state.Load()
}

// Request wraps a uWebSockets request. Sync handlers receive a Request
// backed by the live uWS HttpRequest; async/shared handlers receive a
// Request backed by a snapshot copied into the AsyncCtx before the original
// request was freed.
//
// Both modes expose the same accessors. The shared snapshot has fixed capacity
// per field (URL 256, query 512, params 64 each up to 8, headers 4 KB total);
// requests past those caps are rejected with 431 before reaching user code.
type Request struct {
	inner requestNative
	snap  *requestSnapshot

	// body is the fully collected request body for PostAsync routes, set
	// by the runtime wrapper before invoking the async middleware chain so
	// async middleware can inspect it via Body() and the final user
	// PostAsyncHandler receives it as a parameter.
	body []byte

	// locals carries request-scoped key/value state between middleware and
	// the final handler. Lazy: nil until the first SetLocal. Cleared (but
	// the map is reused) when the Request returns to the pool.
	locals map[string]any

	// sync{Method,URL,Query}{Ptr,Len} and syncParam{Ptrs,Lens} point at
	// uWS-parsed bytes inside the live HttpRequest. The C++ bridge fills
	// them at handler entry so Request's accessors can materialize a Go
	// string on first read without a cgo round-trip. Pointers are valid
	// only for the lifetime of the sync callback; resetForPool clears them
	// before the wrapper returns to the pool.
	//
	// Only the first four route parameters are pre-cached; reads past
	// index 3 fall through to the cgo Parameter helper. Real routes
	// rarely exceed that.
	syncMethodPtr unsafe.Pointer
	syncMethodLen int
	syncURLPtr    unsafe.Pointer
	syncURLLen    int
	syncQueryPtr  unsafe.Pointer
	syncQueryLen  int
	syncParamPtrs [4]unsafe.Pointer
	syncParamLens [4]int

	// syncResPtr is the live uWS response pointer for sync-mode handlers.
	// req.IP() uses it to lazily fetch the peer address via cgo on demand
	// (most handlers don't read IP, so pre-caching would be wasted work).
	// Async / shared snapshots carry the IP in r.snap.ip and don't need
	// this pointer.
	syncResPtr unsafe.Pointer

	// cached{URL,Method,Query,Params,IP} hold the materialized Go strings
	// allocated lazily on the first accessor call. The corresponding
	// *Cached field is true once the cache slot is valid (the empty
	// string is a legitimate cached value).
	cachedURL    string
	urlCached    bool
	cachedMethod string
	methodCached bool
	cachedQuery  string
	queryCached  bool
	cachedParams [4]string
	paramCached  [4]bool
	cachedIP     string
	ipCached     bool
}

// requestSnapshot holds the Go-side captured copy of an HttpRequest, used by
// async and shared-dispatch handlers where uWS has already freed the
// underlying request. Populated either by the worker reading AsyncCtx fields
// (shared path) or by the sync wrapper before spawning a goroutine
// (middleware fallback path).
type requestSnapshot struct {
	method    string
	url       string
	query     string
	ip        string
	params    []string
	truncated bool
	// headers is the raw "name\0value\0name\0value\0..." buffer captured from
	// C++; we parse on access rather than building a map up front so the hot
	// path stays allocation-light when headers aren't read.
	headers []byte
}

// SetLocal stores a request-scoped value under key. Intended for passing
// state from middleware down to the handler (e.g. an authenticated user
// resolved by async middleware). The value lives only as long as the
// request — the map is cleared when the Request returns to its pool.
//
// SetLocal is not safe for concurrent use within a single request; treat
// the Request as owned by whatever goroutine is currently running it.
func (r *Request) SetLocal(key string, value any) {
	if r.locals == nil {
		r.locals = make(map[string]any, 4)
	}
	r.locals[key] = value
}

// Local fetches a value previously stored with SetLocal. Returns nil if
// the key is absent.
func (r *Request) Local(key string) any {
	if r.locals == nil {
		return nil
	}
	return r.locals[key]
}

// Body returns the fully collected request body for PostAsync routes; for
// other routes (Get, Post, GetAsync, Any) it returns nil. The slice is owned
// by the framework — do not retain it past the handler call.
func (r *Request) Body() []byte {
	return r.body
}

// resetForPool clears every request-scoped field so the wrapper can return
// to the sync.Pool without leaking the previous request's data into the
// next user. Keeping the locals map alive avoids re-allocating on the next
// SetLocal — just empty it.
func (r *Request) resetForPool() {
	r.inner = requestNative{}
	r.snap = nil
	r.body = nil
	r.syncMethodPtr = nil
	r.syncMethodLen = 0
	r.syncURLPtr = nil
	r.syncURLLen = 0
	r.syncQueryPtr = nil
	r.syncQueryLen = 0
	r.syncParamPtrs = [4]unsafe.Pointer{}
	r.syncParamLens = [4]int{}
	r.syncResPtr = nil
	r.cachedURL = ""
	r.urlCached = false
	r.cachedMethod = ""
	r.methodCached = false
	r.cachedQuery = ""
	r.queryCached = false
	r.cachedParams = [4]string{}
	r.paramCached = [4]bool{}
	r.cachedIP = ""
	r.ipCached = false
	for k := range r.locals {
		delete(r.locals, k)
	}
}

// URL returns the request URL path. Query string is exposed separately via
// Query(); URL() does not include it.
//
// In sync mode the bridge stashes the URL bytes uWS already parsed onto
// the Request at handler entry, so the first call materializes a Go string
// from those bytes (one allocation, no cgo). Subsequent calls return the
// cached string. Async/shared handlers read from the captured snapshot.
func (r *Request) URL() string {
	if r.snap != nil {
		return r.snap.url
	}
	if r.urlCached {
		return r.cachedURL
	}
	if r.syncURLPtr != nil {
		r.cachedURL = goStringFromC(r.syncURLPtr, r.syncURLLen)
		r.urlCached = true
		return r.cachedURL
	}
	// Fallback: bridge did not pre-fill the URL (shouldn't happen for sync
	// HTTP handlers, but the cgo path remains available for safety).
	s := r.inner.url()
	r.cachedURL = s
	r.urlCached = true
	return s
}

// Method returns the HTTP method ("get", "post", ...). uWS lower-cases it
// during parsing. Sync handlers serve this from the pre-cached method
// pointer the bridge stashed at handler entry; first call allocates a
// Go string, subsequent calls return the cached value — no cgo on the
// hot path.
func (r *Request) Method() string {
	if r.snap != nil {
		return r.snap.method
	}
	if r.methodCached {
		return r.cachedMethod
	}
	if r.syncMethodPtr != nil {
		r.cachedMethod = goStringFromC(r.syncMethodPtr, r.syncMethodLen)
		r.methodCached = true
		return r.cachedMethod
	}
	s := r.inner.method()
	r.cachedMethod = s
	r.methodCached = true
	return s
}

// Header returns a request header value. Header lookups in async/shared
// handlers parse the snapshot buffer on every call; cache the value if you
// need it multiple times.
func (r *Request) Header(name string) string {
	if r.snap != nil {
		return r.snap.lookupHeader(name)
	}
	return r.inner.header(name)
}

// Get is an alias for Header (case-insensitive header lookup). Mirrors the
// req.get(name) helper that fiber / express users reach for first.
func (r *Request) Get(name string) string {
	return r.Header(name)
}

// Hostname returns the host portion of the Host header, with any ":port"
// suffix stripped. Returns "" if the request has no Host header.
func (r *Request) Hostname() string {
	host := r.Header("host")
	if host == "" {
		return ""
	}
	if i := strings.IndexByte(host, ':'); i >= 0 {
		return host[:i]
	}
	return host
}

// Protocol returns "http" or "https" depending on whether the App was
// created via NewApp (plaintext) or a future TLS variant. Until TLS lands
// this always returns "http".
func (r *Request) Protocol() string {
	// TLS support is a separate PR; harded to plaintext for now. Switch
	// to a per-Request flag (set by the bridge for SSLApp) when SSLApp
	// lands so the answer reflects the actual socket type.
	return "http"
}

// Secure reports whether the connection is encrypted (TLS / HTTPS).
// Always false until the SSLApp branch lands.
func (r *Request) Secure() bool {
	return r.Protocol() == "https"
}

// IP returns the formatted peer IP for this connection. For routes
// behind a proxy use IPs() and pick from the X-Forwarded-For chain
// instead — this returns the immediate TCP peer, which will be the
// proxy itself.
//
// Sync handlers lazily cgo into uWS on first read and cache the result
// for subsequent reads. Async / shared handlers serve from the
// snapshot captured at request arrival.
func (r *Request) IP() string {
	if r.ipCached {
		return r.cachedIP
	}
	if r.snap != nil {
		r.cachedIP = r.snap.ip
	} else if r.syncResPtr != nil {
		r.cachedIP = remoteAddrFromPtr(r.syncResPtr)
	}
	r.ipCached = true
	return r.cachedIP
}

// IPs parses the X-Forwarded-For header into a slice of IPs in the order
// the proxies appended them (leftmost = original client). Returns nil if
// the header is absent or empty. Trim trailing whitespace and strip the
// optional port suffix on each entry.
func (r *Request) IPs() []string {
	xff := r.Header("x-forwarded-for")
	if xff == "" {
		return nil
	}
	parts := strings.Split(xff, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// Strip ":port" if present (IPv4-only — IPv6 needs bracket parsing).
		if i := strings.LastIndexByte(p, ':'); i >= 0 && strings.IndexByte(p, '.') >= 0 {
			p = p[:i]
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Parameter returns a route parameter by index. Returns "" for negative or
// out-of-range indices. Snapshot mode caps at 8 parameters; sync mode
// caches indices 0..3 inline (the bridge pre-fills them at handler entry
// — no cgo on first read) and falls back to the cgo getParameter helper
// for indices 4+.
func (r *Request) Parameter(index int) string {
	if index < 0 {
		return ""
	}
	if r.snap != nil {
		if index >= len(r.snap.params) {
			return ""
		}
		return r.snap.params[index]
	}
	if index < 4 {
		if !r.paramCached[index] {
			r.cachedParams[index] = goStringFromC(r.syncParamPtrs[index], r.syncParamLens[index])
			r.paramCached[index] = true
		}
		return r.cachedParams[index]
	}
	return r.inner.parameter(index)
}

// Query returns the raw query string portion of the URL with the leading '?'
// stripped. Returns "" if the request has no query string.
//
// For parsed access, prefer QueryParam(key) for single keys or pass the
// result to net/url.ParseQuery for a full map.
//
// Sync handlers serve from the pre-cached query pointer (no cgo) on first
// call; subsequent calls hit the cache.
func (r *Request) Query() string {
	if r.snap != nil {
		return r.snap.query
	}
	if r.queryCached {
		return r.cachedQuery
	}
	if r.syncQueryPtr != nil {
		r.cachedQuery = goStringFromC(r.syncQueryPtr, r.syncQueryLen)
		r.queryCached = true
		return r.cachedQuery
	}
	s := r.inner.query()
	r.cachedQuery = s
	r.queryCached = true
	return s
}

// QueryParam returns the value of a single query parameter. Returns "" if
// the key is absent. Snapshot mode parses the raw query string on each call;
// cache the result if you need it more than once. Returns "" for an empty key.
func (r *Request) QueryParam(name string) string {
	if name == "" {
		return ""
	}
	if r.snap != nil {
		return parseSingleQueryParam(r.snap.query, name)
	}
	return r.inner.queryParam(name)
}

// Truncated reports whether an async request snapshot exceeded one of gogo's
// fixed capture buffers. The shared fast path rejects truncated requests before
// invoking handlers; this remains useful for diagnostics and future snapshot
// paths.
func (r *Request) Truncated() bool {
	return r.snap != nil && r.snap.truncated
}

// lookupHeader scans the raw "name\0value\0..." buffer for a matching key.
// Header names are case-insensitive (uWS lower-cases on parse).
func (s *requestSnapshot) lookupHeader(name string) string {
	if len(s.headers) == 0 {
		return ""
	}
	// Match against the lower-cased name so callers don't have to.
	needle := strings.ToLower(name)
	buf := s.headers
	for len(buf) > 0 {
		i := indexOfZero(buf)
		if i < 0 {
			return ""
		}
		key := buf[:i]
		buf = buf[i+1:]
		j := indexOfZero(buf)
		if j < 0 {
			return ""
		}
		value := buf[:j]
		buf = buf[j+1:]
		if len(key) == len(needle) && bytesEqualLower(key, needle) {
			return string(value)
		}
	}
	return ""
}

func indexOfZero(b []byte) int {
	for i, c := range b {
		if c == 0 {
			return i
		}
	}
	return -1
}

func bytesEqualLower(b []byte, lower string) bool {
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != lower[i] {
			return false
		}
	}
	return true
}

// snapshotFromSync materializes a snapshot from a sync-mode Request so the
// data survives past the cgo callback's return. The middleware-fallback path
// in GetAsync/PostAsync calls this before spawning the async goroutine.
func (r *Request) snapshotFromSync() *requestSnapshot {
	if r.snap != nil {
		return r.snap
	}
	snap := &requestSnapshot{
		method:  r.inner.method(),
		url:     r.inner.url(),
		query:   r.inner.query(),
		ip:      remoteAddrFromPtr(r.syncResPtr),
		headers: r.inner.headersAll(),
	}
	for i := 0; i < 8; i++ {
		p := r.inner.parameter(i)
		if p == "" {
			break
		}
		snap.params = append(snap.params, p)
	}
	return snap
}

// Cookie returns the value of a named cookie from the Cookie header, or ""
// if the cookie is absent. Linear scan; cache the value if you need it more
// than once. Cookie names are case-sensitive per RFC 6265.
func (r *Request) Cookie(name string) string {
	if name == "" {
		return ""
	}
	header := r.Header("cookie")
	if header == "" {
		return ""
	}
	return parseCookieValue(header, name)
}

// parseCookieValue scans a Cookie header for a name=value pair. Pairs are
// separated by ";" optionally followed by whitespace. Values surrounded by
// a matched pair of ASCII double quotes are unquoted per RFC 6265 §5.2;
// single quotes are not special.
func parseCookieValue(header, name string) string {
	for len(header) > 0 {
		// Skip leading whitespace.
		for len(header) > 0 && (header[0] == ' ' || header[0] == '\t') {
			header = header[1:]
		}
		semi := strings.IndexByte(header, ';')
		var pair string
		if semi < 0 {
			pair = header
			header = ""
		} else {
			pair = header[:semi]
			header = header[semi+1:]
		}
		eq := strings.IndexByte(pair, '=')
		if eq < 0 {
			continue
		}
		if pair[:eq] == name {
			v := pair[eq+1:]
			if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
				v = v[1 : len(v)-1]
			}
			return v
		}
	}
	return ""
}

// SameSite is the value of the SameSite cookie attribute. Empty means the
// attribute is not emitted (browser default applies).
type SameSite string

const (
	SameSiteStrict SameSite = "Strict"
	SameSiteLax    SameSite = "Lax"
	SameSiteNone   SameSite = "None"
)

// Cookie configures a Set-Cookie header. Zero-value fields are omitted —
// browsers fall back to their defaults (session cookie, no Path, etc.).
type Cookie struct {
	Name     string
	Value    string
	Path     string
	Domain   string
	MaxAge   int // seconds; <0 means delete, 0 means session, >0 explicit
	Expires  string
	Secure   bool
	HttpOnly bool
	SameSite SameSite
}

// SetCookie writes a Set-Cookie response header. Name and Value are
// validated against the cookie token grammar (no CTL/separator chars) —
// invalid characters panic, since they typically indicate a programming bug
// rather than user input the framework should silently accept.
//
// Multiple calls append multiple Set-Cookie headers; browsers handle them
// independently.
func (r *Response) SetCookie(c Cookie) {
	if c.Name == "" {
		panic("gogo: SetCookie requires a name")
	}
	validateCookieName(c.Name)
	validateCookieValue(c.Value)

	var b strings.Builder
	b.Grow(len(c.Name) + len(c.Value) + 64)
	b.WriteString(c.Name)
	b.WriteByte('=')
	b.WriteString(c.Value)
	if c.Path != "" {
		b.WriteString("; Path=")
		b.WriteString(c.Path)
	}
	if c.Domain != "" {
		b.WriteString("; Domain=")
		b.WriteString(c.Domain)
	}
	if c.MaxAge > 0 {
		b.WriteString("; Max-Age=")
		b.WriteString(strconv.Itoa(c.MaxAge))
	} else if c.MaxAge < 0 {
		// Negative Max-Age tells the browser to discard immediately.
		b.WriteString("; Max-Age=0")
	}
	if c.Expires != "" {
		b.WriteString("; Expires=")
		b.WriteString(c.Expires)
	}
	if c.Secure {
		b.WriteString("; Secure")
	}
	if c.HttpOnly {
		b.WriteString("; HttpOnly")
	}
	if c.SameSite != "" {
		b.WriteString("; SameSite=")
		b.WriteString(string(c.SameSite))
	}
	r.Header("Set-Cookie", b.String())
}

func validateCookieName(name string) {
	for i := 0; i < len(name); i++ {
		c := name[i]
		// RFC 6265 cookie-name = token; reject CTL, space, separators.
		if c < 0x20 || c == 0x7f {
			panic(fmt.Sprintf("gogo: cookie name %q contains a control character", name))
		}
		switch c {
		case '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=', '{', '}', ' ', '\t':
			panic(fmt.Sprintf("gogo: cookie name %q contains an invalid character %q", name, c))
		}
	}
}

func validateCookieValue(value string) {
	// RFC 6265 cookie-octet excludes CTL, whitespace, '"', ',', ';', '\'.
	// We're strict because CR/LF here would enable response splitting via
	// the Set-Cookie header that Response.Header already guards.
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < 0x20 || c == 0x7f || c == '"' || c == ',' || c == ';' || c == '\\' || c == ' ' {
			panic(fmt.Sprintf("gogo: cookie value contains invalid byte %q at offset %d", c, i))
		}
	}
}

// parseSingleQueryParam mirrors uWS::HttpRequest::getQuery(key): linear scan
// of the form name=value&name=value, no allocations for the search; the value
// returned is a new string copied out of the query buffer.
func parseSingleQueryParam(q, name string) string {
	for len(q) > 0 {
		amp := strings.IndexByte(q, '&')
		var pair string
		if amp < 0 {
			pair = q
			q = ""
		} else {
			pair = q[:amp]
			q = q[amp+1:]
		}
		eq := strings.IndexByte(pair, '=')
		var k, v string
		if eq < 0 {
			k = pair
		} else {
			k = pair[:eq]
			v = pair[eq+1:]
		}
		if k == name {
			return v
		}
	}
	return ""
}

// WebSocket wraps a uWebSockets WebSocket connection.
type WebSocket struct {
	inner websocketNative
}

// Send sends a WebSocket message.
func (ws *WebSocket) Send(message []byte, opcode OpCode) bool {
	return ws.inner.send(message, opcode)
}

// SendText sends a text WebSocket message.
func (ws *WebSocket) SendText(message string) bool {
	return ws.inner.sendString(message, Text)
}

// End closes the WebSocket connection.
func (ws *WebSocket) End(code int, message string) {
	ws.inner.end(code, message)
}
