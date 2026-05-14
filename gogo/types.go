package gogo

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// PanicHandler is invoked from the worker goroutine when a route handler
// panics, after the worker has emitted a best-effort 500 response. The
// argument is the recovered value (the panic payload). Setting a panic
// handler is optional; without one panics are silently caught.
type PanicHandler func(recovered any)

var (
	panicHandlerMu sync.RWMutex
	panicHandlerFn PanicHandler
)

// SetPanicHandler registers fn as the global panic handler for shared
// workers. Pass nil to clear. The handler must not panic itself.
func SetPanicHandler(fn PanicHandler) {
	panicHandlerMu.Lock()
	panicHandlerFn = fn
	panicHandlerMu.Unlock()
}

func getPanicHandler() PanicHandler {
	panicHandlerMu.RLock()
	defer panicHandlerMu.RUnlock()
	return panicHandlerFn
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

// WebSocketBehavior contains callbacks for a WebSocket route.
type WebSocketBehavior struct {
	Open    func(*WebSocket)
	Message func(*WebSocket, []byte, OpCode)
	Close   func(*WebSocket, int, []byte)
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
// Static replies (Reply, string, []byte targets of App.Get) and GetShared
// routes bypass middleware because they have no Go-side handler to wrap.
type Middleware func(next Handler) Handler

// App is a uWebSockets HTTP application.
type App struct {
	inner       appNative
	middlewares []Middleware
}

// NewApp creates a non-TLS uWebSockets app.
func NewApp() (*App, error) {
	inner, err := newAppNative()
	if err != nil {
		return nil, err
	}
	initSharedLayout()
	return &App{inner: inner}, nil
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
// this call wraps its handler in the current chain at registration time.
// Pass multiple middlewares to apply them in left-to-right order — the
// first argument runs first (outermost). Safe to call multiple times.
func (a *App) Use(mw ...Middleware) {
	a.middlewares = append(a.middlewares, mw...)
}

// wrap composes the registered middleware around h. Outermost-first: the
// first middleware registered runs first, wraps the next, and so on.
func (a *App) wrap(h Handler) Handler {
	for i := len(a.middlewares) - 1; i >= 0; i-- {
		h = a.middlewares[i](h)
	}
	return h
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
		a.inner.get(pattern, a.wrap(v))
	case func(*Response, *Request):
		a.inner.get(pattern, a.wrap(Handler(v)))
	case Reply:
		code := v.Status
		if code == 0 {
			code = 200
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
	if len(a.middlewares) == 0 {
		a.inner.getShared(pattern, handler)
		return
	}
	a.inner.get(pattern, a.wrap(func(res *Response, req *Request) {
		// Capture req fields before the sync wrapper returns — uWS frees the
		// underlying HttpRequest the moment we return from this cgo callback.
		snap := req.snapshotFromSync()
		res.Async(func() {
			snapReq := requestPool.Get().(*Request)
			snapReq.snap = snap
			handler(res, snapReq)
			snapReq.snap = nil
			requestPool.Put(snapReq)
		})
	}))
}

// Post registers a POST route.
func (a *App) Post(pattern string, handler Handler) {
	validatePattern(pattern)
	a.inner.post(pattern, a.wrap(handler))
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
	a.inner.post(pattern, a.wrap(func(res *Response, req *Request) {
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
				handler(res, snapReq, body)
				snapReq.snap = nil
				requestPool.Put(snapReq)
			})
		})
	}))
}

// Any registers a route for every HTTP method.
func (a *App) Any(pattern string, handler Handler) {
	validatePattern(pattern)
	a.inner.any(pattern, a.wrap(handler))
}

// WebSocket registers a WebSocket route.
func (a *App) WebSocket(pattern string, behavior WebSocketBehavior) {
	validatePattern(pattern)
	a.inner.websocket(pattern, behavior)
}

// Listen binds the app to the given port and reports whether binding succeeded.
func (a *App) Listen(port int) bool {
	return a.inner.listen(port)
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

	// bodyPending is true after OnData/Body has been registered but the final
	// chunk hasn't arrived yet. The sync uwsgoHandleHTTP wrapper checks this
	// to keep the wrapper out of the pool — onData fires after the handler
	// has already returned, and recycling here would corrupt the closure.
	bodyPending bool
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

// Send writes status code, an optional Content-Type header, and body in a
// single trip across the C boundary. Pass an empty contentType to omit the
// header. In async mode the values are buffered and flushed together when
// the asynchronous work completes, also in a single cgo call.
func (r *Response) Send(code int, contentType, body string) {
	if contentType != "" {
		validateHeaderValue("Content-Type", contentType)
	}
	line := statusLine(code)
	if r.async != nil {
		r.async.status = line
		r.async.contentType = contentType
		r.async.body.WriteString(body)
		r.flushAsync()
		return
	}
	r.inner.send(line, contentType, body)
}

// JSON marshals v and sends it with Content-Type: application/json. If
// marshalling fails the response is replaced with 500 and the marshal error
// is written as plain text — json.Marshal only fails for unsupported value
// shapes (channels, functions, cyclic structures), which are programming
// errors the caller should fix.
//
// Inside an async handler this uses the same fast path as Send/SendShared;
// pass jsonContentTypeShared to skip the second Send overhead if you've
// already serialized — but for typical use, just call JSON.
func (r *Response) JSON(code int, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		r.Send(500, "text/plain; charset=utf-8", "json marshal error: "+err.Error())
		return
	}
	// Hot path: prefer SendShared when we're inside async mode so the cgo
	// crossing is avoided for response bodies that fit the inline cap.
	if r.async != nil {
		r.SendShared(code, "application/json", string(data))
		return
	}
	r.Send(code, "application/json", string(data))
}

// SendShared writes the response into the AsyncCtx's inline buffers and
// enqueues it on the shared lock-free ring for the uWS loop to flush. The
// hot path makes ZERO cgo crossings — pure shared-memory writes plus atomic
// ring push. Body must fit the inline cap (8192 bytes), otherwise the call
// falls back to the cgo Send path. Only valid inside an Async handler.
func (r *Response) SendShared(code int, contentType, body string) {
	if contentType != "" {
		validateHeaderValue("Content-Type", contentType)
	}
	if r.async == nil || r.async.sent {
		// Fall back to regular Send for sync mode or double-send.
		r.Send(code, contentType, body)
		return
	}
	if asyncSendShared(r.async.ctxHandle, statusLine(code), contentType, body) {
		r.async.sent = true
		return
	}
	// Inline buffers too small — fall back to cgo defer path.
	r.Send(code, contentType, body)
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

	go func() {
		fn()
		if !a.sent {
			// fn returned without invoking Send/End. Drop the response.
			asyncCtxRelease(a.ctxHandle)
		}
		// Recycle the Response wrapper here, AFTER fn returns, so the wrapper
		// isn't reused for another request while our goroutine is still alive.
		r.recycleAsync(a)
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

// recycleAsync resets and returns the asyncState and Response wrappers to
// their pools. Must be called only from the Async goroutine wrapper, after
// fn has returned, to avoid handing out the Response while it is still in use.
func (r *Response) recycleAsync(a *asyncState) {
	r.async = nil
	r.bodyPending = false
	r.inner = responseNative{}

	a.loopPtr = 0
	a.ctxHandle = 0
	a.status = ""
	a.contentType = ""
	a.body.Reset()
	a.sent = false

	asyncStatePool.Put(a)
	responsePool.Put(r)
}

// recycleSync returns a sync-mode Response wrapper to the pool. Called by the
// OnData done callback when the user did not switch to Async mode and the
// uwsgoHandleHTTP path skipped recycling because bodyPending was set.
func (r *Response) recycleSync() {
	r.bodyPending = false
	r.inner = responseNative{}
	responsePool.Put(r)
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
func (r *Response) OnData(fn func(chunk []byte, isLast bool)) {
	r.bodyPending = true
	r.inner.onData(func(chunk []byte, isLast bool) {
		if isLast {
			r.bodyPending = false
		}
		fn(chunk, isLast)
		if isLast && r.async == nil {
			// User did not switch to async mode in the done callback, so the
			// sync wrapper that called OnData has already returned and the
			// pool slot is waiting on us. Return the wrapper now.
			r.recycleSync()
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
	aborted := r.OnAborted()
	r.OnData(func(chunk []byte, isLast bool) {
		if finished || aborted.Load() {
			return
		}
		if len(buf)+len(chunk) > maxBytes {
			finished = true
			done(nil, ErrBodyTooLarge)
			return
		}
		if buf == nil && len(chunk) > 0 {
			buf = make([]byte, 0, len(chunk))
		}
		buf = append(buf, chunk...)
		if isLast {
			finished = true
			done(buf, nil)
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
// Both modes expose the same accessors. The snapshot has fixed capacity
// per field (URL 256, query 512, params 64 each up to 8, headers 4 KB total);
// anything past those caps is silently truncated.
type Request struct {
	inner requestNative
	snap  *requestSnapshot
}

// requestSnapshot holds the Go-side captured copy of an HttpRequest, used by
// async and shared-dispatch handlers where uWS has already freed the
// underlying request. Populated either by the worker reading AsyncCtx fields
// (shared path) or by the sync wrapper before spawning a goroutine
// (middleware fallback path).
type requestSnapshot struct {
	method  string
	url     string
	query   string
	params  []string
	// headers is the raw "name\0value\0name\0value\0..." buffer captured from
	// C++; we parse on access rather than building a map up front so the hot
	// path stays allocation-light when headers aren't read.
	headers []byte
}

// URL returns the request URL path. Query string is exposed separately via
// Query(); URL() does not include it.
func (r *Request) URL() string {
	if r.snap != nil {
		return r.snap.url
	}
	return r.inner.url()
}

// Method returns the HTTP method ("get", "post", ...). uWS lower-cases it
// during parsing.
func (r *Request) Method() string {
	if r.snap != nil {
		return r.snap.method
	}
	return r.inner.method()
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

// Parameter returns a route parameter by index. Returns "" for negative or
// out-of-range indices. Snapshot mode caps at 8 parameters.
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
	return r.inner.parameter(index)
}

// Query returns the raw query string portion of the URL with the leading '?'
// stripped. Returns "" if the request has no query string.
//
// For parsed access, prefer QueryParam(key) for single keys or pass the
// result to net/url.ParseQuery for a full map.
func (r *Request) Query() string {
	if r.snap != nil {
		return r.snap.query
	}
	return r.inner.query()
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
// separated by ";" optionally followed by whitespace.
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
			return pair[eq+1:]
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
