package uwebsockets

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// statusLine formats an HTTP status code into the "<code> <reason>" string
// uWS writes verbatim onto the wire. Standard codes get their canonical reason
// phrase via net/http; unknown codes fall back to just the number.
func statusLine(code int) string {
	text := http.StatusText(code)
	if text == "" {
		return strconv.Itoa(code)
	}
	return strconv.Itoa(code) + " " + text
}

var (
	responsePool   = sync.Pool{New: func() any { return &Response{} }}
	requestPool    = sync.Pool{New: func() any { return &Request{} }}
	asyncStatePool = sync.Pool{New: func() any { return &asyncState{} }}
)

// Handler handles a single HTTP request.
//
// The Request and Response values are only valid for the duration of the
// callback. Do not store them or use them from another goroutine.
type Handler func(*Response, *Request)

// AsyncHandler handles a request on a fresh goroutine that is free to block.
// The Response arrives in async mode with the abort context pre-attached, so
// every call site saves one cgo crossing versus a Handler that wraps its body
// in res.Async. The Request is not provided; capture anything needed from it
// from a sync Handler that delegates with res.Async.
type AsyncHandler func(*Response)

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

// App is a uWebSockets HTTP application.
type App struct {
	inner appNative
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

// Get registers a GET route. The target may be:
//
//   - Handler / func(*Response, *Request) — dynamic, invoked per request via cgo
//   - string                              — static body served by C++ (no cgo per request)
//   - []byte                              — same as string
//   - Reply                               — static body with explicit status and Content-Type
//
// Static targets are served entirely in C++ with no Go work per request.
func (a *App) Get(pattern string, target any) {
	switch v := target.(type) {
	case Handler:
		a.inner.get(pattern, v)
	case func(*Response, *Request):
		a.inner.get(pattern, Handler(v))
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
		panic(fmt.Sprintf("uwebsockets: unsupported Get target type %T for %q", target, pattern))
	}
}

// GetAsync registers a GET route whose handler runs on a fresh goroutine and
// receives a Response that is already in async mode. Use this when the handler
// will block (DB call, IO) and does not need to read the request.
func (a *App) GetAsync(pattern string, handler AsyncHandler) {
	a.inner.getAsync(pattern, handler)
}

// Post registers a POST route.
func (a *App) Post(pattern string, handler Handler) {
	a.inner.post(pattern, handler)
}

// Any registers a route for every HTTP method.
func (a *App) Any(pattern string, handler Handler) {
	a.inner.any(pattern, handler)
}

// WebSocket registers a WebSocket route.
func (a *App) WebSocket(pattern string, behavior WebSocketBehavior) {
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

// Close frees native resources. Call it only after Run has returned, or before
// Run if the app was never started.
func (a *App) Close() {
	a.inner.close()
}

// Response wraps a uWebSockets response.
type Response struct {
	inner responseNative

	// async is non-nil after Async has been called. Subsequent Status/Header/
	// Write/End calls buffer into it instead of touching the C++ response;
	// End flushes the buffer back onto the loop with a single cork.
	async *asyncState
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
	if r.async != nil {
		if key != "Content-Type" {
			panic("uwebsockets: Header in async mode only supports Content-Type; use Loop.Defer/Cork for multi-header async responses")
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

// SendShared writes the response into the AsyncCtx's inline buffers and
// enqueues it on the shared lock-free ring for the uWS loop to flush. The
// hot path makes ZERO cgo crossings — pure shared-memory writes plus atomic
// ring push. Body must fit the inline cap (8192 bytes), otherwise the call
// falls back to the cgo Send path. Only valid inside an Async handler.
func (r *Response) SendShared(code int, contentType, body string) {
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

// Request wraps a uWebSockets request.
type Request struct {
	inner requestNative
}

// URL returns the request URL path with query string.
func (r *Request) URL() string {
	return r.inner.url()
}

// Header returns a request header value.
func (r *Request) Header(name string) string {
	return r.inner.header(name)
}

// Parameter returns a route parameter by index.
func (r *Request) Parameter(index int) string {
	return r.inner.parameter(index)
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
