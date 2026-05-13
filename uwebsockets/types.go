package uwebsockets

import "sync/atomic"

// Handler handles a single HTTP request.
//
// The Request and Response values are only valid for the duration of the
// callback. Do not store them or use them from another goroutine.
type Handler func(*Response, *Request)

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

	return &App{inner: inner}, nil
}

// Get registers a GET route.
func (a *App) Get(pattern string, handler Handler) {
	a.inner.get(pattern, handler)
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

// Run starts the uWebSockets event loop and blocks.
func (a *App) Run() {
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
}

// Status sets the HTTP status text, for example "200 OK" or "404 Not Found".
func (r *Response) Status(status string) *Response {
	r.inner.status(status)
	return r
}

// Header writes a response header.
func (r *Response) Header(key, value string) *Response {
	r.inner.header(key, value)
	return r
}

// Write appends a response chunk without ending the response.
func (r *Response) Write(body string) *Response {
	r.inner.write(body)
	return r
}

// End finishes the response.
func (r *Response) End(body string) {
	r.inner.end(body)
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
