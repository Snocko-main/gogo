package uwebsockets

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
