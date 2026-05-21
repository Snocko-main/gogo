// Test helpers — public API for spinning up an App on a free local
// port and driving it from unit tests, plus an adapter that turns
// any net/http.Handler into a gogo.Handler for migration.

package gogo

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// TestServer is a running App bound to a loopback port — built to
// be driven from unit tests. The constructor takes a setup callback
// that registers routes, middleware, and any other per-App state;
// Close stops the server and waits for the loop goroutine to exit.
//
// The server runs on a real port so every cgo path the framework
// takes in production fires the same way under test (middleware
// chains, sync wrapping, snapshot-then-async dispatch, the
// zero-cgo shared-memory ring, …). The trade-off vs an in-process
// dispatcher is per-request latency (~50–100 µs vs ~1 µs) and the
// need for the OS to allocate one port per server — both
// acceptable for typical unit tests.
type TestServer struct {
	app       *App
	port      int
	host      string
	runDone   chan struct{}
	closeOnce sync.Once
	release   func()
	client    *http.Client
}

var testServerMu sync.Mutex

// NewTestServer starts an App on a free port, runs the setup
// callback on the loop's locked goroutine, and waits for the
// listening socket to accept connections before returning. The
// returned TestServer drives the running App via its http.Client;
// Close shuts the App down cleanly.
//
//	ts, err := gogo.NewTestServer(func(app *gogo.App) {
//	    app.Get("/users/:id", showUser)
//	    app.Use(middleware.Logger())
//	})
//	if err != nil { t.Fatal(err) }
//	defer ts.Close()
//
//	req := httptest.NewRequest("GET", "/users/42", nil)
//	resp, err := ts.Do(req)
//	if err != nil { t.Fatal(err) }
//	body, _ := io.ReadAll(resp.Body)
//	// assert body / resp.StatusCode / resp.Header
func NewTestServer(setup func(*App)) (*TestServer, error) {
	if setup == nil {
		return nil, fmt.Errorf("gogo: NewTestServer: setup callback is required")
	}
	// The native binding has process-wide shared worker/ring state. Serialize
	// public test servers so package tests that call t.Parallel don't create
	// multiple native Apps in the same process at once.
	testServerMu.Lock()
	locked := true
	defer func() {
		if locked {
			testServerMu.Unlock()
		}
	}()

	port, err := freeLocalPort()
	if err != nil {
		return nil, fmt.Errorf("gogo: NewTestServer: %w", err)
	}

	ready := make(chan *App, 1)
	listenErr := make(chan error, 1)
	runDone := make(chan struct{})

	go func() {
		// uWS pins each App to the OS thread that created it; the
		// loop goroutine must own the thread for its entire
		// lifetime or the framework's Listen / Run / Close calls
		// will fault.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		var app *App
		defer func() {
			if r := recover(); r != nil {
				if app != nil {
					app.Close()
				}
				listenErr <- fmt.Errorf("setup panic: %v", r)
				close(runDone)
			}
		}()

		var err error
		app, err = NewApp(Config{BindAddr: "127.0.0.1"})
		if err != nil {
			listenErr <- fmt.Errorf("NewApp: %w", err)
			close(runDone)
			return
		}
		setup(app)
		if !app.Listen(port) {
			listenErr <- fmt.Errorf("Listen :%d failed", port)
			app.Close()
			close(runDone)
			return
		}
		ready <- app
		app.Run()
		app.Close()
		close(runDone)
	}()

	var app *App
	select {
	case app = <-ready:
	case err := <-listenErr:
		return nil, err
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("gogo: NewTestServer: setup timed out after 5s")
	}

	host := "127.0.0.1:" + strconv.Itoa(port)
	if err := waitForPortAccept(host, 2*time.Second); err != nil {
		// Best-effort cleanup; the loop goroutine is still
		// running so we have to ask it to stop before returning.
		app.Shutdown()
		<-runDone
		return nil, fmt.Errorf("gogo: NewTestServer: %w", err)
	}

	ts := &TestServer{
		app:     app,
		port:    port,
		host:    host,
		runDone: runDone,
		release: func() {
			testServerMu.Unlock()
		},
		client: &http.Client{
			// Disable keep-alive so Close doesn't have to wait
			// for idle connections to drain. Tests typically
			// fire a handful of requests against each server
			// instance, so the extra TCP handshake per request
			// isn't a concern.
			Transport: &http.Transport{DisableKeepAlives: true},
			Timeout:   10 * time.Second,
		},
	}
	locked = false
	return ts, nil
}

// Port returns the loopback port the server is listening on.
func (ts *TestServer) Port() int { return ts.port }

// URL returns the http://host:port base for this server. Use it to
// build URLs manually when you need control over the path.
func (ts *TestServer) URL() string { return "http://" + ts.host }

// App exposes the underlying *App for setup that the constructor
// callback cannot handle — e.g. publishing to a topic from a Go
// worker goroutine, or installing middleware after the server is
// running.
func (ts *TestServer) App() *App { return ts.app }

// Client returns the *http.Client wired to talk to this server.
// Cookies / redirects / other client-side behaviour can be tuned
// by setting fields on the returned client; the call site shares
// the same client across requests.
func (ts *TestServer) Client() *http.Client { return ts.client }

// Do rewrites the request URL to point at the test server, then
// sends it via the test client. The request method, headers, and
// body are sent unchanged; the URL's scheme and host are
// overwritten and the path / raw query are preserved.
//
// Use this for requests built via httptest.NewRequest (which sets
// the URL to a placeholder host) or any custom *http.Request:
//
//	req := httptest.NewRequest("POST", "/login", strings.NewReader(body))
//	req.Header.Set("Content-Type", "application/json")
//	resp, err := ts.Do(req)
func (ts *TestServer) Do(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = "http"
	req.URL.Host = ts.host
	req.RequestURI = "" // forbidden on client requests
	if req.Host == "" || req.Host == "example.com" {
		// httptest.NewRequest defaults to "example.com" — keep
		// the loop's Host header pointed at the actual server so
		// per-Host routing (if any) sees the right value.
		req.Host = ts.host
	}
	return ts.client.Do(req)
}

// Get is a convenience wrapper for a GET to the given path
// (relative to the server URL).
func (ts *TestServer) Get(path string) (*http.Response, error) {
	return ts.client.Get(ts.URL() + path)
}

// Post is a convenience wrapper for a POST. body may be nil.
func (ts *TestServer) Post(path, contentType string, body io.Reader) (*http.Response, error) {
	return ts.client.Post(ts.URL()+path, contentType, body)
}

// Close shuts the App down gracefully and waits for the loop
// goroutine to return. Safe to call from any goroutine, but only
// the first call does work; subsequent calls are no-ops.
func (ts *TestServer) Close() {
	ts.closeOnce.Do(func() {
		ts.app.Shutdown()
		select {
		case <-ts.runDone:
		case <-time.After(5 * time.Second):
			// The loop didn't exit — best we can do is stop
			// waiting. The leaked goroutine will be reaped when
			// the process exits.
		}
		if ts.release != nil {
			ts.release()
		}
	})
}

// freeLocalPort asks the kernel for a free TCP port on 127.0.0.1
// and immediately releases it. Race window between us closing and
// the App's Listen reopening is small enough that real tests don't
// hit it in practice; pair with waitForPortAccept which retries.
func freeLocalPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
}

// waitForPortAccept dials host every 10 ms until either the
// connection succeeds (server is up) or the timeout expires.
func waitForPortAccept(host string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", host, 50*time.Millisecond)
		if err == nil {
			c.Close()
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("server at %s did not accept connections within %s", host, timeout)
}

// HTTPAdapter wraps a net/http.Handler so it can be registered on a
// gogo route. The adapter:
//
//   - builds a net/http.Request from the gogo.Request (URL,
//     method, headers; body left nil),
//   - runs the handler against a httptest.ResponseRecorder,
//   - copies the recorded status, headers, and body to the
//     gogo.Response via res.Send.
//
// Useful for migrating routes a-handler-at-a-time from a stdlib
// net/http codebase, or for serving stdlib-shaped handlers
// (expvar.Handler, net/http/pprof.Handler, …) under gogo.
//
//	app.Get("/debug/vars", gogo.HTTPAdapter(expvar.Handler()))
//	app.Get("/debug/pprof/*", gogo.HTTPAdapter(http.HandlerFunc(pprof.Index)))
//
// Body access: gogo's Get path does not collect request bodies, so
// the wrapped handler sees an empty body. For methods that carry
// payloads register via PostAsync and call HTTPAdapter from
// inside a wrapper that builds the http.Request with the
// collected body:
//
//	app.PostAsync("/upload", 8<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
//	    httpReq := httptest.NewRequest("POST", req.URL(), bytes.NewReader(body))
//	    copyHeadersIntoHTTPReq(httpReq, req)
//	    rec := httptest.NewRecorder()
//	    legacyHandler.ServeHTTP(rec, httpReq)
//	    res.Header("Content-Type", rec.Header().Get("Content-Type"))
//	    res.Send(rec.Code, "", rec.Body.String())
//	})
func HTTPAdapter(h http.Handler) Handler {
	if h == nil {
		panic("gogo: HTTPAdapter: handler is nil")
	}
	return func(res *Response, req *Request) {
		httpReq, err := buildAdapterRequest(req, nil)
		if err != nil {
			reportPanic(fmt.Errorf("gogo: HTTPAdapter: build request: %w", err))
			res.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
			return
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httpReq)
		flushAdapterRecorder(res, rec)
	}
}

// HTTPAdapterWithBody is the variant that ships the collected body
// into the wrapped handler. Use it from PostAsync (or any path
// where you have access to the full request body):
//
//	app.PostAsync("/legacy", 8<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
//	    gogo.HTTPAdapterWithBody(legacyHandler, body)(res, req)
//	})
//
// The returned Handler is a closure that captures body; do not
// reuse it across requests.
func HTTPAdapterWithBody(h http.Handler, body []byte) Handler {
	if h == nil {
		panic("gogo: HTTPAdapterWithBody: handler is nil")
	}
	return func(res *Response, req *Request) {
		httpReq, err := buildAdapterRequest(req, body)
		if err != nil {
			reportPanic(fmt.Errorf("gogo: HTTPAdapterWithBody: build request: %w", err))
			res.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
			return
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httpReq)
		flushAdapterRecorder(res, rec)
	}
}

// buildAdapterRequest constructs a net/http.Request that mirrors
// the gogo.Request's method / URL / headers, optionally carrying a
// body buffer for adapter variants that have one.
func buildAdapterRequest(req *Request, body []byte) (*http.Request, error) {
	method := req.Method()
	if method == "" {
		method = "GET"
	}
	// Upper-case the method — gogo stores it lower-case per uWS
	// convention but stdlib net/http expects upper-case strings.
	method = upperASCII(method)

	url := req.URL()
	if q := req.Query(); q != "" {
		url += "?" + q
	}

	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}
	httpReq, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	// uWS does not retain a parsed header map for us, but the
	// snapshot path packs every header into a name\0value\0 blob
	// — copy through that so middleware-style adapters (which
	// often inspect Auth headers, Cookies, etc.) see what the
	// real client sent.
	copyHeadersFromRequest(httpReq.Header, req)
	if httpReq.Header.Get("Content-Length") == "" && len(body) > 0 {
		httpReq.Header.Set("Content-Length", strconv.Itoa(len(body)))
	}
	httpReq.RemoteAddr = req.IP()
	return httpReq, nil
}

// flushAdapterRecorder copies the recorded status, headers, and
// body from a httptest.ResponseRecorder onto the gogo.Response.
func flushAdapterRecorder(res *Response, rec *httptest.ResponseRecorder) {
	contentType := rec.Header().Get("Content-Type")
	for key, values := range rec.Header() {
		if key == "Content-Type" || key == "Content-Length" {
			continue
		}
		for _, v := range values {
			res.Header(key, v)
		}
	}
	code := rec.Code
	if code == 0 {
		code = 200
	}
	res.Send(code, contentType, rec.Body.String())
}

// copyHeadersFromRequest packs every header on req into the
// destination http.Header map. The snapshot path stores headers as
// a NUL-separated blob; sync mode uses a different read path but
// Request.Headers abstracts both without one lookup per known name.
func copyHeadersFromRequest(dst http.Header, req *Request) {
	req.Headers(func(name, value string) bool {
		dst.Set(canonicalHeaderName(name), value)
		return true
	})
}

// upperASCII upper-cases a method string without touching non-ASCII
// bytes. uWS stores methods lower-case ("get", "post", …);
// stdlib's net/http.NewRequest expects upper-case strings.
func upperASCII(s string) string {
	var hasLower bool
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			hasLower = true
			break
		}
	}
	if !hasLower {
		return s
	}
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

// canonicalHeaderName mirrors net/textproto.CanonicalMIMEHeaderKey
// without the package dependency — Title-Case-With-Dashes. Used so
// stdlib handlers see header keys in the expected shape.
func canonicalHeaderName(s string) string {
	b := make([]byte, len(s))
	upper := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if upper {
			if c >= 'a' && c <= 'z' {
				c -= 'a' - 'A'
			}
		} else {
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
		}
		b[i] = c
		upper = c == '-'
	}
	return string(b)
}
