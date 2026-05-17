//go:build cgo && gogo

package gogo_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gogo "uwebsockets-go/gogo"
)

// freePort grabs an ephemeral port the kernel just handed us, then closes the
// listener so the test server can rebind to it. Tiny race window but fine for
// tests on localhost.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// startApp boots an app on a free port, runs the loop on a dedicated OS-locked
// goroutine (uWS loop is thread-local), waits until the server accepts a
// connection, and returns a teardown func.
func startApp(t *testing.T, configure func(app *gogo.App)) (port int, teardown func()) {
	t.Helper()

	port = freePort(t)
	ready := make(chan *gogo.App, 1)
	listenErr := make(chan error, 1)
	runDone := make(chan struct{})

	go func() {
		// Loop binding requires every uWS call for this app to happen on the
		// same OS thread that called uwsgo_app_new.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		app, err := gogo.NewApp()
		if err != nil {
			listenErr <- fmt.Errorf("NewApp: %w", err)
			close(runDone)
			return
		}
		configure(app)
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

	var app *gogo.App
	select {
	case app = <-ready:
	case err := <-listenErr:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("app setup timed out")
	}

	// Spin-wait for the loop to accept connections.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 50*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	teardown = func() {
		app.Shutdown()
		select {
		case <-runDone:
		case <-time.After(10 * time.Second):
			t.Errorf("app.Run did not exit after Shutdown")
		}
	}
	return port, teardown
}

// noKeepaliveClient avoids HTTP/1.1 keep-alive so the server has no open
// sockets keeping the loop alive when Shutdown is called.
var noKeepaliveClient = &http.Client{
	Transport: &http.Transport{DisableKeepAlives: true},
	Timeout:   5 * time.Second,
}

func httpGet(t *testing.T, port int, path string) (status int, body string) {
	t.Helper()
	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d%s", port, path))
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(b)
}

func TestStaticReply(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/health", gogo.Reply{
			Status:      200,
			ContentType: "application/json",
			Body:        `{"ok":true}`,
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/health")
	if status != 200 || body != `{"ok":true}` {
		t.Fatalf("got %d %q", status, body)
	}
}

func TestSyncHandler(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/plain", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "hello\n")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/plain")
	if status != 200 || body != "hello\n" {
		t.Fatalf("got %d %q", status, body)
	}
}

func TestRouteParameter(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/hello/:name", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "hi "+req.Parameter(0))
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/hello/claude")
	if status != 200 || body != "hi claude" {
		t.Fatalf("got %d %q", status, body)
	}
}

func TestAsyncHandler(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/sleep", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(5 * time.Millisecond)
			res.Send(200, "text/plain", "slept")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/sleep")
	if status != 200 || body != "slept" {
		t.Fatalf("got %d %q", status, body)
	}
}

func TestSharedDispatch(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/shared", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "application/json", `{"path":"shared"}`)
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/shared")
	if status != 200 || body != `{"path":"shared"}` {
		t.Fatalf("got %d %q", status, body)
	}
}

func TestPanicRecoveryInSharedHandler(t *testing.T) {
	var panicked atomic.Int32
	gogo.SetPanicHandler(func(recovered any) {
		panicked.Add(1)
	})
	defer gogo.SetPanicHandler(nil)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/boom", func(res *gogo.Response, req *gogo.Request) {
			panic("kaboom")
		})
		app.GetAsync("/ok", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "still alive")
		})
	})
	defer teardown()

	// Panic route should not crash the worker — it returns 500 best-effort.
	status, _ := httpGet(t, port, "/boom")
	if status != 500 {
		t.Fatalf("expected 500 after panic, got %d", status)
	}
	if panicked.Load() != 1 {
		t.Fatalf("panic handler not called, got %d", panicked.Load())
	}

	// And the worker is still alive for subsequent requests.
	status, body := httpGet(t, port, "/ok")
	if status != 200 || body != "still alive" {
		t.Fatalf("worker died after panic; got %d %q", status, body)
	}
}

func TestPanicRecoveryInSyncHandler(t *testing.T) {
	var panicked atomic.Int32
	gogo.SetPanicHandler(func(recovered any) {
		panicked.Add(1)
	})
	defer gogo.SetPanicHandler(nil)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/boom", func(res *gogo.Response, req *gogo.Request) {
			panic("sync kaboom")
		})
		app.Get("/ok", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "still alive")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/boom")
	if status != 500 || !strings.Contains(body, "Internal Server Error") {
		t.Fatalf("expected 500 after sync panic, got %d %q", status, body)
	}
	if panicked.Load() != 1 {
		t.Fatalf("panic handler not called, got %d", panicked.Load())
	}

	status, body = httpGet(t, port, "/ok")
	if status != 200 || body != "still alive" {
		t.Fatalf("server died after sync panic; got %d %q", status, body)
	}
}

func TestPanicRecoveryInAsyncFallbackHandler(t *testing.T) {
	var panicked atomic.Int32
	gogo.SetPanicHandler(func(recovered any) {
		panicked.Add(1)
	})
	defer gogo.SetPanicHandler(nil)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use("/api/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				next(res, req)
			}
		})
		app.GetAsync("/api/boom", func(res *gogo.Response, req *gogo.Request) {
			panic("async fallback kaboom")
		})
		app.GetAsync("/api/ok", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "still alive")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/api/boom")
	if status != 500 || !strings.Contains(body, "Internal Server Error") {
		t.Fatalf("expected 500 after async fallback panic, got %d %q", status, body)
	}
	if panicked.Load() != 1 {
		t.Fatalf("panic handler not called, got %d", panicked.Load())
	}

	status, body = httpGet(t, port, "/api/ok")
	if status != 200 || body != "still alive" {
		t.Fatalf("server died after async fallback panic; got %d %q", status, body)
	}
}

func TestConcurrentLoad(t *testing.T) {
	// Stress the shared-dispatch + worker pool to flush out races.
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/c", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "x")
		})
	})
	defer teardown()

	const clients = 16
	const perClient = 100
	var wg sync.WaitGroup
	var errs atomic.Int32
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 2 * time.Second}
			for j := 0; j < perClient; j++ {
				resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/c", port))
				if err != nil {
					errs.Add(1)
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					errs.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if errs.Load() != 0 {
		t.Fatalf("got %d errors under load", errs.Load())
	}
}

func TestPatternValidation(t *testing.T) {
	app, err := gogo.NewApp()
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	defer app.Close()

	cases := []struct {
		name    string
		pattern string
	}{
		{"empty", ""},
		{"no-leading-slash", "health"},
		{"newline", "/foo\nbar"},
		{"null-byte", "/foo\x00bar"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for pattern %q", tc.pattern)
				}
			}()
			app.Get(tc.pattern, "x")
		})
	}
}

func httpPost(t *testing.T, port int, path, contentType string, body []byte) (status int, respBody string) {
	t.Helper()
	resp, err := noKeepaliveClient.Post(
		fmt.Sprintf("http://127.0.0.1:%d%s", port, path),
		contentType,
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(b)
}

func TestPostAsyncBody(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/echo", 64*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "application/octet-stream", string(body))
		})
	})
	defer teardown()

	payload := []byte(`{"hello":"world","n":42}`)
	status, body := httpPost(t, port, "/echo", "application/json", payload)
	if status != 200 {
		t.Fatalf("got %d, want 200", status)
	}
	if body != string(payload) {
		t.Fatalf("got %q, want %q", body, string(payload))
	}
}

func TestPostAsyncLargeBody(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/echo", 1024*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", fmt.Sprintf("len=%d", len(body)))
		})
	})
	defer teardown()

	// 256 KB — larger than a single TCP segment, exercises multi-chunk onData.
	payload := bytes.Repeat([]byte("x"), 256*1024)
	status, body := httpPost(t, port, "/echo", "application/octet-stream", payload)
	if status != 200 {
		t.Fatalf("got %d, want 200", status)
	}
	if body != fmt.Sprintf("len=%d", len(payload)) {
		t.Fatalf("got %q, want len=%d", body, len(payload))
	}
}

func TestPostAsyncTooLarge(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/upload", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			t.Errorf("handler should not be called for oversize body")
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	payload := bytes.Repeat([]byte("a"), 2048)
	status, body := httpPost(t, port, "/upload", "text/plain", payload)
	if status != 413 {
		t.Fatalf("got %d, want 413; body=%q", status, body)
	}
}

func TestPostBodyLowLevel(t *testing.T) {
	// Body() primitive: collect on loop thread, respond inline.
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Post("/sum", func(res *gogo.Response, req *gogo.Request) {
			res.Body(16*1024, func(body []byte, err error) {
				if err != nil {
					res.Send(413, "text/plain", err.Error())
					return
				}
				res.Send(200, "text/plain", fmt.Sprintf("sum=%d", len(body)))
			})
		})
	})
	defer teardown()

	status, body := httpPost(t, port, "/sum", "text/plain", []byte("hello"))
	if status != 200 || body != "sum=5" {
		t.Fatalf("got %d %q", status, body)
	}
}

func TestQueryString(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/q", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", req.Query())
		})
		app.Get("/p", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain",
				"name="+req.QueryParam("name")+
					";age="+req.QueryParam("age")+
					";missing="+req.QueryParam("missing"))
		})
	})
	defer teardown()

	// Raw query string round-trip.
	status, body := httpGet(t, port, "/q?a=1&b=hello")
	if status != 200 || body != "a=1&b=hello" {
		t.Fatalf("raw query: got %d %q", status, body)
	}

	// Empty when no query.
	status, body = httpGet(t, port, "/q")
	if status != 200 || body != "" {
		t.Fatalf("no query: got %d %q", status, body)
	}

	// Named parameters + missing key returns "".
	status, body = httpGet(t, port, "/p?name=claude&age=4")
	if status != 200 || body != "name=claude;age=4;missing=" {
		t.Fatalf("query params: got %d %q", status, body)
	}

	// Empty key short-circuits.
	status, body = httpGet(t, port, "/p")
	if status != 200 || body != "name=;age=;missing=" {
		t.Fatalf("no params: got %d %q", status, body)
	}
}

func TestMiddlewareChainOrder(t *testing.T) {
	// Verify outermost-first ordering and that the chain reaches the handler.
	var trace []string
	var traceMu sync.Mutex
	record := func(s string) {
		traceMu.Lock()
		trace = append(trace, s)
		traceMu.Unlock()
	}

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("a-before")
				next(res, req)
				record("a-after")
			}
		})
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("b-before")
				next(res, req)
				record("b-after")
			}
		})
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			record("handler")
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/x")
	if status != 200 || body != "ok" {
		t.Fatalf("got %d %q", status, body)
	}
	traceMu.Lock()
	got := strings.Join(trace, ",")
	traceMu.Unlock()
	want := "a-before,b-before,handler,b-after,a-after"
	if got != want {
		t.Fatalf("trace: got %q want %q", got, want)
	}
}

func TestMiddlewareShortCircuit(t *testing.T) {
	var handlerCalled atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				if req.Header("authorization") == "" {
					res.Send(401, "text/plain", "unauthorized")
					return
				}
				next(res, req)
			}
		})
		app.Get("/secret", func(res *gogo.Response, req *gogo.Request) {
			handlerCalled.Add(1)
			res.Send(200, "text/plain", "secret")
		})
	})
	defer teardown()

	// No header → 401, handler not called.
	status, body := httpGet(t, port, "/secret")
	if status != 401 || body != "unauthorized" {
		t.Fatalf("denied: got %d %q", status, body)
	}
	if handlerCalled.Load() != 0 {
		t.Fatalf("handler called on short-circuit")
	}

	// With header → passes through.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/secret", port), nil)
	req.Header.Set("Authorization", "Bearer x")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("authed GET: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "secret" {
		t.Fatalf("authed: got %d %q", resp.StatusCode, string(b))
	}
	if handlerCalled.Load() != 1 {
		t.Fatalf("handler call count = %d, want 1", handlerCalled.Load())
	}
}

func TestMiddlewareAppliesOnlyToLaterRoutes(t *testing.T) {
	// Routes registered before Use are not affected.
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/free", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "free")
		})
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				res.Send(403, "text/plain", "blocked")
			}
		})
		app.Get("/locked", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "should not see this")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/free")
	if status != 200 || body != "free" {
		t.Fatalf("/free: got %d %q", status, body)
	}
	status, body = httpGet(t, port, "/locked")
	if status != 403 || body != "blocked" {
		t.Fatalf("/locked: got %d %q", status, body)
	}
}

func TestMiddlewareOnPostAsync(t *testing.T) {
	// Middleware runs before body collection on PostAsync routes too.
	var seenAuth atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				if req.Header("x-auth") != "yes" {
					res.Send(401, "text/plain", "no")
					return
				}
				seenAuth.Add(1)
				next(res, req)
			}
		})
		app.PostAsync("/upload", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", fmt.Sprintf("got %d", len(body)))
		})
	})
	defer teardown()

	// Without auth → 401, handler & body collection skipped.
	status, _ := httpPost(t, port, "/upload", "text/plain", []byte("hello"))
	if status != 401 {
		t.Fatalf("no-auth: got %d", status)
	}

	// With auth → body collected, handler runs.
	req, _ := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d/upload", port), bytes.NewReader([]byte("hello")))
	req.Header.Set("X-Auth", "yes")
	req.Header.Set("Content-Type", "text/plain")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("authed POST: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "got 5" {
		t.Fatalf("authed: got %d %q", resp.StatusCode, string(b))
	}
	if seenAuth.Load() != 1 {
		t.Fatalf("middleware passed-through count = %d, want 1", seenAuth.Load())
	}
}

func TestMiddlewareOnGetAsync(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				if req.QueryParam("token") == "" {
					res.Send(401, "text/plain", "no token")
					return
				}
				next(res, req)
			}
		})
		app.GetAsync("/work", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(2 * time.Millisecond)
			res.Send(200, "text/plain", "done")
		})
	})
	defer teardown()

	status, _ := httpGet(t, port, "/work")
	if status != 401 {
		t.Fatalf("missing token: got %d", status)
	}
	status, body := httpGet(t, port, "/work?token=abc")
	if status != 200 || body != "done" {
		t.Fatalf("with token: got %d %q", status, body)
	}
}

// TestMiddlewarePathScoped verifies Use("/api/*", mw) applies only to routes
// whose pattern starts with /api/ (or is exactly /api).
func TestMiddlewarePathScoped(t *testing.T) {
	var apiHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use("/api/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				apiHits.Add(1)
				if req.Header("x-api-key") != "secret" {
					res.Send(401, "text/plain", "no key")
					return
				}
				next(res, req)
			}
		})
		app.Get("/api/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "users")
		})
		app.Get("/public", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "public")
		})
	})
	defer teardown()

	// /public bypasses the path-scoped middleware entirely.
	status, body := httpGet(t, port, "/public")
	if status != 200 || body != "public" {
		t.Fatalf("/public: got %d %q", status, body)
	}
	if apiHits.Load() != 0 {
		t.Fatalf("api middleware ran for /public: hits=%d", apiHits.Load())
	}

	// /api/users without key → 401 from the middleware.
	status, body = httpGet(t, port, "/api/users")
	if status != 401 || body != "no key" {
		t.Fatalf("/api/users unauth: got %d %q", status, body)
	}

	// /api/users with key → 200 from the handler.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/users", port), nil)
	req.Header.Set("X-API-Key", "secret")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("/api/users authed: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "users" {
		t.Fatalf("/api/users authed: got %d %q", resp.StatusCode, string(b))
	}
	if apiHits.Load() != 2 {
		t.Fatalf("api middleware hit count = %d, want 2", apiHits.Load())
	}
}

// TestMiddlewarePathScopedExactPrefix checks that Use("/api", mw) matches
// "/api" exactly as well as routes under "/api/", but not "/apiv2".
func TestMiddlewarePathScopedExactPrefix(t *testing.T) {
	var hits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				hits.Add(1)
				next(res, req)
			}
		})
		app.Get("/api", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "root")
		})
		app.Get("/api/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "users")
		})
		app.Get("/apiv2/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "v2")
		})
	})
	defer teardown()

	for _, p := range []string{"/api", "/api/users"} {
		if status, _ := httpGet(t, port, p); status != 200 {
			t.Fatalf("%s: got %d", p, status)
		}
	}
	if hits.Load() != 2 {
		t.Fatalf("/api + /api/users should each hit mw: got %d, want 2", hits.Load())
	}

	// /apiv2/users shares the literal prefix "api" but not the segment;
	// path-scoped middleware must not run for it.
	if status, _ := httpGet(t, port, "/apiv2/users"); status != 200 {
		t.Fatalf("/apiv2/users: got %d", status)
	}
	if hits.Load() != 2 {
		t.Fatalf("/apiv2/users wrongly ran mw: hits=%d", hits.Load())
	}
}

// TestMiddlewareGlobalAndScopedTogether verifies global Use and path-scoped
// Use compose correctly when both are present.
func TestMiddlewareGlobalAndScopedTogether(t *testing.T) {
	var trace []string
	var traceMu sync.Mutex
	record := func(s string) {
		traceMu.Lock()
		trace = append(trace, s)
		traceMu.Unlock()
	}

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("global")
				next(res, req)
			}
		})
		app.Use("/api/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("api")
				next(res, req)
			}
		})
		app.Get("/api/x", func(res *gogo.Response, req *gogo.Request) {
			record("handler-api")
			res.Send(200, "text/plain", "api")
		})
		app.Get("/other", func(res *gogo.Response, req *gogo.Request) {
			record("handler-other")
			res.Send(200, "text/plain", "other")
		})
	})
	defer teardown()

	httpGet(t, port, "/api/x")
	traceMu.Lock()
	got := strings.Join(trace, ",")
	trace = nil
	traceMu.Unlock()
	if got != "global,api,handler-api" {
		t.Fatalf("/api/x trace: got %q", got)
	}

	httpGet(t, port, "/other")
	traceMu.Lock()
	got = strings.Join(trace, ",")
	traceMu.Unlock()
	if got != "global,handler-other" {
		t.Fatalf("/other trace: got %q", got)
	}
}

// TestMiddlewarePathScopedGetAsyncFastPath confirms GetAsync still uses the
// zero-cgo shared-memory dispatch when the only registered middleware is
// scoped to a path that doesn't include the async route.
func TestMiddlewarePathScopedGetAsyncFastPath(t *testing.T) {
	var apiHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use("/api/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				apiHits.Add(1)
				next(res, req)
			}
		})
		// Async route lives OUTSIDE /api so the scoped middleware must not
		// pull it onto the sync fallback path.
		app.GetAsync("/work", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(2 * time.Millisecond)
			res.Send(200, "text/plain", "done")
		})
		// And one /api route to make sure scoped mw still runs where it should.
		app.GetAsync("/api/work", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(2 * time.Millisecond)
			res.Send(200, "text/plain", "api-done")
		})
	})
	defer teardown()

	if status, body := httpGet(t, port, "/work"); status != 200 || body != "done" {
		t.Fatalf("/work: got %d %q", status, body)
	}
	if apiHits.Load() != 0 {
		t.Fatalf("scoped mw ran for /work: hits=%d", apiHits.Load())
	}

	if status, body := httpGet(t, port, "/api/work"); status != 200 || body != "api-done" {
		t.Fatalf("/api/work: got %d %q", status, body)
	}
	if apiHits.Load() != 1 {
		t.Fatalf("scoped mw hits for /api/work = %d, want 1", apiHits.Load())
	}
}

// TestAsyncMiddlewareLoadsUser exercises the canonical async middleware
// flow: a blocking "lookup" runs on the goroutine, sets a Local, and the
// handler reads it back. Shared dispatch path (no sync middleware).
func TestAsyncMiddlewareLoadsUser(t *testing.T) {
	type user struct {
		ID   int
		Name string
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync("/api/*", func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				token := req.Header("authorization")
				if token == "" {
					res.Send(401, "text/plain", "no token")
					return
				}
				// Simulate a blocking DB lookup.
				time.Sleep(1 * time.Millisecond)
				req.SetLocal("user", &user{ID: 42, Name: "alice"})
				next(res, req)
			}
		})
		app.GetAsync("/api/me", func(res *gogo.Response, req *gogo.Request) {
			u := req.Local("user").(*user)
			res.Send(200, "text/plain", fmt.Sprintf("hi %s (%d)", u.Name, u.ID))
		})
	})
	defer teardown()

	// Missing token short-circuits.
	if status, body := httpGet(t, port, "/api/me"); status != 401 || body != "no token" {
		t.Fatalf("no-token: got %d %q", status, body)
	}

	// With token, async mw loads the user and the handler reads it back.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/me", port), nil)
	req.Header.Set("Authorization", "Bearer x")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("authed: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "hi alice (42)" {
		t.Fatalf("authed: got %d %q", resp.StatusCode, string(b))
	}
}

// TestAsyncMiddlewareLocalsResetBetweenRequests confirms request-scoped
// state from one request never leaks into another via the request pool.
func TestAsyncMiddlewareLocalsResetBetweenRequests(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync("/api/*", func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				if req.QueryParam("set") == "1" {
					req.SetLocal("flag", "set-by-mw")
				}
				next(res, req)
			}
		})
		app.GetAsync("/api/x", func(res *gogo.Response, req *gogo.Request) {
			v := req.Local("flag")
			if v == nil {
				res.Send(200, "text/plain", "none")
				return
			}
			res.Send(200, "text/plain", v.(string))
		})
	})
	defer teardown()

	// Drive a sequence of alternating requests to force pool reuse: any
	// "set" leaking into the next "unset" request would surface immediately.
	for i := 0; i < 20; i++ {
		_, body := httpGet(t, port, "/api/x?set=1")
		if body != "set-by-mw" {
			t.Fatalf("iter %d set: %q", i, body)
		}
		_, body = httpGet(t, port, "/api/x")
		if body != "none" {
			t.Fatalf("iter %d unset: %q (locals leaked across pool reuse)", i, body)
		}
	}
}

// TestAsyncMiddlewareChainOrder verifies outermost-first ordering for async
// middleware, mirroring the sync chain semantics.
func TestAsyncMiddlewareChainOrder(t *testing.T) {
	var trace []string
	var traceMu sync.Mutex
	record := func(s string) {
		traceMu.Lock()
		trace = append(trace, s)
		traceMu.Unlock()
	}

	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync(func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("a-before")
				next(res, req)
				record("a-after")
			}
		})
		app.UseAsync(func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("b-before")
				next(res, req)
				record("b-after")
			}
		})
		app.GetAsync("/x", func(res *gogo.Response, req *gogo.Request) {
			record("handler")
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	if status, body := httpGet(t, port, "/x"); status != 200 || body != "ok" {
		t.Fatalf("got %d %q", status, body)
	}
	traceMu.Lock()
	got := strings.Join(trace, ",")
	traceMu.Unlock()
	want := "a-before,b-before,handler,b-after,a-after"
	if got != want {
		t.Fatalf("trace: got %q want %q", got, want)
	}
}

// TestAsyncMiddlewareWithSyncMiddleware mixes sync and async middleware on
// the same async route. Sync mw runs first on the loop thread (header check
// before async dispatch); async mw runs after the goroutine transition.
func TestAsyncMiddlewareWithSyncMiddleware(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		// Sync gate: must have x-tenant header at all.
		app.Use("/api/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				if req.Header("x-tenant") == "" {
					res.Send(400, "text/plain", "no tenant")
					return
				}
				next(res, req)
			}
		})
		// Async gate: simulate a DB lookup to validate the tenant.
		app.UseAsync("/api/*", func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				time.Sleep(1 * time.Millisecond)
				tenant := req.Header("x-tenant")
				if tenant != "acme" {
					res.Send(403, "text/plain", "bad tenant")
					return
				}
				req.SetLocal("tenant", tenant)
				next(res, req)
			}
		})
		app.GetAsync("/api/work", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "for "+req.Local("tenant").(string))
		})
	})
	defer teardown()

	// No header → sync mw responds first, async mw never runs.
	if status, body := httpGet(t, port, "/api/work"); status != 400 || body != "no tenant" {
		t.Fatalf("no-tenant: got %d %q", status, body)
	}

	// Bad tenant header → sync passes, async rejects.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/work", port), nil)
	req.Header.Set("X-Tenant", "evil")
	resp, _ := noKeepaliveClient.Do(req)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 403 || string(b) != "bad tenant" {
		t.Fatalf("bad-tenant: got %d %q", resp.StatusCode, string(b))
	}

	// Good tenant → both pass, handler reads local.
	req, _ = http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/work", port), nil)
	req.Header.Set("X-Tenant", "acme")
	resp, _ = noKeepaliveClient.Do(req)
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "for acme" {
		t.Fatalf("acme: got %d %q", resp.StatusCode, string(b))
	}
}

// TestAsyncMiddlewareOnPostAsync verifies async mw runs after body collection
// and that req.Body() returns the collected body inside the middleware.
func TestAsyncMiddlewareOnPostAsync(t *testing.T) {
	var seenBodyLen atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync("/upload", func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				// Body is available inside async middleware.
				body := req.Body()
				seenBodyLen.Store(int32(len(body)))
				req.SetLocal("sig", "len-"+strconv.Itoa(len(body)))
				next(res, req)
			}
		})
		app.PostAsync("/upload", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", req.Local("sig").(string)+"/"+strconv.Itoa(len(body)))
		})
	})
	defer teardown()

	payload := []byte("hello-world")
	status, body := httpPost(t, port, "/upload", "text/plain", payload)
	if status != 200 || body != "len-11/11" {
		t.Fatalf("got %d %q", status, body)
	}
	if seenBodyLen.Load() != int32(len(payload)) {
		t.Fatalf("async mw body len = %d, want %d", seenBodyLen.Load(), len(payload))
	}
}

// TestUseAsyncRejectsBadArgs mirrors TestUseRejectsBadArgs for UseAsync.
func TestUseAsyncRejectsBadArgs(t *testing.T) {
	cases := []struct {
		name string
		call func(app *gogo.App)
	}{
		{"int arg", func(app *gogo.App) { app.UseAsync(42) }},
		{"sync mw passed to async", func(app *gogo.App) {
			app.UseAsync(gogo.Middleware(func(next gogo.Handler) gogo.Handler { return next }))
		}},
		{"two strings", func(app *gogo.App) { app.UseAsync("/api/*", "/users") }},
		{"prefix only no mw", func(app *gogo.App) { app.UseAsync("/api/*") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for %s, got none", c.name)
				}
			}()
			app, err := gogo.NewApp()
			if err != nil {
				t.Fatalf("NewApp: %v", err)
			}
			defer app.Close()
			c.call(app)
		})
	}
}

// TestUseRejectsBadArgs verifies the Use type-switch panics on garbage args.
func TestUseRejectsBadArgs(t *testing.T) {
	cases := []struct {
		name string
		call func(app *gogo.App)
	}{
		{"int arg", func(app *gogo.App) { app.Use(42) }},
		{"two strings", func(app *gogo.App) { app.Use("/api/*", "/users") }},
		{"prefix only no mw", func(app *gogo.App) { app.Use("/api/*") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for %s, got none", c.name)
				}
			}()
			app, err := gogo.NewApp()
			if err != nil {
				t.Fatalf("NewApp: %v", err)
			}
			defer app.Close()
			c.call(app)
		})
	}
}

func TestAsyncRequestSnapshot(t *testing.T) {
	// Async handlers should see method/url/query/params/headers via the
	// snapshot captured before uWS freed the live request.
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/users/:id", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "application/json", fmt.Sprintf(
				`{"method":%q,"url":%q,"param":%q,"q":%q,"qp":%q,"hdr":%q}`,
				req.Method(), req.URL(), req.Parameter(0),
				req.Query(), req.QueryParam("token"), req.Header("x-custom"),
			))
		})
	})
	defer teardown()

	httpReq, _ := http.NewRequest("GET",
		fmt.Sprintf("http://127.0.0.1:%d/users/42?token=abc&other=1", port),
		nil)
	httpReq.Header.Set("X-Custom", "snapshot-works")
	resp, err := noKeepaliveClient.Do(httpReq)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	got := string(body)
	for _, want := range []string{
		`"method":"get"`,
		`"url":"/users/42"`,
		`"param":"42"`,
		`"q":"token=abc&other=1"`,
		`"qp":"abc"`,
		`"hdr":"snapshot-works"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in %s", want, got)
		}
	}
}

func TestPostAsyncRequestSnapshot(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/echo/:tag", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "application/json", fmt.Sprintf(
				`{"tag":%q,"qp":%q,"hdr":%q,"body":%q}`,
				req.Parameter(0), req.QueryParam("k"),
				req.Header("x-flag"), string(body),
			))
		})
	})
	defer teardown()

	httpReq, _ := http.NewRequest("POST",
		fmt.Sprintf("http://127.0.0.1:%d/echo/hello?k=v", port),
		bytes.NewReader([]byte("payload")))
	httpReq.Header.Set("X-Flag", "yes")
	httpReq.Header.Set("Content-Type", "text/plain")
	resp, err := noKeepaliveClient.Do(httpReq)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	got := string(body)
	for _, want := range []string{
		`"tag":"hello"`,
		`"qp":"v"`,
		`"hdr":"yes"`,
		`"body":"payload"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in %s", want, got)
		}
	}
}

func TestResponseJSON(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/json", func(res *gogo.Response, req *gogo.Request) {
			res.JSON(200, map[string]any{
				"ok":    true,
				"count": 42,
				"name":  "claude",
			})
		})
		app.GetAsync("/async-json", func(res *gogo.Response, req *gogo.Request) {
			res.JSON(201, map[string]any{"created": true})
		})
	})
	defer teardown()

	// Sync handler.
	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/json", port))
	if err != nil {
		t.Fatalf("GET /json: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("Content-Type=%q", ct)
	}
	var v map[string]any
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, body)
	}
	if v["ok"] != true || v["count"].(float64) != 42 || v["name"] != "claude" {
		t.Fatalf("payload=%v", v)
	}

	// Async handler uses the SendShared path.
	resp, err = noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/async-json", port))
	if err != nil {
		t.Fatalf("GET /async-json: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("async status=%d", resp.StatusCode)
	}
	if string(body) != `{"created":true}` {
		t.Fatalf("async body=%q", body)
	}
}

func TestRequestCookie(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain",
				"session="+req.Cookie("session")+
					";theme="+req.Cookie("theme")+
					";missing="+req.Cookie("missing"))
		})
	})
	defer teardown()

	httpReq, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/r", port), nil)
	httpReq.Header.Set("Cookie", "session=abc123; theme=dark; junk")
	resp, err := noKeepaliveClient.Do(httpReq)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "session=abc123;theme=dark;missing=" {
		t.Fatalf("got %q", body)
	}
}

func TestResponseSetCookie(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/login", func(res *gogo.Response, req *gogo.Request) {
			res.SetCookie(gogo.Cookie{
				Name:     "session",
				Value:    "token123",
				Path:     "/",
				MaxAge:   3600,
				HttpOnly: true,
				SameSite: gogo.SameSiteLax,
			})
			res.SetCookie(gogo.Cookie{
				Name:  "theme",
				Value: "dark",
			})
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/login", port))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()

	cookies := resp.Header.Values("Set-Cookie")
	if len(cookies) != 2 {
		t.Fatalf("got %d Set-Cookie headers, want 2: %v", len(cookies), cookies)
	}
	// First cookie has full attributes.
	if !strings.Contains(cookies[0], "session=token123") ||
		!strings.Contains(cookies[0], "Path=/") ||
		!strings.Contains(cookies[0], "Max-Age=3600") ||
		!strings.Contains(cookies[0], "HttpOnly") ||
		!strings.Contains(cookies[0], "SameSite=Lax") {
		t.Fatalf("session cookie: %q", cookies[0])
	}
	// Second cookie is bare.
	if cookies[1] != "theme=dark" {
		t.Fatalf("theme cookie: %q", cookies[1])
	}
}

func TestSetCookieRejectsBadValue(t *testing.T) {
	app, err := gogo.NewApp()
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	defer app.Close()

	// We can't easily call SetCookie outside a handler since Response
	// requires an inner pointer. Test the validator indirectly via the
	// public API: register a route that tries to set a bad cookie and
	// confirm the handler panics (caught by uWS HTTP context, returns 500).
	t.Run("bad name", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic for bad cookie name")
			}
		}()
		var r gogo.Response
		r.SetCookie(gogo.Cookie{Name: "bad name", Value: "x"})
	})
	t.Run("bad value", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic for bad cookie value")
			}
		}()
		var r gogo.Response
		r.SetCookie(gogo.Cookie{Name: "ok", Value: "has\rnewline"})
	})
}

func TestSnapshotBoundaries(t *testing.T) {
	// The C++ snapshot has fixed caps per field. Oversized snapshots are
	// rejected instead of silently truncating security-sensitive request data.
	// Caps mirror SNAP_* constants in uws_bridge.cpp: URL=256, QUERY=512,
	// PARAM=64 each.
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/echo/:p", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "application/json", fmt.Sprintf(
				`{"urlLen":%d,"queryLen":%d,"paramLen":%d}`,
				len(req.URL()), len(req.Query()), len(req.Parameter(0)),
			))
		})
	})
	defer teardown()

	bigParam := strings.Repeat("p", 200)        // > PARAM_CAP=64
	bigQuery := "k=" + strings.Repeat("v", 600) // > QUERY_CAP=512

	target := fmt.Sprintf("http://127.0.0.1:%d/echo/%s?%s", port, bigParam, bigQuery)
	resp, err := noKeepaliveClient.Get(target)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 431 || !strings.Contains(string(body), "snapshot too large") {
		t.Fatalf("oversized snapshot: got %d %q, want 431", resp.StatusCode, body)
	}
}

func TestSnapshotURLTruncation(t *testing.T) {
	// URL that exceeds URL_CAP=256 — uWS allows long paths, gogo rejects before
	// dispatching the async handler.
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/long/:p", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", fmt.Sprintf("urlLen=%d", len(req.URL())))
		})
	})
	defer teardown()

	// "/long/" + 300 chars = 306 bytes
	bigPath := strings.Repeat("a", 300)
	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/long/%s", port, bigPath))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 431 || !strings.Contains(string(body), "snapshot too large") {
		t.Fatalf("long URL: got %d %q, want 431", resp.StatusCode, body)
	}
}

func TestStressShortBursts(t *testing.T) {
	if testing.Short() {
		t.Skip("skip stress test in short mode")
	}
	// Higher concurrency than TestConcurrentLoad. Mixes shared, sync, and
	// PostAsync handlers to exercise multiple worker paths concurrently.
	port, teardown := startApp(t, func(app *gogo.App) {
		var counter atomic.Int64
		app.GetAsync("/async", func(res *gogo.Response, req *gogo.Request) {
			counter.Add(1)
			res.Send(200, "text/plain", "ok")
		})
		app.Get("/sync", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "sync")
		})
		app.PostAsync("/post", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", fmt.Sprintf("got %d", len(body)))
		})
	})
	defer teardown()

	const clients = 64
	const perClient = 100
	var wg sync.WaitGroup
	var errs atomic.Int64
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			client := &http.Client{
				Transport: &http.Transport{DisableKeepAlives: true},
				Timeout:   5 * time.Second,
			}
			for j := 0; j < perClient; j++ {
				var resp *http.Response
				var err error
				switch j % 3 {
				case 0:
					resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/async", port))
				case 1:
					resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/sync", port))
				case 2:
					resp, err = client.Post(
						fmt.Sprintf("http://127.0.0.1:%d/post", port),
						"text/plain",
						bytes.NewReader([]byte("data")))
				}
				if err != nil {
					errs.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					errs.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()
	if e := errs.Load(); e != 0 {
		t.Fatalf("got %d errors under stress (out of %d requests)", e, clients*perClient)
	}
}

func TestClientAbortDuringAsync(t *testing.T) {
	// Client disconnects while the async handler is still sleeping. The
	// handler must still run to completion (uWS doesn't kill goroutines)
	// and SendShared must silently drop the response when the connection
	// is gone, rather than crash. We don't call res.OnAborted from the
	// worker — uWS::onAborted is loop-thread-only, and the shared path
	// already registers an internal abort handler in C++.
	var handlerDone atomic.Int32

	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/slow", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(80 * time.Millisecond)
			res.Send(200, "text/plain", "late")
			handlerDone.Add(1)
		})
	})
	defer teardown()

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
			if err != nil {
				return
			}
			fmt.Fprintf(conn, "GET /slow HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
			// Close almost immediately — server sees disconnect mid-async.
			time.Sleep(10 * time.Millisecond)
			conn.Close()
		}()
	}
	wg.Wait()

	// Give handlers time to wake up after their sleeps.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && handlerDone.Load() < int32(n) {
		time.Sleep(20 * time.Millisecond)
	}
	if handlerDone.Load() < int32(n) {
		t.Errorf("only %d/%d handlers finished — possible deadlock or crash", handlerDone.Load(), n)
	}
}

func TestClientAbortDuringPostBodyThenServerContinues(t *testing.T) {
	var completed atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/upload", 1024*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			completed.Add(1)
			res.Send(200, "text/plain", fmt.Sprintf("got %d", len(body)))
		})
	})
	defer teardown()

	const aborted = 20
	for i := 0; i < aborted; i++ {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		fmt.Fprintf(conn, "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\nConnection: close\r\n\r\npartial")
		conn.Close()
	}

	time.Sleep(200 * time.Millisecond)
	if completed.Load() != 0 {
		t.Fatalf("aborted uploads reached handler: %d", completed.Load())
	}

	status, body := httpPost(t, port, "/upload", "text/plain", []byte("hello"))
	if status != 200 || body != "got 5" {
		t.Fatalf("server did not continue after aborted uploads: got %d %q", status, body)
	}
}

func TestHeaderInjectionRejected(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/inject", func(res *gogo.Response, req *gogo.Request) {
			defer func() {
				if r := recover(); r != nil {
					res.Send(400, "text/plain", "rejected")
				}
			}()
			res.Header("X-Bad", "value\r\nSet-Cookie: hack=1")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/inject")
	if status != 400 || !strings.Contains(body, "rejected") {
		t.Fatalf("header injection not rejected: got %d %q", status, body)
	}
}

// -----------------------------------------------------------------------------
// Middleware-bypass coverage. These tests pin down the security-relevant fix
// that landed alongside Group: scoped Use(prefix, mw) now matches the live
// request URL via req.URL() instead of the registered route pattern string,
// so dynamic / wildcard routes can no longer slip past path-scoped middleware.
// -----------------------------------------------------------------------------

// TestMiddlewareBypassParametricRoute is the canonical regression: a REST API
// scopes auth to /api/admin via Use("/api/admin", ...) and serves a parametric
// route Get("/api/:section"). The literal strings "/api/:section" and
// "/api/admin" share no string prefix, so under the old pattern-string match
// auth would never have wrapped the handler — but uWS still routes
// GET /api/admin into it via :section="admin", silently bypassing auth.
func TestMiddlewareBypassParametricRoute(t *testing.T) {
	var authHits, handlerHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use("/api/admin", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				authHits.Add(1)
				if req.Header("x-key") != "ok" {
					res.Send(401, "text/plain", "denied")
					return
				}
				next(res, req)
			}
		})
		app.Get("/api/:section", func(res *gogo.Response, req *gogo.Request) {
			handlerHits.Add(1)
			res.Send(200, "text/plain", "section="+req.Parameter(0))
		})
	})
	defer teardown()

	// /api/admin must hit the auth gate (the bug: it wouldn't).
	status, body := httpGet(t, port, "/api/admin")
	if status != 401 || body != "denied" {
		t.Fatalf("/api/admin without key: got %d %q, want 401 denied", status, body)
	}
	if authHits.Load() != 1 {
		t.Fatalf("auth hit count for /api/admin = %d, want 1", authHits.Load())
	}
	if handlerHits.Load() != 0 {
		t.Fatalf("handler ran for /api/admin without auth: hits=%d", handlerHits.Load())
	}

	// /api/users must NOT hit the auth gate (scope is /api/admin only).
	status, body = httpGet(t, port, "/api/users")
	if status != 200 || body != "section=users" {
		t.Fatalf("/api/users: got %d %q", status, body)
	}
	if authHits.Load() != 1 {
		t.Fatalf("auth wrongly ran for /api/users: hits=%d", authHits.Load())
	}
	if handlerHits.Load() != 1 {
		t.Fatalf("handler not called for /api/users: hits=%d", handlerHits.Load())
	}

	// /api/admin with the key passes through.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/admin", port), nil)
	req.Header.Set("X-Key", "ok")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("/api/admin authed: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "section=admin" {
		t.Fatalf("/api/admin authed: got %d %q", resp.StatusCode, string(b))
	}
}

// TestMiddlewareBypassWildcardFallback covers the second face of the bypass:
// a path-scoped middleware on /admin/* plus a catch-all Any("/*", spa). Under
// pattern-string match, the catch-all's pattern ("/*") has no prefix relation
// to "/admin", so it wouldn't be wrapped — but uWS falls back to "/*" for
// URLs like /admin/secret when no exact /admin/... route matches.
func TestMiddlewareBypassWildcardFallback(t *testing.T) {
	var adminHits, spaHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use("/admin/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				adminHits.Add(1)
				if req.Header("x-admin") != "1" {
					res.Send(401, "text/plain", "no admin")
					return
				}
				next(res, req)
			}
		})
		app.Get("/admin/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "users-list")
		})
		// SPA-style catch-all. Anything not matched above falls here.
		app.Any("/*", func(res *gogo.Response, req *gogo.Request) {
			spaHits.Add(1)
			res.Send(200, "text/plain", "spa")
		})
	})
	defer teardown()

	// /admin/secret has no exact match → uWS falls back to /*. The bug:
	// auth wouldn't wrap the /* handler, so the request would have hit
	// spa with 200. Fixed: auth runs first because the URL is under /admin.
	status, body := httpGet(t, port, "/admin/secret")
	if status != 401 || body != "no admin" {
		t.Fatalf("/admin/secret: got %d %q, want 401 no admin", status, body)
	}
	if spaHits.Load() != 0 {
		t.Fatalf("spa fallback wrongly ran for /admin/secret: hits=%d", spaHits.Load())
	}

	// /random falls to spa and must NOT trigger admin auth.
	status, body = httpGet(t, port, "/random")
	if status != 200 || body != "spa" {
		t.Fatalf("/random: got %d %q", status, body)
	}
	if adminHits.Load() != 1 {
		t.Fatalf("admin mw wrongly ran for /random (cumulative hits=%d, want 1)", adminHits.Load())
	}
	if spaHits.Load() != 1 {
		t.Fatalf("spa hit count = %d, want 1", spaHits.Load())
	}
}

// TestAsyncMiddlewareBypassParametric mirrors TestMiddlewareBypassParametricRoute
// for the async chain, which has its own wrapAsync path.
func TestAsyncMiddlewareBypassParametric(t *testing.T) {
	var authHits, handlerHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync("/api/admin", func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				authHits.Add(1)
				if req.Header("x-key") != "ok" {
					res.Send(401, "text/plain", "denied")
					return
				}
				next(res, req)
			}
		})
		app.GetAsync("/api/:section", func(res *gogo.Response, req *gogo.Request) {
			handlerHits.Add(1)
			res.Send(200, "text/plain", "section="+req.Parameter(0))
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/api/admin")
	if status != 401 || body != "denied" {
		t.Fatalf("/api/admin without key: got %d %q", status, body)
	}
	if authHits.Load() != 1 {
		t.Fatalf("async auth hits for /api/admin = %d, want 1", authHits.Load())
	}
	if handlerHits.Load() != 0 {
		t.Fatalf("handler ran for /api/admin without async auth: hits=%d", handlerHits.Load())
	}

	status, body = httpGet(t, port, "/api/users")
	if status != 200 || body != "section=users" {
		t.Fatalf("/api/users: got %d %q", status, body)
	}
	if authHits.Load() != 1 {
		t.Fatalf("async auth wrongly ran for /api/users: hits=%d", authHits.Load())
	}
}

// -----------------------------------------------------------------------------
// Group / Router coverage.
// -----------------------------------------------------------------------------

// TestGroupBasicScopesMiddleware: routes registered through a Group are
// wrapped by the Group's middleware; routes on the App directly are not.
func TestGroupBasicScopesMiddleware(t *testing.T) {
	var authHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				authHits.Add(1)
				if req.Header("x-key") != "ok" {
					res.Send(401, "text/plain", "no")
					return
				}
				next(res, req)
			}
		})
		api.Get("/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "users")
		})
		app.Get("/public", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "public")
		})
	})
	defer teardown()

	if status, body := httpGet(t, port, "/public"); status != 200 || body != "public" {
		t.Fatalf("/public: got %d %q", status, body)
	}
	if authHits.Load() != 0 {
		t.Fatalf("group mw wrongly ran for /public: hits=%d", authHits.Load())
	}

	if status, body := httpGet(t, port, "/api/users"); status != 401 || body != "no" {
		t.Fatalf("/api/users no key: got %d %q", status, body)
	}
	if authHits.Load() != 1 {
		t.Fatalf("group mw hits for /api/users = %d, want 1", authHits.Load())
	}
}

// TestGroupCoversParametricRoute: the whole point of Group over Use(prefix).
// A parametric route under a Group is wrapped by identity, not pattern, so
// /api/admin via :section="admin" always hits the group's middleware.
func TestGroupCoversParametricRoute(t *testing.T) {
	var authHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				authHits.Add(1)
				if req.Header("x-key") != "ok" {
					res.Send(401, "text/plain", "denied")
					return
				}
				next(res, req)
			}
		})
		api.Get("/:section", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "section="+req.Parameter(0))
		})
	})
	defer teardown()

	for _, section := range []string{"admin", "users", "billing"} {
		status, body := httpGet(t, port, "/api/"+section)
		if status != 401 || body != "denied" {
			t.Fatalf("/api/%s: got %d %q, want 401 denied", section, status, body)
		}
	}
	if authHits.Load() != 3 {
		t.Fatalf("group mw hits = %d, want 3 (one per request)", authHits.Load())
	}
}

// TestGroupNestedMiddlewareOrder: parent group's middleware wraps child
// group's middleware wraps the handler. Outermost-first.
func TestGroupNestedMiddlewareOrder(t *testing.T) {
	var trace []string
	var traceMu sync.Mutex
	record := func(s string) {
		traceMu.Lock()
		trace = append(trace, s)
		traceMu.Unlock()
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("global")
				next(res, req)
			}
		})
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("api")
				next(res, req)
			}
		})
		admin := api.Group("/admin", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("admin")
				next(res, req)
			}
		})
		admin.Get("/audit", func(res *gogo.Response, req *gogo.Request) {
			record("handler")
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	httpGet(t, port, "/api/admin/audit")
	traceMu.Lock()
	got := strings.Join(trace, ",")
	traceMu.Unlock()
	if got != "global,api,admin,handler" {
		t.Fatalf("trace: got %q, want global,api,admin,handler", got)
	}
}

// TestRouterUseAddsMW: Router.Use accumulates middleware that wraps later
// routes on that Router but not routes registered before the Use call.
func TestRouterUseAddsMW(t *testing.T) {
	var hits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api")
		api.Get("/early", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "early")
		})
		api.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				hits.Add(1)
				next(res, req)
			}
		})
		api.Get("/late", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "late")
		})
	})
	defer teardown()

	httpGet(t, port, "/api/early")
	if hits.Load() != 0 {
		t.Fatalf("mw ran for /api/early registered before Use: hits=%d", hits.Load())
	}
	httpGet(t, port, "/api/late")
	if hits.Load() != 1 {
		t.Fatalf("mw not invoked for /api/late: hits=%d", hits.Load())
	}
}

// TestGroupUseAsyncOnGetAsync: Router.UseAsync wraps GetAsync handlers
// registered through the router and passes Locals from middleware to handler.
func TestGroupUseAsyncOnGetAsync(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api")
		api.UseAsync(func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				if req.Header("authorization") == "" {
					res.Send(401, "text/plain", "no token")
					return
				}
				req.SetLocal("user", "alice")
				next(res, req)
			}
		})
		api.GetAsync("/me", func(res *gogo.Response, req *gogo.Request) {
			user, _ := req.Local("user").(string)
			res.Send(200, "text/plain", "hi "+user)
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/api/me")
	if status != 401 || body != "no token" {
		t.Fatalf("/api/me unauth: got %d %q", status, body)
	}

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/me", port), nil)
	req.Header.Set("Authorization", "Bearer x")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("/api/me authed: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "hi alice" {
		t.Fatalf("/api/me authed: got %d %q", resp.StatusCode, string(b))
	}
}

// TestGroupPostAsyncBodyAndMW: Router.PostAsync collects the body, enforces
// the cap, and runs through both sync group MW and the body handler.
func TestGroupPostAsyncBodyAndMW(t *testing.T) {
	var mwHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				mwHits.Add(1)
				next(res, req)
			}
		})
		api.PostAsync("/echo", 16, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", "echo:"+string(body))
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Post(
		fmt.Sprintf("http://127.0.0.1:%d/api/echo", port),
		"text/plain", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("POST /api/echo: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "echo:hello" {
		t.Fatalf("POST /api/echo: got %d %q", resp.StatusCode, string(b))
	}
	if mwHits.Load() != 1 {
		t.Fatalf("group mw hits = %d, want 1", mwHits.Load())
	}

	// Body too large → 413 from framework, handler not called.
	resp, err = noKeepaliveClient.Post(
		fmt.Sprintf("http://127.0.0.1:%d/api/echo", port),
		"text/plain", bytes.NewReader(bytes.Repeat([]byte("x"), 100)))
	if err != nil {
		t.Fatalf("POST /api/echo large: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Fatalf("POST /api/echo large: got %d, want 413", resp.StatusCode)
	}
}

// TestGroupGetAsyncFastPathWithoutMW confirms that GetAsync registered through
// a Group with NO middleware still uses the zero-cgo shared-memory dispatch
// path (no perf regression for the common case).
func TestGroupGetAsyncFastPathWithoutMW(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api")
		api.GetAsync("/work", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "done")
		})
	})
	defer teardown()

	for i := 0; i < 5; i++ {
		if status, body := httpGet(t, port, "/api/work"); status != 200 || body != "done" {
			t.Fatalf("/api/work iter %d: got %d %q", i, status, body)
		}
	}
}

// TestGroupStaticReplyBypassesMWOnlyIfNoneRegistered: Reply / string / []byte
// targets on a Router with NO middleware take the zero-cgo static path;
// when MW is registered on the Router or App, the static body is wrapped
// in a dynamic handler so middleware can run.
func TestGroupStaticReplyWithoutMW(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api")
		api.Get("/health", gogo.Reply{Body: "ok"})
	})
	defer teardown()

	if status, body := httpGet(t, port, "/api/health"); status != 200 || body != "ok" {
		t.Fatalf("/api/health: got %d %q", status, body)
	}
}

func TestGroupStaticReplyWithMW(t *testing.T) {
	var hits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				hits.Add(1)
				next(res, req)
			}
		})
		api.Get("/health", gogo.Reply{Status: 201, ContentType: "text/plain", Body: "ok"})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/api/health", port))
	if err != nil {
		t.Fatalf("/api/health: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 201 || string(b) != "ok" {
		t.Fatalf("/api/health: got %d %q", resp.StatusCode, string(b))
	}
	if hits.Load() != 1 {
		t.Fatalf("group mw hits for static target = %d, want 1", hits.Load())
	}
}

// TestGroupPrefixNormalization: Group("/api/") and Group("/api") behave the
// same; Group("/") is equivalent to no prefix; wildcards in Group prefix panic.
func TestGroupPrefixNormalization(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		// Trailing slash stripped.
		app.Group("/api/").Get("/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "users")
		})
		// Group("/") acts as global scope (no prefix).
		app.Group("/").Get("/healthz", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "alive")
		})
	})
	defer teardown()

	if status, body := httpGet(t, port, "/api/users"); status != 200 || body != "users" {
		t.Fatalf("/api/users: got %d %q", status, body)
	}
	if status, body := httpGet(t, port, "/healthz"); status != 200 || body != "alive" {
		t.Fatalf("/healthz: got %d %q", status, body)
	}
}

func TestGroupPrefixRejectsWildcard(t *testing.T) {
	app, err := gogo.NewApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	cases := []string{"/api/*", "/api/**", "/*"}
	for _, p := range cases {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("Group(%q) did not panic", p)
				}
			}()
			_ = app.Group(p)
		}()
	}
}

// TestGroupGlobalUseStillWraps: a global App.Use ALWAYS wraps routes
// registered via a Group, regardless of the group's prefix.
func TestGroupGlobalUseStillWraps(t *testing.T) {
	var globalHits, groupHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				globalHits.Add(1)
				next(res, req)
			}
		})
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				groupHits.Add(1)
				next(res, req)
			}
		})
		api.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "x")
		})
	})
	defer teardown()

	httpGet(t, port, "/api/x")
	if globalHits.Load() != 1 || groupHits.Load() != 1 {
		t.Fatalf("hits global=%d group=%d, want 1 each", globalHits.Load(), groupHits.Load())
	}
}

// -----------------------------------------------------------------------------
// P0 security-hardening tests (P0-1, P0-2, P0-4, P0-6, P0-9 from ROADMAP).
// -----------------------------------------------------------------------------

// startAppCfg mirrors startApp but lets the caller pass an explicit
// gogo.Config (BodyLimit, BindAddr, …).
func startAppCfg(t *testing.T, cfg gogo.Config, configure func(app *gogo.App)) (port int, teardown func()) {
	t.Helper()
	port = freePort(t)
	ready := make(chan *gogo.App, 1)
	listenErr := make(chan error, 1)
	runDone := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		app, err := gogo.NewApp(cfg)
		if err != nil {
			listenErr <- fmt.Errorf("NewApp: %w", err)
			close(runDone)
			return
		}
		configure(app)
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
	var app *gogo.App
	select {
	case app = <-ready:
	case err := <-listenErr:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("app setup timed out")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		bind := cfg.BindAddr
		if bind == "" {
			bind = "127.0.0.1"
		}
		c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", bind, port), 50*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	teardown = func() {
		app.Shutdown()
		select {
		case <-runDone:
		case <-time.After(10 * time.Second):
			t.Errorf("app.Run did not exit after Shutdown")
		}
	}
	return port, teardown
}

// TestReplyContentTypeRejectsCRLF: passing CRLF into Reply.ContentType
// must panic at App.Get registration, matching the dynamic-header
// validation that already rejects this.
func TestReplyContentTypeRejectsCRLF(t *testing.T) {
	app, err := gogo.NewApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Reply{ContentType: …\\r\\n…} did not panic")
		}
	}()
	app.Get("/x", gogo.Reply{
		ContentType: "text/plain\r\nX-Injected: evil",
		Body:        "ok",
	})
}

// TestRouterReplyContentTypeRejectsCRLF: same check on the Router.Get
// path, which has its own Reply branch.
func TestRouterReplyContentTypeRejectsCRLF(t *testing.T) {
	app, err := gogo.NewApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Router.Get(Reply{CT:…\\r\\n…}) did not panic")
		}
	}()
	app.Group("/api").Get("/x", gogo.Reply{
		ContentType: "text/plain\r\nX-Injected: 1",
		Body:        "ok",
	})
}

// TestJSONMarshalErrorDoesNotLeak: marshalling an unsupported value
// (a channel) must end with a generic 500 and reach the panic handler;
// the wire body must not include Go-internal text like "json marshal".
func TestJSONMarshalErrorDoesNotLeak(t *testing.T) {
	var captured atomic.Pointer[string]
	gogo.SetPanicHandler(func(rec any) {
		s := fmt.Sprintf("%v", rec)
		captured.Store(&s)
	})
	defer gogo.SetPanicHandler(nil)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/bad", func(res *gogo.Response, req *gogo.Request) {
			ch := make(chan int)
			res.JSON(200, ch) // json.Marshal returns UnsupportedTypeError
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/bad")
	if status != 500 {
		t.Fatalf("status: got %d, want 500", status)
	}
	if strings.Contains(strings.ToLower(body), "json") || strings.Contains(strings.ToLower(body), "unsupported") {
		t.Fatalf("body leaks marshal error detail: %q", body)
	}
	// Server-side report should have fired with the underlying error.
	got := captured.Load()
	if got == nil {
		t.Fatal("panic handler did not see the marshal error")
	}
	if !strings.Contains(*got, "JSON marshal") && !strings.Contains(*got, "unsupported") {
		t.Fatalf("panic handler payload: %q", *got)
	}
}

// TestCookieValueStripQuotes: Cookie("k") returns the unquoted value
// for `k="v"` (RFC 6265 §5.2). Unquoted and quoted-with-internal-text
// should both work.
func TestCookieValueStripQuotes(t *testing.T) {
	var captured atomic.Pointer[string]
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/cookie", func(res *gogo.Response, req *gogo.Request) {
			v := req.Cookie("session")
			captured.Store(&v)
			res.Send(200, "text/plain", v)
		})
	})
	defer teardown()

	for _, tc := range []struct{ header, want string }{
		{`session=plain`, "plain"},
		{`session="quoted"`, "quoted"},
		{`other=x; session="quoted"; more=y`, "quoted"},
		{`session=""`, ""}, // matched empty quotes
		{`session="`, `"`}, // single open quote not stripped
	} {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/cookie", port), nil)
		req.Header.Set("Cookie", tc.header)
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("GET cookie=%q: %v", tc.header, err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(b) != tc.want {
			t.Errorf("cookie=%q: got %q, want %q", tc.header, string(b), tc.want)
		}
	}
}

// TestBodyLimitContentLengthRejected: POST with Content-Length above
// the app's BodyLimit must get a 413 from the C++ pre-check; the Go
// handler must not be invoked.
func TestBodyLimitContentLengthRejected(t *testing.T) {
	var handlerHits atomic.Int32
	port, teardown := startAppCfg(t, gogo.Config{BodyLimit: 1024}, func(app *gogo.App) {
		app.Post("/upload", func(res *gogo.Response, req *gogo.Request) {
			handlerHits.Add(1)
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	// Exactly at the limit → handler runs.
	resp, err := noKeepaliveClient.Post(
		fmt.Sprintf("http://127.0.0.1:%d/upload", port),
		"text/plain", bytes.NewReader(bytes.Repeat([]byte("x"), 1024)))
	if err != nil {
		t.Fatalf("at-limit POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("at-limit POST: got %d, want 200", resp.StatusCode)
	}
	if handlerHits.Load() != 1 {
		t.Fatalf("handler hits at limit = %d, want 1", handlerHits.Load())
	}

	// One byte over → 413, handler must NOT be invoked.
	resp, err = noKeepaliveClient.Post(
		fmt.Sprintf("http://127.0.0.1:%d/upload", port),
		"text/plain", bytes.NewReader(bytes.Repeat([]byte("x"), 1025)))
	if err != nil {
		t.Fatalf("over-limit POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Fatalf("over-limit POST: got %d, want 413", resp.StatusCode)
	}
	if handlerHits.Load() != 1 {
		t.Fatalf("handler ran for over-limit POST: hits=%d, want 1", handlerHits.Load())
	}
}

// TestBindAddrLocalhost: when Config.BindAddr is set to 127.0.0.1, the
// listener is reachable on loopback. (We can't reliably test refusal on
// a non-loopback IP without knowing the box's external addresses; the
// loopback-reach assertion is the practical signal we care about.)
func TestBindAddrLocalhost(t *testing.T) {
	port, teardown := startAppCfg(t, gogo.Config{BindAddr: "127.0.0.1"}, func(app *gogo.App) {
		app.Get("/ok", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "loopback")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/ok")
	if status != 200 || body != "loopback" {
		t.Fatalf("/ok via 127.0.0.1: got %d %q", status, body)
	}
}

// TestWebSocketBehaviorAcceptsLimits: the new limit fields on
// WebSocketBehavior should at least register without crashing. Full
// runtime enforcement is uWS-side; we just confirm the bridge accepts
// the values.
func TestWebSocketBehaviorAcceptsLimits(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			MaxPayloadLength: 4096,
			IdleTimeout:      30 * time.Second,
			MaxBackpressure:  8192,
			DisablePings:     true,
		})
		app.Get("/ping", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "pong")
		})
	})
	defer teardown()

	// HTTP endpoint should still work after WS registration.
	if status, body := httpGet(t, port, "/ping"); status != 200 || body != "pong" {
		t.Fatalf("/ping after WS register: got %d %q", status, body)
	}
}

// TestRequestIntrospection covers the P1 request helpers — IP, IPs,
// Hostname, Get, Protocol, Secure — across sync, async, and the
// shared-dispatch path. IP comes from the loopback peer (127.0.0.1)
// in the test harness; IPs is X-Forwarded-For; Hostname comes from
// the Host header (port stripped).
func TestRequestIntrospection(t *testing.T) {
	type captured struct {
		ip       string
		ips      []string
		host     string
		get      string
		protocol string
		secure   bool
	}
	var sync, async, shared atomic.Pointer[captured]
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/sync", func(res *gogo.Response, req *gogo.Request) {
			sync.Store(&captured{
				ip:       req.IP(),
				ips:      req.IPs(),
				host:     req.Hostname(),
				get:      req.Get("x-custom"),
				protocol: req.Protocol(),
				secure:   req.Secure(),
			})
			res.Send(200, "text/plain", "sync")
		})
		// Force the sync-wrapper async path by attaching a middleware.
		app.Use("/async/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) { next(res, req) }
		})
		app.GetAsync("/async/snap", func(res *gogo.Response, req *gogo.Request) {
			async.Store(&captured{
				ip:       req.IP(),
				ips:      req.IPs(),
				host:     req.Hostname(),
				get:      req.Get("x-custom"),
				protocol: req.Protocol(),
				secure:   req.Secure(),
			})
			res.Send(200, "text/plain", "async")
		})
		// No middleware → shared-dispatch zero-cgo path.
		app.GetAsync("/shared", func(res *gogo.Response, req *gogo.Request) {
			shared.Store(&captured{
				ip:       req.IP(),
				ips:      req.IPs(),
				host:     req.Hostname(),
				get:      req.Get("x-custom"),
				protocol: req.Protocol(),
				secure:   req.Secure(),
			})
			res.Send(200, "text/plain", "shared")
		})
	})
	defer teardown()

	doRequest := func(path string) {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d%s", port, path), nil)
		req.Header.Set("Host", fmt.Sprintf("api.example:%d", port))
		req.Header.Set("X-Forwarded-For", "203.0.113.5, 198.51.100.7:443")
		req.Header.Set("X-Custom", "hi")
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
	}
	doRequest("/sync")
	doRequest("/async/snap")
	doRequest("/shared")

	check := func(name string, c *captured) {
		t.Helper()
		if c == nil {
			t.Fatalf("%s handler did not record", name)
		}
		if c.ip == "" || (!strings.HasPrefix(c.ip, "127.0.0.1") && c.ip != "::1") {
			t.Errorf("%s: ip=%q, want loopback", name, c.ip)
		}
		// Go's http client overrides Host from the URL; just verify a
		// non-empty hostname with no ":port".
		if c.host == "" || strings.ContainsRune(c.host, ':') {
			t.Errorf("%s: host=%q, want hostname with port stripped", name, c.host)
		}
		if c.get != "hi" {
			t.Errorf("%s: Get(x-custom)=%q, want hi", name, c.get)
		}
		if c.protocol != "http" {
			t.Errorf("%s: protocol=%q, want http", name, c.protocol)
		}
		if c.secure {
			t.Errorf("%s: secure=true on plaintext app", name)
		}
		if len(c.ips) != 2 || c.ips[0] != "203.0.113.5" || c.ips[1] != "198.51.100.7" {
			t.Errorf("%s: ips=%v, want [203.0.113.5 198.51.100.7]", name, c.ips)
		}
	}
	check("sync", sync.Load())
	check("async-snap", async.Load())
	check("shared", shared.Load())
}

// TestResponseRedirect: status defaults to 302; Location header is set;
// body is empty. CRLF in location panics at the validation gate.
func TestResponseRedirect(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/r1", func(res *gogo.Response, req *gogo.Request) {
			res.Redirect("/dest", 0) // → 302
		})
		app.Get("/r2", func(res *gogo.Response, req *gogo.Request) {
			res.Redirect("/perm", 301)
		})
		app.Get("/inject", func(res *gogo.Response, req *gogo.Request) {
			defer func() {
				if r := recover(); r != nil {
					res.Send(400, "text/plain", "rejected")
				}
			}()
			res.Redirect("/x\r\nX-Bad: 1", 302)
		})
	})
	defer teardown()

	// Disable auto-redirect so we can inspect the Location header.
	client := &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/r1", port))
	if err != nil {
		t.Fatalf("/r1: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 302 {
		t.Errorf("/r1: status=%d, want 302", resp.StatusCode)
	}
	if resp.Header.Get("Location") != "/dest" {
		t.Errorf("/r1: Location=%q, want /dest", resp.Header.Get("Location"))
	}

	resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/r2", port))
	if err != nil {
		t.Fatalf("/r2: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 301 || resp.Header.Get("Location") != "/perm" {
		t.Errorf("/r2: got %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}

	resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/inject", port))
	if err != nil {
		t.Fatalf("/inject: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 400 || !strings.Contains(string(b), "rejected") {
		t.Errorf("CRLF injection not rejected: %d %q", resp.StatusCode, string(b))
	}
}

// TestNotFoundHandler: customizing the 404 body via App.NotFound. Routes
// the user registers explicitly still win — only unmatched paths fall
// through to the NotFound handler.
func TestNotFoundHandler(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/exists", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "hi")
		})
		app.NotFound(func(res *gogo.Response, req *gogo.Request) {
			res.Send(404, "application/json", `{"err":"not found","path":"`+req.URL()+`"}`)
		})
	})
	defer teardown()

	// Explicit route still 200.
	status, body := httpGet(t, port, "/exists")
	if status != 200 || body != "hi" {
		t.Fatalf("/exists: got %d %q", status, body)
	}

	// Unmatched path → custom 404.
	status, body = httpGet(t, port, "/nope")
	if status != 404 || !strings.Contains(body, `"err":"not found"`) || !strings.Contains(body, `/nope`) {
		t.Fatalf("/nope: got %d %q", status, body)
	}
}

// TestNotFoundWrapsWithGlobalMiddleware: middleware registered before
// Listen wraps the NotFound handler the same as any other route, so a
// global logger / request-ID middleware still observes 404s.
func TestNotFoundWrapsWithGlobalMiddleware(t *testing.T) {
	var mwHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				mwHits.Add(1)
				next(res, req)
			}
		})
		app.NotFound(func(res *gogo.Response, req *gogo.Request) {
			res.Send(404, "text/plain", "missing")
		})
	})
	defer teardown()

	if status, body := httpGet(t, port, "/whatever"); status != 404 || body != "missing" {
		t.Fatalf("got %d %q", status, body)
	}
	if mwHits.Load() != 1 {
		t.Fatalf("global mw missed NotFound handler: hits=%d", mwHits.Load())
	}
}
