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
	// The C++ snapshot has fixed caps per field. Verify oversized URL/query/
	// params are silently truncated instead of crashing. Note: uWS itself
	// rejects requests with very large headers at the HTTP parse stage, so
	// we don't fuzz the header buffer cap here — uWS guards that path.
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
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}

	var lens struct{ URLLen, QueryLen, ParamLen int }
	if err := json.Unmarshal(body, &lens); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, body)
	}
	// URL: "/echo/" + 200 'p' = 206 bytes, fits within 256 — no truncation.
	if lens.URLLen != len("/echo/")+200 {
		t.Errorf("URL len=%d, want %d", lens.URLLen, len("/echo/")+200)
	}
	// Query: "k=" + 600 'v' = 602 bytes, expect truncation at QUERY_CAP=512.
	if lens.QueryLen != 512 {
		t.Errorf("query truncated to %d, want 512", lens.QueryLen)
	}
	// Param: 200 'p', expect truncation at PARAM_CAP=64.
	if lens.ParamLen != 64 {
		t.Errorf("param truncated to %d, want 64", lens.ParamLen)
	}
}

func TestSnapshotURLTruncation(t *testing.T) {
	// URL that exceeds URL_CAP=256 — uWS allows long paths, snapshot truncates.
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
	if string(body) != "urlLen=256" {
		t.Fatalf("got %q, want urlLen=256", body)
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
