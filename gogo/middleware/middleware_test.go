//go:build cgo && gogo

package middleware_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	gogo "uwebsockets-go/gogo"
	"uwebsockets-go/gogo/middleware"
)

var noKeepaliveClient = &http.Client{
	Transport: &http.Transport{DisableKeepAlives: true},
	Timeout:   5 * time.Second,
}

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

func startApp(t *testing.T, configure func(app *gogo.App)) (port int, teardown func()) {
	t.Helper()
	port = freePort(t)
	ready := make(chan *gogo.App, 1)
	listenErr := make(chan error, 1)
	runDone := make(chan struct{})

	go func() {
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
		t.Fatal("setup timeout")
	}
	for i := 0; i < 50; i++ {
		c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	teardown = func() {
		app.Shutdown()
		<-runDone
	}
	return port, teardown
}

// safeBuf is a bytes.Buffer that's safe for concurrent Write from the
// logger's serialized writes. The Logger's mutex already serializes;
// this just guards us from the test reading while a write is in flight.
type safeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestLoggerDefault: one line per request, default text format,
// fields in expected order. The test waits a few ms for the log line
// to flush (Logger writes after handler returns, but the writer is
// synchronous so by the time we receive the HTTP response, the line
// is already in the buffer).
func TestLoggerDefault(t *testing.T) {
	buf := &safeBuf{}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Logger(middleware.LoggerOptions{Output: buf}))
		app.Get("/ok", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
		app.Get("/teapot", func(res *gogo.Response, req *gogo.Request) {
			res.Send(418, "text/plain", "i'm a teapot")
		})
	})
	defer teardown()

	_, _ = noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/ok", port))
	_, _ = noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/teapot", port))

	// Allow the Logger's write to complete after the response returned
	// — the wrap closure runs after next() but the bytes.Buffer is
	// synchronous so the line is in the buffer as soon as
	// fmt.Fprintln returns.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Count(buf.String(), "\n") >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	out := buf.String()
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 log lines, got %d: %q", len(lines), out)
	}
	okPattern := regexp.MustCompile(`^get /ok 200 \S+ 127\.0\.0\.1 ".*"$`)
	teapotPattern := regexp.MustCompile(`^get /teapot 418 \S+ 127\.0\.0\.1 ".*"$`)
	if !okPattern.MatchString(lines[0]) {
		t.Errorf("line 1: %q (want %s)", lines[0], okPattern)
	}
	if !teapotPattern.MatchString(lines[1]) {
		t.Errorf("line 2: %q (want %s)", lines[1], teapotPattern)
	}
}

// TestLoggerJSONFormat: with JSONFormat, each line is valid JSON with
// the expected keys.
func TestLoggerJSONFormat(t *testing.T) {
	buf := &safeBuf{}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Logger(middleware.LoggerOptions{
			Output: buf,
			Format: middleware.JSONFormat,
		}))
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(201, "text/plain", "x")
		})
	})
	defer teardown()
	_, _ = noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/x", port))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "\n") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	line := strings.TrimSpace(buf.String())
	var entry struct {
		Method    string  `json:"method"`
		URL       string  `json:"url"`
		Status    int     `json:"status"`
		DurationMs float64 `json:"duration_ms"`
		IP        string  `json:"ip"`
	}
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("invalid JSON log line %q: %v", line, err)
	}
	if entry.Method != "get" || entry.URL != "/x" || entry.Status != 201 {
		t.Errorf("entry: %+v", entry)
	}
	if entry.DurationMs <= 0 {
		t.Errorf("duration_ms = %f, want > 0", entry.DurationMs)
	}
}

// TestLoggerSkipPaths: paths in SkipPaths produce no log line.
func TestLoggerSkipPaths(t *testing.T) {
	buf := &safeBuf{}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Logger(middleware.LoggerOptions{
			Output:    buf,
			SkipPaths: []string{"/healthz"},
		}))
		app.Get("/healthz", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
		app.Get("/api", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "api")
		})
	})
	defer teardown()
	for i := 0; i < 3; i++ {
		_, _ = noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
	}
	_, _ = noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/api", port))

	// Wait for the /api log line.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "/api") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	out := buf.String()
	if strings.Contains(out, "/healthz") {
		t.Errorf("expected /healthz to be skipped, got: %q", out)
	}
	if !strings.Contains(out, "/api") {
		t.Errorf("expected /api in log, got: %q", out)
	}
}

// TestCORSPermissive: zero-value CORSOptions emits Allow-Origin: * and
// echoes preflight requests with the default method/header list.
func TestCORSPermissive(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		middleware.UseCORS(app)
		app.Get("/api", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	// Simple cross-origin GET — should carry Allow-Origin.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api", port), nil)
	req.Header.Set("Origin", "https://example.com")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api: %v", err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Allow-Origin: got %q, want *", got)
	}

	// Preflight OPTIONS — 204, Allow-Methods, Allow-Headers.
	req, _ = http.NewRequest("OPTIONS", fmt.Sprintf("http://127.0.0.1:%d/api", port), nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "X-Custom")
	resp, err = noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("OPTIONS /api: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Errorf("preflight status: got %d, want 204", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Methods"); !strings.Contains(got, "POST") {
		t.Errorf("Allow-Methods: %q", got)
	}
	// We echo the requested headers when the caller sends them.
	if got := resp.Header.Get("Access-Control-Allow-Headers"); got != "X-Custom" {
		t.Errorf("Allow-Headers: got %q, want X-Custom", got)
	}
}

// TestCORSAllowList: an explicit AllowOrigins list reflects matched
// origins and omits the Allow-Origin header for the rest.
func TestCORSAllowList(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		middleware.UseCORS(app, middleware.CORSOptions{
			AllowOrigins:     []string{"https://app.example.com", "https://*.trusted.io"},
			AllowCredentials: true,
			MaxAge:           600,
		})
		app.Get("/api", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	for _, tc := range []struct {
		origin     string
		wantAllow  string
		wantVary   bool
		wantCreds  string
	}{
		{"https://app.example.com", "https://app.example.com", true, "true"},
		{"https://sub.trusted.io", "https://sub.trusted.io", true, "true"},
		{"https://other.com", "", false, ""},
	} {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api", port), nil)
		req.Header.Set("Origin", tc.origin)
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("Origin=%q: %v", tc.origin, err)
		}
		resp.Body.Close()
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != tc.wantAllow {
			t.Errorf("Origin=%q: Allow-Origin %q, want %q", tc.origin, got, tc.wantAllow)
		}
		if got := resp.Header.Get("Access-Control-Allow-Credentials"); got != tc.wantCreds {
			t.Errorf("Origin=%q: Allow-Credentials %q, want %q", tc.origin, got, tc.wantCreds)
		}
	}
}

// TestCORSWildcardWithCredentialsPanics: spec violation — defending
// against it at construction time.
func TestCORSWildcardWithCredentialsPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when combining * origin with credentials")
		}
	}()
	_ = middleware.CORS(middleware.CORSOptions{
		AllowOrigins:     []string{"*"},
		AllowCredentials: true,
	})
}

// TestRequestIDGenerated: a request without the X-Request-ID header
// gets a generated ID echoed back and stashed in Locals.
func TestRequestIDGenerated(t *testing.T) {
	capturedID := make(chan string, 1)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.RequestID())
		app.Get("/r", func(res *gogo.Response, req *gogo.Request) {
			id, _ := req.Local(middleware.RequestIDLocalKey).(string)
			capturedID <- id
			res.Send(200, "text/plain", id)
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/r", port))
	if err != nil {
		t.Fatalf("GET /r: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	headerID := resp.Header.Get("X-Request-ID")
	if headerID == "" {
		t.Fatal("X-Request-ID header missing on response")
	}
	if string(body) != headerID {
		t.Errorf("body ID %q != header ID %q", body, headerID)
	}
	got := <-capturedID
	if got != headerID {
		t.Errorf("Locals ID %q != header ID %q", got, headerID)
	}
	if len(headerID) != 16 {
		t.Errorf("ID length %d, want 16", len(headerID))
	}
}

// TestRequestIDReused: when X-Request-ID arrives on the request, the
// middleware reuses it verbatim — vital for end-to-end tracing
// where the edge proxy assigns the ID first.
func TestRequestIDReused(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.RequestID())
		app.Get("/r", func(res *gogo.Response, req *gogo.Request) {
			id, _ := req.Local(middleware.RequestIDLocalKey).(string)
			res.Send(200, "text/plain", id)
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/r", port), nil)
	req.Header.Set("X-Request-ID", "trace-abc-123")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("GET /r: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.Header.Get("X-Request-ID") != "trace-abc-123" {
		t.Errorf("header ID: got %q, want %q", resp.Header.Get("X-Request-ID"), "trace-abc-123")
	}
	if string(body) != "trace-abc-123" {
		t.Errorf("Locals ID: got %q, want %q", body, "trace-abc-123")
	}
}

// TestRequestIDCustomHeader: the Header option points at a different
// header (X-Correlation-ID); both incoming and outgoing use that name.
func TestRequestIDCustomHeader(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.RequestID(middleware.RequestIDOptions{Header: "X-Correlation-ID"}))
		app.Get("/r", func(res *gogo.Response, req *gogo.Request) {
			id, _ := req.Local(middleware.RequestIDLocalKey).(string)
			res.Send(200, "text/plain", id)
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/r", port), nil)
	req.Header.Set("X-Correlation-ID", "corr-99")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("GET /r: %v", err)
	}
	resp.Body.Close()
	if resp.Header.Get("X-Correlation-ID") != "corr-99" {
		t.Errorf("Custom header: got %q, want %q", resp.Header.Get("X-Correlation-ID"), "corr-99")
	}
}

// TestStackedMiddleware: Logger + CORS + RequestID stacked together
// behave as expected — logger sees the final status, CORS adds its
// headers, RequestID flows through.
func TestStackedMiddleware(t *testing.T) {
	buf := &safeBuf{}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Logger(middleware.LoggerOptions{Output: buf}))
		middleware.UseCORS(app, middleware.CORSOptions{AllowOrigins: []string{"*"}})
		app.Use(middleware.RequestID())
		app.Get("/api", func(res *gogo.Response, req *gogo.Request) {
			id, _ := req.Local(middleware.RequestIDLocalKey).(string)
			res.Header("X-Echo-ID", id)
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api", port), nil)
	req.Header.Set("Origin", "https://example.com")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api: %v", err)
	}
	resp.Body.Close()

	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("Allow-Origin missing")
	}
	if id := resp.Header.Get("X-Request-ID"); id == "" || id != resp.Header.Get("X-Echo-ID") {
		t.Errorf("X-Request-ID (%q) != X-Echo-ID (%q)", id, resp.Header.Get("X-Echo-ID"))
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "/api") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(buf.String(), "/api 200") {
		t.Errorf("logger didn't capture status: %q", buf.String())
	}
}
