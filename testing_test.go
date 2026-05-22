//go:build cgo && gogo

package gogo_test

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

var expvarMarkerSeq atomic.Uint64

// TestTestServerBasic spins up a TestServer, hits a sync route,
// closes cleanly.
func TestTestServerBasic(t *testing.T) {
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Get("/ping", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "pong")
		})
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	resp, err := ts.Get("/ping")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "pong" {
		t.Errorf("body = %q, want pong", string(body))
	}
}

func TestNewTestServerSetupPanicReturnsError(t *testing.T) {
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		panic("boom")
	})
	if ts != nil {
		ts.Close()
	}
	if err == nil {
		t.Fatal("NewTestServer returned nil error after setup panic")
	}
	if !strings.Contains(err.Error(), "setup panic: boom") {
		t.Fatalf("NewTestServer error = %q, want setup panic", err)
	}
}

// TestTestServerDo rewrites a httptest.NewRequest URL to point at
// the running server and ships custom headers / body through Do.
func TestTestServerDo(t *testing.T) {
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Post("/echo", func(res *gogo.Response, req *gogo.Request) {
			// Capture the Authorization header BEFORE res.Body
			// — uWS frees the underlying HttpRequest the moment
			// the sync callback returns, and Body's callback
			// fires after that, so reading req.Header from
			// inside it would dereference freed memory.
			auth := req.Header("authorization")
			res.Body(1<<16, func(body []byte, err error) {
				if err != nil {
					res.Send(400, "text/plain", err.Error())
					return
				}
				res.Header("X-Saw-Auth", auth)
				res.Send(200, "application/json", string(body))
			})
		})
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	payload := []byte(`{"hello":"world"}`)
	req := httptest.NewRequest("POST", "/echo", strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err := ts.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != string(payload) {
		t.Errorf("body = %q, want %q", string(body), string(payload))
	}
	if got := resp.Header.Get("X-Saw-Auth"); got != "Bearer test-token" {
		t.Errorf("X-Saw-Auth = %q, want Bearer test-token", got)
	}
}

// TestTestServerExposesApp checks the App() accessor works for
// post-setup configuration (e.g. publishing from a goroutine).
func TestTestServerExposesApp(t *testing.T) {
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()
	if ts.App() == nil {
		t.Errorf("App() returned nil")
	}
	if ts.URL() == "" || ts.Port() == 0 {
		t.Errorf("URL/Port empty: url=%q port=%d", ts.URL(), ts.Port())
	}
}

// TestTestServerSetupError covers the path where Listen fails (no
// route registered + the user binds two TestServers to the same
// port). NewTestServer should return the error, not panic.
func TestTestServerNilSetup(t *testing.T) {
	if _, err := gogo.NewTestServer(nil); err == nil {
		t.Errorf("expected error on nil setup")
	}
}

// TestTestServerWithMiddleware ensures the full middleware chain
// (cookies, auth, logger) actually fires under the TestServer.
func TestTestServerWithMiddleware(t *testing.T) {
	const secret = "signed-cookie-secret-32-bytes-AAAA"
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Use(middleware.Helmet())
		app.Use(middleware.RequestID())
		app.Get("/secret", func(res *gogo.Response, req *gogo.Request) {
			res.SetCookieSigned(gogo.Cookie{Name: "s", Value: "alice"}, secret)
			res.Send(200, "text/plain", "ok")
		})
		app.Get("/whoami", func(res *gogo.Response, req *gogo.Request) {
			val, ok := req.CookieSigned("s", secret)
			if !ok {
				res.Send(401, "text/plain", "no session")
				return
			}
			res.Send(200, "text/plain", val)
		})
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	resp, _ := ts.Get("/secret")
	resp.Body.Close()
	cookie := resp.Header.Get("Set-Cookie")
	if !strings.Contains(cookie, "s=alice.") {
		t.Errorf("Set-Cookie missing signed payload: %q", cookie)
	}
	if resp.Header.Get("X-Request-Id") == "" {
		t.Errorf("middleware did not run (missing X-Request-Id)")
	}
	if !strings.Contains(resp.Header.Get("Strict-Transport-Security"), "max-age") {
		t.Errorf("Helmet did not run (missing HSTS)")
	}

	// Round-trip: bring the cookie back in a second request and
	// verify the route reads it via req.CookieSigned.
	req2, _ := http.NewRequest("GET", "/whoami", nil)
	req2.Header.Set("Cookie", cookie)
	resp2, err := ts.Do(req2)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	body, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 || string(body) != "alice" {
		t.Errorf("whoami: status=%d body=%q", resp2.StatusCode, string(body))
	}
}

// TestHTTPAdapterBasic wraps a stdlib http.HandlerFunc and exposes
// it as a gogo route.
func TestHTTPAdapterBasic(t *testing.T) {
	stdHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-From-Std", "yes")
		w.WriteHeader(http.StatusTeapot)
		io.WriteString(w, "I'm a teapot")
	})

	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Get("/legacy", gogo.HTTPAdapter(stdHandler))
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	resp, err := ts.Get("/legacy")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("status = %d, want 418", resp.StatusCode)
	}
	if string(body) != "I'm a teapot" {
		t.Errorf("body = %q", string(body))
	}
	if resp.Header.Get("X-From-Std") != "yes" {
		t.Errorf("X-From-Std missing")
	}
}

// TestHTTPAdapterHeadersIn ensures the wrapped handler sees inbound
// headers, including custom names, even on sync routes.
func TestHTTPAdapterHeadersIn(t *testing.T) {
	stdHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Saw-Authorization", r.Header.Get("Authorization"))
		w.Header().Set("X-Saw-Cookie", r.Header.Get("Cookie"))
		w.Header().Set("X-Saw-UA", r.Header.Get("User-Agent"))
		w.Header().Set("X-Saw-Tenant", r.Header.Get("X-Tenant"))
		w.WriteHeader(200)
	})

	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Get("/h", gogo.HTTPAdapter(stdHandler))
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	req, _ := http.NewRequest("GET", "/h", nil)
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Cookie", "session=xyz")
	req.Header.Set("User-Agent", "gogo-test/1.0")
	req.Header.Set("X-Tenant", "acme")
	resp, err := ts.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("X-Saw-Authorization"); got != "Bearer abc" {
		t.Errorf("Authorization passthrough: %q", got)
	}
	if got := resp.Header.Get("X-Saw-Cookie"); got != "session=xyz" {
		t.Errorf("Cookie passthrough: %q", got)
	}
	if got := resp.Header.Get("X-Saw-UA"); got != "gogo-test/1.0" {
		t.Errorf("User-Agent passthrough: %q", got)
	}
	if got := resp.Header.Get("X-Saw-Tenant"); got != "acme" {
		t.Errorf("custom header passthrough: %q", got)
	}
}

func TestHTTPAdapterPreservesHostAndRepeatedHeaders(t *testing.T) {
	stdHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Saw-Host", r.Host)
		w.Header().Set("X-Multi-Count", fmt.Sprintf("%d", len(r.Header.Values("X-Multi"))))
		w.Header().Set("X-Multi-Joined", strings.Join(r.Header.Values("X-Multi"), ","))
		w.WriteHeader(200)
	})

	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Get("/h", gogo.HTTPAdapter(stdHandler))
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	req, _ := http.NewRequest("GET", "/h", nil)
	req.Host = "tenant.example"
	req.Header.Add("X-Multi", "first")
	req.Header.Add("X-Multi", "second")
	resp, err := ts.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()

	if got := resp.Header.Get("X-Saw-Host"); got != "tenant.example" {
		t.Fatalf("Host = %q, want tenant.example", got)
	}
	if got := resp.Header.Get("X-Multi-Count"); got != "2" {
		t.Fatalf("X-Multi count = %q, want 2", got)
	}
	if got := resp.Header.Get("X-Multi-Joined"); got != "first,second" {
		t.Fatalf("X-Multi values = %q, want first,second", got)
	}
}

// TestHTTPAdapterMethodAndURL confirms the wrapped handler sees
// the original method (upper-cased) and URL (with the query
// string).
func TestHTTPAdapterMethodAndURL(t *testing.T) {
	stdHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Method", r.Method)
		w.Header().Set("X-URL", r.URL.RequestURI())
		w.WriteHeader(200)
	})

	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Get("/u", gogo.HTTPAdapter(stdHandler))
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	resp, err := ts.Get("/u?x=1&y=two")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.Header.Get("X-Method") != "GET" {
		t.Errorf("Method = %q, want GET", resp.Header.Get("X-Method"))
	}
	if got := resp.Header.Get("X-URL"); got != "/u?x=1&y=two" {
		t.Errorf("URL = %q, want /u?x=1&y=two", got)
	}
}

// TestHTTPAdapterStdlibHandler plugs in expvar.Handler — a real
// stdlib handler — and verifies it serves its JSON payload.
func TestHTTPAdapterStdlibHandler(t *testing.T) {
	// Register a known expvar so the JSON body has predictable
	// content. expvar names are process-global, so make the marker
	// unique for -count=N runs.
	markerName := fmt.Sprintf("gogo_test_marker_%d", expvarMarkerSeq.Add(1))
	v := expvar.NewString(markerName)
	v.Set("hello-from-test")

	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Get("/debug/vars", gogo.HTTPAdapter(expvar.Handler()))
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	resp, err := ts.Get("/debug/vars")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("expvar response not JSON: %v\nbody=%s", err, string(body))
	}
	if got := decoded[markerName]; got != "hello-from-test" {
		t.Errorf("expvar marker = %v, want hello-from-test", got)
	}
}

// TestHTTPAdapterWithBody covers the bodied variant: wrap a
// PostAsync route and pass the collected body into the stdlib
// handler.
func TestHTTPAdapterWithBody(t *testing.T) {
	stdHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Echo back what the stdlib handler received.
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Echo-CT", r.Header.Get("Content-Type"))
		w.WriteHeader(201)
		w.Write(b)
	})

	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.PostAsync("/legacy", 1<<16, func(res *gogo.Response, req *gogo.Request, body []byte) {
			gogo.HTTPAdapterWithBody(stdHandler, body)(res, req)
		})
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	payload := `{"k":"v"}`
	resp, err := ts.Post("/legacy", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Errorf("status = %d, want 201", resp.StatusCode)
	}
	if string(body) != payload {
		t.Errorf("body = %q, want %q", string(body), payload)
	}
	if resp.Header.Get("X-Echo-CT") != "application/json" {
		t.Errorf("Content-Type round-trip: %q", resp.Header.Get("X-Echo-CT"))
	}
}

func TestHTTPAdapterRequestBodyIsNonNil(t *testing.T) {
	stdHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil {
			http.Error(w, "nil body", 500)
			return
		}
		if err := r.Body.Close(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(204)
	})

	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Get("/legacy", gogo.HTTPAdapter(stdHandler))
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	resp, err := ts.Get("/legacy")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
}

func TestHTTPAdapterAcceptsBufferedFlushAndContentTypeSniff(t *testing.T) {
	stdHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", 500)
			return
		}
		// HTTPAdapter accepts Flush for compatibility, but still
		// buffers the complete response before sending through gogo.
		flusher.Flush()
		_, _ = io.WriteString(w, "<html><body>ok</body></html>")
	})

	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Get("/legacy", gogo.HTTPAdapter(stdHandler))
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	resp, err := ts.Get("/legacy")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "<html><body>ok</body></html>" {
		t.Errorf("body = %q", string(body))
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html sniffed", ct)
	}
}

func TestHTTPAdapterRejectsOversizeResponseBody(t *testing.T) {
	old := gogo.GetMaxHTTPAdapterBodyBytes()
	gogo.SetMaxHTTPAdapterBodyBytes(8)
	t.Cleanup(func() { gogo.SetMaxHTTPAdapterBodyBytes(old) })

	stdHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = io.WriteString(w, strings.Repeat("x", 32))
	})

	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Get("/legacy", gogo.HTTPAdapter(stdHandler))
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	resp, err := ts.Get("/legacy")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 500 {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
	if string(body) != "Internal Server Error\n" {
		t.Errorf("body = %q, want generic 500 body", string(body))
	}
}

// TestHTTPAdapterPanicsOnNil ensures HTTPAdapter(nil) panics
// loudly rather than silently producing a broken route.
func TestHTTPAdapterPanicsOnNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil handler")
		}
	}()
	gogo.HTTPAdapter(nil)
}
