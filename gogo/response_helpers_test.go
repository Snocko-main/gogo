//go:build cgo && gogo

package gogo_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	htmltmpl "html/template"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogo "uwebsockets-go/gogo"
)

// TestJSONPBasic confirms a well-formed callback wraps the JSON
// value and sets application/javascript.
func TestJSONPBasic(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/api", func(res *gogo.Response, req *gogo.Request) {
			res.JSONP(req.QueryParam("callback"), map[string]any{
				"id":   42,
				"name": "alice",
			})
		})
	})
	defer teardown()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/api?callback=onLoad", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body := readAllString(t, resp)
	resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/javascript") {
		t.Errorf("Content-Type = %q, want application/javascript", ct)
	}
	if !strings.HasPrefix(body, "/**/onLoad(") || !strings.HasSuffix(body, ");") {
		t.Errorf("body shape wrong: %q", body)
	}
	// Strip the wrapper and verify the payload is the original JSON.
	payload := strings.TrimSuffix(strings.TrimPrefix(body, "/**/onLoad("), ");")
	var got map[string]any
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatalf("inner JSON parse: %v: payload=%q", err, payload)
	}
	if got["name"] != "alice" || int(got["id"].(float64)) != 42 {
		t.Errorf("inner payload = %+v", got)
	}
}

// TestJSONPRejectsBadCallback ensures attempts to smuggle script or
// HTML through the callback name return 400 without echoing the
// attacker-controlled string.
func TestJSONPRejectsBadCallback(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/api", func(res *gogo.Response, req *gogo.Request) {
			res.JSONP(req.QueryParam("callback"), map[string]any{"x": 1})
		})
	})
	defer teardown()

	bad := []struct {
		name string
		// encoded is the URL-escaped value sent on the wire; the
		// server decodes it back to `name` before validating.
		encoded string
	}{
		{"empty", ""},
		{"digit-leading", "1bad"},
		{"semicolon", "alert%28%29%3Bfoo"},
		{"html breakout", "foo%3C%2Fscript%3E"},
		{"space", "a%20b"},
		{"too long", strings.Repeat("a", 200)},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/api?callback=%s", port, c.encoded))
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != 400 {
				t.Errorf("callback %q (encoded %q): status = %d, want 400", c.name, c.encoded, resp.StatusCode)
			}
		})
	}
}

// TestJSONPAcceptsDottedCallback covers the namespaced-callback case
// (jQuery / common JSONP servers).
func TestJSONPAcceptsDottedCallback(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/api", func(res *gogo.Response, req *gogo.Request) {
			res.JSONP(req.QueryParam("callback"), []int{1, 2, 3})
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/api?callback=window.jsonp.cb", port))
	body := readAllString(t, resp)
	resp.Body.Close()
	if !strings.Contains(body, "window.jsonp.cb([1,2,3]);") {
		t.Errorf("body = %q", body)
	}
}

// TestRenderHTMLTemplate exercises the default html/template engine
// end-to-end: write a few templates, install the engine, render via
// the route.
func TestRenderHTMLTemplate(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "hello.tmpl"),
		`<h1>Hello, {{.Name}}!</h1>`)
	mustWrite(t, filepath.Join(dir, "layout/page.tmpl"),
		`<html><body>{{.Body}}</body></html>`)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.SetTemplateEngine(gogo.NewHTMLTemplateEngine(gogo.HTMLTemplateOptions{
			Root: dir,
		}))
		app.Get("/hello", func(res *gogo.Response, req *gogo.Request) {
			res.Render("hello", map[string]any{"Name": "World"})
		})
		app.Get("/page", func(res *gogo.Response, req *gogo.Request) {
			res.Render("layout/page", map[string]any{"Body": "rendered"})
		})
	})
	defer teardown()

	r1, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/hello", port))
	b1 := readAllString(t, r1)
	r1.Body.Close()
	if !strings.Contains(b1, "<h1>Hello, World!</h1>") {
		t.Errorf("hello body = %q", b1)
	}
	if ct := r1.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}

	r2, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/page", port))
	b2 := readAllString(t, r2)
	r2.Body.Close()
	if !strings.Contains(b2, "<html><body>rendered</body></html>") {
		t.Errorf("page body = %q", b2)
	}
}

// TestRenderAutoEscapesXSS confirms the default engine escapes HTML
// in interpolated values — guarding against the most common
// templating XSS.
func TestRenderAutoEscapesXSS(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "x.tmpl"), `<p>{{.User}}</p>`)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.SetTemplateEngine(gogo.NewHTMLTemplateEngine(gogo.HTMLTemplateOptions{Root: dir}))
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Render("x", map[string]any{"User": "<script>alert(1)</script>"})
		})
	})
	defer teardown()

	r, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/x", port))
	body := readAllString(t, r)
	r.Body.Close()
	if strings.Contains(body, "<script>") {
		t.Errorf("unescaped script tag in body: %q", body)
	}
	if !strings.Contains(body, "&lt;script&gt;") {
		t.Errorf("expected escaped script tag, body = %q", body)
	}
}

// TestRenderMissingTemplate returns 500 (with the framework logging
// the error) rather than crashing the server.
func TestRenderMissingTemplate(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "x.tmpl"), `ok`)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.SetTemplateEngine(gogo.NewHTMLTemplateEngine(gogo.HTMLTemplateOptions{Root: dir}))
		app.Get("/missing", func(res *gogo.Response, req *gogo.Request) {
			res.Render("does-not-exist", nil)
		})
	})
	defer teardown()

	r, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/missing", port))
	r.Body.Close()
	if r.StatusCode != 500 {
		t.Errorf("status = %d, want 500", r.StatusCode)
	}
}

// TestRenderNoEngineInstalled returns 500 when no engine has been
// configured.
func TestRenderNoEngineInstalled(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Render("anything", nil)
		})
	})
	defer teardown()

	r, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/r", port))
	r.Body.Close()
	if r.StatusCode != 500 {
		t.Errorf("status = %d, want 500", r.StatusCode)
	}
}

// TestRenderFuncMap exposes a helper to templates.
func TestRenderFuncMap(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "x.tmpl"), `{{upper .Name}}`)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.SetTemplateEngine(gogo.NewHTMLTemplateEngine(gogo.HTMLTemplateOptions{
			Root: dir,
			FuncMap: htmltmpl.FuncMap{
				"upper": strings.ToUpper,
			},
		}))
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Render("x", map[string]any{"Name": "alice"})
		})
	})
	defer teardown()

	r, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/x", port))
	body := readAllString(t, r)
	r.Body.Close()
	if !strings.Contains(body, "ALICE") {
		t.Errorf("body = %q", body)
	}
}

// TestRenderCustomEngine plugs in a non-html/template engine to
// verify the TemplateEngine interface is honored.
func TestRenderCustomEngine(t *testing.T) {
	custom := &stubEngine{}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.SetTemplateEngine(custom)
		app.Get("/c", func(res *gogo.Response, req *gogo.Request) {
			res.Render("anything", map[string]any{"k": "v"})
		})
	})
	defer teardown()

	r, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/c", port))
	body := readAllString(t, r)
	r.Body.Close()
	if body != "stub:anything:map[k:v]" {
		t.Errorf("custom engine output = %q", body)
	}
}

// stubEngine satisfies the gogo.TemplateEngine interface with a
// trivial format string. Confirms the interface is wired up
// without depending on the default html/template path.
type stubEngine struct{}

func (stubEngine) Render(w *bytes.Buffer, name string, data any) error {
	fmt.Fprintf(w, "stub:%s:%v", name, data)
	return nil
}

// Compile-time assertion that stubEngine satisfies the engine
// interface (otherwise SetTemplateEngine wouldn't accept it).
var _ gogo.TemplateEngine = stubEngine{}

// TestStreamSSE exercises Server-Sent Events: open an async route,
// stream a handful of events, and verify the client receives them
// in order on a single connection.
func TestStreamSSE(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
			err := res.Stream(200, "text/event-stream", func(w io.Writer) error {
				for i := 0; i < 5; i++ {
					if _, err := fmt.Fprintf(w, "data: tick-%d\n\n", i); err != nil {
						return err
					}
					time.Sleep(5 * time.Millisecond)
				}
				return nil
			})
			_ = err
		})
	})
	defer teardown()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/events", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var lines []string
	deadline := time.Now().Add(5 * time.Second)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			lines = append(lines, line)
		}
		if len(lines) >= 5 || time.Now().After(deadline) {
			break
		}
	}
	if len(lines) != 5 {
		t.Fatalf("got %d lines, want 5: %v", len(lines), lines)
	}
	for i, line := range lines {
		want := fmt.Sprintf("data: tick-%d", i)
		if line != want {
			t.Errorf("line %d = %q, want %q", i, line, want)
		}
	}
}

// TestStreamLargeBody confirms chunks well past the inline-response
// cap stream correctly.
func TestStreamLargeBody(t *testing.T) {
	const chunkSize = 16 * 1024
	const chunks = 8

	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/big", func(res *gogo.Response, req *gogo.Request) {
			chunk := strings.Repeat("x", chunkSize)
			res.Stream(200, "application/octet-stream", func(w io.Writer) error {
				for i := 0; i < chunks; i++ {
					if _, err := w.Write([]byte(chunk)); err != nil {
						return err
					}
				}
				return nil
			})
		})
	})
	defer teardown()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/big", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if want := chunkSize * chunks; len(body) != want {
		t.Errorf("body length = %d, want %d", len(body), want)
	}
}

// TestStreamHeadersIncludeBuffered verifies headers set via
// res.Header before Stream go out in the initial frame.
func TestStreamHeadersIncludeBuffered(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/h", func(res *gogo.Response, req *gogo.Request) {
			res.Header("X-Test", "alpha")
			res.Header("Cache-Control", "no-store")
			res.Stream(200, "text/plain", func(w io.Writer) error {
				_, err := w.Write([]byte("hi"))
				return err
			})
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/h", port))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.Header.Get("X-Test") != "alpha" {
		t.Errorf("X-Test = %q", resp.Header.Get("X-Test"))
	}
	if resp.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q", resp.Header.Get("Cache-Control"))
	}
	if string(body) != "hi" {
		t.Errorf("body = %q", string(body))
	}
}

// TestStreamPanicsOnSyncRoute ensures calling Stream from a sync
// handler (no res.Async first) panics immediately — better than
// silently writing nothing.
func TestStreamPanicsOnSyncRoute(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			defer func() {
				if r := recover(); r != nil {
					res.Send(500, "text/plain", "caught")
				}
			}()
			res.Stream(200, "text/plain", func(w io.Writer) error { return nil })
		})
	})
	defer teardown()

	r, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/x", port))
	body := readAllString(t, r)
	r.Body.Close()
	if r.StatusCode != 500 || body != "caught" {
		t.Errorf("expected caught-panic 500: status=%d body=%q", r.StatusCode, body)
	}
}

// mustWrite writes content under path, creating intermediate dirs as
// needed. Used by the templating tests to build per-test view trees.
func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
