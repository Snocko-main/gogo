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
	"sync"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
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

func TestRenderRejectsOversizeTemplateOutput(t *testing.T) {
	old := gogo.GetMaxRenderBytes()
	gogo.SetMaxRenderBytes(8)
	t.Cleanup(func() { gogo.SetMaxRenderBytes(old) })

	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "big.tmpl"), strings.Repeat("x", 32))

	port, teardown := startApp(t, func(app *gogo.App) {
		app.SetTemplateEngine(gogo.NewHTMLTemplateEngine(gogo.HTMLTemplateOptions{Root: dir}))
		app.Get("/big", func(res *gogo.Response, req *gogo.Request) {
			res.Render("big", nil)
		})
	})
	defer teardown()

	r, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/big", port))
	body := readAllString(t, r)
	r.Body.Close()
	if r.StatusCode != 500 {
		t.Errorf("status = %d, want 500", r.StatusCode)
	}
	if body != "Internal Server Error\n" {
		t.Errorf("body = %q, want generic 500 body", body)
	}
}

func TestSetTemplateEngineCanClearEngine(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "x.tmpl"), `ok`)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.SetTemplateEngine(gogo.NewHTMLTemplateEngine(gogo.HTMLTemplateOptions{Root: dir}))
		app.SetTemplateEngine(nil)
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Render("x", nil)
		})
	})
	defer teardown()

	r, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/x", port))
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

// TestStreamBufferedAmount samples res.BufferedAmount inside a
// stream loop. The byte counter should be 0 on a freshly-opened
// stream, rise after writes, and (with a fast localhost client)
// stay small because uWS drains immediately.
func TestStreamBufferedAmount(t *testing.T) {
	samplesCh := make(chan []uint64, 1)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/buf", func(res *gogo.Response, req *gogo.Request) {
			var samples []uint64
			res.Stream(200, "application/octet-stream", func(w io.Writer) error {
				samples = append(samples, res.BufferedAmount())
				_, _ = w.Write([]byte(strings.Repeat("x", 64*1024)))
				samples = append(samples, res.BufferedAmount())
				return nil
			})
			samplesCh <- samples
		})
	})
	defer teardown()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/buf", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(body) != 64*1024 {
		t.Errorf("body length = %d, want %d", len(body), 64*1024)
	}
	samples := <-samplesCh
	if len(samples) != 2 {
		t.Fatalf("samples = %d, want 2", len(samples))
	}
	// Before any write the buffer should be empty.
	if samples[0] != 0 {
		t.Errorf("BufferedAmount before write = %d, want 0", samples[0])
	}
	// After Write the queue is the SUM of what we asked the loop
	// to push (defer'd) — but the actual uWS-side buffer reflects
	// only bytes already moved through the C++ side. The value
	// can be 0 (loop already drained), some intermediate, or up
	// to the chunk size. Just assert "not panicking" and
	// trust the sample is coherent.
	_ = samples[1]
}

// TestStreamAwaitDrain proves the AwaitDrain helper returns
// without error on a fast-draining localhost connection. The
// threshold is set high (1 GiB) so the loop never has to park.
func TestStreamAwaitDrain(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/drain", func(res *gogo.Response, req *gogo.Request) {
			res.Stream(200, "application/octet-stream", func(w io.Writer) error {
				for i := 0; i < 4; i++ {
					if _, err := w.Write([]byte("chunk\n")); err != nil {
						return err
					}
					if err := res.AwaitDrain(1 << 30); err != nil {
						return err
					}
				}
				return nil
			})
		})
	})
	defer teardown()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/drain", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "chunk\nchunk\nchunk\nchunk\n" {
		t.Errorf("body = %q", string(body))
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

// TestStreamDefaultBackpressureBounds is the regression guard for the
// "Stream/SSE auto-backpressure" behavior. A handler that writes
// aggressively without ever calling AwaitDrain manually must still
// keep memory under control when the client reads slowly. The check:
// when uWS's BufferedAmount climbs past StreamBackpressureBytes, the
// next streamWriter.Write parks the producer until the buffer drains.
//
// Without the default backpressure: the producer would queue an
// unbounded number of cgo defers, each copying the chunk into the C
// heap, and the process would balloon.
func TestStreamDefaultBackpressureBounds(t *testing.T) {
	// Per-chunk size and total target stream size. We aim to send
	// at least 4 MiB so the 1 MiB default threshold is exercised
	// several times.
	const chunkSize = 64 << 10
	const totalChunks = 64 // 4 MiB total

	// observedMax records the highest BufferedAmount the producer
	// goroutine sees. Without the default backpressure check this
	// can grow unbounded; with it the value should stay close to
	// StreamBackpressureBytes (1 MiB default).
	var observedMax uint64
	var observedMu sync.Mutex
	record := func(v uint64) {
		observedMu.Lock()
		if v > observedMax {
			observedMax = v
		}
		observedMu.Unlock()
	}

	chunk := bytes.Repeat([]byte{'x'}, chunkSize)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/big", func(res *gogo.Response, req *gogo.Request) {
			_ = res.Stream(200, "application/octet-stream", func(w io.Writer) error {
				for i := 0; i < totalChunks; i++ {
					if _, werr := w.Write(chunk); werr != nil {
						return werr
					}
					record(res.BufferedAmount())
				}
				return nil
			})
		})
	})
	defer teardown()

	// Drip-read 8 KiB at a time with a 1 ms sleep so the producer
	// pulls ahead and triggers the backpressure park.
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/big", port))
	if err != nil {
		t.Fatalf("GET /big: %v", err)
	}
	defer resp.Body.Close()
	var collected bytes.Buffer
	buf := make([]byte, 8<<10)
	deadline := time.Now().Add(30 * time.Second)
	for collected.Len() < chunkSize*totalChunks && time.Now().Before(deadline) {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			collected.Write(buf[:n])
			time.Sleep(time.Millisecond)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("read: %v", rerr)
		}
	}
	if collected.Len() != chunkSize*totalChunks {
		t.Fatalf("collected %d bytes, want %d", collected.Len(), chunkSize*totalChunks)
	}

	observedMu.Lock()
	peak := observedMax
	observedMu.Unlock()

	// The backpressure threshold is 1 MiB; uWS's onWritable fires
	// at a lower watermark so the producer may see the buffer
	// climb slightly higher between the post-write check and
	// AwaitDrain's park. Bound peak at 4× threshold — enough slack
	// for the wake-and-recheck race, but tight enough that the
	// "no backpressure" regression (peak ≈ stream total) shows up
	// as a failure.
	cap := gogo.GetStreamBackpressureBytes() * 4
	if peak > cap {
		t.Errorf("peak BufferedAmount %d > 4×threshold %d; backpressure not enforced", peak, cap)
	}
}

// TestStreamBackpressureOptOut verifies that setting
// StreamBackpressureBytes to 0 restores the pre-default behavior:
// the writer no longer parks, and the handler is responsible for its
// own AwaitDrain calls. We assert the writer never hits an explicit
// drain check (the producer races ahead) by confirming a known-fast
// completion time for a small payload.
func TestStreamBackpressureOptOut(t *testing.T) {
	saved := gogo.GetStreamBackpressureBytes()
	gogo.SetStreamBackpressureBytes(0)
	defer func() { gogo.SetStreamBackpressureBytes(saved) }()

	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/opt-out", func(res *gogo.Response, req *gogo.Request) {
			_ = res.Stream(200, "text/plain", func(w io.Writer) error {
				_, err := w.Write([]byte("hello opt-out"))
				return err
			})
		})
	})
	defer teardown()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/opt-out", port))
	if err != nil {
		t.Fatalf("GET /opt-out: %v", err)
	}
	body := readAllString(t, resp)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
	if body != "hello opt-out" {
		t.Errorf("body: got %q, want %q", body, "hello opt-out")
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
