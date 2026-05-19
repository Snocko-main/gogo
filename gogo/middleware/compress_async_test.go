//go:build cgo && gogo

package middleware_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	gogo "uwebsockets-go/gogo"
	"uwebsockets-go/gogo/middleware"
)

// TestCompressAsyncResponseAsync exercises the Response.Async path:
// sync middleware sets up the encoder, the handler hands off work to
// a goroutine that ends up calling res.Send inside async mode. The
// encoder must apply and Content-Encoding must travel with the body.
func TestCompressAsyncResponseAsync(t *testing.T) {
	payload := strings.Repeat("async-compress-payload ", 200)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Compress())
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Async(func() {
				res.Send(200, "text/plain", payload)
			})
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding: %q", resp.Header.Get("Content-Encoding"))
	}
	if !strings.Contains(resp.Header.Get("Vary"), "Accept-Encoding") {
		t.Errorf("Vary missing Accept-Encoding")
	}
	body, _ := io.ReadAll(resp.Body)
	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	defer gr.Close()
	decoded, _ := io.ReadAll(gr)
	if string(decoded) != payload {
		t.Errorf("decoded body mismatch (got %d bytes want %d)", len(decoded), len(payload))
	}
}

// TestCompressGetAsync exercises the GetAsync route path: the worker
// goroutine runs the handler with the response wrapper in async
// mode. Compress is installed via UseAsync so it participates in the
// async middleware chain — UseAsync accepts a sync gogo.Middleware
// directly (Middleware and AsyncMiddleware share their underlying
// shape, the framework converts at registration time).
func TestCompressGetAsync(t *testing.T) {
	payload := strings.Repeat("getasync-payload ", 200)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync(middleware.Compress())
		app.GetAsync("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", payload)
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding: %q", resp.Header.Get("Content-Encoding"))
	}
	body, _ := io.ReadAll(resp.Body)
	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	defer gr.Close()
	decoded, _ := io.ReadAll(gr)
	if string(decoded) != payload {
		t.Errorf("decoded body mismatch")
	}
}

// TestCompressAsyncLargeBody pushes a body well past the
// shared-memory inline buffer size to force the cgo defer path. The
// encoder is the same code either way, but the integration confirms
// the with-headers defer path handles big payloads.
func TestCompressAsyncLargeBody(t *testing.T) {
	payload := strings.Repeat("X", 64*1024) // 64 KiB
	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync(middleware.Compress())
		app.GetAsync("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", payload)
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding: %q", resp.Header.Get("Content-Encoding"))
	}
	body, _ := io.ReadAll(resp.Body)
	gr, _ := gzip.NewReader(bytes.NewReader(body))
	defer gr.Close()
	decoded, _ := io.ReadAll(gr)
	if len(decoded) != len(payload) {
		t.Errorf("decoded length %d want %d", len(decoded), len(payload))
	}
}

// TestCompressAsyncSkipsWhenNoAcceptEncoding makes sure the encoder
// only attaches when the client opts in via Accept-Encoding, even on
// the async path.
func TestCompressAsyncSkipsWhenNoAcceptEncoding(t *testing.T) {
	payload := strings.Repeat("plain ", 500)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync(middleware.Compress())
		app.GetAsync("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", payload)
		})
	})
	defer teardown()

	tr := &http.Transport{DisableCompression: true, DisableKeepAlives: true}
	cl := &http.Client{Transport: tr}
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
	resp, err := cl.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "" {
		t.Errorf("got encoding %q without Accept-Encoding", resp.Header.Get("Content-Encoding"))
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != payload {
		t.Errorf("body mismatch")
	}
}
