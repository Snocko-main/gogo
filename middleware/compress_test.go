//go:build cgo && gogo

package middleware_test

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

func TestCompressGzip(t *testing.T) {
	payload := strings.Repeat("hello-world ", 1000)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Compress())
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
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
	if !strings.Contains(resp.Header.Get("Vary"), "Accept-Encoding") {
		t.Errorf("Vary missing Accept-Encoding: %q", resp.Header.Get("Vary"))
	}
	body, _ := io.ReadAll(resp.Body)
	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gr.Close()
	decoded, _ := io.ReadAll(gr)
	if string(decoded) != payload {
		t.Errorf("body mismatch after gunzip")
	}
	if len(body) >= len(payload) {
		t.Errorf("expected compressed body smaller; got %d vs %d", len(body), len(payload))
	}
}

func TestCompressDeflateFallback(t *testing.T) {
	payload := strings.Repeat("abcdef ", 500)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Compress())
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", payload)
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
	req.Header.Set("Accept-Encoding", "deflate")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "deflate" {
		t.Fatalf("Content-Encoding: %q", resp.Header.Get("Content-Encoding"))
	}
	body, _ := io.ReadAll(resp.Body)
	fr := flate.NewReader(bytes.NewReader(body))
	defer fr.Close()
	decoded, _ := io.ReadAll(fr)
	if string(decoded) != payload {
		t.Errorf("decoded body mismatch")
	}
}

func TestCompressSkipsSmallBody(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Compress(middleware.CompressOptions{MinSize: 1024}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "tiny")
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
	if resp.Header.Get("Content-Encoding") != "" {
		t.Errorf("small body should not be compressed; got %q", resp.Header.Get("Content-Encoding"))
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "tiny" {
		t.Errorf("body: %q", body)
	}
}

func TestCompressSkipsNoAcceptEncoding(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Compress())
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", strings.Repeat("x", 10000))
		})
	})
	defer teardown()

	// noKeepaliveClient doesn't auto-add Accept-Encoding either when
	// we use http.NewRequest without it — Go's http.Client adds gzip
	// automatically unless DisableCompression is set. Use a fresh
	// client that disables auto-compression for this test.
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
}

func TestCompressFiltersNonCompressibleContentType(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Compress())
		app.Get("/img", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "image/png", strings.Repeat("x", 5000))
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/img", port), nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Encoding") != "" {
		t.Errorf("image/png should not be compressed; got %q", resp.Header.Get("Content-Encoding"))
	}
}

func TestCompressJSONViaWriteEnd(t *testing.T) {
	// Use Write+End streaming pattern with explicit Content-Type
	// header to exercise the End encoder path and pendingHeaders
	// Content-Type sniff.
	payload := strings.Repeat(`{"k":"v"}`+"\n", 200)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Compress())
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Header("Content-Type", "application/json; charset=utf-8")
			res.Status(200)
			res.Write(payload[:len(payload)/2])
			res.End(payload[len(payload)/2:])
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
	decoded, _ := io.ReadAll(gr)
	if string(decoded) != payload {
		t.Errorf("decoded mismatch (len got=%d want=%d)", len(decoded), len(payload))
	}
}

// TestCompressSkipsOversizeBody asserts the MaxSize cap kicks in:
// bodies larger than the configured cap are emitted uncompressed
// instead of allocating a secondary buffer + burning multi-MiB
// compression CPU. The previous behavior compressed every body
// regardless of size, which made a multi-MiB JSON response a
// trivial DoS vector against process memory.
func TestCompressSkipsOversizeBody(t *testing.T) {
	// 4 KiB cap is small enough to test cheaply but large enough
	// to exceed MinSize so the small-body skip doesn't fire first.
	const cap = 4 << 10
	payload := strings.Repeat("zlib-compressible-content ", 1<<8) // ~6 KiB
	if len(payload) <= cap {
		t.Fatalf("test payload %d bytes is not larger than cap %d", len(payload), cap)
	}

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Compress(middleware.CompressOptions{
			MaxSize: cap,
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
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

	// Oversize body: no Content-Encoding, raw bytes match.
	if got := resp.Header.Get("Content-Encoding"); got != "" {
		t.Errorf("Content-Encoding: got %q, want empty (body exceeded MaxSize)", got)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != payload {
		t.Errorf("raw body mismatch: got %d bytes, want %d", len(body), len(payload))
	}
}

// TestCompressMaxSizeDisableSentinel: setting MaxSize=-1 disables
// the cap, restoring the pre-default behavior of compressing every
// body that passes the other filters. Useful for benchmarks and for
// callers who genuinely want to compress arbitrarily large responses.
func TestCompressMaxSizeDisableSentinel(t *testing.T) {
	const cap = -1
	payload := strings.Repeat("compressible-content ", 1<<10) // ~21 KiB

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Compress(middleware.CompressOptions{
			MaxSize: cap, // disabled
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
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

	if got := resp.Header.Get("Content-Encoding"); got != "gzip" {
		t.Errorf("Content-Encoding: got %q, want gzip (MaxSize disabled)", got)
	}
	body, _ := io.ReadAll(resp.Body)
	gr, _ := gzip.NewReader(bytes.NewReader(body))
	decoded, _ := io.ReadAll(gr)
	if string(decoded) != payload {
		t.Errorf("decoded mismatch (len got=%d want=%d)", len(decoded), len(payload))
	}
}
