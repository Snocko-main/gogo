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

	gogo "uwebsockets-go/gogo"
	"uwebsockets-go/gogo/middleware"
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
