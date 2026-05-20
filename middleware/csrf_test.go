//go:build cgo && gogo

package middleware_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

func TestCSRFIssuesTokenOnSafeMethod(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.CSRF(middleware.CSRFOptions{
			Secret: []byte("csrf-secret"),
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			tok, _ := req.Local(middleware.CSRFLocalKey).(string)
			res.Send(200, "text/plain", tok)
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	cookies := resp.Cookies()
	var tok string
	for _, c := range cookies {
		if c.Name == "csrf_token" {
			tok = c.Value
		}
	}
	if tok == "" {
		t.Fatal("no csrf_token cookie issued")
	}
	if !strings.Contains(tok, ".") {
		t.Errorf("token should be random.sig, got %q", tok)
	}
}

func TestCSRFRejectsUnsafeWithoutToken(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.CSRF(middleware.CSRFOptions{
			Secret: []byte("csrf-secret"),
		}))
		app.Post("/api", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d/api", port), nil)
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("got %d want 403", resp.StatusCode)
	}
}

func TestCSRFAcceptsUnsafeWithMatchingToken(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.CSRF(middleware.CSRFOptions{
			Secret: []byte("csrf-secret"),
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
		app.Post("/api", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	// Step 1: get token.
	resp1, _ := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	resp1.Body.Close()
	var tok string
	for _, c := range resp1.Cookies() {
		if c.Name == "csrf_token" {
			tok = c.Value
		}
	}
	if tok == "" {
		t.Fatal("no token issued")
	}

	// Step 2: POST with cookie + header.
	req, _ := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d/api", port), nil)
	req.AddCookie(&http.Cookie{Name: "csrf_token", Value: tok})
	req.Header.Set("X-CSRF-Token", tok)
	resp2, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("got %d want 200", resp2.StatusCode)
	}
}

func TestCSRFRejectsForgedCookie(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.CSRF(middleware.CSRFOptions{
			Secret: []byte("csrf-secret"),
		}))
		app.Post("/api", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	// Forged cookie value with arbitrary content. The HMAC check
	// should reject it even though the header echoes the same string.
	forged := "deadbeef.lookslikesig"
	req, _ := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d/api", port), nil)
	req.AddCookie(&http.Cookie{Name: "csrf_token", Value: forged})
	req.Header.Set("X-CSRF-Token", forged)
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("got %d want 403", resp.StatusCode)
	}
}

func TestCSRFRejectsMismatchedHeader(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.CSRF(middleware.CSRFOptions{Secret: []byte("csrf-secret")}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
		app.Post("/api", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	resp1, _ := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	resp1.Body.Close()
	var tok string
	for _, c := range resp1.Cookies() {
		if c.Name == "csrf_token" {
			tok = c.Value
		}
	}

	req, _ := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d/api", port), nil)
	req.AddCookie(&http.Cookie{Name: "csrf_token", Value: tok})
	req.Header.Set("X-CSRF-Token", "totally-different")
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("got %d want 403", resp.StatusCode)
	}
}
