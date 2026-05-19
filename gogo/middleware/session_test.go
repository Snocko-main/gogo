//go:build cgo && gogo

package middleware_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"testing"
	"time"

	gogo "uwebsockets-go/gogo"
	"uwebsockets-go/gogo/middleware"
)

func TestSessionRoundTrip(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.NewSession(middleware.SessionOptions{
			Secret: []byte("session-secret"),
			TTL:    time.Minute,
		}))
		app.Get("/set", func(res *gogo.Response, req *gogo.Request) {
			sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
			sess.Set("count", float64(1))
			res.Send(200, "text/plain", "set")
		})
		app.Get("/get", func(res *gogo.Response, req *gogo.Request) {
			sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
			v := sess.Get("count")
			if v == nil {
				res.Send(404, "text/plain", "miss")
				return
			}
			res.Send(200, "text/plain", fmt.Sprintf("%v", v))
		})
	})
	defer teardown()

	jar, _ := newCookieJar()
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Jar: jar, Timeout: 5 * time.Second}

	// Step 1: set.
	r1, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/set", port))
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	r1.Body.Close()

	// Step 2: get with the same jar (cookie carries the id).
	r2, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/get", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer r2.Body.Close()
	if r2.StatusCode != 200 {
		t.Fatalf("get status %d", r2.StatusCode)
	}
	body, _ := io.ReadAll(r2.Body)
	if string(body) != "1" {
		t.Errorf("value: %q", body)
	}
}

func TestSessionIsolatedPerCookie(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.NewSession(middleware.SessionOptions{Secret: []byte("session-secret")}))
		app.Get("/whoami", func(res *gogo.Response, req *gogo.Request) {
			sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
			res.Send(200, "text/plain", sess.ID)
		})
	})
	defer teardown()

	r1, _ := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/whoami", port))
	b1, _ := io.ReadAll(r1.Body)
	r1.Body.Close()

	r2, _ := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/whoami", port))
	b2, _ := io.ReadAll(r2.Body)
	r2.Body.Close()

	if string(b1) == string(b2) || string(b1) == "" {
		t.Errorf("expected different session IDs across clients without cookie jar; got %q and %q", b1, b2)
	}
}

func TestSessionRejectsForgedID(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.NewSession(middleware.SessionOptions{Secret: []byte("session-secret")}))
		app.Get("/whoami", func(res *gogo.Response, req *gogo.Request) {
			sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
			res.Send(200, "text/plain", sess.ID)
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/whoami", port), nil)
	req.AddCookie(&http.Cookie{Name: "session", Value: "forged.sig"})
	resp, _ := noKeepaliveClient.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if string(body) == "forged" {
		t.Errorf("forged session id was accepted")
	}
	// A new session should have been issued.
	hasNew := false
	for _, c := range resp.Cookies() {
		if c.Name == "session" && c.Value != "forged.sig" {
			hasNew = true
		}
	}
	if !hasNew {
		t.Errorf("expected new session cookie after forged id rejection; got %v", resp.Cookies())
	}
}

func TestSessionMemoryStoreSaveLoad(t *testing.T) {
	store := middleware.NewMemorySessionStore()
	if err := store.Save("k", map[string]any{"a": 1}, time.Minute); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok := store.Load("k")
	if !ok || got["a"] != 1 {
		t.Errorf("load: %v %v", got, ok)
	}
	store.Delete("k")
	if _, ok := store.Load("k"); ok {
		t.Errorf("delete didn't remove")
	}
}

func TestSessionMemoryStoreExpires(t *testing.T) {
	store := middleware.NewMemorySessionStore()
	store.Save("k", map[string]any{"a": 1}, 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if _, ok := store.Load("k"); ok {
		t.Errorf("expired entry still present")
	}
}

func newCookieJar() (http.CookieJar, error) {
	return cookiejar.New(nil)
}
