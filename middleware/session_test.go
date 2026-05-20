//go:build cgo && gogo

package middleware_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
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

// TestSessionPersistsAfterResponseAsync is the regression test for
// the session-vs-Response.Async lifecycle footgun: the middleware
// used to persist immediately after next() returned, but a sync
// handler that upgraded via res.Async would mutate the session
// AFTER persist had already fired — losing the mutation. With the
// Response.OnFinish hook the save is deferred until the goroutine
// completes, so mutations land correctly.
func TestSessionPersistsAfterResponseAsync(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.NewSession(middleware.SessionOptions{
			Secret: []byte("session-secret-32-bytes-AAAAAAAA"),
			TTL:    time.Minute,
		}))
		// Sync handler that does its real work inside a Response.Async
		// goroutine. The goroutine sets a session value AFTER the
		// outer middleware's next() returned — the old persist-on-
		// next-return path would have missed this write.
		app.Get("/login-async", func(res *gogo.Response, req *gogo.Request) {
			sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
			res.Async(func() {
				// Simulate work that has to happen off the loop thread.
				time.Sleep(20 * time.Millisecond)
				sess.Set("user_id", float64(42))
				sess.Set("logged_in", true)
				res.Send(200, "text/plain", "logged in")
			})
		})
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
			uid := sess.Get("user_id")
			loggedIn := sess.Get("logged_in")
			if uid == nil || loggedIn != true {
				res.Send(401, "text/plain", "no session")
				return
			}
			res.Send(200, "text/plain", fmt.Sprintf("user=%v logged_in=%v", uid, loggedIn))
		})
	})
	defer teardown()

	jar, _ := newCookieJar()
	client := &http.Client{Jar: jar, Timeout: 5 * time.Second}

	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/login-async", port))
	if err != nil {
		t.Fatalf("login-async: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "logged in" {
		t.Fatalf("login-async: status=%d body=%q", resp.StatusCode, string(body))
	}

	// Follow-up request reads the session. If the async mutation
	// didn't persist, /me returns 401.
	resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/me", port))
	if err != nil {
		t.Fatalf("me: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("session mutation in res.Async goroutine was not persisted: /me returned %d %q",
			resp.StatusCode, string(body))
	}
	if string(body) != "user=42 logged_in=true" {
		t.Errorf("session payload: got %q, want %q", string(body), "user=42 logged_in=true")
	}
}

// TestSessionDestroyAfterResponseAsync covers the destruction half:
// a Response.Async goroutine that calls sess.Destroy() must trigger
// the Store.Delete call when the response finishes, not before.
func TestSessionDestroyAfterResponseAsync(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.NewSession(middleware.SessionOptions{
			Secret: []byte("session-secret-32-bytes-AAAAAAAA"),
			TTL:    time.Minute,
		}))
		// Seed
		app.Get("/seed", func(res *gogo.Response, req *gogo.Request) {
			sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
			sess.Set("seed", "yes")
			res.Send(200, "text/plain", "seeded")
		})
		// Destroy from inside res.Async
		app.Get("/logout-async", func(res *gogo.Response, req *gogo.Request) {
			sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
			res.Async(func() {
				time.Sleep(20 * time.Millisecond)
				sess.Destroy()
				res.Send(200, "text/plain", "destroyed")
			})
		})
		// Verify — same cookie, but Destroy should have wiped the store entry.
		app.Get("/probe", func(res *gogo.Response, req *gogo.Request) {
			sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
			if sess.Get("seed") != nil {
				res.Send(200, "text/plain", "still here")
				return
			}
			res.Send(200, "text/plain", "gone")
		})
	})
	defer teardown()

	jar, _ := newCookieJar()
	client := &http.Client{Jar: jar, Timeout: 5 * time.Second}

	resp, _ := client.Get(fmt.Sprintf("http://127.0.0.1:%d/seed", port))
	resp.Body.Close()
	resp, _ = client.Get(fmt.Sprintf("http://127.0.0.1:%d/probe", port))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "still here" {
		t.Fatalf("seed didn't persist: probe returned %q", string(body))
	}

	resp, _ = client.Get(fmt.Sprintf("http://127.0.0.1:%d/logout-async", port))
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "destroyed" {
		t.Fatalf("logout-async: got %q", string(body))
	}

	resp, _ = client.Get(fmt.Sprintf("http://127.0.0.1:%d/probe", port))
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "gone" {
		t.Errorf("Destroy() in res.Async did not persist: probe returned %q", string(body))
	}
}

// TestSessionExplicitSave covers Session.Save() — a handler that
// wants to checkpoint state before the response finishes can call
// it, and the change must be visible to a parallel request that
// arrives BEFORE the original handler returns. The handler runs
// inside Response.Async so the uWS loop thread stays free to serve
// the concurrent probe request.
func TestSessionExplicitSave(t *testing.T) {
	released := make(chan struct{})
	checkpointed := make(chan struct{})

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.NewSession(middleware.SessionOptions{
			Secret: []byte("session-secret-32-bytes-AAAAAAAA"),
			TTL:    time.Minute,
		}))
		// Seed runs first so the cookie lands in the jar before the
		// long-running /checkpoint holds its response open.
		app.Get("/seed", func(res *gogo.Response, req *gogo.Request) {
			sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
			sess.Set("phase", "seed")
			res.Send(200, "text/plain", "seeded")
		})
		app.Get("/checkpoint", func(res *gogo.Response, req *gogo.Request) {
			sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
			res.Async(func() {
				sess.Set("phase", "checkpoint")
				sess.Save() // explicit, mid-handler
				close(checkpointed)
				<-released // hold the response open
				sess.Set("phase", "final")
				res.Send(200, "text/plain", "done")
			})
		})
		app.Get("/probe", func(res *gogo.Response, req *gogo.Request) {
			sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
			res.Send(200, "text/plain", fmt.Sprintf("phase=%v", sess.Get("phase")))
		})
	})
	defer teardown()

	jar, _ := newCookieJar()
	client := &http.Client{Jar: jar, Timeout: 5 * time.Second}

	// Seed first — establishes the session cookie in the jar.
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/seed", port))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	resp.Body.Close()

	// Kick off the long handler in a goroutine. It reuses the seeded
	// cookie so it operates on the same session row.
	done := make(chan struct{})
	go func() {
		resp, _ := client.Get(fmt.Sprintf("http://127.0.0.1:%d/checkpoint", port))
		if resp != nil {
			resp.Body.Close()
		}
		close(done)
	}()

	// Wait for the explicit Save to fire.
	<-checkpointed

	// Now probe — should see the checkpointed value even though the
	// original handler's goroutine is still running.
	resp, _ = client.Get(fmt.Sprintf("http://127.0.0.1:%d/probe", port))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "phase=checkpoint" {
		t.Errorf("explicit Save not visible mid-handler: probe returned %q", string(body))
	}

	close(released)
	<-done

	// Final probe — OnFinish should have persisted the "final" value.
	resp, _ = client.Get(fmt.Sprintf("http://127.0.0.1:%d/probe", port))
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "phase=final" {
		t.Errorf("final state not persisted by OnFinish: probe returned %q", string(body))
	}
}
