//go:build cgo && gogo

package middleware

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Snocko-main/gogo"
)

func TestSessionCopiesSecret(t *testing.T) {
	secret := []byte("session-secret-32-bytes-AAAAAAAA")
	store := NewMemorySessionStore()
	if err := store.Save("known-session", map[string]any{"user": "alice"}, time.Hour); err != nil {
		t.Fatalf("store save: %v", err)
	}
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Use(NewSession(SessionOptions{
			Secret: secret,
			Store:  store,
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			sess, _ := req.Local(SessionLocalKey).(*Session)
			res.Send(200, "text/plain", sess.ID)
		})
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	signed := signSessionID([]byte("session-secret-32-bytes-AAAAAAAA"), "known-session")
	secret[0] ^= 0xff

	req, _ := http.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: "session", Value: signed})
	resp, err := ts.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if got := string(body); got != "known-session" {
		t.Fatalf("session id = %q, want known-session", got)
	}
}

func TestCSRFCopiesSecret(t *testing.T) {
	secret := []byte("csrf-secret-32-bytes-AAAAAAAAAAAA")
	token, err := newCSRFToken([]byte("csrf-secret-32-bytes-AAAAAAAAAAAA"))
	if err != nil {
		t.Fatalf("newCSRFToken: %v", err)
	}
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Use(CSRF(CSRFOptions{
			Secret: secret,
		}))
		app.Post("/submit", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	secret[0] ^= 0xff

	req, _ := http.NewRequest("POST", "/submit", strings.NewReader(""))
	req.Header.Set("X-CSRF-Token", token)
	req.Header.Set("Cookie", fmt.Sprintf("csrf_token=%s", token))
	resp, err := ts.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "ok" {
		t.Fatalf("status=%d body=%q, want 200 ok", resp.StatusCode, string(body))
	}
}
