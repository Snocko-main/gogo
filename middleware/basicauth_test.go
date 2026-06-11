//go:build cgo && gogo

package middleware_test

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

func TestBasicAuthValidCredentials(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.BasicAuth(middleware.BasicAuthOptions{
			Users: map[string]string{"alice": "wonderland"},
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			user, _ := req.Local(middleware.BasicAuthLocalKey).(string)
			res.Send(200, "text/plain", "hello "+user)
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
	req.SetBasicAuth("alice", "wonderland")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "hello alice" {
		t.Errorf("body: %q", body)
	}
}

func TestBasicAuthCopiesUsersMap(t *testing.T) {
	users := map[string]string{"alice": "wonderland"}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.BasicAuth(middleware.BasicAuthOptions{
			Users: users,
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	users["alice"] = "mutated"
	users["mallory"] = "letmein"

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
	req.SetBasicAuth("alice", "wonderland")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("get original credentials: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("original credentials after map mutation: got %d, want 200", resp.StatusCode)
	}

	req, _ = http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
	req.SetBasicAuth("mallory", "letmein")
	resp, err = noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("get mutated credentials: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("mutated credentials: got %d, want 401", resp.StatusCode)
	}
}

func TestBasicAuthMissingCredentials(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.BasicAuth(middleware.BasicAuthOptions{
			Users: map[string]string{"u": "p"},
			Realm: "kingdom",
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("got %d want 401", resp.StatusCode)
	}
	if w := resp.Header.Get("WWW-Authenticate"); !strings.Contains(w, "kingdom") {
		t.Errorf("challenge: %q", w)
	}
}

func TestBasicAuthBadPassword(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.BasicAuth(middleware.BasicAuthOptions{
			Users: map[string]string{"u": "p"},
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
	auth := base64.StdEncoding.EncodeToString([]byte("u:wrong"))
	req.Header.Set("Authorization", "Basic "+auth)
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("got %d", resp.StatusCode)
	}
}

func TestBasicAuthMultipleUsersNoCrossMatch(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.BasicAuth(middleware.BasicAuthOptions{
			Users: map[string]string{"alice": "wonderland", "bob": "builder"},
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			user, _ := req.Local(middleware.BasicAuthLocalKey).(string)
			res.Send(200, "text/plain", "hello "+user)
		})
	})
	defer teardown()

	cases := []struct {
		user, pass string
		want       int
	}{
		{"alice", "wonderland", 200},
		{"bob", "builder", 200},
		// A valid password paired with the wrong username must not
		// satisfy the entry-by-entry scan.
		{"alice", "builder", 401},
		{"bob", "wonderland", 401},
		{"mallory", "wonderland", 401},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
		req.SetBasicAuth(tc.user, tc.pass)
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("%s:%s get: %v", tc.user, tc.pass, err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s:%s got %d want %d", tc.user, tc.pass, resp.StatusCode, tc.want)
		}
	}
}

func TestBasicAuthValidatorCallback(t *testing.T) {
	var calls atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.BasicAuth(middleware.BasicAuthOptions{
			Validator: func(u, p string) bool {
				calls.Add(1)
				return u == "admin" && p == "secret"
			},
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
	req.SetBasicAuth("admin", "secret")
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("got %d", resp.StatusCode)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("validator calls=%d", got)
	}
}

// TestBasicAuthRealmEscapesUnsafeChars: Realm values that contain RFC
// 7230 quoted-string delimiters or control characters must be escaped
// before going into the WWW-Authenticate header. Pre-fix the framework
// would either emit a malformed header (silent grammar break for "
// or \) or panic via validateHeaderValue (CR/LF/NUL). The escaper
// shared with JWT authParam handles both cases.
func TestBasicAuthRealmEscapesUnsafeChars(t *testing.T) {
	cases := []struct {
		name      string
		realm     string
		wantChunk string // substring that must appear in the response header
		notChunk  string // substring that must NOT appear (raw, unescaped)
	}{
		{
			name:      "quote",
			realm:     `we"have"quotes`,
			wantChunk: `realm="we\"have\"quotes"`,
		},
		{
			name:      "backslash",
			realm:     `back\slash`,
			wantChunk: `realm="back\\slash"`,
		},
		{
			name:      "CR LF dropped",
			realm:     "with\r\nCRLF",
			wantChunk: `realm="withCRLF"`,
			notChunk:  "\r\n",
		},
		{
			name:      "NUL dropped",
			realm:     "nul\x00here",
			wantChunk: `realm="nulhere"`,
			notChunk:  "\x00",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			port, teardown := startApp(t, func(app *gogo.App) {
				app.Use(middleware.BasicAuth(middleware.BasicAuthOptions{
					Users: map[string]string{"u": "p"},
					Realm: tc.realm,
				}))
				app.Get("/", func(res *gogo.Response, req *gogo.Request) {
					res.Send(200, "text/plain", "ok")
				})
			})
			defer teardown()

			resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != 401 {
				t.Fatalf("status %d, want 401 (so we can inspect WWW-Authenticate)", resp.StatusCode)
			}
			got := resp.Header.Get("WWW-Authenticate")
			if !strings.Contains(got, tc.wantChunk) {
				t.Errorf("WWW-Authenticate=%q missing %q", got, tc.wantChunk)
			}
			if tc.notChunk != "" && strings.Contains(got, tc.notChunk) {
				t.Errorf("WWW-Authenticate=%q leaked unescaped %q", got, tc.notChunk)
			}
		})
	}
}
