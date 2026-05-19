//go:build cgo && gogo

package middleware_test

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	gogo "uwebsockets-go/gogo"
	"uwebsockets-go/gogo/middleware"
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

func TestBasicAuthValidatorCallback(t *testing.T) {
	calls := 0
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.BasicAuth(middleware.BasicAuthOptions{
			Validator: func(u, p string) bool {
				calls++
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
	if calls != 1 {
		t.Errorf("validator calls=%d", calls)
	}
}
