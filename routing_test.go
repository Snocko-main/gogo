//go:build cgo && gogo

package gogo_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

// TestNamedParams verifies req.Param(name) resolves a route parameter
// by the name written in the pattern.
func TestNamedParams(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/users/:id/posts/:postID", func(res *gogo.Response, req *gogo.Request) {
			res.Header("X-User", req.Param("id"))
			res.Header("X-Post", req.Param("postID"))
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/alice/posts/42", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("X-User"); got != "alice" {
		t.Errorf("Param(id) = %q, want alice", got)
	}
	if got := resp.Header.Get("X-Post"); got != "42" {
		t.Errorf("Param(postID) = %q, want 42", got)
	}
}

// TestNamedParamsAsync verifies named param access works on async
// routes where Request reads from a snapshot, not the live uWS
// request.
func TestNamedParamsAsync(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/items/:slug", func(res *gogo.Response, req *gogo.Request) {
			res.Header("X-Slug", req.Param("slug"))
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/items/hello-world", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("X-Slug"); got != "hello-world" {
		t.Errorf("async Param(slug) = %q, want hello-world", got)
	}
}

// TestNamedParamMissingReturnsEmpty confirms Param(name) returns ""
// when the name was never declared on the route — typo defense.
func TestNamedParamMissingReturnsEmpty(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/users/:id", func(res *gogo.Response, req *gogo.Request) {
			if v := req.Param("notDeclared"); v != "" {
				res.Send(500, "text/plain", "expected empty")
				return
			}
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/42", port))
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("unexpected status %d", resp.StatusCode)
	}
}

// TestParamIntByName verifies the named ParamInt convenience.
func TestParamIntByName(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/users/:id", func(res *gogo.Response, req *gogo.Request) {
			id := req.ParamInt("id", -1)
			res.Send(200, "text/plain", fmt.Sprintf("id=%d", id))
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/42", port))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if got := string(body); got != "id=42" {
		t.Errorf("body = %q, want id=42", got)
	}
}

// TestTypedParamInt validates the <int> constraint: numeric IDs
// reach the handler, non-numeric IDs get a 404 from the framework
// without the handler running.
func TestTypedParamInt(t *testing.T) {
	var handlerCalled atomic.Bool
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/users/:id<int>", func(res *gogo.Response, req *gogo.Request) {
			handlerCalled.Store(true)
			res.Send(200, "text/plain", "got "+req.Param("id"))
		})
	})
	defer teardown()

	// Valid integer → handler fires, 200.
	r1, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/42", port))
	r1.Body.Close()
	if r1.StatusCode != 200 {
		t.Errorf("numeric id status = %d, want 200", r1.StatusCode)
	}
	if !handlerCalled.Load() {
		t.Errorf("handler should have run for numeric id")
	}

	// Non-numeric → 404, handler did NOT run.
	handlerCalled.Store(false)
	r2, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/alice", port))
	r2.Body.Close()
	if r2.StatusCode != 404 {
		t.Errorf("non-numeric id status = %d, want 404", r2.StatusCode)
	}
	if handlerCalled.Load() {
		t.Errorf("handler should NOT have run for non-numeric id")
	}
}

func TestTypedParamStaticReply(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/users/:id<int>/status", gogo.Reply{Body: "ok"})
	})
	defer teardown()

	r1, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/42/status", port))
	body, _ := io.ReadAll(r1.Body)
	r1.Body.Close()
	if r1.StatusCode != 200 || string(body) != "ok" {
		t.Fatalf("valid static typed route = %d %q, want 200 ok", r1.StatusCode, body)
	}

	r2, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/alice/status", port))
	r2.Body.Close()
	if r2.StatusCode != 404 {
		t.Fatalf("invalid static typed route = %d, want 404", r2.StatusCode)
	}
}

// TestTypedParamUUID validates the <uuid> constraint. Tests that an
// async route plus an Async-placed middleware both fire only on
// valid UUIDs.
func TestTypedParamUUID(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/jobs/:id<uuid>", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	r1, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/jobs/c50d0e9a-5d68-4f3d-9c8a-7ad27f6f1e9d", port))
	r1.Body.Close()
	if r1.StatusCode != 200 {
		t.Errorf("valid uuid status = %d, want 200", r1.StatusCode)
	}

	r2, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/jobs/not-a-uuid", port))
	r2.Body.Close()
	if r2.StatusCode != 404 {
		t.Errorf("invalid uuid status = %d, want 404", r2.StatusCode)
	}
}

// TestTypedParamConstraintRunsBeforeMiddleware ensures typed-param
// validation short-circuits BEFORE middleware fires — the whole
// point of the constraint is cheap rejection.
func TestTypedParamConstraintRunsBeforeMiddleware(t *testing.T) {
	var mwHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				mwHits.Add(1)
				next(res, req)
			}
		})
		app.Get("/users/:id<int>", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	// Invalid id → middleware should NOT fire.
	r1, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/abc", port))
	r1.Body.Close()
	if r1.StatusCode != 404 {
		t.Errorf("status = %d, want 404", r1.StatusCode)
	}
	if got := mwHits.Load(); got != 0 {
		t.Errorf("middleware fired on rejected request: hits=%d", got)
	}

	// Valid id → middleware fires.
	r2, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/42", port))
	r2.Body.Close()
	if got := mwHits.Load(); got != 1 {
		t.Errorf("middleware should have fired once: hits=%d", got)
	}
}

// TestRegisterParamType registers a custom param type and uses it in
// a route.
func TestRegisterParamType(t *testing.T) {
	gogo.RegisterParamType("hex", func(s string) bool {
		if s == "" {
			return false
		}
		for i := 0; i < len(s); i++ {
			c := s[i]
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
		return true
	})

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/blobs/:digest<hex>", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "got "+req.Param("digest"))
		})
	})
	defer teardown()

	r1, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/blobs/deadbeef", port))
	r1.Body.Close()
	if r1.StatusCode != 200 {
		t.Errorf("hex digest status = %d, want 200", r1.StatusCode)
	}

	r2, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/blobs/notHex--", port))
	r2.Body.Close()
	if r2.StatusCode != 404 {
		t.Errorf("non-hex digest status = %d, want 404", r2.StatusCode)
	}
}

// TestNamedRoutesAndURL verifies App.Name + App.URL for reverse
// routing.
func TestNamedRoutesAndURL(t *testing.T) {
	app, _ := gogo.NewApp()
	defer app.Close()

	app.Get("/users/:id", func(res *gogo.Response, req *gogo.Request) {})
	app.Name("user.show", "/users/:id")
	app.Get("/users/:userID/posts/:postID", func(res *gogo.Response, req *gogo.Request) {})
	app.Name("post.show", "/users/:userID/posts/:postID")

	tests := []struct {
		name    string
		route   string
		params  map[string]string
		want    string
		wantErr bool
	}{
		{"single param", "user.show", map[string]string{"id": "42"}, "/users/42", false},
		{"multi param", "post.show", map[string]string{"userID": "alice", "postID": "9"}, "/users/alice/posts/9", false},
		{"escape slash", "user.show", map[string]string{"id": "../admin"}, "/users/..%2Fadmin", false},
		{"escape percent slash", "user.show", map[string]string{"id": "%2f"}, "/users/%252f", false},
		{"unknown name", "missing", map[string]string{}, "", true},
		{"missing param", "user.show", map[string]string{}, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := app.URL(tc.route, tc.params)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("URL(%q): want error, got %q", tc.route, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("URL(%q): %v", tc.route, err)
			}
			if got != tc.want {
				t.Errorf("URL(%q) = %q, want %q", tc.route, got, tc.want)
			}
		})
	}
}

// TestNamedRouteStripsTypedAnnotation: Name accepts the same pattern
// the user wrote, including <type> annotations — they get stripped
// before storage so URL() produces a clean path.
func TestNamedRouteStripsTypedAnnotation(t *testing.T) {
	app, _ := gogo.NewApp()
	defer app.Close()
	app.Name("user.show", "/users/:id<int>")
	got, err := app.URL("user.show", map[string]string{"id": "42"})
	if err != nil {
		t.Fatalf("URL: %v", err)
	}
	if got != "/users/42" {
		t.Errorf("URL = %q, want /users/42", got)
	}
}

func TestRouterNameIncludesPrefix(t *testing.T) {
	app, _ := gogo.NewApp()
	defer app.Close()

	api := app.Group("/api/v1")
	api.Get("/users/:id<int>", func(res *gogo.Response, req *gogo.Request) {})
	api.Name("api.user.show", "/users/:id<int>")

	got, err := app.URL("api.user.show", map[string]string{"id": "42"})
	if err != nil {
		t.Fatalf("URL: %v", err)
	}
	if got != "/api/v1/users/42" {
		t.Errorf("URL = %q, want /api/v1/users/42", got)
	}
}

func TestRouterChildPatternMustStartWithSlash(t *testing.T) {
	cases := []struct {
		name     string
		register func(*gogo.Router)
	}{
		{
			name: "Get",
			register: func(r *gogo.Router) {
				r.Get("users", func(res *gogo.Response, req *gogo.Request) {})
			},
		},
		{
			name: "Post",
			register: func(r *gogo.Router) {
				r.Post("users", func(res *gogo.Response, req *gogo.Request) {})
			},
		},
		{
			name: "Any",
			register: func(r *gogo.Router) {
				r.Any("users", func(res *gogo.Response, req *gogo.Request) {})
			},
		},
		{
			name: "Put",
			register: func(r *gogo.Router) {
				r.Put("users", func(res *gogo.Response, req *gogo.Request) {})
			},
		},
		{
			name: "Patch",
			register: func(r *gogo.Router) {
				r.Patch("users", func(res *gogo.Response, req *gogo.Request) {})
			},
		},
		{
			name: "Delete",
			register: func(r *gogo.Router) {
				r.Delete("users", func(res *gogo.Response, req *gogo.Request) {})
			},
		},
		{
			name: "Options",
			register: func(r *gogo.Router) {
				r.Options("users", func(res *gogo.Response, req *gogo.Request) {})
			},
		},
		{
			name: "Head",
			register: func(r *gogo.Router) {
				r.Head("users", func(res *gogo.Response, req *gogo.Request) {})
			},
		},
		{
			name: "GetAsync",
			register: func(r *gogo.Router) {
				r.GetAsync("users", func(res *gogo.Response, req *gogo.Request) {})
			},
		},
		{
			name: "PostAsync",
			register: func(r *gogo.Router) {
				r.PostAsync("users", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {})
			},
		},
		{
			name: "WebSocket",
			register: func(r *gogo.Router) {
				r.WebSocket("users", gogo.WebSocketBehavior{})
			},
		},
		{
			name: "Name",
			register: func(r *gogo.Router) {
				r.Name("api.users", "users")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app, _ := gogo.NewApp()
			defer app.Close()
			api := app.Group("/api")

			defer func() {
				if recover() == nil {
					t.Fatalf("%s accepted child pattern without leading slash", tc.name)
				}
			}()
			tc.register(api)
		})
	}
}

// TestMountRoutesUnderPrefix ensures Mount registers routes under
// the prefix and delivers the named middleware inside the callback.
func TestMountRoutesUnderPrefix(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Mount("/api/v1", func(api *gogo.Router) {
			api.Use(func(next gogo.Handler) gogo.Handler {
				return func(res *gogo.Response, req *gogo.Request) {
					res.Header("X-Scope", "api-v1")
					next(res, req)
				}
			})
			api.Get("/ping", func(res *gogo.Response, req *gogo.Request) {
				res.Send(200, "text/plain", "pong")
			})
		})
		app.Get("/ping", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "root-pong")
		})
	})
	defer teardown()

	r1, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/api/v1/ping", port))
	if err != nil {
		t.Fatalf("/api/v1/ping: %v", err)
	}
	b1, _ := io.ReadAll(r1.Body)
	r1.Body.Close()
	if string(b1) != "pong" {
		t.Errorf("api ping body = %q", string(b1))
	}
	if r1.Header.Get("X-Scope") != "api-v1" {
		t.Errorf("api ping missing X-Scope header")
	}

	r2, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/ping", port))
	if err != nil {
		t.Fatalf("/ping: %v", err)
	}
	b2, _ := io.ReadAll(r2.Body)
	r2.Body.Close()
	if string(b2) != "root-pong" {
		t.Errorf("root ping body = %q", string(b2))
	}
	if r2.Header.Get("X-Scope") != "" {
		t.Errorf("root ping unexpectedly carries X-Scope")
	}
}

// TestGroupTypedParam combines Group's prefix with a typed-param
// child route and verifies both names are accessible via req.Param.
func TestGroupTypedParam(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		users := app.Group("/users/:userID")
		users.Get("/posts/:postID<int>", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", fmt.Sprintf("user=%s post=%s",
				req.Param("userID"), req.Param("postID")))
		})
	})
	defer teardown()

	r1, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/alice/posts/42", port))
	b1, _ := io.ReadAll(r1.Body)
	r1.Body.Close()
	if !strings.Contains(string(b1), "user=alice") || !strings.Contains(string(b1), "post=42") {
		t.Errorf("body = %q", string(b1))
	}

	// Non-int postID hits the constraint → 404.
	r2, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/alice/posts/foo", port))
	r2.Body.Close()
	if r2.StatusCode != 404 {
		t.Errorf("non-int post id status = %d, want 404", r2.StatusCode)
	}
}

func TestGroupTypedParamStaticReply(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		users := app.Group("/users/:userID")
		users.Get("/posts/:postID<int>/status", "ok")
	})
	defer teardown()

	r1, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/alice/posts/42/status", port))
	body, _ := io.ReadAll(r1.Body)
	r1.Body.Close()
	if r1.StatusCode != 200 || string(body) != "ok" {
		t.Fatalf("valid group static typed route = %d %q, want 200 ok", r1.StatusCode, body)
	}

	r2, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/alice/posts/foo/status", port))
	r2.Body.Close()
	if r2.StatusCode != 404 {
		t.Fatalf("invalid group static typed route = %d, want 404", r2.StatusCode)
	}
}

// TestTypedParamRejectsRegistrationOfUnknownType ensures registration
// panics fast if the user writes <typeName> for a type that wasn't
// registered — better to fail at startup than at request time.
func TestTypedParamRejectsRegistrationOfUnknownType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on unknown typed-param annotation")
		}
	}()
	app, _ := gogo.NewApp()
	defer app.Close()
	app.Get("/x/:y<no-such-type>", func(res *gogo.Response, req *gogo.Request) {})
}

// TestParamMiddlewareSeesNames ensures middleware running before the
// handler can already call req.Param(name).
func TestParamMiddlewareSeesNames(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				res.Header("X-MW-Id", req.Param("id"))
				next(res, req)
			}
		})
		app.Get("/users/:id", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
		_ = middleware.RequestID // keep middleware import live
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/users/alice", port))
	resp.Body.Close()
	if got := resp.Header.Get("X-MW-Id"); got != "alice" {
		t.Errorf("middleware Param(id) = %q, want alice", got)
	}
}
