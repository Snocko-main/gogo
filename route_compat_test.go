//go:build cgo && gogo

package gogo_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	gogo "github.com/Snocko-main/gogo"
)

func TestRouteMatchingCompatibility(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "root")
		})
		app.Get("/files/static", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "file-static")
		})
		app.Get("/files/:name", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "file:"+req.Param("name"))
		})
		app.Get("/typed/:id<int>", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "typed:"+req.Param("id"))
		})
		app.Get("/static/:id<int>/status", gogo.Reply{
			Status:      202,
			ContentType: "text/plain",
			Body:        "typed-static",
		})
		app.Get("/assets/*", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "asset:"+req.Parameter(0))
		})
		app.Get("/api/:section", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "api:"+req.Param("section"))
		})
		app.Get("/case/Sensitive", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "case")
		})
		app.Get("/trail/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "trail")
		})
		app.Post("/submit", func(res *gogo.Response, req *gogo.Request) {
			res.Send(201, "text/plain", "submitted")
		})
		app.NotFound(func(res *gogo.Response, req *gogo.Request) {
			res.Send(404, "text/plain", "missing:"+req.URL())
		})
		app.MethodNotAllowed(func(res *gogo.Response, req *gogo.Request) {
			res.Status(405)
			res.Header("Allow", strings.Join(app.AllowedMethods(req.URL()), ", "))
			res.Header("Content-Type", "text/plain")
			res.End("method-not-allowed:" + req.URL())
		})
	})
	defer teardown()

	tests := []struct {
		name      string
		method    string
		path      string
		wantCode  int
		wantBody  string
		wantAllow string
	}{
		{
			name:     "root route",
			method:   http.MethodGet,
			path:     "/",
			wantCode: 200,
			wantBody: "root",
		},
		{
			name:     "static beats parameter route",
			method:   http.MethodGet,
			path:     "/files/static",
			wantCode: 200,
			wantBody: "file-static",
		},
		{
			name:     "single segment parameter",
			method:   http.MethodGet,
			path:     "/files/report.txt",
			wantCode: 200,
			wantBody: "file:report.txt",
		},
		{
			name:     "parameter does not span slash",
			method:   http.MethodGet,
			path:     "/files/a/b",
			wantCode: 404,
			wantBody: "missing:/files/a/b",
		},
		{
			name:     "typed int accepts unsigned digits",
			method:   http.MethodGet,
			path:     "/typed/42",
			wantCode: 200,
			wantBody: "typed:42",
		},
		{
			name:     "typed int accepts negative digits",
			method:   http.MethodGet,
			path:     "/typed/-7",
			wantCode: 200,
			wantBody: "typed:-7",
		},
		{
			name:     "typed int rejects plus sign",
			method:   http.MethodGet,
			path:     "/typed/+7",
			wantCode: 404,
			wantBody: "Not Found\n",
		},
		{
			name:     "typed int rejects non-numeric",
			method:   http.MethodGet,
			path:     "/typed/abc",
			wantCode: 404,
			wantBody: "Not Found\n",
		},
		{
			name:     "typed static target validates before reply",
			method:   http.MethodGet,
			path:     "/static/123/status",
			wantCode: 202,
			wantBody: "typed-static",
		},
		{
			name:     "typed static target rejects invalid param",
			method:   http.MethodGet,
			path:     "/static/nope/status",
			wantCode: 404,
			wantBody: "Not Found\n",
		},
		{
			name:     "wildcard matches without exposed capture",
			method:   http.MethodGet,
			path:     "/assets/css/app.css",
			wantCode: 200,
			wantBody: "asset:",
		},
		{
			name:     "matching is case sensitive",
			method:   http.MethodGet,
			path:     "/case/sensitive",
			wantCode: 404,
			wantBody: "missing:/case/sensitive",
		},
		{
			name:     "trailing slash route matches exact slash",
			method:   http.MethodGet,
			path:     "/trail/",
			wantCode: 200,
			wantBody: "trail",
		},
		{
			name:     "trailing slash is significant",
			method:   http.MethodGet,
			path:     "/trail",
			wantCode: 404,
			wantBody: "missing:/trail",
		},
		{
			name:      "literal wrong method becomes 405",
			method:    http.MethodGet,
			path:      "/submit",
			wantCode:  405,
			wantBody:  "method-not-allowed:/submit",
			wantAllow: "POST",
		},
		{
			name:      "dynamic wrong method becomes 405",
			method:    http.MethodPost,
			path:      "/api/admin",
			wantCode:  405,
			wantBody:  "method-not-allowed:/api/admin",
			wantAllow: "GET",
		},
		{
			name:     "unknown path falls through to not found",
			method:   http.MethodGet,
			path:     "/unknown",
			wantCode: 404,
			wantBody: "missing:/unknown",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := routeCompatRequest(t, port, tc.method, tc.path)
			if resp.StatusCode != tc.wantCode {
				t.Fatalf("%s %s status = %d body=%q, want %d", tc.method, tc.path, resp.StatusCode, body, tc.wantCode)
			}
			if body != tc.wantBody {
				t.Fatalf("%s %s body = %q, want %q", tc.method, tc.path, body, tc.wantBody)
			}
			if got := resp.Header.Get("Allow"); got != tc.wantAllow {
				t.Fatalf("%s %s Allow = %q, want %q", tc.method, tc.path, got, tc.wantAllow)
			}
		})
	}
}

func TestReverseRoutingCompatibility(t *testing.T) {
	app, err := gogo.NewApp()
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	defer app.Close()

	app.Name("root", "/")
	app.Name("user.show", "/users/:id<int>")
	app.Name("post.show", "/users/:userID/posts/:postID")
	app.Name("files.glob", "/files/*")
	app.Name("search", "/search/:term")
	app.Name("copy", "/copy/:from/:to")
	app.Group("/api/v1/").Name("api.item", "/items/:slug")

	tests := []struct {
		name      string
		routeName string
		params    map[string]string
		want      string
		wantErr   string
	}{
		{
			name:      "root",
			routeName: "root",
			want:      "/",
		},
		{
			name:      "typed annotation stripped",
			routeName: "user.show",
			params:    map[string]string{"id": "42"},
			want:      "/users/42",
		},
		{
			name:      "multiple params",
			routeName: "post.show",
			params:    map[string]string{"userID": "alice", "postID": "9"},
			want:      "/users/alice/posts/9",
		},
		{
			name:      "path segment escaping",
			routeName: "user.show",
			params:    map[string]string{"id": "../admin"},
			want:      "/users/..%2Fadmin",
		},
		{
			name:      "percent escaping is preserved by escaping percent",
			routeName: "user.show",
			params:    map[string]string{"id": "%2f"},
			want:      "/users/%252f",
		},
		{
			name:      "space escapes as percent twenty",
			routeName: "search",
			params:    map[string]string{"term": "hello world"},
			want:      "/search/hello%20world",
		},
		{
			name:      "extra params ignored",
			routeName: "user.show",
			params:    map[string]string{"id": "42", "ignored": "x"},
			want:      "/users/42",
		},
		{
			name:      "empty param allowed",
			routeName: "user.show",
			params:    map[string]string{"id": ""},
			want:      "/users/",
		},
		{
			name:      "group name includes normalized prefix",
			routeName: "api.item",
			params:    map[string]string{"slug": "release-notes"},
			want:      "/api/v1/items/release-notes",
		},
		{
			name:      "unknown name errors",
			routeName: "missing",
			wantErr:   `no route named "missing"`,
		},
		{
			name:      "missing param errors",
			routeName: "copy",
			params:    map[string]string{"from": "a"},
			wantErr:   `requires param "to"`,
		},
		{
			name:      "wildcard cannot reverse route",
			routeName: "files.glob",
			params:    map[string]string{},
			wantErr:   `has a wildcard "/files/*"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := app.URL(tc.routeName, tc.params)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("URL(%q) = %q, want error containing %q", tc.routeName, got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("URL(%q) error = %q, want to contain %q", tc.routeName, err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("URL(%q): %v", tc.routeName, err)
			}
			if got != tc.want {
				t.Fatalf("URL(%q) = %q, want %q", tc.routeName, got, tc.want)
			}
		})
	}
}

func routeCompatRequest(t *testing.T, port int, method, path string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d%s", port, path), nil)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s read body: %v", method, path, err)
	}
	return resp, string(body)
}
