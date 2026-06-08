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

func TestCORSCredentialedWildcardAllowHeadersFallbackOmitsWildcard(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.CORS(middleware.CORSOptions{
			AllowOrigins:     []string{"https://app.example.com"},
			AllowHeaders:     []string{"*", "Content-Type", "X-CSRF-Token"},
			AllowCredentials: true,
		}))
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("OPTIONS", fmt.Sprintf("http://127.0.0.1:%d/x", port), nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	resp.Body.Close()

	if got := resp.Header.Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Fatalf("Allow-Credentials = %q, want true", got)
	}
	got := resp.Header.Get("Access-Control-Allow-Headers")
	if strings.Contains(got, "*") {
		t.Fatalf("credentialed fallback leaked wildcard Allow-Headers: %q", got)
	}
	if !strings.Contains(strings.ToLower(got), "content-type") ||
		!strings.Contains(strings.ToLower(got), "x-csrf-token") {
		t.Fatalf("Allow-Headers = %q, want explicit configured headers", got)
	}
}
