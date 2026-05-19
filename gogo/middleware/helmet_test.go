//go:build cgo && gogo

package middleware_test

import (
	"fmt"
	"testing"

	gogo "uwebsockets-go/gogo"
	"uwebsockets-go/gogo/middleware"
)

func TestHelmetDefaults(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Helmet())
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

	expected := map[string]string{
		"Strict-Transport-Security":         "max-age=15552000; includeSubDomains",
		"X-Frame-Options":                   "SAMEORIGIN",
		"X-Content-Type-Options":            "nosniff",
		"Referrer-Policy":                   "no-referrer",
		"X-XSS-Protection":                  "0",
		"X-DNS-Prefetch-Control":            "off",
		"X-Download-Options":                "noopen",
		"X-Permitted-Cross-Domain-Policies": "none",
		"Cross-Origin-Opener-Policy":        "same-origin",
		"Cross-Origin-Resource-Policy":      "same-origin",
	}
	for k, v := range expected {
		if got := resp.Header.Get(k); got != v {
			t.Errorf("%s: got %q want %q", k, got, v)
		}
	}
	if resp.Header.Get("Content-Security-Policy") != "" {
		t.Errorf("CSP must be off by default")
	}
}

func TestHelmetOmitsFields(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Helmet(middleware.HelmetOptions{
			HSTS:                  "off",
			FrameOptions:          "off",
			ContentSecurityPolicy: "default-src 'self'",
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

	if resp.Header.Get("Strict-Transport-Security") != "" {
		t.Errorf("HSTS should be omitted when set to 'off'")
	}
	if resp.Header.Get("X-Frame-Options") != "" {
		t.Errorf("X-Frame-Options should be omitted when set to 'off'")
	}
	if got := resp.Header.Get("Content-Security-Policy"); got != "default-src 'self'" {
		t.Errorf("CSP: got %q", got)
	}
}
