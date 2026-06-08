//go:build cgo && gogo

package middleware_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

func TestJWTValidToken(t *testing.T) {
	secret := []byte("test-secret-with-enough-bytes-AAAA")
	tok, err := middleware.SignJWT(middleware.JWTHS256, secret, map[string]any{
		"sub": "user-42",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{Secret: secret}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			claims, _ := req.Local(middleware.JWTLocalKey).(map[string]any)
			res.JSON(200, claims)
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/me", port), nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("got %d", resp.StatusCode)
	}
	var out map[string]any
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("json: %v", err)
	}
	if out["sub"] != "user-42" {
		t.Errorf("sub: %v", out["sub"])
	}
}

func TestJWTValidTokenWithClaimValidationOptions(t *testing.T) {
	secret := []byte("test-secret-with-enough-bytes-AAAA")
	tok, err := middleware.SignJWT(middleware.JWTHS256, secret, map[string]any{
		"iss": "https://issuer.example",
		"aud": []string{"cli", "api"},
		"sub": "user-42",
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{
			Secret:         secret,
			Issuer:         "https://issuer.example",
			Audience:       "api",
			RequiredClaims: []string{"sub"},
		}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			claims, _ := req.Local(middleware.JWTLocalKey).(map[string]any)
			res.JSON(200, claims)
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/me", port), nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("got %d", resp.StatusCode)
	}
	var out map[string]any
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("json: %v", err)
	}
	if out["sub"] != "user-42" {
		t.Errorf("sub: %v", out["sub"])
	}
}

func TestJWTMissingToken(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{Secret: []byte("secret-secret-secret-secret-AAAA")}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	resp, _ := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/me", port))
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("got %d", resp.StatusCode)
	}
}

func TestJWTBadSignature(t *testing.T) {
	secret := []byte("real-secret-real-secret-AAAAAAAA")
	tok, _ := middleware.SignJWT(middleware.JWTHS256, []byte("other-secret-other-secret-AAAAAA"),
		map[string]any{"sub": "x"})

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{Secret: secret}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/me", port), nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("got %d", resp.StatusCode)
	}
}

func TestJWTAlgorithmNone(t *testing.T) {
	// Hand-craft an "alg":"none" token. Even with empty signature,
	// the middleware must reject because expected alg is HS256.
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"attacker"}`))
	tok := header + "." + payload + "."

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{Secret: []byte("secret-secret-secret-secret-AAAA")}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/me", port), nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("got %d want 401 for alg:none", resp.StatusCode)
	}
}

func TestJWTExpired(t *testing.T) {
	secret := []byte("test-secret-test-secret-AAAAAAAA")
	tok, _ := middleware.SignJWT(middleware.JWTHS256, secret, map[string]any{
		"sub": "x",
		"exp": float64(time.Now().Add(-time.Minute).Unix()),
	})
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{Secret: secret}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/me", port), nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("got %d want 401 for expired", resp.StatusCode)
	}
}

// TestJWTNonNumericExp asserts that a token carrying a non-numeric `exp`
// claim (e.g. "tomorrow" instead of a Unix timestamp) is rejected as
// malformed instead of silently skipping the expiration check.
// Regression test for the security review finding: the previous
// implementation type-asserted to float64 and skipped the check when
// the assertion failed, letting attackers forge tokens with no expiry.
func TestJWTNonNumericExp(t *testing.T) {
	secret := []byte("test-secret-test-secret-AAAAAAAA")
	tok, _ := middleware.SignJWT(middleware.JWTHS256, secret, map[string]any{
		"sub": "x",
		"exp": "tomorrow", // string, not numeric
	})
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{Secret: secret}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/me", port), nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("non-numeric exp not rejected: got %d, want 401", resp.StatusCode)
	}
}

// TestJWTNonNumericNbf is the not-before twin of TestJWTNonNumericExp:
// a string-typed `nbf` claim must be rejected, not silently skipped.
func TestJWTNonNumericNbf(t *testing.T) {
	secret := []byte("test-secret-test-secret-AAAAAAAA")
	tok, _ := middleware.SignJWT(middleware.JWTHS256, secret, map[string]any{
		"sub": "x",
		"nbf": []any{1, 2, 3}, // array, not numeric
	})
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{Secret: secret}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/me", port), nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("non-numeric nbf not rejected: got %d, want 401", resp.StatusCode)
	}
}

func TestJWTOptional(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{
			Secret:   []byte("secret-secret-secret-secret-AAAA"),
			Optional: true,
		}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			if c := req.Local(middleware.JWTLocalKey); c != nil {
				res.Send(200, "text/plain", "authed")
				return
			}
			res.Send(200, "text/plain", "anon")
		})
	})
	defer teardown()

	resp, _ := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/me", port))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), "anon") {
		t.Fatalf("optional anon: %d %q", resp.StatusCode, body)
	}
}
