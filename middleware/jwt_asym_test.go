//go:build cgo && gogo

package middleware_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

func TestJWTRS256RoundTrip(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa keygen: %v", err)
	}
	tok, err := middleware.SignJWT(middleware.JWTRS256, priv, map[string]any{
		"sub": "rsa-user",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{
			Algorithm: middleware.JWTRS256,
			Key:       &priv.PublicKey,
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
	body, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("json: %v", err)
	}
	if out["sub"] != "rsa-user" {
		t.Errorf("sub: %v", out["sub"])
	}
}

func TestJWTPS256RoundTrip(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa keygen: %v", err)
	}
	tok, _ := middleware.SignJWT(middleware.JWTPS256, priv, map[string]any{"sub": "ps-user"})

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{
			Algorithm: middleware.JWTPS256,
			Key:       &priv.PublicKey,
		}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/me", port), nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("got %d", resp.StatusCode)
	}
}

func TestJWTES256RoundTrip(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ec keygen: %v", err)
	}
	tok, err := middleware.SignJWT(middleware.JWTES256, priv, map[string]any{
		"sub": "ec-user",
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{
			Algorithm: middleware.JWTES256,
			Key:       &priv.PublicKey,
		}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/me", port), nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("got %d", resp.StatusCode)
	}
}

func TestJWTES512RoundTrip(t *testing.T) {
	// P-521 — the "ES512" misnomer (named for SHA-512, not the curve).
	priv, _ := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	tok, _ := middleware.SignJWT(middleware.JWTES512, priv, map[string]any{"sub": "521"})

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{
			Algorithm: middleware.JWTES512,
			Key:       &priv.PublicKey,
		}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/me", port), nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("got %d", resp.StatusCode)
	}
}

func TestJWTAlgorithmConfusion(t *testing.T) {
	// Classic alg-confusion attack: server is configured for RS256
	// with an RSA public key, but the attacker mints a token with
	// `alg: HS256` and uses the public key bytes as the HMAC
	// secret. A naive verifier that picks the algorithm from the
	// token header would happily verify.
	//
	// The middleware MUST reject this because it locks the
	// expected algorithm at construction.
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	pubDER, _ := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})

	// Attacker forges HS256 token using the PEM bytes as HMAC secret.
	attackerTok, err := middleware.SignJWT(middleware.JWTHS256, pubPEM, map[string]any{"sub": "attacker"})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{
			Algorithm: middleware.JWTRS256,
			Key:       &priv.PublicKey,
		}))
		app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/me", port), nil)
	req.Header.Set("Authorization", "Bearer "+attackerTok)
	resp, _ := noKeepaliveClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("alg-confusion not rejected: got %d want 401", resp.StatusCode)
	}
}

func TestJWTRSATamperedSignature(t *testing.T) {
	priv1, _ := rsa.GenerateKey(rand.Reader, 2048)
	priv2, _ := rsa.GenerateKey(rand.Reader, 2048)

	// Sign with priv2, verify with priv1's public — must fail.
	tok, _ := middleware.SignJWT(middleware.JWTRS256, priv2, map[string]any{"sub": "x"})

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.JWT(middleware.JWTOptions{
			Algorithm: middleware.JWTRS256,
			Key:       &priv1.PublicKey,
		}))
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

func TestParseRSAPublicKeyPKIX(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	pubDER, _ := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})

	parsed, err := middleware.ParseRSAPublicKey(pubPEM)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.N.Cmp(priv.PublicKey.N) != 0 {
		t.Errorf("parsed modulus mismatch")
	}
}

func TestParseECPublicKey(t *testing.T) {
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	pubDER, _ := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})

	parsed, err := middleware.ParseECPublicKey(pubPEM)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.X.Cmp(priv.PublicKey.X) != 0 {
		t.Errorf("parsed X mismatch")
	}
}
