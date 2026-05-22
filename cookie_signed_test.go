//go:build cgo && gogo

package gogo_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"testing"

	gogo "github.com/Snocko-main/gogo"
)

func newCookieJar() (*cookiejar.Jar, error) {
	return cookiejar.New(nil)
}

func readAllString(t *testing.T, r *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

const (
	testCookieSecret    = "signed-cookie-secret-32-bytes-AAAA"
	testCookieSecretAlt = "signed-cookie-secret-32-bytes-BBBB"
)

// TestSignVerifyRoundTrip is the basic shape: sign, then verify with
// the same key, get back the original value.
func TestSignVerifyRoundTrip(t *testing.T) {
	signed := gogo.SignCookieValue("alice:42", testCookieSecret)
	if !strings.Contains(signed, ".") {
		t.Fatalf("signed value missing separator: %q", signed)
	}
	got, ok := gogo.VerifyCookieValue(signed, testCookieSecret)
	if !ok {
		t.Fatalf("VerifyCookieValue returned !ok for fresh signed value")
	}
	if got != "alice:42" {
		t.Errorf("recovered value = %q, want alice:42", got)
	}
}

// TestVerifyRejectsWrongSecret confirms a different key fails to
// authenticate even when the value is otherwise legal.
func TestVerifyRejectsWrongSecret(t *testing.T) {
	signed := gogo.SignCookieValue("alice", testCookieSecret)
	if _, ok := gogo.VerifyCookieValue(signed, testCookieSecretAlt); ok {
		t.Errorf("VerifyCookieValue accepted a signature from a different key")
	}
}

// TestVerifyRejectsTamperedValue confirms changing the payload after
// signing invalidates the cookie.
func TestVerifyRejectsTamperedValue(t *testing.T) {
	signed := gogo.SignCookieValue("alice", testCookieSecret)
	// Replace the value part with "bob" but keep the signature.
	sigIdx := strings.LastIndexByte(signed, '.')
	tampered := "bob" + signed[sigIdx:]
	if _, ok := gogo.VerifyCookieValue(tampered, testCookieSecret); ok {
		t.Errorf("VerifyCookieValue accepted tampered value")
	}
}

// TestVerifyRejectsTamperedSignature confirms a forged signature
// fails even when the value is unchanged.
func TestVerifyRejectsTamperedSignature(t *testing.T) {
	signed := gogo.SignCookieValue("alice", testCookieSecret)
	tampered := signed[:len(signed)-2] + "AA"
	if _, ok := gogo.VerifyCookieValue(tampered, testCookieSecret); ok {
		t.Errorf("VerifyCookieValue accepted a forged signature")
	}
}

// TestVerifyMalformedInputs covers a handful of pathological inputs
// the verifier must reject without panicking.
func TestVerifyMalformedInputs(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"no separator", "abc"},
		{"trailing separator", "abc."},
		{"leading separator", ".abc"},
		{"non base64 signature", "abc.not-base64!!"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := gogo.VerifyCookieValue(tc.input, testCookieSecret); ok {
				t.Errorf("expected !ok for %q", tc.input)
			}
		})
	}
}

// TestKeyRotation verifies the multi-secret path: sign with the OLD
// key and read back with the NEW key listed first (the deployment
// pattern during rotation).
func TestKeyRotation(t *testing.T) {
	oldKey, newKey := testCookieSecret, testCookieSecretAlt
	signed := gogo.SignCookieValue("session-123", oldKey)

	// New key listed first (the canonical "sign with new, accept old"
	// rotation order).
	got, ok := gogo.VerifyCookieValue(signed, newKey, oldKey)
	if !ok {
		t.Fatalf("rotation: failed to verify cookie signed with the old key")
	}
	if got != "session-123" {
		t.Errorf("rotation: recovered %q, want session-123", got)
	}

	// Once the old key is removed, the cookie should stop verifying.
	if _, ok := gogo.VerifyCookieValue(signed, newKey); ok {
		t.Errorf("rotation: stale cookie still validates after old key dropped")
	}
}

// TestSignEmptyValue signs an empty string. The resulting cookie
// carries only the signature segment after the separator, and
// VerifyCookieValue should recover the empty payload.
func TestSignEmptyValue(t *testing.T) {
	signed := gogo.SignCookieValue("", testCookieSecret)
	// Empty value still produces a "."-prefixed signature; our
	// verifier rejects a leading separator because we use
	// LastIndexByte and require dot > 0. Document the behavior
	// rather than silently round-tripping an empty payload.
	if _, ok := gogo.VerifyCookieValue(signed, testCookieSecret); ok {
		t.Errorf("VerifyCookieValue accepted an empty-payload cookie; current contract is to reject")
	}
}

// TestSignNoSecretsPanic ensures we fail loudly when no secret is
// supplied — a signed cookie without a secret would be a security
// footgun.
func TestSignNoSecretsPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty secrets")
		}
	}()
	_ = gogo.SignCookieValue("alice")
}

func TestSignEmptySecretPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty secret")
		}
	}()
	_ = gogo.SignCookieValue("alice", "")
}

func TestSignWeakSecretPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on weak secret")
		}
	}()
	_ = gogo.SignCookieValue("alice", "too-short")
}

// TestVerifyNoSecretsReturnsFalse confirms verification with zero
// secrets returns a clean ("", false) — no panic.
func TestVerifyNoSecretsReturnsFalse(t *testing.T) {
	signed := gogo.SignCookieValue("alice", testCookieSecret)
	if _, ok := gogo.VerifyCookieValue(signed); ok {
		t.Errorf("VerifyCookieValue accepted with no secrets")
	}
}

func TestVerifyEmptySecretReturnsFalse(t *testing.T) {
	mac := hmac.New(sha256.New, nil)
	mac.Write([]byte("admin"))
	forged := "admin." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if _, ok := gogo.VerifyCookieValue(forged, ""); ok {
		t.Errorf("VerifyCookieValue accepted an empty secret")
	}
}

func TestVerifyWeakSecretReturnsFalse(t *testing.T) {
	mac := hmac.New(sha256.New, []byte("too-short"))
	mac.Write([]byte("admin"))
	forged := "admin." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if _, ok := gogo.VerifyCookieValue(forged, "too-short"); ok {
		t.Errorf("VerifyCookieValue accepted a weak secret")
	}
}

// TestSetCookieSignedRoundTripsViaHTTP exercises the convenience
// methods through a real request: SetCookieSigned writes a Set-Cookie
// header, the client echoes it back, and CookieSigned verifies the
// signature on the next request.
func TestSetCookieSignedRoundTripsViaHTTP(t *testing.T) {
	const secret = testCookieSecret

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/issue", func(res *gogo.Response, req *gogo.Request) {
			res.SetCookieSigned(gogo.Cookie{
				Name:  "session",
				Value: "user-42",
				Path:  "/",
			}, secret)
			res.Send(200, "text/plain", "issued")
		})
		app.Get("/whoami", func(res *gogo.Response, req *gogo.Request) {
			val, ok := req.CookieSigned("session", secret)
			if !ok {
				res.Send(401, "text/plain", "unauthorized")
				return
			}
			res.Send(200, "text/plain", val)
		})
	})
	defer teardown()

	// Step 1: GET /issue → server sets a signed cookie.
	jar, err := newCookieJar()
	if err != nil {
		t.Fatalf("cookie jar: %v", err)
	}
	client := &http.Client{Jar: jar}

	r1, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/issue", port))
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	r1.Body.Close()
	setCookie := r1.Header.Get("Set-Cookie")
	if !strings.Contains(setCookie, "session=user-42.") {
		t.Errorf("Set-Cookie %q does not embed the signed payload as expected", setCookie)
	}

	// Step 2: GET /whoami → client echoes the cookie, server verifies.
	r2, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/whoami", port))
	if err != nil {
		t.Fatalf("whoami: %v", err)
	}
	body := readAllString(t, r2)
	r2.Body.Close()
	if r2.StatusCode != 200 {
		t.Errorf("whoami status = %d, want 200", r2.StatusCode)
	}
	if body != "user-42" {
		t.Errorf("whoami body = %q, want user-42", body)
	}
}

// TestCookieSignedRejectsForgery: a hand-crafted cookie that pairs
// the real value with a bogus signature should fail verification.
func TestCookieSignedRejectsForgery(t *testing.T) {
	const secret = testCookieSecret

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/whoami", func(res *gogo.Response, req *gogo.Request) {
			_, ok := req.CookieSigned("session", secret)
			if !ok {
				res.Send(401, "text/plain", "unauthorized")
				return
			}
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/whoami", port), nil)
	req.Header.Set("Cookie", "session=admin.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("forged cookie accepted: status = %d, want 401", resp.StatusCode)
	}
}

// TestCookieSignedMissing returns false when the cookie isn't set
// at all.
func TestCookieSignedMissing(t *testing.T) {
	const secret = testCookieSecret

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			_, ok := req.CookieSigned("nope", secret)
			if ok {
				res.Send(500, "text/plain", "expected false")
				return
			}
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/x", port))
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}
