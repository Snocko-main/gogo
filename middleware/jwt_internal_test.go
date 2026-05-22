package middleware

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestJWTAuthParamEscapesChallengeValue(t *testing.T) {
	got := jwtAuthParam("bad \"token\"\\value\r\nnext")
	want := `bad \"token\"\\valuenext`
	if got != want {
		t.Fatalf("jwtAuthParam() = %q, want %q", got, want)
	}
}

func TestVerifyJWTRejectsOverMaxTokenBytes(t *testing.T) {
	verifier := func(signingInput, signature []byte) error { return nil }
	_, err := verifyJWT(strings.Repeat("a", 32)+".b.c", verifier, "HS256", 0, 16)
	if err == nil || err.Error() != "token too large" {
		t.Fatalf("verifyJWT error = %v, want token too large", err)
	}
}

func TestVerifyJWTMaxTokenBytesCanBeDisabled(t *testing.T) {
	verifier := func(signingInput, signature []byte) error {
		return errors.New("verifier should not run before header decode")
	}
	_, err := verifyJWT(strings.Repeat("a", 32)+".b.c", verifier, "HS256", time.Second, -1)
	if err == nil || err.Error() == "token too large" {
		t.Fatalf("verifyJWT error = %v, want non-size parse error", err)
	}
}

func TestJWTRejectsECDSACurveMismatch(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("JWT did not panic for ES256 with a P-384 key")
		}
	}()
	_ = JWT(JWTOptions{
		Algorithm: JWTES256,
		Key:       &priv.PublicKey,
	})
}

func TestSignJWTRejectsECDSACurveMismatch(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	if _, err := SignJWT(JWTES256, priv, map[string]any{"sub": "x"}); err == nil {
		t.Fatal("SignJWT accepted ES256 with a P-384 key")
	}
}
