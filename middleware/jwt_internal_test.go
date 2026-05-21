package middleware

import (
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
