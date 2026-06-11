package middleware

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"math"
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
	_, err := verifyJWT(strings.Repeat("a", 32)+".b.c", verifier, "HS256", 0, 16, jwtClaimValidation{})
	if err == nil || err.Error() != "token too large" {
		t.Fatalf("verifyJWT error = %v, want token too large", err)
	}
}

func TestVerifyJWTMaxTokenBytesCanBeDisabled(t *testing.T) {
	verifier := func(signingInput, signature []byte) error {
		return errors.New("verifier should not run before header decode")
	}
	_, err := verifyJWT(strings.Repeat("a", 32)+".b.c", verifier, "HS256", time.Second, NoJWTTokenLimit, jwtClaimValidation{})
	if err == nil || err.Error() == "token too large" {
		t.Fatalf("verifyJWT error = %v, want non-size parse error", err)
	}
}

func TestVerifyJWTPreservesValidTokenWithoutClaimValidation(t *testing.T) {
	tok := signJWTForVerifyTest(t, map[string]any{
		"sub": "user-42",
	})

	claims, err := verifySignedJWTForTest(t, tok, jwtClaimValidation{})
	if err != nil {
		t.Fatalf("verifyJWT: %v", err)
	}
	if claims["sub"] != "user-42" {
		t.Fatalf("sub claim = %v, want user-42", claims["sub"])
	}
}

func TestVerifyJWTAcceptsIssuerAudienceAndRequiredClaims(t *testing.T) {
	tok := signJWTForVerifyTest(t, map[string]any{
		"iss":   "https://issuer.example",
		"aud":   "api",
		"sub":   "user-42",
		"scope": "read:things",
	})

	_, err := verifySignedJWTForTest(t, tok, jwtClaimValidation{
		issuer:         "https://issuer.example",
		audience:       "api",
		requiredClaims: []string{"sub", "scope"},
	})
	if err != nil {
		t.Fatalf("verifyJWT: %v", err)
	}
}

func TestVerifyJWTAcceptsAudienceArray(t *testing.T) {
	tok := signJWTForVerifyTest(t, map[string]any{
		"aud": []string{"cli", "api"},
	})

	_, err := verifySignedJWTForTest(t, tok, jwtClaimValidation{audience: "api"})
	if err != nil {
		t.Fatalf("verifyJWT: %v", err)
	}
}

func TestVerifyJWTRejectsInvalidIssuerClaims(t *testing.T) {
	tests := []struct {
		name    string
		claims  map[string]any
		wantErr string
	}{
		{
			name:    "missing",
			claims:  map[string]any{"sub": "user-42"},
			wantErr: "missing iss claim",
		},
		{
			name:    "non-string",
			claims:  map[string]any{"iss": 42},
			wantErr: "malformed iss claim",
		},
		{
			name:    "mismatch",
			claims:  map[string]any{"iss": "https://issuer.invalid"},
			wantErr: "invalid issuer",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tok := signJWTForVerifyTest(t, tc.claims)
			_, err := verifySignedJWTForTest(t, tok, jwtClaimValidation{
				issuer: "https://issuer.example",
			})
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("verifyJWT error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestVerifyJWTRejectsInvalidAudienceClaims(t *testing.T) {
	tests := []struct {
		name    string
		claims  map[string]any
		wantErr string
	}{
		{
			name:    "missing",
			claims:  map[string]any{"sub": "user-42"},
			wantErr: "missing aud claim",
		},
		{
			name:    "non-string",
			claims:  map[string]any{"aud": 42},
			wantErr: "malformed aud claim",
		},
		{
			name:    "array contains non-string",
			claims:  map[string]any{"aud": []any{"api", 42}},
			wantErr: "malformed aud claim",
		},
		{
			name:    "mismatch",
			claims:  map[string]any{"aud": []string{"cli", "worker"}},
			wantErr: "invalid audience",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tok := signJWTForVerifyTest(t, tc.claims)
			_, err := verifySignedJWTForTest(t, tok, jwtClaimValidation{
				audience: "api",
			})
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("verifyJWT error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestVerifyJWTRejectsMissingRequiredClaims(t *testing.T) {
	tests := []struct {
		name           string
		claims         map[string]any
		requiredClaims []string
		wantErr        string
	}{
		{
			name:           "missing",
			claims:         map[string]any{"sub": "user-42"},
			requiredClaims: []string{"sub", "scope"},
			wantErr:        "missing required claim: scope",
		},
		{
			name:           "null",
			claims:         map[string]any{"sub": nil},
			requiredClaims: []string{"sub"},
			wantErr:        "missing required claim: sub",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tok := signJWTForVerifyTest(t, tc.claims)
			_, err := verifySignedJWTForTest(t, tok, jwtClaimValidation{
				requiredClaims: tc.requiredClaims,
			})
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("verifyJWT error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestJWTRejectsEmptyRequiredClaimName(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("JWT accepted an empty required claim name")
		}
	}()
	_ = JWT(JWTOptions{
		Secret:         []byte("test-secret-with-enough-bytes-AAAA"),
		RequiredClaims: []string{""},
	})
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

func TestJWTRejectsWeakHMACSecret(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("JWT accepted a weak HMAC secret")
		}
	}()
	_ = JWT(JWTOptions{Secret: []byte("too-short")})
}

func TestSignJWTRejectsWeakHMACSecret(t *testing.T) {
	if _, err := SignJWT(JWTHS256, []byte("too-short"), map[string]any{"sub": "x"}); err == nil {
		t.Fatal("SignJWT accepted a weak HMAC secret")
	}
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

func TestVerifyJWTRejectsOutOfRangeTimeClaims(t *testing.T) {
	tests := []struct {
		name    string
		claims  map[string]any
		wantErr string
	}{
		{
			name:    "negative exp",
			claims:  map[string]any{"exp": -1},
			wantErr: "malformed exp claim",
		},
		{
			name:    "huge exp",
			claims:  map[string]any{"exp": 1e300},
			wantErr: "malformed exp claim",
		},
		{
			name:    "negative nbf",
			claims:  map[string]any{"nbf": -1e300},
			wantErr: "malformed nbf claim",
		},
		{
			name:    "huge nbf",
			claims:  map[string]any{"nbf": 1e300},
			wantErr: "malformed nbf claim",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tok := signJWTForVerifyTest(t, tc.claims)
			_, err := verifySignedJWTForTest(t, tok, jwtClaimValidation{})
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("verifyJWT error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestJWTNumericDateRejectsNaN(t *testing.T) {
	if _, ok := jwtNumericDate(math.NaN()); ok {
		t.Fatal("jwtNumericDate(NaN) accepted, want rejection")
	}
	if _, ok := jwtNumericDate(0); !ok {
		t.Fatal("jwtNumericDate(0) rejected, want acceptance")
	}
}

func signJWTForVerifyTest(t *testing.T, claims map[string]any) string {
	t.Helper()
	tok, err := SignJWT(JWTHS256, []byte("test-secret-with-enough-bytes-AAAA"), claims)
	if err != nil {
		t.Fatalf("SignJWT: %v", err)
	}
	return tok
}

func verifySignedJWTForTest(t *testing.T, tok string, validation jwtClaimValidation) (map[string]any, error) {
	t.Helper()
	info, ok := jwtAlgInfoFor(JWTHS256)
	if !ok {
		t.Fatal("missing HS256 algorithm info")
	}
	verifier, err := jwtBuildVerifier(info, []byte("test-secret-with-enough-bytes-AAAA"), nil)
	if err != nil {
		t.Fatalf("jwtBuildVerifier: %v", err)
	}
	return verifyJWT(tok, verifier, string(JWTHS256), 0, defaultJWTMaxTokenBytes, validation)
}
