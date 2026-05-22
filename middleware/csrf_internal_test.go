package middleware

import (
	"strings"
	"testing"

	"github.com/Snocko-main/gogo"
)

func TestCSRFRejectsOverMaxTokenBytesBeforeDecode(t *testing.T) {
	secret := []byte("csrf-secret")
	tok, err := newCSRFToken(secret)
	if err != nil {
		t.Fatalf("newCSRFToken: %v", err)
	}
	if err := verifyCSRFToken(secret, tok, len(tok)); err != nil {
		t.Fatalf("verifyCSRFToken fresh token: %v", err)
	}
	if err := verifyCSRFToken(secret, tok, len(tok)-1); err == nil {
		t.Fatalf("verifyCSRFToken accepted token over max length")
	}
	if !csrfTokenTooLong(strings.Repeat("a", defaultCSRFMaxTokenBytes+1), defaultCSRFMaxTokenBytes) {
		t.Fatalf("csrfTokenTooLong did not reject oversized token")
	}
}

func TestCSRFValidatesOptionsAtConstruction(t *testing.T) {
	tests := []struct {
		name string
		opt  CSRFOptions
	}{
		{
			name: "bad cookie name",
			opt: CSRFOptions{
				Secret:     []byte("csrf-secret"),
				CookieName: "bad name",
			},
		},
		{
			name: "bad header name",
			opt: CSRFOptions{
				Secret:     []byte("csrf-secret"),
				HeaderName: "X-Bad\r\n",
			},
		},
		{
			name: "bad cookie path",
			opt: CSRFOptions{
				Secret:     []byte("csrf-secret"),
				CookiePath: "/; Domain=evil.example",
			},
		},
		{
			name: "bad cookie domain",
			opt: CSRFOptions{
				Secret:       []byte("csrf-secret"),
				CookieDomain: "example.com; Secure",
			},
		},
		{
			name: "bad samesite",
			opt: CSRFOptions{
				Secret:         []byte("csrf-secret"),
				CookieSameSite: gogo.SameSite("Lax; Domain=evil.example"),
			},
		},
		{
			name: "samesite none without secure",
			opt: CSRFOptions{
				Secret:         []byte("csrf-secret"),
				CookieSameSite: gogo.SameSiteNone,
			},
		},
		{
			name: "negative cookie max age",
			opt: CSRFOptions{
				Secret:       []byte("csrf-secret"),
				CookieMaxAge: -1,
			},
		},
		{
			name: "generated token cannot fit max",
			opt: CSRFOptions{
				Secret:        []byte("csrf-secret"),
				MaxTokenBytes: csrfGeneratedTokenBytes - 1,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("CSRF did not panic for invalid options")
				}
			}()
			_ = CSRF(tc.opt)
		})
	}
}
