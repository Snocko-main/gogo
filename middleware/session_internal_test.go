package middleware

import (
	"testing"

	"github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/internal/mwhint"
)

func TestSessionDefaultUsesBothPlacement(t *testing.T) {
	h := NewSession(SessionOptions{Secret: []byte("session-secret")})
	if h.Place != mwhint.Both {
		t.Fatalf("default Session placement = %v, want Both", h.Place)
	}
}

func TestSessionAsyncStoreUsesAsyncPlacement(t *testing.T) {
	h := NewSession(SessionOptions{
		Secret:     []byte("session-secret"),
		AsyncStore: true,
	})
	if h.Place != mwhint.Async {
		t.Fatalf("AsyncStore Session placement = %v, want Async", h.Place)
	}
}

func TestSessionValidatesCookieOptionsAtConstruction(t *testing.T) {
	tests := []struct {
		name string
		opt  SessionOptions
	}{
		{
			name: "bad cookie name",
			opt: SessionOptions{
				Secret:     []byte("session-secret"),
				CookieName: "bad name",
			},
		},
		{
			name: "bad cookie path",
			opt: SessionOptions{
				Secret:     []byte("session-secret"),
				CookiePath: "/; Domain=evil.example",
			},
		},
		{
			name: "bad cookie domain",
			opt: SessionOptions{
				Secret:       []byte("session-secret"),
				CookieDomain: "example.com; Secure",
			},
		},
		{
			name: "bad samesite",
			opt: SessionOptions{
				Secret:         []byte("session-secret"),
				CookieSameSite: gogo.SameSite("Lax; Domain=evil.example"),
			},
		},
		{
			name: "samesite none without secure",
			opt: SessionOptions{
				Secret:         []byte("session-secret"),
				CookieSameSite: gogo.SameSiteNone,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewSession did not panic for invalid options")
				}
			}()
			_ = NewSession(tc.opt)
		})
	}
}
