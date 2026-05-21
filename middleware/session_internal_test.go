package middleware

import (
	"testing"

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
