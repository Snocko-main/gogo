package middleware

import (
	"testing"
	"time"

	"github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/internal/mwhint"
)

func TestSessionDefaultUsesBothPlacement(t *testing.T) {
	h := NewSession(SessionOptions{Secret: []byte("session-secret-32-bytes-AAAAAAAA")})
	if h.Place != mwhint.Both {
		t.Fatalf("default Session placement = %v, want Both", h.Place)
	}
}

func TestSessionAsyncStoreUsesAsyncPlacement(t *testing.T) {
	h := NewSession(SessionOptions{
		Secret:     []byte("session-secret-32-bytes-AAAAAAAA"),
		AsyncStore: true,
	})
	if h.Place != mwhint.Async {
		t.Fatalf("AsyncStore Session placement = %v, want Async", h.Place)
	}
}

func TestSessionEntryLimitDisableSentinel(t *testing.T) {
	h := NewSession(SessionOptions{
		Secret:     []byte("session-secret-32-bytes-AAAAAAAA"),
		MaxEntries: NoSessionEntryLimit,
	})
	if h.Place != mwhint.Both {
		t.Fatalf("Session with NoSessionEntryLimit placement = %v, want Both", h.Place)
	}
}

func TestSessionRejectsWeakSecret(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewSession accepted a weak HMAC secret")
		}
	}()
	_ = NewSession(SessionOptions{Secret: []byte("too-short")})
}

func TestSessionValidatesCookieOptionsAtConstruction(t *testing.T) {
	tests := []struct {
		name string
		opt  SessionOptions
	}{
		{
			name: "bad cookie name",
			opt: SessionOptions{
				Secret:     []byte("session-secret-32-bytes-AAAAAAAA"),
				CookieName: "bad name",
			},
		},
		{
			name: "bad cookie path",
			opt: SessionOptions{
				Secret:     []byte("session-secret-32-bytes-AAAAAAAA"),
				CookiePath: "/; Domain=evil.example",
			},
		},
		{
			name: "bad cookie domain",
			opt: SessionOptions{
				Secret:       []byte("session-secret-32-bytes-AAAAAAAA"),
				CookieDomain: "example.com; Secure",
			},
		},
		{
			name: "bad samesite",
			opt: SessionOptions{
				Secret:         []byte("session-secret-32-bytes-AAAAAAAA"),
				CookieSameSite: gogo.SameSite("Lax; Domain=evil.example"),
			},
		},
		{
			name: "negative ttl",
			opt: SessionOptions{
				Secret: []byte("session-secret-32-bytes-AAAAAAAA"),
				TTL:    -time.Second,
			},
		},
		{
			name: "samesite none without secure",
			opt: SessionOptions{
				Secret:         []byte("session-secret-32-bytes-AAAAAAAA"),
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

func TestPersistSessionClearsDirtyAfterSuccessfulSave(t *testing.T) {
	store := &countingSessionStore{}
	sess := &Session{
		ID:    "session-id",
		data:  map[string]any{"phase": "checkpoint"},
		dirty: true,
	}
	opt := SessionOptions{
		Store: store,
		TTL:   time.Minute,
	}

	persistSession(sess, opt)
	persistSession(sess, opt)

	if store.saves != 1 {
		t.Fatalf("Save calls = %d, want 1", store.saves)
	}
	if sess.dirty {
		t.Fatal("session remained dirty after successful save")
	}
}

type countingSessionStore struct {
	saves int
}

func (s *countingSessionStore) Load(string) (map[string]any, bool) {
	return nil, false
}

func (s *countingSessionStore) Save(string, map[string]any, time.Duration) error {
	s.saves++
	return nil
}

func (s *countingSessionStore) Delete(string) error {
	return nil
}
