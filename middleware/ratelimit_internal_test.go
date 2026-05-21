package middleware

import (
	"testing"
	"time"

	"github.com/Snocko-main/gogo/internal/mwhint"
)

func TestRateLimitDisabledUsesBothPlacement(t *testing.T) {
	for _, opt := range []RateLimitOptions{
		{},
		{Max: 10},
		{Window: time.Minute},
	} {
		h := RateLimit(opt)
		if h.Place != mwhint.Both {
			t.Fatalf("disabled RateLimit placement = %v, want Both", h.Place)
		}
	}
}

func TestRateLimitConfiguredUsesSyncPlacement(t *testing.T) {
	h := RateLimit(RateLimitOptions{Max: 10, Window: time.Minute})
	if h.Place != mwhint.Sync {
		t.Fatalf("configured RateLimit placement = %v, want Sync", h.Place)
	}
}

func TestRateLimitAsyncStoreUsesAsyncPlacement(t *testing.T) {
	h := RateLimit(RateLimitOptions{Max: 10, Window: time.Minute, AsyncStore: true})
	if h.Place != mwhint.Async {
		t.Fatalf("AsyncStore RateLimit placement = %v, want Async", h.Place)
	}
}
