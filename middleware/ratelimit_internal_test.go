package middleware

import (
	"testing"
	"time"

	"github.com/Snocko-main/gogo/internal/mwhint"
)

func TestRateLimitDisabledUsesBothPlacement(t *testing.T) {
	h := RateLimit(RateLimitOptions{})
	if h.Place != mwhint.Both {
		t.Fatalf("disabled RateLimit placement = %v, want Both", h.Place)
	}
}

func TestRateLimitRejectsPartialOrInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		opt  RateLimitOptions
	}{
		{name: "max without window", opt: RateLimitOptions{Max: 10}},
		{name: "window without max", opt: RateLimitOptions{Window: time.Minute}},
		{name: "negative max", opt: RateLimitOptions{Max: -1, Window: time.Minute}},
		{name: "negative window", opt: RateLimitOptions{Max: 10, Window: -time.Second}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("RateLimit did not panic")
				}
			}()
			_ = RateLimit(tc.opt)
		})
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
