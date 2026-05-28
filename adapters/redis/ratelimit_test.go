package redis

import (
	"errors"
	"math"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func TestNewRateLimitStoreClientNilClient(t *testing.T) {
	if _, err := NewRateLimitStoreClient(nil, "app:"); err == nil {
		t.Fatal("NewRateLimitStoreClient(nil) succeeded, want error")
	}
}

func TestRateLimitStoreDefaults(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer client.Close()
	store, err := NewRateLimitStoreClientOptions(client, RateLimitOptions{})
	if err != nil {
		t.Fatalf("NewRateLimitStoreClientOptions: %v", err)
	}
	if store.prefix != defaultRateLimitPrefix {
		t.Fatalf("prefix = %q, want %q", store.prefix, defaultRateLimitPrefix)
	}
	if store.timeout != defaultRateLimitTimeout {
		t.Fatalf("timeout = %v, want %v", store.timeout, defaultRateLimitTimeout)
	}
	if store.failClosed {
		t.Fatal("failClosed = true, want false (default fail open)")
	}
	if store.own {
		t.Fatal("own = true for caller-supplied client, want false")
	}
}

func TestRateLimitStoreCustomOptions(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer client.Close()
	store, err := NewRateLimitStoreClientOptions(client, RateLimitOptions{
		KeyPrefix:  "custom:",
		Timeout:    2 * time.Second,
		FailClosed: true,
	})
	if err != nil {
		t.Fatalf("NewRateLimitStoreClientOptions: %v", err)
	}
	if store.prefix != "custom:" {
		t.Fatalf("prefix = %q, want custom:", store.prefix)
	}
	if store.timeout != 2*time.Second {
		t.Fatalf("timeout = %v, want 2s", store.timeout)
	}
	if !store.failClosed {
		t.Fatal("failClosed = false, want true")
	}
}

func TestRateLimitStoreClientDoesNotOwn(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer client.Close()
	store, err := NewRateLimitStoreClient(client, "app:")
	if err != nil {
		t.Fatalf("NewRateLimitStoreClient: %v", err)
	}
	// Close must be a no-op for borrowed clients: the deferred client.Close
	// above must still succeed (a double close on go-redis returns nil, but a
	// borrowed store closing it would be a lifecycle bug regardless).
	if err := store.Close(); err != nil {
		t.Fatalf("Close on borrowed client: %v", err)
	}
}

func TestRateLimitOnFailureFailOpen(t *testing.T) {
	var seen error
	store := &RateLimitStore{onError: func(err error) { seen = err }}
	now := time.Now()
	count, resetAt := store.onFailure(errors.New("boom"), now, time.Minute)
	if count != 1 {
		t.Fatalf("fail-open count = %d, want 1 (request allowed)", count)
	}
	if !resetAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("resetAt = %v, want %v", resetAt, now.Add(time.Minute))
	}
	if seen == nil {
		t.Fatal("OnError was not invoked")
	}
}

func TestRateLimitOnFailureFailClosed(t *testing.T) {
	store := &RateLimitStore{failClosed: true}
	now := time.Now()
	count, resetAt := store.onFailure(errors.New("boom"), now, time.Minute)
	if count != math.MaxInt32 {
		t.Fatalf("fail-closed count = %d, want MaxInt32 (request rejected)", count)
	}
	if !resetAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("resetAt = %v, want %v", resetAt, now.Add(time.Minute))
	}
}

func TestParseRateLimitResult(t *testing.T) {
	count, ttl, err := parseRateLimitResult([]any{int64(3), int64(45000)})
	if err != nil {
		t.Fatalf("parseRateLimitResult: %v", err)
	}
	if count != 3 || ttl != 45000 {
		t.Fatalf("got count=%d ttl=%d, want 3 / 45000", count, ttl)
	}

	for _, bad := range []any{
		"not a slice",
		[]any{int64(1)},
		[]any{int64(1), int64(2), int64(3)},
		[]any{nil, int64(2)},
	} {
		if _, _, err := parseRateLimitResult(bad); err == nil {
			t.Fatalf("parseRateLimitResult(%#v) succeeded, want error", bad)
		}
	}
}

func TestToInt64(t *testing.T) {
	cases := []struct {
		in   any
		want int64
	}{
		{int64(7), 7},
		{int(7), 7},
		{"7", 7},
	}
	for _, c := range cases {
		got, err := toInt64(c.in)
		if err != nil {
			t.Fatalf("toInt64(%#v): %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("toInt64(%#v) = %d, want %d", c.in, got, c.want)
		}
	}
	if _, err := toInt64(3.14); err == nil {
		t.Fatal("toInt64(float) succeeded, want error")
	}
}
