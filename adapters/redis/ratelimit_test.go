package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/Snocko-main/gogo/middleware"
	goredis "github.com/redis/go-redis/v9"
)

// Assert against the real interface here (test-only) so a signature change to
// middleware.RateLimitStore breaks the build without coupling the production
// adapter package to middleware.
var _ middleware.RateLimitStore = (*RateLimitStore)(nil)

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

func TestRateLimitStoreRedisIntegrationSharesCounters(t *testing.T) {
	client := redisIntegrationClient(t)
	defer client.Close()

	prefix := fmt.Sprintf("gogo:test:rl:%d:", time.Now().UnixNano())
	key := "shared-user"
	defer deleteRedisKeys(client, prefix+key)

	storeA, err := NewRateLimitStoreClientOptions(client, RateLimitOptions{
		KeyPrefix: prefix,
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewRateLimitStoreClientOptions A: %v", err)
	}
	storeB, err := NewRateLimitStoreClientOptions(client, RateLimitOptions{
		KeyPrefix: prefix,
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewRateLimitStoreClientOptions B: %v", err)
	}

	count, resetAt := storeA.Hit(key, time.Second)
	if count != 1 {
		t.Fatalf("first count = %d, want 1", count)
	}
	count, resetAt2 := storeB.Hit(key, time.Second)
	if count != 2 {
		t.Fatalf("second count = %d, want 2 shared through Redis", count)
	}
	if delta := resetAt2.Sub(resetAt); delta < -100*time.Millisecond || delta > 100*time.Millisecond {
		t.Fatalf("resetAt delta = %v, want same fixed window", delta)
	}
}

func TestRateLimitStoreRedisIntegrationExpiresWindow(t *testing.T) {
	client := redisIntegrationClient(t)
	defer client.Close()

	prefix := fmt.Sprintf("gogo:test:rl:%d:", time.Now().UnixNano())
	key := "expiring-user"
	defer deleteRedisKeys(client, prefix+key)

	store, err := NewRateLimitStoreClientOptions(client, RateLimitOptions{
		KeyPrefix: prefix,
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewRateLimitStoreClientOptions: %v", err)
	}

	if count, _ := store.Hit(key, 150*time.Millisecond); count != 1 {
		t.Fatalf("first count = %d, want 1", count)
	}
	if count, _ := store.Hit(key, 150*time.Millisecond); count != 2 {
		t.Fatalf("second count = %d, want 2", count)
	}
	time.Sleep(250 * time.Millisecond)
	if count, _ := store.Hit(key, 150*time.Millisecond); count != 1 {
		t.Fatalf("count after expiry = %d, want 1", count)
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
	if count != math.MaxInt {
		t.Fatalf("fail-closed count = %d, want MaxInt (request rejected)", count)
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

func redisIntegrationClient(t *testing.T) goredis.UniversalClient {
	t.Helper()

	var (
		client goredis.UniversalClient
		err    error
	)
	if url := os.Getenv("REDIS_URL"); url != "" {
		client, err = newRedisClient(url, "", "", "", 0)
		if err != nil {
			t.Fatalf("parse REDIS_URL: %v", err)
		}
	} else {
		client, err = newRedisClient("", "127.0.0.1:6379", "", "", 0)
		if err != nil {
			t.Fatalf("new Redis client: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("Redis integration skipped; no reachable Redis at REDIS_URL or 127.0.0.1:6379: %v", err)
	}
	return client
}

func deleteRedisKeys(client goredis.UniversalClient, keys ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = client.Del(ctx, keys...).Err()
}
