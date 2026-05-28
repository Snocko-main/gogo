package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultRateLimitPrefix  = "gogo:rl:"
	defaultRateLimitTimeout = 100 * time.Millisecond
)

// rateLimitScript atomically increments the per-key counter for the current
// fixed window and returns {count, ttlMillis}. The TTL is set only on the
// first hit of a window (count == 1) so the window rolls forward exactly once
// and resets on its own boundary — matching the semantics of the in-memory
// MemoryRateLimitStore. If a key somehow lost its TTL (PTTL < 0) the expiry is
// re-applied defensively so a counter can never leak forever.
var rateLimitScript = goredis.NewScript(`
local count = redis.call('INCR', KEYS[1])
if count == 1 then
	redis.call('PEXPIRE', KEYS[1], ARGV[1])
	return {count, tonumber(ARGV[1])}
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 0 then
	redis.call('PEXPIRE', KEYS[1], ARGV[1])
	ttl = tonumber(ARGV[1])
end
return {count, ttl}
`)

// RateLimitOptions configures a Redis-backed RateLimitStore.
type RateLimitOptions struct {
	// URL is parsed with redis.ParseURL. Example: redis://localhost:6379/0
	URL string

	// Addr/Username/Password/DB are used when URL is empty.
	Addr     string
	Username string
	Password string
	DB       int

	// KeyPrefix is prepended to every counter key. Defaults to "gogo:rl:".
	// Use a distinct prefix per application when sharing a Redis instance.
	KeyPrefix string

	// Timeout bounds each Hit's Redis round-trip. Defaults to 100ms. A tight
	// timeout keeps a slow or unreachable Redis from stalling request handling;
	// pair it with the FailOpen/FailClosed policy below. Negative disables the
	// per-call deadline.
	Timeout time.Duration

	// FailClosed selects the behaviour when the Redis round-trip fails (timeout,
	// connection error, malformed reply). The zero value fails OPEN: the request
	// is allowed through (the limiter never takes the site down because the
	// backing store blipped). Set FailClosed to reject instead — every request
	// is treated as over-limit until Redis recovers.
	FailClosed bool

	// OnError, when non-nil, is invoked with the underlying error whenever a Hit
	// falls back to the FailOpen/FailClosed policy. Useful for metrics/logging.
	OnError func(error)
}

// RateLimitStore is a Redis-backed middleware.RateLimitStore. It shares
// fixed-window counters across every process pointed at the same Redis, so a
// fleet of gogo instances enforces one global quota per key instead of one per
// node.
//
// Because the middleware.RateLimitStore.Hit signature returns no error, Redis
// failures are absorbed here according to the FailOpen/FailClosed policy and
// surfaced through OnError. Always register the middleware with
// RateLimitOptions.AsyncStore set to true so the Redis round-trip runs on a
// worker goroutine rather than the uWS event-loop thread.
type RateLimitStore struct {
	client     goredis.UniversalClient
	own        bool
	prefix     string
	timeout    time.Duration
	failClosed bool
	onError    func(error)
}

// Compile-time check that RateLimitStore satisfies middleware.RateLimitStore
// without importing the middleware package (keeps the adapter decoupled).
var _ interface {
	Hit(key string, window time.Duration) (count int, resetAt time.Time)
} = (*RateLimitStore)(nil)

// NewRateLimitStore creates a Redis-backed rate-limit store, opening and owning
// a new Redis client from the supplied options. Close releases that client.
func NewRateLimitStore(opt RateLimitOptions) (*RateLimitStore, error) {
	var client goredis.UniversalClient
	if opt.URL != "" {
		parsed, err := goredis.ParseURL(opt.URL)
		if err != nil {
			return nil, err
		}
		client = goredis.NewClient(parsed)
	} else {
		addr := opt.Addr
		if addr == "" {
			addr = "localhost:6379"
		}
		client = goredis.NewClient(&goredis.Options{
			Addr:     addr,
			Username: opt.Username,
			Password: opt.Password,
			DB:       opt.DB,
		})
	}
	s := newRateLimitStore(client, opt)
	s.own = true
	return s, nil
}

// NewRateLimitStoreClient wraps an existing Redis client with default options.
// The store does not close client on Close.
func NewRateLimitStoreClient(client goredis.UniversalClient, keyPrefix string) (*RateLimitStore, error) {
	return NewRateLimitStoreClientOptions(client, RateLimitOptions{KeyPrefix: keyPrefix})
}

// NewRateLimitStoreClientOptions wraps an existing Redis client with full
// options. URL, Addr, Username, Password, and DB are ignored because client is
// supplied. The store does not close client on Close.
func NewRateLimitStoreClientOptions(client goredis.UniversalClient, opt RateLimitOptions) (*RateLimitStore, error) {
	if client == nil {
		return nil, errors.New("gogo/adapters/redis: nil Redis client")
	}
	return newRateLimitStore(client, opt), nil
}

func newRateLimitStore(client goredis.UniversalClient, opt RateLimitOptions) *RateLimitStore {
	prefix := opt.KeyPrefix
	if prefix == "" {
		prefix = defaultRateLimitPrefix
	}
	timeout := opt.Timeout
	if timeout == 0 {
		timeout = defaultRateLimitTimeout
	}
	return &RateLimitStore{
		client:     client,
		prefix:     prefix,
		timeout:    timeout,
		failClosed: opt.FailClosed,
		onError:    opt.OnError,
	}
}

// Hit implements middleware.RateLimitStore. It atomically increments the
// shared counter for key in the current window and returns the new count and
// the time the window resets. On any Redis failure it applies the configured
// fail-open / fail-closed policy.
func (s *RateLimitStore) Hit(key string, window time.Duration) (int, time.Time) {
	now := time.Now()

	ctx := context.Background()
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	windowMs := window.Milliseconds()
	if windowMs < 1 {
		windowMs = 1
	}

	res, err := rateLimitScript.Run(ctx, s.client, []string{s.prefix + key}, windowMs).Result()
	if err != nil {
		return s.onFailure(err, now, window)
	}
	count, ttlMs, err := parseRateLimitResult(res)
	if err != nil {
		return s.onFailure(err, now, window)
	}
	if ttlMs < 0 {
		ttlMs = windowMs
	}
	return count, now.Add(time.Duration(ttlMs) * time.Millisecond)
}

// Close releases the underlying Redis client when the store owns it (created
// via NewRateLimitStore). For stores wrapping a caller-supplied client it is a
// no-op so the caller keeps control of the connection's lifecycle.
func (s *RateLimitStore) Close() error {
	if s == nil || !s.own || s.client == nil {
		return nil
	}
	return s.client.Close()
}

// onFailure applies the fail-open / fail-closed policy and reports the error.
func (s *RateLimitStore) onFailure(err error, now time.Time, window time.Duration) (int, time.Time) {
	if s.onError != nil {
		s.onError(err)
	}
	if s.failClosed {
		// Report a count guaranteed to exceed any positive Max so the
		// middleware rejects the request while Redis is unavailable.
		return math.MaxInt32, now.Add(window)
	}
	// Fail open: report a single hit so the request is allowed through.
	return 1, now.Add(window)
}

func parseRateLimitResult(res any) (count int, ttlMs int64, err error) {
	vals, ok := res.([]any)
	if !ok || len(vals) != 2 {
		return 0, 0, fmt.Errorf("gogo/adapters/redis: unexpected rate limit result %T", res)
	}
	c, err := toInt64(vals[0])
	if err != nil {
		return 0, 0, err
	}
	ttl, err := toInt64(vals[1])
	if err != nil {
		return 0, 0, err
	}
	return int(c), ttl, nil
}

func toInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	default:
		return 0, fmt.Errorf("gogo/adapters/redis: unexpected numeric type %T", v)
	}
}
