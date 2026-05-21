package middleware

import (
	"strconv"
	"sync"
	"time"

	"github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/internal/mwhint"
)

// RateLimitOptions configures the in-memory fixed-window rate limiter.
// Zero value means "no limit" — at minimum Max and Window must be set.
type RateLimitOptions struct {
	// Max is the number of requests allowed per Window per key.
	// Required; zero disables the middleware (it passes everything
	// through).
	Max int

	// Window is the fixed window length. Counters reset at each
	// boundary. Required.
	Window time.Duration

	// KeyFunc derives the bucket key from the request. Default
	// req.IP(). Override to rate-limit by user ID, API key, etc.
	KeyFunc func(*gogo.Request) string

	// SkipFunc, when non-nil and returning true, bypasses the limit
	// entirely. Useful for whitelisting health-check probes or
	// authenticated admin traffic.
	SkipFunc func(*gogo.Request) bool

	// OnLimit, when non-nil, is invoked instead of the default 429
	// Too Many Requests response when a key is over its limit. The
	// middleware has already attached the X-RateLimit-* and
	// Retry-After headers before calling OnLimit.
	OnLimit gogo.Handler

	// Store overrides the in-memory bucket store. Empty default is a
	// process-local map[string]*rateBucket guarded by a mutex.
	// Provide an implementation backed by Redis / Memcache for
	// distributed deployments.
	Store RateLimitStore

	// MaxBuckets caps the in-memory store's bucket count to bound
	// memory growth under high key cardinality (e.g. attacker spam
	// with a unique key per request). When the cap is hit the store
	// sweeps expired buckets first; if that still doesn't free space,
	// the OLDEST remaining bucket (by resetAt) is evicted to make
	// room. Zero (default) means 100_000 — enough for legitimate
	// fleets, low enough that worst-case memory stays under ~10 MiB.
	// Negative disables the cap (not recommended outside tests).
	// Only consulted when Store is the default MemoryRateLimitStore.
	MaxBuckets int
}

// RateLimitStore abstracts the bucket backend so production
// deployments can swap in Redis / Memcache for shared counters
// across instances.
type RateLimitStore interface {
	// Hit increments the counter for key in the current window and
	// returns the new count and the time the window resets. If the
	// window has rolled over, the store should reset to 1.
	Hit(key string, window time.Duration) (count int, resetAt time.Time)
}

// RateLimit returns a middleware that enforces a fixed-window per-key
// quota. When a key exceeds Max requests within Window, subsequent
// requests are short-circuited with 429 Too Many Requests and a
// Retry-After header. The default key is the client IP; override
// KeyFunc to rate-limit by user, API key, etc.
//
// The middleware also emits the conventional informational headers
// on every response:
//
//	X-RateLimit-Limit      — configured Max
//	X-RateLimit-Remaining  — requests left in the current window
//	X-RateLimit-Reset      — Unix timestamp when the window resets
//
// In-memory backing is single-process. For multi-instance fleets
// supply a Store implementation that consults a shared backend.
func RateLimit(opt RateLimitOptions) mwhint.Hinted {
	if opt.Max <= 0 || opt.Window <= 0 {
		// No-op pass-through when not configured.
		return mwhint.Hinted{Place: mwhint.Sync, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler { return next })}
	}
	if opt.KeyFunc == nil {
		opt.KeyFunc = func(req *gogo.Request) string { return req.IP() }
	}
	if opt.Store == nil {
		mem := NewMemoryRateLimitStore()
		switch {
		case opt.MaxBuckets == 0:
			mem.maxBuckets = 100_000
		case opt.MaxBuckets > 0:
			mem.maxBuckets = opt.MaxBuckets
		default:
			mem.maxBuckets = 0 // negative → disabled
		}
		opt.Store = mem
	}
	maxStr := strconv.Itoa(opt.Max)

	return mwhint.Hinted{Place: mwhint.Sync, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			if opt.SkipFunc != nil && opt.SkipFunc(req) {
				next(res, req)
				return
			}
			key := opt.KeyFunc(req)
			count, resetAt := opt.Store.Hit(key, opt.Window)
			remaining := opt.Max - count
			if remaining < 0 {
				remaining = 0
			}
			res.Header("X-RateLimit-Limit", maxStr)
			res.Header("X-RateLimit-Remaining", strconv.Itoa(remaining))
			res.Header("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))

			if count > opt.Max {
				retryAfter := int(time.Until(resetAt).Seconds())
				if retryAfter < 1 {
					retryAfter = 1
				}
				res.Header("Retry-After", strconv.Itoa(retryAfter))
				if opt.OnLimit != nil {
					opt.OnLimit(res, req)
					return
				}
				res.Send(429, "text/plain; charset=utf-8", "Too Many Requests\n")
				return
			}
			next(res, req)
		}
	})}
}

// MemoryRateLimitStore is the default in-memory backend for RateLimit.
// Counters are kept in a map[string]*rateBucket protected by a single
// mutex; suitable for a single process at moderate QPS.
//
// Memory is bounded by maxBuckets (set via RateLimitOptions.MaxBuckets,
// default 100_000): when the cap is reached Hit first sweeps expired
// buckets, then evicts the oldest-resetAt remaining bucket to make
// room — so the worst-case footprint stays predictable even under
// high-cardinality key spam (per-user-agent, per-token, etc.).
//
// GC may also be called manually to reclaim space before the cap is
// hit; otherwise the lazy sweep inside Hit covers the common case.
//
// # Scale guidance
//
// Every Hit acquires a single mutex. On a single host with one or two
// dozen cores this is fine well past 50 k RPS — the critical section
// is < 1 µs (map probe + count++) and Go's mutex is fair under
// moderate contention. Above ~100 k RPS the lock starts to show up in
// CPU profiles; above ~250 k RPS it dominates.
//
// Beyond those thresholds, prefer:
//
//   - A Redis-backed Store for shared counters across a fleet. The
//     network round-trip costs more per call but parallelizes across
//     cores, and you can swap atomic.Int64 in Redis or use INCR with
//     EXPIRE for the same algorithm.
//   - Shard your own map[string]*rateBucket across N independent
//     stores keyed by hash(key) % N. Each shard has its own mutex.
//     Useful when you must stay in-process but contention shows up
//     in profiles.
type MemoryRateLimitStore struct {
	mu         sync.Mutex
	buckets    map[string]*rateBucket
	maxBuckets int // 0 = unbounded; set by RateLimit constructor
}

type rateBucket struct {
	count   int
	resetAt time.Time
}

// NewMemoryRateLimitStore returns an empty in-memory store ready for
// use as RateLimitOptions.Store. The store is unbounded by default
// when used directly; routing it through RateLimit applies the
// MaxBuckets cap (default 100_000) automatically.
func NewMemoryRateLimitStore() *MemoryRateLimitStore {
	return &MemoryRateLimitStore{buckets: make(map[string]*rateBucket)}
}

// Hit implements RateLimitStore. The window rolls forward to
// now+window the first time a key is seen, and on every reset crossing
// thereafter. When maxBuckets is configured and the store is at
// capacity for a brand-new key, expired buckets are reclaimed first;
// if that doesn't free space the oldest-resetAt bucket is evicted.
func (s *MemoryRateLimitStore) Hit(key string, window time.Duration) (int, time.Time) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.buckets[key]
	if !ok || now.After(b.resetAt) {
		if !ok && s.maxBuckets > 0 && len(s.buckets) >= s.maxBuckets {
			s.evictLocked(now)
		}
		b = &rateBucket{count: 1, resetAt: now.Add(window)}
		s.buckets[key] = b
		return 1, b.resetAt
	}
	b.count++
	return b.count, b.resetAt
}

// evictLocked frees one slot using approximate-LRU random sampling.
// Caller must hold s.mu.
//
// A full O(N) scan over a 100k+ bucket map under the store lock was
// a CPU/latency spike waiting to happen — every "new key" Hit while
// the cap was binding would stall every other goroutine touching
// the limiter for ~milliseconds. The fix here is the same trick
// Redis uses for its allkeys-lru policy: sample a small random
// subset, evict the oldest from the sample. Go's map iteration is
// randomized, so the first K visits constitute a uniform sample
// without any explicit shuffle.
//
// During the sample pass we also opportunistically clear any
// expired entries we happen to land on — that's free reclamation
// on the same scan. Stop the moment we've freed a slot (a single
// expired delete satisfies the caller); otherwise fall through to
// evicting the oldest non-expired entry from the sample.
func (s *MemoryRateLimitStore) evictLocked(now time.Time) {
	const sampleSize = 32
	var oldestKey string
	var oldestReset time.Time
	sampled := 0
	for k, b := range s.buckets {
		if now.After(b.resetAt) {
			delete(s.buckets, k)
			if s.maxBuckets <= 0 || len(s.buckets) < s.maxBuckets {
				return
			}
			continue
		}
		if oldestKey == "" || b.resetAt.Before(oldestReset) {
			oldestKey = k
			oldestReset = b.resetAt
		}
		sampled++
		if sampled >= sampleSize {
			break
		}
	}
	if oldestKey != "" && (s.maxBuckets <= 0 || len(s.buckets) >= s.maxBuckets) {
		delete(s.buckets, oldestKey)
	}
}

// GC removes buckets whose window has already expired. Call
// periodically if your key cardinality is unbounded and you want
// to cap memory. Returns the number of buckets reclaimed.
func (s *MemoryRateLimitStore) GC() int {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for k, b := range s.buckets {
		if now.After(b.resetAt) {
			delete(s.buckets, k)
			removed++
		}
	}
	return removed
}
