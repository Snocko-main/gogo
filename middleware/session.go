package middleware

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"strings"
	"sync"
	"time"

	"github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/internal/mwhint"
)

// SessionLocalKey is the req.Local key carrying the *Session for the
// current request. Handlers read and mutate session data via:
//
//	sess, _ := req.Local(middleware.SessionLocalKey).(*middleware.Session)
//	sess.Set("user_id", 42)
const SessionLocalKey = "gogo.session"

// SessionStore is the persistence backend. The default in-memory
// implementation is suitable for single-process deployments; supply
// a Redis / SQL-backed implementation for horizontally scaled apps.
//
// Implementations must be safe for concurrent use from many
// goroutines.
type SessionStore interface {
	// Load returns the data map for id, or (nil, false) when the
	// id is unknown or expired. Implementations should treat
	// expired sessions as absent.
	Load(id string) (map[string]any, bool)

	// Save persists the data for id with the supplied TTL.
	// Implementations are free to round / truncate TTL.
	Save(id string, data map[string]any, ttl time.Duration) error

	// Delete removes the session.
	Delete(id string) error
}

// SessionOptions configures the Session middleware.
type SessionOptions struct {
	// Secret signs the session-id cookie with HMAC so it cannot be
	// forged by clients (and is bound to this server fleet). Required;
	// must be at least 32 bytes of entropy.
	Secret []byte

	// Store is the persistence backend. Default
	// NewMemorySessionStore() — single-process, lost on restart.
	Store SessionStore

	// CookieName is the Set-Cookie name carrying the session id.
	// Default "session".
	CookieName string

	// CookiePath / CookieDomain mirror the cookie attributes of
	// the same names. Path defaults to "/", Domain empty.
	CookiePath   string
	CookieDomain string

	// CookieSecure sets the Secure flag — should be true in
	// production so the session id never leaks over plain HTTP.
	CookieSecure bool

	// CookieSameSite sets SameSite. Default Lax.
	CookieSameSite gogo.SameSite

	// TTL is the session lifetime. Default 24 hours. Used for
	// both the cookie Max-Age and the Store's expiry hint.
	TTL time.Duration

	// SkipFunc, when non-nil and returning true, bypasses session
	// loading for that request — useful for completely
	// session-irrelevant routes (static assets) that don't want
	// to pay the store-read cost.
	SkipFunc func(*gogo.Request) bool

	// LocalKey overrides the req.Local key. Default
	// SessionLocalKey.
	LocalKey string

	// MaxEntries caps the in-memory store's bucket count to bound
	// memory growth under high session-id cardinality (long-running
	// servers with sustained signup traffic, or attackers spamming
	// new session cookies). When the cap is hit on a fresh Save,
	// the store sweeps expired entries first; if that still doesn't
	// free space, the OLDEST remaining entry (by expires) is
	// evicted to make room. Zero (default) means 100_000 — enough
	// for typical fleets, low enough that worst-case memory stays
	// under ~50 MiB even with rich session payloads.
	// NoSessionEntryLimit disables the cap (not recommended outside
	// tests).
	//
	// Only consulted when Store is the default MemorySessionStore.
	MaxEntries int

	// AsyncStore places the middleware in the async chain only. Enable
	// this when Store may block on Redis, SQL, disk, or network I/O so
	// Load / Save / Delete run on the worker goroutine instead of the
	// uWS event-loop thread.
	//
	// Async-placed middleware fires only on GetAsync / PostAsync routes;
	// sync routes do not see it. Keep the default false for the built-in
	// in-memory store or for fast non-blocking custom stores that must
	// protect sync routes too.
	AsyncStore bool
}

// Session is the per-request session handle handlers manipulate
// during a request. Mutations are persisted to the Store after the
// handler returns (via Response.OnFinish, which runs after any
// Response.Async goroutine completes); concurrent goroutines writing
// to the same handle race — sessions are intended to be used from
// the request goroutine only.
//
// Handlers that need state to land mid-flight (before the response
// is fully written) can call Save explicitly.
type Session struct {
	ID        string
	data      map[string]any
	dirty     bool
	destroyed bool
	// persist is the per-request closure that writes Session state
	// back to the configured Store. Set by loadOrCreateSession.
	// Captured by Save and by the middleware's deferred OnFinish.
	persist       func(s *Session)
	expire        func()
	rotate        func(s *Session)
	rotateOnWrite bool
}

// NoSessionEntryLimit disables the in-memory session entry cap. This is
// intended for tests only; production deployments should keep the cap enabled
// or use a bounded external store such as Redis.
const NoSessionEntryLimit = -1

// Get returns the value at key, or nil when absent.
func (s *Session) Get(key string) any {
	if s.data == nil {
		return nil
	}
	return s.data[key]
}

// Set writes value at key and marks the session for save.
//
// Call Set / Delete BEFORE the response begins streaming (i.e. before
// res.Send / Status / Write / Stream / SSE start emitting bytes). If
// the session was loaded from a stale signed cookie, the first write
// rotates the session id and the rotation cookie is queued onto the
// response via res.SetCookie — once the response headers are on the
// wire that cookie can no longer reach the client and the next
// request from this user will arrive with the now-orphaned old id.
// The framework cannot prevent this without breaking valid streaming
// patterns, so the contract is documented and enforced by convention:
// finish your session mutations early in the handler.
func (s *Session) Set(key string, value any) {
	s.rotateIfNeeded()
	if s.data == nil {
		s.data = make(map[string]any)
	}
	s.data[key] = value
	s.dirty = true
}

// Delete removes a key from the session. The same "mutate before
// streaming" contract documented on Set applies here.
func (s *Session) Delete(key string) {
	if s.data == nil {
		return
	}
	s.rotateIfNeeded()
	delete(s.data, key)
	s.dirty = true
}

// rotateIfNeeded swaps out a stale signed session id on the first
// write. When that happens AFTER the response has already started
// flushing the rotation cookie is silently dropped — see the Set
// godoc for the contract that guards against it. The rotation closure
// is supplied by loadOrCreateSession at request entry; tests
// substitute their own to assert when rotation actually fires.
func (s *Session) rotateIfNeeded() {
	if s == nil || !s.rotateOnWrite || s.rotate == nil {
		return
	}
	s.rotate(s)
	s.rotateOnWrite = false
}

// Destroy clears the session payload, asks the middleware to remove the store
// row after the handler returns, and expires the session cookie immediately
// when headers are still writable. If the response is already in flight, the
// next request rotates a stale signed cookie to a fresh session id rather than
// reusing the destroyed id.
func (s *Session) Destroy() {
	s.data = nil
	s.dirty = false
	s.destroyed = true
	if s.expire != nil {
		s.expire()
		s.expire = nil
	}
}

// Save persists the current session state to the configured Store
// immediately, without waiting for the response to finish. Useful
// when a handler:
//
//   - wants the session row to land before launching a background
//     goroutine that depends on the persisted state
//   - performs a Response.Async upgrade and writes session
//     mutations from inside the goroutine but wants an
//     intermediate checkpoint
//   - emits a streaming response (SSE) and needs the auth payload
//     committed before sending events
//
// The middleware's deferred OnFinish callback still runs at the
// end of the request, so a Save followed by additional mutations
// followed by handler return all end up persisted — Save just
// adds a synchronous checkpoint.
//
// Safe to call multiple times. Idempotent when nothing changed
// (dirty flag short-circuits the store write).
func (s *Session) Save() {
	if s == nil || s.persist == nil {
		return
	}
	s.persist(s)
}

// NewSession returns a Middleware that loads the session for the
// request, exposes a *Session at req.Local(LocalKey), and saves the
// session back to the store after the handler returns.
//
//	app.Use(middleware.NewSession(middleware.SessionOptions{
//	    Secret: []byte(os.Getenv("SESSION_SECRET")),
//	    CookieSecure: true,
//	}))
//
// Session ids are signed with HMAC-SHA256: "<random>.<sig>". The
// random portion identifies the session in the store; the sig binds
// the cookie to the server secret so attackers cannot fabricate a
// valid id.
//
// The factory keeps the "New" prefix to disambiguate from the
// per-request *Session struct that handlers manipulate; other
// middleware in this package follow the bare-noun convention
// (Helmet, CORS, …) but for sessions the struct name carries the
// weight since handlers reference it constantly.
func NewSession(opt SessionOptions) mwhint.Hinted {
	if err := validateHMACSecret("Session", opt.Secret); err != nil {
		panic("gogo/middleware: " + err.Error())
	}
	opt.Secret = append([]byte(nil), opt.Secret...)
	if opt.Store == nil {
		mem := NewMemorySessionStore()
		switch {
		case opt.MaxEntries == 0:
			mem.maxEntries = 100_000
		case opt.MaxEntries > 0:
			mem.maxEntries = opt.MaxEntries
		default:
			mem.maxEntries = 0 // NoSessionEntryLimit / legacy negative disables.
		}
		opt.Store = mem
	}
	if opt.CookieName == "" {
		opt.CookieName = "session"
	}
	if opt.CookiePath == "" {
		opt.CookiePath = "/"
	}
	if opt.CookieSameSite == "" {
		opt.CookieSameSite = gogo.SameSiteLax
	}
	if opt.TTL == 0 {
		opt.TTL = 24 * time.Hour
	}
	if opt.LocalKey == "" {
		opt.LocalKey = SessionLocalKey
	}
	validateSessionOptions(opt)
	maxAge := int(opt.TTL.Seconds())

	// Default placement registers Session in both chains — the
	// in-memory store doesn't block, so the middleware is safe
	// either on the loop thread (sync routes) or in the worker
	// (async routes). Deployments with a DB-backed Store that does
	// block should set AsyncStore so the middleware only runs in
	// the worker goroutine.
	placement := mwhint.Both
	if opt.AsyncStore {
		placement = mwhint.Async
	}
	return mwhint.Hinted{Place: placement, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			if opt.SkipFunc != nil && opt.SkipFunc(req) {
				next(res, req)
				return
			}
			sess := loadOrCreateSession(req, res, opt, maxAge)
			req.SetLocal(opt.LocalKey, sess)
			// Register from a defer so panic recovery still persists
			// mutations such as Destroy before the response releases.
			defer res.OnFinish(func() {
				persistSession(sess, opt)
			})
			next(res, req)
		}
	})}
}

func loadOrCreateSession(req *gogo.Request, res *gogo.Response, opt SessionOptions, maxAge int) *Session {
	// One closure per request, captured by both Session.Save and the
	// middleware's OnFinish callback so explicit and deferred saves
	// share a single write path.
	persist := func(s *Session) {
		persistSession(s, opt)
	}
	expire := func() {
		expireSessionCookie(res, opt)
	}
	rotate := func(s *Session) {
		oldID := s.ID
		s.ID = newSessionID()
		setSessionCookie(res, opt, maxAge, s.ID)
		_ = opt.Store.Delete(oldID)
	}
	raw := req.Cookie(opt.CookieName)
	if raw != "" {
		if id, ok := verifySessionID(opt.Secret, raw); ok {
			if data, exists := opt.Store.Load(id); exists {
				return &Session{ID: id, data: data, persist: persist, expire: expire}
			}
			// Signed but absent/expired store rows stay stable for read-only
			// requests, but rotate before the next write so logout and store
			// expiry cannot resurrect a fixated identifier.
			return &Session{ID: id, persist: persist, expire: expire, rotate: rotate, rotateOnWrite: true}
		}
	}
	return issueSession(res, opt, maxAge, persist, expire)
}

func issueSession(res *gogo.Response, opt SessionOptions, maxAge int, persist func(*Session), expire func()) *Session {
	id := newSessionID()
	setSessionCookie(res, opt, maxAge, id)
	return &Session{ID: id, persist: persist, expire: expire}
}

func setSessionCookie(res *gogo.Response, opt SessionOptions, maxAge int, id string) {
	res.SetCookie(gogo.Cookie{
		Name:     opt.CookieName,
		Value:    signSessionID(opt.Secret, id),
		Path:     opt.CookiePath,
		Domain:   opt.CookieDomain,
		MaxAge:   maxAge,
		Secure:   opt.CookieSecure,
		HttpOnly: true,
		SameSite: opt.CookieSameSite,
	})
}

func expireSessionCookie(res *gogo.Response, opt SessionOptions) {
	res.SetCookie(gogo.Cookie{
		Name:     opt.CookieName,
		Path:     opt.CookiePath,
		Domain:   opt.CookieDomain,
		MaxAge:   -1,
		Expires:  "Thu, 01 Jan 1970 00:00:00 GMT",
		Secure:   opt.CookieSecure,
		HttpOnly: true,
		SameSite: opt.CookieSameSite,
	})
}

func validateSessionOptions(opt SessionOptions) {
	validateMiddlewareCookieName("Session CookieName", opt.CookieName)
	if opt.CookiePath != "" {
		validateMiddlewareCookiePath("Session CookiePath", opt.CookiePath)
	}
	if opt.CookieDomain != "" {
		validateMiddlewareCookieDomain("Session CookieDomain", opt.CookieDomain)
	}
	validateMiddlewareCookieSameSite("Session CookieSameSite", opt.CookieSameSite)
	if opt.CookieSameSite == gogo.SameSiteNone && !opt.CookieSecure {
		panic("gogo/middleware: Session CookieSameSite=None requires CookieSecure=true")
	}
	if opt.TTL <= 0 {
		panic("gogo/middleware: Session TTL must be positive")
	}
}

func persistSession(s *Session, opt SessionOptions) {
	if s == nil {
		return
	}
	if s.destroyed {
		_ = opt.Store.Delete(s.ID)
		return
	}
	if s.dirty {
		if err := opt.Store.Save(s.ID, s.data, opt.TTL); err == nil {
			s.dirty = false
		}
	}
}

func newSessionID() string {
	var buf [24]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand.Read is documented as infallible on modern
		// OSes; if it ever fails we fall back to time-based but
		// that's not collision-safe — panic instead so the
		// operator notices.
		panic("gogo/middleware: crypto/rand unavailable: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(buf[:])
}

func signSessionID(secret []byte, id string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(id))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return id + "." + sig
}

func verifySessionID(secret []byte, signed string) (string, bool) {
	dot := strings.IndexByte(signed, '.')
	if dot <= 0 || dot == len(signed)-1 {
		return "", false
	}
	id := signed[:dot]
	sig, err := base64.RawURLEncoding.DecodeString(signed[dot+1:])
	if err != nil {
		return "", false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(id))
	if subtle.ConstantTimeCompare(sig, mac.Sum(nil)) != 1 {
		return "", false
	}
	return id, true
}

// MemorySessionStore is the default Store: a map[string]*sessionEntry
// guarded by a sync.Mutex. Expired entries are reclaimed lazily —
// on Load when an expired id is touched, and on Save when the cap
// (maxEntries) is reached. The manual GC method is still available
// for callers that want to force a full sweep.
//
// Memory is bounded by maxEntries (set via SessionOptions.MaxEntries,
// default 100_000): on a fresh Save at the cap the store first
// sweeps expired entries, then evicts the oldest-expires entry
// remaining to make room. Worst-case memory therefore stays
// predictable even under high-cardinality session-id spam.
//
// # Scale guidance
//
// Every Load and Save acquires the single mutex. The critical section
// is small (map probe + a few atomic loads) but for session-heavy
// workloads at high RPS the lock can show up as the dominant CPU
// cost in profiles — typically beyond ~50 k authenticated RPS per
// process.
//
// Beyond that, prefer:
//
//   - A Redis or other shared SessionStore for horizontal scale;
//     the network round-trip is more expensive per call but
//     parallelizes across cores and stops being a single-process
//     bottleneck.
//   - A sharded in-process implementation: N independent stores
//     keyed by hash(id) % N. Each shard has its own mutex.
//     Reasonable when sessions must stay process-local.
//
// The pattern is identical to MemoryRateLimitStore — see its
// "Scale guidance" comment for the same trade-offs.
type MemorySessionStore struct {
	mu         sync.Mutex
	entries    map[string]*sessionEntry
	maxEntries int // 0 = unbounded; set by NewSession constructor
}

type sessionEntry struct {
	data    map[string]any
	expires time.Time
}

// NewMemorySessionStore returns an empty MemorySessionStore ready for
// use as SessionOptions.Store. The store is unbounded by default
// when used directly; routing it through NewSession applies the
// MaxEntries cap (default 100_000) automatically.
func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{entries: make(map[string]*sessionEntry)}
}

// Load implements SessionStore. Expired entries are deleted from
// the map before reporting them absent — the doc claim that
// "expired entries are reclaimed lazily on Load" now holds. Without
// this delete, an attacker who never revisits a session-id leaves
// the entry pinned in the map forever, defeating the TTL.
func (s *MemorySessionStore) Load(id string) (map[string]any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[id]
	if !ok {
		return nil, false
	}
	if time.Now().After(e.expires) {
		delete(s.entries, id)
		return nil, false
	}
	cp := make(map[string]any, len(e.data))
	for k, v := range e.data {
		cp[k] = v
	}
	return cp, true
}

// Save implements SessionStore. New entries trigger eviction when
// the store is at maxEntries capacity (existing entries don't, so
// the steady-state hot path stays O(1)).
func (s *MemorySessionStore) Save(id string, data map[string]any, ttl time.Duration) error {
	cp := make(map[string]any, len(data))
	for k, v := range data {
		cp[k] = v
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[id]; !exists && s.maxEntries > 0 && len(s.entries) >= s.maxEntries {
		s.evictLocked(now)
	}
	s.entries[id] = &sessionEntry{data: cp, expires: now.Add(ttl)}
	return nil
}

// evictLocked frees one slot using approximate-LRU random sampling.
// Caller must hold s.mu.
//
// A full O(N) scan under the store lock was a CPU/latency spike
// waiting to happen — every new-session Save while the cap was
// binding would stall every other goroutine touching the store for
// ~milliseconds at the 100k default. The fix uses Redis's allkeys-
// lru trick: sample a small random subset and evict the oldest
// from the sample. Go's map iteration is randomized, so the first
// K visits constitute a uniform sample without any explicit
// shuffle.
//
// During the sample pass we also opportunistically clear expired
// entries we happen to land on — free reclamation on the same
// scan. Stop the moment we've freed a slot; otherwise fall through
// to evicting the oldest non-expired entry from the sample.
func (s *MemorySessionStore) evictLocked(now time.Time) {
	const sampleSize = 32
	var oldestKey string
	var oldestExpires time.Time
	sampled := 0
	for k, e := range s.entries {
		if now.After(e.expires) {
			delete(s.entries, k)
			if s.maxEntries <= 0 || len(s.entries) < s.maxEntries {
				return
			}
			continue
		}
		if oldestKey == "" || e.expires.Before(oldestExpires) {
			oldestKey = k
			oldestExpires = e.expires
		}
		sampled++
		if sampled >= sampleSize {
			break
		}
	}
	if oldestKey != "" && (s.maxEntries <= 0 || len(s.entries) >= s.maxEntries) {
		delete(s.entries, oldestKey)
	}
}

func (s *MemorySessionStore) Delete(id string) error {
	s.mu.Lock()
	delete(s.entries, id)
	s.mu.Unlock()
	return nil
}

// GC removes expired entries. Returns the number reclaimed.
func (s *MemorySessionStore) GC() int {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for k, e := range s.entries {
		if now.After(e.expires) {
			delete(s.entries, k)
			removed++
		}
	}
	return removed
}
