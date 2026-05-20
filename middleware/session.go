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
	// forged by clients (and is bound to this server fleet).
	// Required.
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
}

// Session is the per-request session handle handlers manipulate
// during a request. Mutations are persisted to the Store after the
// handler returns; concurrent goroutines writing to the same handle
// race — sessions are intended to be used from the request goroutine
// only.
type Session struct {
	ID        string
	data      map[string]any
	dirty     bool
	destroyed bool
}

// Get returns the value at key, or nil when absent.
func (s *Session) Get(key string) any {
	if s.data == nil {
		return nil
	}
	return s.data[key]
}

// Set writes value at key and marks the session for save.
func (s *Session) Set(key string, value any) {
	if s.data == nil {
		s.data = make(map[string]any)
	}
	s.data[key] = value
	s.dirty = true
}

// Delete removes a key from the session.
func (s *Session) Delete(key string) {
	if s.data == nil {
		return
	}
	delete(s.data, key)
	s.dirty = true
}

// Destroy clears the session payload and instructs the middleware
// to remove the row from the store after the handler returns. The
// session cookie remains until it expires naturally — clients can
// be issued a new one on next visit. (We can't safely write a new
// Set-Cookie header after Destroy because the response may already
// be in flight by the time the middleware tail runs.)
func (s *Session) Destroy() {
	s.data = nil
	s.destroyed = true
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
	if len(opt.Secret) == 0 {
		panic("gogo/middleware: Session requires a Secret")
	}
	if opt.Store == nil {
		opt.Store = NewMemorySessionStore()
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
	maxAge := int(opt.TTL.Seconds())

	// Default placement registers Session in both chains — the
	// in-memory store doesn't block, so the middleware is safe
	// either on the loop thread (sync routes) or in the worker
	// (async routes). Deployments with a DB-backed Store that does
	// block should wrap the bare closure with middleware.Async so
	// the middleware only runs in the worker goroutine.
	return mwhint.Hinted{Place: mwhint.Both, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			if opt.SkipFunc != nil && opt.SkipFunc(req) {
				next(res, req)
				return
			}
			sess := loadOrCreateSession(req, res, opt, maxAge)
			req.SetLocal(opt.LocalKey, sess)
			next(res, req)
			persistSession(sess, opt)
		}
	})}
}

func loadOrCreateSession(req *gogo.Request, res *gogo.Response, opt SessionOptions, maxAge int) *Session {
	raw := req.Cookie(opt.CookieName)
	if raw != "" {
		if id, ok := verifySessionID(opt.Secret, raw); ok {
			if data, exists := opt.Store.Load(id); exists {
				return &Session{ID: id, data: data}
			}
			return &Session{ID: id}
		}
	}
	id := newSessionID()
	signed := signSessionID(opt.Secret, id)
	res.SetCookie(gogo.Cookie{
		Name:     opt.CookieName,
		Value:    signed,
		Path:     opt.CookiePath,
		Domain:   opt.CookieDomain,
		MaxAge:   maxAge,
		Secure:   opt.CookieSecure,
		HttpOnly: true,
		SameSite: opt.CookieSameSite,
	})
	return &Session{ID: id}
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
		_ = opt.Store.Save(s.ID, s.data, opt.TTL)
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
// guarded by a sync.RWMutex. Expired entries are reclaimed lazily on
// Load; call GC manually if your session-id distribution is bursty
// and you want bounded memory.
type MemorySessionStore struct {
	mu      sync.RWMutex
	entries map[string]*sessionEntry
}

type sessionEntry struct {
	data    map[string]any
	expires time.Time
}

// NewMemorySessionStore returns an empty MemorySessionStore ready for
// use as SessionOptions.Store.
func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{entries: make(map[string]*sessionEntry)}
}

func (s *MemorySessionStore) Load(id string) (map[string]any, bool) {
	s.mu.RLock()
	e, ok := s.entries[id]
	s.mu.RUnlock()
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	cp := make(map[string]any, len(e.data))
	for k, v := range e.data {
		cp[k] = v
	}
	return cp, true
}

func (s *MemorySessionStore) Save(id string, data map[string]any, ttl time.Duration) error {
	cp := make(map[string]any, len(data))
	for k, v := range data {
		cp[k] = v
	}
	s.mu.Lock()
	s.entries[id] = &sessionEntry{data: cp, expires: time.Now().Add(ttl)}
	s.mu.Unlock()
	return nil
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
