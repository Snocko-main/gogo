package middleware

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"strings"

	"github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/internal/mwhint"
)

// BasicAuthLocalKey is the req.Local key carrying the authenticated
// username. Handlers retrieve it via:
//
//	user, _ := req.Local(middleware.BasicAuthLocalKey).(string)
const BasicAuthLocalKey = "gogo.basicauth.user"

// BasicAuthOptions configures BasicAuth. Provide either Users or
// Validator (Validator wins if both are set).
type BasicAuthOptions struct {
	// Users is a static {username: password} map. Lookups use
	// constant-time comparison so timing leaks of valid usernames
	// are kept negligible. Intended for small fleets and CI;
	// production credentials should live in Validator backed by a
	// hashed-password store.
	Users map[string]string

	// Validator authenticates a (user, pass) pair. Return true to
	// allow the request, false to reject with 401. Validator is
	// called once per request — keep it fast (bcrypt only on the
	// register / login path, not here).
	Validator func(user, pass string) bool

	// Realm appears in the WWW-Authenticate challenge header on a
	// 401 response. Default "Restricted". Browsers use the realm
	// string to namespace cached credentials.
	Realm string

	// LocalKey overrides the req.Local key used to stash the
	// authenticated username. Default BasicAuthLocalKey.
	LocalKey string

	// SkipFunc, when non-nil and returning true, bypasses
	// authentication for that request. Useful to expose /healthz
	// without credentials while protecting the rest of the app.
	SkipFunc func(*gogo.Request) bool

	// MaxCredentialBytes caps the base64 credentials payload before
	// decoding. Default 8 KiB. Set negative to disable.
	MaxCredentialBytes int
}

const defaultBasicAuthMaxCredentialBytes = 8 << 10

// BasicAuth returns a Middleware that enforces HTTP Basic
// authentication (RFC 7617). Requests without a valid Authorization
// header receive 401 Unauthorized with a WWW-Authenticate challenge;
// authenticated requests have the username stashed at LocalKey for
// downstream handlers.
//
//	app.Use(middleware.BasicAuth(middleware.BasicAuthOptions{
//	    Users: map[string]string{"admin": "secret"},
//	}))
//
// Always pair Basic auth with TLS — the credentials travel
// base64-encoded but otherwise in plaintext. The middleware does not
// enforce HTTPS; configure that at the edge (load balancer, reverse
// proxy) or via HSTS via Helmet.
func BasicAuth(opt BasicAuthOptions) mwhint.Hinted {
	if opt.Validator == nil && len(opt.Users) == 0 {
		panic("gogo/middleware: BasicAuth requires Users or Validator")
	}
	if opt.Realm == "" {
		opt.Realm = "Restricted"
	}
	if opt.LocalKey == "" {
		opt.LocalKey = BasicAuthLocalKey
	}
	if opt.MaxCredentialBytes == 0 {
		opt.MaxCredentialBytes = defaultBasicAuthMaxCredentialBytes
	}
	// RFC 7235 quoted-string grammar — realm is bound by double
	// quotes, and any " or \ in the value must be backslash-escaped.
	// CTLs and DEL are illegal in a quoted-string per RFC 7230 §3.2.6
	// and would otherwise trip validateHeaderValue's panic. Reuse the
	// JWT auth-param escaper so both challenges follow the same rule.
	challenge := `Basic realm="` + jwtAuthParam(opt.Realm) + `", charset="UTF-8"`

	verify := opt.Validator
	if verify == nil {
		users := make(map[string][sha256.Size]byte, len(opt.Users))
		for user, pass := range opt.Users {
			users[user] = sha256.Sum256([]byte(pass))
		}
		dummy := sha256.Sum256(nil)
		verify = func(u, p string) bool {
			supplied := sha256.Sum256([]byte(p))
			expected, ok := users[u]
			if !ok {
				// Run the compare anyway to keep the timing
				// roughly constant across known vs. unknown
				// usernames.
				subtle.ConstantTimeCompare(supplied[:], dummy[:])
				return false
			}
			return subtle.ConstantTimeCompare(supplied[:], expected[:]) == 1
		}
	}

	return mwhint.Hinted{Place: mwhint.Sync, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			if opt.SkipFunc != nil && opt.SkipFunc(req) {
				next(res, req)
				return
			}
			user, pass, ok := parseBasicAuth(req.Header("authorization"), opt.MaxCredentialBytes)
			if !ok || !verify(user, pass) {
				res.Header("WWW-Authenticate", challenge)
				res.Send(401, "text/plain; charset=utf-8", "Unauthorized\n")
				return
			}
			req.SetLocal(opt.LocalKey, user)
			next(res, req)
		}
	})}
}

// parseBasicAuth pulls (user, pass) out of an Authorization header
// value. Returns ok=false if the scheme is not Basic, the base64
// payload is invalid, or the decoded payload has no ":" separator.
func parseBasicAuth(auth string, maxCredentialBytes int) (user, pass string, ok bool) {
	const prefix = "Basic "
	if len(auth) < len(prefix) {
		return "", "", false
	}
	// Case-insensitive scheme check.
	if !strings.EqualFold(auth[:len(prefix)], prefix) {
		return "", "", false
	}
	encoded := strings.TrimSpace(auth[len(prefix):])
	if maxCredentialBytes >= 0 && len(encoded) > maxCredentialBytes {
		return "", "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", "", false
	}
	colon := bytes.IndexByte(decoded, ':')
	if colon < 0 {
		return "", "", false
	}
	return string(decoded[:colon]), string(decoded[colon+1:]), true
}
