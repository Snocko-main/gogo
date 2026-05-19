package middleware

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"strings"

	"uwebsockets-go/gogo"
	"uwebsockets-go/gogo/internal/mwhint"
)

// CSRFLocalKey is the req.Local key carrying the CSRF token for the
// current request. Handlers reading it can embed the value into HTML
// forms or pass it back to the client (e.g. via a JSON envelope) so
// subsequent unsafe requests can echo it.
const CSRFLocalKey = "gogo.csrf.token"

// CSRFOptions configures the double-submit-cookie CSRF middleware.
type CSRFOptions struct {
	// Secret is the HMAC key used to sign tokens. Required.
	// Rotating the secret invalidates outstanding tokens, which is
	// the intended behavior for logout-everywhere flows.
	Secret []byte

	// CookieName is the Set-Cookie name carrying the token to the
	// browser. Default "csrf_token". The cookie is *not* HttpOnly
	// — JavaScript must be able to read it to echo into an
	// X-CSRF-Token header.
	CookieName string

	// HeaderName is the request header checked on unsafe methods.
	// Default "X-CSRF-Token".
	HeaderName string

	// CookiePath sets the Path attribute on the issued cookie.
	// Default "/".
	CookiePath string

	// CookieDomain sets the Domain attribute on the issued cookie.
	// Default empty (host-only cookie).
	CookieDomain string

	// CookieSecure sets the Secure attribute. Default false —
	// enable in production so the cookie never leaves HTTPS.
	CookieSecure bool

	// CookieSameSite sets the SameSite attribute. Default Lax,
	// which is the modern recommendation: prevents cross-site
	// POSTs from carrying the cookie while still allowing
	// top-level GET navigation.
	CookieSameSite gogo.SameSite

	// CookieMaxAge sets Max-Age in seconds. Default 86400 (1 day).
	CookieMaxAge int

	// SkipFunc, when non-nil and returning true, bypasses CSRF
	// entirely. Useful to allow specific routes (e.g. signed
	// webhook endpoints) to skip the check.
	SkipFunc func(*gogo.Request) bool

	// LocalKey overrides the req.Local key for the issued token.
	// Default CSRFLocalKey.
	LocalKey string
}

// CSRF returns a Middleware that enforces the double-submit-cookie
// pattern: the server issues a signed random token in a non-HttpOnly
// cookie and on unsafe requests (POST/PUT/PATCH/DELETE) demands the
// same value in the X-CSRF-Token header. JavaScript on the trusted
// origin can read the cookie and echo it; cross-origin attackers
// cannot read the cookie thanks to the same-origin policy.
//
//	app.Use(middleware.CSRF(middleware.CSRFOptions{
//	    Secret: []byte(os.Getenv("CSRF_SECRET")),
//	    CookieSecure: true,
//	}))
//
// Token format: <random>.<hmac(secret, random)>. The HMAC binds the
// token to the server secret, so an attacker who can set a cookie on
// a sibling subdomain still cannot forge a token that verifies.
//
// Limitations: this middleware only checks the header, not form
// fields. SPA / JSON clients can echo the cookie via JavaScript;
// classic HTML form posts need to include the token in the X-CSRF-
// Token header via fetch or set it via a custom hidden-field flow
// the handler manages.
func CSRF(opt CSRFOptions) mwhint.Hinted {
	if len(opt.Secret) == 0 {
		panic("gogo/middleware: CSRF requires a Secret")
	}
	if opt.CookieName == "" {
		opt.CookieName = "csrf_token"
	}
	if opt.HeaderName == "" {
		opt.HeaderName = "X-CSRF-Token"
	}
	if opt.CookiePath == "" {
		opt.CookiePath = "/"
	}
	if opt.CookieSameSite == "" {
		opt.CookieSameSite = gogo.SameSiteLax
	}
	if opt.CookieMaxAge == 0 {
		opt.CookieMaxAge = 86400
	}
	if opt.LocalKey == "" {
		opt.LocalKey = CSRFLocalKey
	}
	headerLookup := lowercaseAscii(opt.HeaderName)

	return mwhint.Hinted{Place: mwhint.Sync, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			if opt.SkipFunc != nil && opt.SkipFunc(req) {
				next(res, req)
				return
			}

			cookieVal := req.Cookie(opt.CookieName)
			validIncoming := cookieVal != "" && verifyCSRFToken(opt.Secret, cookieVal) == nil

			if csrfIsUnsafe(req.Method()) {
				headerVal := req.Header(headerLookup)
				if !validIncoming || headerVal == "" ||
					subtle.ConstantTimeCompare([]byte(headerVal), []byte(cookieVal)) != 1 {
					res.Send(403, "text/plain; charset=utf-8", "Forbidden: invalid CSRF token\n")
					return
				}
				req.SetLocal(opt.LocalKey, cookieVal)
				next(res, req)
				return
			}

			token := cookieVal
			if !validIncoming {
				t, err := newCSRFToken(opt.Secret)
				if err != nil {
					res.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
					return
				}
				token = t
				res.SetCookie(gogo.Cookie{
					Name:     opt.CookieName,
					Value:    token,
					Path:     opt.CookiePath,
					Domain:   opt.CookieDomain,
					MaxAge:   opt.CookieMaxAge,
					Secure:   opt.CookieSecure,
					HttpOnly: false,
					SameSite: opt.CookieSameSite,
				})
			}
			req.SetLocal(opt.LocalKey, token)
			next(res, req)
		}
	})}
}

func csrfIsUnsafe(method string) bool {
	switch method {
	case "post", "put", "patch", "delete":
		return true
	}
	return false
}

// newCSRFToken produces <random>.<hmac> in URL-safe base64. 16 bytes
// of random is enough for collision resistance; the HMAC binds the
// token to the server secret.
func newCSRFToken(secret []byte) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	randPart := base64.RawURLEncoding.EncodeToString(raw[:])
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(randPart))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return randPart + "." + sig, nil
}

func verifyCSRFToken(secret []byte, tok string) error {
	dot := strings.IndexByte(tok, '.')
	if dot <= 0 || dot == len(tok)-1 {
		return errors.New("malformed token")
	}
	randPart := tok[:dot]
	sigPart := tok[dot+1:]
	sig, err := base64.RawURLEncoding.DecodeString(sigPart)
	if err != nil {
		return errors.New("invalid signature encoding")
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(randPart))
	if subtle.ConstantTimeCompare(sig, mac.Sum(nil)) != 1 {
		return errors.New("signature mismatch")
	}
	return nil
}
