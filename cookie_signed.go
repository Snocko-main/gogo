// Signed-cookie helpers — HMAC-SHA256 authenticated cookie values.
//
// A signed cookie carries a payload plus a base64url-encoded HMAC of
// that payload computed with a server-only secret. The browser
// receives the combined `value.signature` string and round-trips it
// unchanged; on a later request the server recomputes the HMAC and
// rejects any tampering before handing the value to the handler.
//
// The framework exposes two layers:
//
//   - Standalone primitives (SignCookieValue, VerifyCookieValue) for
//     callers that already manage cookie I/O themselves or want to
//     sign / verify outside the Request / Response lifecycle.
//
//   - Convenience methods on Response / Request
//     (SetCookieSigned / CookieSigned) that pair the crypto with the
//     existing SetCookie / Cookie call sites.
//
// Both layers accept multiple secrets to support seamless key
// rotation: signing always uses the first secret, verification tries
// every secret in order until one matches. Rotate by prepending the
// new secret and keeping the old one until every issued cookie has
// expired, then drop the old key.

package gogo

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"strings"
)

// signedCookieSep is the delimiter between the cookie value and its
// HMAC. '.' is outside both the base64url alphabet (which uses
// A-Za-z0-9-_) and the typical RFC 6265 cookie-octet set, so the
// split is unambiguous even when the underlying value contains
// arbitrary opaque text.
const signedCookieSep = '.'

// SignCookieValue returns value + "." + base64url(HMAC-SHA256(value,
// secret)) using the FIRST entry of secrets as the signing key.
// Additional secrets are not used by this function; supply them to
// VerifyCookieValue instead so a rotation can accept the old key
// while new cookies are issued with the new one.
//
// Panics on an empty secrets slice — signing without a key is never
// what you want, and silently producing an unauthenticated cookie
// would be a security footgun.
//
//	signed := gogo.SignCookieValue("alice:42", secret)
//	res.SetCookie(gogo.Cookie{Name: "session", Value: signed, HttpOnly: true})
func SignCookieValue(value string, secrets ...string) string {
	if len(secrets) == 0 || secrets[0] == "" {
		panic("gogo: SignCookieValue requires at least one secret")
	}
	mac := hmac.New(sha256.New, []byte(secrets[0]))
	mac.Write([]byte(value))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	var b strings.Builder
	b.Grow(len(value) + 1 + len(sig))
	b.WriteString(value)
	b.WriteByte(signedCookieSep)
	b.WriteString(sig)
	return b.String()
}

// VerifyCookieValue splits a signed cookie value at the final '.',
// recomputes the HMAC of the prefix under each supplied secret, and
// returns the prefix when any secret produces a matching signature.
// Returns ("", false) when:
//
//   - signed is empty or contains no '.',
//   - the signature segment is not valid base64url,
//   - no secret produces a matching signature,
//   - secrets is empty.
//
// Comparison is constant-time so a forged cookie cannot leak the
// expected signature byte-by-byte through timing.
//
//	val, ok := gogo.VerifyCookieValue(req.Cookie("session"), secret, oldSecret)
//	if !ok {
//	    res.Send(401, "text/plain", "bad session")
//	    return
//	}
func VerifyCookieValue(signed string, secrets ...string) (string, bool) {
	if signed == "" || len(secrets) == 0 {
		return "", false
	}
	dot := strings.LastIndexByte(signed, signedCookieSep)
	if dot <= 0 || dot == len(signed)-1 {
		return "", false
	}
	value := signed[:dot]
	gotSig, err := base64.RawURLEncoding.DecodeString(signed[dot+1:])
	if err != nil {
		return "", false
	}
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(value))
		if subtle.ConstantTimeCompare(gotSig, mac.Sum(nil)) == 1 {
			return value, true
		}
	}
	return "", false
}

// SetCookieSigned signs c.Value with the first secret, replaces
// c.Value with the signed form, and writes the cookie via SetCookie.
// All of SetCookie's validation and header semantics apply
// (Path / Domain / Max-Age / Secure / HttpOnly / SameSite).
//
// Pair with Request.CookieSigned on the read side to verify and
// recover the original value:
//
//	res.SetCookieSigned(gogo.Cookie{
//	    Name:     "session",
//	    Value:    "alice:42",
//	    Path:     "/",
//	    HttpOnly: true,
//	    Secure:   true,
//	    SameSite: gogo.SameSiteStrict,
//	    MaxAge:   3600,
//	}, secret)
func (r *Response) SetCookieSigned(c Cookie, secrets ...string) {
	c.Value = SignCookieValue(c.Value, secrets...)
	r.SetCookie(c)
}

// CookieSigned reads the named cookie and verifies its signature
// against secrets. Returns the original (pre-signing) value when any
// secret validates, ("", false) otherwise — the same way Cookie
// returns "" when a cookie is absent.
//
//	user, ok := req.CookieSigned("session", secret)
//	if !ok {
//	    res.Send(401, "text/plain; charset=utf-8", "unauthorized")
//	    return
//	}
func (r *Request) CookieSigned(name string, secrets ...string) (string, bool) {
	raw := r.Cookie(name)
	if raw == "" {
		return "", false
	}
	return VerifyCookieValue(raw, secrets...)
}
