package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"hash"
	"strings"
	"time"

	"uwebsockets-go/gogo"
)

// JWTLocalKey is the req.Local key carrying the verified token's
// claim set as a map[string]any.
const JWTLocalKey = "gogo.jwt.claims"

// JWTAlgorithm selects the HMAC algorithm used to verify tokens.
// Only HMAC families are supported by this middleware — asymmetric
// algorithms (RS256, ES256) require key parsing infrastructure that
// is left to a future extension.
type JWTAlgorithm string

const (
	JWTHS256 JWTAlgorithm = "HS256"
	JWTHS384 JWTAlgorithm = "HS384"
	JWTHS512 JWTAlgorithm = "HS512"
)

// JWTOptions configures the JWT middleware.
type JWTOptions struct {
	// Secret is the HMAC key shared with the issuer. Required.
	// For HS256 use at least 32 bytes of entropy; HS384/HS512
	// proportionally more.
	Secret []byte

	// Algorithm selects the expected signing algorithm. Default
	// HS256. The middleware rejects tokens whose `alg` header
	// disagrees with this value — preventing the well-known
	// "none" and algorithm-confusion attacks.
	Algorithm JWTAlgorithm

	// TokenFunc extracts the JWT string from the request. Default
	// reads "Authorization: Bearer <token>". Override to source
	// the token from a cookie, query parameter, etc.
	TokenFunc func(*gogo.Request) string

	// LocalKey overrides the req.Local key used to stash claims.
	// Default JWTLocalKey.
	LocalKey string

	// SkipFunc, when non-nil and returning true, bypasses JWT
	// verification for that request.
	SkipFunc func(*gogo.Request) bool

	// Optional, when true, lets requests without a token through
	// (handler can inspect req.Local(LocalKey) to detect the
	// anonymous case). Tokens that are present but invalid are
	// still rejected with 401 — Optional doesn't make malformed
	// tokens acceptable.
	Optional bool

	// Leeway is the clock-skew tolerance when validating `exp` and
	// `nbf` claims. Default 0 (strict).
	Leeway time.Duration
}

// JWT returns a Middleware that authenticates requests carrying a
// JSON Web Token signed with HMAC. On success the verified claims
// (as a map[string]any from json.Unmarshal) are stashed at
// req.Local(LocalKey) for the handler chain.
//
//	app.Use(middleware.JWT(middleware.JWTOptions{
//	    Secret: []byte(os.Getenv("JWT_SECRET")),
//	}))
//
//	app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
//	    claims, _ := req.Local(middleware.JWTLocalKey).(map[string]any)
//	    res.JSON(200, claims)
//	})
//
// Limitations: only HMAC algorithms (HS256/HS384/HS512). Asymmetric
// algorithms are intentionally unsupported here to keep the
// surface small and avoid common verification pitfalls. For
// RS256/ES256, wire up a custom middleware that imports
// crypto/rsa or crypto/ecdsa directly.
func JWT(opt JWTOptions) gogo.Middleware {
	if len(opt.Secret) == 0 {
		panic("gogo/middleware: JWT requires a Secret")
	}
	if opt.Algorithm == "" {
		opt.Algorithm = JWTHS256
	}
	if opt.TokenFunc == nil {
		opt.TokenFunc = defaultJWTTokenFunc
	}
	if opt.LocalKey == "" {
		opt.LocalKey = JWTLocalKey
	}
	hashFn, expectedAlg, ok := jwtHashFor(opt.Algorithm)
	if !ok {
		panic("gogo/middleware: JWT unsupported algorithm " + string(opt.Algorithm))
	}

	return func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			if opt.SkipFunc != nil && opt.SkipFunc(req) {
				next(res, req)
				return
			}
			tok := opt.TokenFunc(req)
			if tok == "" {
				if opt.Optional {
					next(res, req)
					return
				}
				jwtReject(res, "missing token")
				return
			}
			claims, err := verifyJWT(tok, opt.Secret, hashFn, expectedAlg, opt.Leeway)
			if err != nil {
				jwtReject(res, err.Error())
				return
			}
			req.SetLocal(opt.LocalKey, claims)
			next(res, req)
		}
	}
}

func jwtReject(res *gogo.Response, reason string) {
	res.Header("WWW-Authenticate", `Bearer error="invalid_token", error_description="`+reason+`"`)
	res.Send(401, "text/plain; charset=utf-8", "Unauthorized\n")
}

func defaultJWTTokenFunc(req *gogo.Request) string {
	const prefix = "Bearer "
	v := req.Header("authorization")
	if len(v) < len(prefix) || !strings.EqualFold(v[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(v[len(prefix):])
}

func jwtHashFor(alg JWTAlgorithm) (func() hash.Hash, string, bool) {
	switch alg {
	case JWTHS256:
		return sha256.New, "HS256", true
	case JWTHS384:
		return sha512.New384, "HS384", true
	case JWTHS512:
		return sha512.New, "HS512", true
	}
	return nil, "", false
}

// verifyJWT parses a token of the form header.payload.signature,
// confirms the header `alg` matches expectedAlg, validates the HMAC
// signature, and decodes the payload into a claims map. exp / nbf
// are checked against the wall clock with the configured leeway.
func verifyJWT(tok string, secret []byte, h func() hash.Hash, expectedAlg string, leeway time.Duration) (map[string]any, error) {
	first := strings.IndexByte(tok, '.')
	if first < 0 {
		return nil, errors.New("malformed token")
	}
	second := strings.IndexByte(tok[first+1:], '.')
	if second < 0 {
		return nil, errors.New("malformed token")
	}
	second += first + 1
	headerB64 := tok[:first]
	payloadB64 := tok[first+1 : second]
	sigB64 := tok[second+1:]
	if headerB64 == "" || payloadB64 == "" || sigB64 == "" {
		return nil, errors.New("malformed token")
	}

	headerJSON, err := base64.RawURLEncoding.DecodeString(headerB64)
	if err != nil {
		return nil, errors.New("invalid header encoding")
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return nil, errors.New("invalid header")
	}
	if header.Alg != expectedAlg {
		// Reject "none", algorithm-confusion attacks, or any
		// mismatch with the configured expectation.
		return nil, errors.New("unexpected algorithm")
	}

	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return nil, errors.New("invalid signature encoding")
	}
	mac := hmac.New(h, secret)
	mac.Write([]byte(tok[:second]))
	expected := mac.Sum(nil)
	if subtle.ConstantTimeCompare(sig, expected) != 1 {
		return nil, errors.New("signature mismatch")
	}

	payloadJSON, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return nil, errors.New("invalid payload encoding")
	}
	var claims map[string]any
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return nil, errors.New("invalid payload")
	}

	now := time.Now()
	if expRaw, ok := claims["exp"]; ok {
		if expF, ok := expRaw.(float64); ok {
			expT := time.Unix(int64(expF), 0)
			if now.After(expT.Add(leeway)) {
				return nil, errors.New("token expired")
			}
		}
	}
	if nbfRaw, ok := claims["nbf"]; ok {
		if nbfF, ok := nbfRaw.(float64); ok {
			nbfT := time.Unix(int64(nbfF), 0)
			if now.Add(leeway).Before(nbfT) {
				return nil, errors.New("token not yet valid")
			}
		}
	}
	return claims, nil
}

// SignJWT produces a compact JWT signed with the given HMAC algorithm
// and secret. Intended for tests and small login flows — production
// auth servers usually mint tokens in dedicated identity-provider
// code with richer key management.
func SignJWT(alg JWTAlgorithm, secret []byte, claims map[string]any) (string, error) {
	hFn, algName, ok := jwtHashFor(alg)
	if !ok {
		return "", errors.New("unsupported algorithm")
	}
	headerJSON, _ := json.Marshal(map[string]string{"alg": algName, "typ": "JWT"})
	payloadJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	header := base64.RawURLEncoding.EncodeToString(headerJSON)
	payload := base64.RawURLEncoding.EncodeToString(payloadJSON)
	signingInput := header + "." + payload
	mac := hmac.New(hFn, secret)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig, nil
}
