package middleware

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"hash"
	"math/big"
	"strings"
	"time"

	"github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/internal/mwhint"
)

// JWTLocalKey is the req.Local key carrying the verified token's
// claim set as a map[string]any.
const JWTLocalKey = "gogo.jwt.claims"

// JWTAlgorithm selects the signing algorithm the middleware will
// accept. The middleware refuses to verify any token whose `alg`
// header disagrees with this value, blocking alg-confusion and
// `alg: "none"` attacks.
//
// Supported families:
//
//   - HS256/HS384/HS512 — HMAC. Key is a []byte secret.
//   - RS256/RS384/RS512 — RSASSA-PKCS1-v1_5. Key is a *rsa.PublicKey.
//   - PS256/PS384/PS512 — RSASSA-PSS. Key is a *rsa.PublicKey.
//   - ES256/ES384/ES512 — ECDSA on P-256 / P-384 / P-521. Key is a
//     *ecdsa.PublicKey.
type JWTAlgorithm string

const (
	JWTHS256 JWTAlgorithm = "HS256"
	JWTHS384 JWTAlgorithm = "HS384"
	JWTHS512 JWTAlgorithm = "HS512"
	JWTRS256 JWTAlgorithm = "RS256"
	JWTRS384 JWTAlgorithm = "RS384"
	JWTRS512 JWTAlgorithm = "RS512"
	JWTPS256 JWTAlgorithm = "PS256"
	JWTPS384 JWTAlgorithm = "PS384"
	JWTPS512 JWTAlgorithm = "PS512"
	JWTES256 JWTAlgorithm = "ES256"
	JWTES384 JWTAlgorithm = "ES384"
	JWTES512 JWTAlgorithm = "ES512"
)

// JWTOptions configures the JWT middleware.
type JWTOptions struct {
	// Secret is the HMAC verification key for HS256/384/512.
	// Required when Algorithm is an HMAC variant. Use at least
	// 32 bytes of entropy for HS256, proportionally more for the
	// larger variants. Ignored for asymmetric algorithms.
	Secret []byte

	// Key is the public key for asymmetric algorithms (RS*, PS*,
	// ES*). Must match the algorithm family:
	//   - *rsa.PublicKey for RS256/384/512 and PS256/384/512
	//   - *ecdsa.PublicKey for ES256/384/512
	// The middleware panics at construction if Algorithm and Key
	// disagree. Ignored for HMAC algorithms (use Secret).
	Key crypto.PublicKey

	// Algorithm selects the expected signing algorithm. Default
	// HS256. Tokens whose `alg` header differs from this value
	// are rejected — preventing both the legacy "none" attack
	// and algorithm-confusion attacks (HMAC-vs-RSA, where an
	// attacker could sign an RS256 token using the public key as
	// an HMAC secret if the verifier blindly used the alg header).
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
// JSON Web Token signed with HMAC, RSA, RSA-PSS, or ECDSA. On
// success the verified claims (as a map[string]any from
// json.Unmarshal) are stashed at req.Local(LocalKey) for the
// handler chain.
//
//	// HMAC
//	app.Use(middleware.JWT(middleware.JWTOptions{
//	    Secret: []byte(os.Getenv("JWT_SECRET")),
//	}))
//
//	// RSA
//	pub, _ := middleware.ParseRSAPublicKey([]byte(rsaPEM))
//	app.Use(middleware.JWT(middleware.JWTOptions{
//	    Algorithm: middleware.JWTRS256,
//	    Key:       pub,
//	}))
//
//	// ECDSA
//	pub, _ := middleware.ParseECPublicKey([]byte(ecPEM))
//	app.Use(middleware.JWT(middleware.JWTOptions{
//	    Algorithm: middleware.JWTES256,
//	    Key:       pub,
//	}))
//
//	app.Get("/me", func(res *gogo.Response, req *gogo.Request) {
//	    claims, _ := req.Local(middleware.JWTLocalKey).(map[string]any)
//	    res.JSON(200, claims)
//	})
//
// JWKS-style key rotation (looking up the key per token via the
// header `kid`) is not built in. Wire it by setting TokenFunc to a
// custom verifier that resolves the key from your JWKS cache and
// returns the validated claims — or write a thin middleware that
// dispatches between multiple JWT instances keyed on `kid`.
func JWT(opt JWTOptions) mwhint.Hinted {
	if opt.Algorithm == "" {
		opt.Algorithm = JWTHS256
	}
	info, ok := jwtAlgInfoFor(opt.Algorithm)
	if !ok {
		panic("gogo/middleware: JWT unsupported algorithm " + string(opt.Algorithm))
	}
	verifier, err := jwtBuildVerifier(info, opt.Secret, opt.Key)
	if err != nil {
		panic("gogo/middleware: JWT key/algorithm mismatch: " + err.Error())
	}
	if opt.TokenFunc == nil {
		opt.TokenFunc = defaultJWTTokenFunc
	}
	if opt.LocalKey == "" {
		opt.LocalKey = JWTLocalKey
	}
	expectedAlg := string(opt.Algorithm)

	return mwhint.Hinted{Place: mwhint.Sync, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
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
			claims, err := verifyJWT(tok, verifier, expectedAlg, opt.Leeway)
			if err != nil {
				jwtReject(res, err.Error())
				return
			}
			req.SetLocal(opt.LocalKey, claims)
			next(res, req)
		}
	})}
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

// jwtAlgInfo describes an algorithm: which hash to use and which
// signature family it belongs to.
type jwtAlgInfo struct {
	hashID    crypto.Hash
	hashNew   func() hash.Hash
	family    string // "HS", "RS", "PS", "ES"
	ecdsaSize int    // bytes per r/s component (ES family only)
}

func jwtAlgInfoFor(alg JWTAlgorithm) (jwtAlgInfo, bool) {
	switch alg {
	case JWTHS256:
		return jwtAlgInfo{crypto.SHA256, sha256.New, "HS", 0}, true
	case JWTHS384:
		return jwtAlgInfo{crypto.SHA384, sha512.New384, "HS", 0}, true
	case JWTHS512:
		return jwtAlgInfo{crypto.SHA512, sha512.New, "HS", 0}, true
	case JWTRS256:
		return jwtAlgInfo{crypto.SHA256, sha256.New, "RS", 0}, true
	case JWTRS384:
		return jwtAlgInfo{crypto.SHA384, sha512.New384, "RS", 0}, true
	case JWTRS512:
		return jwtAlgInfo{crypto.SHA512, sha512.New, "RS", 0}, true
	case JWTPS256:
		return jwtAlgInfo{crypto.SHA256, sha256.New, "PS", 0}, true
	case JWTPS384:
		return jwtAlgInfo{crypto.SHA384, sha512.New384, "PS", 0}, true
	case JWTPS512:
		return jwtAlgInfo{crypto.SHA512, sha512.New, "PS", 0}, true
	case JWTES256:
		return jwtAlgInfo{crypto.SHA256, sha256.New, "ES", 32}, true
	case JWTES384:
		return jwtAlgInfo{crypto.SHA384, sha512.New384, "ES", 48}, true
	case JWTES512:
		// P-521 produces 521-bit components → 66 bytes each
		// rounded up. Yes, ES512 uses P-521, not P-512 — there
		// is no NIST P-512 curve.
		return jwtAlgInfo{crypto.SHA512, sha512.New, "ES", 66}, true
	}
	return jwtAlgInfo{}, false
}

// jwtVerifier closes over the algorithm info + key so the hot path
// just calls a single function per request.
type jwtVerifier func(signingInput, signature []byte) error

func jwtBuildVerifier(info jwtAlgInfo, secret []byte, key crypto.PublicKey) (jwtVerifier, error) {
	switch info.family {
	case "HS":
		if len(secret) == 0 {
			return nil, errors.New("HMAC algorithm requires Secret")
		}
		s := append([]byte(nil), secret...) // defensive copy
		hashNew := info.hashNew
		return func(input, sig []byte) error {
			mac := hmac.New(hashNew, s)
			mac.Write(input)
			expected := mac.Sum(nil)
			if subtle.ConstantTimeCompare(sig, expected) != 1 {
				return errors.New("signature mismatch")
			}
			return nil
		}, nil
	case "RS":
		pk, ok := key.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("RS algorithm requires *rsa.PublicKey")
		}
		hashID := info.hashID
		hashNew := info.hashNew
		return func(input, sig []byte) error {
			h := hashNew()
			h.Write(input)
			return rsa.VerifyPKCS1v15(pk, hashID, h.Sum(nil), sig)
		}, nil
	case "PS":
		pk, ok := key.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("PS algorithm requires *rsa.PublicKey")
		}
		hashID := info.hashID
		hashNew := info.hashNew
		opts := &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: hashID}
		return func(input, sig []byte) error {
			h := hashNew()
			h.Write(input)
			return rsa.VerifyPSS(pk, hashID, h.Sum(nil), sig, opts)
		}, nil
	case "ES":
		pk, ok := key.(*ecdsa.PublicKey)
		if !ok {
			return nil, errors.New("ES algorithm requires *ecdsa.PublicKey")
		}
		size := info.ecdsaSize
		hashNew := info.hashNew
		return func(input, sig []byte) error {
			if len(sig) != 2*size {
				return errors.New("invalid ECDSA signature length")
			}
			r := new(big.Int).SetBytes(sig[:size])
			s := new(big.Int).SetBytes(sig[size:])
			h := hashNew()
			h.Write(input)
			if !ecdsa.Verify(pk, h.Sum(nil), r, s) {
				return errors.New("signature mismatch")
			}
			return nil
		}, nil
	}
	return nil, errors.New("unknown algorithm family")
}

// verifyJWT parses a token of the form header.payload.signature,
// confirms the header `alg` matches expectedAlg, runs the per-family
// verifier on the signing input, and decodes the payload into a
// claims map. exp / nbf are checked against the wall clock with the
// configured leeway.
func verifyJWT(tok string, verify jwtVerifier, expectedAlg string, leeway time.Duration) (map[string]any, error) {
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
		return nil, errors.New("unexpected algorithm")
	}

	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return nil, errors.New("invalid signature encoding")
	}
	if err := verify([]byte(tok[:second]), sig); err != nil {
		return nil, err
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
		// Reject non-numeric exp claims rather than silently skipping
		// the expiration check — a token carrying `"exp": "tomorrow"`
		// would otherwise pass verification with no expiry enforced.
		expF, ok := expRaw.(float64)
		if !ok {
			return nil, errors.New("malformed exp claim")
		}
		expT := time.Unix(int64(expF), 0)
		if now.After(expT.Add(leeway)) {
			return nil, errors.New("token expired")
		}
	}
	if nbfRaw, ok := claims["nbf"]; ok {
		nbfF, ok := nbfRaw.(float64)
		if !ok {
			return nil, errors.New("malformed nbf claim")
		}
		nbfT := time.Unix(int64(nbfF), 0)
		if now.Add(leeway).Before(nbfT) {
			return nil, errors.New("token not yet valid")
		}
	}
	return claims, nil
}

// SignJWT produces a compact JWT signed with the given algorithm
// and key. Intended for tests and small login flows — production
// auth servers usually mint tokens in dedicated identity-provider
// code with richer key management.
//
// The key argument's type depends on Algorithm:
//
//   - HS256/384/512: []byte
//   - RS256/384/512 and PS256/384/512: *rsa.PrivateKey
//   - ES256/384/512: *ecdsa.PrivateKey
func SignJWT(alg JWTAlgorithm, key any, claims map[string]any) (string, error) {
	info, ok := jwtAlgInfoFor(alg)
	if !ok {
		return "", errors.New("unsupported algorithm")
	}
	headerJSON, _ := json.Marshal(map[string]string{"alg": string(alg), "typ": "JWT"})
	payloadJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	header := base64.RawURLEncoding.EncodeToString(headerJSON)
	payload := base64.RawURLEncoding.EncodeToString(payloadJSON)
	signingInput := header + "." + payload

	var sig []byte
	switch info.family {
	case "HS":
		secret, ok := key.([]byte)
		if !ok {
			return "", errors.New("HMAC SignJWT requires []byte key")
		}
		mac := hmac.New(info.hashNew, secret)
		mac.Write([]byte(signingInput))
		sig = mac.Sum(nil)
	case "RS":
		pk, ok := key.(*rsa.PrivateKey)
		if !ok {
			return "", errors.New("RS SignJWT requires *rsa.PrivateKey")
		}
		h := info.hashNew()
		h.Write([]byte(signingInput))
		sig, err = rsa.SignPKCS1v15(rand.Reader, pk, info.hashID, h.Sum(nil))
		if err != nil {
			return "", err
		}
	case "PS":
		pk, ok := key.(*rsa.PrivateKey)
		if !ok {
			return "", errors.New("PS SignJWT requires *rsa.PrivateKey")
		}
		h := info.hashNew()
		h.Write([]byte(signingInput))
		sig, err = rsa.SignPSS(rand.Reader, pk, info.hashID, h.Sum(nil),
			&rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: info.hashID})
		if err != nil {
			return "", err
		}
	case "ES":
		pk, ok := key.(*ecdsa.PrivateKey)
		if !ok {
			return "", errors.New("ES SignJWT requires *ecdsa.PrivateKey")
		}
		h := info.hashNew()
		h.Write([]byte(signingInput))
		r, s, signErr := ecdsa.Sign(rand.Reader, pk, h.Sum(nil))
		if signErr != nil {
			return "", signErr
		}
		size := info.ecdsaSize
		sig = make([]byte, 2*size)
		rBytes := r.Bytes()
		sBytes := s.Bytes()
		copy(sig[size-len(rBytes):], rBytes)
		copy(sig[2*size-len(sBytes):], sBytes)
	default:
		return "", errors.New("unknown algorithm family")
	}

	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// ParseRSAPublicKey decodes a PEM-encoded RSA public key. Accepts
// both PKCS1 (BEGIN RSA PUBLIC KEY) and PKIX/SPKI (BEGIN PUBLIC
// KEY) blocks — the two formats Go's stdlib emits via
// x509.MarshalPKCS1PublicKey and x509.MarshalPKIXPublicKey.
func ParseRSAPublicKey(pemBytes []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("no PEM block")
	}
	if pk, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		rk, ok := pk.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("PEM does not contain RSA key")
		}
		return rk, nil
	}
	return x509.ParsePKCS1PublicKey(block.Bytes)
}

// ParseECPublicKey decodes a PEM-encoded EC public key in
// PKIX/SPKI format (the format `openssl ec -pubout` produces).
func ParseECPublicKey(pemBytes []byte) (*ecdsa.PublicKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("no PEM block")
	}
	pk, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ek, ok := pk.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("PEM does not contain ECDSA key")
	}
	return ek, nil
}
