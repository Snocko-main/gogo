package middleware

import (
	"crypto/rand"
	"encoding/hex"

	"uwebsockets-go/gogo"
	"uwebsockets-go/gogo/internal/mwhint"
)

// RequestIDLocalKey is the key used to stash the request ID into
// req.SetLocal / req.Local. Handlers and downstream middleware read
// the ID via req.Local(middleware.RequestIDLocalKey).(string).
const RequestIDLocalKey = "gogo.requestID"

// RequestIDOptions configures RequestID. Zero value reads / sets
// X-Request-ID and generates 16-hex-char IDs from crypto/rand.
type RequestIDOptions struct {
	// Header is the request/response header name carrying the ID.
	// Default "X-Request-ID". Some shops prefer X-Correlation-ID or
	// Traceparent-style values — set this to whatever your edge proxy
	// already uses so the chain stays consistent.
	Header string

	// Generator produces a new ID when the request has no incoming
	// value. Default generates 16 random hex characters (8 bytes of
	// crypto/rand). Replace with uuid.NewString or a ULID generator
	// for systems with established conventions.
	Generator func() string
}

// RequestID returns a middleware that ensures every request has an ID:
//   - If the incoming Header value is non-empty, that ID is reused.
//   - Otherwise a new ID is generated via Options.Generator.
//
// The ID is echoed on the response in the same header and stashed in
// req.Locals so handlers / downstream middleware can read it via
//
//	id, _ := req.Local(middleware.RequestIDLocalKey).(string)
//
// Pair with Logger by extending LogEntry / Format to include the ID.
func RequestID(opts ...RequestIDOptions) mwhint.Hinted {
	var opt RequestIDOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.Header == "" {
		opt.Header = "X-Request-ID"
	}
	if opt.Generator == nil {
		opt.Generator = defaultRequestID
	}
	// uWS header lookups are case-insensitive when going through the
	// snapshot path but case-sensitive (lowercase only) through the
	// sync path. Use the lowercased form for the lookup.
	lookupName := lowercaseAscii(opt.Header)

	return mwhint.Hinted{Place: mwhint.Both, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			id := req.Header(lookupName)
			if id == "" {
				id = opt.Generator()
			}
			req.SetLocal(RequestIDLocalKey, id)
			res.Header(opt.Header, id)
			next(res, req)
		}
	})}
}

// defaultRequestID generates 16 hex characters from 8 bytes of
// crypto/rand. ~1 µs per call; effectively zero collision risk for
// any reasonable QPS / retention window.
func defaultRequestID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand.Read should never fail on modern OSes; if it
		// does we fall back to a constant so the pipeline doesn't
		// break. Production deployments should set their own
		// Generator if they need stricter guarantees.
		return "00000000-rand-fail"
	}
	return hex.EncodeToString(buf[:])
}

func lowercaseAscii(s string) string {
	hasUpper := false
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			hasUpper = true
			break
		}
	}
	if !hasUpper {
		return s
	}
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}
