package middleware

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	mrand "math/rand/v2"
	"sync"

	"github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/internal/mwhint"
)

// RequestIDLocalKey is the key used to stash the request ID into
// req.SetLocal / req.Local. Handlers and downstream middleware read
// the ID via req.Local(middleware.RequestIDLocalKey).(string).
const RequestIDLocalKey = "gogo.requestID"

// RequestIDOptions configures RequestID. Zero value reads / sets
// X-Request-ID and generates 32-hex-char (128-bit) IDs from crypto/rand.
type RequestIDOptions struct {
	// Header is the request/response header name carrying the ID.
	// Default "X-Request-ID". Some shops prefer X-Correlation-ID or
	// Traceparent-style values — set this to whatever your edge proxy
	// already uses so the chain stays consistent.
	Header string

	// MaxLength caps an incoming request ID before it is reused and echoed
	// back. Overlong IDs are discarded and a fresh ID is generated instead.
	// Default 128. Negative disables the length cap.
	MaxLength int

	// Validator can reject incoming IDs that don't match your fleet's
	// format. When nil, the default accepts visible non-space ASCII only.
	// Rejected IDs are replaced with a generated ID.
	Validator func(string) bool

	// Generator produces a new ID when the request has no incoming
	// value. Default generates 32 random hex characters (16 bytes of
	// crypto/rand, 128 bits of entropy). Replace with uuid.NewString
	// or a ULID generator for systems with established conventions.
	Generator func() string
}

// RequestID returns a middleware that ensures every request has an ID:
//   - If the incoming Header value is non-empty and passes the configured
//     length/format policy, that ID is reused.
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
	validateRequestIDHeaderName(opt.Header)
	if opt.Generator == nil {
		opt.Generator = defaultRequestID
	}
	if opt.MaxLength == 0 {
		opt.MaxLength = 128
	}
	if opt.Validator == nil {
		opt.Validator = validDefaultRequestID
	}
	// uWS header lookups are case-insensitive when going through the
	// snapshot path but case-sensitive (lowercase only) through the
	// sync path. Use the lowercased form for the lookup.
	lookupName := lowercaseAscii(opt.Header)

	return mwhint.Hinted{Place: mwhint.Both, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			id := req.Header(lookupName)
			if !validRequestIDValue(id, opt.MaxLength, opt.Validator) {
				id = opt.Generator()
			}
			if !validRequestIDValue(id, opt.MaxLength, opt.Validator) {
				id = defaultRequestID()
			}
			req.SetLocal(RequestIDLocalKey, id)
			res.Header(opt.Header, id)
			next(res, req)
		}
	})}
}

func requestIDTooLong(id string, max int) bool {
	return max >= 0 && len(id) > max
}

func validRequestIDValue(id string, max int, validator func(string) bool) bool {
	return id != "" && !requestIDTooLong(id, max) && validRequestIDHeaderValue(id) && validator(id)
}

func validRequestIDHeaderValue(id string) bool {
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c <= ' ' || c >= 0x7f {
			return false
		}
	}
	return true
}

func validateRequestIDHeaderName(name string) {
	if name == "" {
		panic("gogo/middleware: RequestID header name is empty")
	}
	for i := 0; i < len(name); i++ {
		if !isHTTPTokenChar(name[i]) {
			panic(fmt.Sprintf("gogo/middleware: RequestID header name %q contains invalid byte 0x%02x (must be RFC 7230 tchar)", name, name[i]))
		}
	}
}

func isHTTPTokenChar(c byte) bool {
	if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
		return true
	}
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return false
}

func validDefaultRequestID(id string) bool {
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c <= ' ' || c >= 0x7f {
			return false
		}
	}
	return true
}

// defaultRequestID generates 32 hex characters (128 bits) from
// crypto/rand. 128 bits keeps collision probability negligible even
// at >10^15 IDs and matches the entropy of FastRequestIDGenerator —
// the previous 64-bit default could collide in long-retention traces.
func defaultRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand.Read should never fail on modern OSes; if it
		// does we fall back to a constant so the pipeline doesn't
		// break. Production deployments should set their own
		// Generator if they need stricter guarantees.
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(buf[:])
}

// FastRequestIDGenerator returns a generator that produces 22-character
// base64url IDs from a ChaCha8 PRNG seeded once per pool slot from
// crypto/rand. The entropy matches the default generator's 16-byte
// (128-bit) payload while emitting a shorter 22-character base64url string
// instead of the default 32-character hex string.
//
// Tracing identifiers don't need cryptographic strength — they need
// uniqueness with high probability. ChaCha8 has a 256-bit internal
// state so collisions in any realistic window are astronomically
// unlikely; the seed itself comes from crypto/rand so an attacker
// can't predict the sequence from the outside.
//
// Use it via RequestIDOptions.Generator:
//
//	app.Use(middleware.RequestID(middleware.RequestIDOptions{
//	    Generator: middleware.FastRequestIDGenerator(),
//	}))
//
// The returned function is safe for concurrent use — a sync.Pool
// serializes access to each ChaCha8 instance. A single shared
// instance under a mutex would tank throughput at high RPS; the
// pool lets each loop / goroutine pull its own ChaCha8 and put it
// back when done.
func FastRequestIDGenerator() func() string {
	pool := &sync.Pool{
		New: func() any {
			var seed [32]byte
			if _, err := rand.Read(seed[:]); err != nil {
				// Same defensive behavior as defaultRequestID — a
				// seed-time crypto/rand failure on a modern OS is
				// effectively impossible, but we don't panic the
				// whole app if it happens.
				return mrand.NewChaCha8([32]byte{})
			}
			return mrand.NewChaCha8(seed)
		},
	}
	return func() string {
		r := pool.Get().(*mrand.ChaCha8)
		var b [16]byte
		_, _ = r.Read(b[:])
		pool.Put(r)
		return base64.RawURLEncoding.EncodeToString(b[:])
	}
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
