package middleware

import (
	"strconv"
	"strings"

	"uwebsockets-go/gogo"
	"uwebsockets-go/gogo/internal/mwhint"
)

// CORSOptions configures the CORS middleware. Zero value is permissive
// (Access-Control-Allow-Origin: *, common methods, common headers); use
// the explicit struct for production deployments.
type CORSOptions struct {
	// AllowOrigins is the list of origins that are allowed to make
	// cross-origin requests. "*" allows any origin (default). Exact
	// hostnames or full origins (https://app.example.com) are matched
	// case-insensitively. Wildcard suffixes like "https://*.example.com"
	// are supported — the literal "*" must appear right after "://".
	//
	// AllowCredentials cannot be combined with "*" — browsers reject it
	// per spec. When credentials are required, list the origins
	// explicitly.
	AllowOrigins []string

	// AllowMethods is the list of HTTP methods returned in the preflight
	// Access-Control-Allow-Methods header. Defaults to the standard
	// safe-plus-write set: GET, POST, PUT, PATCH, DELETE, OPTIONS, HEAD.
	AllowMethods []string

	// AllowHeaders is the list of headers returned in the preflight
	// Access-Control-Allow-Headers. Defaults to common request headers
	// that don't satisfy the CORS-safelisted-request-header check
	// (Content-Type, Authorization).
	AllowHeaders []string

	// ExposeHeaders lists response headers the browser is allowed to
	// surface to client JavaScript. Empty by default (browsers expose
	// only the CORS-safelisted-response headers).
	ExposeHeaders []string

	// AllowCredentials sets Access-Control-Allow-Credentials: true.
	// Cannot be combined with AllowOrigins == []string{"*"}.
	AllowCredentials bool

	// MaxAge sets Access-Control-Max-Age in seconds. The browser caches
	// the preflight result for this long. 0 omits the header (browser
	// default, ~5 seconds).
	MaxAge int
}

var defaultAllowMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"}
var defaultAllowHeaders = []string{"Content-Type", "Authorization"}

// CORS returns a Middleware that sets the appropriate Access-Control-*
// headers and short-circuits OPTIONS preflight requests with a 204.
// Install it like any other middleware:
//
//	app.Use(middleware.CORS(middleware.CORSOptions{
//	    AllowOrigins: []string{"https://app.example.com"},
//	    AllowCredentials: true,
//	}))
//
// Preflight requests (OPTIONS + Access-Control-Request-Method header)
// reach this middleware even when the path has no user-registered
// OPTIONS handler — gogo's App.Listen auto-registers a global catch-all
// route the moment any middleware is registered, so the middleware
// chain fires on every URL the way express / fiber users expect.
func CORS(opts ...CORSOptions) mwhint.Hinted {
	var opt CORSOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	if len(opt.AllowOrigins) == 0 {
		opt.AllowOrigins = []string{"*"}
	}
	if len(opt.AllowMethods) == 0 {
		opt.AllowMethods = defaultAllowMethods
	}
	if len(opt.AllowHeaders) == 0 {
		opt.AllowHeaders = defaultAllowHeaders
	}
	if opt.AllowCredentials && len(opt.AllowOrigins) == 1 && opt.AllowOrigins[0] == "*" {
		panic("gogo/middleware: AllowCredentials=true cannot be combined with AllowOrigins={\"*\"}; list explicit origins")
	}

	methodsCSV := strings.Join(opt.AllowMethods, ", ")
	headersCSV := strings.Join(opt.AllowHeaders, ", ")
	exposeCSV := strings.Join(opt.ExposeHeaders, ", ")
	maxAgeStr := ""
	if opt.MaxAge > 0 {
		maxAgeStr = strconv.Itoa(opt.MaxAge)
	}
	allowAny := len(opt.AllowOrigins) == 1 && opt.AllowOrigins[0] == "*"
	// Compile patterns ONCE at construction so the request hot
	// path is a single ToLower(origin) plus direct == / HasPrefix
	// / HasSuffix comparisons. The old EqualFold-per-pattern
	// path repeated the same case-folding work on every request.
	compiledOrigins := compileOrigins(opt.AllowOrigins)

	return mwhint.Hinted{Place: mwhint.Both, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			origin := req.Header("origin")
			allowed := ""
			if allowAny && !opt.AllowCredentials {
				allowed = "*"
			} else if origin != "" && matchCompiledOrigin(compiledOrigins, origin) {
				allowed = origin
			}

			// Preflight short-circuit. uWS locks the response status on
			// the first writeHeader call (it auto-writes "200 OK" if no
			// status was sent first), so for the preflight 204 we must
			// call Status BEFORE any Header — otherwise the response
			// goes out as 200 with our headers attached.
			isPreflight := req.Method() == "options" &&
				req.Header("access-control-request-method") != ""

			if isPreflight {
				res.Status(204)
				if allowed != "" {
					res.Header("Access-Control-Allow-Origin", allowed)
					res.Header("Vary", "Origin")
					if opt.AllowCredentials {
						res.Header("Access-Control-Allow-Credentials", "true")
					}
					res.Header("Access-Control-Allow-Methods", methodsCSV)
					if reqHeaders := req.Header("access-control-request-headers"); reqHeaders != "" {
						res.Header("Access-Control-Allow-Headers", reqHeaders)
					} else if headersCSV != "" {
						res.Header("Access-Control-Allow-Headers", headersCSV)
					}
					if maxAgeStr != "" {
						res.Header("Access-Control-Max-Age", maxAgeStr)
					}
				}
				res.End("")
				return
			}

			// Non-preflight (actual request): attach CORS headers but
			// let the handler control status. uWS will lock to 200 on
			// the first header below if the handler hasn't called
			// Status yet, but that's the expected default and the
			// handler can still override via Send / JSON / etc. which
			// build the response line themselves at end time.
			if allowed != "" {
				res.Header("Access-Control-Allow-Origin", allowed)
				res.Header("Vary", "Origin")
				if opt.AllowCredentials {
					res.Header("Access-Control-Allow-Credentials", "true")
				}
				if exposeCSV != "" {
					res.Header("Access-Control-Expose-Headers", exposeCSV)
				}
			}

			next(res, req)
		}
	})}
}

// compiledOrigin is the parsed-once form of an AllowOrigins entry.
// Exact patterns store their lowercase form in full; wildcard
// patterns (scheme://*.domain) split into the scheme prefix and
// the dotted suffix, both lowercase, so the request hot path is
// pure HasPrefix / HasSuffix against a single ToLower(origin).
type compiledOrigin struct {
	full     string // lowercase exact match (empty if wildcard)
	wildcard bool
	scheme   string // lowercase, no "://" (only set when wildcard)
	suffix   string // ".example.com" (leading dot, lowercase)
}

// compileOrigins converts the user-supplied AllowOrigins list into
// compiledOrigin form once at middleware construction. The "*"
// wildcard is handled outside this function (allowAny short-circuit
// in CORS) so it does not appear in the compiled slice.
func compileOrigins(patterns []string) []compiledOrigin {
	out := make([]compiledOrigin, 0, len(patterns))
	for _, p := range patterns {
		if p == "*" {
			// Handled by the allowAny fast path; skip so we
			// don't accidentally lowercase the literal "*".
			continue
		}
		lp := strings.ToLower(p)
		if idx := strings.Index(lp, "://*."); idx > 0 {
			out = append(out, compiledOrigin{
				wildcard: true,
				scheme:   lp[:idx],
				suffix:   "." + lp[idx+len("://*."):],
			})
			continue
		}
		out = append(out, compiledOrigin{full: lp})
	}
	return out
}

// matchCompiledOrigin tests whether origin matches any compiled
// pattern. Lowercases origin once, then runs a single == /
// HasPrefix / HasSuffix per pattern.
func matchCompiledOrigin(compiled []compiledOrigin, origin string) bool {
	if len(compiled) == 0 {
		return false
	}
	lower := strings.ToLower(origin)
	for _, p := range compiled {
		if p.wildcard {
			if strings.HasPrefix(lower, p.scheme+"://") &&
				strings.HasSuffix(lower, p.suffix) {
				return true
			}
			continue
		}
		if p.full == lower {
			return true
		}
	}
	return false
}
