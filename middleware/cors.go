package middleware

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/internal/mwhint"
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

	// AllowHeaders is the whitelist of request headers the server
	// permits on cross-origin requests. The preflight response's
	// Access-Control-Allow-Headers is computed as the case-insensitive
	// intersection of the client's Access-Control-Request-Headers and
	// this list — headers the client asks for that are NOT in this
	// list are silently dropped from the response, which causes the
	// browser to refuse to send them on the actual request.
	//
	// Default: Content-Type, Authorization. The literal "*" is
	// supported and means "any header" per the Fetch spec, but ONLY
	// when AllowCredentials is false; with credentials enabled the
	// wildcard is ignored and the configured list is enforced exactly.
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
	allowOrigins := normalizeCORSOriginPatterns(opt.AllowOrigins)
	validateCORSMethods(opt.AllowMethods)
	validateCORSHeaders("AllowHeaders", opt.AllowHeaders, true)
	validateCORSHeaders("ExposeHeaders", opt.ExposeHeaders, false)

	methodsCSV := strings.Join(opt.AllowMethods, ", ")
	headersCSV := strings.Join(opt.AllowHeaders, ", ")
	exposeCSV := strings.Join(opt.ExposeHeaders, ", ")
	maxAgeStr := ""
	if opt.MaxAge > 0 {
		maxAgeStr = strconv.Itoa(opt.MaxAge)
	}
	allowAny := len(allowOrigins) == 1 && allowOrigins[0] == "*"
	// Compile patterns ONCE at construction so the request hot
	// path is a single ToLower(origin) plus direct == / HasPrefix
	// / HasSuffix comparisons. The old EqualFold-per-pattern
	// path repeated the same case-folding work on every request.
	compiledOrigins := compileOrigins(allowOrigins)

	// Lowercase set of allowed request headers, used at preflight to
	// filter Access-Control-Request-Headers against the configured
	// whitelist instead of echoing the client's value verbatim. The
	// previous behavior let a malicious page bypass AllowHeaders by
	// simply listing every header it wanted in the preflight — the
	// configured list had no effect.
	allowHeadersAny := false
	allowHeadersSet := make(map[string]bool, len(opt.AllowHeaders))
	for _, h := range opt.AllowHeaders {
		if h == "*" {
			allowHeadersAny = true
			continue
		}
		allowHeadersSet[strings.ToLower(h)] = true
	}

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
						// Filter the requested header list against the
						// configured AllowHeaders whitelist. The Fetch
						// spec lets the browser send any header it likes
						// in the preflight; the server's job is to
						// answer with the subset it actually permits.
						// Echoing the request verbatim would make the
						// whitelist a no-op.
						switch {
						case allowHeadersAny && !opt.AllowCredentials:
							// "*" is a literal wildcard only when
							// credentials are not used; echo what the
							// client asked for so non-safelisted
							// headers (the whole point of "*") get
							// through.
							if matched := filterRequestedHeaders(reqHeaders); matched != "" {
								res.Header("Access-Control-Allow-Headers", matched)
							}
						default:
							if matched := filterAllowedHeaders(reqHeaders, allowHeadersSet); matched != "" {
								res.Header("Access-Control-Allow-Headers", matched)
							}
							// Empty match → omit the header. The
							// browser will refuse to send the actual
							// request, which is the correct gate.
						}
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

func validateCORSMethods(methods []string) {
	for _, method := range methods {
		if method == "" {
			panic("gogo/middleware: CORS AllowMethods contains an empty method")
		}
		for i := 0; i < len(method); i++ {
			if !isHTTPTokenChar(method[i]) {
				panic("gogo/middleware: CORS AllowMethods contains an invalid HTTP method token")
			}
		}
	}
}

func validateCORSHeaders(field string, headers []string, allowWildcard bool) {
	for _, header := range headers {
		if allowWildcard && header == "*" {
			continue
		}
		if header == "" {
			panic("gogo/middleware: CORS " + field + " contains an empty header")
		}
		for i := 0; i < len(header); i++ {
			if !isHTTPTokenChar(header[i]) {
				panic("gogo/middleware: CORS " + field + " contains an invalid HTTP header token")
			}
		}
	}
}

func normalizeCORSOriginPatterns(patterns []string) []string {
	out := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		normalized, ok := normalizeCORSOriginPattern(pattern)
		if !ok {
			panic("gogo/middleware: CORS AllowOrigins contains an invalid origin")
		}
		out = append(out, normalized)
	}
	return out
}

func normalizeCORSOriginPattern(pattern string) (string, bool) {
	for i := 0; i < len(pattern); i++ {
		if pattern[i] < 0x20 || pattern[i] == 0x7f {
			return "", false
		}
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "*" {
		return "*", true
	}
	if !strings.Contains(pattern, "*") {
		return normalizeOriginValue(pattern)
	}
	idx := strings.Index(pattern, "://*.")
	if idx <= 0 || strings.Contains(pattern[idx+len("://*."):], "*") {
		return "", false
	}
	scheme := pattern[:idx]
	suffix := strings.TrimSuffix(pattern[idx+len("://*."):], "/")
	if suffix == "" {
		return "", false
	}
	parsed, err := url.Parse(scheme + "://wildcard." + suffix)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", false
	}
	return strings.ToLower(parsed.Scheme) + "://*." + strings.TrimPrefix(strings.ToLower(parsed.Host), "wildcard."), true
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

// filterAllowedHeaders parses the comma-separated Access-Control-
// Request-Headers value and returns a CSV of entries that appear in
// the (lowercased) allowed set. The original casing from the request
// is preserved in the output. Unmatched headers are dropped silently
// — the browser's preflight check will then refuse to send those
// headers on the actual request, which is the intended gate.
func filterAllowedHeaders(raw string, allowed map[string]bool) string {
	if len(allowed) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(len(raw))
	for _, p := range strings.Split(raw, ",") {
		h := strings.TrimSpace(p)
		if h == "" || !validCORSHeaderToken(h) {
			continue
		}
		if !allowed[strings.ToLower(h)] {
			continue
		}
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		b.WriteString(h)
	}
	return b.String()
}

func filterRequestedHeaders(raw string) string {
	var b strings.Builder
	b.Grow(len(raw))
	for _, p := range strings.Split(raw, ",") {
		h := strings.TrimSpace(p)
		if h == "" || !validCORSHeaderToken(h) {
			continue
		}
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		b.WriteString(h)
	}
	return b.String()
}

func validCORSHeaderToken(header string) bool {
	if header == "" {
		return false
	}
	for i := 0; i < len(header); i++ {
		if !isHTTPTokenChar(header[i]) {
			return false
		}
	}
	return true
}

// matchCompiledOrigin tests whether origin matches any compiled pattern.
// Runtime origins are normalized with the same parser used for configured
// origins; malformed request headers simply fail closed instead of matching a
// wildcard suffix by raw string shape.
func matchCompiledOrigin(compiled []compiledOrigin, origin string) bool {
	if len(compiled) == 0 {
		return false
	}
	lower, ok := normalizeOriginValue(origin)
	if !ok {
		return false
	}
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
