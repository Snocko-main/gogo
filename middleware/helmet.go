package middleware

import (
	"github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/internal/mwhint"
)

// HelmetOptions configures Helmet. Each field's zero value falls back
// to the OWASP-recommended default listed below. Set a field to the
// sentinel string "off" to suppress that header entirely.
type HelmetOptions struct {
	// HSTS sets Strict-Transport-Security. Default
	// "max-age=15552000; includeSubDomains" (180 days). The header is
	// only meaningful over HTTPS; browsers ignore it on plain HTTP.
	// Set "off" to omit (useful for HTTP-only dev environments).
	HSTS string

	// ContentSecurityPolicy sets Content-Security-Policy. Empty by
	// default — CSP is highly app-specific and a wrong policy breaks
	// pages. Set "off" or leave empty to omit. Common starter:
	// "default-src 'self'".
	ContentSecurityPolicy string

	// FrameOptions sets X-Frame-Options. Default "SAMEORIGIN".
	// Standard values: DENY, SAMEORIGIN. "off" omits.
	FrameOptions string

	// ContentTypeOptions sets X-Content-Type-Options. Default
	// "nosniff". "off" omits.
	ContentTypeOptions string

	// ReferrerPolicy sets Referrer-Policy. Default "no-referrer".
	// "off" omits.
	ReferrerPolicy string

	// XSSProtection sets X-XSS-Protection. Default "0" — modern
	// guidance is to disable the legacy IE/old-Chrome filter, which
	// could be abused to introduce XSS in otherwise safe pages.
	// "off" omits.
	XSSProtection string

	// DNSPrefetchControl sets X-DNS-Prefetch-Control. Default "off"
	// (browsers will not pre-resolve hostnames found in the page).
	// "off" as a value still emits the header; pass "" to use the
	// default and pass the literal "" override using SetHeader-style
	// is N/A — use HelmetOptions{DNSPrefetchControl: "on"} to enable.
	// To suppress the header entirely set "omit".
	DNSPrefetchControl string

	// DownloadOptions sets X-Download-Options (IE legacy). Default
	// "noopen". "omit" suppresses.
	DownloadOptions string

	// PermittedCrossDomainPolicies sets
	// X-Permitted-Cross-Domain-Policies (Adobe legacy). Default
	// "none". "omit" suppresses.
	PermittedCrossDomainPolicies string

	// CrossOriginOpenerPolicy sets Cross-Origin-Opener-Policy.
	// Default "same-origin" — isolates the browsing context group
	// to mitigate Spectre / side-channel leaks. "omit" suppresses.
	CrossOriginOpenerPolicy string

	// CrossOriginResourcePolicy sets Cross-Origin-Resource-Policy.
	// Default "same-origin". "omit" suppresses.
	CrossOriginResourcePolicy string
}

// Helmet returns a Middleware that sets a curated bundle of HTTP
// security response headers on every request. The defaults follow
// OWASP / helmet.js guidance and are safe to drop into most apps;
// HSTS and CSP are the two that typically need explicit configuration
// before going to production.
//
// The middleware does not inspect the request — every response gets
// the same headers. Pair with App.Use so the headers attach to every
// route, including 404s for unknown paths.
//
//	app.Use(middleware.Helmet())
//
//	app.Use(middleware.Helmet(middleware.HelmetOptions{
//	    ContentSecurityPolicy: "default-src 'self'",
//	    HSTS: "max-age=63072000; includeSubDomains; preload",
//	}))
func Helmet(opts ...HelmetOptions) mwhint.Hinted {
	var opt HelmetOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	headers := buildHelmetHeaders(opt)
	for _, h := range headers {
		validateHelmetHeaderValue(h.key, h.value)
	}

	return mwhint.Hinted{Place: mwhint.Both, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			for _, h := range headers {
				res.Header(h.key, h.value)
			}
			next(res, req)
		}
	})}
}

func validateHelmetHeaderValue(key, value string) {
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\r', '\n', 0:
			panic("gogo/middleware: Helmet " + key + " contains a control character")
		}
	}
}

type helmetHeader struct{ key, value string }

func buildHelmetHeaders(opt HelmetOptions) []helmetHeader {
	pick := func(set string, def string) string {
		if set == "off" || set == "omit" {
			return ""
		}
		if set == "" {
			return def
		}
		return set
	}

	out := make([]helmetHeader, 0, 10)
	if v := pick(opt.HSTS, "max-age=15552000; includeSubDomains"); v != "" {
		out = append(out, helmetHeader{"Strict-Transport-Security", v})
	}
	if v := opt.ContentSecurityPolicy; v != "" && v != "off" && v != "omit" {
		out = append(out, helmetHeader{"Content-Security-Policy", v})
	}
	if v := pick(opt.FrameOptions, "SAMEORIGIN"); v != "" {
		out = append(out, helmetHeader{"X-Frame-Options", v})
	}
	if v := pick(opt.ContentTypeOptions, "nosniff"); v != "" {
		out = append(out, helmetHeader{"X-Content-Type-Options", v})
	}
	if v := pick(opt.ReferrerPolicy, "no-referrer"); v != "" {
		out = append(out, helmetHeader{"Referrer-Policy", v})
	}
	if v := pick(opt.XSSProtection, "0"); v != "" {
		out = append(out, helmetHeader{"X-XSS-Protection", v})
	}
	if v := pick(opt.DNSPrefetchControl, "off"); v != "" {
		out = append(out, helmetHeader{"X-DNS-Prefetch-Control", v})
	}
	if v := pick(opt.DownloadOptions, "noopen"); v != "" {
		out = append(out, helmetHeader{"X-Download-Options", v})
	}
	if v := pick(opt.PermittedCrossDomainPolicies, "none"); v != "" {
		out = append(out, helmetHeader{"X-Permitted-Cross-Domain-Policies", v})
	}
	if v := pick(opt.CrossOriginOpenerPolicy, "same-origin"); v != "" {
		out = append(out, helmetHeader{"Cross-Origin-Opener-Policy", v})
	}
	if v := pick(opt.CrossOriginResourcePolicy, "same-origin"); v != "" {
		out = append(out, helmetHeader{"Cross-Origin-Resource-Policy", v})
	}
	return out
}
