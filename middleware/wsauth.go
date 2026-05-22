package middleware

import (
	"net/url"
	"strings"

	"github.com/Snocko-main/gogo"
)

// WebSocketAuthOptions configures the WebSocketAuth helper. Every
// field is optional; the zero value still installs a baseline
// Origin-allow-list check, which is the minimum defense against
// Cross-Site WebSocket Hijacking (CSWSH).
//
// CSWSH is the WebSocket equivalent of CSRF: a third-party page the
// user has open can open ws://yoursite/... from the browser and ride
// the user's session cookies / bearer headers (browsers attach
// ambient credentials to WebSocket handshakes the same way they
// attach them to <img> tags). The HTTP CORS middleware does NOT
// cover WebSocket — the upgrade goes through a separate code path
// in the framework. Without an Origin check at the handshake there's
// nothing stopping the attacker page.
//
// The helper wraps a user-supplied Verify callback so applications
// can layer their own auth (verify a bearer, look up a session row)
// while still benefiting from the standard Origin gate.
type WebSocketAuthOptions struct {
	// AllowedOrigins is the exact-match list of permitted Origin
	// header values. Empty means "no Origin header AND no Sec-
	// WebSocket-Origin header" is required — i.e. CLI clients only,
	// no browser. To allow browser clients from a specific site,
	// list it explicitly: []string{"https://app.example.com"}.
	//
	// Matching is case-insensitive. Trailing slashes are ignored.
	// The literal "*" is treated as "allow any origin" and is
	// intentionally not the zero-value default — opt in explicitly
	// when you understand the risk. Do not mix "*" with explicit
	// origins; the helper panics at construction time.
	//
	// Entries must be valid origins ("scheme://host[:port]"), "null",
	// or "*". Paths other than a single trailing slash, queries,
	// fragments, userinfo, empty entries, and control characters panic
	// at middleware construction.
	AllowedOrigins []string

	// AllowMissingOrigin permits handshakes that arrive with neither
	// Origin nor legacy Sec-WebSocket-Origin. Default false. Useful
	// for CLI tooling (websocat, curl --include) that doesn't set
	// Origin, but dangerous if any of your real clients are browsers
	// — browser requests always carry an Origin and a request without
	// one is either a non-browser or a stripped-down forgery.
	AllowMissingOrigin bool

	// Verify, when non-nil, runs after the Origin check passes and
	// is the place to plug in app-specific auth (bearer-token
	// verification, session-cookie lookup, etc.). Return a non-empty
	// userData to accept the connection with that value attached via
	// ctx.SetUserData; return nil userData with ok=true to accept
	// anonymously; return ok=false to reject with the supplied
	// status / message (defaults to 401 / "unauthorized" when zero).
	//
	// Run on the loop thread — keep it fast. For DB-backed
	// verification, look the token up at HTTP login time and stash
	// the verified bits in a signed cookie that Verify can decode
	// without I/O.
	Verify func(ctx *gogo.UpgradeContext) (userData any, ok bool, rejectStatus int, rejectMsg string)

	// AllowedSubprotocols, when non-empty, restricts the
	// Sec-WebSocket-Protocol negotiation: the helper picks the
	// first protocol from this list that the client also offered
	// in ctx.Protocols(). If the client offered no overlap the
	// connection is rejected with 400. When AllowedSubprotocols is
	// nil the helper accepts without negotiating a protocol. Entries
	// must be valid WebSocket subprotocol tokens; empty or malformed
	// entries panic at middleware construction.
	AllowedSubprotocols []string
}

// WebSocketAuth returns an Upgrade callback that gates incoming
// WebSocket handshakes against an Origin allow-list and an optional
// app-supplied Verify check. Attach it to WebSocketBehavior.Upgrade:
//
//	app.WebSocket("/ws", gogo.WebSocketBehavior{
//	    Upgrade: middleware.WebSocketAuth(middleware.WebSocketAuthOptions{
//	        AllowedOrigins: []string{"https://app.example.com"},
//	        Verify: func(ctx *gogo.UpgradeContext) (any, bool, int, string) {
//	            user, err := verifyToken(ctx.QueryParam("token"))
//	            if err != nil {
//	                return nil, false, 401, "bad token"
//	            }
//	            return user, true, 0, ""
//	        },
//	    }),
//	    Open:    handleOpen,
//	    Message: handleMessage,
//	    Close:   handleClose,
//	})
//
// CSWSH defense is the primary reason this helper exists. If you
// only need a hand-rolled Origin check it's a few lines of code —
// but the failure mode of "forgot to write the Upgrade callback at
// all" is catastrophic enough that having a documented defaults-on
// helper is worth the abstraction.
func WebSocketAuth(opt WebSocketAuthOptions) func(*gogo.UpgradeContext) {
	// Pre-normalize the allow-list once at construction so the
	// per-handshake hot path is just a slice scan.
	allowAny := false
	wildcards := 0
	allowed := make([]string, 0, len(opt.AllowedOrigins))
	for _, o := range opt.AllowedOrigins {
		if o == "*" {
			allowAny = true
			wildcards++
			continue
		}
		allowed = append(allowed, normalizeAllowedOriginValue(o))
	}
	if wildcards > 0 && len(opt.AllowedOrigins) > 1 {
		panic("gogo/middleware: WebSocketAuth AllowedOrigins cannot mix \"*\" with explicit origins")
	}
	subprotocols := make([]string, len(opt.AllowedSubprotocols))
	copy(subprotocols, opt.AllowedSubprotocols)
	for _, protocol := range subprotocols {
		if !validConfiguredWebSocketSubprotocol(protocol) {
			panic("gogo/middleware: WebSocketAuth AllowedSubprotocols contains an invalid token")
		}
	}

	return func(ctx *gogo.UpgradeContext) {
		origin := ctx.Header("origin")
		if origin == "" {
			origin = ctx.Header("sec-websocket-origin")
		}
		if origin == "" {
			if !opt.AllowMissingOrigin {
				ctx.Reject(403, "missing Origin")
				return
			}
		} else {
			if !allowAny && !originAllowed(origin, allowed) {
				ctx.Reject(403, "origin not allowed")
				return
			}
		}

		var userData any
		if opt.Verify != nil {
			ud, ok, status, msg := opt.Verify(ctx)
			if !ok {
				if status == 0 {
					status = 401
				}
				if msg == "" {
					msg = "unauthorized"
				}
				ctx.Reject(status, msg)
				return
			}
			userData = ud
		}

		chosen := ""
		if len(subprotocols) > 0 {
			offered := ctx.Protocols()
			for _, want := range subprotocols {
				for _, got := range offered {
					if strings.EqualFold(want, got) {
						chosen = want
						break
					}
				}
				if chosen != "" {
					break
				}
			}
			if chosen == "" {
				ctx.Reject(400, "no acceptable subprotocol")
				return
			}
		}

		if userData != nil {
			ctx.SetUserData(userData)
		}
		ctx.Accept(chosen)
	}
}

// normalizeAllowedOriginValue lower-cases the scheme + host portion and strips
// a single trailing slash. The allow-list is application-owned config, so
// fail fast on malformed entries instead of producing a silent runtime reject.
func normalizeAllowedOriginValue(o string) string {
	normalized, ok := normalizeOriginValue(o)
	if !ok {
		panic("gogo/middleware: WebSocketAuth AllowedOrigins contains an invalid origin")
	}
	return normalized
}

// normalizeOriginValue lower-cases the scheme + host portion and strips any
// trailing slash so the allow-list comparison is robust against trivial
// differences ("HTTPS://APP" vs "https://app/"). Runtime request origins return
// ok=false when malformed so a hostile peer cannot turn a bad Origin into a
// panic.
func normalizeOriginValue(o string) (string, bool) {
	if o == "" {
		return "", false
	}
	for i := 0; i < len(o); i++ {
		if o[i] < 0x20 || o[i] == 0x7f {
			return "", false
		}
	}
	o = strings.TrimSpace(o)
	if o == "" {
		return "", false
	}
	if o == "null" {
		return "null", true
	}
	o = strings.TrimSuffix(o, "/")
	u, err := url.Parse(o)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil ||
		u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return "", false
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host), true
}

// originAllowed reports whether origin matches any entry in
// allowed (after both sides have been through normalizeOriginValue).
func originAllowed(origin string, allowed []string) bool {
	o, ok := normalizeOriginValue(origin)
	if !ok {
		return false
	}
	for _, a := range allowed {
		if o == a {
			return true
		}
	}
	return false
}

func validConfiguredWebSocketSubprotocol(protocol string) bool {
	if protocol == "" {
		return false
	}
	for i := 0; i < len(protocol); i++ {
		if !isHTTPTokenChar(protocol[i]) {
			return false
		}
	}
	return true
}
