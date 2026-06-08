// authmw demonstrates a production-ish middleware stack:
//
//   - Helmet with local HTTP HSTS disabled and production HSTS enabled by env.
//
//   - Explicit credentialed CORS for one trusted browser origin.
//
//   - Session + CSRF scoped to browser cookie routes.
//
//   - BasicAuth scoped to admin token minting.
//
//   - JWT scoped to bearer-token API routes, with issuer, audience, and
//     required-claim validation configured in the middleware.
//
//   - WebSocketAuth on the upgrade path, separate from HTTP CORS.
//
//     CGO_ENABLED=1 go run -tags gogo ./examples/authmw
//     curl -i http://localhost:3002/
//     curl -i -c jar http://localhost:3002/browser/csrf
//     curl -i -b jar -c jar -H 'Content-Type: application/json' \
//     -H 'X-CSRF-Token: <token-from-/browser/csrf>' \
//     -d '{"username":"alice","password":"wonderland"}' \
//     http://localhost:3002/browser/login
//     curl -i -b jar http://localhost:3002/browser/private/me
//     curl -i -u admin:dev-admin-password http://localhost:3002/admin/token
//     curl -i -H "Authorization: Bearer <token-from-/admin/token>" \
//     http://localhost:3002/api/bearer/me
package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Role string `json:"role"`
}

type userRecord struct {
	User
	Password string
}

type appConfig struct {
	origin        string
	production    bool
	cookieSecure  bool
	sessionSecret []byte
	csrfSecret    []byte
	jwtSecret     []byte
	jwtIssuer     string
	jwtAudience   string
	adminUser     string
	adminPassword string
	wsTicket      string
}

var usersByName = map[string]userRecord{
	"alice": {
		User:     User{ID: 1, Name: "alice", Role: "user"},
		Password: "wonderland",
	},
}

var usersByID = map[int]User{
	1: {ID: 1, Name: "alice", Role: "user"},
}

func loadConfig() appConfig {
	production := strings.EqualFold(os.Getenv("APP_ENV"), "production")
	return appConfig{
		origin:        env("APP_ORIGIN", "http://localhost:3002"),
		production:    production,
		cookieSecure:  production || envBool("COOKIE_SECURE", false),
		sessionSecret: secretFromEnv("SESSION_SECRET", "dev-session-secret-32-bytes-change-me", production),
		csrfSecret:    secretFromEnv("CSRF_SECRET", "dev-csrf-secret-32-bytes-change-me-now", production),
		jwtSecret:     secretFromEnv("JWT_SECRET", "dev-jwt-secret-32-bytes-change-me-now", production),
		jwtIssuer:     env("JWT_ISSUER", "authmw.example.local"),
		jwtAudience:   env("JWT_AUDIENCE", "authmw-api"),
		adminUser:     env("ADMIN_USER", "admin"),
		adminPassword: requiredInProduction("ADMIN_PASSWORD", "dev-admin-password", production),
		wsTicket:      requiredInProduction("WS_TICKET", "dev-ws-ticket", production),
	}
}

func env(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}

func envBool(name string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		log.Fatalf("%s must be a boolean", name)
		return fallback
	}
}

func secretFromEnv(name, fallback string, production bool) []byte {
	value := requiredInProduction(name, fallback, production)
	if len(value) < 32 {
		log.Fatalf("%s must be at least 32 bytes", name)
	}
	return []byte(value)
}

func requiredInProduction(name, fallback string, production bool) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	if production {
		log.Fatalf("%s is required when APP_ENV=production", name)
	}
	return fallback
}

func main() {
	cfg := loadConfig()

	app, err := gogo.NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	app.Use(middleware.Helmet(middleware.HelmetOptions{
		HSTS:                  hstsPolicy(cfg.production),
		ContentSecurityPolicy: "default-src 'self'; base-uri 'self'; frame-ancestors 'none'",
	}))
	app.Use(middleware.CORS(middleware.CORSOptions{
		AllowOrigins:     []string{cfg.origin},
		AllowMethods:     []string{"GET", "POST", "OPTIONS"},
		AllowHeaders:     []string{"Content-Type", "Authorization", "X-CSRF-Token"},
		AllowCredentials: true,
		MaxAge:           600,
	}))

	app.Use("/browser/*", middleware.NewSession(middleware.SessionOptions{
		Secret:         cfg.sessionSecret,
		CookieSecure:   cfg.cookieSecure,
		CookieSameSite: gogo.SameSiteLax,
		TTL:            8 * time.Hour,
	}))
	app.Use("/browser/*", middleware.CSRF(middleware.CSRFOptions{
		Secret:         cfg.csrfSecret,
		CookieSecure:   cfg.cookieSecure,
		CookieSameSite: gogo.SameSiteLax,
		CookieMaxAge:   int((8 * time.Hour) / time.Second),
	}))
	app.Use("/browser/private/*", requireSession())

	app.Use("/admin/*", middleware.BasicAuth(middleware.BasicAuthOptions{
		Users: map[string]string{cfg.adminUser: cfg.adminPassword},
		Realm: "authmw-admin",
	}))

	app.Use("/api/bearer/*", middleware.JWT(middleware.JWTOptions{
		Secret:         cfg.jwtSecret,
		Algorithm:      middleware.JWTHS256,
		Issuer:         cfg.jwtIssuer,
		Audience:       cfg.jwtAudience,
		RequiredClaims: []string{"sub", "role"},
		Leeway:         30 * time.Second,
	}))

	app.Get("/", gogo.Reply{
		ContentType: "text/plain; charset=utf-8",
		Body:        "authmw example\n",
	})

	app.Get("/browser/csrf", func(res *gogo.Response, req *gogo.Request) {
		token, _ := req.Local(middleware.CSRFLocalKey).(string)
		res.JSON(200, map[string]string{"csrfToken": token})
	})

	app.PostAsync("/browser/login", 4096, func(res *gogo.Response, req *gogo.Request, body []byte) {
		var in struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.Unmarshal(body, &in); err != nil {
			res.Send(400, "text/plain; charset=utf-8", "bad json\n")
			return
		}
		user, ok := authenticate(in.Username, in.Password)
		if !ok {
			res.Send(401, "text/plain; charset=utf-8", "unauthorized\n")
			return
		}
		sess := req.Local(middleware.SessionLocalKey).(*middleware.Session)
		sess.Set("user_id", user.ID)
		res.JSON(200, user)
	})

	app.Get("/browser/private/me", func(res *gogo.Response, req *gogo.Request) {
		user, _ := userFromSession(req)
		res.JSON(200, user)
	})

	app.Get("/admin/token", func(res *gogo.Response, req *gogo.Request) {
		admin, _ := req.Local(middleware.BasicAuthLocalKey).(string)
		token, err := mintJWT(cfg, admin, "admin", 15*time.Minute)
		if err != nil {
			res.Send(500, "text/plain; charset=utf-8", "token error\n")
			return
		}
		res.JSON(200, map[string]string{"token": token})
	})

	app.Get("/api/bearer/me", func(res *gogo.Response, req *gogo.Request) {
		claims := req.Local(middleware.JWTLocalKey).(map[string]any)
		res.JSON(200, claims)
	})

	app.WebSocket("/ws", gogo.WebSocketBehavior{
		Upgrade: middleware.WebSocketAuth(middleware.WebSocketAuthOptions{
			AllowedOrigins:      []string{cfg.origin},
			AllowedSubprotocols: []string{"authmw.v1"},
			Verify: func(ctx *gogo.UpgradeContext) (any, bool, int, string) {
				// Mint short-lived, one-time tickets for browser clients;
				// avoid putting long-lived account tokens in URLs.
				if ctx.QueryParam("ticket") != cfg.wsTicket {
					return nil, false, 401, "bad ticket"
				}
				return User{ID: 0, Name: "websocket-client", Role: "realtime"}, true, 0, ""
			},
		}),
		MaxPayloadLength: 1 << 20,
		IdleTimeout:      30 * time.Second,
		MaxBackpressure:  64 << 10,
		Open: func(ws *gogo.WebSocket) {
			ws.SendText("ready\n")
		},
		Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
			if op != gogo.Text {
				ws.End(1003, "text only")
				return
			}
			ws.SendText("echo: " + string(msg))
		},
	})

	if !app.Listen(3002) {
		log.Fatal("listen :3002 failed")
	}
	log.Printf("gogo~ authmw listening on http://localhost:3002; origin=%s secureCookies=%v", cfg.origin, cfg.cookieSecure)
	app.Run()
}

func hstsPolicy(production bool) string {
	if production {
		return "max-age=63072000; includeSubDomains; preload"
	}
	return "off"
}

func authenticate(username, password string) (User, bool) {
	rec, ok := usersByName[username]
	if !ok || rec.Password != password {
		return User{}, false
	}
	return rec.User, true
}

func requireSession() gogo.Middleware {
	return func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			if _, ok := userFromSession(req); !ok {
				res.Send(401, "text/plain; charset=utf-8", "login required\n")
				return
			}
			next(res, req)
		}
	}
}

func userFromSession(req *gogo.Request) (User, bool) {
	sess, ok := req.Local(middleware.SessionLocalKey).(*middleware.Session)
	if !ok {
		return User{}, false
	}
	id, ok := sess.Get("user_id").(int)
	if !ok {
		return User{}, false
	}
	user, ok := usersByID[id]
	return user, ok
}

func mintJWT(cfg appConfig, subject, role string, ttl time.Duration) (string, error) {
	now := time.Now()
	return middleware.SignJWT(middleware.JWTHS256, cfg.jwtSecret, map[string]any{
		"sub":  subject,
		"role": role,
		"iss":  cfg.jwtIssuer,
		"aud":  cfg.jwtAudience,
		"iat":  now.Unix(),
		"nbf":  now.Add(-30 * time.Second).Unix(),
		"exp":  now.Add(ttl).Unix(),
	})
}
