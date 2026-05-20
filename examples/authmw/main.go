// authmw demonstrates the full middleware stack:
//
//   - A global sync logger that wraps every request.
//   - A sync auth gate scoped to /api/* that checks a bearer token without
//     blocking.
//   - An async middleware scoped to /api/* that simulates a DB lookup to
//     resolve the user, then hands the loaded user down to the route handler
//     via Request.SetLocal / Request.Local.
//
//	CGO_ENABLED=1 go run -tags gogo ./examples/authmw
//	curl -i http://localhost:3002/                              # 200, no auth
//	curl -i http://localhost:3002/api/me                        # 401
//	curl -i -H 'Authorization: Bearer secret-token' \
//	     http://localhost:3002/api/me                           # 200 + user JSON
//	curl -i http://localhost:3002/work                          # 200, no auth
package main

import (
	"log"
	"time"

	gogo "github.com/Snocko-main/gogo"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// fakeUserLookup stands in for a DB call. Returns an error for an unknown
// token. Sleeps to make the "this would block" point obvious.
func fakeUserLookup(token string) (*User, error) {
	time.Sleep(2 * time.Millisecond)
	if token != "Bearer secret-token" {
		return nil, errBadToken
	}
	return &User{ID: 42, Name: "alice"}, nil
}

type errString string

func (e errString) Error() string { return string(e) }

const errBadToken = errString("bad token")

func main() {
	app, err := gogo.NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	// Global sync logger — runs on the loop thread for every request.
	app.Use(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			start := time.Now()
			log.Printf("--> %s %s", req.Method(), req.URL())
			next(res, req)
			log.Printf("<-- %s %s (%v)", req.Method(), req.URL(), time.Since(start))
		}
	})

	// Sync auth gate scoped to /api/*: cheap header presence check on the
	// loop thread. Reject early without spending goroutine + DB cycles.
	app.Use("/api/*", func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			if req.Header("authorization") == "" {
				res.Send(401, "text/plain; charset=utf-8", "missing token\n")
				return
			}
			next(res, req)
		}
	})

	// Async user-loader scoped to /api/*: runs on the goroutine that runs
	// the user handler, so the simulated DB lookup is free to block. The
	// resolved user is stashed on the request via SetLocal for the handler.
	app.Use("/api/*", func(next gogo.AsyncHandler) gogo.AsyncHandler {
		return func(res *gogo.Response, req *gogo.Request) {
			user, err := fakeUserLookup(req.Header("authorization"))
			if err != nil {
				res.Send(401, "text/plain; charset=utf-8", "unauthorized\n")
				return
			}
			req.SetLocal("user", user)
			next(res, req)
		}
	})

	// Public landing — only the global logger runs.
	app.Get("/", func(res *gogo.Response, req *gogo.Request) {
		res.Send(200, "text/plain", "public landing\n")
	})

	// Locked async route under /api/. By the time this handler runs:
	//   1. logger logged the inbound request
	//   2. sync /api/* gate confirmed an Authorization header is present
	//   3. async /api/* loader queried the "DB" and stored the user
	app.GetAsync("/api/me", func(res *gogo.Response, req *gogo.Request) {
		user := req.Local("user").(*User)
		res.JSON(200, user)
	})

	// Async route OUTSIDE /api — none of the /api/* gates apply. The global
	// sync logger still wraps it, which keeps GetAsync on the sync-wrapper
	// fallback (one cgo crossing per req). Drop the logger and /work would
	// run via the zero-cgo shared-memory dispatch path.
	app.GetAsync("/work", func(res *gogo.Response, req *gogo.Request) {
		time.Sleep(20 * time.Millisecond)
		res.Send(200, "text/plain", "ok\n")
	})

	if !app.Listen(3002) {
		log.Fatal("listen :3002 failed")
	}
	log.Println("gogo~ authmw listening on http://localhost:3002")
	app.Run()
}
