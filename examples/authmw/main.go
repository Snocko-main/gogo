// authmw demonstrates middleware composition: a bearer-token auth gate
// applied to every route registered after Use, plus a logger that
// wraps before+after.
//
//	CGO_ENABLED=1 go run -tags gogo ./examples/authmw
//	curl -i http://localhost:3002/secret             # 401
//	curl -i -H 'Authorization: Bearer secret-token' http://localhost:3002/secret
package main

import (
	"log"
	"time"

	gogo "uwebsockets-go/gogo"
)

func main() {
	app, err := gogo.NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	// Logger wraps each request: before and after the handler runs.
	app.Use(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			start := time.Now()
			log.Printf("--> %s %s", req.Method(), req.URL())
			next(res, req)
			log.Printf("<-- %s %s (%v)", req.Method(), req.URL(), time.Since(start))
		}
	})

	// Bearer-token gate. Short-circuits with 401 if missing/wrong.
	app.Use(func(next gogo.Handler) gogo.Handler {
		const want = "Bearer secret-token"
		return func(res *gogo.Response, req *gogo.Request) {
			if req.Header("authorization") != want {
				res.Send(401, "text/plain; charset=utf-8", "unauthorized\n")
				return
			}
			next(res, req)
		}
	})

	app.Get("/secret", func(res *gogo.Response, req *gogo.Request) {
		res.Send(200, "text/plain", "the secret is 42\n")
	})

	// Async route — middleware runs sync first, then the handler runs on
	// a goroutine (one extra cgo crossing per req because middleware is
	// registered; without Use, GetAsync would use the zero-cgo path).
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
