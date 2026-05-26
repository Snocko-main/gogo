// hello demonstrates the smallest gogo~ server: one sync route and one
// async route, plus a static reply served entirely from C++.
//
//	CGO_ENABLED=1 go run -tags gogo ./examples/hello
//	curl http://localhost:3000/hello/claude
//	curl http://localhost:3000/health
//	curl http://localhost:3000/work
package main

import (
	"log"
	"time"

	gogo "github.com/Snocko-main/gogo"
)

func main() {
	app, err := gogo.NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	// Static reply — served entirely in C++ with zero cgo per request.
	app.Get("/health", gogo.Reply{
		Status:      200,
		ContentType: "application/json",
		Body:        `{"ok":true}`,
	})

	// Sync handler with a route parameter.
	app.Get("/hello/:name", func(res *gogo.Response, req *gogo.Request) {
		res.Send(200, "text/plain; charset=utf-8", "hi "+req.Parameter(0)+"\n")
	})

	// Async handler — runs on a goroutine and is free to block.
	// Without sync middleware, gogo dispatches through the shared worker ring.
	app.GetAsync("/work", func(res *gogo.Response, req *gogo.Request) {
		time.Sleep(10 * time.Millisecond)
		res.Send(200, "text/plain; charset=utf-8", "done\n")
	})

	if !app.Listen(3000) {
		log.Fatal("listen :3000 failed")
	}
	log.Println("gogo~ hello listening on http://localhost:3000")
	app.Run()
}
