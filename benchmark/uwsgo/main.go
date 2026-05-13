package main

import (
	"log"
	"time"

	uws "uwebsockets-go/uwebsockets"
)

func main() {
	app, err := uws.NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	app.Get("/plain", func(res *uws.Response, req *uws.Request) {
		res.
			Status("200 OK").
			Header("Content-Type", "text/plain; charset=utf-8").
			End("hello world\n")
	})

	app.Get("/json", func(res *uws.Response, req *uws.Request) {
		res.
			Status("200 OK").
			Header("Content-Type", "application/json").
			End(`{"message":"hello world","ok":true}` + "\n")
	})

	app.Get("/hello/:name", func(res *uws.Response, req *uws.Request) {
		res.
			Status("200 OK").
			Header("Content-Type", "text/plain; charset=utf-8").
			End("hello " + req.Parameter(0) + "\n")
	})

	app.Get("/sleep", func(res *uws.Response, req *uws.Request) {
		res.Async(func() {
			time.Sleep(2 * time.Millisecond)
			res.
				Status("200 OK").
				Header("Content-Type", "text/plain; charset=utf-8").
				End("slept\n")
		})
	})

	if !app.Listen(3002) {
		log.Fatal("failed to listen on :3002")
	}

	log.Println("uWebSockets-Go listening on http://localhost:3002")
	app.Run()
}
