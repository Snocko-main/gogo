package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	uws "uwebsockets-go/uwebsockets"
)

func main() {
	go func() {
		log.Println("pprof on http://localhost:6060/debug/pprof")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	app, err := uws.NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	app.Get("/plain", func(res *uws.Response, req *uws.Request) {
		res.Send("200 OK", "text/plain; charset=utf-8", "hello world\n")
	})

	app.Get("/json", func(res *uws.Response, req *uws.Request) {
		res.Send("200 OK", "application/json", `{"message":"hello world","ok":true}`+"\n")
	})

	app.Get("/hello/:name", func(res *uws.Response, req *uws.Request) {
		res.Send("200 OK", "text/plain; charset=utf-8", "hello "+req.Parameter(0)+"\n")
	})

	app.Get("/sleep", func(res *uws.Response, req *uws.Request) {
		res.Async(func() {
			time.Sleep(2 * time.Millisecond)
			res.Send("200 OK", "text/plain; charset=utf-8", "slept\n")
		})
	})

	if !app.Listen(3002) {
		log.Fatal("failed to listen on :3002")
	}

	log.Println("uWebSockets-Go listening on http://localhost:3002")
	app.Run()
}
