package main

import (
	"fmt"
	"log"

	uws "uwebsockets-go/uwebsockets"
)

func main() {
	app, err := uws.NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	app.Get("/hello/:name", func(res *uws.Response, req *uws.Request) {
		name := req.Parameter(0)
		if name == "" {
			name = "world"
		}

		res.
			Status(200).
			Header("Content-Type", "text/plain; charset=utf-8").
			End(fmt.Sprintf("hello %s\n", name))
	})

	app.WebSocket("/ws", uws.WebSocketBehavior{
		Message: func(ws *uws.WebSocket, message []byte, opcode uws.OpCode) {
			ws.Send(message, opcode)
		},
	})

	if !app.Listen(3000) {
		log.Fatal("failed to listen on :3000")
	}

	log.Println("listening on http://localhost:3000")
	app.Run()
}
