// gogo server — mirror of uws_server.js for the comparison bench.
//
// Same surface:
// - WS /ws subscribes to "bench/topic" in Open
// - GET /publish?n=N broadcasts via App.Publish in a loop
// - GET /publishbatch?n=N broadcasts via App.PublishBatch
// - GET /stat returns RSS + conn count as JSON

package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"

	gogo "uwebsockets-go/gogo"
)

func main() {
	port := 7100
	if p := os.Getenv("PORT"); p != "" {
		port, _ = strconv.Atoi(p)
	}
	payload := make([]byte, 128)
	for i := range payload {
		payload[i] = 'A'
	}

	runtime.LockOSThread()
	app, err := gogo.NewApp()
	if err != nil {
		fmt.Fprintln(os.Stderr, "NewApp:", err)
		os.Exit(1)
	}

	var conns atomic.Int64

	app.WebSocket("/ws", gogo.WebSocketBehavior{
		MaxPayloadLength: 16 * 1024,
		MaxBackpressure:  64 * 1024 * 1024,
		Open: func(ws *gogo.WebSocket) {
			ws.Subscribe("bench/topic")
			conns.Add(1)
		},
		Close: func(*gogo.WebSocket, int, []byte) {
			conns.Add(-1)
		},
	})

	// Pre-built batch struct reused per /publishbatch call so we don't
	// reallocate per-request in the path under test. The batch's
	// underlying byte buffers come from `payload` — same Message
	// pointer is fine because PublishBatch copies before returning.
	app.Get("/publish", func(res *gogo.Response, req *gogo.Request) {
		n := req.QueryInt("n", 1)
		for i := 0; i < n; i++ {
			app.Publish("bench/topic", payload, gogo.Text)
		}
		res.Send(200, "text/plain", strconv.Itoa(n))
	})

	app.Get("/publishbatch", func(res *gogo.Response, req *gogo.Request) {
		n := req.QueryInt("n", 1)
		batch := make([]gogo.PublishMessage, n)
		for i := range batch {
			batch[i] = gogo.PublishMessage{
				Topic:   "bench/topic",
				Message: payload,
				OpCode:  gogo.Text,
			}
		}
		app.PublishBatch(batch)
		res.Send(200, "text/plain", strconv.Itoa(n))
	})

	app.Get("/stat", func(res *gogo.Response, req *gogo.Request) {
		var rss int64
		var ru syscall.Rusage
		if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) == nil {
			// ru_maxrss on Linux is in KiB.
			rss = ru.Maxrss * 1024
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		body := fmt.Sprintf(`{"rss":%d,"heapUsed":%d,"conns":%d}`,
			rss, ms.HeapAlloc, conns.Load())
		res.Send(200, "application/json", body)
	})

	if !app.Listen(port) {
		fmt.Fprintln(os.Stderr, "listen failed")
		os.Exit(1)
	}
	fmt.Printf("gogo listening on :%d\n", port)
	app.Run()
}
