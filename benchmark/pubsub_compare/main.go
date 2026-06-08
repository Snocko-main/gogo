// gogo server — mirror of uws_server.js for the side-by-side bench.
//
// This package compares gogo against uWebSockets.js v20 with both
// servers exposing the same surface:
//
//   - WS /ws            subscribes to "bench/topic" in Open
//   - GET /publish?n=N  broadcasts N msgs via App.Publish in a loop
//   - GET /publishbatch?n=N
//                       broadcasts via App.PublishBatch (gogo) or
//                       a tight app.publish loop (uWS.js — no batch
//                       API exists)
//   - GET /stat         RSS + conn count as JSON
//
// A single Go load generator drives them over WebSocket + HTTP and
// prints throughput, RSS, and per-connection memory. For reproducible
// v0.7 baseline runs, see README.md and run_baseline.sh:
//
//	(cd benchmark && go build -tags gogo -o gogo_server ./pubsub_compare)
//	(cd benchmark && go build           -o loadgen     ./pubsub_compare/loadgen)
//
//	# uWS.js side
//	mkdir -p /tmp/wsbench && cd /tmp/wsbench && npm init -y && \
//	  npm install uWebSockets.js@uNetworking/uWebSockets.js#v20.51.0
//	PORT=7000 node ./benchmark/pubsub_compare/uws_server.js &
//	./loadgen -target 127.0.0.1:7000 -conns 1000 -duration 5s \
//	  -batch 100 -mode publishbatch -label uWS.js
//
//	# gogo side
//	PORT=7100 ./gogo_server &
//	./loadgen -target 127.0.0.1:7100 -conns 1000 -duration 5s \
//	  -batch 100 -mode publishbatch -label gogo
//
// Always `pkill -9 -f "node|gogo_server"` between iterations — a
// stale server holding the port made a debug session here last
// several hours (SO_REUSEPORT means a fresh server binds OK but
// requests still hit the old one).
//
// Reference numbers and caveats live in README.md. Rerun the baseline
// on target hardware before quoting results.

package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"

	gogo "github.com/Snocko-main/gogo"
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
			rss = ru.Maxrss
			// ru_maxrss is bytes on Darwin and KiB on Linux.
			if runtime.GOOS != "darwin" {
				rss *= 1024
			}
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
