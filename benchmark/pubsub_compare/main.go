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
// prints throughput, RSS, and per-connection memory:
//
//	go build -tags gogo -o gogo_server ./benchmark/pubsub_compare/
//	go build           -o loadgen     ./benchmark/pubsub_compare/loadgen/
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
// Reference numbers from one run on a 4-vCPU VM (ulimit -n = 4096,
// 128-byte payload, 1000 subscribers, 5 s workload). Rerun on the
// target hardware before quoting — these are indicative, not
// authoritative:
//
//	Metric                           uWS.js v20.51   gogo
//	Baseline RSS (no conns)              48 MB         7.5 MB
//	RSS @ 1000 idle conns                48 MB         8.2 MB
//	RSS @ 3000 idle conns                57 MB         9.3 MB
//	Mem / conn idle (N=3000)            830 B          262 B
//	Single publish, 1000 subs           659k msgs/s    715k msgs/s
//	Batch=100, 1000 subs                280k msgs/s    5.1M msgs/s
//	Batch HTTP-call rate (4 workers)      3 calls/s     51 calls/s
//
// Single-publish is within ~10 % (both bound by the same uWS
// TopicTree fan-out). Batch is where PublishBatch dominates: gogo
// defers the broadcast onto the loop and returns from the HTTP
// handler immediately, so the next request isn't serialised behind
// 100 publishes × 1000 frame writes. uWS.js's app.publish is
// synchronous on the loop — the HTTP handler holds the response
// until the broadcast queues up, and effective throughput collapses.
//
// Caveats: same-machine setup (no NIC latency, shared CPU), 4-vCPU,
// fd-cap 4096 means ~3000 conn ceiling without raising ulimit. RSS
// deltas at small N are page-granular (4 KiB) noise — trust the
// per-conn figure at N=3000.

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
