//go:build cgo && gogo

package gogo_test

import (
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
)

// startAppB mirrors startApp but takes testing.TB so it works for
// both tests and benchmarks. Kept separate so we don't have to touch
// every existing startApp call site.
func startAppB(tb testing.TB, configure func(*gogo.App)) (*gogo.App, int, func()) {
	tb.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	ready := make(chan *gogo.App, 1)
	listenErr := make(chan error, 1)
	runDone := make(chan struct{})

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		app, err := gogo.NewApp(gogo.Config{BindAddr: "127.0.0.1"})
		if err != nil {
			listenErr <- err
			close(runDone)
			return
		}
		configure(app)
		if !app.Listen(port) {
			listenErr <- fmt.Errorf("listen :%d failed", port)
			app.Close()
			close(runDone)
			return
		}
		ready <- app
		app.Run()
		app.Close()
		close(runDone)
	}()

	var app *gogo.App
	select {
	case app = <-ready:
	case err := <-listenErr:
		tb.Fatalf("app start: %v", err)
	case <-time.After(5 * time.Second):
		tb.Fatal("app setup timed out")
	}
	// Spin-wait until the listener accepts.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 50*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	teardown := func() {
		app.Shutdown()
		<-runDone
	}
	return app, port, teardown
}

// BenchmarkAppPublishNoSubs isolates the cost of app.Publish on the
// hot cross-thread path (cgo crossing + bridge alloc + loop->defer).
// No subscribers, so uWS's TopicTree::publish exits early after the
// topic lookup miss — what we're measuring is the publish *overhead*
// our wrapper adds, not the fan-out cost.
//
// Drains synchronously by closing the app on b.Cleanup so the
// deferred publishes complete before the benchmark exits.
func BenchmarkAppPublishNoSubs(b *testing.B) {
	app, _, teardown := startAppB(b, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{})
	})
	b.Cleanup(teardown)

	payload := make([]byte, 128)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		app.Publish("bench/topic", payload, gogo.Text)
	}
}

// BenchmarkAppPublishCrossover sweeps batch sizes to find the
// crossover point where PublishBatch starts beating a Publish loop.
// Run with -bench=BenchmarkAppPublishCrossover and inspect the
// per-publish cost (ns/op divided by N). Below the crossover,
// PublishBatch's fixed Go-side packing cost (one buf slice +
// one items slice) outweighs the cgo + defer-mutex savings; above
// it, batching dominates.
//
// Reports two numbers per N: "loop" = N individual Publish calls,
// "batch" = one PublishBatch of N items. Same payload size in both.
func BenchmarkAppPublishCrossover(b *testing.B) {
	for _, n := range []int{1, 2, 5, 10, 50, 100} {
		app, _, teardown := startAppB(b, func(app *gogo.App) {
			app.WebSocket("/ws", gogo.WebSocketBehavior{})
		})
		payload := make([]byte, 128)
		batch := make([]gogo.PublishMessage, n)
		for i := range batch {
			batch[i] = gogo.PublishMessage{
				Topic:   "bench/topic",
				Message: payload,
				OpCode:  gogo.Text,
			}
		}

		b.Run(fmt.Sprintf("loop/N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < n; j++ {
					app.Publish("bench/topic", payload, gogo.Text)
				}
			}
		})
		b.Run(fmt.Sprintf("batch/N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				app.PublishBatch(batch)
			}
		})
		teardown()
	}
}

// BenchmarkAppPublish1Sub: one connected subscriber, real fan-out.
// Catches regressions in the full path (publish enqueue + loop drain
// + frame write). The subscriber drains in a background goroutine
// driven by the test wsClient so reads don't backpressure the loop.
func BenchmarkAppPublish1Sub(b *testing.B) {
	app, port, teardown := startAppB(b, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("bench/topic")
			},
			// Generous buffers; benchmark must not stall on backpressure.
			MaxBackpressure: 64 * 1024 * 1024,
		})
	})
	b.Cleanup(teardown)

	client, err := dialWebSocket(port, "/ws")
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	b.Cleanup(client.Close)
	// Let the Open handler subscribe before we start measuring.
	time.Sleep(50 * time.Millisecond)

	// Drain reader — keeps backpressure low so the publish path doesn't
	// block waiting for the kernel send buffer.
	stop := make(chan struct{})
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, err := client.ReadText(500 * time.Millisecond)
			if err != nil {
				return
			}
		}
	}()
	b.Cleanup(func() {
		close(stop)
		<-drainDone
	})

	payload := make([]byte, 128)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		app.Publish("bench/topic", payload, gogo.Text)
	}
}
