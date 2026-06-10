// graceful demonstrates signal-driven single-app graceful shutdown.
//
// This is the drain-oriented shutdown path for one gogo App:
//
//   - SIGINT / SIGTERM stops accepting new connections
//   - accepted requests are allowed to finish naturally
//   - the context deadline force-closes active connections if they hang
//   - resources are closed after Run returns
//
// Run with:
//
//	CGO_ENABLED=1 go run -tags gogo ./examples/graceful
//	curl http://127.0.0.1:3006/health
//	curl http://127.0.0.1:3006/slow
//
// While /slow is running, press Ctrl+C in the server terminal. The
// request should finish before the process exits. RunMultiCore has a
// different group-level shutdown model: MultiCoreHandle.Shutdown calls
// each worker's immediate Shutdown and does not currently expose this
// graceful drain.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	gogo "github.com/Snocko-main/gogo"
)

func main() {
	port := 3006
	if env := os.Getenv("PORT"); env != "" {
		n, err := strconv.Atoi(env)
		if err != nil || n <= 0 {
			log.Fatalf("PORT must be a positive integer, got %q", env)
		}
		port = n
	}

	app, err := gogo.NewApp(gogo.Config{
		BindAddr: "127.0.0.1",
	})
	if err != nil {
		log.Fatal(err)
	}
	// main waits for Run to exit before returning, so Close runs after
	// the graceful drain has finished or timed out.
	defer app.Close()

	var requests atomic.Int64

	app.OnShutdown(func() {
		log.Println("shutdown started: closing listener and draining accepted requests")
	})

	app.Get("/health", gogo.Reply{
		Status:      200,
		ContentType: "application/json",
		Body:        `{"ok":true}`,
	})

	app.Get("/", func(res *gogo.Response, req *gogo.Request) {
		total := requests.Add(1)
		res.Send(200, "text/plain; charset=utf-8",
			fmt.Sprintf("graceful example, request %d\n", total))
	})

	app.GetAsync("/slow", func(res *gogo.Response, req *gogo.Request) {
		total := requests.Add(1)
		// Simulate in-flight work that should finish during a graceful drain.
		time.Sleep(5 * time.Second)
		res.Send(200, "text/plain; charset=utf-8",
			fmt.Sprintf("slow request %d finished\n", total))
	})

	if !app.Listen(port) {
		log.Fatalf("listen 127.0.0.1:%d failed", port)
	}

	runDone := make(chan struct{})
	go func() {
		app.Run()
		close(runDone)
	}()

	log.Printf("gogo~ graceful listening on http://127.0.0.1:%d", port)

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	select {
	case <-signalCtx.Done():
	case <-runDone:
		log.Printf("server stopped before a shutdown signal")
		return
	}
	stopSignals()

	const shutdownTimeout = 30 * time.Second
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	log.Printf("shutdown signal received; draining for up to %s", shutdownTimeout)
	if err := app.ShutdownContext(shutdownCtx); err != nil {
		log.Printf("shutdown deadline reached; active connections were force-closed: %v", err)
	}
	<-runDone

	log.Printf("stopped after serving %d requests", requests.Load())
}
