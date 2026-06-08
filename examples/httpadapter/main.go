// httpadapter demonstrates using stdlib net/http handlers inside gogo.
//
// Run:
//
//	CGO_ENABLED=1 go run -tags gogo ./examples/httpadapter
//	curl http://127.0.0.1:3005/
//	curl http://127.0.0.1:3005/debug/vars
//	curl http://127.0.0.1:3005/debug/pprof/goroutine?debug=1
//	curl -o cpu.pprof 'http://127.0.0.1:3005/debug/pprof/profile?seconds=5'
package main

import (
	"expvar"
	"log"
	"net/http"
	"net/http/pprof"
	"time"

	gogo "github.com/Snocko-main/gogo"
)

var (
	requestsTotal = expvar.NewInt("httpadapter_requests_total")
	startedUnix   = expvar.NewInt("httpadapter_started_unix")
)

func main() {
	startedUnix.Set(time.Now().Unix())

	app, err := gogo.NewApp(gogo.Config{
		BindAddr: "127.0.0.1",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	// HTTPAdapter stages stdlib responses before copying them into gogo.
	// Keep a cap on trusted debug endpoints too; raise this only when
	// your profiles routinely exceed the default.
	gogo.SetMaxHTTPAdapterBodyBytes(16 << 20)

	app.Get("/", func(res *gogo.Response, req *gogo.Request) {
		requestsTotal.Add(1)
		res.Send(200, "text/plain; charset=utf-8", "httpadapter example\n")
	})

	app.Get("/debug/vars", gogo.HTTPAdapter(expvar.Handler()))
	registerPprof(app)

	if !app.Listen(3005) {
		log.Fatal("listen 127.0.0.1:3005 failed")
	}
	log.Println("gogo~ httpadapter listening on http://127.0.0.1:3005")
	app.Run()
}

func registerPprof(app *gogo.App) {
	index := gogo.HTTPAdapter(http.HandlerFunc(pprof.Index))

	app.Get("/debug/pprof/", index)
	app.Get("/debug/pprof/cmdline", gogo.HTTPAdapter(http.HandlerFunc(pprof.Cmdline)))
	app.Get("/debug/pprof/symbol", gogo.HTTPAdapter(http.HandlerFunc(pprof.Symbol)))
	app.PostAsync("/debug/pprof/symbol", 32<<10, func(res *gogo.Response, req *gogo.Request, body []byte) {
		gogo.HTTPAdapterWithBody(http.HandlerFunc(pprof.Symbol), body)(res, req)
	})

	// These handlers can block while data is collected, so run them on
	// gogo's async route path. Their responses are still staged by the
	// adapter; use native gogo APIs for true streaming or large downloads.
	app.GetAsync("/debug/pprof/profile", asyncHTTPAdapter(http.HandlerFunc(pprof.Profile)))
	app.GetAsync("/debug/pprof/trace", asyncHTTPAdapter(http.HandlerFunc(pprof.Trace)))

	// Heap, goroutine, allocs, block, mutex, and threadcreate profiles
	// are served by pprof.Index based on the final path segment.
	app.Get("/debug/pprof/:profile", index)
}

func asyncHTTPAdapter(h http.Handler) gogo.AsyncHandler {
	adapter := gogo.HTTPAdapter(h)
	return func(res *gogo.Response, req *gogo.Request) {
		adapter(res, req)
	}
}
