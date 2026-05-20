// sse demonstrates the simplest SSE pattern in gogo: register an
// async route, install Server-Sent Events via res.SSE, and emit
// events on a timer with a keepalive ping every few seconds.
//
// Run:
//
//	CGO_ENABLED=1 go run -tags gogo ./examples/sse
//
// Then open the browser to http://localhost:3000/ — the embedded
// HTML page subscribes to /events via the standard EventSource
// API and renders each tick.
//
// Or stream from the CLI:
//
//	curl -N http://localhost:3000/events
package main

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	gogo "uwebsockets-go/gogo"
)

const indexHTML = `<!doctype html>
<html><body>
<h1>gogo~ SSE demo</h1>
<ul id="log" style="font-family:monospace"></ul>
<script>
const log = document.getElementById('log');
const ev = new EventSource('/events');
ev.addEventListener('tick', e => {
    const li = document.createElement('li');
    const d = JSON.parse(e.data);
    li.textContent = "tick #" + d.n + " at " + d.at;
    log.prepend(li);
});
ev.onerror = e => console.error('sse err', e);
</script>
</body></html>`

func main() {
	app, err := gogo.NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	app.Get("/", gogo.Reply{
		ContentType: "text/html; charset=utf-8",
		Body:        indexHTML,
	})

	// /events is the SSE feed. Counter is process-global so every
	// connection sees a continuous sequence (the client's
	// Last-Event-ID resumes from the right tick on reconnect).
	var counter atomic.Int64

	app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
		// Resume from the client-supplied Last-Event-ID when
		// present — the canonical SSE resumability pattern.
		resumeFrom := req.Header("last-event-id")

		res.SSE(func(s *gogo.SSEStream) error {
			if resumeFrom != "" {
				_ = s.Comment("resuming after id=" + resumeFrom)
			}

			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			pinger := time.NewTicker(15 * time.Second)
			defer pinger.Stop()

			for {
				select {
				case t := <-ticker.C:
					n := counter.Add(1)
					if err := s.SendEvent(gogo.SSEEvent{
						ID:    fmt.Sprintf("%d", n),
						Event: "tick",
						Data: map[string]any{
							"n":  n,
							"at": t.Format(time.RFC3339),
						},
					}); err != nil {
						return err
					}
				case <-pinger.C:
					if err := s.Ping(); err != nil {
						return err
					}
				}
			}
		})
	})

	if !app.Listen(3000) {
		log.Fatal("listen :3000 failed")
	}
	log.Printf("gogo~ SSE demo listening on http://localhost:3000")
	app.Run()
}
