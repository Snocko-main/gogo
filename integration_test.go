//go:build cgo && gogo

package gogo_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

// freePort grabs an ephemeral port the kernel just handed us, then closes the
// listener so the test server can rebind to it. Tiny race window but fine for
// tests on localhost.
func freePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// startApp boots an app on a free port, runs the loop on a dedicated OS-locked
// goroutine (uWS loop is thread-local), waits until the server accepts a
// connection, and returns a teardown func.
func startApp(t testing.TB, configure func(app *gogo.App)) (port int, teardown func()) {
	t.Helper()

	port = freePort(t)
	ready := make(chan *gogo.App, 1)
	listenErr := make(chan error, 1)
	runDone := make(chan struct{})

	go func() {
		// Loop binding requires every uWS call for this app to happen on the
		// same OS thread that called uwsgo_app_new.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		app, err := gogo.NewApp(gogo.Config{BindAddr: "127.0.0.1"})
		if err != nil {
			listenErr <- fmt.Errorf("NewApp: %w", err)
			close(runDone)
			return
		}
		configure(app)
		if !app.Listen(port) {
			listenErr <- fmt.Errorf("Listen :%d failed", port)
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
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("app setup timed out")
	}

	// Spin-wait for the loop to accept connections.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 50*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	teardown = func() {
		app.Shutdown()
		select {
		case <-runDone:
		case <-time.After(10 * time.Second):
			t.Errorf("app.Run did not exit after Shutdown")
		}
	}
	return port, teardown
}

func TestRunMultiCoreDistributesAcceptedSockets(t *testing.T) {
	const workers = 4

	oldProcs := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(oldProcs)

	port := freePort(t)
	var next atomic.Int32
	var counts [workers]atomic.Int64

	handle, err := gogo.RunMultiCore(workers, port, func(app *gogo.App) {
		id := int(next.Add(1)) - 1
		app.Get("/id", func(res *gogo.Response, req *gogo.Request) {
			counts[id].Add(1)
			res.Send(200, "text/plain", strconv.Itoa(id))
		})
	})
	if err != nil {
		t.Fatalf("RunMultiCore: %v", err)
	}
	defer func() {
		handle.Shutdown()
		handle.Wait()
	}()

	client := &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   2 * time.Second,
	}

	seen := make(map[int]bool)
	for i := 0; i < workers*8; i++ {
		resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/id", port))
		if err != nil {
			t.Fatalf("GET /id: %v", err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d body=%q, want 200", resp.StatusCode, body)
		}
		id, err := strconv.Atoi(strings.TrimSpace(string(body)))
		if err != nil {
			t.Fatalf("worker id body %q: %v", body, err)
		}
		seen[id] = true
	}

	if len(seen) != workers {
		var got [workers]int64
		for i := range counts {
			got[i] = counts[i].Load()
		}
		t.Fatalf("requests reached %d/%d workers; counts=%v", len(seen), workers, got)
	}
}

func TestRunMultiCoreSetupPanicReturnsError(t *testing.T) {
	port := freePort(t)
	handle, err := gogo.RunMultiCore(2, port, func(app *gogo.App) {
		panic("setup failed")
	})
	if err == nil {
		if handle != nil {
			handle.Shutdown()
			handle.Wait()
		}
		t.Fatal("RunMultiCore returned nil error after setup panic")
	}
	if handle != nil {
		t.Fatal("RunMultiCore returned a handle after setup panic")
	}
	if !strings.Contains(err.Error(), "RunMultiCore setup panic") || !strings.Contains(err.Error(), "setup failed") {
		t.Fatalf("RunMultiCore error = %q, want setup panic context", err)
	}
}

func TestRunMultiCoreAppPublishReachesAllLoops(t *testing.T) {
	const workers = 4
	const clientsN = workers * 2

	oldProcs := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(oldProcs)

	port := freePort(t)
	appCh := make(chan *gogo.App, workers)
	handle, err := gogo.RunMultiCore(workers, port, func(app *gogo.App) {
		appCh <- app
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("global")
			},
		})
	})
	if err != nil {
		t.Fatalf("RunMultiCore: %v", err)
	}
	defer func() {
		handle.Shutdown()
		handle.Wait()
	}()

	apps := make([]*gogo.App, 0, workers)
	for i := 0; i < workers; i++ {
		apps = append(apps, <-appCh)
	}

	clients := make([]*wsClient, 0, clientsN)
	for i := 0; i < clientsN; i++ {
		c, err := dialWebSocket(port, "/ws")
		if err != nil {
			t.Fatalf("dial client %d: %v", i, err)
		}
		defer c.Close()
		clients = append(clients, c)
	}
	time.Sleep(100 * time.Millisecond)

	apps[0].Publish("global", []byte("broadcast"), gogo.Text)
	for i, c := range clients {
		got, err := c.ReadText(2 * time.Second)
		if err != nil {
			t.Fatalf("client %d read: %v", i, err)
		}
		if got != "broadcast" {
			t.Fatalf("client %d got %q, want broadcast", i, got)
		}
		if err := c.expectNoMessage(300 * time.Millisecond); err != nil {
			t.Fatalf("client %d got duplicate after Publish: %v", i, err)
		}
	}
}

func TestRunMultiCoreAppPublishBatchReachesAllLoopsOnce(t *testing.T) {
	const workers = 4
	const clientsN = workers * 2

	oldProcs := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(oldProcs)

	port := freePort(t)
	appCh := make(chan *gogo.App, workers)
	handle, err := gogo.RunMultiCore(workers, port, func(app *gogo.App) {
		appCh <- app
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("global")
			},
		})
	})
	if err != nil {
		t.Fatalf("RunMultiCore: %v", err)
	}
	defer func() {
		handle.Shutdown()
		handle.Wait()
	}()

	apps := make([]*gogo.App, 0, workers)
	for i := 0; i < workers; i++ {
		apps = append(apps, <-appCh)
	}

	clients := make([]*wsClient, 0, clientsN)
	for i := 0; i < clientsN; i++ {
		c, err := dialWebSocket(port, "/ws")
		if err != nil {
			t.Fatalf("dial client %d: %v", i, err)
		}
		defer c.Close()
		clients = append(clients, c)
	}
	time.Sleep(100 * time.Millisecond)

	apps[0].PublishBatch([]gogo.PublishMessage{{
		Topic:   "global",
		Message: []byte("batch broadcast"),
		OpCode:  gogo.Text,
	}})
	for i, c := range clients {
		got, err := c.ReadText(2 * time.Second)
		if err != nil {
			t.Fatalf("client %d read: %v", i, err)
		}
		if got != "batch broadcast" {
			t.Fatalf("client %d got %q, want batch broadcast", i, got)
		}
		if err := c.expectNoMessage(300 * time.Millisecond); err != nil {
			t.Fatalf("client %d got duplicate after PublishBatch: %v", i, err)
		}
	}
}

func TestRunMultiCoreWebSocketPublishReachesPeerLoops(t *testing.T) {
	const workers = 4
	const clientsN = workers * 2

	oldProcs := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(oldProcs)

	port := freePort(t)
	handle, err := gogo.RunMultiCore(workers, port, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("room")
			},
			Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
				ws.Publish("room", msg, op)
			},
		})
	})
	if err != nil {
		t.Fatalf("RunMultiCore: %v", err)
	}
	defer func() {
		handle.Shutdown()
		handle.Wait()
	}()

	clients := make([]*wsClient, 0, clientsN)
	for i := 0; i < clientsN; i++ {
		c, err := dialWebSocket(port, "/ws")
		if err != nil {
			t.Fatalf("dial client %d: %v", i, err)
		}
		defer c.Close()
		clients = append(clients, c)
	}
	time.Sleep(100 * time.Millisecond)

	if err := clients[0].SendText("hello peers"); err != nil {
		t.Fatalf("send: %v", err)
	}
	for i := 1; i < len(clients); i++ {
		got, err := clients[i].ReadText(2 * time.Second)
		if err != nil {
			t.Fatalf("client %d read: %v", i, err)
		}
		if got != "hello peers" {
			t.Fatalf("client %d got %q, want hello peers", i, got)
		}
		if err := clients[i].expectNoMessage(300 * time.Millisecond); err != nil {
			t.Fatalf("client %d got duplicate after WebSocket.Publish: %v", i, err)
		}
	}
	if err := clients[0].expectNoMessage(300 * time.Millisecond); err != nil {
		t.Fatalf("publisher received its own message: %v", err)
	}
}

func TestRunMultiCoreWSHubPublishDoesNotDuplicate(t *testing.T) {
	const workers = 2

	oldProcs := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(oldProcs)

	port := freePort(t)
	hub := gogo.NewWSHub(gogo.WithWSHubNodeID("test-node"))
	defer hub.Close()

	handle, err := gogo.RunMultiCore(workers, port, func(app *gogo.App) {
		hub.WebSocket(app, "/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("room")
			},
		})
	})
	if err != nil {
		t.Fatalf("RunMultiCore: %v", err)
	}
	defer func() {
		handle.Shutdown()
		handle.Wait()
	}()

	client, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	time.Sleep(100 * time.Millisecond)

	if err := hub.Publish("room", []byte("hello"), gogo.Text); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got, err := client.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != "hello" {
		t.Fatalf("got %q, want hello", got)
	}
	if err := client.expectNoMessage(300 * time.Millisecond); err != nil {
		t.Fatalf("duplicate delivery after Publish: %v", err)
	}
}

func TestRunMultiCoreWSHubPublishBatchDoesNotDuplicate(t *testing.T) {
	const workers = 2

	oldProcs := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(oldProcs)

	port := freePort(t)
	hub := gogo.NewWSHub(gogo.WithWSHubNodeID("test-node"))
	defer hub.Close()

	handle, err := gogo.RunMultiCore(workers, port, func(app *gogo.App) {
		hub.WebSocket(app, "/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("room")
			},
		})
	})
	if err != nil {
		t.Fatalf("RunMultiCore: %v", err)
	}
	defer func() {
		handle.Shutdown()
		handle.Wait()
	}()

	client, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	time.Sleep(100 * time.Millisecond)

	if err := hub.PublishBatch([]gogo.PublishMessage{{
		Topic:   "room",
		Message: []byte("batch"),
		OpCode:  gogo.Text,
	}}); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	got, err := client.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != "batch" {
		t.Fatalf("got %q, want batch", got)
	}
	if err := client.expectNoMessage(300 * time.Millisecond); err != nil {
		t.Fatalf("duplicate delivery after PublishBatch: %v", err)
	}
}

func TestRunMultiCoreWSHubPublishFromSkipsSender(t *testing.T) {
	const workers = 2
	const clientsN = workers * 2

	oldProcs := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(oldProcs)

	port := freePort(t)
	hub := gogo.NewWSHub(gogo.WithWSHubNodeID("test-node"))
	defer hub.Close()

	handle, err := gogo.RunMultiCore(workers, port, func(app *gogo.App) {
		hub.WebSocket(app, "/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("room")
			},
			Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
				if err := hub.PublishFrom(ws, "room", msg, op); err != nil {
					t.Errorf("PublishFrom: %v", err)
				}
			},
		})
	})
	if err != nil {
		t.Fatalf("RunMultiCore: %v", err)
	}
	defer func() {
		handle.Shutdown()
		handle.Wait()
	}()

	clients := make([]*wsClient, 0, clientsN)
	for i := 0; i < clientsN; i++ {
		c, err := dialWebSocket(port, "/ws")
		if err != nil {
			t.Fatalf("dial client %d: %v", i, err)
		}
		defer c.Close()
		clients = append(clients, c)
	}
	time.Sleep(100 * time.Millisecond)

	if err := clients[0].SendText("from"); err != nil {
		t.Fatalf("send: %v", err)
	}
	for i := 1; i < len(clients); i++ {
		got, err := clients[i].ReadText(2 * time.Second)
		if err != nil {
			t.Fatalf("client %d read: %v", i, err)
		}
		if got != "from" {
			t.Fatalf("client %d got %q, want from", i, got)
		}
		if err := clients[i].expectNoMessage(300 * time.Millisecond); err != nil {
			t.Fatalf("client %d got duplicate: %v", i, err)
		}
	}
	if err := clients[0].expectNoMessage(300 * time.Millisecond); err != nil {
		t.Fatalf("publisher received its own hub message: %v", err)
	}
}

// noKeepaliveClient avoids HTTP/1.1 keep-alive so the server has no open
// sockets keeping the loop alive when Shutdown is called.
var noKeepaliveClient = &http.Client{
	Transport: &http.Transport{DisableKeepAlives: true},
	Timeout:   5 * time.Second,
}

func httpGet(t *testing.T, port int, path string) (status int, body string) {
	t.Helper()
	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d%s", port, path))
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(b)
}

func TestStaticReply(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/health", gogo.Reply{
			Status:      200,
			ContentType: "application/json",
			Body:        `{"ok":true}`,
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/health")
	if status != 200 || body != `{"ok":true}` {
		t.Fatalf("got %d %q", status, body)
	}
}

func TestAppStaticReplyWithMiddleware(t *testing.T) {
	var hits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				hits.Add(1)
				res.Header("X-Middleware", "hit")
				next(res, req)
			}
		})
		app.Get("/health", gogo.Reply{
			Status:      201,
			ContentType: "text/plain",
			Body:        "ok",
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
	if err != nil {
		t.Fatalf("/health: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 201 || string(body) != "ok" {
		t.Fatalf("/health: got %d %q", resp.StatusCode, body)
	}
	if resp.Header.Get("X-Middleware") != "hit" {
		t.Fatalf("static reply bypassed middleware header")
	}
	if hits.Load() != 1 {
		t.Fatalf("middleware hits = %d, want 1", hits.Load())
	}
}

func TestSyncHandler(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/plain", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "hello\n")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/plain")
	if status != 200 || body != "hello\n" {
		t.Fatalf("got %d %q", status, body)
	}
}

func TestRouteParameter(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/hello/:name", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "hi "+req.Parameter(0))
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/hello/claude")
	if status != 200 || body != "hi claude" {
		t.Fatalf("got %d %q", status, body)
	}
}

func TestAsyncHandler(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/sleep", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(5 * time.Millisecond)
			res.Send(200, "text/plain", "slept")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/sleep")
	if status != 200 || body != "slept" {
		t.Fatalf("got %d %q", status, body)
	}
}

func TestSharedDispatch(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/shared", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "application/json", `{"path":"shared"}`)
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/shared")
	if status != 200 || body != `{"path":"shared"}` {
		t.Fatalf("got %d %q", status, body)
	}
}

// TestSharedWorkersDrainOnAppClose verifies the shared-dispatch
// worker pool exits cleanly once the last App that referenced it is
// closed. Without the drain, every test that uses a shared route
// would leak workerCount goroutines into the next test.
func TestSharedWorkersDrainOnAppClose(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/p", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})

	status, body := httpGet(t, port, "/p")
	if status != 200 || body != "ok" {
		t.Fatalf("got %d %q", status, body)
	}

	// Teardown closes the app; the deferred WaitForSharedWorkers
	// must return true within the timeout — slow drain would
	// indicate a worker stuck against a freed ring.
	teardown()
	if !gogo.WaitForSharedWorkers(2 * time.Second) {
		t.Fatal("shared workers did not drain within 2s after App.Close")
	}
}

func TestSharedWorkersDrainWithSyncOnlyAppOpen(t *testing.T) {
	syncOnly, err := gogo.NewApp(gogo.Config{BindAddr: "127.0.0.1"})
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	defer syncOnly.Close()

	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/p", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})

	status, body := httpGet(t, port, "/p")
	if status != 200 || body != "ok" {
		t.Fatalf("got %d %q", status, body)
	}

	teardown()
	if !gogo.WaitForSharedWorkers(2 * time.Second) {
		t.Fatal("shared workers stayed alive while only a sync-only App remained open")
	}
}

func TestWaitForSharedWorkersTimeoutWhileAppOpen(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/p", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})

	status, body := httpGet(t, port, "/p")
	if status != 200 || body != "ok" {
		t.Fatalf("got %d %q", status, body)
	}
	if gogo.WaitForSharedWorkers(20 * time.Millisecond) {
		t.Fatal("workers reported drained while the shared App was still open")
	}

	teardown()
	if !gogo.WaitForSharedWorkers(2 * time.Second) {
		t.Fatal("shared workers did not drain after App.Close")
	}
}

// TestRequestContextSyncHandler: a sync handler that completes
// normally should still observe its req.Context() cancel on pool
// release, so background goroutines kicked off with the context
// don't leak past the request.
func TestRequestContextSyncHandler(t *testing.T) {
	canceled := make(chan struct{}, 1)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/ctx", func(res *gogo.Response, req *gogo.Request) {
			ctx := req.Context()
			go func() {
				<-ctx.Done()
				canceled <- struct{}{}
			}()
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/ctx")
	if status != 200 || body != "ok" {
		t.Fatalf("got %d %q", status, body)
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("req.Context() did not cancel after handler completion")
	}
}

// TestRequestContextCancelsOnClientAbort: when the client closes
// the connection before the async handler responds, the context
// returned by req.Context() should fire with a non-nil Err.
func TestRequestContextCancelsOnClientAbort(t *testing.T) {
	ctxCanceled := make(chan error, 1)
	handlerEntered := make(chan struct{}, 1)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/slow", func(res *gogo.Response, req *gogo.Request) {
			ctx := req.Context()
			handlerEntered <- struct{}{}
			select {
			case <-ctx.Done():
				ctxCanceled <- ctx.Err()
			case <-time.After(3 * time.Second):
				ctxCanceled <- nil
			}
			// Best-effort send — connection is gone but uWS swallows it.
			res.Send(200, "text/plain", "late")
		})
	})
	defer teardown()

	// Dial raw so we can close mid-flight before the server replies.
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_, err = conn.Write([]byte("GET /slow HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"))
	if err != nil {
		conn.Close()
		t.Fatalf("write: %v", err)
	}

	select {
	case <-handlerEntered:
	case <-time.After(2 * time.Second):
		conn.Close()
		t.Fatal("handler did not run")
	}

	conn.Close()

	select {
	case err := <-ctxCanceled:
		if err == nil {
			t.Fatal("req.Context() did not cancel within 3s after client abort")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("ctxCanceled receive timed out")
	}
}

func TestRequestContextAsyncDoesNotCrash(t *testing.T) {
	const iterations = 50

	ctxCanceled := make(chan error, iterations)
	handlerEntered := make(chan struct{}, iterations)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/abort", func(res *gogo.Response, req *gogo.Request) {
			ctx := req.Context()
			handlerEntered <- struct{}{}
			select {
			case <-ctx.Done():
				ctxCanceled <- ctx.Err()
			case <-time.After(2 * time.Second):
				ctxCanceled <- nil
			}
		})
	})
	defer teardown()

	for i := 0; i < iterations; i++ {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		_, err = conn.Write([]byte("GET /abort HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"))
		if err != nil {
			conn.Close()
			t.Fatalf("write %d: %v", i, err)
		}

		select {
		case <-handlerEntered:
		case <-time.After(2 * time.Second):
			conn.Close()
			t.Fatalf("handler did not run for request %d", i)
		}

		conn.Close()

		select {
		case err := <-ctxCanceled:
			if err == nil {
				t.Fatalf("req.Context() did not cancel after client abort for request %d", i)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("ctxCanceled receive timed out for request %d", i)
		}
	}
}

func TestPanicRecoveryInSharedHandler(t *testing.T) {
	var panicked atomic.Int32
	gogo.SetPanicHandler(func(recovered any) {
		panicked.Add(1)
	})
	defer gogo.SetPanicHandler(nil)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/boom", func(res *gogo.Response, req *gogo.Request) {
			panic("kaboom")
		})
		app.GetAsync("/ok", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "still alive")
		})
	})
	defer teardown()

	// Panic route should not crash the worker — it returns 500 best-effort.
	status, _ := httpGet(t, port, "/boom")
	if status != 500 {
		t.Fatalf("expected 500 after panic, got %d", status)
	}
	if panicked.Load() != 1 {
		t.Fatalf("panic handler not called, got %d", panicked.Load())
	}

	// And the worker is still alive for subsequent requests.
	status, body := httpGet(t, port, "/ok")
	if status != 200 || body != "still alive" {
		t.Fatalf("worker died after panic; got %d %q", status, body)
	}
}

func TestPanicRecoveryInSyncHandler(t *testing.T) {
	var panicked atomic.Int32
	gogo.SetPanicHandler(func(recovered any) {
		panicked.Add(1)
	})
	defer gogo.SetPanicHandler(nil)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/boom", func(res *gogo.Response, req *gogo.Request) {
			panic("sync kaboom")
		})
		app.Get("/ok", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "still alive")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/boom")
	if status != 500 || !strings.Contains(body, "Internal Server Error") {
		t.Fatalf("expected 500 after sync panic, got %d %q", status, body)
	}
	if panicked.Load() != 1 {
		t.Fatalf("panic handler not called, got %d", panicked.Load())
	}

	status, body = httpGet(t, port, "/ok")
	if status != 200 || body != "still alive" {
		t.Fatalf("server died after sync panic; got %d %q", status, body)
	}
}

func TestPanicRecoveryInAsyncFallbackHandler(t *testing.T) {
	var panicked atomic.Int32
	gogo.SetPanicHandler(func(recovered any) {
		panicked.Add(1)
	})
	defer gogo.SetPanicHandler(nil)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use("/api/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				next(res, req)
			}
		})
		app.GetAsync("/api/boom", func(res *gogo.Response, req *gogo.Request) {
			panic("async fallback kaboom")
		})
		app.GetAsync("/api/ok", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "still alive")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/api/boom")
	if status != 500 || !strings.Contains(body, "Internal Server Error") {
		t.Fatalf("expected 500 after async fallback panic, got %d %q", status, body)
	}
	if panicked.Load() != 1 {
		t.Fatalf("panic handler not called, got %d", panicked.Load())
	}

	status, body = httpGet(t, port, "/api/ok")
	if status != 200 || body != "still alive" {
		t.Fatalf("server died after async fallback panic; got %d %q", status, body)
	}
}

func TestConcurrentLoad(t *testing.T) {
	// Stress the shared-dispatch + worker pool to flush out races.
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/c", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "x")
		})
	})
	defer teardown()

	const clients = 16
	const perClient = 100
	var wg sync.WaitGroup
	var errs atomic.Int32
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 2 * time.Second}
			for j := 0; j < perClient; j++ {
				resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/c", port))
				if err != nil {
					errs.Add(1)
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					errs.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if errs.Load() != 0 {
		t.Fatalf("got %d errors under load", errs.Load())
	}
}

func TestPatternValidation(t *testing.T) {
	app, err := gogo.NewApp()
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	defer app.Close()

	cases := []struct {
		name    string
		pattern string
	}{
		{"empty", ""},
		{"no-leading-slash", "health"},
		{"newline", "/foo\nbar"},
		{"null-byte", "/foo\x00bar"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for pattern %q", tc.pattern)
				}
			}()
			app.Get(tc.pattern, "x")
		})
	}
}

func httpPost(t *testing.T, port int, path, contentType string, body []byte) (status int, respBody string) {
	t.Helper()
	resp, err := noKeepaliveClient.Post(
		fmt.Sprintf("http://127.0.0.1:%d%s", port, path),
		contentType,
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(b)
}

func httpBody(t *testing.T, port int, method, path, contentType string, body []byte) (status int, respBody string, header http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d%s", port, path), bytes.NewReader(body))
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(b), resp.Header.Clone()
}

func TestPostAsyncBody(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/echo", 64*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "application/octet-stream", string(body))
		})
	})
	defer teardown()

	payload := []byte(`{"hello":"world","n":42}`)
	status, body := httpPost(t, port, "/echo", "application/json", payload)
	if status != 200 {
		t.Fatalf("got %d, want 200", status)
	}
	if body != string(payload) {
		t.Fatalf("got %q, want %q", body, string(payload))
	}
}

func TestPostAsyncLargeBody(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/echo", 1024*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", fmt.Sprintf("len=%d", len(body)))
		})
	})
	defer teardown()

	// 256 KB — larger than a single TCP segment, exercises multi-chunk onData.
	payload := bytes.Repeat([]byte("x"), 256*1024)
	status, body := httpPost(t, port, "/echo", "application/octet-stream", payload)
	if status != 200 {
		t.Fatalf("got %d, want 200", status)
	}
	if body != fmt.Sprintf("len=%d", len(payload)) {
		t.Fatalf("got %q, want len=%d", body, len(payload))
	}
}

func TestPostAsyncTooLarge(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/upload", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			t.Errorf("handler should not be called for oversize body")
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	payload := bytes.Repeat([]byte("a"), 2048)
	status, body := httpPost(t, port, "/upload", "text/plain", payload)
	if status != 413 {
		t.Fatalf("got %d, want 413; body=%q", status, body)
	}
}

func TestBodyAsyncMethodHelpers(t *testing.T) {
	var tooLargeHandlerCalled atomic.Bool

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(gogo.AsyncMiddleware(func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				req.SetLocal("mw-body", string(req.Body()))
				next(res, req)
			}
		}))

		handler := func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", fmt.Sprintf("%s|%s|%s|%v",
				req.Method(), req.Param("id"), string(body), req.Local("mw-body")))
		}
		app.PutAsync("/put/:id", 64, handler)
		app.PatchAsync("/patch/:id", 64, handler)
		app.DeleteAsync("/delete/:id", 64, handler)
		app.PutAsync("/too-large", 8, func(res *gogo.Response, req *gogo.Request, body []byte) {
			tooLargeHandlerCalled.Store(true)
			res.Send(200, "text/plain", "unexpected")
		})
	})
	defer teardown()

	for _, tc := range []struct {
		method string
		path   string
		body   string
		want   string
	}{
		{"PUT", "/put/42", "replace", "put|42|replace|replace"},
		{"PATCH", "/patch/42", "patch", "patch|42|patch|patch"},
		{"DELETE", "/delete/42", "delete", "delete|42|delete|delete"},
	} {
		status, body, _ := httpBody(t, port, tc.method, tc.path, "text/plain", []byte(tc.body))
		if status != 200 || body != tc.want {
			t.Fatalf("%s %s: got %d %q, want 200 %q", tc.method, tc.path, status, body, tc.want)
		}
	}

	status, body, _ := httpBody(t, port, "PUT", "/too-large", "text/plain", []byte("0123456789"))
	if status != 413 {
		t.Fatalf("PUT /too-large: got %d %q, want 413", status, body)
	}
	if tooLargeHandlerCalled.Load() {
		t.Fatal("PutAsync handler ran for oversized body")
	}
}

func TestPostBodyLowLevel(t *testing.T) {
	// Body() primitive: collect on loop thread, respond inline.
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Post("/sum", func(res *gogo.Response, req *gogo.Request) {
			res.Body(16*1024, func(body []byte, err error) {
				if err != nil {
					res.Send(413, "text/plain", err.Error())
					return
				}
				res.Send(200, "text/plain", fmt.Sprintf("sum=%d", len(body)))
			})
		})
	})
	defer teardown()

	status, body := httpPost(t, port, "/sum", "text/plain", []byte("hello"))
	if status != 200 || body != "sum=5" {
		t.Fatalf("got %d %q", status, body)
	}
}

func TestQueryString(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/q", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", req.Query())
		})
		app.Get("/p", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain",
				"name="+req.QueryParam("name")+
					";age="+req.QueryParam("age")+
					";missing="+req.QueryParam("missing"))
		})
	})
	defer teardown()

	// Raw query string round-trip.
	status, body := httpGet(t, port, "/q?a=1&b=hello")
	if status != 200 || body != "a=1&b=hello" {
		t.Fatalf("raw query: got %d %q", status, body)
	}

	// Empty when no query.
	status, body = httpGet(t, port, "/q")
	if status != 200 || body != "" {
		t.Fatalf("no query: got %d %q", status, body)
	}

	// Named parameters + missing key returns "".
	status, body = httpGet(t, port, "/p?name=claude&age=4")
	if status != 200 || body != "name=claude;age=4;missing=" {
		t.Fatalf("query params: got %d %q", status, body)
	}

	// Empty key short-circuits.
	status, body = httpGet(t, port, "/p")
	if status != 200 || body != "name=;age=;missing=" {
		t.Fatalf("no params: got %d %q", status, body)
	}
}

func TestMiddlewareChainOrder(t *testing.T) {
	// Verify outermost-first ordering and that the chain reaches the handler.
	var trace []string
	var traceMu sync.Mutex
	record := func(s string) {
		traceMu.Lock()
		trace = append(trace, s)
		traceMu.Unlock()
	}

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("a-before")
				next(res, req)
				record("a-after")
			}
		})
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("b-before")
				next(res, req)
				record("b-after")
			}
		})
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			record("handler")
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/x")
	if status != 200 || body != "ok" {
		t.Fatalf("got %d %q", status, body)
	}
	traceMu.Lock()
	got := strings.Join(trace, ",")
	traceMu.Unlock()
	want := "a-before,b-before,handler,b-after,a-after"
	if got != want {
		t.Fatalf("trace: got %q want %q", got, want)
	}
}

func TestMiddlewareShortCircuit(t *testing.T) {
	var handlerCalled atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				if req.Header("authorization") == "" {
					res.Send(401, "text/plain", "unauthorized")
					return
				}
				next(res, req)
			}
		})
		app.Get("/secret", func(res *gogo.Response, req *gogo.Request) {
			handlerCalled.Add(1)
			res.Send(200, "text/plain", "secret")
		})
	})
	defer teardown()

	// No header → 401, handler not called.
	status, body := httpGet(t, port, "/secret")
	if status != 401 || body != "unauthorized" {
		t.Fatalf("denied: got %d %q", status, body)
	}
	if handlerCalled.Load() != 0 {
		t.Fatalf("handler called on short-circuit")
	}

	// With header → passes through.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/secret", port), nil)
	req.Header.Set("Authorization", "Bearer x")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("authed GET: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "secret" {
		t.Fatalf("authed: got %d %q", resp.StatusCode, string(b))
	}
	if handlerCalled.Load() != 1 {
		t.Fatalf("handler call count = %d, want 1", handlerCalled.Load())
	}
}

func TestMiddlewareAppliesOnlyToLaterRoutes(t *testing.T) {
	// Routes registered before Use are not affected.
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/free", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "free")
		})
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				res.Send(403, "text/plain", "blocked")
			}
		})
		app.Get("/locked", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "should not see this")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/free")
	if status != 200 || body != "free" {
		t.Fatalf("/free: got %d %q", status, body)
	}
	status, body = httpGet(t, port, "/locked")
	if status != 403 || body != "blocked" {
		t.Fatalf("/locked: got %d %q", status, body)
	}
}

func TestMiddlewareOnPostAsync(t *testing.T) {
	// Middleware runs before body collection on PostAsync routes too.
	var seenAuth atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				if req.Header("x-auth") != "yes" {
					res.Send(401, "text/plain", "no")
					return
				}
				seenAuth.Add(1)
				next(res, req)
			}
		})
		app.PostAsync("/upload", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", fmt.Sprintf("got %d", len(body)))
		})
	})
	defer teardown()

	// Without auth → 401, handler & body collection skipped.
	status, _ := httpPost(t, port, "/upload", "text/plain", []byte("hello"))
	if status != 401 {
		t.Fatalf("no-auth: got %d", status)
	}

	// With auth → body collected, handler runs.
	req, _ := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d/upload", port), bytes.NewReader([]byte("hello")))
	req.Header.Set("X-Auth", "yes")
	req.Header.Set("Content-Type", "text/plain")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("authed POST: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "got 5" {
		t.Fatalf("authed: got %d %q", resp.StatusCode, string(b))
	}
	if seenAuth.Load() != 1 {
		t.Fatalf("middleware passed-through count = %d, want 1", seenAuth.Load())
	}
}

func TestMiddlewareOnGetAsync(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				if req.QueryParam("token") == "" {
					res.Send(401, "text/plain", "no token")
					return
				}
				next(res, req)
			}
		})
		app.GetAsync("/work", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(2 * time.Millisecond)
			res.Send(200, "text/plain", "done")
		})
	})
	defer teardown()

	status, _ := httpGet(t, port, "/work")
	if status != 401 {
		t.Fatalf("missing token: got %d", status)
	}
	status, body := httpGet(t, port, "/work?token=abc")
	if status != 200 || body != "done" {
		t.Fatalf("with token: got %d %q", status, body)
	}
}

// TestMiddlewarePathScoped verifies Use("/api/*", mw) applies only to routes
// whose pattern starts with /api/ (or is exactly /api).
func TestMiddlewarePathScoped(t *testing.T) {
	var apiHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use("/api/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				apiHits.Add(1)
				if req.Header("x-api-key") != "secret" {
					res.Send(401, "text/plain", "no key")
					return
				}
				next(res, req)
			}
		})
		app.Get("/api/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "users")
		})
		app.Get("/public", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "public")
		})
	})
	defer teardown()

	// /public bypasses the path-scoped middleware entirely.
	status, body := httpGet(t, port, "/public")
	if status != 200 || body != "public" {
		t.Fatalf("/public: got %d %q", status, body)
	}
	if apiHits.Load() != 0 {
		t.Fatalf("api middleware ran for /public: hits=%d", apiHits.Load())
	}

	// /api/users without key → 401 from the middleware.
	status, body = httpGet(t, port, "/api/users")
	if status != 401 || body != "no key" {
		t.Fatalf("/api/users unauth: got %d %q", status, body)
	}

	// /api/users with key → 200 from the handler.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/users", port), nil)
	req.Header.Set("X-API-Key", "secret")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("/api/users authed: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "users" {
		t.Fatalf("/api/users authed: got %d %q", resp.StatusCode, string(b))
	}
	if apiHits.Load() != 2 {
		t.Fatalf("api middleware hit count = %d, want 2", apiHits.Load())
	}
}

// TestMiddlewarePathScopedExactPrefix checks that Use("/api", mw) matches
// "/api" exactly as well as routes under "/api/", but not "/apiv2".
func TestMiddlewarePathScopedExactPrefix(t *testing.T) {
	var hits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				hits.Add(1)
				next(res, req)
			}
		})
		app.Get("/api", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "root")
		})
		app.Get("/api/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "users")
		})
		app.Get("/apiv2/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "v2")
		})
	})
	defer teardown()

	for _, p := range []string{"/api", "/api/users"} {
		if status, _ := httpGet(t, port, p); status != 200 {
			t.Fatalf("%s: got %d", p, status)
		}
	}
	if hits.Load() != 2 {
		t.Fatalf("/api + /api/users should each hit mw: got %d, want 2", hits.Load())
	}

	// /apiv2/users shares the literal prefix "api" but not the segment;
	// path-scoped middleware must not run for it.
	if status, _ := httpGet(t, port, "/apiv2/users"); status != 200 {
		t.Fatalf("/apiv2/users: got %d", status)
	}
	if hits.Load() != 2 {
		t.Fatalf("/apiv2/users wrongly ran mw: hits=%d", hits.Load())
	}
}

// TestMiddlewareGlobalAndScopedTogether verifies global Use and path-scoped
// Use compose correctly when both are present.
func TestMiddlewareGlobalAndScopedTogether(t *testing.T) {
	var trace []string
	var traceMu sync.Mutex
	record := func(s string) {
		traceMu.Lock()
		trace = append(trace, s)
		traceMu.Unlock()
	}

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("global")
				next(res, req)
			}
		})
		app.Use("/api/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("api")
				next(res, req)
			}
		})
		app.Get("/api/x", func(res *gogo.Response, req *gogo.Request) {
			record("handler-api")
			res.Send(200, "text/plain", "api")
		})
		app.Get("/other", func(res *gogo.Response, req *gogo.Request) {
			record("handler-other")
			res.Send(200, "text/plain", "other")
		})
	})
	defer teardown()

	httpGet(t, port, "/api/x")
	traceMu.Lock()
	got := strings.Join(trace, ",")
	trace = nil
	traceMu.Unlock()
	if got != "global,api,handler-api" {
		t.Fatalf("/api/x trace: got %q", got)
	}

	httpGet(t, port, "/other")
	traceMu.Lock()
	got = strings.Join(trace, ",")
	traceMu.Unlock()
	if got != "global,handler-other" {
		t.Fatalf("/other trace: got %q", got)
	}
}

// TestMiddlewarePathScopedGetAsyncFastPath confirms GetAsync still uses the
// zero-cgo shared-memory dispatch when the only registered middleware is
// scoped to a path that doesn't include the async route.
func TestMiddlewarePathScopedGetAsyncFastPath(t *testing.T) {
	var apiHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use("/api/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				apiHits.Add(1)
				next(res, req)
			}
		})
		// Async route lives OUTSIDE /api so the scoped middleware must not
		// pull it onto the sync fallback path.
		app.GetAsync("/work", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(2 * time.Millisecond)
			res.Send(200, "text/plain", "done")
		})
		// And one /api route to make sure scoped mw still runs where it should.
		app.GetAsync("/api/work", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(2 * time.Millisecond)
			res.Send(200, "text/plain", "api-done")
		})
	})
	defer teardown()

	if status, body := httpGet(t, port, "/work"); status != 200 || body != "done" {
		t.Fatalf("/work: got %d %q", status, body)
	}
	if apiHits.Load() != 0 {
		t.Fatalf("scoped mw ran for /work: hits=%d", apiHits.Load())
	}

	if status, body := httpGet(t, port, "/api/work"); status != 200 || body != "api-done" {
		t.Fatalf("/api/work: got %d %q", status, body)
	}
	if apiHits.Load() != 1 {
		t.Fatalf("scoped mw hits for /api/work = %d, want 1", apiHits.Load())
	}
}

// TestAsyncMiddlewareLoadsUser exercises the canonical async middleware
// flow: a blocking "lookup" runs on the goroutine, sets a Local, and the
// handler reads it back. Shared dispatch path (no sync middleware).
func TestAsyncMiddlewareLoadsUser(t *testing.T) {
	type user struct {
		ID   int
		Name string
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync("/api/*", func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				token := req.Header("authorization")
				if token == "" {
					res.Send(401, "text/plain", "no token")
					return
				}
				// Simulate a blocking DB lookup.
				time.Sleep(1 * time.Millisecond)
				req.SetLocal("user", &user{ID: 42, Name: "alice"})
				next(res, req)
			}
		})
		app.GetAsync("/api/me", func(res *gogo.Response, req *gogo.Request) {
			u := req.Local("user").(*user)
			res.Send(200, "text/plain", fmt.Sprintf("hi %s (%d)", u.Name, u.ID))
		})
	})
	defer teardown()

	// Missing token short-circuits.
	if status, body := httpGet(t, port, "/api/me"); status != 401 || body != "no token" {
		t.Fatalf("no-token: got %d %q", status, body)
	}

	// With token, async mw loads the user and the handler reads it back.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/me", port), nil)
	req.Header.Set("Authorization", "Bearer x")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("authed: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "hi alice (42)" {
		t.Fatalf("authed: got %d %q", resp.StatusCode, string(b))
	}
}

// TestAsyncMiddlewareLocalsResetBetweenRequests confirms request-scoped
// state from one request never leaks into another via the request pool.
func TestAsyncMiddlewareLocalsResetBetweenRequests(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync("/api/*", func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				if req.QueryParam("set") == "1" {
					req.SetLocal("flag", "set-by-mw")
				}
				next(res, req)
			}
		})
		app.GetAsync("/api/x", func(res *gogo.Response, req *gogo.Request) {
			v := req.Local("flag")
			if v == nil {
				res.Send(200, "text/plain", "none")
				return
			}
			res.Send(200, "text/plain", v.(string))
		})
	})
	defer teardown()

	// Drive a sequence of alternating requests to force pool reuse: any
	// "set" leaking into the next "unset" request would surface immediately.
	for i := 0; i < 20; i++ {
		_, body := httpGet(t, port, "/api/x?set=1")
		if body != "set-by-mw" {
			t.Fatalf("iter %d set: %q", i, body)
		}
		_, body = httpGet(t, port, "/api/x")
		if body != "none" {
			t.Fatalf("iter %d unset: %q (locals leaked across pool reuse)", i, body)
		}
	}
}

// TestAsyncMiddlewareChainOrder verifies outermost-first ordering for async
// middleware, mirroring the sync chain semantics.
func TestAsyncMiddlewareChainOrder(t *testing.T) {
	var trace []string
	var traceMu sync.Mutex
	record := func(s string) {
		traceMu.Lock()
		trace = append(trace, s)
		traceMu.Unlock()
	}

	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync(func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("a-before")
				next(res, req)
				record("a-after")
			}
		})
		app.UseAsync(func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("b-before")
				next(res, req)
				record("b-after")
			}
		})
		app.GetAsync("/x", func(res *gogo.Response, req *gogo.Request) {
			record("handler")
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	if status, body := httpGet(t, port, "/x"); status != 200 || body != "ok" {
		t.Fatalf("got %d %q", status, body)
	}
	traceMu.Lock()
	got := strings.Join(trace, ",")
	traceMu.Unlock()
	want := "a-before,b-before,handler,b-after,a-after"
	if got != want {
		t.Fatalf("trace: got %q want %q", got, want)
	}
}

// TestAsyncMiddlewareWithSyncMiddleware mixes sync and async middleware on
// the same async route. Sync mw runs first on the loop thread (header check
// before async dispatch); async mw runs after the goroutine transition.
func TestAsyncMiddlewareWithSyncMiddleware(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		// Sync gate: must have x-tenant header at all.
		app.Use("/api/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				if req.Header("x-tenant") == "" {
					res.Send(400, "text/plain", "no tenant")
					return
				}
				next(res, req)
			}
		})
		// Async gate: simulate a DB lookup to validate the tenant.
		app.UseAsync("/api/*", func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				time.Sleep(1 * time.Millisecond)
				tenant := req.Header("x-tenant")
				if tenant != "acme" {
					res.Send(403, "text/plain", "bad tenant")
					return
				}
				req.SetLocal("tenant", tenant)
				next(res, req)
			}
		})
		app.GetAsync("/api/work", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "for "+req.Local("tenant").(string))
		})
	})
	defer teardown()

	// No header → sync mw responds first, async mw never runs.
	if status, body := httpGet(t, port, "/api/work"); status != 400 || body != "no tenant" {
		t.Fatalf("no-tenant: got %d %q", status, body)
	}

	// Bad tenant header → sync passes, async rejects.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/work", port), nil)
	req.Header.Set("X-Tenant", "evil")
	resp, _ := noKeepaliveClient.Do(req)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 403 || string(b) != "bad tenant" {
		t.Fatalf("bad-tenant: got %d %q", resp.StatusCode, string(b))
	}

	// Good tenant → both pass, handler reads local.
	req, _ = http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/work", port), nil)
	req.Header.Set("X-Tenant", "acme")
	resp, _ = noKeepaliveClient.Do(req)
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "for acme" {
		t.Fatalf("acme: got %d %q", resp.StatusCode, string(b))
	}
}

// TestAsyncMiddlewareOnPostAsync verifies async mw runs after body collection
// and that req.Body() returns the collected body inside the middleware.
func TestAsyncMiddlewareOnPostAsync(t *testing.T) {
	var seenBodyLen atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync("/upload", func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				// Body is available inside async middleware.
				body := req.Body()
				seenBodyLen.Store(int32(len(body)))
				req.SetLocal("sig", "len-"+strconv.Itoa(len(body)))
				next(res, req)
			}
		})
		app.PostAsync("/upload", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", req.Local("sig").(string)+"/"+strconv.Itoa(len(body)))
		})
	})
	defer teardown()

	payload := []byte("hello-world")
	status, body := httpPost(t, port, "/upload", "text/plain", payload)
	if status != 200 || body != "len-11/11" {
		t.Fatalf("got %d %q", status, body)
	}
	if seenBodyLen.Load() != int32(len(payload)) {
		t.Fatalf("async mw body len = %d, want %d", seenBodyLen.Load(), len(payload))
	}
}

// TestUseAsyncRejectsBadArgs mirrors TestUseRejectsBadArgs for UseAsync.
//
// Cross-type middleware (gogo.Middleware passed to UseAsync, or
// gogo.AsyncMiddleware passed to Use) is now accepted — see
// TestUseAcceptsCrossTypeMiddleware. This test only covers genuinely
// malformed arguments.
func TestUseAsyncRejectsBadArgs(t *testing.T) {
	cases := []struct {
		name string
		call func(app *gogo.App)
	}{
		{"int arg", func(app *gogo.App) { app.UseAsync(42) }},
		{"two strings", func(app *gogo.App) { app.UseAsync("/api/*", "/users") }},
		{"prefix only no mw", func(app *gogo.App) { app.UseAsync("/api/*") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for %s, got none", c.name)
				}
			}()
			app, err := gogo.NewApp()
			if err != nil {
				t.Fatalf("NewApp: %v", err)
			}
			defer app.Close()
			c.call(app)
		})
	}
}

// TestPostSharedDispatch sends a small JSON body to a PostAsync
// route registered with maxBodyBytes within the SNAP_BODY_CAP
// (zero-cgo path). The handler receives the body via the
// snapshot — no res.Body cgo call needed — and echoes it back.
func TestPostSharedDispatch(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/echo", 4096, func(res *gogo.Response, req *gogo.Request, body []byte) {
			// Body should match what the client sent verbatim.
			res.Header("X-Body-Len", strconv.Itoa(len(body)))
			res.Send(200, "application/json", string(body))
		})
	})
	defer teardown()

	payload := []byte(`{"hello":"world","n":42}`)
	resp, err := http.Post(
		fmt.Sprintf("http://127.0.0.1:%d/echo", port),
		"application/json",
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200; body=%q", resp.StatusCode, string(body))
	}
	if string(body) != string(payload) {
		t.Errorf("body = %q, want %q", string(body), string(payload))
	}
	if got := resp.Header.Get("X-Body-Len"); got != strconv.Itoa(len(payload)) {
		t.Errorf("X-Body-Len = %q, want %d", got, len(payload))
	}
}

// TestPostSharedOversize_413 sends a body larger than the route's
// maxBodyBytes. The shared-dispatch path rejects on the loop
// thread with 413 — no goroutine ever spawns.
func TestPostSharedOversize_413(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/echo", 64, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	payload := make([]byte, 256) // > maxBodyBytes=64
	for i := range payload {
		payload[i] = 'x'
	}
	resp, err := http.Post(
		fmt.Sprintf("http://127.0.0.1:%d/echo", port),
		"application/octet-stream",
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Errorf("status = %d, want 413", resp.StatusCode)
	}
}

// TestPostSharedFallbackForLargeBody confirms PostAsync auto-falls
// back to the cgo-mediated async path when the route's maxBodyBytes
// exceeds the shared-dispatch cap. The visible behavior should be
// identical (body echoed back); the difference is the dispatch
// mechanism, which we don't expose to user code.
func TestPostSharedFallbackForLargeBody(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		// 32 KiB cap — well above the 8 KiB shared-dispatch cap,
		// forces the fallback path.
		app.PostAsync("/big", 32*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Header("X-Body-Len", strconv.Itoa(len(body)))
			res.Send(200, "application/octet-stream", string(body))
		})
	})
	defer teardown()

	payload := make([]byte, 16*1024) // 16 KiB — fits the 32 KiB cap, exceeds 8 KiB shared cap
	for i := range payload {
		payload[i] = byte(i)
	}
	resp, err := http.Post(
		fmt.Sprintf("http://127.0.0.1:%d/big", port),
		"application/octet-stream",
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !bytes.Equal(body, payload) {
		t.Errorf("body mismatch: got %d bytes, want %d", len(body), len(payload))
	}
}

// TestPostSharedSeesHeaders confirms the request-side snapshot
// (headers / URL / params / etc.) reaches the worker the same way
// it does on GetAsync.
func TestPostSharedSeesHeaders(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/users/:id", 4096, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Header("X-Param-Id", req.Param("id"))
			res.Header("X-Saw-Auth", req.Header("authorization"))
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	r, _ := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d/users/alice", port),
		strings.NewReader("hi"))
	r.Header.Set("Authorization", "Bearer abc")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("X-Param-Id"); got != "alice" {
		t.Errorf("X-Param-Id = %q, want alice", got)
	}
	if got := resp.Header.Get("X-Saw-Auth"); got != "Bearer abc" {
		t.Errorf("X-Saw-Auth = %q, want Bearer abc", got)
	}
}

// TestRequestHeadersIterator confirms req.Headers walks every
// (name, value) pair without cgo on sync routes. Multiple
// custom request headers are sent; the handler stamps every one
// it saw back into the response so the test can assert names +
// values came through unchanged.
func TestRequestHeadersIterator(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/dump", func(res *gogo.Response, req *gogo.Request) {
			// Aggregate every header into a sortable list so the
			// assertion is order-independent (uWS may reorder
			// trailing headers; user-agent / accept come from
			// the client in a particular order).
			var pairs []string
			n := req.Headers(func(name, value string) bool {
				pairs = append(pairs, name+":"+value)
				return true
			})
			res.Header("X-Header-Count", strconv.Itoa(n))
			res.Send(200, "text/plain", strings.Join(pairs, "\n"))
		})
	})
	defer teardown()

	r, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/dump", port), nil)
	r.Header.Set("X-Custom-A", "alpha")
	r.Header.Set("X-Custom-B", "beta")
	r.Header.Set("X-Custom-C", "gamma")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	pairs := strings.Split(string(body), "\n")

	have := map[string]string{}
	for _, p := range pairs {
		k, v, _ := strings.Cut(p, ":")
		have[k] = v
	}
	for _, want := range []struct{ name, value string }{
		{"x-custom-a", "alpha"},
		{"x-custom-b", "beta"},
		{"x-custom-c", "gamma"},
	} {
		if got := have[want.name]; got != want.value {
			t.Errorf("Headers: %q = %q, want %q (full = %v)", want.name, got, want.value, have)
		}
	}
	if cnt := resp.Header.Get("X-Header-Count"); cnt == "" || cnt == "0" {
		t.Errorf("X-Header-Count = %q, want non-zero", cnt)
	}
}

// TestRequestHeadersIteratorEarlyStop verifies that returning
// false from the iterator callback stops the walk and the count
// reflects only the headers visited up to that point.
func TestRequestHeadersIteratorEarlyStop(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/stop", func(res *gogo.Response, req *gogo.Request) {
			visited := 0
			req.Headers(func(name, value string) bool {
				visited++
				return visited < 2 // stop after the 2nd header
			})
			res.Header("X-Visited", strconv.Itoa(visited))
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	r, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/stop", port), nil)
	r.Header.Set("X-A", "1")
	r.Header.Set("X-B", "2")
	r.Header.Set("X-C", "3")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("X-Visited"); got != "2" {
		t.Errorf("X-Visited = %q, want 2", got)
	}
}

// TestRequestHeadersIteratorAsync covers the snapshot path —
// req.Headers walks the AsyncCtx's packed blob inside an async
// handler. The snapshot was already captured before the worker
// ran, so the underlying live request is gone by this point;
// the test confirms the headers still arrive via the snapshot.
func TestRequestHeadersIteratorAsync(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/dump-async", func(res *gogo.Response, req *gogo.Request) {
			var pairs []string
			req.Headers(func(name, value string) bool {
				pairs = append(pairs, name+":"+value)
				return true
			})
			res.Send(200, "text/plain", strings.Join(pairs, "\n"))
		})
	})
	defer teardown()

	r, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/dump-async", port), nil)
	r.Header.Set("X-Async-Trace", "trace-42")
	r.Header.Set("X-Async-Auth", "Bearer xyz")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	got := string(body)
	if !strings.Contains(got, "x-async-trace:trace-42") {
		t.Errorf("missing x-async-trace in async Headers walk: %q", got)
	}
	if !strings.Contains(got, "x-async-auth:Bearer xyz") {
		t.Errorf("missing x-async-auth in async Headers walk: %q", got)
	}
}

// TestPlaceBothFiresOnceAndKeepsFastPath registers a raw counter
// middleware via App.Use. Raw middleware defaults to "both chains"
// placement so both a sync and async route should observe exactly
// one invocation each — proving that the dual-chain registration
// does NOT cause double-execution. The async route should also keep
// the zero-cgo dispatch path: alsoAsync entries in the sync chain
// are skipped by hasMatchingMiddleware.
func TestPlaceBothFiresOnceAndKeepsFastPath(t *testing.T) {
	var syncCount atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				syncCount.Add(1)
				next(res, req)
			}
		})
		app.Get("/sync", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "sync")
		})
		app.GetAsync("/async", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "async")
		})
	})
	defer teardown()

	r1, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/sync", port))
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	r1.Body.Close()
	if got := syncCount.Load(); got != 1 {
		t.Errorf("sync route: count=%d want 1", got)
	}

	syncCount.Store(0)
	r2, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/async", port))
	if err != nil {
		t.Fatalf("async: %v", err)
	}
	r2.Body.Close()
	if got := syncCount.Load(); got != 1 {
		t.Errorf("async route: count=%d want 1 (double-fire would be 2)", got)
	}
}

// TestSyncChainForcesSlowPath uses the bundled RateLimit middleware
// (Place=Sync internally) on an async route and verifies the
// middleware still fires — the framework takes the slow path
// (sync wrap → snapshot → async dispatch) because at least one
// sync-only middleware matches.
func TestSyncChainForcesSlowPath(t *testing.T) {
	var hits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		// RateLimit is internally placed in the sync chain so cheap
		// rejecters can short-circuit before a goroutine spawns.
		// Counter middleware composed below it observes invocation.
		app.Use(middleware.RateLimit(middleware.RateLimitOptions{
			Max:    1000000,
			Window: time.Hour,
		}))
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				hits.Add(1)
				next(res, req)
			}
		})
		app.GetAsync("/async", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/async", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if hits.Load() != 1 {
		t.Errorf("sync-chain mw did not fire on async route")
	}
}

// TestAsyncOnlyMiddlewareSkipsSyncRoute confirms that a middleware
// wrapped via middleware.Async is invisible to sync routes.
func TestAsyncOnlyMiddlewareSkipsSyncRoute(t *testing.T) {
	var hits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.Async(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				hits.Add(1)
				next(res, req)
			}
		}))
		app.Get("/sync", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
		app.GetAsync("/async", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	r1, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/sync", port))
	r1.Body.Close()
	if hits.Load() != 0 {
		t.Errorf("Async-wrapped mw fired on sync route: %d", hits.Load())
	}

	r2, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/async", port))
	r2.Body.Close()
	if hits.Load() != 1 {
		t.Errorf("Async-wrapped mw did not fire on async route: %d", hits.Load())
	}
}

// TestUseAcceptsCrossTypeMiddleware confirms App.Use and App.UseAsync
// App.Use accepts both Middleware and AsyncMiddleware shapes. After
// the redesign:
//   - Raw Middleware → both chains (sync routes fire it on the loop
//     thread, async routes fire it in the worker).
//   - Raw AsyncMiddleware → async chain only (signals "I want to
//     run on the goroutine that runs the async handler"); sync
//     routes do not see it.
func TestUseAcceptsCrossTypeMiddleware(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(gogo.AsyncMiddleware(func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				res.Header("X-From-Async-MW", "1")
				next(res, req)
			}
		}))
		app.Use(gogo.Middleware(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				res.Header("X-From-Sync-MW", "1")
				next(res, req)
			}
		}))
		app.Get("/sync", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "sync")
		})
		app.GetAsync("/async", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "async")
		})
	})
	defer teardown()

	r1, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/sync", port))
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	r1.Body.Close()
	// AsyncMiddleware is async-only by default — sync route does NOT
	// see it. The sync Middleware fires here (PlaceBoth default).
	if r1.Header.Get("X-From-Async-MW") != "" {
		t.Errorf("sync route: async-typed middleware should not fire")
	}
	if r1.Header.Get("X-From-Sync-MW") != "1" {
		t.Errorf("sync route: missing sync-typed middleware header")
	}

	r2, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/async", port))
	if err != nil {
		t.Fatalf("async: %v", err)
	}
	r2.Body.Close()
	// Async route: both fire (async-typed in async chain, sync-typed
	// via the PlaceBoth dup in async chain — slow path is not active
	// because no PlaceSync entry matches).
	if r2.Header.Get("X-From-Async-MW") != "1" {
		t.Errorf("async route: missing async-typed middleware header")
	}
	if r2.Header.Get("X-From-Sync-MW") != "1" {
		t.Errorf("async route: missing sync-typed middleware header")
	}
}

// TestUseRejectsBadArgs verifies the Use type-switch panics on garbage args.
func TestUseRejectsBadArgs(t *testing.T) {
	cases := []struct {
		name string
		call func(app *gogo.App)
	}{
		{"int arg", func(app *gogo.App) { app.Use(42) }},
		{"two strings", func(app *gogo.App) { app.Use("/api/*", "/users") }},
		{"prefix only no mw", func(app *gogo.App) { app.Use("/api/*") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for %s, got none", c.name)
				}
			}()
			app, err := gogo.NewApp()
			if err != nil {
				t.Fatalf("NewApp: %v", err)
			}
			defer app.Close()
			c.call(app)
		})
	}
}

func TestAsyncRequestSnapshot(t *testing.T) {
	// Async handlers should see method/url/query/params/headers via the
	// snapshot captured before uWS freed the live request.
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/users/:id", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "application/json", fmt.Sprintf(
				`{"method":%q,"url":%q,"param":%q,"q":%q,"qp":%q,"hdr":%q}`,
				req.Method(), req.URL(), req.Parameter(0),
				req.Query(), req.QueryParam("token"), req.Header("x-custom"),
			))
		})
	})
	defer teardown()

	httpReq, _ := http.NewRequest("GET",
		fmt.Sprintf("http://127.0.0.1:%d/users/42?token=abc&other=1", port),
		nil)
	httpReq.Header.Set("X-Custom", "snapshot-works")
	resp, err := noKeepaliveClient.Do(httpReq)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	got := string(body)
	for _, want := range []string{
		`"method":"get"`,
		`"url":"/users/42"`,
		`"param":"42"`,
		`"q":"token=abc&other=1"`,
		`"qp":"abc"`,
		`"hdr":"snapshot-works"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in %s", want, got)
		}
	}
}

func TestPostAsyncRequestSnapshot(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/echo/:tag", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "application/json", fmt.Sprintf(
				`{"tag":%q,"qp":%q,"hdr":%q,"body":%q}`,
				req.Parameter(0), req.QueryParam("k"),
				req.Header("x-flag"), string(body),
			))
		})
	})
	defer teardown()

	httpReq, _ := http.NewRequest("POST",
		fmt.Sprintf("http://127.0.0.1:%d/echo/hello?k=v", port),
		bytes.NewReader([]byte("payload")))
	httpReq.Header.Set("X-Flag", "yes")
	httpReq.Header.Set("Content-Type", "text/plain")
	resp, err := noKeepaliveClient.Do(httpReq)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	got := string(body)
	for _, want := range []string{
		`"tag":"hello"`,
		`"qp":"v"`,
		`"hdr":"yes"`,
		`"body":"payload"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in %s", want, got)
		}
	}
}

func TestResponseJSON(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/json", func(res *gogo.Response, req *gogo.Request) {
			res.JSON(200, map[string]any{
				"ok":    true,
				"count": 42,
				"name":  "claude",
			})
		})
		app.GetAsync("/async-json", func(res *gogo.Response, req *gogo.Request) {
			res.JSON(201, map[string]any{"created": true})
		})
	})
	defer teardown()

	// Sync handler.
	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/json", port))
	if err != nil {
		t.Fatalf("GET /json: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("Content-Type=%q", ct)
	}
	var v map[string]any
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, body)
	}
	if v["ok"] != true || v["count"].(float64) != 42 || v["name"] != "claude" {
		t.Fatalf("payload=%v", v)
	}

	// Async handler uses the SendShared path.
	resp, err = noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/async-json", port))
	if err != nil {
		t.Fatalf("GET /async-json: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("async status=%d", resp.StatusCode)
	}
	if string(body) != `{"created":true}` {
		t.Fatalf("async body=%q", body)
	}
}

func TestResponseJSONUsesConfiguredEncoder(t *testing.T) {
	var calls atomic.Int32
	cfg := gogo.Config{
		JSONEncoder: func(v any) ([]byte, error) {
			calls.Add(1)
			return []byte(`{"custom":true}`), nil
		},
	}
	port, teardown := startAppCfg(t, cfg, func(app *gogo.App) {
		app.Get("/json", func(res *gogo.Response, req *gogo.Request) {
			res.JSON(200, map[string]any{"ignored": true})
		})
		app.GetAsync("/async-json", func(res *gogo.Response, req *gogo.Request) {
			res.JSON(201, map[string]any{"ignored": true})
		})
		app.Get("/jsonp", func(res *gogo.Response, req *gogo.Request) {
			res.JSONP("cb", map[string]any{"ignored": true})
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/json", port))
	if err != nil {
		t.Fatalf("GET /json: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != `{"custom":true}` {
		t.Fatalf("sync JSON body=%q", body)
	}

	resp, err = noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/async-json", port))
	if err != nil {
		t.Fatalf("GET /async-json: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 201 || string(body) != `{"custom":true}` {
		t.Fatalf("async JSON got status=%d body=%q", resp.StatusCode, body)
	}

	resp, err = noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/jsonp", port))
	if err != nil {
		t.Fatalf("GET /jsonp: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != `/**/cb({"custom":true});` {
		t.Fatalf("JSONP body=%q", body)
	}
	if calls.Load() != 3 {
		t.Fatalf("encoder calls=%d, want 3", calls.Load())
	}
}

func TestResponseJSONPWithCustomEncoderEscapesScriptBreakout(t *testing.T) {
	cfg := gogo.Config{
		JSONEncoder: func(v any) ([]byte, error) {
			return []byte("{\"x\":\"</script>&\xc2\x85\xe2\x80\xa8\xe2\x80\xa9\"}"), nil
		},
	}
	port, teardown := startAppCfg(t, cfg, func(app *gogo.App) {
		app.Get("/jsonp", func(res *gogo.Response, req *gogo.Request) {
			res.JSONP("cb", map[string]any{"ignored": true})
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/jsonp", port))
	if err != nil {
		t.Fatalf("GET /jsonp: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	want := `/**/cb({"x":"\u003c/script\u003e\u0026\u0085\u2028\u2029"});`
	if string(body) != want {
		t.Fatalf("JSONP body=%q, want %q", body, want)
	}
}

// TestResponseJSONBytes confirms the pre-marshaled JSON shortcut emits
// the exact bytes with Content-Type: application/json. Useful for
// cached responses and faster encoders.
func TestResponseJSONBytes(t *testing.T) {
	preMarshaled := []byte(`{"cached":true,"items":[1,2,3]}`)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.JSONBytes(200, preMarshaled)
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(body, preMarshaled) {
		t.Errorf("body: got %q, want %q", body, preMarshaled)
	}
}

// TestResponseJSONStream exercises the streaming encoder path —
// avoids the per-response buffer allocation json.Marshal makes
// for large bodies. Asserts each Encode call lands on the wire as
// a newline-delimited JSON value.
func TestResponseJSONStream(t *testing.T) {
	type item struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/items", func(res *gogo.Response, req *gogo.Request) {
			_ = res.JSONStream(200, func(enc *json.Encoder) error {
				for i := 1; i <= 3; i++ {
					if err := enc.Encode(item{ID: i, Name: fmt.Sprintf("n-%d", i)}); err != nil {
						return err
					}
				}
				return nil
			})
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/items", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("ndjson lines: got %d, want 3 (body=%q)", len(lines), body)
	}
	for i, line := range lines {
		var got item
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Errorf("line %d %q: %v", i, line, err)
			continue
		}
		want := item{ID: i + 1, Name: fmt.Sprintf("n-%d", i+1)}
		if got != want {
			t.Errorf("line %d: got %+v, want %+v", i, got, want)
		}
	}
}

func TestRequestCookie(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain",
				"session="+req.Cookie("session")+
					";theme="+req.Cookie("theme")+
					";missing="+req.Cookie("missing"))
		})
	})
	defer teardown()

	httpReq, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/r", port), nil)
	httpReq.Header.Set("Cookie", "session=abc123; theme=dark; junk")
	resp, err := noKeepaliveClient.Do(httpReq)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "session=abc123;theme=dark;missing=" {
		t.Fatalf("got %q", body)
	}
}

func TestResponseSetCookie(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/login", func(res *gogo.Response, req *gogo.Request) {
			res.SetCookie(gogo.Cookie{
				Name:     "session",
				Value:    "token123",
				Path:     "/",
				MaxAge:   3600,
				HttpOnly: true,
				SameSite: gogo.SameSiteLax,
			})
			res.SetCookie(gogo.Cookie{
				Name:  "theme",
				Value: "dark",
			})
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/login", port))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()

	cookies := resp.Header.Values("Set-Cookie")
	if len(cookies) != 2 {
		t.Fatalf("got %d Set-Cookie headers, want 2: %v", len(cookies), cookies)
	}
	// First cookie has full attributes.
	if !strings.Contains(cookies[0], "session=token123") ||
		!strings.Contains(cookies[0], "Path=/") ||
		!strings.Contains(cookies[0], "Max-Age=3600") ||
		!strings.Contains(cookies[0], "HttpOnly") ||
		!strings.Contains(cookies[0], "SameSite=Lax") {
		t.Fatalf("session cookie: %q", cookies[0])
	}
	// Second cookie is bare.
	if cookies[1] != "theme=dark" {
		t.Fatalf("theme cookie: %q", cookies[1])
	}
}

func TestSetCookieRejectsBadValue(t *testing.T) {
	app, err := gogo.NewApp()
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	defer app.Close()

	// SetCookie validation runs before any wire output, so we can drive
	// it against a zero-value Response and observe the panic without
	// standing up a server.
	cases := []struct {
		label  string
		cookie gogo.Cookie
	}{
		{"bad name (space)", gogo.Cookie{Name: "bad name", Value: "x"}},
		{"bad value (CR)", gogo.Cookie{Name: "ok", Value: "has\rnewline"}},
		// Path / Domain / Expires / SameSite were not validated in the
		// original implementation — these subtests are the regression
		// guard for the cookie-attribute-injection bug. The fix
		// rejects ";", CTLs, and (for SameSite) any value outside the
		// three RFC 6265bis tokens.
		{"path semicolon injection", gogo.Cookie{
			Name:  "ok",
			Value: "x",
			Path:  "/; Domain=evil.example; Secure=false",
		}},
		{"path CR injection", gogo.Cookie{Name: "ok", Value: "x", Path: "/foo\r\nX-Bad: 1"}},
		{"domain semicolon injection", gogo.Cookie{
			Name:   "ok",
			Value:  "x",
			Domain: "victim.com; HttpOnly=false",
		}},
		{"domain space injection", gogo.Cookie{Name: "ok", Value: "x", Domain: "victim com"}},
		{"expires semicolon injection", gogo.Cookie{
			Name:    "ok",
			Value:   "x",
			Expires: "Wed, 21 Oct 2025 07:28:00 GMT; Secure=false",
		}},
		{"samesite injection via cast", gogo.Cookie{
			Name:     "ok",
			Value:    "x",
			SameSite: gogo.SameSite("Lax; Domain=evil.example"),
		}},
		{"samesite unknown token", gogo.Cookie{
			Name:     "ok",
			Value:    "x",
			SameSite: gogo.SameSite("Whatever"),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("SetCookie %s did not panic", tc.label)
				}
			}()
			var r gogo.Response
			r.SetCookie(tc.cookie)
		})
	}
}

// TestSetCookieSameSiteNoneRequiresSecure: per RFC 6265bis §5.5,
// SameSite=None cookies without Secure are rejected by modern
// browsers entirely. The framework panics at the boundary so the
// misconfig surfaces during development instead of silently breaking
// auth in production.
func TestSetCookieSameSiteNoneRequiresSecure(t *testing.T) {
	t.Run("none-without-secure panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatalf("SetCookie SameSite=None without Secure did not panic")
			}
		}()
		var r gogo.Response
		r.SetCookie(gogo.Cookie{
			Name:     "session",
			Value:    "x",
			SameSite: gogo.SameSiteNone,
			// Secure intentionally omitted
		})
	})
	t.Run("none-with-secure accepted", func(t *testing.T) {
		// Drive through a real handler so SetCookie's tail (which
		// writes to res.inner) doesn't crash on a zero-value Response.
		port, teardown := startApp(t, func(app *gogo.App) {
			app.Get("/", func(res *gogo.Response, req *gogo.Request) {
				res.SetCookie(gogo.Cookie{
					Name:     "session",
					Value:    "x",
					SameSite: gogo.SameSiteNone,
					Secure:   true,
				})
				res.Send(200, "text/plain", "ok")
			})
		})
		defer teardown()

		resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status=%d, want 200 — SameSite=None+Secure should be accepted", resp.StatusCode)
		}
		cookie := resp.Header.Get("Set-Cookie")
		if !strings.Contains(cookie, "SameSite=None") || !strings.Contains(cookie, "Secure") {
			t.Errorf("Set-Cookie missing expected attrs: %q", cookie)
		}
	})
}

// TestSetCookieAttributesValid: confirm well-formed values for Path,
// Domain, Expires, and SameSite all pass validation. The serialized
// header must contain each attribute exactly once.
func TestSetCookieAttributesValid(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.SetCookie(gogo.Cookie{
				Name:     "session",
				Value:    "abc123",
				Path:     "/api",
				Domain:   ".example.com",
				Expires:  "Wed, 21 Oct 2025 07:28:00 GMT",
				MaxAge:   3600,
				Secure:   true,
				HttpOnly: true,
				SameSite: gogo.SameSiteLax,
			})
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
	setCookie := resp.Header.Get("Set-Cookie")
	for _, want := range []string{
		"session=abc123",
		"Path=/api",
		"Domain=.example.com",
		"Expires=Wed, 21 Oct 2025 07:28:00 GMT",
		"Max-Age=3600",
		"Secure",
		"HttpOnly",
		"SameSite=Lax",
	} {
		if !strings.Contains(setCookie, want) {
			t.Errorf("Set-Cookie %q missing %q", setCookie, want)
		}
	}
}

func TestSnapshotBoundaries(t *testing.T) {
	// The C++ snapshot has fixed caps per field. Oversized snapshots are
	// rejected instead of silently truncating security-sensitive request data.
	// Caps mirror SNAP_* constants in uws_bridge.cpp: URL=256, QUERY=512,
	// PARAM=64 each.
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/echo/:p", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "application/json", fmt.Sprintf(
				`{"urlLen":%d,"queryLen":%d,"paramLen":%d}`,
				len(req.URL()), len(req.Query()), len(req.Parameter(0)),
			))
		})
	})
	defer teardown()

	bigParam := strings.Repeat("p", 200)        // > PARAM_CAP=64
	bigQuery := "k=" + strings.Repeat("v", 600) // > QUERY_CAP=512

	target := fmt.Sprintf("http://127.0.0.1:%d/echo/%s?%s", port, bigParam, bigQuery)
	resp, err := noKeepaliveClient.Get(target)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 431 || !strings.Contains(string(body), "snapshot too large") {
		t.Fatalf("oversized snapshot: got %d %q, want 431", resp.StatusCode, body)
	}
}

func TestSnapshotURLTruncation(t *testing.T) {
	// URL that exceeds URL_CAP=256 — uWS allows long paths, gogo rejects before
	// dispatching the async handler.
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/long/:p", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", fmt.Sprintf("urlLen=%d", len(req.URL())))
		})
	})
	defer teardown()

	// "/long/" + 300 chars = 306 bytes
	bigPath := strings.Repeat("a", 300)
	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/long/%s", port, bigPath))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 431 || !strings.Contains(string(body), "snapshot too large") {
		t.Fatalf("long URL: got %d %q, want 431", resp.StatusCode, body)
	}
}

func TestStressShortBursts(t *testing.T) {
	if testing.Short() {
		t.Skip("skip stress test in short mode")
	}
	// Higher concurrency than TestConcurrentLoad. Mixes shared, sync, and
	// PostAsync handlers to exercise multiple worker paths concurrently.
	port, teardown := startApp(t, func(app *gogo.App) {
		var counter atomic.Int64
		app.GetAsync("/async", func(res *gogo.Response, req *gogo.Request) {
			counter.Add(1)
			res.Send(200, "text/plain", "ok")
		})
		app.Get("/sync", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "sync")
		})
		app.PostAsync("/post", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", fmt.Sprintf("got %d", len(body)))
		})
	})
	defer teardown()

	const clients = 64
	const perClient = 100
	var wg sync.WaitGroup
	var errs atomic.Int64
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			client := &http.Client{
				Transport: &http.Transport{DisableKeepAlives: true},
				Timeout:   5 * time.Second,
			}
			for j := 0; j < perClient; j++ {
				var resp *http.Response
				var err error
				switch j % 3 {
				case 0:
					resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/async", port))
				case 1:
					resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/sync", port))
				case 2:
					resp, err = client.Post(
						fmt.Sprintf("http://127.0.0.1:%d/post", port),
						"text/plain",
						bytes.NewReader([]byte("data")))
				}
				if err != nil {
					errs.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					errs.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()
	if e := errs.Load(); e != 0 {
		t.Fatalf("got %d errors under stress (out of %d requests)", e, clients*perClient)
	}
}

func TestClientAbortDuringAsync(t *testing.T) {
	// Client disconnects while the async handler is still sleeping. The
	// handler must still run to completion (uWS doesn't kill goroutines)
	// and SendShared must silently drop the response when the connection
	// is gone, rather than crash. We don't call res.OnAborted from the
	// worker — uWS::onAborted is loop-thread-only, and the shared path
	// already registers an internal abort handler in C++.
	var handlerDone atomic.Int32

	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/slow", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(80 * time.Millisecond)
			res.Send(200, "text/plain", "late")
			handlerDone.Add(1)
		})
	})
	defer teardown()

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
			if err != nil {
				return
			}
			fmt.Fprintf(conn, "GET /slow HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
			// Close almost immediately — server sees disconnect mid-async.
			time.Sleep(10 * time.Millisecond)
			conn.Close()
		}()
	}
	wg.Wait()

	// Give handlers time to wake up after their sleeps.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && handlerDone.Load() < int32(n) {
		time.Sleep(20 * time.Millisecond)
	}
	if handlerDone.Load() < int32(n) {
		t.Errorf("only %d/%d handlers finished — possible deadlock or crash", handlerDone.Load(), n)
	}
}

func TestClientAbortDuringPostBodyThenServerContinues(t *testing.T) {
	var completed atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/upload", 1024*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			completed.Add(1)
			res.Send(200, "text/plain", fmt.Sprintf("got %d", len(body)))
		})
	})
	defer teardown()

	const aborted = 20
	for i := 0; i < aborted; i++ {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		fmt.Fprintf(conn, "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\nConnection: close\r\n\r\npartial")
		conn.Close()
	}

	time.Sleep(200 * time.Millisecond)
	if completed.Load() != 0 {
		t.Fatalf("aborted uploads reached handler: %d", completed.Load())
	}

	status, body := httpPost(t, port, "/upload", "text/plain", []byte("hello"))
	if status != 200 || body != "got 5" {
		t.Fatalf("server did not continue after aborted uploads: got %d %q", status, body)
	}
}

// TestBodyReadTimeout: a slow client that opens a POST, sends a
// Content-Length, then drips a few bytes and stalls must see the
// server short-circuit with ErrBodyTimeout once Config.BodyReadTimeout
// elapses — instead of holding the goroutine forever (slow-loris).
func TestBodyReadTimeout(t *testing.T) {
	timedOut := make(chan error, 1)
	port, teardown := startAppCfg(t, gogo.Config{BodyReadTimeout: 150 * time.Millisecond}, func(app *gogo.App) {
		app.Post("/slow", func(res *gogo.Response, req *gogo.Request) {
			res.Body(64*1024, func(body []byte, err error) {
				timedOut <- err
				if err != nil {
					res.Send(408, "text/plain", err.Error())
					return
				}
				res.Send(200, "text/plain", "ok")
			})
		})
	})
	defer teardown()

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	// Promise a 1 KiB body, send only the first 5 bytes, then go silent.
	fmt.Fprintf(conn, "POST /slow HTTP/1.1\r\nHost: x\r\nContent-Length: 1024\r\nConnection: close\r\n\r\nhello")

	select {
	case err := <-timedOut:
		if err != gogo.ErrBodyTimeout {
			t.Fatalf("expected ErrBodyTimeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("body read timeout did not fire within 2s")
	}
}

func TestBodyAsyncReadTimeoutSends408WithoutHandler(t *testing.T) {
	var handlerCalls atomic.Int32
	handler := func(res *gogo.Response, req *gogo.Request, body []byte) {
		handlerCalls.Add(1)
		res.Send(200, "text/plain", "unexpected")
	}

	port, teardown := startAppCfg(t, gogo.Config{BodyReadTimeout: 100 * time.Millisecond}, func(app *gogo.App) {
		app.PostAsync("/post", 64*1024, handler)
		app.PutAsync("/put", 64*1024, handler)
		app.PatchAsync("/patch", 64*1024, handler)
		app.DeleteAsync("/delete", 64*1024, handler)

		api := app.Group("/api")
		api.PostAsync("/post", 64*1024, handler)
	})
	defer teardown()

	for _, tc := range []struct {
		method string
		path   string
	}{
		{"POST", "/post"},
		{"PUT", "/put"},
		{"PATCH", "/patch"},
		{"DELETE", "/delete"},
		{"POST", "/api/post"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			status, body := slowBodyResponse(t, port, tc.method, tc.path)
			if status != 408 {
				t.Fatalf("got %d %q, want 408", status, body)
			}
			if body != "request timeout\n" {
				t.Fatalf("got body %q, want request timeout", body)
			}
		})
	}

	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("body-async handler calls = %d, want 0", got)
	}
}

func slowBodyResponse(t *testing.T, port int, method, path string) (status int, body string) {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if _, err := fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: x\r\nContent-Length: 1024\r\nConnection: close\r\n\r\nhello", method, path); err != nil {
		t.Fatalf("write slow request: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp.StatusCode, string(b)
}

// TestBodyReadTimeoutDoesNotFireOnNormalUpload: a well-behaved POST
// that finishes promptly must NOT see the timeout error — the timer
// has to stop on the success path.
func TestBodyReadTimeoutDoesNotFireOnNormalUpload(t *testing.T) {
	port, teardown := startAppCfg(t, gogo.Config{BodyReadTimeout: 500 * time.Millisecond}, func(app *gogo.App) {
		app.Post("/fast", func(res *gogo.Response, req *gogo.Request) {
			res.Body(64*1024, func(body []byte, err error) {
				if err != nil {
					res.Send(500, "text/plain", "unexpected: "+err.Error())
					return
				}
				res.Send(200, "text/plain", fmt.Sprintf("got %d", len(body)))
			})
		})
	})
	defer teardown()

	status, body := httpPost(t, port, "/fast", "text/plain", []byte("hello"))
	if status != 200 || body != "got 5" {
		t.Fatalf("got %d %q", status, body)
	}
	// Give any stray timer a chance to misfire.
	time.Sleep(600 * time.Millisecond)
}

func TestHeaderInjectionRejected(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/inject", func(res *gogo.Response, req *gogo.Request) {
			defer func() {
				if r := recover(); r != nil {
					res.Send(400, "text/plain", "rejected")
				}
			}()
			res.Header("X-Bad", "value\r\nSet-Cookie: hack=1")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/inject")
	if status != 400 || !strings.Contains(body, "rejected") {
		t.Fatalf("header injection not rejected: got %d %q", status, body)
	}
}

// -----------------------------------------------------------------------------
// Middleware-bypass coverage. These tests pin down the security-relevant fix
// that landed alongside Group: scoped Use(prefix, mw) now matches the live
// request URL via req.URL() instead of the registered route pattern string,
// so dynamic / wildcard routes can no longer slip past path-scoped middleware.
// -----------------------------------------------------------------------------

// TestMiddlewareBypassParametricRoute is the canonical regression: a REST API
// scopes auth to /api/admin via Use("/api/admin", ...) and serves a parametric
// route Get("/api/:section"). The literal strings "/api/:section" and
// "/api/admin" share no string prefix, so under the old pattern-string match
// auth would never have wrapped the handler — but uWS still routes
// GET /api/admin into it via :section="admin", silently bypassing auth.
func TestMiddlewareBypassParametricRoute(t *testing.T) {
	var authHits, handlerHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use("/api/admin", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				authHits.Add(1)
				if req.Header("x-key") != "ok" {
					res.Send(401, "text/plain", "denied")
					return
				}
				next(res, req)
			}
		})
		app.Get("/api/:section", func(res *gogo.Response, req *gogo.Request) {
			handlerHits.Add(1)
			res.Send(200, "text/plain", "section="+req.Parameter(0))
		})
	})
	defer teardown()

	// /api/admin must hit the auth gate (the bug: it wouldn't).
	status, body := httpGet(t, port, "/api/admin")
	if status != 401 || body != "denied" {
		t.Fatalf("/api/admin without key: got %d %q, want 401 denied", status, body)
	}
	if authHits.Load() != 1 {
		t.Fatalf("auth hit count for /api/admin = %d, want 1", authHits.Load())
	}
	if handlerHits.Load() != 0 {
		t.Fatalf("handler ran for /api/admin without auth: hits=%d", handlerHits.Load())
	}

	// /api/users must NOT hit the auth gate (scope is /api/admin only).
	status, body = httpGet(t, port, "/api/users")
	if status != 200 || body != "section=users" {
		t.Fatalf("/api/users: got %d %q", status, body)
	}
	if authHits.Load() != 1 {
		t.Fatalf("auth wrongly ran for /api/users: hits=%d", authHits.Load())
	}
	if handlerHits.Load() != 1 {
		t.Fatalf("handler not called for /api/users: hits=%d", handlerHits.Load())
	}

	// /api/admin with the key passes through.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/admin", port), nil)
	req.Header.Set("X-Key", "ok")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("/api/admin authed: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "section=admin" {
		t.Fatalf("/api/admin authed: got %d %q", resp.StatusCode, string(b))
	}
}

// TestMiddlewareBypassWildcardFallback covers the second face of the bypass:
// a path-scoped middleware on /admin/* plus a catch-all Any("/*", spa). Under
// pattern-string match, the catch-all's pattern ("/*") has no prefix relation
// to "/admin", so it wouldn't be wrapped — but uWS falls back to "/*" for
// URLs like /admin/secret when no exact /admin/... route matches.
func TestMiddlewareBypassWildcardFallback(t *testing.T) {
	var adminHits, spaHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use("/admin/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				adminHits.Add(1)
				if req.Header("x-admin") != "1" {
					res.Send(401, "text/plain", "no admin")
					return
				}
				next(res, req)
			}
		})
		app.Get("/admin/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "users-list")
		})
		// SPA-style catch-all. Anything not matched above falls here.
		app.Any("/*", func(res *gogo.Response, req *gogo.Request) {
			spaHits.Add(1)
			res.Send(200, "text/plain", "spa")
		})
	})
	defer teardown()

	// /admin/secret has no exact match → uWS falls back to /*. The bug:
	// auth wouldn't wrap the /* handler, so the request would have hit
	// spa with 200. Fixed: auth runs first because the URL is under /admin.
	status, body := httpGet(t, port, "/admin/secret")
	if status != 401 || body != "no admin" {
		t.Fatalf("/admin/secret: got %d %q, want 401 no admin", status, body)
	}
	if spaHits.Load() != 0 {
		t.Fatalf("spa fallback wrongly ran for /admin/secret: hits=%d", spaHits.Load())
	}

	// /random falls to spa and must NOT trigger admin auth.
	status, body = httpGet(t, port, "/random")
	if status != 200 || body != "spa" {
		t.Fatalf("/random: got %d %q", status, body)
	}
	if adminHits.Load() != 1 {
		t.Fatalf("admin mw wrongly ran for /random (cumulative hits=%d, want 1)", adminHits.Load())
	}
	if spaHits.Load() != 1 {
		t.Fatalf("spa hit count = %d, want 1", spaHits.Load())
	}
}

// TestAsyncMiddlewareBypassParametric mirrors TestMiddlewareBypassParametricRoute
// for the async chain, which has its own wrapAsync path.
func TestAsyncMiddlewareBypassParametric(t *testing.T) {
	var authHits, handlerHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.UseAsync("/api/admin", func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				authHits.Add(1)
				if req.Header("x-key") != "ok" {
					res.Send(401, "text/plain", "denied")
					return
				}
				next(res, req)
			}
		})
		app.GetAsync("/api/:section", func(res *gogo.Response, req *gogo.Request) {
			handlerHits.Add(1)
			res.Send(200, "text/plain", "section="+req.Parameter(0))
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/api/admin")
	if status != 401 || body != "denied" {
		t.Fatalf("/api/admin without key: got %d %q", status, body)
	}
	if authHits.Load() != 1 {
		t.Fatalf("async auth hits for /api/admin = %d, want 1", authHits.Load())
	}
	if handlerHits.Load() != 0 {
		t.Fatalf("handler ran for /api/admin without async auth: hits=%d", handlerHits.Load())
	}

	status, body = httpGet(t, port, "/api/users")
	if status != 200 || body != "section=users" {
		t.Fatalf("/api/users: got %d %q", status, body)
	}
	if authHits.Load() != 1 {
		t.Fatalf("async auth wrongly ran for /api/users: hits=%d", authHits.Load())
	}
}

// -----------------------------------------------------------------------------
// Group / Router coverage.
// -----------------------------------------------------------------------------

// TestGroupBasicScopesMiddleware: routes registered through a Group are
// wrapped by the Group's middleware; routes on the App directly are not.
func TestGroupBasicScopesMiddleware(t *testing.T) {
	var authHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				authHits.Add(1)
				if req.Header("x-key") != "ok" {
					res.Send(401, "text/plain", "no")
					return
				}
				next(res, req)
			}
		})
		api.Get("/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "users")
		})
		app.Get("/public", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "public")
		})
	})
	defer teardown()

	if status, body := httpGet(t, port, "/public"); status != 200 || body != "public" {
		t.Fatalf("/public: got %d %q", status, body)
	}
	if authHits.Load() != 0 {
		t.Fatalf("group mw wrongly ran for /public: hits=%d", authHits.Load())
	}

	if status, body := httpGet(t, port, "/api/users"); status != 401 || body != "no" {
		t.Fatalf("/api/users no key: got %d %q", status, body)
	}
	if authHits.Load() != 1 {
		t.Fatalf("group mw hits for /api/users = %d, want 1", authHits.Load())
	}
}

// TestGroupCoversParametricRoute: the whole point of Group over Use(prefix).
// A parametric route under a Group is wrapped by identity, not pattern, so
// /api/admin via :section="admin" always hits the group's middleware.
func TestGroupCoversParametricRoute(t *testing.T) {
	var authHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				authHits.Add(1)
				if req.Header("x-key") != "ok" {
					res.Send(401, "text/plain", "denied")
					return
				}
				next(res, req)
			}
		})
		api.Get("/:section", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "section="+req.Parameter(0))
		})
	})
	defer teardown()

	for _, section := range []string{"admin", "users", "billing"} {
		status, body := httpGet(t, port, "/api/"+section)
		if status != 401 || body != "denied" {
			t.Fatalf("/api/%s: got %d %q, want 401 denied", section, status, body)
		}
	}
	if authHits.Load() != 3 {
		t.Fatalf("group mw hits = %d, want 3 (one per request)", authHits.Load())
	}
}

// TestGroupNestedMiddlewareOrder: parent group's middleware wraps child
// group's middleware wraps the handler. Outermost-first.
func TestGroupNestedMiddlewareOrder(t *testing.T) {
	var trace []string
	var traceMu sync.Mutex
	record := func(s string) {
		traceMu.Lock()
		trace = append(trace, s)
		traceMu.Unlock()
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("global")
				next(res, req)
			}
		})
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("api")
				next(res, req)
			}
		})
		admin := api.Group("/admin", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				record("admin")
				next(res, req)
			}
		})
		admin.Get("/audit", func(res *gogo.Response, req *gogo.Request) {
			record("handler")
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	httpGet(t, port, "/api/admin/audit")
	traceMu.Lock()
	got := strings.Join(trace, ",")
	traceMu.Unlock()
	if got != "global,api,admin,handler" {
		t.Fatalf("trace: got %q, want global,api,admin,handler", got)
	}
}

// TestRouterUseAddsMW: Router.Use accumulates middleware that wraps later
// routes on that Router but not routes registered before the Use call.
func TestRouterUseAddsMW(t *testing.T) {
	var hits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api")
		api.Get("/early", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "early")
		})
		api.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				hits.Add(1)
				next(res, req)
			}
		})
		api.Get("/late", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "late")
		})
	})
	defer teardown()

	httpGet(t, port, "/api/early")
	if hits.Load() != 0 {
		t.Fatalf("mw ran for /api/early registered before Use: hits=%d", hits.Load())
	}
	httpGet(t, port, "/api/late")
	if hits.Load() != 1 {
		t.Fatalf("mw not invoked for /api/late: hits=%d", hits.Load())
	}
}

// TestGroupUseAsyncOnGetAsync: Router.UseAsync wraps GetAsync handlers
// registered through the router and passes Locals from middleware to handler.
func TestGroupUseAsyncOnGetAsync(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api")
		api.UseAsync(func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				if req.Header("authorization") == "" {
					res.Send(401, "text/plain", "no token")
					return
				}
				req.SetLocal("user", "alice")
				next(res, req)
			}
		})
		api.GetAsync("/me", func(res *gogo.Response, req *gogo.Request) {
			user, _ := req.Local("user").(string)
			res.Send(200, "text/plain", "hi "+user)
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/api/me")
	if status != 401 || body != "no token" {
		t.Fatalf("/api/me unauth: got %d %q", status, body)
	}

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/me", port), nil)
	req.Header.Set("Authorization", "Bearer x")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("/api/me authed: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "hi alice" {
		t.Fatalf("/api/me authed: got %d %q", resp.StatusCode, string(b))
	}
}

// TestGroupPostAsyncBodyAndMW: Router.PostAsync collects the body, enforces
// the cap, and runs through both sync group MW and the body handler.
func TestGroupPostAsyncBodyAndMW(t *testing.T) {
	var mwHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				mwHits.Add(1)
				next(res, req)
			}
		})
		api.PostAsync("/echo", 16, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", "echo:"+string(body))
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Post(
		fmt.Sprintf("http://127.0.0.1:%d/api/echo", port),
		"text/plain", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("POST /api/echo: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "echo:hello" {
		t.Fatalf("POST /api/echo: got %d %q", resp.StatusCode, string(b))
	}
	if mwHits.Load() != 1 {
		t.Fatalf("group mw hits = %d, want 1", mwHits.Load())
	}

	// Body too large → 413 from framework, handler not called.
	resp, err = noKeepaliveClient.Post(
		fmt.Sprintf("http://127.0.0.1:%d/api/echo", port),
		"text/plain", bytes.NewReader(bytes.Repeat([]byte("x"), 100)))
	if err != nil {
		t.Fatalf("POST /api/echo large: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Fatalf("POST /api/echo large: got %d, want 413", resp.StatusCode)
	}
}

func TestGroupBodyAsyncMethodHelpersAndMW(t *testing.T) {
	var syncHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				syncHits.Add(1)
				res.Header("X-Group-MW", "sync")
				next(res, req)
			}
		})
		api.UseAsync(func(next gogo.AsyncHandler) gogo.AsyncHandler {
			return func(res *gogo.Response, req *gogo.Request) {
				req.SetLocal("group-body", string(req.Body()))
				next(res, req)
			}
		})

		handler := func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.Send(200, "text/plain", fmt.Sprintf("%s|%s|%s|%v",
				req.Method(), req.Param("id"), string(body), req.Local("group-body")))
		}
		api.PutAsync("/put/:id", 64, handler)
		api.PatchAsync("/patch/:id", 64, handler)
		api.DeleteAsync("/delete/:id", 64, handler)
	})
	defer teardown()

	for _, tc := range []struct {
		method string
		path   string
		body   string
		want   string
	}{
		{"PUT", "/api/put/7", "replace", "put|7|replace|replace"},
		{"PATCH", "/api/patch/7", "patch", "patch|7|patch|patch"},
		{"DELETE", "/api/delete/7", "delete", "delete|7|delete|delete"},
	} {
		status, body, header := httpBody(t, port, tc.method, tc.path, "text/plain", []byte(tc.body))
		if status != 200 || body != tc.want {
			t.Fatalf("%s %s: got %d %q, want 200 %q", tc.method, tc.path, status, body, tc.want)
		}
		if header.Get("X-Group-MW") != "sync" {
			t.Fatalf("%s %s: X-Group-MW=%q, want sync", tc.method, tc.path, header.Get("X-Group-MW"))
		}
	}
	if got := syncHits.Load(); got != 3 {
		t.Fatalf("sync group middleware hits = %d, want 3", got)
	}
}

// TestGroupGetAsyncFastPathWithoutMW confirms that GetAsync registered through
// a Group with NO middleware still uses the zero-cgo shared-memory dispatch
// path (no perf regression for the common case).
func TestGroupGetAsyncFastPathWithoutMW(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api")
		api.GetAsync("/work", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "done")
		})
	})
	defer teardown()

	for i := 0; i < 5; i++ {
		if status, body := httpGet(t, port, "/api/work"); status != 200 || body != "done" {
			t.Fatalf("/api/work iter %d: got %d %q", i, status, body)
		}
	}
}

// TestGroupStaticReplyBypassesMWOnlyIfNoneRegistered: Reply / string / []byte
// targets on a Router with NO middleware take the zero-cgo static path;
// when MW is registered on the Router or App, the static body is wrapped
// in a dynamic handler so middleware can run.
func TestGroupStaticReplyWithoutMW(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api")
		api.Get("/health", gogo.Reply{Body: "ok"})
	})
	defer teardown()

	if status, body := httpGet(t, port, "/api/health"); status != 200 || body != "ok" {
		t.Fatalf("/api/health: got %d %q", status, body)
	}
}

func TestGroupStaticReplyWithMW(t *testing.T) {
	var hits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				hits.Add(1)
				next(res, req)
			}
		})
		api.Get("/health", gogo.Reply{Status: 201, ContentType: "text/plain", Body: "ok"})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/api/health", port))
	if err != nil {
		t.Fatalf("/api/health: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 201 || string(b) != "ok" {
		t.Fatalf("/api/health: got %d %q", resp.StatusCode, string(b))
	}
	if hits.Load() != 1 {
		t.Fatalf("group mw hits for static target = %d, want 1", hits.Load())
	}
}

func TestGroupStaticReplyWithAppMiddleware(t *testing.T) {
	var hits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				hits.Add(1)
				res.Header("X-App-MW", "hit")
				next(res, req)
			}
		})
		app.Group("/api").Get("/health", "ok")
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/api/health", port))
	if err != nil {
		t.Fatalf("/api/health: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "ok" {
		t.Fatalf("/api/health: got %d %q", resp.StatusCode, body)
	}
	if resp.Header.Get("X-App-MW") != "hit" {
		t.Fatalf("group static target bypassed app middleware header")
	}
	if hits.Load() != 1 {
		t.Fatalf("app middleware hits for group static target = %d, want 1", hits.Load())
	}
}

// TestGroupPrefixNormalization: Group("/api/") and Group("/api") behave the
// same; Group("/") is equivalent to no prefix; wildcards in Group prefix panic.
func TestGroupPrefixNormalization(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		// Trailing slash stripped.
		app.Group("/api/").Get("/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "users")
		})
		// Group("/") acts as global scope (no prefix).
		app.Group("/").Get("/healthz", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "alive")
		})
	})
	defer teardown()

	if status, body := httpGet(t, port, "/api/users"); status != 200 || body != "users" {
		t.Fatalf("/api/users: got %d %q", status, body)
	}
	if status, body := httpGet(t, port, "/healthz"); status != 200 || body != "alive" {
		t.Fatalf("/healthz: got %d %q", status, body)
	}
}

func TestGroupPrefixRejectsWildcard(t *testing.T) {
	app, err := gogo.NewApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	cases := []string{"/api/*", "/api/**", "/*"}
	for _, p := range cases {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("Group(%q) did not panic", p)
				}
			}()
			_ = app.Group(p)
		}()
	}
}

func TestNewAppAllowsZeroOrOneConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  []gogo.Config
	}{
		{name: "default"},
		{name: "one config", cfg: []gogo.Config{{BindAddr: "127.0.0.1"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app, err := gogo.NewApp(tc.cfg...)
			if err != nil {
				t.Fatalf("NewApp: %v", err)
			}
			app.Close()
		})
	}
}

// TestGroupGlobalUseStillWraps: a global App.Use ALWAYS wraps routes
// registered via a Group, regardless of the group's prefix.
func TestGroupGlobalUseStillWraps(t *testing.T) {
	var globalHits, groupHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				globalHits.Add(1)
				next(res, req)
			}
		})
		api := app.Group("/api", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				groupHits.Add(1)
				next(res, req)
			}
		})
		api.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "x")
		})
	})
	defer teardown()

	httpGet(t, port, "/api/x")
	if globalHits.Load() != 1 || groupHits.Load() != 1 {
		t.Fatalf("hits global=%d group=%d, want 1 each", globalHits.Load(), groupHits.Load())
	}
}

// -----------------------------------------------------------------------------
// P0 security-hardening tests (P0-1, P0-2, P0-4, P0-6, P0-9 from ROADMAP).
// -----------------------------------------------------------------------------

// startAppCfg mirrors startApp but lets the caller pass an explicit
// gogo.Config (BodyLimit, BindAddr, …).
func startAppCfg(t *testing.T, cfg gogo.Config, configure func(app *gogo.App)) (port int, teardown func()) {
	t.Helper()
	if cfg.BindAddr == "" {
		cfg.BindAddr = "127.0.0.1"
	}
	port = freePort(t)
	ready := make(chan *gogo.App, 1)
	listenErr := make(chan error, 1)
	runDone := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		app, err := gogo.NewApp(cfg)
		if err != nil {
			listenErr <- fmt.Errorf("NewApp: %w", err)
			close(runDone)
			return
		}
		configure(app)
		if !app.Listen(port) {
			listenErr <- fmt.Errorf("Listen :%d failed", port)
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
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("app setup timed out")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		bind := cfg.BindAddr
		if bind == "" {
			bind = "127.0.0.1"
		}
		c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", bind, port), 50*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	teardown = func() {
		app.Shutdown()
		select {
		case <-runDone:
		case <-time.After(10 * time.Second):
			t.Errorf("app.Run did not exit after Shutdown")
		}
	}
	return port, teardown
}

// TestReplyContentTypeRejectsCRLF: passing CRLF into Reply.ContentType
// must panic at App.Get registration, matching the dynamic-header
// validation that already rejects this.
func TestReplyContentTypeRejectsCRLF(t *testing.T) {
	app, err := gogo.NewApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Reply{ContentType: …\\r\\n…} did not panic")
		}
	}()
	app.Get("/x", gogo.Reply{
		ContentType: "text/plain\r\nX-Injected: evil",
		Body:        "ok",
	})
}

// TestRouterReplyContentTypeRejectsCRLF: same check on the Router.Get
// path, which has its own Reply branch.
func TestRouterReplyContentTypeRejectsCRLF(t *testing.T) {
	app, err := gogo.NewApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Router.Get(Reply{CT:…\\r\\n…}) did not panic")
		}
	}()
	app.Group("/api").Get("/x", gogo.Reply{
		ContentType: "text/plain\r\nX-Injected: 1",
		Body:        "ok",
	})
}

// TestJSONMarshalErrorDoesNotLeak: marshalling an unsupported value
// (a channel) must end with a generic 500 and reach the panic handler;
// the wire body must not include Go-internal text like "json marshal".
func TestJSONMarshalErrorDoesNotLeak(t *testing.T) {
	var captured atomic.Pointer[string]
	gogo.SetPanicHandler(func(rec any) {
		s := fmt.Sprintf("%v", rec)
		captured.Store(&s)
	})
	defer gogo.SetPanicHandler(nil)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/bad", func(res *gogo.Response, req *gogo.Request) {
			ch := make(chan int)
			res.JSON(200, ch) // json.Marshal returns UnsupportedTypeError
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/bad")
	if status != 500 {
		t.Fatalf("status: got %d, want 500", status)
	}
	if strings.Contains(strings.ToLower(body), "json") || strings.Contains(strings.ToLower(body), "unsupported") {
		t.Fatalf("body leaks marshal error detail: %q", body)
	}
	// Server-side report should have fired with the underlying error.
	got := captured.Load()
	if got == nil {
		t.Fatal("panic handler did not see the marshal error")
	}
	if !strings.Contains(*got, "Response.JSON Config.JSONEncoder") && !strings.Contains(*got, "unsupported") {
		t.Fatalf("panic handler payload: %q", *got)
	}
}

// TestCookieValueStripQuotes: Cookie("k") returns the unquoted value
// for `k="v"` (RFC 6265 §5.2). Unquoted and quoted-with-internal-text
// should both work.
func TestCookieValueStripQuotes(t *testing.T) {
	var captured atomic.Pointer[string]
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/cookie", func(res *gogo.Response, req *gogo.Request) {
			v := req.Cookie("session")
			captured.Store(&v)
			res.Send(200, "text/plain", v)
		})
	})
	defer teardown()

	for _, tc := range []struct{ header, want string }{
		{`session=plain`, "plain"},
		{`session="quoted"`, "quoted"},
		{`other=x; session="quoted"; more=y`, "quoted"},
		{`session=""`, ""}, // matched empty quotes
		{`session="`, `"`}, // single open quote not stripped
	} {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/cookie", port), nil)
		req.Header.Set("Cookie", tc.header)
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("GET cookie=%q: %v", tc.header, err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(b) != tc.want {
			t.Errorf("cookie=%q: got %q, want %q", tc.header, string(b), tc.want)
		}
	}
}

// TestBodyLimitContentLengthRejected: POST with Content-Length above
// the app's BodyLimit must get a 413 from the C++ pre-check; the Go
// handler must not be invoked.
func TestBodyLimitContentLengthRejected(t *testing.T) {
	var handlerHits atomic.Int32
	port, teardown := startAppCfg(t, gogo.Config{BodyLimit: 1024}, func(app *gogo.App) {
		app.Post("/upload", func(res *gogo.Response, req *gogo.Request) {
			handlerHits.Add(1)
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	// Exactly at the limit → handler runs.
	resp, err := noKeepaliveClient.Post(
		fmt.Sprintf("http://127.0.0.1:%d/upload", port),
		"text/plain", bytes.NewReader(bytes.Repeat([]byte("x"), 1024)))
	if err != nil {
		t.Fatalf("at-limit POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("at-limit POST: got %d, want 200", resp.StatusCode)
	}
	if handlerHits.Load() != 1 {
		t.Fatalf("handler hits at limit = %d, want 1", handlerHits.Load())
	}

	// One byte over → 413, handler must NOT be invoked.
	resp, err = noKeepaliveClient.Post(
		fmt.Sprintf("http://127.0.0.1:%d/upload", port),
		"text/plain", bytes.NewReader(bytes.Repeat([]byte("x"), 1025)))
	if err != nil {
		t.Fatalf("over-limit POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Fatalf("over-limit POST: got %d, want 413", resp.StatusCode)
	}
	if handlerHits.Load() != 1 {
		t.Fatalf("handler ran for over-limit POST: hits=%d, want 1", handlerHits.Load())
	}
}

// TestBodyLimitChunkedRejected: a chunked-transfer-encoded POST that
// streams more than the configured BodyLimit must get a 413 from the
// Go-side OnData accumulator. The C++ pre-check only sees Content-
// Length; chunked requests slip past it, so without this gate a
// handler that called OnData against a malicious unbounded chunked
// upload would accumulate bytes forever.
//
// Drive via raw TCP since net/http buffers chunked bodies and may
// add its own Content-Length.
func TestBodyLimitChunkedRejected(t *testing.T) {
	var handlerHits atomic.Int32
	var dataBytes atomic.Int64
	port, teardown := startAppCfg(t, gogo.Config{BodyLimit: 1024}, func(app *gogo.App) {
		app.Post("/upload", func(res *gogo.Response, req *gogo.Request) {
			handlerHits.Add(1)
			res.OnData(func(chunk []byte, isLast bool) {
				dataBytes.Add(int64(len(chunk)))
				if isLast {
					res.Send(200, "text/plain", "ok")
				}
			})
		})
	})
	defer teardown()

	// Build a chunked-encoded POST that streams 4096 bytes total
	// (4× the cap). Each chunk is 512 bytes so we cross the
	// boundary mid-stream.
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	requestHead := "POST /upload HTTP/1.1\r\n" +
		fmt.Sprintf("Host: 127.0.0.1:%d\r\n", port) +
		"Content-Type: application/octet-stream\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"Connection: close\r\n\r\n"
	if _, err := conn.Write([]byte(requestHead)); err != nil {
		t.Fatalf("write head: %v", err)
	}
	chunk := bytes.Repeat([]byte("x"), 512)
	for i := 0; i < 8; i++ {
		// chunk-size hex CRLF chunk-data CRLF
		if _, err := fmt.Fprintf(conn, "%x\r\n", len(chunk)); err != nil {
			break
		}
		if _, err := conn.Write(chunk); err != nil {
			break
		}
		if _, err := conn.Write([]byte("\r\n")); err != nil {
			break
		}
	}
	// trailing zero-length chunk to terminate
	_, _ = conn.Write([]byte("0\r\n\r\n"))

	// Read response.
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Errorf("chunked over-limit: got %d, want 413", resp.StatusCode)
	}
	// Verify the handler stopped receiving data well before the
	// full 4 KiB arrived — at most one cap-worth plus the partial
	// chunk that tripped the gate.
	if dataBytes.Load() > 2*1024 {
		t.Errorf("OnData kept accepting bytes past the limit: got %d", dataBytes.Load())
	}
	_ = handlerHits.Load()
}

// TestBodyLimitChunkedUnderLimit: a chunked upload that stays under
// the cap completes normally and the handler sees the full body.
func TestBodyLimitChunkedUnderLimit(t *testing.T) {
	var dataBytes atomic.Int64
	const limit = 4096
	port, teardown := startAppCfg(t, gogo.Config{BodyLimit: limit}, func(app *gogo.App) {
		app.Post("/upload", func(res *gogo.Response, req *gogo.Request) {
			res.OnData(func(chunk []byte, isLast bool) {
				dataBytes.Add(int64(len(chunk)))
				if isLast {
					res.Send(200, "text/plain", "ok")
				}
			})
		})
	})
	defer teardown()

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	requestHead := "POST /upload HTTP/1.1\r\n" +
		fmt.Sprintf("Host: 127.0.0.1:%d\r\n", port) +
		"Content-Type: application/octet-stream\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"Connection: close\r\n\r\n"
	if _, err := conn.Write([]byte(requestHead)); err != nil {
		t.Fatalf("write head: %v", err)
	}
	// 4 chunks of 512 bytes = 2 KiB total, well under the 4 KiB cap.
	chunk := bytes.Repeat([]byte("x"), 512)
	for i := 0; i < 4; i++ {
		fmt.Fprintf(conn, "%x\r\n", len(chunk))
		conn.Write(chunk)
		conn.Write([]byte("\r\n"))
	}
	conn.Write([]byte("0\r\n\r\n"))

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("under-limit chunked POST: got %d, want 200", resp.StatusCode)
	}
	if dataBytes.Load() != 2048 {
		t.Errorf("OnData total bytes: got %d, want 2048", dataBytes.Load())
	}
}

// TestBodyLimitChunkedBodyClampedByConfig: when the handler asks
// Body(maxBytes, ...) for a value LARGER than the app's BodyLimit,
// the effective cap is BodyLimit — the app-wide ceiling wins. Tests
// the clamp on chunked uploads, where there's no Content-Length for
// the C++ pre-check to catch.
//
// Without the clamp, a handler that asked res.Body(10<<20, ...) on
// an app with BodyLimit=1024 could accept a multi-MiB chunked
// upload — the OnData accumulator wraps res.OnData, not the
// internal res.Body path, so the app cap was bypassable.
func TestBodyLimitChunkedBodyClampedByConfig(t *testing.T) {
	const bodyLimit = 1024
	port, teardown := startAppCfg(t, gogo.Config{BodyLimit: bodyLimit}, func(app *gogo.App) {
		// Handler asks for 10 MiB — bigger than the app cap. The
		// clamp must kick in and reject at bodyLimit instead.
		app.Post("/upload", func(res *gogo.Response, req *gogo.Request) {
			res.Body(10<<20, func(body []byte, err error) {
				if err != nil {
					res.Send(413, "text/plain", err.Error())
					return
				}
				res.Send(200, "text/plain", "ok")
			})
		})
	})
	defer teardown()

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	requestHead := "POST /upload HTTP/1.1\r\n" +
		fmt.Sprintf("Host: 127.0.0.1:%d\r\n", port) +
		"Content-Type: application/octet-stream\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"Connection: close\r\n\r\n"
	if _, err := conn.Write([]byte(requestHead)); err != nil {
		t.Fatalf("write head: %v", err)
	}
	// 4 KiB total via 512-byte chunks — handler's 10 MiB cap would
	// happily accept this, but the app's 1 KiB cap must reject.
	chunk := bytes.Repeat([]byte("x"), 512)
	for i := 0; i < 8; i++ {
		fmt.Fprintf(conn, "%x\r\n", len(chunk))
		conn.Write(chunk)
		conn.Write([]byte("\r\n"))
	}
	conn.Write([]byte("0\r\n\r\n"))

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Errorf("chunked body via Body(10MiB) on app with BodyLimit=1024: got %d, want 413", resp.StatusCode)
	}
}

// TestBodyLimitChunkedBodyHandlerCapWinsWhenStricter: when the
// handler's maxBytes is SMALLER than the app's BodyLimit, the
// handler-supplied cap wins (i.e. the stricter of the two always
// applies). Locks in the min() semantics in both directions.
func TestBodyLimitChunkedBodyHandlerCapWinsWhenStricter(t *testing.T) {
	const bodyLimit = 1 << 20 // 1 MiB app cap
	const handlerCap = 512    // way smaller — handler wants tight bound

	port, teardown := startAppCfg(t, gogo.Config{BodyLimit: bodyLimit}, func(app *gogo.App) {
		app.Post("/upload", func(res *gogo.Response, req *gogo.Request) {
			res.Body(handlerCap, func(body []byte, err error) {
				if err != nil {
					res.Send(413, "text/plain", err.Error())
					return
				}
				res.Send(200, "text/plain", "ok")
			})
		})
	})
	defer teardown()

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	requestHead := "POST /upload HTTP/1.1\r\n" +
		fmt.Sprintf("Host: 127.0.0.1:%d\r\n", port) +
		"Content-Type: application/octet-stream\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"Connection: close\r\n\r\n"
	conn.Write([]byte(requestHead))
	// 1 KiB body — under app cap, over handler cap.
	chunk := bytes.Repeat([]byte("x"), 512)
	for i := 0; i < 2; i++ {
		fmt.Fprintf(conn, "%x\r\n", len(chunk))
		conn.Write(chunk)
		conn.Write([]byte("\r\n"))
	}
	conn.Write([]byte("0\r\n\r\n"))

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Errorf("handler's tighter cap should reject: got %d, want 413", resp.StatusCode)
	}
}

// TestBodyLimitChunkedBodyZeroMaxIsLiteral: Body(0, ...) is taken
// literally — accept zero bytes, reject anything larger — even when
// the app's BodyLimit is positive. The clamp is one-directional;
// the app cap can tighten, never loosen, the caller's request.
//
// The first iteration of the clamp accidentally treated maxBytes <= 0
// as "use app cap instead", which would have flipped a handler that
// explicitly asked for 0 bytes into accepting up to 1 MiB. This test
// locks the literal-zero semantics in.
func TestBodyLimitChunkedBodyZeroMaxIsLiteral(t *testing.T) {
	const appCap = 1 << 20 // generous app cap — handler asks for tighter
	port, teardown := startAppCfg(t, gogo.Config{BodyLimit: appCap}, func(app *gogo.App) {
		app.Post("/empty", func(res *gogo.Response, req *gogo.Request) {
			res.Body(0, func(body []byte, err error) {
				if err != nil {
					res.Send(413, "text/plain", err.Error())
					return
				}
				res.Send(200, "text/plain", fmt.Sprintf("got %d bytes", len(body)))
			})
		})
	})
	defer teardown()

	// A single byte must be rejected — Body(0) means 0 means 0.
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	fmt.Fprintf(conn, "POST /empty HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n", port)
	fmt.Fprintf(conn, "Content-Type: application/octet-stream\r\n")
	fmt.Fprintf(conn, "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
	// One single-byte chunk + terminator.
	conn.Write([]byte("1\r\nx\r\n0\r\n\r\n"))

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Errorf("Body(0) accepted a 1-byte payload: got %d, want 413", resp.StatusCode)
	}
}

// TestOnDataPreservesUserOnAborted: Response.OnData installs its own abort
// callback to release the pinned response ref on early disconnect, but it must
// not overwrite a user callback registered through Response.OnAborted. uWS only
// stores one native onAborted handler, so the framework has to multiplex
// Go-side callbacks.
func TestOnDataPreservesUserOnAborted(t *testing.T) {
	started := make(chan *gogo.Aborted, 1)
	port, teardown := startAppCfg(t, gogo.Config{BodyLimit: 1 << 20}, func(app *gogo.App) {
		app.Post("/upload", func(res *gogo.Response, req *gogo.Request) {
			aborted := res.OnAborted()
			res.OnData(func(chunk []byte, isLast bool) {})
			started <- aborted
		})
	})
	defer teardown()

	aborted := openChunkedUploadAndAbort(t, port, "/upload", started)
	waitForAbortFlag(t, aborted)
}

// TestBodyPreservesUserOnAborted is the same overwrite guard for Response.Body,
// which also needs an internal abort callback to release its body-collection
// ref when the client disconnects before isLast.
func TestBodyPreservesUserOnAborted(t *testing.T) {
	started := make(chan *gogo.Aborted, 1)
	bodyDone := make(chan error, 1)
	port, teardown := startAppCfg(t, gogo.Config{BodyLimit: 1 << 20}, func(app *gogo.App) {
		app.Post("/upload", func(res *gogo.Response, req *gogo.Request) {
			aborted := res.OnAborted()
			res.Body(1<<20, func(body []byte, err error) {
				bodyDone <- err
			})
			started <- aborted
		})
	})
	defer teardown()

	aborted := openChunkedUploadAndAbort(t, port, "/upload", started)
	waitForAbortFlag(t, aborted)
	select {
	case err := <-bodyDone:
		t.Fatalf("Body callback ran after client abort: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

func openChunkedUploadAndAbort(t *testing.T, port int, path string, started <-chan *gogo.Aborted) *gogo.Aborted {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n", path, port)
	fmt.Fprintf(conn, "Content-Type: application/octet-stream\r\n")
	fmt.Fprintf(conn, "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
	_, _ = conn.Write([]byte("1\r\nx\r\n"))

	var aborted *gogo.Aborted
	select {
	case aborted = <-started:
	case <-time.After(2 * time.Second):
		conn.Close()
		t.Fatal("handler did not start")
	}
	conn.Close()
	return aborted
}

func waitForAbortFlag(t *testing.T, aborted *gogo.Aborted) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if aborted.Load() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("user OnAborted flag was not set after client abort")
}

// TestBindAddrLocalhost: when Config.BindAddr is set to 127.0.0.1, the
// listener is reachable on loopback. (We can't reliably test refusal on
// a non-loopback IP without knowing the box's external addresses; the
// loopback-reach assertion is the practical signal we care about.)
func TestBindAddrLocalhost(t *testing.T) {
	port, teardown := startAppCfg(t, gogo.Config{BindAddr: "127.0.0.1"}, func(app *gogo.App) {
		app.Get("/ok", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "loopback")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/ok")
	if status != 200 || body != "loopback" {
		t.Fatalf("/ok via 127.0.0.1: got %d %q", status, body)
	}
}

// TestWebSocketBehaviorAcceptsLimits: the new limit fields on
// WebSocketBehavior should at least register without crashing. Full
// runtime enforcement is uWS-side; we just confirm the bridge accepts
// the values.
func TestWebSocketBehaviorAcceptsLimits(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			MaxPayloadLength: 4096,
			IdleTimeout:      30 * time.Second,
			MaxBackpressure:  8192,
			DisablePings:     true,
		})
		app.Get("/ping", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "pong")
		})
	})
	defer teardown()

	// HTTP endpoint should still work after WS registration.
	if status, body := httpGet(t, port, "/ping"); status != 200 || body != "pong" {
		t.Fatalf("/ping after WS register: got %d %q", status, body)
	}
}

// TestRequestIntrospection covers the P1 request helpers — IP, IPs,
// Hostname, Get, Protocol, Secure — across sync, async, and the
// shared-dispatch path. IP comes from the loopback peer (127.0.0.1)
// in the test harness; IPs is X-Forwarded-For; Hostname comes from
// the Host header (port stripped).
//
// CapturePeerIP is enabled so async / shared paths populate the IP
// snapshot. The default (off) is exercised by TestPeerIPDefaultOff.
func TestRequestIntrospection(t *testing.T) {
	type captured struct {
		ip       string
		ips      []string
		host     string
		get      string
		protocol string
		secure   bool
	}
	var sync, async, shared atomic.Pointer[captured]
	// TrustProxy enables IPs() — without it the helper deliberately
	// returns nil since the X-Forwarded-For header is attacker-controlled
	// on an internet-facing server.
	port, teardown := startAppCfg(t, gogo.Config{CapturePeerIP: true, TrustProxy: true}, func(app *gogo.App) {
		app.Get("/sync", func(res *gogo.Response, req *gogo.Request) {
			sync.Store(&captured{
				ip:       req.IP(),
				ips:      req.IPs(),
				host:     req.Hostname(),
				get:      req.Get("x-custom"),
				protocol: req.Protocol(),
				secure:   req.Secure(),
			})
			res.Send(200, "text/plain", "sync")
		})
		// Force the sync-wrapper async path by attaching a middleware.
		app.Use("/async/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) { next(res, req) }
		})
		app.GetAsync("/async/snap", func(res *gogo.Response, req *gogo.Request) {
			async.Store(&captured{
				ip:       req.IP(),
				ips:      req.IPs(),
				host:     req.Hostname(),
				get:      req.Get("x-custom"),
				protocol: req.Protocol(),
				secure:   req.Secure(),
			})
			res.Send(200, "text/plain", "async")
		})
		// No middleware → shared-dispatch zero-cgo path.
		app.GetAsync("/shared", func(res *gogo.Response, req *gogo.Request) {
			shared.Store(&captured{
				ip:       req.IP(),
				ips:      req.IPs(),
				host:     req.Hostname(),
				get:      req.Get("x-custom"),
				protocol: req.Protocol(),
				secure:   req.Secure(),
			})
			res.Send(200, "text/plain", "shared")
		})
	})
	defer teardown()

	doRequest := func(path string) {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d%s", port, path), nil)
		req.Header.Set("Host", fmt.Sprintf("api.example:%d", port))
		req.Header.Set("X-Forwarded-For", "203.0.113.5, 198.51.100.7:443")
		req.Header.Set("X-Custom", "hi")
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
	}
	doRequest("/sync")
	doRequest("/async/snap")
	doRequest("/shared")

	check := func(name string, c *captured) {
		t.Helper()
		if c == nil {
			t.Fatalf("%s handler did not record", name)
		}
		if c.ip == "" || (!strings.HasPrefix(c.ip, "127.0.0.1") && c.ip != "::1") {
			t.Errorf("%s: ip=%q, want loopback", name, c.ip)
		}
		// Go's http client overrides Host from the URL; just verify a
		// non-empty hostname with no ":port".
		if c.host == "" || strings.ContainsRune(c.host, ':') {
			t.Errorf("%s: host=%q, want hostname with port stripped", name, c.host)
		}
		if c.get != "hi" {
			t.Errorf("%s: Get(x-custom)=%q, want hi", name, c.get)
		}
		if c.protocol != "http" {
			t.Errorf("%s: protocol=%q, want http", name, c.protocol)
		}
		if c.secure {
			t.Errorf("%s: secure=true on plaintext app", name)
		}
		if len(c.ips) != 2 || c.ips[0] != "203.0.113.5" || c.ips[1] != "198.51.100.7" {
			t.Errorf("%s: ips=%v, want [203.0.113.5 198.51.100.7]", name, c.ips)
		}
	}
	check("sync", sync.Load())
	check("async-snap", async.Load())
	check("shared", shared.Load())
}

// TestPeerIPDefaultOff: with the default Config (CapturePeerIP=false),
// shared-dispatch and sync-wrapper-async handlers see req.IP() == "".
// Sync handlers still get the live IP — their lookup goes through the
// res pointer directly and isn't tied to the snapshot.
func TestPeerIPDefaultOff(t *testing.T) {
	var syncIP, asyncIP, sharedIP atomic.Pointer[string]
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/sync", func(res *gogo.Response, req *gogo.Request) {
			ip := req.IP()
			syncIP.Store(&ip)
			res.Send(200, "text/plain", "ok")
		})
		app.Use("/snap/*", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) { next(res, req) }
		})
		app.GetAsync("/snap/x", func(res *gogo.Response, req *gogo.Request) {
			ip := req.IP()
			asyncIP.Store(&ip)
			res.Send(200, "text/plain", "ok")
		})
		app.GetAsync("/shared", func(res *gogo.Response, req *gogo.Request) {
			ip := req.IP()
			sharedIP.Store(&ip)
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	httpGet(t, port, "/sync")
	httpGet(t, port, "/snap/x")
	httpGet(t, port, "/shared")

	if v := syncIP.Load(); v == nil || *v == "" {
		t.Errorf("sync handler IP empty with CapturePeerIP=false; live res lookup should still work, got %v", v)
	}
	if v := asyncIP.Load(); v == nil || *v != "" {
		t.Errorf("async (snapshot) IP should be empty by default, got %q", *v)
	}
	if v := sharedIP.Load(); v == nil || *v != "" {
		t.Errorf("shared-dispatch IP should be empty by default, got %q", *v)
	}
}

// TestIPsRequiresTrustProxy asserts that Request.IPs() returns nil
// unless Config.TrustProxy is true — the X-Forwarded-For header is
// attacker-controlled when the server faces the public internet
// directly, so exposing it via a typed helper would let any client
// spoof their apparent identity to a logger or rate limiter.
func TestIPsRequiresTrustProxy(t *testing.T) {
	type result struct {
		ips []string
	}
	var captured atomic.Pointer[result]

	// CapturePeerIP enabled so the snapshot path has something to
	// read; TrustProxy intentionally left at false (the default).
	port, teardown := startAppCfg(t, gogo.Config{CapturePeerIP: true}, func(app *gogo.App) {
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			captured.Store(&result{ips: req.IPs()})
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
	req.Header.Set("X-Forwarded-For", "10.0.0.1, 10.0.0.2")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()

	got := captured.Load()
	if got == nil {
		t.Fatal("handler did not record IPs result")
	}
	if got.ips != nil {
		t.Errorf("IPs() returned %v with TrustProxy=false; want nil so spoofed X-Forwarded-For is not exposed", got.ips)
	}
}

func TestIPsNormalizesAndDropsInvalidForwardedEntries(t *testing.T) {
	type result struct {
		ips []string
	}
	var captured atomic.Pointer[result]

	port, teardown := startAppCfg(t, gogo.Config{CapturePeerIP: true, TrustProxy: true}, func(app *gogo.App) {
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			captured.Store(&result{ips: req.IPs()})
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/", port), nil)
	req.Header.Set("X-Forwarded-For", "bad, 203.0.113.5:443, [2001:db8::1]:8443, ::ffff:192.0.2.9")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()

	got := captured.Load()
	if got == nil {
		t.Fatal("handler did not record IPs result")
	}
	want := []string{"203.0.113.5", "2001:db8::1", "192.0.2.9"}
	if !reflect.DeepEqual(got.ips, want) {
		t.Errorf("IPs() = %v, want %v", got.ips, want)
	}
}

// TestResponseRedirect: status defaults to 302; Location header is set;
// body is empty. CRLF in location is rejected with a 500 (panic-free)
// so a malicious request can't trigger the panic handler on every hit.
func TestResponseRedirect(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/r1", func(res *gogo.Response, req *gogo.Request) {
			res.Redirect("/dest", 0) // → 302
		})
		app.Get("/r2", func(res *gogo.Response, req *gogo.Request) {
			res.Redirect("/perm", 301)
		})
		app.Get("/inject", func(res *gogo.Response, req *gogo.Request) {
			res.Redirect("/x\r\nX-Bad: 1", 302)
		})
	})
	defer teardown()

	// Disable auto-redirect so we can inspect the Location header.
	client := &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/r1", port))
	if err != nil {
		t.Fatalf("/r1: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 302 {
		t.Errorf("/r1: status=%d, want 302", resp.StatusCode)
	}
	if resp.Header.Get("Location") != "/dest" {
		t.Errorf("/r1: Location=%q, want /dest", resp.Header.Get("Location"))
	}

	resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/r2", port))
	if err != nil {
		t.Fatalf("/r2: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 301 || resp.Header.Get("Location") != "/perm" {
		t.Errorf("/r2: got %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}

	resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/inject", port))
	if err != nil {
		t.Fatalf("/inject: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 500 {
		t.Errorf("CRLF injection not rejected with 500: status=%d", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "" {
		t.Errorf("CRLF injection wrote Location header: %q", loc)
	}
	if extra := resp.Header.Get("X-Bad"); extra != "" {
		t.Errorf("CRLF injection smuggled a header: X-Bad=%q", extra)
	}
}

// TestResponseHeaderNameValidation: response handlers that pass a
// malformed header name (CRLF, NUL, colon, empty) panic at the
// validation gate — never reach the wire. The framework's panic
// handler then returns a 500 so the connection drops cleanly instead
// of emitting a corrupted header frame. This guards against:
//
//   - response splitting (CR/LF in header name)
//   - cgo bridge frame corruption (NUL terminator inside a packed
//     name\0value\0 buffer)
//   - malformed wire lines from spaces / colons in user-supplied
//     header names
func TestResponseHeaderNameValidation(t *testing.T) {
	cases := []struct {
		label string
		name  string
	}{
		{"crlf-in-name", "X-Foo\r\nEvil"},
		{"nul-in-name", "X-Foo\x00Bar"},
		{"colon-in-name", "X-Foo:Bar"},
		{"space-in-name", "X-Foo Bar"},
		{"empty-name", ""},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			port, teardown := startApp(t, func(app *gogo.App) {
				app.Get("/", func(res *gogo.Response, req *gogo.Request) {
					res.Header(tc.name, "value")
					res.Send(200, "text/plain", "should not reach the wire")
				})
			})
			defer teardown()

			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != 500 {
				t.Errorf("malformed name %q reached the wire: status=%d", tc.name, resp.StatusCode)
			}
		})
	}
}

// TestNotFoundHandler: customizing the 404 body via App.NotFound. Routes
// the user registers explicitly still win — only unmatched paths fall
// through to the NotFound handler.
func TestNotFoundHandler(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/exists", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "hi")
		})
		app.NotFound(func(res *gogo.Response, req *gogo.Request) {
			res.Send(404, "application/json", `{"err":"not found","path":"`+req.URL()+`"}`)
		})
	})
	defer teardown()

	// Explicit route still 200.
	status, body := httpGet(t, port, "/exists")
	if status != 200 || body != "hi" {
		t.Fatalf("/exists: got %d %q", status, body)
	}

	// Unmatched path → custom 404.
	status, body = httpGet(t, port, "/nope")
	if status != 404 || !strings.Contains(body, `"err":"not found"`) || !strings.Contains(body, `/nope`) {
		t.Fatalf("/nope: got %d %q", status, body)
	}
}

// TestNotFoundWrapsWithGlobalMiddleware: middleware registered before
// Listen wraps the NotFound handler the same as any other route, so a
// global logger / request-ID middleware still observes 404s.
func TestNotFoundWrapsWithGlobalMiddleware(t *testing.T) {
	var mwHits atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				mwHits.Add(1)
				next(res, req)
			}
		})
		app.NotFound(func(res *gogo.Response, req *gogo.Request) {
			res.Send(404, "text/plain", "missing")
		})
	})
	defer teardown()

	if status, body := httpGet(t, port, "/whatever"); status != 404 || body != "missing" {
		t.Fatalf("got %d %q", status, body)
	}
	if mwHits.Load() != 1 {
		t.Fatalf("global mw missed NotFound handler: hits=%d", mwHits.Load())
	}
}

// TestShutdownGracefullyDrainsInFlight: an in-flight slow request must
// complete after the listen socket is closed; new TCP dials after
// the shutdown fail. The graceful timeout cap should be high enough
// that the existing connection wins the race.
func TestShutdownGracefullyDrainsInFlight(t *testing.T) {
	requestStarted := make(chan struct{})
	requestFinish := make(chan struct{})

	port := freePort(t)
	ready := make(chan *gogo.App, 1)
	runDone := make(chan struct{})

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		app, err := gogo.NewApp()
		if err != nil {
			t.Errorf("NewApp: %v", err)
			close(runDone)
			return
		}
		// Async so the loop thread keeps spinning while the handler
		// blocks on requestFinish; a sync handler would freeze the loop
		// and ShutdownGracefully's defer would never run.
		app.GetAsync("/slow", func(res *gogo.Response, req *gogo.Request) {
			close(requestStarted)
			<-requestFinish
			res.Send(200, "text/plain", "done")
		})
		if !app.Listen(port) {
			t.Errorf("Listen :%d failed", port)
			app.Close()
			close(runDone)
			return
		}
		ready <- app
		app.Run()
		app.Close()
		close(runDone)
	}()

	app := <-ready
	// Wait for the listener to accept.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 50*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Start one slow request; wait until the handler is on the wire.
	respCh := make(chan *http.Response, 1)
	go func() {
		resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/slow", port))
		if err != nil {
			t.Errorf("slow GET: %v", err)
			respCh <- nil
			return
		}
		respCh <- resp
	}()
	<-requestStarted

	// Begin graceful shutdown with a generous timeout — the in-flight
	// request should finish well before it fires.
	app.ShutdownGracefully(5 * time.Second)

	// New dials should fail (listen socket closed). Give the loop a
	// beat to process the close.
	time.Sleep(100 * time.Millisecond)
	if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond); err == nil {
		c.Close()
		t.Errorf("new TCP dial succeeded after ShutdownGracefully — listen socket should be closed")
	}

	// Release the in-flight handler and read the response.
	close(requestFinish)
	resp := <-respCh
	if resp == nil {
		t.Fatal("in-flight response lost during graceful shutdown")
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "done" {
		t.Fatalf("in-flight response: got %d %q, want 200 done", resp.StatusCode, string(body))
	}

	// Loop should now exit naturally.
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after graceful drain")
	}
}

// TestShutdownGracefullyForceCloseTimeout: a hung handler that never
// finishes must be force-closed when the timeout fires.
func TestShutdownGracefullyForceCloseTimeout(t *testing.T) {
	port := freePort(t)
	ready := make(chan *gogo.App, 1)
	runDone := make(chan struct{})
	hold := make(chan struct{}) // never closed — handler blocks forever

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		app, err := gogo.NewApp()
		if err != nil {
			t.Errorf("NewApp: %v", err)
			close(runDone)
			return
		}
		// Async, otherwise the sync handler would freeze the loop thread
		// and the force-close timeout would have nothing to fire on.
		app.GetAsync("/hang", func(res *gogo.Response, req *gogo.Request) {
			<-hold
			res.Send(200, "text/plain", "never")
		})
		if !app.Listen(port) {
			t.Errorf("Listen :%d failed", port)
			app.Close()
			close(runDone)
			return
		}
		ready <- app
		app.Run()
		app.Close()
		close(runDone)
	}()

	app := <-ready
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 50*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Fire the hung request and don't wait for it.
	go noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/hang", port))
	time.Sleep(100 * time.Millisecond) // let the handler start

	start := time.Now()
	app.ShutdownGracefully(500 * time.Millisecond)

	select {
	case <-runDone:
		elapsed := time.Since(start)
		if elapsed < 400*time.Millisecond {
			t.Errorf("Run returned before timeout fired: %v", elapsed)
		}
		if elapsed > 2*time.Second {
			t.Errorf("Run returned too late after force timeout: %v", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not exit after force timeout fired")
	}
	close(hold) // unblock any leftover handler goroutine
}

// TestLifecycleHooks: OnListen fires once after Listen succeeds with
// the bound port; OnShutdown fires synchronously when Shutdown is
// invoked. Multiple hooks run in registration order.
func TestLifecycleHooks(t *testing.T) {
	var listenSeen atomic.Pointer[int]
	var shutdownOrder []string
	var shutdownMu sync.Mutex
	recordShutdown := func(name string) {
		shutdownMu.Lock()
		shutdownOrder = append(shutdownOrder, name)
		shutdownMu.Unlock()
	}

	port, teardown := startApp(t, func(app *gogo.App) {
		app.OnListen(func(p int) {
			pp := p
			listenSeen.Store(&pp)
		})
		app.OnShutdown(func() { recordShutdown("first") })
		app.OnShutdown(func() { recordShutdown("second") })
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})

	// OnListen should have fired by the time the listener accepts.
	if p := listenSeen.Load(); p == nil || *p != port {
		t.Fatalf("OnListen port=%v, want %d", p, port)
	}
	if status, _ := httpGet(t, port, "/x"); status != 200 {
		t.Fatalf("post-OnListen request failed: %d", status)
	}

	teardown() // triggers Shutdown

	shutdownMu.Lock()
	got := strings.Join(shutdownOrder, ",")
	shutdownMu.Unlock()
	if got != "first,second" {
		t.Fatalf("OnShutdown order: got %q, want first,second", got)
	}
}

func TestLifecycleHookPanicsAreRecovered(t *testing.T) {
	var panicked atomic.Int32
	gogo.SetPanicHandler(func(recovered any) {
		panicked.Add(1)
	})
	defer gogo.SetPanicHandler(nil)

	var listenAfter atomic.Bool
	var shutdownAfter atomic.Bool
	port, teardown := startApp(t, func(app *gogo.App) {
		app.OnListen(nil)
		app.OnListen(func(int) { panic("listen hook boom") })
		app.OnListen(func(int) { listenAfter.Store(true) })

		app.OnShutdown(nil)
		app.OnShutdown(func() { panic("shutdown hook boom") })
		app.OnShutdown(func() { shutdownAfter.Store(true) })

		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})

	if !listenAfter.Load() {
		t.Fatal("OnListen hook after panic did not run")
	}
	if got := panicked.Load(); got != 1 {
		t.Fatalf("panic handler after OnListen = %d, want 1", got)
	}
	if status, _ := httpGet(t, port, "/x"); status != 200 {
		t.Fatalf("post-OnListen-panic request failed: %d", status)
	}

	teardown()

	if !shutdownAfter.Load() {
		t.Fatal("OnShutdown hook after panic did not run")
	}
	if got := panicked.Load(); got != 2 {
		t.Fatalf("panic handler after OnShutdown = %d, want 2", got)
	}
}

// TestShutdownHooksFireOnGraceful: same hooks fire from
// ShutdownGracefully as from Shutdown.
func TestShutdownHooksFireOnGraceful(t *testing.T) {
	fired := make(chan struct{}, 1)

	port := freePort(t)
	ready := make(chan *gogo.App, 1)
	runDone := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		app, err := gogo.NewApp()
		if err != nil {
			t.Errorf("NewApp: %v", err)
			close(runDone)
			return
		}
		app.OnShutdown(func() { fired <- struct{}{} })
		if !app.Listen(port) {
			t.Errorf("Listen :%d failed", port)
			app.Close()
			close(runDone)
			return
		}
		ready <- app
		app.Run()
		app.Close()
		close(runDone)
	}()
	app := <-ready

	app.ShutdownGracefully(100 * time.Millisecond)

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("OnShutdown hook never fired on graceful path")
	}
	<-runDone
}

func TestShutdownHooksFireOnce(t *testing.T) {
	var fired atomic.Int32

	port, teardown := startApp(t, func(app *gogo.App) {
		app.OnShutdown(func() { fired.Add(1) })
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})

	if status, _ := httpGet(t, port, "/x"); status != 200 {
		t.Fatalf("pre-shutdown request failed: %d", status)
	}

	teardown()

	if got := fired.Load(); got != 1 {
		t.Fatalf("OnShutdown fired %d times after teardown, want 1", got)
	}
}

func TestShutdownHooksFireOnceAcrossGracefulAndImmediate(t *testing.T) {
	var fired atomic.Int32

	port := freePort(t)
	ready := make(chan *gogo.App, 1)
	runDone := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		app, err := gogo.NewApp()
		if err != nil {
			t.Errorf("NewApp: %v", err)
			close(runDone)
			return
		}
		app.OnShutdown(func() { fired.Add(1) })
		if !app.Listen(port) {
			t.Errorf("Listen :%d failed", port)
			app.Close()
			close(runDone)
			return
		}
		ready <- app
		app.Run()
		app.Close()
		close(runDone)
	}()
	app := <-ready

	app.ShutdownGracefully(time.Second)
	app.Shutdown()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after Shutdown")
	}
	if got := fired.Load(); got != 1 {
		t.Fatalf("OnShutdown fired %d times after graceful+immediate shutdown, want 1", got)
	}
}

// TestMethodNotAllowed: a path with registered methods returns 405 +
// Allow header for unregistered methods; the handler can customize the
// response body. Literal, parametric, wildcard, router-prefixed, and
// typed-param routes all participate in the lookup.
func TestMethodNotAllowed(t *testing.T) {
	var notAllowedHits atomic.Int32
	var capturedApp *gogo.App
	port, teardown := startApp(t, func(app *gogo.App) {
		capturedApp = app
		app.Get("/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "list")
		})
		app.Post("/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(201, "text/plain", "create")
		})
		app.Get("/api/:section", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "section="+req.Parameter(0))
		})
		app.Get("/assets/*", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "asset")
		})
		api := app.Group("/v1")
		api.Delete("/items/:id<int>", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "delete-item")
		})
		app.NotFound(func(res *gogo.Response, req *gogo.Request) {
			res.Send(404, "text/plain", "missing:"+req.URL())
		})
		app.MethodNotAllowed(func(res *gogo.Response, req *gogo.Request) {
			notAllowedHits.Add(1)
			// uWS requires status before every header write, so the
			// canonical sequence for a 405 with Allow is
			// Status → Header(Allow) → Header(CT) → End. res.Send
			// would emit a default 200 status line before the
			// user's Allow header.
			allow := strings.Join(capturedApp.AllowedMethods(req.URL()), ", ")
			res.Status(405)
			res.Header("Allow", allow)
			res.Header("Content-Type", "text/plain; charset=utf-8")
			res.End("no:" + req.URL())
		})
	})
	defer teardown()

	doReq := func(method, path string) (int, string, string) {
		t.Helper()
		req, _ := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d%s", port, path), nil)
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, string(body), resp.Header.Get("Allow")
	}

	// Allowed method → 200.
	if status, body := httpGet(t, port, "/users"); status != 200 || body != "list" {
		t.Fatalf("GET /users: got %d %q", status, body)
	}

	// Unregistered method on a literal path → 405 with Allow header.
	status, body, allow := doReq("DELETE", "/users")
	if status != 405 || body != "no:/users" {
		t.Fatalf("DELETE /users: got %d %q", status, body)
	}
	if allow != "GET, POST" {
		t.Errorf("Allow header: got %q, want GET, POST", allow)
	}
	if notAllowedHits.Load() != 1 {
		t.Errorf("MethodNotAllowed hits = %d, want 1", notAllowedHits.Load())
	}

	// Unknown path → NotFound handler.
	status, body = httpGet(t, port, "/nope")
	if status != 404 || body != "missing:/nope" {
		t.Fatalf("/nope: got %d %q", status, body)
	}

	// Parametric path with wrong method → 405.
	status, body, allow = doReq("DELETE", "/api/admin")
	if status != 405 || body != "no:/api/admin" {
		t.Fatalf("DELETE /api/admin: got %d %q, want 405 no:/api/admin", status, body)
	}
	if allow != "GET" {
		t.Errorf("parametric Allow: got %q, want GET", allow)
	}

	// Terminal wildcard path with wrong method → 405.
	status, body, allow = doReq("POST", "/assets/css/site.css")
	if status != 405 || body != "no:/assets/css/site.css" {
		t.Fatalf("POST /assets/css/site.css: got %d %q, want 405 wildcard", status, body)
	}
	if allow != "GET" {
		t.Errorf("wildcard Allow: got %q, want GET", allow)
	}

	// uWS wildcard semantics require a segment at the wildcard position.
	status, body, _ = doReq("POST", "/assets")
	if status != 404 || body != "missing:/assets" {
		t.Fatalf("POST /assets: got %d %q, want 404 missing:/assets", status, body)
	}

	// Router-prefixed typed route: valid typed path participates in 405.
	status, body, allow = doReq("PATCH", "/v1/items/42")
	if status != 405 || body != "no:/v1/items/42" {
		t.Fatalf("PATCH /v1/items/42: got %d %q, want 405 typed route", status, body)
	}
	if allow != "DELETE" {
		t.Errorf("typed Allow: got %q, want DELETE", allow)
	}

	// Typed constraint mismatch is still a path miss, not MethodNotAllowed.
	status, body, _ = doReq("PATCH", "/v1/items/alice")
	if status != 404 || body != "missing:/v1/items/alice" {
		t.Fatalf("PATCH /v1/items/alice: got %d %q, want 404 typed mismatch", status, body)
	}

	if notAllowedHits.Load() != 4 {
		t.Errorf("MethodNotAllowed hits = %d, want 4", notAllowedHits.Load())
	}
}

// TestMethodNotAllowedDefault405: with no handler, the framework still
// emits a default 405 + Allow header when only NotFound is registered
// (the catch-all is shared between both fallbacks).
func TestMethodNotAllowedDefault405(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/users", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "list")
		})
		app.Get("/api/:section", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "section")
		})
		app.Get("/assets/*", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "asset")
		})
		app.NotFound(func(res *gogo.Response, req *gogo.Request) {
			res.Send(404, "text/plain", "missing")
		})
	})
	defer teardown()

	for _, path := range []string{"/users", "/api/admin", "/assets/css/site.css"} {
		req, _ := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d%s", port, path), nil)
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 405 {
			t.Fatalf("default 405 for %s: got %d, want 405", path, resp.StatusCode)
		}
		if allow := resp.Header.Get("Allow"); allow != "GET" {
			t.Errorf("Allow for %s: got %q, want GET", path, allow)
		}
	}
}

// writeTempFile drops content at a temp path with the given extension
// and returns the path. The file is removed via t.Cleanup.
func writeTempFile(t *testing.T, ext string, content []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/sendfile" + ext
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return path
}

// TestSendFileBasic: SendFile returns 200 with the file body and a
// sniffed Content-Type. Last-Modified, ETag, and Accept-Ranges are
// populated.
func TestSendFileBasic(t *testing.T) {
	path := writeTempFile(t, ".txt", []byte("hello sendfile"))

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/file", func(res *gogo.Response, req *gogo.Request) {
			if err := res.SendFile(req, path); err != nil {
				t.Errorf("SendFile: %v", err)
			}
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/file", port))
	if err != nil {
		t.Fatalf("GET /file: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	if string(body) != "hello sendfile" {
		t.Fatalf("body: got %q, want %q", body, "hello sendfile")
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type: got %q, want text/plain*", ct)
	}
	if ar := resp.Header.Get("Accept-Ranges"); ar != "bytes" {
		t.Errorf("Accept-Ranges: got %q, want bytes", ar)
	}
	if resp.Header.Get("ETag") == "" {
		t.Error("ETag is empty")
	}
	if resp.Header.Get("Last-Modified") == "" {
		t.Error("Last-Modified is empty")
	}
}

// TestSendFileContentTypeSniff: files with an unknown extension fall
// back to http.DetectContentType, which can identify PNG / JPEG headers
// even without an extension.
func TestSendFileContentTypeSniff(t *testing.T) {
	// PNG magic: 89 50 4E 47 0D 0A 1A 0A
	pngBytes := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0}
	path := writeTempFile(t, ".unknownext", pngBytes)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/png", func(res *gogo.Response, req *gogo.Request) {
			_ = res.SendFile(req, path)
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/png", port))
	if err != nil {
		t.Fatalf("GET /png: %v", err)
	}
	resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "image/png" {
		t.Errorf("sniffed Content-Type: got %q, want image/png", ct)
	}
}

// TestSendFileRange: a single-byte Range request returns 206 with the
// requested slice and a matching Content-Range header.
func TestSendFileRange(t *testing.T) {
	content := []byte("0123456789abcdef")
	path := writeTempFile(t, ".bin", content)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/r", func(res *gogo.Response, req *gogo.Request) {
			_ = res.SendFile(req, path)
		})
	})
	defer teardown()

	cases := []struct {
		hdr    string
		want   string
		wantCR string
		wantSt int
	}{
		{"bytes=0-3", "0123", "bytes 0-3/16", 206},
		{"bytes=10-", "abcdef", "bytes 10-15/16", 206},
		{"bytes=-4", "cdef", "bytes 12-15/16", 206},
		{"bytes=5-100", "56789abcdef", "bytes 5-15/16", 206},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/r", port), nil)
		req.Header.Set("Range", tc.hdr)
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("Range %q: %v", tc.hdr, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != tc.wantSt {
			t.Errorf("Range %q: status %d, want %d", tc.hdr, resp.StatusCode, tc.wantSt)
		}
		if string(body) != tc.want {
			t.Errorf("Range %q: body %q, want %q", tc.hdr, body, tc.want)
		}
		if cr := resp.Header.Get("Content-Range"); cr != tc.wantCR {
			t.Errorf("Range %q: Content-Range %q, want %q", tc.hdr, cr, tc.wantCR)
		}
	}
}

// TestSendFileMalformedRange: malformed Range headers are ignored per
// RFC 7233; the full file goes out with status 200.
func TestSendFileMalformedRange(t *testing.T) {
	content := []byte("abcdefgh")
	path := writeTempFile(t, ".txt", content)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/r", func(res *gogo.Response, req *gogo.Request) {
			_ = res.SendFile(req, path)
		})
	})
	defer teardown()

	for _, bad := range []string{"items=0-3", "bytes=0-3,5-7", "bytes=banana", "bytes="} {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/r", port), nil)
		req.Header.Set("Range", bad)
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("Range %q: %v", bad, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("Range %q: status %d, want 200 (ignored)", bad, resp.StatusCode)
		}
		if string(body) != string(content) {
			t.Errorf("Range %q: body %q, want %q", bad, body, content)
		}
	}
}

// TestSendFileUnsatisfiableRange: syntactically valid ranges outside the
// representation are rejected with 416 instead of falling back to a full 200.
func TestSendFileUnsatisfiableRange(t *testing.T) {
	content := []byte("abcdefgh")
	path := writeTempFile(t, ".txt", content)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/r", func(res *gogo.Response, req *gogo.Request) {
			_ = res.SendFile(req, path)
		})
	})
	defer teardown()

	for _, hdr := range []string{"bytes=100-200", "bytes=5-4"} {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/r", port), nil)
		req.Header.Set("Range", hdr)
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("Range %q: %v", hdr, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 416 {
			t.Errorf("Range %q: status %d, want 416", hdr, resp.StatusCode)
		}
		if string(body) != "" {
			t.Errorf("Range %q: body %q, want empty", hdr, body)
		}
		if cr := resp.Header.Get("Content-Range"); cr != "bytes */8" {
			t.Errorf("Range %q: Content-Range %q, want bytes */8", hdr, cr)
		}
	}
}

// TestSendFileConditionalETag: If-None-Match matching the response ETag
// returns 304 Not Modified with no body.
func TestSendFileConditionalETag(t *testing.T) {
	path := writeTempFile(t, ".txt", []byte("conditional"))

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/c", func(res *gogo.Response, req *gogo.Request) {
			_ = res.SendFile(req, path)
		})
	})
	defer teardown()

	// First request — grab the ETag.
	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/c", port))
	if err != nil {
		t.Fatalf("first GET: %v", err)
	}
	resp.Body.Close()
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag on first response")
	}

	// Second request with If-None-Match — should be 304.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/c", port), nil)
	req.Header.Set("If-None-Match", etag)
	resp, err = noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("conditional GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 304 {
		t.Errorf("status: got %d, want 304", resp.StatusCode)
	}
	if len(body) != 0 {
		t.Errorf("body: got %q, want empty", body)
	}

	// Wildcard If-None-Match also triggers 304.
	req, _ = http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/c", port), nil)
	req.Header.Set("If-None-Match", "*")
	resp, err = noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("wildcard GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 304 {
		t.Errorf("wildcard status: got %d, want 304", resp.StatusCode)
	}
}

// TestSendFileConditionalIMS: If-Modified-Since at or after the file's
// mtime returns 304; earlier values pass through to 200.
func TestSendFileConditionalIMS(t *testing.T) {
	path := writeTempFile(t, ".txt", []byte("ims test"))

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/c", func(res *gogo.Response, req *gogo.Request) {
			_ = res.SendFile(req, path)
		})
	})
	defer teardown()

	// Future IMS → 304.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/c", port), nil)
	req.Header.Set("If-Modified-Since", time.Now().Add(24*time.Hour).UTC().Format(http.TimeFormat))
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("future IMS: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 304 {
		t.Errorf("future IMS: got %d, want 304", resp.StatusCode)
	}

	// Past IMS → 200.
	req, _ = http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/c", port), nil)
	req.Header.Set("If-Modified-Since", time.Now().Add(-24*time.Hour).UTC().Format(http.TimeFormat))
	resp, err = noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("past IMS: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("past IMS: got %d, want 200", resp.StatusCode)
	}
}

// TestSendFileMissing: SendFile returns an error for a missing path and
// does not touch the response. The handler can then send its own 404.
func TestSendFileMissing(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			if err := res.SendFile(req, "/nonexistent/path/file.txt"); err != nil {
				res.Send(404, "text/plain", "not found")
				return
			}
			res.Send(500, "text/plain", "should not reach")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/x")
	if status != 404 || body != "not found" {
		t.Errorf("missing file: got %d %q, want 404 %q", status, body, "not found")
	}
}

// TestSendFileDirectory: SendFile on a directory returns an error
// without writing to the response.
func TestSendFileDirectory(t *testing.T) {
	dir := t.TempDir()

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/d", func(res *gogo.Response, req *gogo.Request) {
			if err := res.SendFile(req, dir); err != nil {
				res.Send(400, "text/plain", "is dir")
				return
			}
			res.Send(500, "text/plain", "should not reach")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/d")
	if status != 400 || body != "is dir" {
		t.Errorf("dir: got %d %q, want 400 %q", status, body, "is dir")
	}
}

// TestSendFileTooLarge: a file above MaxSendFileBytes returns
// ErrFileTooLarge without sending a body.
func TestSendFileTooLarge(t *testing.T) {
	// Temporarily lower the ceiling for the duration of this test.
	orig := gogo.GetMaxSendFileBytes()
	gogo.SetMaxSendFileBytes(16)
	t.Cleanup(func() { gogo.SetMaxSendFileBytes(orig) })

	path := writeTempFile(t, ".bin", make([]byte, 64))

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/big", func(res *gogo.Response, req *gogo.Request) {
			err := res.SendFile(req, path)
			if err == nil {
				res.Send(500, "text/plain", "should have failed")
				return
			}
			if err != gogo.ErrFileTooLarge {
				res.Send(500, "text/plain", "wrong err: "+err.Error())
				return
			}
			res.Send(413, "text/plain", "too large")
		})
	})
	defer teardown()

	status, body := httpGet(t, port, "/big")
	if status != 413 || body != "too large" {
		t.Errorf("too large: got %d %q, want 413 %q", status, body, "too large")
	}
}

// TestDownload: sets Content-Disposition: attachment with the
// requested filename quoted per RFC 6266.
func TestDownload(t *testing.T) {
	path := writeTempFile(t, ".dat", []byte("download body"))

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/dl", func(res *gogo.Response, req *gogo.Request) {
			if err := res.Download(req, path, "report.dat"); err != nil {
				t.Errorf("Download: %v", err)
			}
		})
		app.Get("/dl-default", func(res *gogo.Response, req *gogo.Request) {
			_ = res.Download(req, path, "")
		})
		app.Get("/dl-unicode", func(res *gogo.Response, req *gogo.Request) {
			_ = res.Download(req, path, "รายงาน.dat")
		})
		app.Get("/dl-quote", func(res *gogo.Response, req *gogo.Request) {
			_ = res.Download(req, path, `weird"name\.dat`)
		})
	})
	defer teardown()

	// Custom filename.
	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/dl", port))
	if err != nil {
		t.Fatalf("GET /dl: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "download body" {
		t.Errorf("body: got %q", body)
	}
	cd := resp.Header.Get("Content-Disposition")
	if cd != `attachment; filename="report.dat"` {
		t.Errorf("Content-Disposition: got %q", cd)
	}

	// Default filename from path basename.
	resp, err = noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/dl-default", port))
	if err != nil {
		t.Fatalf("GET /dl-default: %v", err)
	}
	resp.Body.Close()
	cd = resp.Header.Get("Content-Disposition")
	if !strings.HasPrefix(cd, `attachment; filename="sendfile.dat"`) {
		t.Errorf("default filename Content-Disposition: got %q", cd)
	}

	// Non-ASCII filename — gets filename* with UTF-8 percent encoding.
	resp, err = noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/dl-unicode", port))
	if err != nil {
		t.Fatalf("GET /dl-unicode: %v", err)
	}
	resp.Body.Close()
	cd = resp.Header.Get("Content-Disposition")
	if !strings.Contains(cd, `filename*=UTF-8''`) {
		t.Errorf("unicode filename missing filename*: got %q", cd)
	}

	// Embedded quote/backslash in the filename get escaped.
	resp, err = noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/dl-quote", port))
	if err != nil {
		t.Fatalf("GET /dl-quote: %v", err)
	}
	resp.Body.Close()
	cd = resp.Header.Get("Content-Disposition")
	if !strings.Contains(cd, `\"`) || !strings.Contains(cd, `\\`) {
		t.Errorf("quote escaping in Content-Disposition: got %q", cd)
	}
}

// TestSendFileAsync: SendFile works from an async handler — the body is
// streamed through the loop.Defer + cork path.
func TestSendFileAsync(t *testing.T) {
	path := writeTempFile(t, ".txt", []byte("async file body"))

	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/a", func(res *gogo.Response, req *gogo.Request) {
			if err := res.SendFile(req, path); err != nil {
				t.Errorf("SendFile async: %v", err)
			}
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/a", port))
	if err != nil {
		t.Fatalf("GET /a: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("async status: got %d, want 200", resp.StatusCode)
	}
	if string(body) != "async file body" {
		t.Errorf("async body: got %q, want %q", body, "async file body")
	}
	if resp.Header.Get("ETag") == "" {
		t.Error("async ETag empty")
	}
}

// TestSendFileLargeStreaming verifies a multi-MiB file streams
// byte-for-byte through the chunked-encoding path. The old
// whole-file-into-RAM implementation also returned the correct
// bytes; what changed is that this test passing in combination
// with TestSendFileBackpressureSlowReader (which exercises the
// AwaitDrain park/wake cycle) demonstrates the body is being
// produced incrementally rather than allocated up front.
func TestSendFileLargeStreaming(t *testing.T) {
	// 8 MiB deterministic file (xorshift32 keyed by offset so any
	// truncation / reordering on the wire is visible).
	const size = 8 << 20
	src := make([]byte, size)
	state := uint32(0xC0DE1234)
	for i := range src {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		src[i] = byte(state)
	}
	path := writeTempFile(t, ".bin", src)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/big", func(res *gogo.Response, req *gogo.Request) {
			if err := res.SendFile(req, path); err != nil {
				t.Errorf("SendFile: %v", err)
			}
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/big", port))
	if err != nil {
		t.Fatalf("GET /big: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	if len(body) != size {
		t.Fatalf("body length: got %d, want %d", len(body), size)
	}
	if !bytes.Equal(body, src) {
		t.Fatal("body does not match source")
	}
}

// TestSendFileBackpressureSlowReader simulates a slow client and
// asserts the streaming path still completes byte-for-byte. The old
// whole-file-into-RAM path didn't care about consumer speed since
// the body was already buffered; the streaming path relies on
// AwaitDrain to park the goroutine when uWS's send buffer climbs
// past the threshold, then resume when the consumer makes progress.
// If backpressure didn't work the goroutine would either hang or
// the kernel send buffer would balloon past expected bounds.
func TestSendFileBackpressureSlowReader(t *testing.T) {
	const size = 4 << 20 // 4 MiB
	src := make([]byte, size)
	for i := range src {
		src[i] = byte(i & 0xFF)
	}
	path := writeTempFile(t, ".bin", src)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/slow", func(res *gogo.Response, req *gogo.Request) {
			if err := res.SendFile(req, path); err != nil {
				t.Errorf("SendFile: %v", err)
			}
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/slow", port))
	if err != nil {
		t.Fatalf("GET /slow: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// Drip-read 4 KiB at a time with a small sleep so the server's
	// send buffer fills and AwaitDrain has to park. net/http already
	// handles chunked-encoding decoding, so the returned bytes are
	// the decoded body. A successful test means the producer
	// throttled correctly; a hang means backpressure broke.
	deadline := time.Now().Add(30 * time.Second)
	var collected bytes.Buffer
	buf := make([]byte, 4096)
	for collected.Len() < size && time.Now().Before(deadline) {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			collected.Write(buf[:n])
			time.Sleep(2 * time.Millisecond)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("read: %v", rerr)
		}
	}
	if collected.Len() != size {
		t.Fatalf("body length: got %d, want %d", collected.Len(), size)
	}
	if !bytes.Equal(collected.Bytes(), src) {
		t.Fatal("backpressure-streamed body does not match source")
	}
}

// TestRedirectFromGetAsync locks in the fix for the latent loop-pointer
// bug: res.Loop() reads thread-local uWS::Loop::get(), which from a
// shared-dispatch worker goroutine returns the wrong loop (or null).
// Before the fix, Redirect from a GetAsync handler hung indefinitely
// because the deferred cork was queued on a loop that never iterated.
func TestRedirectFromGetAsync(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Redirect("/elsewhere", 301)
		})
	})
	defer teardown()

	// Disable redirect-following so we see the 301 itself.
	client := &http.Client{
		Transport:     &http.Transport{DisableKeepAlives: true},
		Timeout:       3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/r", port))
	if err != nil {
		t.Fatalf("GET /r: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 301 {
		t.Errorf("status: got %d, want 301", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/elsewhere" {
		t.Errorf("Location: got %q, want /elsewhere", loc)
	}
}

func TestRedirectFromGetAsyncPreservesPendingHeaders(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.RequestID())
		app.GetAsync("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Redirect("/elsewhere", 302)
		})
	})
	defer teardown()

	client := &http.Client{
		Transport:     &http.Transport{DisableKeepAlives: true},
		Timeout:       3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/r", port), nil)
	req.Header.Set("X-Request-ID", "redirect-async-id")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET /r: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 302 {
		t.Errorf("status: got %d, want 302", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Request-ID"); got != "redirect-async-id" {
		t.Errorf("X-Request-ID: got %q, want redirect-async-id", got)
	}
}

func TestSendBytesFromGetAsyncPreservesPendingHeaders(t *testing.T) {
	path := writeTempFile(t, ".txt", nil)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.RequestID())
		app.GetAsync("/empty", func(res *gogo.Response, req *gogo.Request) {
			if err := res.SendFile(req, path); err != nil {
				res.Send(500, "text/plain", err.Error())
			}
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/empty", port), nil)
	req.Header.Set("X-Request-ID", "sendbytes-async-id")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("GET /empty: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Request-ID"); got != "sendbytes-async-id" {
		t.Errorf("X-Request-ID: got %q, want sendbytes-async-id", got)
	}
}

// TestHTTPMethodHelpers: Put, Patch, Delete, Options, Head each
// route only on their own method; mismatched methods fall through to
// the default 404 (or 405 via MethodNotAllowed when set).
func TestHTTPMethodHelpers(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Put("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "put")
		})
		app.Patch("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "patch")
		})
		app.Delete("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "delete")
		})
		app.Options("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Status(204)
			res.Header("Allow", "GET, PUT, PATCH, DELETE, OPTIONS")
			res.End("")
		})
		app.Head("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Status(200)
			res.Header("X-Probe", "ok")
			res.End("")
		})
	})
	defer teardown()

	check := func(method, want string, wantStatus int, wantHeader [2]string) {
		t.Helper()
		req, _ := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d/r", port), nil)
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("%s /r: %v", method, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != wantStatus {
			t.Errorf("%s /r: status %d, want %d", method, resp.StatusCode, wantStatus)
		}
		if string(body) != want {
			t.Errorf("%s /r: body %q, want %q", method, body, want)
		}
		if wantHeader[0] != "" && resp.Header.Get(wantHeader[0]) != wantHeader[1] {
			t.Errorf("%s /r: %s header %q, want %q", method, wantHeader[0],
				resp.Header.Get(wantHeader[0]), wantHeader[1])
		}
	}

	check("PUT", "put", 200, [2]string{})
	check("PATCH", "patch", 200, [2]string{})
	check("DELETE", "delete", 200, [2]string{})
	check("OPTIONS", "", 204, [2]string{"Allow", "GET, PUT, PATCH, DELETE, OPTIONS"})
	// HEAD: Go's http.Client strips the body even if the server sent one;
	// just check status and headers.
	check("HEAD", "", 200, [2]string{"X-Probe", "ok"})
}

// TestQueryAndParamConversion: QueryInt / QueryInt64 / QueryBool /
// ParamInt / ParamInt64 each parse the named value or fall back to the
// supplied default.
func TestQueryAndParamConversion(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/q", func(res *gogo.Response, req *gogo.Request) {
			page := req.QueryInt("page", 1)
			limit := req.QueryInt64("limit", 50)
			active := req.QueryBool("active", false)
			res.Send(200, "text/plain",
				fmt.Sprintf("page=%d limit=%d active=%t", page, limit, active))
		})
		app.Get("/items/:id", func(res *gogo.Response, req *gogo.Request) {
			id := req.ParameterInt(0, -1)
			res.Send(200, "text/plain", fmt.Sprintf("id=%d", id))
		})
		app.Get("/items64/:id", func(res *gogo.Response, req *gogo.Request) {
			id := req.ParamInt64("id", -1)
			res.Send(200, "text/plain", fmt.Sprintf("id=%d", id))
		})
		app.GetAsync("/async-items64/:id", func(res *gogo.Response, req *gogo.Request) {
			id := req.ParamInt64("id", -1)
			res.Send(200, "text/plain", fmt.Sprintf("id=%d", id))
		})
	})
	defer teardown()

	cases := []struct {
		path string
		want string
	}{
		// Defaults when params are missing.
		{"/q", "page=1 limit=50 active=false"},
		// Parsed values.
		{"/q?page=3&limit=200&active=true", "page=3 limit=200 active=true"},
		// Bool aliases.
		{"/q?active=1", "page=1 limit=50 active=true"},
		{"/q?active=yes", "page=1 limit=50 active=true"},
		{"/q?active=on", "page=1 limit=50 active=true"},
		{"/q?active=0", "page=1 limit=50 active=false"},
		{"/q?active=off", "page=1 limit=50 active=false"},
		// Garbage falls back to default (true here).
		{"/q?active=banana", "page=1 limit=50 active=false"},
		// Negative int — accepted.
		{"/q?page=-5", "page=-5 limit=50 active=false"},
		// Non-numeric falls back to default.
		{"/q?page=oops", "page=1 limit=50 active=false"},
		// Route param.
		{"/items/42", "id=42"},
		{"/items/abc", "id=-1"},
		{"/items64/9999999999", "id=9999999999"},
		{"/items64/abc", "id=-1"},
		{"/async-items64/9999999999", "id=9999999999"},
	}
	for _, tc := range cases {
		status, body := httpGet(t, port, tc.path)
		if status != 200 {
			t.Errorf("%s: status %d, want 200", tc.path, status)
		}
		if body != tc.want {
			t.Errorf("%s: got %q, want %q", tc.path, body, tc.want)
		}
	}
}

// TestResponseAppend: Append emits multiple header lines with the same
// key; the client sees both values.
func TestResponseAppend(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/h", func(res *gogo.Response, req *gogo.Request) {
			res.Append("X-Multi", "first")
			res.Append("X-Multi", "second")
			res.Header("Set-Cookie", "a=1; Path=/")
			res.Header("Set-Cookie", "b=2; Path=/")
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/h", port))
	if err != nil {
		t.Fatalf("GET /h: %v", err)
	}
	resp.Body.Close()
	if vs := resp.Header.Values("X-Multi"); len(vs) != 2 || vs[0] != "first" || vs[1] != "second" {
		t.Errorf("X-Multi: got %v, want [first second]", vs)
	}
	if vs := resp.Header.Values("Set-Cookie"); len(vs) != 2 {
		t.Errorf("Set-Cookie count: got %d, want 2 (values: %v)", len(vs), vs)
	}
}

// TestProtocolDefault: without Config.TrustProxy, Protocol() always
// returns "http" — X-Forwarded-Proto is ignored so a malicious client
// can't spoof its way to appearing as https.
func TestProtocolDefault(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/p", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", fmt.Sprintf("%s:%t", req.Protocol(), req.Secure()))
		})
	})
	defer teardown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/p", port), nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	resp, err := noKeepaliveClient.Do(req)
	if err != nil {
		t.Fatalf("GET /p: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "http:false" {
		t.Errorf("default: got %q, want %q (X-Forwarded-Proto must be ignored)", body, "http:false")
	}
}

// TestProtocolTrustProxy: with Config.TrustProxy enabled, Protocol()
// honors X-Forwarded-Proto. Comma-separated lists keep the first value.
func TestProtocolTrustProxy(t *testing.T) {
	port := freePort(t)
	ready := make(chan *gogo.App, 1)
	runDone := make(chan struct{})

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		app, err := gogo.NewApp(gogo.Config{TrustProxy: true})
		if err != nil {
			t.Errorf("NewApp: %v", err)
			close(runDone)
			return
		}
		app.Get("/p", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", fmt.Sprintf("%s:%t", req.Protocol(), req.Secure()))
		})
		if !app.Listen(port) {
			t.Errorf("Listen :%d failed", port)
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
	case <-time.After(5 * time.Second):
		t.Fatal("setup timeout")
	}
	for i := 0; i < 50; i++ {
		c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer func() {
		app.Shutdown()
		<-runDone
	}()

	cases := []struct {
		hdr  string
		want string
	}{
		{"https", "https:true"},
		{"HTTPS", "https:true"},
		{"http", "http:false"},
		{"https, http", "https:true"},
		{"", "http:false"},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/p", port), nil)
		if tc.hdr != "" {
			req.Header.Set("X-Forwarded-Proto", tc.hdr)
		}
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("Forwarded-Proto=%q: %v", tc.hdr, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != tc.want {
			t.Errorf("Forwarded-Proto=%q: got %q, want %q", tc.hdr, body, tc.want)
		}
	}
}

// TestHTTPMethodHelpersBodyLimit: PUT, PATCH, DELETE all honor
// App.Config.BodyLimit just like POST. OPTIONS and HEAD are bodyless and
// bypass the limit.
func TestHTTPMethodHelpersBodyLimit(t *testing.T) {
	port := freePort(t)
	ready := make(chan *gogo.App, 1)
	runDone := make(chan struct{})

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		app, err := gogo.NewApp(gogo.Config{BodyLimit: 8})
		if err != nil {
			t.Errorf("NewApp: %v", err)
			close(runDone)
			return
		}
		app.Put("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
		app.Patch("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
		app.Delete("/r", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
		if !app.Listen(port) {
			t.Errorf("Listen :%d failed", port)
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
	case <-time.After(5 * time.Second):
		t.Fatal("app setup timed out")
	}
	// Wait until the port accepts.
	for i := 0; i < 50; i++ {
		c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer func() {
		app.Shutdown()
		<-runDone
	}()

	body := strings.NewReader(strings.Repeat("X", 64)) // > 8 bytes
	for _, method := range []string{"PUT", "PATCH", "DELETE"} {
		body.Seek(0, 0)
		req, _ := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d/r", port), body)
		req.ContentLength = 64
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("%s /r: %v", method, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 413 {
			t.Errorf("%s /r over limit: status %d, want 413", method, resp.StatusCode)
		}
	}
}

// TestBodyParserJSON: PostAsync receives the body, BodyParser unmarshals
// JSON into a typed struct.
func TestBodyParserJSON(t *testing.T) {
	type user struct {
		Name string   `json:"name"`
		Age  int      `json:"age"`
		Tags []string `json:"tags"`
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/u", 64*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			var u user
			if err := req.BodyParser(&u); err != nil {
				res.Send(400, "text/plain", err.Error())
				return
			}
			res.Send(200, "text/plain",
				fmt.Sprintf("name=%s age=%d tags=%v", u.Name, u.Age, u.Tags))
		})
	})
	defer teardown()

	payload := []byte(`{"name":"alice","age":30,"tags":["a","b"]}`)
	status, body := httpPost(t, port, "/u", "application/json", payload)
	if status != 200 || body != "name=alice age=30 tags=[a b]" {
		t.Fatalf("JSON parse: got %d %q", status, body)
	}

	// Charset parameter — should still match application/json.
	status, body = httpPost(t, port, "/u", "application/json; charset=utf-8", payload)
	if status != 200 || body != "name=alice age=30 tags=[a b]" {
		t.Fatalf("JSON+charset: got %d %q", status, body)
	}

	// Bad JSON — surface the parse error.
	status, _ = httpPost(t, port, "/u", "application/json", []byte(`{not json}`))
	if status != 400 {
		t.Errorf("bad JSON: status %d, want 400", status)
	}
}

func TestBodyParserJSONUsesConfiguredDecoder(t *testing.T) {
	type user struct {
		Name string `json:"name"`
	}
	var calls atomic.Int32
	cfg := gogo.Config{
		JSONDecoder: func(data []byte, v any) error {
			calls.Add(1)
			u := v.(*user)
			u.Name = "custom"
			return nil
		},
	}
	port, teardown := startAppCfg(t, cfg, func(app *gogo.App) {
		app.PostAsync("/u", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			var u user
			if err := req.BodyParser(&u); err != nil {
				res.Send(400, "text/plain", err.Error())
				return
			}
			res.Send(200, "text/plain", u.Name)
		})
	})
	defer teardown()

	status, body := httpPost(t, port, "/u", "application/json", []byte(`{"name":"ignored"}`))
	if status != 200 || body != "custom" {
		t.Fatalf("custom decoder: got %d %q, want 200 custom", status, body)
	}
	if calls.Load() != 1 {
		t.Fatalf("decoder calls=%d, want 1", calls.Load())
	}
}

func TestBodyParserJSONUsesConfiguredDecoderOnSlowAndRouterPaths(t *testing.T) {
	type user struct {
		Name string `json:"name"`
	}
	var calls atomic.Int32
	cfg := gogo.Config{
		JSONDecoder: func(data []byte, v any) error {
			calls.Add(1)
			u := v.(*user)
			u.Name = "custom"
			return nil
		},
	}
	port, teardown := startAppCfg(t, cfg, func(app *gogo.App) {
		app.PostAsync("/slow", 16*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			var u user
			if err := req.BodyParser(&u); err != nil {
				res.Send(400, "text/plain", err.Error())
				return
			}
			res.Send(200, "text/plain", u.Name)
		})

		api := app.Group("/api")
		api.PostAsync("/u", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			var u user
			if err := req.BodyParser(&u); err != nil {
				res.Send(400, "text/plain", err.Error())
				return
			}
			res.Send(200, "text/plain", u.Name)
		})
	})
	defer teardown()

	status, body := httpPost(t, port, "/slow", "application/json", []byte(`{"name":"ignored"}`))
	if status != 200 || body != "custom" {
		t.Fatalf("slow custom decoder: got %d %q, want 200 custom", status, body)
	}
	status, body = httpPost(t, port, "/api/u", "application/json", []byte(`{"name":"ignored"}`))
	if status != 200 || body != "custom" {
		t.Fatalf("router custom decoder: got %d %q, want 200 custom", status, body)
	}
	if calls.Load() != 2 {
		t.Fatalf("decoder calls=%d, want 2", calls.Load())
	}
}

// TestBodyParserForm: application/x-www-form-urlencoded into a struct
// with `form:"name"` tags. Covers scalars, slice, bool aliases, and
// pointer-to-int.
func TestBodyParserForm(t *testing.T) {
	type filters struct {
		Q       string   `form:"q"`
		Page    int      `form:"page"`
		Active  bool     `form:"active"`
		Tags    []string `form:"tag"`
		MinAge  *int     `form:"min_age"`
		Limit   int64    `form:"limit"`
		Ratio   float64  `form:"ratio"`
		Skipped string   `form:"-"`
		Unset   *bool    `form:"unset"`
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/f", 64*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			var f filters
			if err := req.BodyParser(&f); err != nil {
				res.Send(400, "text/plain", err.Error())
				return
			}
			minAge := "<nil>"
			if f.MinAge != nil {
				minAge = strconv.Itoa(*f.MinAge)
			}
			unset := "<nil>"
			if f.Unset != nil {
				unset = strconv.FormatBool(*f.Unset)
			}
			res.Send(200, "text/plain", fmt.Sprintf(
				"q=%s page=%d active=%t tags=%v min_age=%s limit=%d ratio=%.2f unset=%s",
				f.Q, f.Page, f.Active, f.Tags, minAge, f.Limit, f.Ratio, unset))
		})
	})
	defer teardown()

	body := []byte("q=hello&page=2&active=yes&tag=a&tag=b&min_age=18&limit=200&ratio=1.5&skipped=x")
	status, respBody := httpPost(t, port, "/f", "application/x-www-form-urlencoded", body)
	want := "q=hello page=2 active=true tags=[a b] min_age=18 limit=200 ratio=1.50 unset=<nil>"
	if status != 200 || respBody != want {
		t.Fatalf("form: got %d %q\n want %q", status, respBody, want)
	}

	// Bad int — surface as 400.
	bad := []byte("q=x&page=notanumber")
	status, _ = httpPost(t, port, "/f", "application/x-www-form-urlencoded", bad)
	if status != 400 {
		t.Errorf("bad form int: status %d, want 400", status)
	}
}

// TestBodyParserMultipart: multipart/form-data — non-file parts are
// surfaced as form values; file parts are ignored by BodyParser.
func TestBodyParserMultipart(t *testing.T) {
	type form struct {
		Name string `form:"name"`
		Age  int    `form:"age"`
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/m", 1<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
			var f form
			if err := req.BodyParser(&f); err != nil {
				res.Send(400, "text/plain", err.Error())
				return
			}
			res.Send(200, "text/plain", fmt.Sprintf("name=%s age=%d", f.Name, f.Age))
		})
	})
	defer teardown()

	// Build a multipart body by hand to control the boundary exactly.
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	_ = mw.WriteField("name", "bob")
	_ = mw.WriteField("age", "42")
	fw, _ := mw.CreateFormFile("upload", "f.txt")
	fw.Write([]byte("file content goes here"))
	mw.Close()

	status, respBody := httpPost(t, port, "/m", mw.FormDataContentType(), body.Bytes())
	if status != 200 || respBody != "name=bob age=42" {
		t.Fatalf("multipart: got %d %q", status, respBody)
	}
}

func TestBodyParserMultipartRejectsOversizeField(t *testing.T) {
	oldLimit := gogo.GetDefaultMultipartPartLimit()
	gogo.SetDefaultMultipartPartLimit(8)
	defer gogo.SetDefaultMultipartPartLimit(oldLimit)

	type form struct {
		Name string `form:"name"`
	}
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/m", 1<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
			var f form
			err := req.BodyParser(&f)
			if errors.Is(err, gogo.ErrMultipartPartTooLarge) {
				res.Send(413, "text/plain", "field too large")
				return
			}
			if err != nil {
				res.Send(500, "text/plain", err.Error())
				return
			}
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	_ = mw.WriteField("name", "0123456789abcdef")
	mw.Close()

	status, respBody := httpPost(t, port, "/m", mw.FormDataContentType(), body.Bytes())
	if status != 413 || respBody != "field too large" {
		t.Fatalf("oversize multipart field: got %d %q, want 413", status, respBody)
	}
}

// TestBodyParserNoBody: sync handlers reach BodyParser without
// collecting the body first → ErrNoBody.
func TestBodyParserNoBody(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Post("/x", func(res *gogo.Response, req *gogo.Request) {
			var v map[string]any
			err := req.BodyParser(&v)
			if !errors.Is(err, gogo.ErrNoBody) {
				res.Send(500, "text/plain", "wrong err: "+fmt.Sprint(err))
				return
			}
			res.Send(200, "text/plain", "no-body")
		})
	})
	defer teardown()

	status, body := httpPost(t, port, "/x", "application/json", []byte(`{"a":1}`))
	if status != 200 || body != "no-body" {
		t.Errorf("sync no-body: got %d %q", status, body)
	}
}

// TestBodyParserUnsupportedMediaType: a non-JSON / non-form content
// type returns ErrUnsupportedMediaType.
func TestBodyParserUnsupportedMediaType(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/u", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			var v any
			err := req.BodyParser(&v)
			if errors.Is(err, gogo.ErrUnsupportedMediaType) {
				res.Send(415, "text/plain", "unsupported")
				return
			}
			res.Send(500, "text/plain", "unexpected: "+fmt.Sprint(err))
		})
	})
	defer teardown()

	status, _ := httpPost(t, port, "/u", "application/xml", []byte("<x/>"))
	if status != 415 {
		t.Errorf("unsupported: status %d, want 415", status)
	}
}

// TestParseBodyDirect: the free helper ParseBody works without a
// Request, for sync handlers that collect the body via Response.Body.
func TestParseBodyDirect(t *testing.T) {
	type pt struct {
		X int `json:"x"`
		Y int `json:"y"`
	}
	var p pt
	if err := gogo.ParseBody("application/json", []byte(`{"x":3,"y":4}`), &p); err != nil {
		t.Fatalf("ParseBody: %v", err)
	}
	if p.X != 3 || p.Y != 4 {
		t.Errorf("got %+v, want {X:3, Y:4}", p)
	}

	// Form variant — direct helper.
	type form struct {
		Name string `form:"name"`
	}
	var f form
	if err := gogo.ParseBody("application/x-www-form-urlencoded", []byte("name=x"), &f); err != nil {
		t.Fatalf("ParseBody form: %v", err)
	}
	if f.Name != "x" {
		t.Errorf("got %+v, want {Name:x}", f)
	}
}

// TestWebSocketSubscribePublish: when one socket publishes via
// ws.Publish, every OTHER subscriber of the topic gets the message.
// uWS deliberately excludes the publishing socket from its own
// broadcast (per WebSocket.h: "Publish as sender, does not receive
// its own messages even if subscribed to relevant topics") — the
// non-publishing subscriber receives the message and the publisher
// itself does not. Use App.Publish (covered separately) when the
// caller wants the publishing socket to also receive the message.
func TestWebSocketSubscribePublish(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("room")
			},
			Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
				ws.Publish("room", msg, op)
			},
		})
	})
	defer teardown()

	a, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	b, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()
	// Let both Open handlers fire and subscribe before a publishes.
	time.Sleep(50 * time.Millisecond)

	if err := a.SendText("hello world"); err != nil {
		t.Fatalf("a.SendText: %v", err)
	}

	// B (non-publisher subscriber) receives the broadcast.
	gotB, err := b.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("b.ReadText: %v", err)
	}
	if gotB != "hello world" {
		t.Errorf("b received %q, want %q", gotB, "hello world")
	}
	// A (publisher) does NOT receive its own broadcast — uWS skips it.
	if err := a.expectNoMessage(300 * time.Millisecond); err != nil {
		t.Errorf("publisher should not receive its own publish: %v", err)
	}
}

func TestWSHubPublishFrom(t *testing.T) {
	hub := gogo.NewWSHub(gogo.WithWSHubNodeID("test-node"))
	defer hub.Close()

	port, teardown := startApp(t, func(app *gogo.App) {
		hub.WebSocket(app, "/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				hub.Subscribe(ws, "room")
			},
			Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
				if err := hub.PublishFrom(ws, "room", msg, op); err != nil {
					t.Errorf("PublishFrom: %v", err)
				}
			},
		})
	})
	defer teardown()

	a, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	b, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()
	time.Sleep(50 * time.Millisecond)

	if err := a.SendText("hub hello"); err != nil {
		t.Fatalf("a.SendText: %v", err)
	}
	gotB, err := b.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("b.ReadText: %v", err)
	}
	if gotB != "hub hello" {
		t.Errorf("b received %q, want hub hello", gotB)
	}
	if err := a.expectNoMessage(300 * time.Millisecond); err != nil {
		t.Errorf("publisher should not receive its own hub publish: %v", err)
	}
}

func TestWSHubPublishFromRequiresTrackedSocket(t *testing.T) {
	hub := gogo.NewWSHub(gogo.WithWSHubNodeID("test-node"))
	defer hub.Close()

	port, teardown := startApp(t, func(app *gogo.App) {
		hub.Attach(app)
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("room")
			},
			Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
				err := hub.PublishFrom(ws, "room", msg, op)
				if !errors.Is(err, gogo.ErrWSHubUntrackedSocket) {
					t.Errorf("PublishFrom error = %v, want ErrWSHubUntrackedSocket", err)
				}
				ws.SendText("untracked")
			},
		})
	})
	defer teardown()

	a, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	b, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()
	time.Sleep(50 * time.Millisecond)

	if err := a.SendText("hub hello"); err != nil {
		t.Fatalf("a.SendText: %v", err)
	}
	gotA, err := a.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("a.ReadText: %v", err)
	}
	if gotA != "untracked" {
		t.Errorf("a received %q, want untracked", gotA)
	}
	if err := b.expectNoMessage(300 * time.Millisecond); err != nil {
		t.Errorf("untracked PublishFrom should not broadcast: %v", err)
	}
}

func TestWSHubPublishFromTracksRawSubscribe(t *testing.T) {
	hub := gogo.NewWSHub(gogo.WithWSHubNodeID("test-node"))
	defer hub.Close()

	port, teardown := startApp(t, func(app *gogo.App) {
		hub.WebSocket(app, "/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("room")
			},
			Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
				if err := hub.PublishFrom(ws, "room", msg, op); err != nil {
					t.Errorf("PublishFrom: %v", err)
				}
			},
		})
	})
	defer teardown()

	a, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	b, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()
	time.Sleep(50 * time.Millisecond)

	if err := a.SendText("raw subscribe hello"); err != nil {
		t.Fatalf("a.SendText: %v", err)
	}
	gotB, err := b.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("b.ReadText: %v", err)
	}
	if gotB != "raw subscribe hello" {
		t.Errorf("b received %q, want raw subscribe hello", gotB)
	}
	if err := a.expectNoMessage(300 * time.Millisecond); err != nil {
		t.Errorf("publisher should not receive its own hub publish: %v", err)
	}
}

func TestWSHubSubscribeRollsBackWhenHubTrackingFails(t *testing.T) {
	hub := gogo.NewWSHub(gogo.WithWSHubNodeID("test-node"))
	var appRef *gogo.App
	var subscribeReturned atomic.Bool
	var opened atomic.Bool

	port, teardown := startApp(t, func(app *gogo.App) {
		appRef = app
		hub.WebSocket(app, "/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				hub.Close()
				subscribeReturned.Store(ws.Subscribe("room"))
				opened.Store(true)
			},
		})
	})
	defer teardown()

	client, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if opened.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !opened.Load() {
		t.Fatal("Open did not run")
	}
	if subscribeReturned.Load() {
		t.Fatal("Subscribe succeeded even though hub tracking failed")
	}

	appRef.Publish("room", []byte("ghost"), gogo.Text)
	if err := client.expectNoMessage(300 * time.Millisecond); err != nil {
		t.Fatalf("rollback left native subscription active: %v", err)
	}
}

func TestWSHubCloseSkipsUserCloseWhenOpenDidNotRun(t *testing.T) {
	hubA := gogo.NewWSHub(gogo.WithWSHubNodeID("hub-a"))
	defer hubA.Close()
	hubB := gogo.NewWSHub(gogo.WithWSHubNodeID("hub-b"))
	defer hubB.Close()

	var opened atomic.Int32
	var closed atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		behavior := gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				opened.Add(1)
			},
			Close: func(ws *gogo.WebSocket, code int, msg []byte) {
				closed.Add(1)
			},
		}
		app.WebSocket("/ws", hubB.Wrap(app, hubA.Wrap(app, behavior)))
	})
	defer teardown()

	client, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	_, _ = client.ReadText(500 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	if got := opened.Load(); got != 0 {
		t.Fatalf("Open calls = %d, want 0", got)
	}
	if got := closed.Load(); got != 0 {
		t.Fatalf("Close calls = %d, want 0", got)
	}
}

func TestWSHubCloseSkipsUserCloseWhenOpenPanics(t *testing.T) {
	hub := gogo.NewWSHub(gogo.WithWSHubNodeID("test-node"))
	defer hub.Close()

	var opened atomic.Int32
	var closed atomic.Int32
	port, teardown := startApp(t, func(app *gogo.App) {
		hub.WebSocket(app, "/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				opened.Add(1)
				panic("open failed after partial setup")
			},
			Close: func(ws *gogo.WebSocket, code int, msg []byte) {
				closed.Add(1)
			},
		})
	})
	defer teardown()

	client, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if closed.Load() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := opened.Load(); got != 1 {
		t.Fatalf("Open calls = %d, want 1", got)
	}
	if got := closed.Load(); got != 0 {
		t.Fatalf("Close calls = %d, want 0", got)
	}
}

type blockingWSHubAdapter struct {
	entered chan struct{}
	release chan struct{}
}

func (a *blockingWSHubAdapter) Start(context.Context, func(gogo.WSHubMessage)) error {
	return nil
}

func (a *blockingWSHubAdapter) Publish(context.Context, gogo.WSHubMessage) error {
	select {
	case a.entered <- struct{}{}:
	default:
	}
	<-a.release
	return nil
}

func (a *blockingWSHubAdapter) Close() error { return nil }

func TestWSHubPublishFromDoesNotBlockOnAdapter(t *testing.T) {
	adapter := &blockingWSHubAdapter{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	hub := gogo.NewWSHub(
		gogo.WithWSHubNodeID("test-node"),
		gogo.WithWSHubAdapter(adapter),
		gogo.WithWSHubAdapterQueueSize(1),
	)
	defer func() {
		close(adapter.release)
		if err := hub.Close(); err != nil {
			t.Errorf("hub.Close: %v", err)
		}
	}()

	port, teardown := startApp(t, func(app *gogo.App) {
		hub.WebSocket(app, "/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				hub.Subscribe(ws, "room")
			},
			Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
				if err := hub.PublishFrom(ws, "room", msg, op); err != nil {
					t.Errorf("PublishFrom: %v", err)
				}
				ws.SendText("after")
			},
		})
	})
	defer teardown()

	a, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	time.Sleep(50 * time.Millisecond)

	if err := a.SendText("hub hello"); err != nil {
		t.Fatalf("a.SendText: %v", err)
	}
	gotA, err := a.ReadText(500 * time.Millisecond)
	if err != nil {
		t.Fatalf("PublishFrom blocked on adapter publish: %v", err)
	}
	if gotA != "after" {
		t.Errorf("a received %q, want after", gotA)
	}
	select {
	case <-adapter.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("adapter publish was not queued")
	}
}

// TestWebSocketAppPublish: App.Publish from a Go worker goroutine
// reaches every subscriber on the loop without the publisher ever
// being on the loop thread.
func TestWebSocketAppPublish(t *testing.T) {
	type appPub struct {
		app *gogo.App
		mu  sync.Mutex
	}
	captured := &appPub{}

	port, teardown := startApp(t, func(app *gogo.App) {
		captured.mu.Lock()
		captured.app = app
		captured.mu.Unlock()
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("news")
			},
		})
	})
	defer teardown()

	client, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	// Give the Open handler a moment to run + subscribe before
	// publishing — the subscribe happens on the loop and we don't
	// have a sync signal otherwise.
	time.Sleep(50 * time.Millisecond)

	// Publish from a worker goroutine — App.Publish must be
	// thread-safe.
	done := make(chan struct{})
	go func() {
		captured.mu.Lock()
		a := captured.app
		captured.mu.Unlock()
		a.Publish("news", []byte("broadcast 1"), gogo.Text)
		a.Publish("news", []byte("broadcast 2"), gogo.Text)
		close(done)
	}()
	<-done

	got1, err := client.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if got1 != "broadcast 1" {
		t.Errorf("msg 1: got %q, want %q", got1, "broadcast 1")
	}
	got2, err := client.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if got2 != "broadcast 2" {
		t.Errorf("msg 2: got %q, want %q", got2, "broadcast 2")
	}
	if err := client.expectNoMessage(300 * time.Millisecond); err != nil {
		t.Fatalf("duplicate after App.Publish: %v", err)
	}
}

// TestWebSocketUnsubscribe: after Unsubscribe, the connection no
// longer receives messages on the topic.
func TestWebSocketUnsubscribe(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("alerts")
			},
			Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
				switch string(msg) {
				case "leave":
					ws.Unsubscribe("alerts")
					ws.Send([]byte("left"), gogo.Text)
				case "ping":
					ws.Publish("alerts", []byte("pong"), gogo.Text)
				}
			},
		})
	})
	defer teardown()

	subscriber, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial subscriber: %v", err)
	}
	defer subscriber.Close()
	publisher, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial publisher: %v", err)
	}
	defer publisher.Close()

	// Sanity: subscriber receives broadcasts.
	if err := publisher.SendText("ping"); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	got, err := subscriber.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("read pre-unsub: %v", err)
	}
	if got != "pong" {
		t.Errorf("pre-unsub: got %q, want pong", got)
	}

	// Subscriber unsubscribes.
	if err := subscriber.SendText("leave"); err != nil {
		t.Fatalf("send leave: %v", err)
	}
	confirm, err := subscriber.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("read leave-confirm: %v", err)
	}
	if confirm != "left" {
		t.Errorf("leave confirm: got %q, want left", confirm)
	}

	// Publisher broadcasts again — subscriber should NOT receive.
	if err := publisher.SendText("ping"); err != nil {
		t.Fatalf("send ping 2: %v", err)
	}
	if err := subscriber.expectNoMessage(300 * time.Millisecond); err != nil {
		t.Errorf("subscriber still receiving after unsub: %v", err)
	}
}

// TestWebSocketPublishBatch covers the batched cross-thread publish
// API: one cgo crossing for N messages, mixed topics + opcodes,
// caller buffers reusable immediately. A subscriber registered to
// two topics receives only the messages on its topics; the third
// topic in the batch has no subscriber and is dropped silently.
func TestWebSocketPublishBatch(t *testing.T) {
	appCh := make(chan *gogo.App, 1)
	port, teardown := startApp(t, func(app *gogo.App) {
		appCh <- app
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("alerts")
				ws.Subscribe("news")
			},
		})
	})
	defer teardown()
	app := <-appCh

	sub, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer sub.Close()
	time.Sleep(50 * time.Millisecond)

	// Reusable buffer: PublishBatch must copy bytes before returning
	// so we can mutate this immediately afterward without affecting
	// the in-flight publish.
	payload := []byte("first")
	batch := []gogo.PublishMessage{
		{Topic: "alerts", Message: payload, OpCode: gogo.Text},
		{Topic: "news", Message: []byte("second"), OpCode: gogo.Text},
		{Topic: "private", Message: []byte("dropped"), OpCode: gogo.Text}, // no subscribers
	}
	app.PublishBatch(batch)
	// Mutate caller buffer right away — must NOT affect delivery.
	for i := range payload {
		payload[i] = 'X'
	}

	got1, err := sub.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	got2, err := sub.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if got1 != "first" || got2 != "second" {
		t.Errorf("batch delivery: got %q, %q; want \"first\", \"second\"", got1, got2)
	}
	if err := sub.expectNoMessage(300 * time.Millisecond); err != nil {
		t.Errorf("unexpected extra message: %v", err)
	}

	// Empty batch is a no-op (no panic, no cgo crossing).
	app.PublishBatch(nil)
	app.PublishBatch([]gogo.PublishMessage{})
}

func TestWebSocketPublishAfterCloseNoop(t *testing.T) {
	appCh := make(chan *gogo.App, 1)
	port, teardown := startApp(t, func(app *gogo.App) {
		appCh <- app
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("closed")
			},
		})
	})
	app := <-appCh

	sub, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	sub.Close()
	teardown()

	app.Publish("closed", []byte("single"), gogo.Text)
	app.PublishBatch([]gogo.PublishMessage{
		{Topic: "closed", Message: []byte("batch"), OpCode: gogo.Text},
	})
}

func TestWebSocketPublishRaceWithClose(t *testing.T) {
	appCh := make(chan *gogo.App, 1)
	port, teardown := startApp(t, func(app *gogo.App) {
		appCh <- app
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("race-close")
			},
			MaxBackpressure: 64 * 1024 * 1024,
		})
	})
	app := <-appCh

	sub, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer sub.Close()
	time.Sleep(50 * time.Millisecond)

	var stop atomic.Bool
	var publishers sync.WaitGroup
	publishers.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer publishers.Done()
			batch := []gogo.PublishMessage{
				{Topic: "race-close", Message: []byte("batch"), OpCode: gogo.Text},
			}
			for !stop.Load() {
				app.Publish("race-close", []byte("single"), gogo.Text)
				app.PublishBatch(batch)
			}
			app.Publish("race-close", []byte("after-stop"), gogo.Text)
			app.PublishBatch(batch)
		}()
	}

	time.Sleep(25 * time.Millisecond)
	sub.Close()
	time.Sleep(25 * time.Millisecond)
	stop.Store(true)
	done := make(chan struct{})
	go func() {
		teardown()
		close(done)
	}()
	publishers.Wait()
	<-done
}

// TestWebSocketPublishConcurrent verifies the thread-safety claim
// on App.Publish and App.PublishBatch: dozens of worker goroutines
// can hammer them simultaneously without a race and every message
// the workers issued reaches the subscriber. Run under -race to
// catch any cgo/Loop::defer misuse that escapes the unit tests.
//
// A single subscriber subscribes to "concurrent". 50 goroutines
// each issue 200 publishes (50 single + 50 batches of 3, mixed).
// Expected delivery = 50 × (50 + 50*3) = 10000 messages.
func TestWebSocketPublishConcurrent(t *testing.T) {
	const (
		workers          = 50
		singlesPerWorker = 50
		batchesPerWorker = 50
		batchSize        = 3
	)
	expected := workers * (singlesPerWorker + batchesPerWorker*batchSize)

	appCh := make(chan *gogo.App, 1)
	port, teardown := startApp(t, func(app *gogo.App) {
		appCh <- app
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("concurrent")
			},
			MaxBackpressure: 64 * 1024 * 1024,
		})
	})
	defer teardown()
	app := <-appCh

	sub, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer sub.Close()
	time.Sleep(50 * time.Millisecond)

	// Count messages as they arrive on the subscriber. The drain
	// goroutine runs until it has seen the expected count or the
	// 10 s safety deadline trips.
	var received atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for received.Load() < int64(expected) {
			if _, err := sub.ReadText(10 * time.Second); err != nil {
				return
			}
			received.Add(1)
		}
	}()

	var startWg, doneWg sync.WaitGroup
	startWg.Add(1)
	doneWg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer doneWg.Done()
			startWg.Wait() // align starts to maximize contention
			payload := []byte(fmt.Sprintf("w%d", w))
			batch := make([]gogo.PublishMessage, batchSize)
			for i := range batch {
				batch[i] = gogo.PublishMessage{
					Topic:   "concurrent",
					Message: payload,
					OpCode:  gogo.Text,
				}
			}
			for i := 0; i < singlesPerWorker; i++ {
				app.Publish("concurrent", payload, gogo.Text)
			}
			for i := 0; i < batchesPerWorker; i++ {
				app.PublishBatch(batch)
			}
		}()
	}
	startWg.Done() // release all workers at once
	doneWg.Wait()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("timeout waiting for %d messages, got %d", expected, received.Load())
	}
	if got := received.Load(); got != int64(expected) {
		t.Errorf("delivery mismatch: got %d, want %d", got, expected)
	}
}

// TestWebSocketMultiTopic: a single socket can be subscribed to
// multiple topics. Publishes route only to subscribers of the exact
// topic string — uWS v20 has no wildcard support, so this also
// guards against accidental cross-topic delivery.
func TestWebSocketMultiTopic(t *testing.T) {
	appCh := make(chan *gogo.App, 1)
	port, teardown := startApp(t, func(app *gogo.App) {
		appCh <- app
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.Subscribe("alerts")
				ws.Subscribe("news")
				// Deliberately NOT subscribed to "private".
			},
		})
	})
	defer teardown()
	app := <-appCh

	sub, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial sub: %v", err)
	}
	defer sub.Close()
	// Wait for the Open handler to run and subscribe before publishing.
	time.Sleep(50 * time.Millisecond)

	app.Publish("alerts", []byte("alert 1"), gogo.Text)
	app.Publish("news", []byte("news 1"), gogo.Text)
	// Not subscribed — should not deliver.
	app.Publish("private", []byte("nope"), gogo.Text)

	got1, err := sub.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	got2, err := sub.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	// Both arrived (ordering is delivery order — assume FIFO per loop drain).
	if got1 != "alert 1" || got2 != "news 1" {
		t.Errorf("multi-topic delivery: got %q, %q; want \"alert 1\", \"news 1\"", got1, got2)
	}
	// "private" must NOT have been delivered.
	if err := sub.expectNoMessage(300 * time.Millisecond); err != nil {
		t.Errorf("unexpected message on non-subscribed topic: %v", err)
	}
}

// expected Name / FileName / ContentType / Data.
func TestMultipartIterate(t *testing.T) {
	type seen struct {
		name, fileName, contentType, data string
	}
	type result struct {
		mu    sync.Mutex
		parts []seen
		err   error
	}
	collected := &result{}

	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/upload", 1<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
			err := req.Multipart(func(p *gogo.MultipartPart) error {
				collected.mu.Lock()
				collected.parts = append(collected.parts, seen{
					name: p.Name, fileName: p.FileName,
					contentType: p.ContentType, data: string(p.Data),
				})
				collected.mu.Unlock()
				return nil
			})
			if err != nil {
				collected.mu.Lock()
				collected.err = err
				collected.mu.Unlock()
				res.Send(400, "text/plain", err.Error())
				return
			}
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	mw.WriteField("title", "hello")
	mw.WriteField("note", "world")
	fw, _ := mw.CreateFormFile("attachment", "report.txt")
	fw.Write([]byte("file body 1"))
	imgWriter, _ := mw.CreatePart(textproto.MIMEHeader{
		"Content-Disposition": []string{`form-data; name="image"; filename="pic.png"`},
		"Content-Type":        []string{"image/png"},
	})
	imgWriter.Write([]byte("\x89PNG\r\n\x1a\nfake"))
	mw.Close()

	status, respBody := httpPost(t, port, "/upload", mw.FormDataContentType(), body.Bytes())
	if status != 200 || respBody != "ok" {
		t.Fatalf("upload: got %d %q", status, respBody)
	}

	collected.mu.Lock()
	defer collected.mu.Unlock()
	if collected.err != nil {
		t.Fatalf("Multipart returned err: %v", collected.err)
	}
	if len(collected.parts) != 4 {
		t.Fatalf("got %d parts, want 4: %+v", len(collected.parts), collected.parts)
	}
	// Value parts come without a filename.
	if collected.parts[0] != (seen{name: "title", data: "hello"}) {
		t.Errorf("part 0: %+v", collected.parts[0])
	}
	if collected.parts[1] != (seen{name: "note", data: "world"}) {
		t.Errorf("part 1: %+v", collected.parts[1])
	}
	// File parts carry FileName + Content-Type sniffed by the framework
	// (defaults to application/octet-stream for the generic file).
	if collected.parts[2].name != "attachment" || collected.parts[2].fileName != "report.txt" ||
		collected.parts[2].data != "file body 1" {
		t.Errorf("part 2: %+v", collected.parts[2])
	}
	if collected.parts[3].name != "image" || collected.parts[3].fileName != "pic.png" ||
		collected.parts[3].contentType != "image/png" {
		t.Errorf("part 3: %+v", collected.parts[3])
	}
}

// TestMultipartSaveInto: SaveInto writes file parts under a directory
// using the basename of FileName, ignoring path components in the
// supplied name.
func TestMultipartSaveInto(t *testing.T) {
	dir := t.TempDir()
	savedPath := make(chan string, 1)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/u", 1<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
			err := req.Multipart(func(p *gogo.MultipartPart) error {
				if !p.IsFile() {
					return nil
				}
				path, err := p.SaveInto(dir)
				if err != nil {
					return err
				}
				savedPath <- path
				return nil
			})
			if err != nil {
				res.Send(400, "text/plain", err.Error())
				return
			}
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	fw, _ := mw.CreateFormFile("up", "evil.txt")
	fw.Write([]byte("hello disk"))
	mw.Close()

	status, _ := httpPost(t, port, "/u", mw.FormDataContentType(), body.Bytes())
	if status != 200 {
		t.Fatalf("save: status %d", status)
	}
	path := <-savedPath
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read saved: %v", err)
	}
	if string(got) != "hello disk" {
		t.Errorf("file contents: got %q, want %q", got, "hello disk")
	}
	if filepath.Dir(path) != dir {
		t.Errorf("saved outside dir: got %s, want under %s", path, dir)
	}
}

func TestMultipartRejectsOversizePart(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/u", 1<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
			err := req.MultipartWithOptions(gogo.MultipartOptions{MaxPartBytes: 8}, func(p *gogo.MultipartPart) error {
				return nil
			})
			if errors.Is(err, gogo.ErrMultipartPartTooLarge) {
				res.Send(413, "text/plain", "part too large")
				return
			}
			if err != nil {
				res.Send(500, "text/plain", err.Error())
				return
			}
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	fw, _ := mw.CreateFormFile("up", "big.txt")
	fw.Write([]byte("0123456789abcdef"))
	mw.Close()

	status, respBody := httpPost(t, port, "/u", mw.FormDataContentType(), body.Bytes())
	if status != 413 || respBody != "part too large" {
		t.Fatalf("oversize part: got %d %q, want 413", status, respBody)
	}
}

func TestMultipartStreamSaveInto(t *testing.T) {
	dir := t.TempDir()
	savedPath := make(chan string, 1)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/u", 1<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
			err := req.MultipartStream(gogo.MultipartOptions{MaxPartBytes: 1 << 10}, func(p *gogo.MultipartStreamPart) error {
				if !p.IsFile() {
					return nil
				}
				path, err := p.SaveInto(dir)
				if err != nil {
					return err
				}
				savedPath <- path
				return nil
			})
			if err != nil {
				res.Send(500, "text/plain", err.Error())
				return
			}
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	fw, _ := mw.CreateFormFile("up", "stream.txt")
	fw.Write([]byte("hello streamed disk"))
	mw.Close()

	status, _ := httpPost(t, port, "/u", mw.FormDataContentType(), body.Bytes())
	if status != 200 {
		t.Fatalf("stream save: status %d", status)
	}
	got, err := os.ReadFile(<-savedPath)
	if err != nil {
		t.Fatalf("read saved: %v", err)
	}
	if string(got) != "hello streamed disk" {
		t.Errorf("saved body = %q", string(got))
	}
}

// TestMultipartSaveIntoRejectsTraversal: SaveInto strips path
// components from FileName so a "../../etc/passwd"-style filename
// can't escape the target directory.
func TestMultipartSaveIntoRejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	type saveResult struct {
		path string
		err  error
	}
	resultCh := make(chan saveResult, 1)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/u", 1<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
			err := req.Multipart(func(p *gogo.MultipartPart) error {
				if !p.IsFile() {
					return nil
				}
				path, err := p.SaveInto(dir)
				resultCh <- saveResult{path: path, err: err}
				return nil
			})
			if err != nil {
				res.Send(500, "text/plain", err.Error())
				return
			}
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	part, _ := mw.CreatePart(textproto.MIMEHeader{
		"Content-Disposition": []string{`form-data; name="up"; filename="../../escape.txt"`},
	})
	part.Write([]byte("nope"))
	mw.Close()

	status, _ := httpPost(t, port, "/u", mw.FormDataContentType(), body.Bytes())
	if status != 200 {
		t.Fatalf("status %d", status)
	}
	r := <-resultCh
	if r.err != nil {
		t.Fatalf("SaveInto err: %v", r.err)
	}
	// Basename of "../../escape.txt" is "escape.txt" — SaveInto writes
	// to dir/escape.txt, not to the traversed location.
	wantPath := filepath.Join(dir, "escape.txt")
	if r.path != wantPath {
		t.Errorf("savedPath: got %q, want %q", r.path, wantPath)
	}
	if _, err := os.Stat(wantPath); err != nil {
		t.Errorf("expected file at %s: %v", wantPath, err)
	}
}

// TestMultipartCallbackError: returning a non-nil error from the
// callback stops iteration and surfaces verbatim.
func TestMultipartCallbackError(t *testing.T) {
	stopErr := errors.New("user stop")
	errCh := make(chan error, 1)

	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/u", 1<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
			seen := 0
			err := req.Multipart(func(p *gogo.MultipartPart) error {
				seen++
				if seen == 2 {
					return stopErr
				}
				return nil
			})
			errCh <- err
			if err != nil {
				res.Send(400, "text/plain", err.Error())
				return
			}
			res.Send(200, "text/plain", fmt.Sprintf("seen=%d", seen))
		})
	})
	defer teardown()

	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	mw.WriteField("a", "1")
	mw.WriteField("b", "2")
	mw.WriteField("c", "3")
	mw.Close()

	status, respBody := httpPost(t, port, "/u", mw.FormDataContentType(), body.Bytes())
	if status != 400 || respBody != "user stop" {
		t.Fatalf("stop: got %d %q", status, respBody)
	}
	if got := <-errCh; !errors.Is(got, stopErr) {
		t.Errorf("ParseMultipart err: %v, want %v", got, stopErr)
	}
}

// TestMultipartUnsupportedMediaType: a non-multipart Content-Type
// surfaces ErrUnsupportedMediaType — callers map to 415.
func TestMultipartUnsupportedMediaType(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.PostAsync("/u", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			err := req.Multipart(func(p *gogo.MultipartPart) error { return nil })
			if errors.Is(err, gogo.ErrUnsupportedMediaType) {
				res.Send(415, "text/plain", "unsupported")
				return
			}
			res.Send(500, "text/plain", "unexpected: "+fmt.Sprint(err))
		})
	})
	defer teardown()

	status, _ := httpPost(t, port, "/u", "application/json", []byte(`{}`))
	if status != 415 {
		t.Errorf("unsupported: status %d, want 415", status)
	}
}

// TestParseMultipartDirect: package-level helper works without a
// Request, for sync handlers that collect the body via Response.Body.
func TestParseMultipartDirect(t *testing.T) {
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	mw.WriteField("k", "v")
	fw, _ := mw.CreateFormFile("file", "x.bin")
	fw.Write([]byte("BIN"))
	mw.Close()

	var parts []string
	err := gogo.ParseMultipart(mw.FormDataContentType(), body.Bytes(), func(p *gogo.MultipartPart) error {
		parts = append(parts, fmt.Sprintf("%s=%s(%q)", p.Name, p.FileName, p.Data))
		return nil
	})
	if err != nil {
		t.Fatalf("ParseMultipart: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("parts: %d, want 2: %v", len(parts), parts)
	}
}
