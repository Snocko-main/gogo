//go:build cgo && gogo

package middleware_test

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

func TestRateLimitAllowsUnderLimit(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.RateLimit(middleware.RateLimitOptions{
			Max:    3,
			Window: time.Second,
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	for i := 0; i < 3; i++ {
		resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
		if err != nil {
			t.Fatalf("req %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("req %d: got %d", i, resp.StatusCode)
		}
		remaining := resp.Header.Get("X-RateLimit-Remaining")
		want := strconv.Itoa(2 - i)
		if remaining != want {
			t.Errorf("req %d: remaining %q want %q", i, remaining, want)
		}
	}
}

func TestRateLimitRejectsOverLimit(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.RateLimit(middleware.RateLimitOptions{
			Max:    2,
			Window: 5 * time.Second,
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	for i := 0; i < 2; i++ {
		resp, _ := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
		if resp != nil {
			resp.Body.Close()
		}
	}
	// 3rd request should be 429.
	resp, err := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	if err != nil {
		t.Fatalf("req 3: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 429 {
		t.Errorf("got %d want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Errorf("Retry-After missing on 429")
	}
}

func TestRateLimitWindowResets(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.RateLimit(middleware.RateLimitOptions{
			Max:    1,
			Window: 250 * time.Millisecond,
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	r1, _ := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	r1.Body.Close()
	r2, _ := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	r2.Body.Close()
	if r2.StatusCode != 429 {
		t.Fatalf("r2: want 429 got %d", r2.StatusCode)
	}
	time.Sleep(300 * time.Millisecond)
	r3, _ := noKeepaliveClient.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	r3.Body.Close()
	if r3.StatusCode != 200 {
		t.Fatalf("r3 after window: want 200 got %d", r3.StatusCode)
	}
}

func TestRateLimitKeyFunc(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Use(middleware.RateLimit(middleware.RateLimitOptions{
			Max:    1,
			Window: 5 * time.Second,
			KeyFunc: func(req *gogo.Request) string {
				return req.Header("x-tenant")
			},
		}))
		app.Get("/", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	defer teardown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", port)
	do := func(tenant string) int {
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("x-tenant", tenant)
		resp, err := noKeepaliveClient.Do(req)
		if err != nil {
			t.Fatalf("tenant %s: %v", tenant, err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if c := do("A"); c != 200 {
		t.Fatalf("tenant A 1st: got %d", c)
	}
	if c := do("A"); c != 429 {
		t.Fatalf("tenant A 2nd: got %d", c)
	}
	if c := do("B"); c != 200 {
		t.Fatalf("tenant B: got %d", c)
	}
}

func TestRateLimitMemoryStoreConcurrent(t *testing.T) {
	store := middleware.NewMemoryRateLimitStore()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				store.Hit("k", time.Second)
			}
		}()
	}
	wg.Wait()
	// 50 * 100 = 5000 hits.
	c, _ := store.Hit("k", time.Second)
	if c != 5001 {
		t.Errorf("count %d want 5001", c)
	}
}
