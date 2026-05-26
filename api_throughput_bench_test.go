//go:build cgo && gogo

package gogo_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
)

func runAPIGetBench(b *testing.B, configure func(*gogo.App), path string, bytesPerReq int64) {
	runAPIGetBenchCfg(b, gogo.Config{}, configure, path, bytesPerReq)
}

func runAPIGetBenchCfg(b *testing.B, cfg gogo.Config, configure func(*gogo.App), path string, bytesPerReq int64) {
	b.Helper()
	b.ReportAllocs()
	if bytesPerReq > 0 {
		b.SetBytes(bytesPerReq)
	}

	port, teardown := startAppCfg(b, cfg, configure)
	defer teardown()

	tr := &http.Transport{
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	url := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)

	for i := 0; i < 100; i++ {
		resp, err := client.Get(url)
		if err != nil {
			b.Fatalf("warmup: %v", err)
		}
		drainBenchResponse(b, resp)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(url)
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		drainBenchResponse(b, resp)
	}
}

func runAPIGetParallelBench(b *testing.B, configure func(*gogo.App), path string, bytesPerReq int64) {
	runAPIGetParallelBenchCfg(b, gogo.Config{}, configure, path, bytesPerReq)
}

func runAPIGetParallelBenchCfg(b *testing.B, cfg gogo.Config, configure func(*gogo.App), path string, bytesPerReq int64) {
	b.Helper()
	b.ReportAllocs()
	if bytesPerReq > 0 {
		b.SetBytes(bytesPerReq)
	}

	port, teardown := startAppCfg(b, cfg, configure)
	defer teardown()

	tr := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	url := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)

	for i := 0; i < 100; i++ {
		resp, err := client.Get(url)
		if err != nil {
			b.Fatalf("warmup: %v", err)
		}
		drainBenchResponse(b, resp)
	}

	var failed atomic.Bool
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if failed.Load() {
				continue
			}
			resp, err := client.Get(url)
			if err != nil {
				if failed.CompareAndSwap(false, true) {
					b.Errorf("get: %v", err)
				}
				continue
			}
			if err := drainBenchResponseErr(resp); err != nil {
				if failed.CompareAndSwap(false, true) {
					b.Error(err)
				}
			}
		}
	})
	if failed.Load() {
		b.FailNow()
	}
}

func runAPIPostBench(b *testing.B, configure func(*gogo.App), path string, contentType string, payload []byte) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	port, teardown := startApp(b, configure)
	defer teardown()

	tr := &http.Transport{
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	url := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)

	makeReq := func() *http.Request {
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			b.Fatalf("new request: %v", err)
		}
		req.Header.Set("Content-Type", contentType)
		return req
	}

	for i := 0; i < 100; i++ {
		resp, err := client.Do(makeReq())
		if err != nil {
			b.Fatalf("warmup: %v", err)
		}
		drainBenchResponse(b, resp)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Do(makeReq())
		if err != nil {
			b.Fatalf("post: %v", err)
		}
		drainBenchResponse(b, resp)
	}
}

func runAPIPostParallelBench(b *testing.B, configure func(*gogo.App), path string, contentType string, payload []byte) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	port, teardown := startApp(b, configure)
	defer teardown()

	tr := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	url := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)

	makeReq := func() *http.Request {
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			b.Fatalf("new request: %v", err)
		}
		req.Header.Set("Content-Type", contentType)
		return req
	}

	for i := 0; i < 100; i++ {
		resp, err := client.Do(makeReq())
		if err != nil {
			b.Fatalf("warmup: %v", err)
		}
		drainBenchResponse(b, resp)
	}

	var failed atomic.Bool
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if failed.Load() {
				continue
			}
			resp, err := client.Do(makeReq())
			if err != nil {
				if failed.CompareAndSwap(false, true) {
					b.Errorf("post: %v", err)
				}
				continue
			}
			if err := drainBenchResponseErr(resp); err != nil {
				if failed.CompareAndSwap(false, true) {
					b.Error(err)
				}
			}
		}
	})
	if failed.Load() {
		b.FailNow()
	}
}

func drainBenchResponse(b *testing.B, resp *http.Response) {
	b.Helper()
	if err := drainBenchResponseErr(resp); err != nil {
		b.Fatal(err)
	}
}

func drainBenchResponseErr(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return fmt.Errorf("status = %d, want 200; body=%q", resp.StatusCode, string(body))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

func BenchmarkRequest_PostAsync_TypedParam_JSONBytes(b *testing.B) {
	payload := []byte(`{"hello":"world","n":42,"active":true}`)
	runAPIPostBench(b, func(app *gogo.App) {
		app.PostAsync("/echo/:id<int>", 4096, func(res *gogo.Response, req *gogo.Request, body []byte) {
			if req.ParamInt("id", 0) != 42 {
				res.Send(500, "text/plain", "bad id")
				return
			}
			res.JSONBytes(200, body)
		})
	}, "/echo/42", "application/json", payload)
}

func BenchmarkRequest_PostAsync_Small_JSONBytes(b *testing.B) {
	payload := []byte(`{"hello":"world","n":42,"active":true}`)
	runAPIPostBench(b, func(app *gogo.App) {
		app.PostAsync("/echo", 4096, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.JSONBytes(200, body)
		})
	}, "/echo", "application/json", payload)
}

func BenchmarkRequest_PostAsync_LargeLimitSmallBody_JSONBytes(b *testing.B) {
	payload := []byte(`{"hello":"world","n":42,"active":true}`)
	runAPIPostBench(b, func(app *gogo.App) {
		app.PostAsync("/echo", 32*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.JSONBytes(200, body)
		})
	}, "/echo", "application/json", payload)
}

func BenchmarkRequest_PostAsync_Small_JSONBytes_Parallel(b *testing.B) {
	payload := []byte(`{"hello":"world","n":42,"active":true}`)
	runAPIPostParallelBench(b, func(app *gogo.App) {
		app.PostAsync("/echo", 4096, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.JSONBytes(200, body)
		})
	}, "/echo", "application/json", payload)
}

func BenchmarkRequest_PostAsync_LargeLimitSmallBody_JSONBytes_Parallel(b *testing.B) {
	payload := []byte(`{"hello":"world","n":42,"active":true}`)
	runAPIPostParallelBench(b, func(app *gogo.App) {
		app.PostAsync("/echo", 32*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.JSONBytes(200, body)
		})
	}, "/echo", "application/json", payload)
}

func BenchmarkRequest_PostAsync_LargeFallback_JSONBytes(b *testing.B) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1024)
	runAPIPostBench(b, func(app *gogo.App) {
		app.PostAsync("/upload", 32*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
			res.JSONBytes(200, body[:len(body):len(body)])
		})
	}, "/upload", "application/json", payload)
}

func BenchmarkRequest_Async_JSONBytes(b *testing.B) {
	payload := []byte(`{"service":"gogo","ok":true,"items":[1,2,3,4]}`)
	runAPIGetBench(b, func(app *gogo.App) {
		app.GetAsync("/json", func(res *gogo.Response, req *gogo.Request) {
			res.JSONBytes(200, payload)
		})
	}, "/json", int64(len(payload)))
}

func BenchmarkRequest_Async_JSONBytes_SyncEntryGetAsync(b *testing.B) {
	payload := []byte(`{"service":"gogo","ok":true,"items":[1,2,3,4]}`)
	runAPIGetBenchCfg(b, gogo.Config{SyncEntryGetAsync: true}, func(app *gogo.App) {
		app.GetAsync("/json", func(res *gogo.Response, req *gogo.Request) {
			res.JSONBytes(200, payload)
		})
	}, "/json", int64(len(payload)))
}

func BenchmarkRequest_Async_JSONBytes_Parallel(b *testing.B) {
	payload := []byte(`{"service":"gogo","ok":true,"items":[1,2,3,4]}`)
	runAPIGetParallelBench(b, func(app *gogo.App) {
		app.GetAsync("/json", func(res *gogo.Response, req *gogo.Request) {
			res.JSONBytes(200, payload)
		})
	}, "/json", int64(len(payload)))
}

func BenchmarkRequest_Async_JSONBytes_SyncEntryGetAsync_Parallel(b *testing.B) {
	payload := []byte(`{"service":"gogo","ok":true,"items":[1,2,3,4]}`)
	runAPIGetParallelBenchCfg(b, gogo.Config{SyncEntryGetAsync: true}, func(app *gogo.App) {
		app.GetAsync("/json", func(res *gogo.Response, req *gogo.Request) {
			res.JSONBytes(200, payload)
		})
	}, "/json", int64(len(payload)))
}

func BenchmarkRequest_Stream_Chunks(b *testing.B) {
	chunk := bytes.Repeat([]byte("x"), 1024)
	const chunks = 8

	prev := gogo.GetStreamBackpressureBytes()
	gogo.SetStreamBackpressureBytes(0)
	b.Cleanup(func() {
		gogo.SetStreamBackpressureBytes(prev)
	})

	runAPIGetBench(b, func(app *gogo.App) {
		app.GetAsync("/stream", func(res *gogo.Response, req *gogo.Request) {
			_ = res.Stream(200, "application/octet-stream", func(w io.Writer) error {
				for i := 0; i < chunks; i++ {
					if _, err := w.Write(chunk); err != nil {
						return err
					}
				}
				return nil
			})
		})
	}, "/stream", int64(len(chunk)*chunks))
}
