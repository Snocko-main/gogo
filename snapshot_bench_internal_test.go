//go:build cgo && gogo

package gogo

import (
	"runtime"
	"strings"
	"testing"
	"unsafe"
)

// buildBenchCtx lays out a realistic GET /user/42 request snapshot in a
// Go-allocated buffer shaped like the C++ AsyncCtx (offsets come from the
// live uwsgo_shared_layout). The buffer is pinned so the uintptr handed to
// newSnapshotFromCtx stays valid for the benchmark's duration.
func buildBenchCtx(b *testing.B, headers string) (uintptr, *runtime.Pinner) {
	b.Helper()
	initSharedLayout()

	buf := make([]byte, 64<<10)
	pinner := &runtime.Pinner{}
	pinner.Pin(&buf[0])
	base := uintptr(unsafe.Pointer(&buf[0]))

	put := func(off uintptr, s string) {
		copy(buf[off:], s)
	}
	putLen := func(off uintptr, n int) {
		*(*uint32)(unsafe.Pointer(base + off)) = uint32(n)
	}

	method := "GET"
	url := "/user/42"
	ip := "127.0.0.1"
	param0 := "42"

	put(shared.ctxMethodOff, method)
	putLen(shared.ctxMethodLenOff, len(method))
	put(shared.ctxURLOff, url)
	putLen(shared.ctxURLLenOff, len(url))
	putLen(shared.ctxQueryLenOff, 0)
	put(shared.ctxIPOff, ip)
	putLen(shared.ctxIPLenOff, len(ip))
	put(shared.ctxHeadersOff, headers)
	putLen(shared.ctxHeadersLenOff, len(headers))
	putLen(shared.ctxParamCountOff, 1)
	put(shared.ctxParamsOff, param0)
	putLen(shared.ctxParamLensOff, len(param0))
	putLen(shared.ctxTruncatedOff, 0)

	return base, pinner
}

// wrkHeaders mirrors what wrk sends: three small headers (~56 bytes).
const wrkHeaders = "host\x00127.0.0.1:3002\x00user-agent\x00wrk/4.1.0\x00accept\x00*/*\x00"

// browserHeaders mirrors a realistic Chrome request with a session cookie:
// ~1.1 KiB across 13 headers — the shape production traffic actually has.
var browserHeaders = strings.Join([]string{
	"host\x00api.example.com\x00",
	"user-agent\x00Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36\x00",
	"accept\x00text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8\x00",
	"accept-encoding\x00gzip, deflate, br, zstd\x00",
	"accept-language\x00en-US,en;q=0.9,th;q=0.8\x00",
	"cache-control\x00no-cache\x00",
	"cookie\x00session=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJVadQssw5c; _ga=GA1.2.123456789.1700000000; _gid=GA1.2.987654321.1700000000; theme=dark; lang=th\x00",
	"referer\x00https://app.example.com/dashboard\x00",
	"sec-ch-ua\x00\"Chromium\";v=\"126\", \"Google Chrome\";v=\"126\"\x00",
	"sec-fetch-dest\x00document\x00",
	"sec-fetch-mode\x00navigate\x00",
	"authorization\x00Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6ImFiYzEyMyJ9.eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlIiwiYXVkIjoiYXBpIn0.signature_payload_padding_padding\x00",
	"x-request-id\x00req_8f14e45fceea167a5a36dedd4bea2543\x00",
}, "")

// BenchmarkNewSnapshotFromCtx measures the per-request cost of capturing the
// C++-written request snapshot on the shared-dispatch worker path, with
// wrk-sized (~56 B) headers.
func BenchmarkNewSnapshotFromCtx(b *testing.B) {
	ctxPtr, pinner := buildBenchCtx(b, wrkHeaders)
	defer pinner.Unpin()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap := newSnapshotFromCtx(ctxPtr)
		if snap.url != "/user/42" || snap.params[0] != "42" {
			b.Fatalf("bad snapshot: url=%q params=%v", snap.url, snap.params)
		}
	}
}

// BenchmarkNewSnapshotFromCtxBrowser is the same with a realistic ~1.1 KiB
// browser header set — the case the lazy-header change targets.
func BenchmarkNewSnapshotFromCtxBrowser(b *testing.B) {
	ctxPtr, pinner := buildBenchCtx(b, browserHeaders)
	defer pinner.Unpin()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap := newSnapshotFromCtx(ctxPtr)
		if snap.url != "/user/42" {
			b.Fatalf("bad snapshot: url=%q", snap.url)
		}
	}
}

// BenchmarkSnapshotBrowserWithLookup adds the auth-middleware access
// pattern: build the snapshot, then read two headers from it.
func BenchmarkSnapshotBrowserWithLookup(b *testing.B) {
	ctxPtr, pinner := buildBenchCtx(b, browserHeaders)
	defer pinner.Unpin()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap := newSnapshotFromCtx(ctxPtr)
		if v := snap.lookupHeader("authorization"); len(v) == 0 {
			b.Fatal("missing authorization header")
		}
		if v := snap.lookupHeader("x-request-id"); len(v) == 0 {
			b.Fatal("missing x-request-id header")
		}
	}
}
