//go:build cgo && gogo

package gogo

import (
	"runtime"
	"testing"
	"unsafe"
)

// buildBenchCtx lays out a realistic GET /user/42 request snapshot in a
// Go-allocated buffer shaped like the C++ AsyncCtx (offsets come from the
// live uwsgo_shared_layout). The buffer is pinned so the uintptr handed to
// newSnapshotFromCtx stays valid for the benchmark's duration.
func buildBenchCtx(b *testing.B) (uintptr, *runtime.Pinner) {
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
	headers := "host\x00127.0.0.1:3002\x00user-agent\x00wrk/4.1.0\x00accept\x00*/*\x00"
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

// BenchmarkNewSnapshotFromCtx measures the per-request cost of copying the
// C++-captured request snapshot into Go-owned memory on the shared-dispatch
// worker path.
func BenchmarkNewSnapshotFromCtx(b *testing.B) {
	ctxPtr, pinner := buildBenchCtx(b)
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
