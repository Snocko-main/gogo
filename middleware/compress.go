package middleware

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"strings"
	"sync"

	"github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/internal/mwhint"
)

// CompressOptions configures Compress. Zero value uses
// gzip.DefaultCompression with a 1 KiB MinSize threshold, a 4 MiB
// MaxSize cap, and the standard "compressible content-type" filter
// (text/*, application/json, application/javascript, application/xml,
// image/svg+xml).
type CompressOptions struct {
	// Level is the gzip / deflate compression level. Valid range
	// 0 (no compression) to 9 (best). Default
	// gzip.DefaultCompression (-1).
	Level int

	// MinSize skips compression when the buffered body is smaller
	// than this many bytes — small payloads are not worth the CPU
	// and the gzip framing overhead often makes the result larger.
	// Default 1024.
	MinSize int

	// MaxSize skips compression when the buffered body is LARGER
	// than this many bytes. The compression step allocates a
	// secondary buffer ~half the input size to hold the encoded
	// output, plus the CPU to run gzip / deflate over the whole
	// thing — both linear in body size, both bounded only by the
	// handler's own response size before this cap was introduced.
	// A 100 MiB dynamic JSON response would peak around 150 MiB of
	// resident heap during compression; concurrent requests
	// multiply that. With the cap in place, bodies larger than
	// MaxSize emit uncompressed (no Content-Encoding header) so
	// downstream proxies / CDNs can apply their own compression if
	// desired.
	//
	// Default 4 MiB. Set to 0 to disable the cap (matches the
	// pre-cap behavior — every body of any size gets compressed if
	// it passes the other filters). Negative is treated as zero.
	//
	// Note: this caps the COMPRESSION work, not the buffering. The
	// handler's bytes still flow through the encoder's staging
	// buffer; for truly streaming responses (SSE, large file
	// downloads) use SkipFunc to bypass the encoder entirely, or
	// use Response.Stream which doesn't route through this hook.
	MaxSize int

	// Filter, when non-nil, is consulted after the body is buffered
	// to decide whether the response is worth compressing. The
	// default accepts standard text-ish content types and skips
	// pre-compressed media (images, archives). Override to add
	// app-specific types.
	Filter func(contentType string) bool

	// SkipFunc, when non-nil and returning true, bypasses the
	// middleware entirely for that request — useful to spare CPU
	// for streaming endpoints (Server-Sent Events, file
	// downloads) where buffering the whole body defeats the point.
	SkipFunc func(*gogo.Request) bool
}

// Compress returns a Middleware that gzips response bodies for
// clients that advertise gzip / deflate support via Accept-Encoding.
// The middleware buffers the handler's Write / End / Send / JSON
// output via Response.SetBodyEncoder, then compresses on flush.
// Both sync and async handlers (Response.Async, PostAsync) are
// supported — when extra headers are present the async path
// transparently falls back from the zero-cgo shared-memory path to
// the defer-send-with-headers shim that carries Content-Encoding
// and Vary alongside the body.
//
//	app.Use(middleware.Compress())
//
// Limitations
//
// SendFile is not compressed — the static-file path streams the
// file directly to uWS without flowing through Write / End.
//
// Brotli is not bundled: it requires a separate Go dependency
// (e.g. github.com/andybalholm/brotli) and is left for callers who
// want it to wire up via their own encoder. Use SetBodyEncoder
// directly from a custom middleware for that.
func Compress(opts ...CompressOptions) mwhint.Hinted {
	var opt CompressOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.Level == 0 {
		opt.Level = gzip.DefaultCompression
	}
	if opt.MinSize == 0 {
		opt.MinSize = 1024
	}
	if opt.MaxSize == 0 {
		opt.MaxSize = 4 << 20 // 4 MiB — bounds compression CPU + transient memory
	} else if opt.MaxSize < 0 {
		opt.MaxSize = 0 // explicit "disable cap" sentinel
	}
	if opt.Filter == nil {
		opt.Filter = defaultCompressFilter
	}

	gzipPool := newGzipWriterPool(opt.Level)
	flatePool := newFlateWriterPool(opt.Level)

	return mwhint.Hinted{Place: mwhint.Both, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			if opt.SkipFunc != nil && opt.SkipFunc(req) {
				next(res, req)
				return
			}
			encoding := negotiateEncoding(req.Header("accept-encoding"))
			if encoding == "" {
				next(res, req)
				return
			}
			res.SetBodyEncoder(func(body []byte, contentType string) ([]byte, string) {
				if len(body) < opt.MinSize {
					return body, ""
				}
				if opt.MaxSize > 0 && len(body) > opt.MaxSize {
					// Compressing a multi-MiB body would allocate a
					// secondary buffer of similar size and burn
					// noticeable CPU. Emit uncompressed instead so
					// concurrent large requests don't spike the
					// process's resident memory.
					return body, ""
				}
				if contentType != "" && !opt.Filter(contentType) {
					return body, ""
				}
				switch encoding {
				case "gzip":
					return gzipPool.compress(body), "gzip"
				case "deflate":
					return flatePool.compress(body), "deflate"
				}
				return body, ""
			})
			next(res, req)
		}
	})}
}

// negotiateEncoding returns "gzip", "deflate", or "" — picking the
// first supported encoding announced by the client. We prefer gzip
// because it's universally supported and slightly cheaper than
// deflate to produce. q-values aren't honored beyond "q=0" meaning
// "do not use"; that matches what nginx / fasthttp do.
func negotiateEncoding(accept string) string {
	if accept == "" {
		return ""
	}
	hasGzip, hasDeflate := false, false
	for _, part := range strings.Split(accept, ",") {
		coding := strings.TrimSpace(part)
		var q string
		if semi := strings.IndexByte(coding, ';'); semi >= 0 {
			q = strings.TrimSpace(coding[semi+1:])
			coding = strings.TrimSpace(coding[:semi])
		}
		// "q=0" means "do not use". Anything else (including no q
		// parameter) is acceptable for our purposes.
		if strings.HasPrefix(q, "q=0") && q != "q=0.0" && (q == "q=0" || !strings.ContainsAny(q[2:], "123456789")) {
			continue
		}
		switch strings.ToLower(coding) {
		case "gzip":
			hasGzip = true
		case "deflate":
			hasDeflate = true
		}
	}
	if hasGzip {
		return "gzip"
	}
	if hasDeflate {
		return "deflate"
	}
	return ""
}

func defaultCompressFilter(contentType string) bool {
	if contentType == "" {
		return true
	}
	ct := strings.ToLower(contentType)
	// Strip any "; charset=..." suffix.
	if semi := strings.IndexByte(ct, ';'); semi >= 0 {
		ct = strings.TrimSpace(ct[:semi])
	}
	if strings.HasPrefix(ct, "text/") {
		return true
	}
	switch ct {
	case "application/json",
		"application/javascript",
		"application/xml",
		"application/xhtml+xml",
		"application/x-javascript",
		"application/manifest+json",
		"application/rss+xml",
		"application/atom+xml",
		"image/svg+xml":
		return true
	}
	return false
}

// gzipWriterPool reuses gzip.Writer instances to avoid the
// per-request allocation. Level is fixed at pool creation; create
// a separate pool for each level you need.
type gzipWriterPool struct {
	level int
	pool  sync.Pool
}

func newGzipWriterPool(level int) *gzipWriterPool {
	p := &gzipWriterPool{level: level}
	p.pool.New = func() any {
		w, _ := gzip.NewWriterLevel(nil, level)
		return w
	}
	return p
}

func (p *gzipWriterPool) compress(body []byte) []byte {
	w := p.pool.Get().(*gzip.Writer)
	defer p.pool.Put(w)
	var buf bytes.Buffer
	buf.Grow(len(body) / 2)
	w.Reset(&buf)
	_, _ = w.Write(body)
	_ = w.Close()
	return buf.Bytes()
}

type flateWriterPool struct {
	level int
	pool  sync.Pool
}

func newFlateWriterPool(level int) *flateWriterPool {
	p := &flateWriterPool{level: level}
	p.pool.New = func() any {
		w, _ := flate.NewWriter(nil, level)
		return w
	}
	return p
}

func (p *flateWriterPool) compress(body []byte) []byte {
	w := p.pool.Get().(*flate.Writer)
	defer p.pool.Put(w)
	var buf bytes.Buffer
	buf.Grow(len(body) / 2)
	w.Reset(&buf)
	_, _ = w.Write(body)
	_ = w.Close()
	return buf.Bytes()
}
