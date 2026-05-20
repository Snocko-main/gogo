package gogo

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

// DefaultMultipartPartLimit is the process default used by ParseMultipart when
// MultipartOptions.MaxPartBytes is zero. It remains assignable for backward
// compatibility with earlier versions; prefer SetDefaultMultipartPartLimit for
// runtime changes so readers observe the update atomically.
var DefaultMultipartPartLimit int64 = 8 << 20

// SetDefaultMultipartPartLimit sets the process-wide multipart per-part cap
// used when MultipartOptions.MaxPartBytes is zero. Values at or below zero
// disable the default cap. Prefer explicit MultipartOptions for per-route
// policies.
func SetDefaultMultipartPartLimit(maxBytes int64) {
	atomic.StoreInt64(&DefaultMultipartPartLimit, maxBytes)
}

// GetDefaultMultipartPartLimit returns the process-wide multipart per-part cap.
func GetDefaultMultipartPartLimit() int64 {
	return atomic.LoadInt64(&DefaultMultipartPartLimit)
}

// ErrMultipartPartTooLarge is returned when a multipart part exceeds the
// configured per-part limit.
var ErrMultipartPartTooLarge = errors.New("gogo: multipart part exceeds max size")

// MultipartOptions configures ParseMultipartWithOptions and
// ParseMultipartStream.
type MultipartOptions struct {
	// MaxPartBytes caps bytes read for each individual part. Zero uses
	// GetDefaultMultipartPartLimit; negative disables the per-part cap.
	MaxPartBytes int64
}

// MultipartPart is one chunk of a parsed multipart/form-data body.
// Returned to the callback passed to ParseMultipart / Request.Multipart.
//
// The Data slice is valid after the callback returns, but retaining it also
// retains that part's bytes. Use MultipartStream when file parts should be
// copied to disk or another writer without an extra per-part allocation.
type MultipartPart struct {
	// Name is the form field name (the "name" attribute of the
	// originating <input>). Empty when the part has no
	// Content-Disposition name parameter.
	Name string

	// FileName is the original filename for file parts (the
	// "filename" attribute of Content-Disposition). Empty for
	// non-file parts.
	FileName string

	// ContentType is the value of the part's Content-Type header.
	// Defaults to "" when omitted by the client — handlers that
	// care should fall back to detecting from FileName or Data.
	ContentType string

	// Data is the raw bytes of this part. For file parts: the file
	// contents. For value parts: the field value bytes.
	Data []byte

	// Header carries every header the client sent on this part —
	// Content-Type, Content-Disposition, and any custom headers
	// like Content-Transfer-Encoding. Use for advanced inspection;
	// the convenience fields above cover the common cases.
	Header textproto.MIMEHeader
}

// IsFile reports whether this part carries a file upload (has a
// FileName). Convenience for the common dispatch on file-vs-value
// parts.
func (p *MultipartPart) IsFile() bool { return p.FileName != "" }

// SaveAt writes the part's Data to dst using os.WriteFile with mode
// 0o644. dst is opened with O_WRONLY|O_CREATE|O_TRUNC and is closed
// before SaveAt returns. Returns the error from os.WriteFile when
// the write fails.
//
// Convenience for the common "save uploaded file to disk" path.
// Callers that need different perms / append behavior / streaming
// should use os.OpenFile directly with p.Data.
func (p *MultipartPart) SaveAt(dst string) error {
	return os.WriteFile(dst, p.Data, 0o644)
}

// SaveInto writes the part's Data into dir, using the basename of
// p.FileName as the on-disk filename. Path components in FileName
// are stripped so a malicious client can't escape the target
// directory (".." or absolute paths are rejected). Returns the full
// path of the written file along with any os.WriteFile error.
//
// Returns an error when FileName is empty (not a file part) or when
// the basename collapses to "." / "..".
func (p *MultipartPart) SaveInto(dir string) (string, error) {
	if p.FileName == "" {
		return "", errors.New("gogo: SaveInto requires a file part with a FileName")
	}
	base := filepath.Base(p.FileName)
	// filepath.Base of ".." or "/" yields ".." / "/" — reject either
	// so the on-disk name is always a plain leaf.
	if base == "." || base == ".." || base == string(filepath.Separator) || base == "" {
		return "", fmt.Errorf("gogo: SaveInto: unsafe filename %q", p.FileName)
	}
	full := filepath.Join(dir, base)
	if err := os.WriteFile(full, p.Data, 0o644); err != nil {
		return "", err
	}
	return full, nil
}

// ParseMultipart iterates every part of a multipart/form-data body,
// calling fn for each. Returning a non-nil error from fn stops
// iteration and surfaces the error verbatim to the caller of
// ParseMultipart; io.EOF specifically is treated as a graceful early
// exit and is swallowed.
//
// contentType must include the boundary parameter ("multipart/form-data;
// boundary=...") — the same header the client sent. Returns
// ErrUnsupportedMediaType when contentType is not multipart/form-data;
// other parse errors surface verbatim from mime/multipart.
//
// Memory: each part is read fully into memory before fn fires, capped by
// GetDefaultMultipartPartLimit unless options override it. Suitable for typical
// avatar / document uploads up to a few MiB. For large file parts, prefer
// ParseMultipartStream / Request.MultipartStream so the part can be copied
// without an extra Data allocation. The request body itself is still governed
// by Config.BodyLimit before multipart parsing begins.
func ParseMultipart(contentType string, body []byte, fn func(*MultipartPart) error) error {
	return ParseMultipartWithOptions(contentType, body, MultipartOptions{}, fn)
}

// ParseMultipartWithOptions is ParseMultipart with explicit per-part limits.
func ParseMultipartWithOptions(contentType string, body []byte, opt MultipartOptions, fn func(*MultipartPart) error) error {
	limit := multipartPartLimit(opt)
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return fmt.Errorf("gogo: parse multipart media type: %w", err)
	}
	if mediaType != "multipart/form-data" {
		return ErrUnsupportedMediaType
	}
	boundary := params["boundary"]
	if boundary == "" {
		return errors.New("gogo: multipart/form-data missing boundary")
	}

	mr := multipart.NewReader(bytes.NewReader(body), boundary)
	for {
		raw, err := mr.NextPart()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("gogo: read multipart part: %w", err)
		}
		data, readErr := readMultipartPart(raw, limit)
		closeErr := raw.Close()
		if readErr != nil {
			return fmt.Errorf("gogo: read part data: %w", readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("gogo: close part: %w", closeErr)
		}

		part := &MultipartPart{
			Name:        raw.FormName(),
			FileName:    raw.FileName(),
			ContentType: strings.TrimSpace(raw.Header.Get("Content-Type")),
			Data:        data,
			Header:      raw.Header,
		}
		if err := fn(part); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// MultipartStreamPart exposes a multipart part as a stream. It avoids the
// extra per-part allocation performed by ParseMultipart's Data field. The
// Reader is valid only during the callback.
type MultipartStreamPart struct {
	Name        string
	FileName    string
	ContentType string
	Header      textproto.MIMEHeader
	Reader      io.Reader
}

func (p *MultipartStreamPart) IsFile() bool { return p.FileName != "" }

// SaveInto streams the part into dir using the basename of FileName.
func (p *MultipartStreamPart) SaveInto(dir string) (string, error) {
	if p.FileName == "" {
		return "", errors.New("gogo: SaveInto requires a file part with a FileName")
	}
	base := filepath.Base(p.FileName)
	if base == "." || base == ".." || base == string(filepath.Separator) || base == "" {
		return "", fmt.Errorf("gogo: SaveInto: unsafe filename %q", p.FileName)
	}
	full := filepath.Join(dir, base)
	f, err := os.OpenFile(full, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return "", err
	}
	_, copyErr := io.Copy(f, p.Reader)
	closeErr := f.Close()
	if copyErr != nil {
		return "", copyErr
	}
	return full, closeErr
}

// ParseMultipartStream iterates multipart parts without materializing each part
// into a Data slice. The request body is still the collected []byte supplied by
// the caller, but file parts can be copied directly from the multipart reader
// to disk or another writer.
func ParseMultipartStream(contentType string, body []byte, opt MultipartOptions, fn func(*MultipartStreamPart) error) error {
	limit := multipartPartLimit(opt)
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return fmt.Errorf("gogo: parse multipart media type: %w", err)
	}
	if mediaType != "multipart/form-data" {
		return ErrUnsupportedMediaType
	}
	boundary := params["boundary"]
	if boundary == "" {
		return errors.New("gogo: multipart/form-data missing boundary")
	}

	mr := multipart.NewReader(bytes.NewReader(body), boundary)
	for {
		raw, err := mr.NextPart()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("gogo: read multipart part: %w", err)
		}
		lr := newMultipartLimitReader(raw, limit)
		part := &MultipartStreamPart{
			Name:        raw.FormName(),
			FileName:    raw.FileName(),
			ContentType: strings.TrimSpace(raw.Header.Get("Content-Type")),
			Header:      raw.Header,
			Reader:      lr,
		}
		cbErr := fn(part)
		if cbErr != nil {
			_ = raw.Close()
			if errors.Is(cbErr, io.EOF) {
				return nil
			}
			return cbErr
		}
		_, drainErr := io.Copy(io.Discard, lr)
		closeErr := raw.Close()
		if drainErr != nil {
			return fmt.Errorf("gogo: read part data: %w", drainErr)
		}
		if closeErr != nil {
			return fmt.Errorf("gogo: close part: %w", closeErr)
		}
	}
}

// Multipart is the Request-side wrapper for ParseMultipart. Use it
// from PostAsync handlers where the body is pre-collected; sync
// handlers should collect the body via Response.Body first and call
// ParseMultipart directly.
//
// Returns ErrNoBody when the body has not been collected, or
// ErrUnsupportedMediaType when the request's Content-Type is not
// multipart/form-data.
func (r *Request) Multipart(fn func(*MultipartPart) error) error {
	if r.body == nil {
		return ErrNoBody
	}
	return ParseMultipart(r.Header("content-type"), r.body, fn)
}

// MultipartWithOptions is Multipart with explicit limits.
func (r *Request) MultipartWithOptions(opt MultipartOptions, fn func(*MultipartPart) error) error {
	if r.body == nil {
		return ErrNoBody
	}
	return ParseMultipartWithOptions(r.Header("content-type"), r.body, opt, fn)
}

// MultipartStream iterates multipart parts as streams.
func (r *Request) MultipartStream(opt MultipartOptions, fn func(*MultipartStreamPart) error) error {
	if r.body == nil {
		return ErrNoBody
	}
	return ParseMultipartStream(r.Header("content-type"), r.body, opt, fn)
}

func multipartPartLimit(opt MultipartOptions) int64 {
	if opt.MaxPartBytes < 0 {
		return 0
	}
	if opt.MaxPartBytes > 0 {
		return opt.MaxPartBytes
	}
	limit := GetDefaultMultipartPartLimit()
	if limit < 0 {
		return 0
	}
	return limit
}

func readMultipartPart(r io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		return io.ReadAll(r)
	}
	lr := newMultipartLimitReader(r, limit)
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	return data, nil
}

type multipartLimitReader struct {
	r     io.Reader
	limit int64
	read  int64
}

func newMultipartLimitReader(r io.Reader, limit int64) *multipartLimitReader {
	return &multipartLimitReader{r: r, limit: limit}
}

func (r *multipartLimitReader) Read(p []byte) (int, error) {
	if r.limit > 0 && r.read >= r.limit {
		var one [1]byte
		n, err := r.r.Read(one[:])
		if n > 0 {
			return 0, ErrMultipartPartTooLarge
		}
		return 0, err
	}
	if r.limit > 0 {
		remaining := r.limit - r.read
		if int64(len(p)) > remaining {
			p = p[:remaining]
		}
	}
	n, err := r.r.Read(p)
	r.read += int64(n)
	return n, err
}
