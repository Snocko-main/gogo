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
)

// MultipartPart is one chunk of a parsed multipart/form-data body.
// Returned to the callback passed to ParseMultipart / Request.Multipart.
//
// The Data slice is valid only for the duration of the callback — it
// aliases an internal buffer that the iterator reuses for the next
// part. Append to a fresh slice or call SaveAt before returning if
// you need the bytes later.
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
// Memory: each part is read fully into memory before fn fires.
// Suitable for typical avatar / document uploads up to a few MiB.
// Very large uploads (multi-GB streaming) need a different surface —
// out of scope here; reach for net/http.MultipartReader manually
// over a streaming body source.
func ParseMultipart(contentType string, body []byte, fn func(*MultipartPart) error) error {
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
		data, readErr := io.ReadAll(raw)
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
