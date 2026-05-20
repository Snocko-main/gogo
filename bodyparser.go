package gogo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/url"
	"reflect"
	"strconv"
	"strings"
)

// ErrNoBody is returned by Request.BodyParser when the request body has
// not been collected yet. Sync handlers must call Response.Body before
// reaching for BodyParser; PostAsync handlers always have the body
// pre-collected.
var ErrNoBody = errors.New("gogo: BodyParser requires a collected body; use PostAsync or Response.Body first")

// ErrUnsupportedMediaType is returned by ParseBody / BodyParser when the
// Content-Type header doesn't match any of the parser's supported media
// types. Handlers can map it to 415.
var ErrUnsupportedMediaType = errors.New("gogo: unsupported media type")

// BodyParser deserializes the request body into out based on the
// request's Content-Type header. Supported media types:
//
//   - application/json — encoding/json
//   - application/x-www-form-urlencoded — form decoding into struct
//     fields tagged with `form:"name"` (falls back to lower-cased field
//     name when the tag is missing).
//   - multipart/form-data — same form-field decoding for non-file
//     parts. File parts are ignored by BodyParser; use ParseMultipart
//     (separate helper) to iterate them.
//
// out must be a non-nil pointer (typically to a struct). Returns
// ErrNoBody when the body has not been collected, or
// ErrUnsupportedMediaType for a media type the parser does not handle.
// JSON / form parse errors surface verbatim from their respective
// packages so handlers can inspect them.
func (r *Request) BodyParser(out any) error {
	if r.body == nil {
		return ErrNoBody
	}
	return ParseBody(r.Header("content-type"), r.body, out)
}

// ParseBody is the lower-level helper behind Request.BodyParser, exposed
// so sync handlers that collect the body manually (via Response.Body)
// can deserialize without round-tripping through the Request wrapper.
//
// contentType may include parameters (`application/json; charset=utf-8`)
// — they are stripped before matching. An empty Content-Type is treated
// as application/octet-stream and returns ErrUnsupportedMediaType.
func ParseBody(contentType string, body []byte, out any) error {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		// Empty or malformed Content-Type. Be lenient: a JSON-shaped
		// body still gets parsed if it parses; otherwise we surface
		// unsupported.
		mediaType = strings.TrimSpace(strings.ToLower(contentType))
		if i := strings.IndexByte(mediaType, ';'); i >= 0 {
			mediaType = strings.TrimSpace(mediaType[:i])
		}
	}

	switch mediaType {
	case "application/json", "text/json":
		return json.Unmarshal(body, out)
	case "application/x-www-form-urlencoded":
		values, err := url.ParseQuery(string(body))
		if err != nil {
			return err
		}
		return decodeForm(values, out)
	case "multipart/form-data":
		_, params, err := mime.ParseMediaType(contentType)
		if err != nil {
			return fmt.Errorf("gogo: parse multipart media type: %w", err)
		}
		boundary := params["boundary"]
		if boundary == "" {
			return errors.New("gogo: multipart/form-data missing boundary")
		}
		mr := multipart.NewReader(strings.NewReader(string(body)), boundary)
		values := url.Values{}
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("gogo: read multipart part: %w", err)
			}
			// File parts have a non-empty FileName; skip them — the
			// caller wants ParseMultipart for those.
			if p.FileName() != "" {
				p.Close()
				continue
			}
			name := p.FormName()
			if name == "" {
				p.Close()
				continue
			}
			data, err := io.ReadAll(p)
			p.Close()
			if err != nil {
				return fmt.Errorf("gogo: read multipart value %q: %w", name, err)
			}
			values.Add(name, string(data))
		}
		return decodeForm(values, out)
	}
	return ErrUnsupportedMediaType
}

// decodeForm walks the fields of *out (must be a pointer to a struct)
// and sets each one from values using the `form:"name"` struct tag, or
// the lower-cased field name when the tag is absent. Supported field
// kinds: string, bool, int / int8 / int16 / int32 / int64, uint*,
// float32 / float64, []string, and pointers to any of those. Unknown
// fields are silently skipped; type-mismatched values return an error
// that names the field.
func decodeForm(values url.Values, out any) error {
	v := reflect.ValueOf(out)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return errors.New("gogo: decodeForm requires a non-nil pointer")
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return errors.New("gogo: decodeForm requires a pointer to a struct")
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		name := field.Tag.Get("form")
		if name == "-" {
			continue
		}
		if name == "" {
			name = strings.ToLower(field.Name)
		}
		raw, ok := values[name]
		if !ok || len(raw) == 0 {
			continue
		}
		if err := setFieldFromForm(v.Field(i), field.Name, raw); err != nil {
			return err
		}
	}
	return nil
}

func setFieldFromForm(field reflect.Value, fieldName string, values []string) error {
	// []string accepts the whole slice; everything else takes the first
	// value (forms with repeated names are rare for scalars, but match
	// fiber/express convention of "first wins").
	if field.Kind() == reflect.Slice && field.Type().Elem().Kind() == reflect.String {
		field.Set(reflect.ValueOf(values))
		return nil
	}
	first := values[0]

	// Unwrap pointer fields: allocate a fresh value of the pointed-to
	// type, fill it, then assign the pointer.
	if field.Kind() == reflect.Pointer {
		elem := reflect.New(field.Type().Elem())
		if err := setScalarFromForm(elem.Elem(), fieldName, first); err != nil {
			return err
		}
		field.Set(elem)
		return nil
	}
	return setScalarFromForm(field, fieldName, first)
}

func setScalarFromForm(field reflect.Value, fieldName, raw string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(raw)
	case reflect.Bool:
		switch strings.ToLower(raw) {
		case "1", "true", "t", "yes", "y", "on":
			field.SetBool(true)
		case "0", "false", "f", "no", "n", "off", "":
			field.SetBool(false)
		default:
			return fmt.Errorf("gogo: form field %q: %q is not a valid bool", fieldName, raw)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, field.Type().Bits())
		if err != nil {
			return fmt.Errorf("gogo: form field %q: %w", fieldName, err)
		}
		field.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(raw, 10, field.Type().Bits())
		if err != nil {
			return fmt.Errorf("gogo: form field %q: %w", fieldName, err)
		}
		field.SetUint(n)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(raw, field.Type().Bits())
		if err != nil {
			return fmt.Errorf("gogo: form field %q: %w", fieldName, err)
		}
		field.SetFloat(f)
	default:
		return fmt.Errorf("gogo: form field %q: unsupported kind %s", fieldName, field.Kind())
	}
	return nil
}
