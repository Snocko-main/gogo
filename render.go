// Templating support — pluggable engine + html/template default.
//
// gogo deliberately separates the engine interface from any specific
// templating library so callers can swap in raymond / pug / a
// pre-compiled binary engine without changing user code. The
// default engine (html/template) is the safe choice for browser
// HTML because it auto-escapes context (HTML attribute vs URL vs
// script vs CSS), which prevents the most common XSS sinks.
//
// Engine setup happens once per App:
//
//	app.SetTemplateEngine(gogo.NewHTMLTemplateEngine(gogo.HTMLTemplateOptions{
//	    Root:    "views",
//	    Suffix:  ".tmpl",
//	    Reload:  false,  // set true in dev for live reload
//	}))
//
// Then handlers render by name:
//
//	res.Render("user/profile", map[string]any{
//	    "user": u,
//	    "now":  time.Now(),
//	})

package gogo

import (
	"bytes"
	"errors"
	"fmt"
	htmltmpl "html/template"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// TemplateEngine is the contract every templating implementation
// satisfies. Render writes the named template, parameterized by
// data, into w. Returning an error tells the framework to surface a
// 500 to the client (with the error reported via the panic logger);
// nil means the bytes in w are the rendered response.
//
// Engines are responsible for context-aware escaping; the framework
// performs no additional sanitization on the bytes that Render
// emits.
type TemplateEngine interface {
	Render(w *bytes.Buffer, name string, data any) error
}

// LimitedTemplateEngine is an optional extension for engines that can stop
// rendering before a response grows past a framework cap. Engines that do not
// implement it still work through TemplateEngine; Response.Render checks the
// final buffer size before sending.
type LimitedTemplateEngine interface {
	TemplateEngine
	RenderLimited(w *bytes.Buffer, name string, data any, maxBytes int64) error
}

// MaxRenderBytes caps the bytes Response.Render will stage before sending the
// rendered body. NoRenderLimit disables the cap. The default bounds accidental
// or maliciously large template output while staying generous for normal pages.
//
// Deprecated for runtime mutation: direct assignment remains supported for
// startup-time configuration. Use SetMaxRenderBytes / GetMaxRenderBytes for
// changes while requests may be running.
var MaxRenderBytes int64 = 8 << 20

// NoRenderLimit disables the Response.Render staging cap. Use only for trusted
// templates where output size is bounded by the application.
const NoRenderLimit int64 = -1

// ErrRenderTooLarge is reported when rendered template output exceeds
// MaxRenderBytes.
var ErrRenderTooLarge = errors.New("gogo: rendered template exceeds MaxRenderBytes")

type templateEngineSlot struct {
	engine TemplateEngine
}

// SetMaxRenderBytes updates the Response.Render staging cap atomically.
// Set to NoRenderLimit to disable the cap.
func SetMaxRenderBytes(maxBytes int64) {
	atomic.StoreInt64(&MaxRenderBytes, maxBytes)
}

// GetMaxRenderBytes returns the current Response.Render staging cap.
func GetMaxRenderBytes() int64 {
	return atomic.LoadInt64(&MaxRenderBytes)
}

// SetTemplateEngine installs e as the active template engine for
// this App. Subsequent Response.Render calls use it. Passing nil
// clears the engine, which causes Render to respond with 500.
//
// Engines are intended to be configured once at startup. Swapping
// engines at runtime is safe (the field is published atomically) but
// not a recommended pattern.
func (a *App) SetTemplateEngine(e TemplateEngine) {
	if e == nil {
		a.templateEngine.Store(nil)
		return
	}
	a.templateEngine.Store(&templateEngineSlot{engine: e})
}

// Render renders a named template with data and writes the result
// as a 200 text/html response. Set Content-Type via res.Header
// before calling Render if the engine emits something other than
// HTML (XML, plain text, etc.).
//
// Errors from the engine — missing template, parse failure,
// execution failure — surface as a 500 response with no body
// detail leaked to the client; the error itself is routed to the
// panic logger so operators can diagnose.
//
//	app.Get("/users/:id", func(res *gogo.Response, req *gogo.Request) {
//	    u, err := db.LoadUser(req.Param("id"))
//	    if err != nil {
//	        res.Send(404, "text/plain", "not found")
//	        return
//	    }
//	    res.Render("user/profile", map[string]any{"user": u})
//	})
func (r *Response) Render(name string, data any) {
	if r.app == nil {
		reportPanic(fmt.Errorf("gogo: Render: no template engine installed; call App.SetTemplateEngine"))
		r.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
		return
	}
	slot := r.app.templateEngine.Load()
	if slot == nil || slot.engine == nil {
		reportPanic(fmt.Errorf("gogo: Render: no template engine installed; call App.SetTemplateEngine"))
		r.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
		return
	}
	var buf bytes.Buffer
	if err := renderTemplate(slot.engine, &buf, name, data, GetMaxRenderBytes()); err != nil {
		reportPanic(fmt.Errorf("gogo: Render(%q): %w", name, err))
		r.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
		return
	}
	r.Send(200, "text/html; charset=utf-8", buf.String())
}

func renderTemplate(e TemplateEngine, buf *bytes.Buffer, name string, data any, maxBytes int64) error {
	if limited, ok := e.(LimitedTemplateEngine); ok {
		return limited.RenderLimited(buf, name, data, maxBytes)
	}
	if err := e.Render(buf, name, data); err != nil {
		return err
	}
	if maxBytes >= 0 && int64(buf.Len()) > maxBytes {
		return ErrRenderTooLarge
	}
	return nil
}

// HTMLTemplateOptions configures NewHTMLTemplateEngine.
type HTMLTemplateOptions struct {
	// Root is the directory containing template files. Required.
	// Walked once at startup so deeply-nested layouts work without
	// explicit registration.
	Root string

	// Suffix filters which files in Root are treated as templates.
	// Default ".tmpl". Files with other extensions are ignored
	// during the walk.
	Suffix string

	// Reload, when true, re-parses every template on each Render
	// call. Useful in development so edits show up without a
	// restart. Off by default — production deployments parse once
	// at startup.
	Reload bool

	// FuncMap registers helper functions exposed to every template.
	// Pair with the engine's auto-escaping by avoiding helpers that
	// return raw HTML; if you must, return template.HTML and
	// understand the XSS risk.
	FuncMap htmltmpl.FuncMap
}

// NewHTMLTemplateEngine builds an html/template-backed engine.
// Templates are named by their path relative to Root, with the
// suffix stripped — `views/user/profile.tmpl` is invoked as
// `user/profile`.
func NewHTMLTemplateEngine(opt HTMLTemplateOptions) TemplateEngine {
	if opt.Root == "" {
		panic("gogo: NewHTMLTemplateEngine: Root is required")
	}
	if opt.Suffix == "" {
		opt.Suffix = ".tmpl"
	}
	e := &htmlEngine{opt: opt}
	if err := e.reload(); err != nil {
		panic(fmt.Errorf("gogo: NewHTMLTemplateEngine: initial parse failed: %w", err))
	}
	return e
}

type htmlEngine struct {
	opt HTMLTemplateOptions
	mu  sync.RWMutex
	t   *htmltmpl.Template
}

func (e *htmlEngine) Render(w *bytes.Buffer, name string, data any) error {
	return e.render(w, name, data)
}

func (e *htmlEngine) RenderLimited(w *bytes.Buffer, name string, data any, maxBytes int64) error {
	if maxBytes < 0 {
		return e.render(w, name, data)
	}
	return e.render(&limitedTemplateWriter{w: w, remaining: maxBytes}, name, data)
}

func (e *htmlEngine) render(w io.Writer, name string, data any) error {
	if e.opt.Reload {
		if err := e.reload(); err != nil {
			return err
		}
	}
	e.mu.RLock()
	t := e.t
	e.mu.RUnlock()
	if t == nil {
		return fmt.Errorf("template engine not initialized")
	}
	tmpl := t.Lookup(name)
	if tmpl == nil {
		return fmt.Errorf("template %q not found", name)
	}
	return tmpl.Execute(w, data)
}

type limitedTemplateWriter struct {
	w         io.Writer
	remaining int64
}

func (w *limitedTemplateWriter) Write(p []byte) (int, error) {
	if int64(len(p)) <= w.remaining {
		n, err := w.w.Write(p)
		w.remaining -= int64(n)
		return n, err
	}
	if w.remaining > 0 {
		n, err := w.w.Write(p[:w.remaining])
		w.remaining -= int64(n)
		if err != nil {
			return n, err
		}
		return n, ErrRenderTooLarge
	}
	return 0, ErrRenderTooLarge
}

// reload walks Root and parses every file with the configured
// suffix into a single template set. Each template's name is its
// path relative to Root with the suffix stripped, so layouts can
// reference siblings via `{{ template "shared/header" . }}`.
func (e *htmlEngine) reload() error {
	root := e.opt.Root
	suffix := e.opt.Suffix
	t := htmltmpl.New("").Option("missingkey=zero")
	if e.opt.FuncMap != nil {
		t = t.Funcs(e.opt.FuncMap)
	}

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, suffix) {
			return nil
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		name := strings.TrimSuffix(filepath.ToSlash(rel), suffix)
		if _, parseErr := t.New(name).Parse(string(body)); parseErr != nil {
			return fmt.Errorf("template %q: %w", name, parseErr)
		}
		return nil
	})
	if err != nil {
		return err
	}
	e.mu.Lock()
	e.t = t
	e.mu.Unlock()
	return nil
}
