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
	"fmt"
	htmltmpl "html/template"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// SetTemplateEngine installs e as the active template engine for
// this App. Subsequent Response.Render calls use it. Passing nil
// clears the engine, which causes Render to respond with 500.
//
// Engines are intended to be configured once at startup. Swapping
// engines at runtime is safe (the field is guarded by an
// atomic.Pointer) but not a recommended pattern.
func (a *App) SetTemplateEngine(e TemplateEngine) {
	a.templateEngine = e
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
	if r.app == nil || r.app.templateEngine == nil {
		reportPanic(fmt.Errorf("gogo: Render: no template engine installed; call App.SetTemplateEngine"))
		r.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
		return
	}
	var buf bytes.Buffer
	if err := r.app.templateEngine.Render(&buf, name, data); err != nil {
		reportPanic(fmt.Errorf("gogo: Render(%q): %w", name, err))
		r.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
		return
	}
	r.Send(200, "text/html; charset=utf-8", buf.String())
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
