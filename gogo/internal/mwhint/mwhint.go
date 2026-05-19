// Package mwhint carries the placement metadata that the bundled
// middleware in gogo/middleware uses to tell gogo.App.Use which
// chain a middleware belongs to. The package lives under internal/
// because users shouldn't see or construct these values directly —
// they call middleware.Logger(), middleware.RateLimit(), etc., and
// pass the result to app.Use. The framework reads .Place at
// registration time and the user never needs to think about it.
//
// The Mw field is type-erased to interface{} (any) to keep this
// package free of any dependency on gogo (avoiding an import
// cycle). gogo type-asserts back to its own Middleware type at
// registration.
package mwhint

// Placement is the chain a middleware should join.
type Placement byte

const (
	// Sync runs the middleware in the uWS sync C-callback. Cheap
	// rejecters (rate limiters, signature verification, auth) that
	// want to short-circuit a request before the worker goroutine
	// is spawned belong here. For async routes this forces the
	// slower wrap → snapshot → dispatch path; the trade-off is
	// worth it when the value is the early-reject.
	Sync Placement = iota + 1

	// Async runs the middleware inside the worker goroutine that
	// runs the async handler. Free to block on DB / HTTP / file I/O.
	// Only fires on GetAsync / PostAsync routes; sync routes never
	// see Async-placed middleware.
	Async

	// Both registers the middleware in both chains. Sync routes
	// fire the middleware in the sync callback, async routes fire
	// it inside the worker — keeping the zero-cgo shared-memory
	// dispatch path alive for async routes that have only
	// perf-neutral middleware attached (Logger, Helmet, Compress,
	// CORS, RequestID, …).
	Both
)

// Hinted pairs a middleware function (type-erased — gogo asserts it
// back to gogo.Middleware) with its Placement. Returned by every
// bundled middleware factory.
type Hinted struct {
	Mw    any
	Place Placement
}
