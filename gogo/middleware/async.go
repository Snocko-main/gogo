package middleware

import "uwebsockets-go/gogo"

// AsAsync converts a sync gogo.Middleware into a gogo.AsyncMiddleware.
// The two named types have identical underlying shapes (the App keeps
// them separate at the type level so the sync and async chains can be
// composed independently); this helper lets you install one of the
// bundled middleware on async routes too:
//
//	app.UseAsync(middleware.AsAsync(middleware.Compress()))
//	app.UseAsync(middleware.AsAsync(middleware.Helmet()))
//
// Not every sync middleware in this package makes sense as an async
// middleware — Logger and RequestID are fine, Compress is fine,
// CSRF / Session work too but rely on req.Local being valid in the
// worker goroutine (which it is). When in doubt, register the sync
// version on App.Use as well so the same middleware fires regardless
// of which handler family the route uses.
func AsAsync(m gogo.Middleware) gogo.AsyncMiddleware {
	return func(next gogo.AsyncHandler) gogo.AsyncHandler {
		// The conversion between Handler and AsyncHandler is safe
		// because both are defined as func(*Response, *Request)
		// and named types with the same underlying type are
		// freely convertible in Go.
		return gogo.AsyncHandler(m(gogo.Handler(next)))
	}
}
