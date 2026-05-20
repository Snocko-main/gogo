// Package middleware: Async marks a middleware as needing the worker
// goroutine.

package middleware

import (
	"github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/internal/mwhint"
)

// Async wraps a Middleware so the framework runs it inside the worker
// goroutine that runs an async handler. Use this for any middleware
// that blocks on I/O — database queries, HTTP calls, file reads —
// because the framework's sync chain runs on the uWS event-loop
// thread, and a blocking call there would stall every other request
// in flight.
//
//	app.Use(middleware.Async(func(next gogo.Handler) gogo.Handler {
//	    return func(res *gogo.Response, req *gogo.Request) {
//	        user, err := db.LookupUser(req.Header("authorization"))
//	        if err != nil {
//	            res.Send(401, "text/plain", "bad token")
//	            return
//	        }
//	        req.SetLocal("user", user)
//	        next(res, req)
//	    }
//	}))
//
// Async-wrapped middleware fires only on GetAsync / PostAsync routes;
// sync routes (Get / Post) do not see it. If you register an Async
// middleware that matches a sync route, the route will simply skip
// the middleware — register it on a Group of async-only routes, or
// move the route to GetAsync.
//
// Non-blocking middleware (header setters, validators, in-memory
// state) does not need Async; pass the raw function to app.Use and
// the framework will route it through the optimal chain for each
// request type.
func Async(mw gogo.Middleware) mwhint.Hinted {
	return mwhint.Hinted{Mw: mw, Place: mwhint.Async}
}
