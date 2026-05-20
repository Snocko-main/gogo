// fiber-perfquickwins mirrors benchmark/perfquickwins (the gogo
// server) so wrk can drive identical workloads against both
// frameworks. Three routes:
//
//	/plain     — sync handler, no middleware, no buffered headers.
//	/cors/x    — middleware sets 3 CORS-style headers per request.
//	/heavy/x   — middleware stamps 8 response headers (worst case
//	             for header-write overhead on either side).
//
// Run with:
//
//	go run ./benchmark/fiber-perfquickwins :8081
//
// And drive via wrk in lockstep with the gogo binary:
//
//	wrk -t 2 -c 32 -d 15s --latency http://127.0.0.1:8081/heavy/x
//	wrk -t 2 -c 32 -d 15s --latency http://127.0.0.1:8080/heavy/x
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/gofiber/fiber/v2"
)

func main() {
	addr := ":8081"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	port := 0
	if _, err := fmt.Sscanf(addr, ":%d", &port); err != nil || port == 0 {
		log.Fatalf("usage: %s :PORT", os.Args[0])
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		// Match the gogo bench's posture — no per-request panic
		// recovery middleware, no body parsing, raw throughput.
	})

	// /plain — bare baseline. Comparable to gogo's /plain.
	app.Get("/plain", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "text/plain")
		return c.SendString("ok")
	})

	// /cors/x — hand-rolled CORS middleware that stamps the
	// same three headers gogo's middleware.CORS emits when the
	// Origin matches an allow-listed entry. We don't pull in
	// fiber's CORS middleware here because that package adds
	// extra book-keeping (preflight short-circuit, OPTIONS auto
	// registration) the gogo test path also pays — comparing
	// like-for-like is easier with the inlined version.
	app.Use("/cors", func(c *fiber.Ctx) error {
		if origin := c.Get("Origin"); origin != "" {
			c.Set("Access-Control-Allow-Origin", origin)
			c.Set("Vary", "Origin")
			c.Set("Access-Control-Expose-Headers", "X-Request-Id, X-Rate-Limit")
		}
		return c.Next()
	})
	app.Get("/cors/x", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "text/plain")
		return c.SendString("ok")
	})

	// /heavy/x — middleware stamps 8 response headers.
	app.Use("/heavy", func(c *fiber.Ctx) error {
		c.Set("X-Service", "fiber")
		c.Set("X-Region", "us-west-2")
		c.Set("X-Build", "abc123")
		c.Set("X-Trace-Id", "trace-1")
		c.Set("Cache-Control", "no-store")
		c.Set("Vary", "Accept-Encoding")
		c.Set("X-Frame-Options", "DENY")
		c.Set("X-Content-Type-Options", "nosniff")
		return c.Next()
	})
	app.Get("/heavy/x", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "text/plain")
		return c.SendString("ok")
	})

	// /post/small — POST with a small body, mirrors gogo's shared-
	// dispatch route. The handler touches the body so fiber can't
	// short-circuit the read.
	app.Post("/post/small", func(c *fiber.Ctx) error {
		_ = len(c.Body())
		c.Set("Content-Type", "text/plain")
		return c.SendString("ok")
	})

	// /post/big — POST with a larger body, mirrors gogo's classic
	// async path.
	app.Post("/post/big", func(c *fiber.Ctx) error {
		_ = len(c.Body())
		c.Set("Content-Type", "text/plain")
		return c.SendString("ok")
	})

	if err := app.Listen(addr); err != nil {
		log.Fatalf("Listen %s: %v", addr, err)
	}
}
