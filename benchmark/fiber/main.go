package main

import (
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
)

func main() {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	app.Get("/plain", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "text/plain; charset=utf-8")
		return c.SendString("hello world\n")
	})

	app.Get("/json", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "application/json")
		return c.SendString(`{"message":"hello world","ok":true}` + "\n")
	})

	app.Get("/hello/:name", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "text/plain; charset=utf-8")
		return c.SendString("hello " + c.Params("name") + "\n")
	})

	app.Get("/sleep", func(c *fiber.Ctx) error {
		time.Sleep(2 * time.Millisecond)
		c.Set("Content-Type", "text/plain; charset=utf-8")
		return c.SendString("slept\n")
	})

	log.Println("Fiber listening on http://localhost:3004")
	log.Fatal(app.Listen(":3004"))
}
