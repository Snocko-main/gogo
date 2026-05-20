// upload demonstrates POST body collection with a max-body limit.
// Oversize uploads get 413 automatically.
//
//	CGO_ENABLED=1 go run -tags gogo ./examples/upload
//
//	# Small upload echoes back the SHA-256 of the body.
//	curl -XPOST --data-binary @go.mod http://localhost:3003/upload
//
//	# Larger than 64 KiB → 413
//	dd if=/dev/zero bs=1024 count=100 | curl -XPOST --data-binary @- http://localhost:3003/upload
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"

	gogo "github.com/Snocko-main/gogo"
)

func main() {
	app, err := gogo.NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	// 64 KiB cap. Oversize uploads return 413 before the handler is called.
	app.PostAsync("/upload", 64*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
		sum := sha256.Sum256(body)
		res.JSON(200, map[string]any{
			"size":   len(body),
			"sha256": hex.EncodeToString(sum[:]),
			"ctype":  req.Header("content-type"),
		})
	})

	// Streaming body using the low-level OnData primitive — useful when
	// you don't want to hold the whole body in memory at once. Counts
	// bytes and responds when the final chunk arrives.
	app.Post("/upload-stream", func(res *gogo.Response, req *gogo.Request) {
		var total int
		res.OnData(func(chunk []byte, isLast bool) {
			total += len(chunk)
			if isLast {
				res.Send(200, "text/plain", fmt.Sprintf("counted %d bytes", total))
			}
		})
	})

	if !app.Listen(3003) {
		log.Fatal("listen :3003 failed")
	}
	log.Println("gogo~ upload listening on http://localhost:3003")
	app.Run()
}
