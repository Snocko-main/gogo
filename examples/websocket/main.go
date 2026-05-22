// websocket demonstrates a browser-friendly WebSocket route:
//
//   - GET / serves a tiny HTML client.
//   - WebSocket /ws upgrades only allowed origins.
//   - Upgrade stashes per-connection state with SetUserData.
//   - Message echoes text frames back to the client.
//
// Run:
//
//	CGO_ENABLED=1 go run -tags gogo ./examples/websocket
//
// Then open http://localhost:3004/ in a browser.
//
// Or connect from the CLI:
//
//	websocat 'ws://localhost:3004/ws?name=cli'
package main

import (
	"log"
	"strings"
	"time"

	gogo "github.com/Snocko-main/gogo"
)

const indexHTML = `<!doctype html>
<html>
<body>
<h1>gogo~ WebSocket demo</h1>
<form id="form">
  <input id="msg" autocomplete="off" value="hello from the browser">
  <button>Send</button>
</form>
<ul id="log" style="font-family:monospace"></ul>
<script>
const log = document.getElementById('log');
const msg = document.getElementById('msg');
const form = document.getElementById('form');
const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
const ws = new WebSocket(scheme + '://' + location.host + '/ws?name=browser');

function line(text) {
    const li = document.createElement('li');
    li.textContent = text;
    log.prepend(li);
}

ws.addEventListener('open', () => line('connected'));
ws.addEventListener('message', e => line('server: ' + e.data));
ws.addEventListener('close', e => line('closed: ' + e.code));
ws.addEventListener('error', () => line('socket error'));

form.addEventListener('submit', e => {
    e.preventDefault();
    ws.send(msg.value);
});
</script>
</body>
</html>`

type clientInfo struct {
	Name string
}

func main() {
	app, err := gogo.NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	app.Get("/", gogo.Reply{
		ContentType: "text/html; charset=utf-8",
		Body:        indexHTML,
	})

	app.WebSocket("/ws", gogo.WebSocketBehavior{
		Upgrade: func(ctx *gogo.UpgradeContext) {
			origin := ctx.Header("origin")
			if origin != "" && origin != "http://localhost:3004" {
				ctx.Reject(403, "bad origin")
				return
			}

			name := strings.TrimSpace(ctx.QueryParam("name"))
			if name == "" {
				name = "guest"
			}
			ctx.SetUserData(&clientInfo{Name: name})
			ctx.Accept("")
		},
		Open: func(ws *gogo.WebSocket) {
			info := ws.UserData().(*clientInfo)
			log.Printf("%s connected", info.Name)
			ws.SendText("welcome, " + info.Name + "\n")
		},
		Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
			info := ws.UserData().(*clientInfo)
			if op != gogo.Text {
				ws.SendText("binary frames are not handled by this demo\n")
				return
			}
			ws.SendText(info.Name + " said: " + string(msg) + "\n")
		},
		Close: func(ws *gogo.WebSocket, code int, msg []byte) {
			if info, ok := ws.UserData().(*clientInfo); ok {
				log.Printf("%s disconnected: %d %s", info.Name, code, msg)
			}
		},

		MaxPayloadLength: 1 << 20,
		IdleTimeout:      120 * time.Second,
		MaxBackpressure:  64 * 1024,
	})

	if !app.Listen(3004) {
		log.Fatal("listen :3004 failed")
	}
	log.Println("gogo~ websocket demo listening on http://localhost:3004")
	app.Run()
}
