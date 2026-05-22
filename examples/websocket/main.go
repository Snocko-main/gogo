// websocket demonstrates a browser-friendly WebSocket route:
//
//   - GET / serves a tiny HTML client.
//   - WebSocket /ws upgrades only allowed origins.
//   - Upgrade stashes per-connection state with SetUserData.
//   - Open subscribes each connection to a room.
//   - Message publishes text frames to the other subscribers.
//
// Run:
//
//	CGO_ENABLED=1 go run -tags gogo ./examples/websocket
//
// Then open http://localhost:3004/ in a browser.
//
// Or connect from the CLI:
//
//	websocat 'ws://localhost:3004/ws?name=cli&room=general'
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
<p>Open this page in two tabs. Messages publish to every other client in the room.</p>
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
const name = 'browser-' + Math.random().toString(16).slice(2, 6);
const ws = new WebSocket(scheme + '://' + location.host + '/ws?name=' + name + '&room=general');

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
	Room string
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
			room := strings.TrimSpace(ctx.QueryParam("room"))
			if room == "" {
				room = "general"
			}
			ctx.SetUserData(&clientInfo{Name: name, Room: room})
			ctx.Accept("")
		},
		Open: func(ws *gogo.WebSocket) {
			info := ws.UserData().(*clientInfo)
			topic := "room." + info.Room
			ws.Subscribe(topic)
			log.Printf("%s connected to %s", info.Name, topic)
			ws.SendText("welcome, " + info.Name + " (" + topic + ")\n")
		},
		Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
			info := ws.UserData().(*clientInfo)
			if op != gogo.Text {
				ws.SendText("binary frames are not handled by this demo\n")
				return
			}
			text := strings.TrimSpace(string(msg))
			if text == "" {
				return
			}
			topic := "room." + info.Room
			ws.SendText("you: " + text + "\n")
			ws.Publish(topic, []byte(info.Name+": "+text+"\n"), gogo.Text)
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
