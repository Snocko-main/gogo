// websocket demonstrates a browser-friendly WebSocket route:
//
//   - GET / serves a tiny HTML client.
//   - WebSocket /ws uses middleware.WebSocketAuth for origin, token, and
//     subprotocol checks.
//   - Upgrade verification returns per-connection state as UserData.
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
//	websocat -H='Origin: http://localhost:3004' -H='Sec-WebSocket-Protocol: chat.v1' 'ws://localhost:3004/ws?name=cli&room=general&token=demo-local-token'
package main

import (
	"log"
	"strings"
	"time"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

const (
	allowedOrigin = "http://localhost:3004"
	demoToken     = "demo-local-token"
	subprotocol   = "chat.v1"
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
const params = new URLSearchParams({ name, room: 'general', token: 'demo-local-token' });
const ws = new WebSocket(scheme + '://' + location.host + '/ws?' + params, 'chat.v1');

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

	hub := gogo.NewWSHub()
	defer hub.Close()

	hub.WebSocket(app, "/ws", gogo.WebSocketBehavior{
		Upgrade: middleware.WebSocketAuth(middleware.WebSocketAuthOptions{
			AllowedOrigins:      []string{allowedOrigin},
			AllowedSubprotocols: []string{subprotocol},
			Verify: func(ctx *gogo.UpgradeContext) (any, bool, int, string) {
				if ctx.QueryParam("token") != demoToken {
					return nil, false, 401, "bad token"
				}

				// This demo uses a static query token so it can run without
				// a login system. Browser production apps usually issue a
				// short-lived signed cookie or one-time WebSocket ticket.
				name := strings.TrimSpace(ctx.QueryParam("name"))
				if name == "" {
					name = "guest"
				}
				room := strings.TrimSpace(ctx.QueryParam("room"))
				if room == "" {
					room = "general"
				}
				return &clientInfo{Name: name, Room: room}, true, 0, ""
			},
		}),
		Open: func(ws *gogo.WebSocket) {
			info := ws.UserData().(*clientInfo)
			topic := "room." + info.Room
			if !hub.Subscribe(ws, topic) {
				log.Printf("subscribe failed for %s to %s", info.Name, topic)
				ws.End(1011, "subscribe failed")
				return
			}
			log.Printf("%s connected to %s", info.Name, topic)
			if !ws.SendText("welcome, " + info.Name + " (" + topic + ")\n") {
				log.Printf("welcome send rejected for %s", info.Name)
				ws.End(1013, "backpressure")
			}
		},
		Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
			info := ws.UserData().(*clientInfo)
			if op != gogo.Text {
				if !ws.SendText("binary frames are not handled by this demo\n") {
					log.Printf("binary warning send rejected for %s", info.Name)
				}
				return
			}
			text := strings.TrimSpace(string(msg))
			if text == "" {
				return
			}
			topic := "room." + info.Room
			if !ws.SendText("you: " + text + "\n") {
				log.Printf("echo send rejected for %s", info.Name)
				return
			}
			if err := hub.PublishFrom(ws, topic, []byte(info.Name+": "+text+"\n"), gogo.Text); err != nil {
				log.Printf("publish: %v", err)
			}
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
