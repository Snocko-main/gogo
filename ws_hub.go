package gogo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
)

// WSHub coordinates WebSocket topic publishes across all App instances
// attached to this process, and optionally across processes through an
// adapter such as RedisWSHubAdapter.
//
// Register routes through hub.WebSocket so the hub can tell which App owns
// each socket. That lets PublishFrom use the fastest path on the sender's
// loop (WebSocket.Publish), while still fanning out to the other attached
// Apps via App.Publish.
type WSHub struct {
	nodeID  string
	adapter WSHubAdapter

	mu      sync.RWMutex
	apps    map[*App]struct{}
	sockets map[uintptr]*App

	startOnce sync.Once
	startErr  error
	closeOnce sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
}

// WSHubOption customizes a WSHub.
type WSHubOption func(*WSHub)

// WithWSHubNodeID sets the process/node identifier used to ignore messages
// that this hub already delivered locally before publishing to an adapter.
func WithWSHubNodeID(id string) WSHubOption {
	return func(h *WSHub) {
		if id != "" {
			h.nodeID = id
		}
	}
}

// WithWSHubAdapter installs a distributed adapter, for example Redis.
func WithWSHubAdapter(adapter WSHubAdapter) WSHubOption {
	return func(h *WSHub) {
		h.adapter = adapter
	}
}

// NewWSHub creates a WebSocket hub. With no adapter, it still fans out across
// every App attached in the current process, which is enough for RunMultiCore.
func NewWSHub(opts ...WSHubOption) *WSHub {
	ctx, cancel := context.WithCancel(context.Background())
	h := &WSHub{
		nodeID:  randomNodeID(),
		apps:    make(map[*App]struct{}),
		sockets: make(map[uintptr]*App),
		ctx:     ctx,
		cancel:  cancel,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
	return h
}

// Attach includes app in process-local fan-out. It is called automatically
// by WebSocket, but is useful when routes are registered manually.
func (h *WSHub) Attach(app *App) {
	if h == nil || app == nil {
		return
	}
	h.mu.Lock()
	h.apps[app] = struct{}{}
	h.mu.Unlock()
	if err := h.Start(); err != nil {
		reportPanic(err)
	}
}

// WebSocket attaches app to the hub and registers a WebSocket route whose
// callbacks are wrapped so PublishFrom can avoid duplicate local delivery.
func (h *WSHub) WebSocket(app *App, pattern string, behavior WebSocketBehavior) {
	if app == nil {
		return
	}
	if h == nil {
		app.WebSocket(pattern, behavior)
		return
	}
	h.Attach(app)
	app.WebSocket(pattern, h.Wrap(app, behavior))
}

// Wrap returns a WebSocketBehavior that tracks socket ownership. Use this
// when a Router is registering the WebSocket route:
//
//	router.WebSocket("/ws", hub.Wrap(app, behavior))
func (h *WSHub) Wrap(app *App, behavior WebSocketBehavior) WebSocketBehavior {
	if h == nil || app == nil {
		return behavior
	}
	h.Attach(app)
	open := behavior.Open
	closeFn := behavior.Close
	behavior.Open = func(ws *WebSocket) {
		h.remember(ws, app)
		if open != nil {
			open(ws)
		}
	}
	behavior.Close = func(ws *WebSocket, code int, msg []byte) {
		defer h.forget(ws)
		if closeFn != nil {
			closeFn(ws, code, msg)
		}
	}
	return behavior
}

// Subscribe enrolls ws in topic. It is a small convenience wrapper around
// WebSocket.Subscribe so user code can stay hub-centered.
func (h *WSHub) Subscribe(ws *WebSocket, topic string) bool {
	if ws == nil {
		return false
	}
	return ws.Subscribe(topic)
}

// Unsubscribe removes ws from topic.
func (h *WSHub) Unsubscribe(ws *WebSocket, topic string) bool {
	if ws == nil {
		return false
	}
	return ws.Unsubscribe(topic)
}

// Publish broadcasts to every local App attached to the hub, then forwards
// to the adapter if present. It is safe to call from any goroutine.
func (h *WSHub) Publish(topic string, message []byte, opcode OpCode) error {
	if h == nil {
		return nil
	}
	msg := WSHubMessage{
		NodeID:  h.nodeID,
		Topic:   topic,
		Message: cloneBytes(message),
		OpCode:  opcode,
	}
	h.publishLocal(msg, nil)
	return h.publishAdapter(msg)
}

// PublishBatch broadcasts many messages with one App.PublishBatch call per
// local App, then forwards each message to the adapter. For local fan-out
// this keeps the same batching advantage as App.PublishBatch.
func (h *WSHub) PublishBatch(msgs []PublishMessage) error {
	if h == nil || len(msgs) == 0 {
		return nil
	}
	local := make([]PublishMessage, len(msgs))
	var firstErr error
	for i, msg := range msgs {
		local[i] = PublishMessage{
			Topic:   msg.Topic,
			Message: cloneBytes(msg.Message),
			OpCode:  msg.OpCode,
		}
	}
	h.mu.RLock()
	apps := make([]*App, 0, len(h.apps))
	for app := range h.apps {
		apps = append(apps, app)
	}
	h.mu.RUnlock()
	for _, app := range apps {
		app.PublishBatch(local)
	}
	for _, msg := range local {
		err := h.publishAdapter(WSHubMessage{
			NodeID:  h.nodeID,
			Topic:   msg.Topic,
			Message: msg.Message,
			OpCode:  msg.OpCode,
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// PublishFrom broadcasts from a WebSocket handler. On the sender's App it
// uses WebSocket.Publish, so the sender does not receive its own message.
// Other Apps attached to this process receive the message through App.Publish.
func (h *WSHub) PublishFrom(ws *WebSocket, topic string, message []byte, opcode OpCode) error {
	if h == nil {
		if ws != nil {
			ws.Publish(topic, message, opcode)
		}
		return nil
	}
	msg := WSHubMessage{
		NodeID:  h.nodeID,
		Topic:   topic,
		Message: cloneBytes(message),
		OpCode:  opcode,
	}
	var origin *App
	if ws != nil {
		origin = h.appFor(ws)
		ws.Publish(topic, msg.Message, opcode)
	}
	h.publishLocal(msg, origin)
	return h.publishAdapter(msg)
}

// Close stops the adapter subscription. It does not close any attached App.
func (h *WSHub) Close() error {
	if h == nil {
		return nil
	}
	var err error
	h.closeOnce.Do(func() {
		h.cancel()
		if h.adapter != nil {
			err = h.adapter.Close()
		}
	})
	return err
}

// Start starts the adapter subscription, if an adapter is configured. It is
// optional: Attach, WebSocket, Publish, and PublishFrom start the adapter
// lazily. Call Start explicitly at boot when you want to fail fast if Redis or
// another adapter is unavailable.
func (h *WSHub) Start() error {
	if h == nil || h.adapter == nil {
		return nil
	}
	h.startOnce.Do(func() {
		h.startErr = h.adapter.Start(h.ctx, func(msg WSHubMessage) {
			if msg.NodeID == h.nodeID {
				return
			}
			h.publishLocal(msg, nil)
		})
	})
	return h.startErr
}

func (h *WSHub) publishLocal(msg WSHubMessage, skip *App) {
	h.mu.RLock()
	apps := make([]*App, 0, len(h.apps))
	for app := range h.apps {
		if app != skip {
			apps = append(apps, app)
		}
	}
	h.mu.RUnlock()
	for _, app := range apps {
		app.Publish(msg.Topic, msg.Message, msg.OpCode)
	}
}

func (h *WSHub) publishAdapter(msg WSHubMessage) error {
	if h.adapter == nil {
		return nil
	}
	if err := h.Start(); err != nil {
		return err
	}
	return h.adapter.Publish(h.ctx, msg)
}

func (h *WSHub) remember(ws *WebSocket, app *App) {
	key := wsNativeKey(ws)
	if key == 0 {
		return
	}
	h.mu.Lock()
	h.sockets[key] = app
	h.mu.Unlock()
}

func (h *WSHub) forget(ws *WebSocket) {
	key := wsNativeKey(ws)
	if key == 0 {
		return
	}
	h.mu.Lock()
	delete(h.sockets, key)
	h.mu.Unlock()
}

func (h *WSHub) appFor(ws *WebSocket) *App {
	key := wsNativeKey(ws)
	if key == 0 {
		return nil
	}
	h.mu.RLock()
	app := h.sockets[key]
	h.mu.RUnlock()
	return app
}

func randomNodeID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "local"
	}
	return hex.EncodeToString(b[:])
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
