package gogo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrWSHubClosed is returned when a publish or start is attempted after Close.
	ErrWSHubClosed = errors.New("gogo: websocket hub is closed")

	// ErrWSHubUntrackedSocket is returned when PublishFrom cannot identify the
	// sender. Register routes with WSHub.WebSocket or WSHub.Wrap.
	ErrWSHubUntrackedSocket = errors.New("gogo: websocket hub socket is not tracked; register route with hub.WebSocket or hub.Wrap")
)

var fallbackNodeCounter atomic.Uint64

// WSHub coordinates WebSocket topic publishes across all App instances
// attached to this process, and optionally across processes through an
// adapter such as RedisWSHubAdapter.
//
// Register routes through hub.WebSocket so the hub can tell which App owns
// each socket. That lets PublishFrom skip the sender without calling
// WebSocket.Publish from a non-loop goroutine.
type WSHub struct {
	nodeID  string
	adapter WSHubAdapter

	mu      sync.RWMutex
	apps    map[*App]struct{}
	sockets map[uintptr]*hubSocket
	members map[string]map[uintptr]struct{}

	startMu        sync.Mutex
	adapterStarted bool
	closed         atomic.Bool
	closeOnce      sync.Once
	ctx            context.Context
	cancel         context.CancelFunc
}

type hubSocket struct {
	app    *App
	direct string
	topics map[string]struct{}
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
		sockets: make(map[uintptr]*hubSocket),
		members: make(map[string]map[uintptr]struct{}),
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

// Attach includes app in process-local fan-out. It is called automatically by
// WebSocket, but is useful when routes are registered manually. PublishFrom
// still needs sockets registered through WebSocket or Wrap. Attach does not
// start the adapter; call Start at boot when you want fail-fast behavior.
func (h *WSHub) Attach(app *App) {
	if h == nil || app == nil {
		return
	}
	h.mu.Lock()
	h.apps[app] = struct{}{}
	h.mu.Unlock()
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
// WebSocket.Subscribe so user code can stay hub-centered. PublishFrom uses
// these tracked subscriptions to exclude the sender safely.
func (h *WSHub) Subscribe(ws *WebSocket, topic string) bool {
	if ws == nil {
		return false
	}
	ok := ws.Subscribe(topic)
	if !ok || h == nil {
		return ok
	}
	key := wsNativeKey(ws)
	if key == 0 {
		return ok
	}
	h.mu.Lock()
	if socket := h.sockets[key]; socket != nil {
		socket.topics[topic] = struct{}{}
		if h.members[topic] == nil {
			h.members[topic] = make(map[uintptr]struct{})
		}
		h.members[topic][key] = struct{}{}
	}
	h.mu.Unlock()
	return ok
}

// Unsubscribe removes ws from topic.
func (h *WSHub) Unsubscribe(ws *WebSocket, topic string) bool {
	if ws == nil {
		return false
	}
	ok := ws.Unsubscribe(topic)
	if h == nil {
		return ok
	}
	key := wsNativeKey(ws)
	if key == 0 {
		return ok
	}
	h.removeMembership(key, topic)
	return ok
}

// Publish broadcasts to every local App attached to the hub, then forwards
// to the adapter if present. It is safe to call from any goroutine.
func (h *WSHub) Publish(topic string, message []byte, opcode OpCode) error {
	if h == nil {
		return nil
	}
	if h.closed.Load() {
		return ErrWSHubClosed
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
	if h.closed.Load() {
		return ErrWSHubClosed
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

// PublishFrom broadcasts from a WebSocket handler and skips the sender. It is
// safe to call from any goroutine, but ws must have been registered through
// WebSocket or Wrap and topic subscriptions must use Subscribe.
func (h *WSHub) PublishFrom(ws *WebSocket, topic string, message []byte, opcode OpCode) error {
	if h == nil {
		return nil
	}
	if h.closed.Load() {
		return ErrWSHubClosed
	}
	key := wsNativeKey(ws)
	if key == 0 {
		return ErrWSHubUntrackedSocket
	}
	msg := WSHubMessage{
		NodeID:  h.nodeID,
		Topic:   topic,
		Message: cloneBytes(message),
		OpCode:  opcode,
	}
	origin, direct := h.publishFromTargets(key, msg)
	if origin == nil {
		return ErrWSHubUntrackedSocket
	}

	if len(direct) > 0 {
		origin.PublishBatch(direct)
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
		h.closed.Store(true)
		h.cancel()
		if h.adapter != nil {
			err = h.adapter.Close()
		}
	})
	return err
}

// Start starts the adapter subscription, if an adapter is configured. It can
// be retried after transient adapter failures. Publish and PublishFrom also
// start the adapter lazily.
func (h *WSHub) Start() error {
	if h == nil || h.adapter == nil {
		return nil
	}
	if h.closed.Load() {
		return ErrWSHubClosed
	}
	h.startMu.Lock()
	defer h.startMu.Unlock()
	if h.adapterStarted {
		return nil
	}
	err := h.adapter.Start(h.ctx, func(msg WSHubMessage) {
		if msg.NodeID == h.nodeID || h.closed.Load() {
			return
		}
		h.publishLocal(msg, nil)
	})
	if err != nil {
		return err
	}
	h.adapterStarted = true
	return nil
}

func (h *WSHub) publishLocal(msg WSHubMessage, skip *App) {
	if h.closed.Load() {
		return
	}
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
	if h.closed.Load() {
		return ErrWSHubClosed
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
	direct := h.directTopic(key)
	ws.Subscribe(direct)
	h.mu.Lock()
	h.sockets[key] = &hubSocket{
		app:    app,
		direct: direct,
		topics: make(map[string]struct{}),
	}
	h.mu.Unlock()
}

func (h *WSHub) forget(ws *WebSocket) {
	key := wsNativeKey(ws)
	if key == 0 {
		return
	}
	h.mu.Lock()
	if socket := h.sockets[key]; socket != nil {
		for topic := range socket.topics {
			h.removeMembershipLocked(key, topic)
		}
	}
	delete(h.sockets, key)
	h.mu.Unlock()
}

func (h *WSHub) publishFromTargets(sender uintptr, msg WSHubMessage) (*App, []PublishMessage) {
	h.mu.RLock()
	socket := h.sockets[sender]
	if socket == nil {
		h.mu.RUnlock()
		return nil, nil
	}
	origin := socket.app
	members := h.members[msg.Topic]
	direct := make([]PublishMessage, 0, len(members))
	for key := range members {
		if key == sender {
			continue
		}
		peer := h.sockets[key]
		if peer == nil || peer.app != origin {
			continue
		}
		direct = append(direct, PublishMessage{
			Topic:   peer.direct,
			Message: msg.Message,
			OpCode:  msg.OpCode,
		})
	}
	h.mu.RUnlock()
	return origin, direct
}

func (h *WSHub) removeMembership(key uintptr, topic string) {
	h.mu.Lock()
	h.removeMembershipLocked(key, topic)
	h.mu.Unlock()
}

func (h *WSHub) removeMembershipLocked(key uintptr, topic string) {
	if socket := h.sockets[key]; socket != nil {
		delete(socket.topics, topic)
	}
	members := h.members[topic]
	if members == nil {
		return
	}
	delete(members, key)
	if len(members) == 0 {
		delete(h.members, topic)
	}
}

func (h *WSHub) directTopic(key uintptr) string {
	return fmt.Sprintf("__gogo_hub:%s:%x", h.nodeID, key)
}

func randomNodeID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("local-%d-%d-%d", os.Getpid(), time.Now().UnixNano(), fallbackNodeCounter.Add(1))
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
