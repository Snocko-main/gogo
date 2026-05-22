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

	// ErrWSHubAdapterQueueFull is returned by PublishFrom when the async
	// adapter queue is full. Local subscribers have already been fanned out.
	ErrWSHubAdapterQueueFull = errors.New("gogo: websocket hub adapter queue is full")

	// ErrWSHubUntrackedSocket is returned when PublishFrom cannot identify the
	// sender. Register routes with WSHub.WebSocket or WSHub.Wrap.
	ErrWSHubUntrackedSocket = errors.New("gogo: websocket hub socket is not tracked; register route with hub.WebSocket or hub.Wrap")
)

const (
	defaultWSHubAdapterQueueSize      = 1024
	defaultWSHubAdapterPublishTimeout = 5 * time.Second
	defaultWSHubStartBackoff          = 100 * time.Millisecond
	maxWSHubStartBackoff              = 2 * time.Second
)

var fallbackNodeCounter atomic.Uint64

var (
	wsHubRegistryMu sync.RWMutex
	wsHubRegistry   = make(map[uintptr]*WSHub)
)

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
	startErr       error
	startNextTry   time.Time
	startBackoff   time.Duration
	adapterQueue   chan WSHubMessage
	adapterQueueSz int
	adapterOnce    sync.Once
	adapterWG      sync.WaitGroup
	adapterQueueMu sync.RWMutex
	adapterTimeout time.Duration
	adapterErrMu   sync.Mutex
	adapterErrFn   func(error)
	adapterErrNext time.Time
	adapterErrLast string
	socketSeq      atomic.Uint64
	closed         atomic.Bool
	closeOnce      sync.Once
	ctx            context.Context
	cancel         context.CancelFunc
}

type hubSocket struct {
	app    *App
	direct string
	token  uint64
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

// WithWSHubAdapterQueueSize sets the bounded async adapter queue used by
// PublishFrom. Larger queues absorb Redis/network bursts without blocking the
// WebSocket loop thread.
func WithWSHubAdapterQueueSize(size int) WSHubOption {
	return func(h *WSHub) {
		if size > 0 {
			h.adapterQueueSz = size
		}
	}
}

// WithWSHubAdapterPublishTimeout bounds each adapter publish. This keeps
// shutdown from waiting indefinitely on a slow or half-open Redis connection.
func WithWSHubAdapterPublishTimeout(timeout time.Duration) WSHubOption {
	return func(h *WSHub) {
		if timeout > 0 {
			h.adapterTimeout = timeout
		}
	}
}

// WithWSHubAdapterErrorHandler receives asynchronous adapter errors from the
// hub worker. The default reports rate-limited errors through SetPanicHandler.
func WithWSHubAdapterErrorHandler(fn func(error)) WSHubOption {
	return func(h *WSHub) {
		h.adapterErrFn = fn
	}
}

// NewWSHub creates a WebSocket hub. With no adapter, it still fans out across
// every App attached in the current process, which is enough for RunMultiCore.
func NewWSHub(opts ...WSHubOption) *WSHub {
	ctx, cancel := context.WithCancel(context.Background())
	h := &WSHub{
		nodeID:         randomNodeID(),
		apps:           make(map[*App]struct{}),
		sockets:        make(map[uintptr]*hubSocket),
		members:        make(map[string]map[uintptr]struct{}),
		adapterQueueSz: defaultWSHubAdapterQueueSize,
		adapterTimeout: defaultWSHubAdapterPublishTimeout,
		adapterErrFn:   defaultWSHubAdapterErrorHandler,
		startBackoff:   defaultWSHubStartBackoff,
		ctx:            ctx,
		cancel:         cancel,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
	if h.adapter != nil {
		h.ensureAdapterWorker()
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
	return h.publishAdapterAsync(msg)
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
		err := h.publishAdapterAsync(WSHubMessage{
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
// WebSocket or Wrap so the hub can identify the sender.
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
	origin, direct := h.publishFromTargets(ws, key, msg)
	if origin == nil {
		return ErrWSHubUntrackedSocket
	}

	if len(direct) > 0 {
		origin.PublishBatch(direct)
	}
	h.publishLocal(msg, origin)
	return h.publishAdapterAsync(msg)
}

// Close stops the adapter subscription. It does not close any attached App.
func (h *WSHub) Close() error {
	if h == nil {
		return nil
	}
	var err error
	h.closeOnce.Do(func() {
		h.closed.Store(true)
		h.adapterQueueMu.Lock()
		if h.adapterQueue != nil {
			close(h.adapterQueue)
			h.adapterQueue = nil
		}
		h.adapterQueueMu.Unlock()
		h.adapterWG.Wait()
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
	return h.startAdapter(true, false)
}

func (h *WSHub) startAdapter(force bool, allowClosed bool) error {
	if h == nil || h.adapter == nil {
		return nil
	}
	if h.closed.Load() && !allowClosed {
		return ErrWSHubClosed
	}
	h.startMu.Lock()
	defer h.startMu.Unlock()
	if h.adapterStarted {
		return nil
	}
	if !force && h.startErr != nil && time.Now().Before(h.startNextTry) {
		return h.startErr
	}
	err := h.adapter.Start(h.ctx, func(msg WSHubMessage) {
		if msg.NodeID == h.nodeID || h.closed.Load() {
			return
		}
		h.publishLocal(msg, nil)
	})
	if err != nil {
		h.startErr = err
		backoff := h.startBackoff
		if backoff <= 0 {
			backoff = defaultWSHubStartBackoff
		}
		h.startNextTry = time.Now().Add(backoff)
		if backoff < maxWSHubStartBackoff {
			backoff *= 2
			if backoff > maxWSHubStartBackoff {
				backoff = maxWSHubStartBackoff
			}
			h.startBackoff = backoff
		}
		return err
	}
	h.adapterStarted = true
	h.startErr = nil
	h.startNextTry = time.Time{}
	h.startBackoff = defaultWSHubStartBackoff
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

func (h *WSHub) publishAdapter(msg WSHubMessage, allowClosed bool) error {
	if h.adapter == nil {
		return nil
	}
	if h.closed.Load() && !allowClosed {
		return ErrWSHubClosed
	}
	if err := h.startAdapter(false, allowClosed); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), h.adapterTimeout)
	defer cancel()
	return h.adapter.Publish(ctx, msg)
}

func (h *WSHub) publishAdapterAsync(msg WSHubMessage) error {
	if h.adapter == nil {
		return nil
	}
	h.adapterQueueMu.RLock()
	defer h.adapterQueueMu.RUnlock()
	if h.closed.Load() {
		return ErrWSHubClosed
	}
	queue := h.adapterQueue
	if queue == nil {
		return h.publishAdapter(msg, false)
	}
	select {
	case queue <- msg:
		return nil
	case <-h.ctx.Done():
		return ErrWSHubClosed
	default:
		return ErrWSHubAdapterQueueFull
	}
}

func (h *WSHub) ensureAdapterWorker() {
	h.adapterOnce.Do(func() {
		h.adapterQueue = make(chan WSHubMessage, h.adapterQueueSz)
		h.adapterWG.Add(1)
		go h.runAdapterWorker(h.adapterQueue)
	})
}

func (h *WSHub) runAdapterWorker(queue <-chan WSHubMessage) {
	defer h.adapterWG.Done()
	for msg := range queue {
		if err := h.publishAdapter(msg, true); err != nil {
			h.reportAdapterError(err)
		}
	}
}

func (h *WSHub) remember(ws *WebSocket, app *App) {
	key := wsNativeKey(ws)
	if key == 0 {
		return
	}
	h.mu.Lock()
	if socket := h.sockets[key]; socket != nil {
		ws.hubToken.Store(socket.token)
		h.mu.Unlock()
		registerWSHub(key, h)
		return
	}
	direct := h.directTopic(key)
	if !ws.inner.subscribe(direct) {
		h.mu.Unlock()
		return
	}
	token := h.socketSeq.Add(1)
	ws.hubToken.Store(token)
	h.sockets[key] = &hubSocket{
		app:    app,
		direct: direct,
		token:  token,
		topics: make(map[string]struct{}),
	}
	h.mu.Unlock()
	registerWSHub(key, h)
}

func (h *WSHub) forget(ws *WebSocket) {
	key := wsNativeKey(ws)
	if key == 0 {
		return
	}
	h.mu.Lock()
	if socket := h.sockets[key]; socket != nil {
		if token := ws.hubToken.Load(); token != 0 && token != socket.token {
			h.mu.Unlock()
			return
		}
		for topic := range socket.topics {
			h.removeMembershipLocked(key, topic)
		}
	}
	delete(h.sockets, key)
	h.mu.Unlock()
	unregisterWSHub(key, h)
}

func (h *WSHub) publishFromTargets(ws *WebSocket, sender uintptr, msg WSHubMessage) (*App, []PublishMessage) {
	h.mu.RLock()
	socket := h.sockets[sender]
	if socket == nil {
		h.mu.RUnlock()
		return nil, nil
	}
	if token := ws.hubToken.Load(); token != 0 && token != socket.token {
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

func (h *WSHub) addMembership(key uintptr, token uint64, topic string) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	socket := h.sockets[key]
	if socket == nil {
		return 0
	}
	if token != 0 && token != socket.token {
		return 0
	}
	socket.topics[topic] = struct{}{}
	if h.members[topic] == nil {
		h.members[topic] = make(map[uintptr]struct{})
	}
	h.members[topic][key] = struct{}{}
	return socket.token
}

func (h *WSHub) removeMembershipIfCurrent(key uintptr, token uint64, topic string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	socket := h.sockets[key]
	if socket == nil {
		return
	}
	if token != 0 && token != socket.token {
		return
	}
	h.removeMembershipLocked(key, topic)
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

func registerWSHub(key uintptr, h *WSHub) {
	wsHubRegistryMu.Lock()
	wsHubRegistry[key] = h
	wsHubRegistryMu.Unlock()
}

func unregisterWSHub(key uintptr, h *WSHub) {
	wsHubRegistryMu.Lock()
	if wsHubRegistry[key] == h {
		delete(wsHubRegistry, key)
	}
	wsHubRegistryMu.Unlock()
}

func hubForWebSocket(ws *WebSocket) (uintptr, *WSHub) {
	key := wsNativeKey(ws)
	if key == 0 {
		return 0, nil
	}
	wsHubRegistryMu.RLock()
	h := wsHubRegistry[key]
	wsHubRegistryMu.RUnlock()
	return key, h
}

func initWSHubSocket(ws *WebSocket) {
	if ws == nil {
		return
	}
	key, h := hubForWebSocket(ws)
	if h == nil {
		return
	}
	h.mu.RLock()
	if socket := h.sockets[key]; socket != nil {
		ws.hubToken.Store(socket.token)
	}
	h.mu.RUnlock()
}

func trackWSHubSubscribe(ws *WebSocket, topic string) {
	if ws == nil {
		return
	}
	key, h := hubForWebSocket(ws)
	if h == nil {
		return
	}
	if token := h.addMembership(key, ws.hubToken.Load(), topic); token != 0 {
		ws.hubToken.Store(token)
	}
}

func untrackWSHubSubscribe(ws *WebSocket, topic string) {
	if ws == nil {
		return
	}
	key, h := hubForWebSocket(ws)
	if h == nil {
		return
	}
	h.removeMembershipIfCurrent(key, ws.hubToken.Load(), topic)
}

func (h *WSHub) reportAdapterError(err error) {
	if err == nil || h.adapterErrFn == nil {
		return
	}
	now := time.Now()
	msg := err.Error()
	h.adapterErrMu.Lock()
	if msg == h.adapterErrLast && now.Before(h.adapterErrNext) {
		h.adapterErrMu.Unlock()
		return
	}
	h.adapterErrLast = msg
	h.adapterErrNext = now.Add(time.Second)
	fn := h.adapterErrFn
	h.adapterErrMu.Unlock()
	fn(err)
}

func defaultWSHubAdapterErrorHandler(err error) {
	reportPanic(fmt.Errorf("gogo: websocket hub adapter: %w", err))
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
