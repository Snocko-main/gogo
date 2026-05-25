package gogo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrWSHubClosed is returned when a publish or start is attempted after Close.
	ErrWSHubClosed = errors.New("gogo: websocket hub is closed")

	// ErrWSHubCloseTimeout is returned when Close cannot drain the adapter
	// worker within the configured close timeout.
	ErrWSHubCloseTimeout = errors.New("gogo: websocket hub close timeout")

	// ErrWSHubAdapterQueueFull is returned by PublishFrom when the async
	// adapter queue is full. Local subscribers have already been fanned out.
	ErrWSHubAdapterQueueFull = errors.New("gogo: websocket hub adapter queue is full")

	// ErrWSHubUntrackedSocket is returned when PublishFrom cannot identify the
	// sender. Register routes with WSHub.WebSocket or WSHub.Wrap.
	ErrWSHubUntrackedSocket = errors.New("gogo: websocket hub socket is not tracked; register route with hub.WebSocket or hub.Wrap")

	// ErrWSHubInvalidOpCode is returned when a hub publish is called with an
	// opcode other than Text or Binary.
	ErrWSHubInvalidOpCode = errors.New("gogo: websocket hub opcode must be Text or Binary")

	// ErrWSHubTrackFailed is reported when a socket cannot be registered with
	// the hub during WebSocket open.
	ErrWSHubTrackFailed = errors.New("gogo: websocket hub could not track socket")
)

const (
	defaultWSHubAdapterQueueSize      = 1024
	defaultWSHubAdapterPublishTimeout = 5 * time.Second
	defaultWSHubCloseTimeout          = 5 * time.Second
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

	startMu              sync.Mutex
	adapterStarted       bool
	startErr             error
	startNextTry         time.Time
	startBackoff         time.Duration
	adapterQueue         chan wsHubAdapterWork
	adapterTopicQ        chan struct{}
	adapterTopicDirty    map[string]struct{}
	adapterTopicBusy     map[string]struct{}
	adapterTopicApplied  map[string]bool
	adapterTopicRetryAt  map[string]time.Time
	adapterTopicRetry    *time.Timer
	adapterQueueSz       int
	adapterWorkers       int
	adapterTopicWorkers  int
	adapterOnce          sync.Once
	adapterLive          atomic.Int32
	adapterDone          chan struct{}
	adapterQueueMu       sync.RWMutex
	adapterTimeout       time.Duration
	adapterErrMu         sync.Mutex
	adapterErrFn         func(error)
	adapterErrNext       time.Time
	adapterErrSuppressed uint64
	closeTimeout         time.Duration
	socketSeq            atomic.Uint64
	closed               atomic.Bool
	closeOnce            sync.Once
	ctx                  context.Context
	cancel               context.CancelFunc
}

type hubSocket struct {
	app    *App
	direct string
	token  uint64
	open   bool
	topics map[string]struct{}
}

type wsHubAdapterOp uint8

const (
	wsHubAdapterPublish wsHubAdapterOp = iota
)

type wsHubAdapterWork struct {
	op    wsHubAdapterOp
	msg   WSHubMessage
	topic string
}

type wsHubAdapterPanicError struct {
	recovered any
	stack     []byte
}

func (e wsHubAdapterPanicError) Error() string {
	return fmt.Sprintf("gogo: websocket hub adapter panic: %v\n%s", e.recovered, e.stack)
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

// WithWSHubAdapterWorkers sets how many goroutines publish queued adapter
// messages. The default is 1, which preserves queue order. Larger values can
// improve Redis throughput when cross-process message ordering is not required.
func WithWSHubAdapterWorkers(workers int) WSHubOption {
	return func(h *WSHub) {
		if workers > 0 {
			h.adapterWorkers = workers
		}
	}
}

// WithWSHubAdapterTopicWorkers sets how many goroutines reconcile adapter
// topic subscriptions. The default is 1; raise it for high subscription churn.
func WithWSHubAdapterTopicWorkers(workers int) WSHubOption {
	return func(h *WSHub) {
		if workers > 0 {
			h.adapterTopicWorkers = workers
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

// WithWSHubCloseTimeout bounds how long Close waits for queued adapter
// publishes to drain before aborting in-flight adapter work.
func WithWSHubCloseTimeout(timeout time.Duration) WSHubOption {
	return func(h *WSHub) {
		if timeout > 0 {
			h.closeTimeout = timeout
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
		nodeID:              randomNodeID(),
		apps:                make(map[*App]struct{}),
		sockets:             make(map[uintptr]*hubSocket),
		members:             make(map[string]map[uintptr]struct{}),
		adapterQueueSz:      defaultWSHubAdapterQueueSize,
		adapterWorkers:      1,
		adapterTopicWorkers: 1,
		adapterTimeout:      defaultWSHubAdapterPublishTimeout,
		adapterErrFn:        defaultWSHubAdapterErrorHandler,
		closeTimeout:        defaultWSHubCloseTimeout,
		startBackoff:        defaultWSHubStartBackoff,
		ctx:                 ctx,
		cancel:              cancel,
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
	if h.closed.Load() {
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
		if !h.remember(ws, app) {
			h.reportAdapterError(ErrWSHubTrackFailed)
			ws.End(1011, "websocket hub setup failed")
			return
		}
		defer func() {
			if recovered := recover(); recovered != nil {
				h.closeAfterOpenPanic(ws, recovered)
			}
		}()
		if open != nil {
			open(ws)
		}
		h.markOpen(ws)
	}
	behavior.Close = func(ws *WebSocket, code int, msg []byte) {
		opened := h.wasOpen(ws)
		defer h.forget(ws)
		if !opened {
			return
		}
		if closeFn != nil {
			closeFn(ws, code, msg)
		}
	}
	return behavior
}

func (h *WSHub) closeAfterOpenPanic(ws *WebSocket, recovered any) {
	defer h.forget(ws)
	func() {
		defer func() {
			if endRecovered := recover(); endRecovered != nil {
				reportPanic(fmt.Errorf("gogo: websocket hub close after Open panic failed: %v", endRecovered))
			}
		}()
		ws.End(1011, "websocket open panic")
	}()
	reportPanic(recovered)
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

// Publish broadcasts to every local App attached to the hub, then queues the
// message for the adapter if present. It is safe to call from any goroutine.
// Adapter publish failures are reported asynchronously through
// WithWSHubAdapterErrorHandler.
func (h *WSHub) Publish(topic string, message []byte, opcode OpCode) error {
	if h == nil {
		return nil
	}
	if h.closed.Load() {
		return ErrWSHubClosed
	}
	if !validWSHubOpCode(opcode) {
		return ErrWSHubInvalidOpCode
	}
	msg := WSHubMessage{
		NodeID:  h.nodeID,
		Topic:   topic,
		Message: cloneBytes(message),
		OpCode:  opcode,
	}
	h.publishLocal(msg, nil)
	return h.queueAdapterPublish(msg)
}

// PublishBatch broadcasts many messages with one App.PublishBatch call per
// local App, then queues each message for the adapter. For local fan-out this
// keeps the same batching advantage as App.PublishBatch. Adapter publish
// failures are reported asynchronously through WithWSHubAdapterErrorHandler.
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
		if !validWSHubOpCode(msg.OpCode) {
			return ErrWSHubInvalidOpCode
		}
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
		app.publishBatchLocal(local)
	}
	for _, msg := range local {
		err := h.queueAdapterPublish(WSHubMessage{
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
// WebSocket or Wrap so the hub can identify the sender. Adapter publish
// failures are reported asynchronously through WithWSHubAdapterErrorHandler.
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
	if !validWSHubOpCode(opcode) {
		return ErrWSHubInvalidOpCode
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
		origin.publishBatchLocal(direct)
	}
	h.publishLocal(msg, origin)
	return h.queueAdapterPublish(msg)
}

// Close stops the adapter subscription. It does not close any attached App.
func (h *WSHub) Close() error {
	if h == nil {
		return nil
	}
	var err error
	h.closeOnce.Do(func() {
		h.closed.Store(true)
		h.mu.Lock()
		if h.adapterTopicRetry != nil {
			h.adapterTopicRetry.Stop()
			h.adapterTopicRetry = nil
		}
		if h.adapterTopicDirty != nil {
			h.adapterTopicDirty = make(map[string]struct{})
			h.adapterTopicBusy = make(map[string]struct{})
			h.adapterTopicApplied = make(map[string]bool)
			h.adapterTopicRetryAt = make(map[string]time.Time)
		}
		h.mu.Unlock()
		h.adapterQueueMu.Lock()
		if h.adapterQueue != nil {
			close(h.adapterQueue)
			h.adapterQueue = nil
		}
		if h.adapterTopicQ != nil {
			close(h.adapterTopicQ)
			h.adapterTopicQ = nil
		}
		h.adapterQueueMu.Unlock()
		if waitErr := h.waitAdapterWorker(h.closeTimeout); waitErr != nil {
			err = waitErr
		}
		h.cancel()
		if h.adapter != nil {
			if closeErr := h.adapter.Close(); err == nil {
				err = closeErr
			}
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
		app.publishLocal(msg.Topic, msg.Message, msg.OpCode)
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
	ctx, cancel := context.WithTimeout(h.ctx, h.adapterTimeout)
	defer cancel()
	return h.adapter.Publish(ctx, msg)
}

func (h *WSHub) waitAdapterWorker(timeout time.Duration) error {
	if h.waitAdapterWorkerDone(timeout) {
		return nil
	}
	h.cancel()
	if h.waitAdapterWorkerDone(timeout) {
		return nil
	}
	return ErrWSHubCloseTimeout
}

func (h *WSHub) waitAdapterWorkerDone(timeout time.Duration) bool {
	done := h.adapterDone
	if done == nil {
		return true
	}
	var timer *time.Timer
	var timeoutC <-chan time.Time
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timeoutC = timer.C
		defer timer.Stop()
	}
	select {
	case <-done:
		return true
	case <-timeoutC:
		return false
	}
}

func (h *WSHub) queueAdapterPublish(msg WSHubMessage) error {
	return h.queueAdapterWork(wsHubAdapterWork{op: wsHubAdapterPublish, msg: msg})
}

func (h *WSHub) queueAdapterTopic(topic string) {
	if _, ok := h.adapter.(WSHubTopicAdapter); !ok {
		return
	}
	h.mu.Lock()
	if h.closed.Load() {
		h.mu.Unlock()
		return
	}
	if h.adapterTopicDirty == nil {
		h.adapterTopicDirty = make(map[string]struct{})
	}
	h.adapterTopicDirty[topic] = struct{}{}
	h.mu.Unlock()
	h.signalAdapterTopic()
}

func (h *WSHub) signalAdapterTopic() {
	h.adapterQueueMu.RLock()
	defer h.adapterQueueMu.RUnlock()
	queue := h.adapterTopicQ
	closed := h.closed.Load()
	if queue == nil || closed {
		return
	}
	select {
	case queue <- struct{}{}:
	case <-h.ctx.Done():
	default:
	}
}

func (h *WSHub) queueAdapterWork(work wsHubAdapterWork) error {
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
		return h.runAdapterWork(work, false)
	}
	select {
	case queue <- work:
		return nil
	case <-h.ctx.Done():
		return ErrWSHubClosed
	default:
		return ErrWSHubAdapterQueueFull
	}
}

func (h *WSHub) ensureAdapterWorker() {
	h.adapterOnce.Do(func() {
		workers := h.adapterWorkers
		topicWorkers := 0
		if _, ok := h.adapter.(WSHubTopicAdapter); ok {
			topicWorkers = h.adapterTopicWorkers
		}
		h.adapterLive.Store(int32(workers + topicWorkers))
		h.adapterDone = make(chan struct{})
		h.adapterQueue = make(chan wsHubAdapterWork, h.adapterQueueSz)
		for range workers {
			go h.runAdapterWorker(h.adapterQueue)
		}
		if topicWorkers > 0 {
			h.adapterTopicQ = make(chan struct{}, 1)
			h.adapterTopicDirty = make(map[string]struct{})
			h.adapterTopicBusy = make(map[string]struct{})
			h.adapterTopicApplied = make(map[string]bool)
			h.adapterTopicRetryAt = make(map[string]time.Time)
			for range topicWorkers {
				go h.runAdapterTopicWorker(h.adapterTopicQ)
			}
		}
	})
}

func (h *WSHub) runAdapterWorker(queue <-chan wsHubAdapterWork) {
	defer func() {
		h.adapterWorkerDone()
	}()
	h.runAdapterWorkLoop(queue)
}

func (h *WSHub) runAdapterTopicWorker(queue <-chan struct{}) {
	defer func() {
		h.adapterWorkerDone()
	}()
	for range queue {
		for {
			topic, ok := h.nextAdapterTopic()
			if !ok {
				h.scheduleAdapterTopicRetry()
				break
			}
			h.reconcileAdapterTopicSafely(topic)
		}
	}
}

func (h *WSHub) adapterWorkerDone() {
	if h.adapterLive.Add(-1) == 0 {
		close(h.adapterDone)
	}
}

func (h *WSHub) runAdapterWorkLoop(queue <-chan wsHubAdapterWork) {
	for work := range queue {
		if err := h.runAdapterWorkSafely(work); err != nil {
			h.reportAdapterError(err)
		}
	}
}

func (h *WSHub) runAdapterWorkSafely(work wsHubAdapterWork) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = wsHubAdapterPanicError{recovered: recovered, stack: debug.Stack()}
		}
	}()
	return h.runAdapterWork(work, true)
}

func (h *WSHub) runAdapterWork(work wsHubAdapterWork, allowClosed bool) error {
	switch work.op {
	case wsHubAdapterPublish:
		return h.publishAdapter(work.msg, allowClosed)
	default:
		return fmt.Errorf("gogo: websocket hub unknown adapter op %d", work.op)
	}
}

func (h *WSHub) nextAdapterTopic() (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for topic := range h.adapterTopicDirty {
		if _, busy := h.adapterTopicBusy[topic]; busy {
			continue
		}
		if retryAt := h.adapterTopicRetryAt[topic]; !retryAt.IsZero() && time.Now().Before(retryAt) {
			continue
		}
		delete(h.adapterTopicDirty, topic)
		h.adapterTopicBusy[topic] = struct{}{}
		return topic, true
	}
	return "", false
}

func (h *WSHub) reconcileAdapterTopic(topic string) {
	desired, applied := h.adapterTopicState(topic)
	if desired == applied {
		h.finishAdapterTopic(topic, applied, nil)
		return
	}
	err := h.updateAdapterTopic(topic, desired)
	h.finishAdapterTopic(topic, desired, err)
}

func (h *WSHub) reconcileAdapterTopicSafely(topic string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			h.finishAdapterTopic(topic, false, wsHubAdapterPanicError{recovered: recovered, stack: debug.Stack()})
		}
	}()
	h.reconcileAdapterTopic(topic)
}

func (h *WSHub) adapterTopicState(topic string) (desired, applied bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.members[topic]) > 0, h.adapterTopicApplied[topic]
}

func (h *WSHub) finishAdapterTopic(topic string, desired bool, err error) {
	h.mu.Lock()
	if h.closed.Load() {
		delete(h.adapterTopicBusy, topic)
		h.mu.Unlock()
		return
	}
	if err == nil {
		if desired {
			h.adapterTopicApplied[topic] = true
		} else {
			delete(h.adapterTopicApplied, topic)
		}
		delete(h.adapterTopicRetryAt, topic)
	} else {
		h.adapterTopicDirty[topic] = struct{}{}
		h.adapterTopicRetryAt[topic] = time.Now().Add(defaultWSHubStartBackoff)
	}
	delete(h.adapterTopicBusy, topic)
	needsSignal := len(h.adapterTopicDirty) > 0 && !h.closed.Load()
	h.mu.Unlock()
	if err != nil {
		h.reportAdapterError(err)
		h.scheduleAdapterTopicRetry()
		return
	}
	if needsSignal {
		h.signalAdapterTopic()
	}
}

func (h *WSHub) scheduleAdapterTopicRetry() {
	h.mu.Lock()
	if h.closed.Load() || h.adapterTopicRetry != nil {
		h.mu.Unlock()
		return
	}
	var next time.Time
	now := time.Now()
	for topic := range h.adapterTopicDirty {
		if _, busy := h.adapterTopicBusy[topic]; busy {
			continue
		}
		retryAt := h.adapterTopicRetryAt[topic]
		if retryAt.IsZero() || !retryAt.After(now) {
			h.mu.Unlock()
			h.signalAdapterTopic()
			return
		}
		if next.IsZero() || retryAt.Before(next) {
			next = retryAt
		}
	}
	if next.IsZero() {
		h.mu.Unlock()
		return
	}
	delay := time.Until(next)
	h.adapterTopicRetry = time.AfterFunc(delay, h.fireAdapterTopicRetry)
	h.mu.Unlock()
}

func (h *WSHub) fireAdapterTopicRetry() {
	h.mu.Lock()
	h.adapterTopicRetry = nil
	closed := h.closed.Load()
	h.mu.Unlock()
	if !closed {
		h.signalAdapterTopic()
	}
}

func (h *WSHub) updateAdapterTopic(topic string, subscribe bool) error {
	adapter, ok := h.adapter.(WSHubTopicAdapter)
	if !ok {
		return nil
	}
	if h.closed.Load() {
		return ErrWSHubClosed
	}
	if err := h.startAdapter(false, false); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(h.ctx, h.adapterTimeout)
	defer cancel()
	if subscribe {
		return adapter.Subscribe(ctx, topic)
	}
	return adapter.Unsubscribe(ctx, topic)
}

func (h *WSHub) remember(ws *WebSocket, app *App) bool {
	if h.closed.Load() {
		return false
	}
	key := wsNativeKey(ws)
	if key == 0 {
		return false
	}
	if !reserveWSHub(key, h) {
		return false
	}
	h.mu.Lock()
	if socket := h.sockets[key]; socket != nil {
		ws.hubToken.Store(socket.token)
		h.mu.Unlock()
		return true
	}
	direct := randomDirectTopic(h.nodeID)
	if !ws.inner.subscribe(direct) {
		h.mu.Unlock()
		unregisterWSHub(key, h)
		return false
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
	return true
}

func (h *WSHub) markOpen(ws *WebSocket) {
	key := wsNativeKey(ws)
	if key == 0 {
		return
	}
	h.mu.Lock()
	if socket := h.sockets[key]; socket != nil {
		if token := ws.hubToken.Load(); token != socket.token {
			h.mu.Unlock()
			return
		}
		socket.open = true
	}
	h.mu.Unlock()
}

func (h *WSHub) wasOpen(ws *WebSocket) bool {
	key := wsNativeKey(ws)
	if key == 0 {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	socket := h.sockets[key]
	if socket == nil {
		return false
	}
	return ws.hubToken.Load() == socket.token && socket.open
}

func (h *WSHub) forget(ws *WebSocket) {
	key := wsNativeKey(ws)
	if key == 0 {
		return
	}
	h.mu.Lock()
	if socket := h.sockets[key]; socket != nil {
		if token := ws.hubToken.Load(); token != socket.token {
			h.mu.Unlock()
			return
		}
		var unsub []string
		for topic := range socket.topics {
			if h.removeMembershipLocked(key, topic) {
				unsub = append(unsub, topic)
			}
		}
		delete(h.sockets, key)
		h.mu.Unlock()
		for _, topic := range unsub {
			h.queueAdapterTopic(topic)
		}
	} else {
		h.mu.Unlock()
	}
	unregisterWSHub(key, h)
}

func (h *WSHub) publishFromTargets(ws *WebSocket, sender uintptr, msg WSHubMessage) (*App, []PublishMessage) {
	h.mu.RLock()
	socket := h.sockets[sender]
	if socket == nil {
		h.mu.RUnlock()
		return nil, nil
	}
	if token := ws.hubToken.Load(); token != socket.token {
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

func (h *WSHub) addMembership(key uintptr, token uint64, topic string) (uint64, bool) {
	if h.closed.Load() {
		return 0, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	socket := h.sockets[key]
	if socket == nil {
		return 0, false
	}
	if token != socket.token {
		return 0, false
	}
	if _, ok := socket.topics[topic]; ok {
		return socket.token, false
	}
	socket.topics[topic] = struct{}{}
	first := len(h.members[topic]) == 0
	if h.members[topic] == nil {
		h.members[topic] = make(map[uintptr]struct{})
	}
	h.members[topic][key] = struct{}{}
	return socket.token, first
}

func (h *WSHub) removeMembershipIfCurrent(key uintptr, token uint64, topic string) {
	_, last := h.removeMembershipForUnsubscribe(key, token, topic)
	if last {
		h.queueAdapterTopic(topic)
	}
}

func (h *WSHub) removeMembershipForUnsubscribe(key uintptr, token uint64, topic string) (bool, bool) {
	h.mu.Lock()
	if h.closed.Load() {
		h.mu.Unlock()
		return false, false
	}
	socket := h.sockets[key]
	if socket == nil {
		h.mu.Unlock()
		return false, false
	}
	if token != socket.token {
		h.mu.Unlock()
		return false, false
	}
	if _, ok := socket.topics[topic]; !ok {
		h.mu.Unlock()
		return false, false
	}
	last := h.removeMembershipLocked(key, topic)
	h.mu.Unlock()
	return true, last
}

func (h *WSHub) restoreMembershipIfCurrent(key uintptr, token uint64, topic string) (bool, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed.Load() {
		return false, false
	}
	socket := h.sockets[key]
	if socket == nil || token != socket.token {
		return false, false
	}
	if _, ok := socket.topics[topic]; ok {
		return false, false
	}
	socket.topics[topic] = struct{}{}
	first := len(h.members[topic]) == 0
	if h.members[topic] == nil {
		h.members[topic] = make(map[uintptr]struct{})
	}
	h.members[topic][key] = struct{}{}
	return first, true
}

func (h *WSHub) removeMembershipLocked(key uintptr, topic string) bool {
	if socket := h.sockets[key]; socket != nil {
		if _, ok := socket.topics[topic]; !ok {
			return false
		}
		delete(socket.topics, topic)
	}
	members := h.members[topic]
	if members == nil {
		return false
	}
	delete(members, key)
	if len(members) == 0 {
		delete(h.members, topic)
		return true
	}
	return false
}

func reserveWSHub(key uintptr, h *WSHub) bool {
	wsHubRegistryMu.Lock()
	defer wsHubRegistryMu.Unlock()
	if existing := wsHubRegistry[key]; existing != nil && existing != h {
		return false
	}
	wsHubRegistry[key] = h
	return true
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

func trackWSHubSubscribe(ws *WebSocket, topic string) (*WSHub, bool) {
	if ws == nil {
		return nil, true
	}
	key, h := hubForWebSocket(ws)
	if h == nil {
		return nil, true
	}
	if token, first := h.addMembership(key, ws.hubToken.Load(), topic); token != 0 {
		ws.hubToken.Store(token)
		if first {
			h.queueAdapterTopic(topic)
		}
		return h, true
	}
	return h, false
}

func (h *WSHub) reportAdapterError(err error) {
	if err == nil || h.adapterErrFn == nil {
		return
	}
	now := time.Now()
	h.adapterErrMu.Lock()
	if now.Before(h.adapterErrNext) {
		h.adapterErrSuppressed++
		h.adapterErrMu.Unlock()
		return
	}
	suppressed := h.adapterErrSuppressed
	h.adapterErrSuppressed = 0
	h.adapterErrNext = now.Add(time.Second)
	fn := h.adapterErrFn
	h.adapterErrMu.Unlock()
	if suppressed > 0 {
		err = fmt.Errorf("%w (suppressed %d websocket hub adapter errors)", err, suppressed)
	}
	callWSHubAdapterErrorHandler(fn, err)
}

func callWSHubAdapterErrorHandler(fn func(error), err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			reportPanic(fmt.Errorf("gogo: websocket hub adapter error handler panicked: %v", recovered))
		}
	}()
	fn(err)
}

func defaultWSHubAdapterErrorHandler(err error) {
	reportPanic(fmt.Errorf("gogo: websocket hub adapter: %w", err))
}

func validWSHubOpCode(opcode OpCode) bool {
	return opcode == Text || opcode == Binary
}

func randomDirectTopic(nodeID string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return fmt.Sprintf("__gogo_hub:%s:%s", nodeID, hex.EncodeToString(b[:]))
	}
	return fmt.Sprintf("__gogo_hub:%s:%s", nodeID, fallbackNodeID())
}

func randomNodeID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fallbackNodeID()
	}
	return hex.EncodeToString(b[:])
}

func fallbackNodeID() string {
	return fmt.Sprintf("local-%d-%d-%d", os.Getpid(), time.Now().UnixNano(), fallbackNodeCounter.Add(1))
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
