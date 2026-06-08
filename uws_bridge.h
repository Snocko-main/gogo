#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct uwsgo_app_t uwsgo_app_t;
typedef struct uwsgo_res_t uwsgo_res_t;
typedef struct uwsgo_req_t uwsgo_req_t;
typedef struct uwsgo_ws_t uwsgo_ws_t;
typedef struct uwsgo_loop_t uwsgo_loop_t;

uwsgo_app_t *uwsgo_app_new(void);
void uwsgo_app_free(uwsgo_app_t *app);

// uwsgo_app_set_body_limit sets the maximum body bytes a Post / Any route
// will accept. uWS evaluates the Content-Length header at request arrival
// and rejects with 413 before dispatching to Go when the declared length
// exceeds the limit. Chunked requests with no Content-Length bypass this
// check; handlers that accept chunked uploads should call res.Body(maxN,
// ...) themselves for protection. Pass 0 to disable.
void uwsgo_app_set_body_limit(uwsgo_app_t *app, size_t limit);

// uwsgo_app_set_capture_peer_ip toggles whether snapshot_request copies
// the formatted peer IP into AsyncCtx for shared-dispatch / async paths.
// Off by default — see Go-side Config.CapturePeerIP for the trade-off.
void uwsgo_app_set_capture_peer_ip(uwsgo_app_t *app, int enable);

void uwsgo_app_get(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id);
void uwsgo_app_post(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id);
void uwsgo_app_any(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id);
// Method helpers — same semantics as uwsgo_app_get / _post; PUT, PATCH,
// and DELETE go through body_limit_rejects so the Content-Length cap
// applies; OPTIONS and HEAD are bodyless and skip the check.
void uwsgo_app_put(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id);
void uwsgo_app_patch(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id);
void uwsgo_app_delete(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id);
void uwsgo_app_options(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id);
void uwsgo_app_head(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id);
// uwsgo_app_ws registers a WebSocket endpoint with per-route limits
// forwarded to the uWS WebSocketBehavior template (max payload, idle
// timeout in seconds, backpressure cap, automatic ping/pong toggle).
//
// with_upgrade != 0 registers an `upgrade` callback that bounces
// every incoming handshake through Go before the connection is
// established. The Go callback inspects request headers / URL /
// peer IP and either calls uwsgo_res_upgrade_accept (to complete
// the handshake, optionally negotiating a subprotocol and attaching
// per-socket user data) or uwsgo_res_upgrade_reject (to refuse with
// an HTTP status). When with_upgrade is 0 every request is
// auto-upgraded by uWS's default path (matches the pre-flag
// behavior).
void uwsgo_app_ws(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id,
    size_t max_payload, int idle_seconds, size_t max_backpressure,
    int send_pings_automatically, int with_upgrade);

// uwsgo_res_upgrade_accept completes a WebSocket handshake started
// by a Go upgrade callback. ctx_ptr is the opaque UpgradeCtx passed
// to Go from the C++ upgrade lambda. user_data (typically a Go
// cgo.Handle stored as uintptr) is stashed on the WebSocket's
// per-socket data so handlers can retrieve it later via ws.UserData.
//
// secProtocol picks the subprotocol echoed back to the client (must
// be one the client offered, or empty for no negotiation). The
// other Sec-WebSocket-* headers are passed through unchanged.
void uwsgo_res_upgrade_accept(void *ctx_ptr,
    const char *sec_protocol, size_t sec_protocol_len,
    uintptr_t user_data);

// uwsgo_res_upgrade_reject refuses a WebSocket upgrade started by a
// Go upgrade callback. Writes a plain-text body with the given HTTP
// status line and closes the response.
void uwsgo_res_upgrade_reject(void *ctx_ptr,
    const char *status, size_t status_len,
    const char *body, size_t body_len);

// uwsgo_ws_user_data returns the per-socket user-data handle that
// the upgrade callback stored via uwsgo_res_upgrade_accept. Returns
// 0 when no handle was attached. Safe to call from any WS callback
// (open / message / close).
uintptr_t uwsgo_ws_user_data(uwsgo_ws_t *ws);

// uwsgo_ws_set_user_data overwrites the per-socket user-data handle
// after the connection is established. Useful for routes that
// authenticate inside the Open callback rather than at upgrade.
// Callers are responsible for releasing the previous handle (if
// any) before installing a new one.
void uwsgo_ws_set_user_data(uwsgo_ws_t *ws, uintptr_t user_data);

// Static GET: response is captured once at registration time and served by
// the C++ event loop directly without any cgo callback per request. Status,
// Content-Type, and body are copied into app-owned memory so the original Go
// strings can be reclaimed. Pass content_type_len=0 to skip the header.
void uwsgo_app_get_static(uwsgo_app_t *app, const char *pattern,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *body, size_t body_len);

// uwsgo_app_listen binds the app to host:port. Pass NULL or "" for host
// to keep uWS's default behavior (all interfaces, 0.0.0.0).
int uwsgo_app_listen(uwsgo_app_t *app, const char *host, int port);
int uwsgo_app_add_child(uwsgo_app_t *parent, uwsgo_app_t *child);
void uwsgo_app_run(uwsgo_app_t *app);
void uwsgo_app_stop(uwsgo_app_t *app);

// uwsgo_app_close_listen closes only the listen socket and the drain
// timer, leaving active connections untouched so they can complete
// their in-flight responses naturally. The uWS loop returns from run()
// only after every remaining socket closes itself (or the caller
// follows up with uwsgo_app_stop to force the rest).
void uwsgo_app_close_listen(uwsgo_app_t *app);

void uwsgo_res_write_status(uwsgo_res_t *res, const char *status, size_t status_len);
void uwsgo_res_write_header(uwsgo_res_t *res, const char *key, size_t key_len, const char *value, size_t value_len);

// uwsgo_res_write_headers_batch writes N headers in a single cgo
// crossing. headers_blob is the packed `key\0value\0key\0value\0`
// representation that flushPendingHeaders already produces for the
// async defer path. count is the number of (key, value) pairs the
// blob carries — the function reads exactly 2*count zero-terminated
// runs starting at headers_blob. Saves (N-1) cgo crossings per
// response when N >= 2 (CORS alone adds 3-4 headers).
void uwsgo_res_write_headers_batch(uwsgo_res_t *res,
    const char *headers_blob, size_t headers_len, size_t count);

void uwsgo_res_write(uwsgo_res_t *res, const char *body, size_t body_len);
void uwsgo_res_end(uwsgo_res_t *res, const char *body, size_t body_len);
void uwsgo_res_send(
    uwsgo_res_t *res,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *body, size_t body_len);
void uwsgo_res_send_split(
    uwsgo_res_t *res,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *headers_blob, size_t headers_len,
    const char *prefix, size_t prefix_len,
    const char *body, size_t body_len);

// uwsgo_res_buffered_amount returns how many bytes uWS has accepted
// for sending but hasn't yet flushed to the socket — the standard
// backpressure signal. Grows when the client isn't draining fast
// enough; shrinks as the kernel acknowledges sends.
//
// The read is intentionally non-locking: uWS's underlying counter
// is a single naturally-aligned size_t in uSockets, so the read
// returns a coherent (possibly slightly stale) snapshot even when
// called from a worker goroutine that isn't the loop thread.
// Callers using this as a throttle threshold should not depend on
// exact synchronization with the loop; the value is sampled, not
// transactional.
size_t uwsgo_res_buffered_amount(uwsgo_res_t *res);

// uwsgo_res_remote_addr writes the formatted peer IP into buffer (returns
// the size needed if buffer is too small or NULL). uWS caches the
// formatted string on first call so this is effectively free for any
// subsequent reads on the same response. Use req.IP() in handlers; it
// caches the materialized Go string on the Request wrapper.
size_t uwsgo_res_remote_addr(uwsgo_res_t *res, char *buffer, size_t buffer_len);

uwsgo_loop_t *uwsgo_res_get_loop(uwsgo_res_t *res);
void uwsgo_loop_defer(uwsgo_loop_t *loop, uintptr_t callback_id);
void uwsgo_res_on_aborted(uwsgo_res_t *res, uintptr_t callback_id);
void uwsgo_res_cork(uwsgo_res_t *res, uintptr_t callback_id);

// Registers an onData callback for body streaming. The Go callback is invoked
// once per chunk uWS receives. is_last == 1 on the final chunk; the handle is
// released by Go after that call. Must be registered inside the route handler,
// before it returns — uWS buffers chunks until the loop processes them.
void uwsgo_res_on_data(uwsgo_res_t *res, uintptr_t callback_id);

// Async fast path: begin_async registers an abort handler internally, returns
// the loop pointer, and sets *out_ctx to an opaque handle that must be passed
// to either uwsgo_res_defer_send (to send a response) or uwsgo_async_ctx_release
// (to drop the response without sending). Combines what previously required
// separate OnAborted + Loop cgo calls into one.
uwsgo_loop_t *uwsgo_res_begin_async(uwsgo_res_t *res, void **out_ctx);

// defer_send schedules a complete response on the loop. Status, Content-Type,
// and body are copied into C heap so the call is safe to make from any goroutine.
// Pass content_type_len = 0 to skip the Content-Type header. The defer callback
// checks the abort flag and silently drops the response if the client has
// already disconnected. Transfers ownership of the ctx's Go-side ref to the
// defer; Go must not touch ctx after this call returns.
void uwsgo_res_defer_send(
    uwsgo_loop_t *loop,
    void *ctx,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *body, size_t body_len);

// defer_send_with_headers is the same as defer_send but also writes a list of
// arbitrary HTTP response headers between the status line and the Content-Type
// header. Used by middleware that needs to attach headers to an async response
// (compression's Content-Encoding/Vary, request-id echoes set by sync
// middleware before the handler called Async, etc.).
//
// headers_blob is a packed list of NUL-terminated name/value pairs:
//
//   name1\0value1\0name2\0value2\0...
//
// Pass headers_len = 0 (and headers_blob = NULL) to skip the extra headers,
// in which case the call behaves identically to uwsgo_res_defer_send.
void uwsgo_res_defer_send_with_headers(
    uwsgo_loop_t *loop,
    void *ctx,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *headers_blob, size_t headers_len,
    const char *body, size_t body_len);

// defer_stream_start writes the response status line and headers but
// does NOT call end() — opening the door for incremental chunked
// body bytes via defer_stream_write. uWS auto-emits HTTP/1.1
// chunked transfer-encoding when Content-Length is absent, which is
// the expected mode for these streaming helpers (don't set
// Content-Length unless you know the total payload size in advance).
//
// Each accepted stream_* operation retains the ctx before queuing the
// loop defer, so the AsyncCtx outlives every pending write. The caller
// still owns the original async ctx ref and must release it when the
// stream function returns. stream_start returns non-zero only when the
// opening frame was accepted for queuing.
int uwsgo_res_defer_stream_start(
    uwsgo_loop_t *loop,
    void *ctx,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *headers_blob, size_t headers_len);

// defer_stream_write appends one body chunk to a streaming response
// started by defer_stream_start. Calls are serialized on the loop
// thread in FIFO order, so chunks reach the wire in the same order
// the caller emitted them. The chunk bytes are duplicated to the C
// heap before the defer is queued so the Go-side memory is free to
// be reused or freed immediately.
void uwsgo_res_defer_stream_write(
    uwsgo_loop_t *loop,
    void *ctx,
    const char *chunk, size_t chunk_len);

// defer_stream_end closes a streaming response with an empty body —
// uWS emits the chunked-encoding terminator and tears down the
// HttpResponse. After this returns, no further stream_* calls are
// valid against the same ctx.
void uwsgo_res_defer_stream_end(
    uwsgo_loop_t *loop,
    void *ctx);

// async_ctx_release drops Go's reference to ctx without sending a response.
// Call this if a goroutine returns without invoking defer_send.
void uwsgo_async_ctx_release(void *ctx);

// async_ctx_retain adds a reference to ctx. Used by Go-side helpers that need
// to poll ctx state after the user handler may have sent the response.
void uwsgo_async_ctx_retain(void *ctx);

// async_ctx_aborted reports whether the async response's client connection
// has already disconnected. It is safe to sample from a worker goroutine.
int uwsgo_async_ctx_aborted(void *ctx);

// async_ctx_stream_pending_bytes reports body bytes copied into queued stream
// defers that the loop thread has not yet handed to uWS.
size_t uwsgo_async_ctx_stream_pending_bytes(void *ctx);

// Memory layout exposed to Go for the shared-memory fast path. Go reads this
// once at startup, then writes responses directly into ctx memory and pushes
// onto the shared ring using plain atomic operations — no cgo crossing per
// response. The loop drains the ring on every drain-timer tick.
typedef struct uwsgo_shared_layout_t {
    void *ring;
    void *request_ring;
    size_t ring_size;
    size_t ring_mask;
    size_t ring_slots_offset;
    size_t ring_slot_stride;
    size_t ring_slot_seq_offset;
    size_t ring_slot_ctx_offset;
    size_t ring_head_offset;
    size_t ring_tail_offset;
    size_t ring_wake_pending_offset;
    size_t ctx_status_len_offset;
    size_t ctx_ct_len_offset;
    size_t ctx_body_len_offset;
    size_t ctx_status_offset;
    size_t ctx_ct_offset;
    size_t ctx_body_offset;
    size_t ctx_handler_id_offset;
    size_t ctx_aborted_offset;
    size_t ctx_response_offset;
    size_t ctx_loop_offset;
    size_t ctx_shared_state_offset;
    size_t state_closing_offset;
    size_t state_active_sends_offset;
    // Per-App response ring pointer carried inline in each AsyncCtx so Go's
    // SendShared can push to the right App's ring when multiple Apps run
    // in the same process.
    size_t ctx_pending_ring_offset;
    size_t ctx_inline_status_cap;
    size_t ctx_inline_ct_cap;
    size_t ctx_inline_body_cap;
    // Request snapshot offsets/caps (see SNAP_* constants in the cpp file).
    size_t ctx_method_len_offset;
    size_t ctx_url_len_offset;
    size_t ctx_query_len_offset;
    size_t ctx_param_count_offset;
    size_t ctx_headers_len_offset;
    size_t ctx_truncated_offset;
    size_t ctx_param_lens_offset;
    size_t ctx_method_offset;
    size_t ctx_url_offset;
    size_t ctx_query_offset;
    size_t ctx_ip_len_offset;
    size_t ctx_ip_offset;
    size_t ctx_params_offset;
    size_t ctx_headers_offset;
    // Request body (post_shared); distinct from the inline
    // response body (ctx_body_offset above) which has a fixed
    // tiny cap for the response payload.
    size_t ctx_req_body_len_offset;
    size_t ctx_req_body_overflow_offset;
    size_t ctx_req_body_offset;
    size_t ctx_snap_method_cap;
    size_t ctx_snap_url_cap;
    size_t ctx_snap_query_cap;
    size_t ctx_snap_ip_cap;
    size_t ctx_snap_param_cap;
    size_t ctx_snap_param_max;
    size_t ctx_snap_headers_cap;
    size_t ctx_snap_req_body_cap;
} uwsgo_shared_layout_t;

void uwsgo_shared_layout(uwsgo_shared_layout_t *out);

// Installs a periodic timer on the given App's loop that drains the App's
// pending ring. libuS timers are millisecond-granularity, so any value below
// 1000 us is clamped to 1 ms. Used as a safety net; the per-response wake
// (uwsgo_wake_drain) keeps latency sub-ms in practice.
void uwsgo_app_start_drain(uwsgo_app_t *app, int interval_us);

// Wakes the given loop and runs one drain pass on the given ring. Called
// from Go after pushing onto the response ring so the loop flushes
// immediately instead of waiting for the periodic timer to fire.
// Thread-safe via uWS::Loop::defer.
void uwsgo_wake_drain(uwsgo_loop_t *loop, void *ring);

// Registers a route whose dispatch path bypasses cgo entirely: C++ pushes
// the AsyncCtx onto the request ring; Go worker goroutines drain the ring
// and invoke the handler bound to handler_id. The Go side keeps the
// handler_id -> handler map.
void uwsgo_app_get_shared(uwsgo_app_t *app, const char *pattern, uint32_t handler_id);

// uwsgo_app_post_shared registers a POST route that uses the same
// zero-cgo shared-memory dispatch path as uwsgo_app_get_shared,
// but also collects the request body into the per-request
// AsyncCtx before pushing onto the ring. Bodies up to max_body
// (capped internally at SNAP_BODY_CAP = 8 KiB) succeed; oversized
// bodies short-circuit with 413 on the loop thread, no goroutine
// spawned. The Go side reads the collected bytes via the snapshot
// layout fields (ctx_body_offset / ctx_body_len_offset).
//
// max_body == 0 means "use SNAP_BODY_CAP as the cap" — the
// default Go shim picks this when the user didn't pass an
// explicit limit smaller than the buffer.
void uwsgo_app_post_shared(uwsgo_app_t *app, const char *pattern,
    uint32_t handler_id, size_t max_body);

// Returns the lower-cased HTTP method ("get", "post", ...).
size_t uwsgo_req_method(uwsgo_req_t *req, char *buffer, size_t buffer_len);
size_t uwsgo_req_url(uwsgo_req_t *req, char *buffer, size_t buffer_len);
size_t uwsgo_req_header(uwsgo_req_t *req, const char *name, size_t name_len, char *buffer, size_t buffer_len);
size_t uwsgo_req_parameter(uwsgo_req_t *req, unsigned long index, char *buffer, size_t buffer_len);
// Returns the raw query string portion of the URL (no leading '?'), or empty
// when the request has no query string.
size_t uwsgo_req_query(uwsgo_req_t *req, char *buffer, size_t buffer_len);
// Returns the value of a single query parameter, or empty when the key is
// missing. uWS performs a linear scan of the query string; cache the value
// if you need it more than once.
size_t uwsgo_req_query_param(uwsgo_req_t *req, const char *name, size_t name_len, char *buffer, size_t buffer_len);
// Writes all headers as "name\0value\0name\0value\0..." into buffer.
// Returns total bytes written, or the bytes needed if buffer is too small
// (caller passes buffer_len=0 first to size). Truncates without splitting a
// key/value pair when the buffer would overflow.
size_t uwsgo_req_headers_all(uwsgo_req_t *req, char *buffer, size_t buffer_len);

int uwsgo_ws_send(uwsgo_ws_t *ws, const char *message, size_t message_len, int opcode);
void uwsgo_ws_end(uwsgo_ws_t *ws, int code, const char *message, size_t message_len);

// WebSocket pub/sub. Subscribe / unsubscribe / publish must be called
// from inside an Open / Message / Close handler (loop thread), where
// the WebSocket pointer is live. uWS's TopicTree is loop-thread-local
// — calling these from a worker goroutine is undefined behavior.
//
// uwsgo_ws_subscribe / unsubscribe return 1 on success, 0 on failure
// (already in the requested state, or the connection is closing).
// uwsgo_ws_publish returns 1 if the message was queued for delivery
// to at least one subscriber. uWS sender publishes exclude the publishing
// socket itself.
int uwsgo_ws_subscribe(uwsgo_ws_t *ws, const char *topic, size_t topic_len);
int uwsgo_ws_unsubscribe(uwsgo_ws_t *ws, const char *topic, size_t topic_len);
int uwsgo_ws_publish(uwsgo_ws_t *ws, const char *topic, size_t topic_len,
    const char *message, size_t message_len, int opcode);

// uwsgo_app_publish broadcasts to every subscriber of topic on the
// app's WebSocket context. Thread-safe — the publish is internally
// dispatched onto the app's loop, so this can be called from any
// goroutine (typical use: a worker that just finished some work and
// wants to push the result to clients). The message bytes are copied
// onto the loop's heap before the deferred publish, so the caller's
// buffer can be reclaimed as soon as this returns.
void uwsgo_app_publish(uwsgo_app_t *app, const char *topic, size_t topic_len,
    const char *message, size_t message_len, int opcode);

// uwsgo_batch_item_t describes one publish inside a batch. All four
// length / offset fields point into the contiguous `bytes` blob the
// caller passes to uwsgo_app_publish_batch — no Go pointers cross
// the cgo boundary inside this struct, so the batch is safe to pass
// from Go as a slice of these without tripping cgocheck.
typedef struct uwsgo_batch_item_t {
    size_t topic_off;
    size_t topic_len;
    size_t message_off;
    size_t message_len;
    int opcode;
} uwsgo_batch_item_t;

// uwsgo_app_publish_batch publishes N messages with a single cgo
// crossing and a single Loop::defer (one mutex acquire + one
// wakeup). The C++ side does ONE allocation that owns both the
// items array and the byte blob; the deferred lambda iterates and
// publishes each item, then frees. Saves (N-1) cgo crossings and
// (N-1) defer-mutex acquires versus calling uwsgo_app_publish N
// times — measured ~10x faster at N=100 on this VM.
//
// `bytes` is the packed concatenation of every (topic, message)
// pair; each item's *_off / *_len pair points into it. `count` is
// the number of items in the array.
void uwsgo_app_publish_batch(
    uwsgo_app_t *app,
    const char *bytes, size_t bytes_len,
    const uwsgo_batch_item_t *items, size_t count);

#ifdef __cplusplus
}
#endif
