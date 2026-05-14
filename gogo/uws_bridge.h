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

void uwsgo_app_get(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id);
void uwsgo_app_post(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id);
void uwsgo_app_any(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id);
void uwsgo_app_ws(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id);

// Static GET: response is captured once at registration time and served by
// the C++ event loop directly without any cgo callback per request. Status,
// Content-Type, and body are copied into app-owned memory so the original Go
// strings can be reclaimed. Pass content_type_len=0 to skip the header.
void uwsgo_app_get_static(uwsgo_app_t *app, const char *pattern,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *body, size_t body_len);

int uwsgo_app_listen(uwsgo_app_t *app, int port);
void uwsgo_app_run(uwsgo_app_t *app);
void uwsgo_app_stop(uwsgo_app_t *app);

void uwsgo_res_write_status(uwsgo_res_t *res, const char *status, size_t status_len);
void uwsgo_res_write_header(uwsgo_res_t *res, const char *key, size_t key_len, const char *value, size_t value_len);
void uwsgo_res_write(uwsgo_res_t *res, const char *body, size_t body_len);
void uwsgo_res_end(uwsgo_res_t *res, const char *body, size_t body_len);
void uwsgo_res_send(
    uwsgo_res_t *res,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *body, size_t body_len);

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

// async_ctx_release drops Go's reference to ctx without sending a response.
// Call this if a goroutine returns without invoking defer_send.
void uwsgo_async_ctx_release(void *ctx);

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
    size_t ctx_status_len_offset;
    size_t ctx_ct_len_offset;
    size_t ctx_body_len_offset;
    size_t ctx_status_offset;
    size_t ctx_ct_offset;
    size_t ctx_body_offset;
    size_t ctx_handler_id_offset;
    size_t ctx_response_offset;
    size_t ctx_loop_offset;
    size_t ctx_inline_status_cap;
    size_t ctx_inline_ct_cap;
    size_t ctx_inline_body_cap;
    // Request snapshot offsets/caps (see SNAP_* constants in the cpp file).
    size_t ctx_method_len_offset;
    size_t ctx_url_len_offset;
    size_t ctx_query_len_offset;
    size_t ctx_param_count_offset;
    size_t ctx_headers_len_offset;
    size_t ctx_param_lens_offset;
    size_t ctx_method_offset;
    size_t ctx_url_offset;
    size_t ctx_query_offset;
    size_t ctx_params_offset;
    size_t ctx_headers_offset;
    size_t ctx_snap_method_cap;
    size_t ctx_snap_url_cap;
    size_t ctx_snap_query_cap;
    size_t ctx_snap_param_cap;
    size_t ctx_snap_param_max;
    size_t ctx_snap_headers_cap;
} uwsgo_shared_layout_t;

void uwsgo_shared_layout(uwsgo_shared_layout_t *out);

// Installs a periodic timer on the current loop that drains the shared ring
// at interval_us microseconds. libuS timers are millisecond-granularity, so
// any value below 1000 us is clamped to 1 ms. Used as a safety net; the
// per-response wake (uwsgo_wake_drain) keeps latency sub-ms in practice.
void uwsgo_app_start_drain(int interval_us);

// Wakes the loop and runs one drain pass — call from Go after pushing onto
// the response ring so the loop flushes immediately instead of waiting for
// the periodic timer to fire. Thread-safe via uWS::Loop::defer.
void uwsgo_wake_drain(uwsgo_loop_t *loop);

// Registers a route whose dispatch path bypasses cgo entirely: C++ pushes
// the AsyncCtx onto the request ring; Go worker goroutines drain the ring
// and invoke the handler bound to handler_id. The Go side keeps the
// handler_id -> handler map.
void uwsgo_app_get_shared(uwsgo_app_t *app, const char *pattern, uint32_t handler_id);

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

#ifdef __cplusplus
}
#endif
