//go:build cgo && gogo

#include "uws_bridge.h"

#include <App.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

extern "C" void uwsgoHandleHTTP(uintptr_t handler_id, uwsgo_res_t *res, uwsgo_req_t *req);
extern "C" void uwsgoHandleWSOpen(uintptr_t handler_id, uwsgo_ws_t *ws);
extern "C" void uwsgoHandleWSMessage(uintptr_t handler_id, uwsgo_ws_t *ws, const char *message, size_t message_len, int opcode);
extern "C" void uwsgoHandleWSClose(uintptr_t handler_id, uwsgo_ws_t *ws, int code, const char *message, size_t message_len);
extern "C" void uwsgoHandleDefer(uintptr_t callback_id);
extern "C" void uwsgoHandleAborted(uintptr_t callback_id);
extern "C" void uwsgoHandleCork(uintptr_t callback_id);
extern "C" void uwsgoHandleData(uintptr_t callback_id, const char *data, size_t len, int is_last);
extern "C" void uwsgoReleaseHandle(uintptr_t callback_id);

namespace {

// GoHandle owns a cgo handle: it releases the Go-side handle when destroyed
// unless it was consumed (id cleared). Used to make sure handles registered for
// callbacks that may never fire (e.g. onAborted on a non-aborted response) are
// still released when the owning C++ lambda dies.
struct GoHandle {
    uintptr_t id;

    explicit GoHandle(uintptr_t i) : id(i) {}
    GoHandle(GoHandle &&other) noexcept : id(other.id) { other.id = 0; }
    GoHandle(const GoHandle &) = delete;
    GoHandle &operator=(GoHandle &&) = delete;
    GoHandle &operator=(const GoHandle &) = delete;

    ~GoHandle() {
        if (id) {
            uwsgoReleaseHandle(id);
        }
    }

    uintptr_t consume() {
        uintptr_t taken = id;
        id = 0;
        return taken;
    }
};

}  // namespace

struct uwsgo_ws_data_t {};

using GoWebSocket = uWS::WebSocket<false, true, uwsgo_ws_data_t>;

// StaticResponse holds the captured bytes for a route that the C++ event loop
// can serve without crossing back into Go. Owned by the app for its lifetime.
struct StaticResponse {
    std::string status;
    std::string content_type;
    std::string body;
};

struct uwsgo_app_t {
    std::unique_ptr<uWS::App> app;
    uWS::Loop *loop = nullptr;
    us_listen_socket_t *listen_socket = nullptr;
    std::vector<std::unique_ptr<StaticResponse>> static_responses;
};

// Forward decl — definition lives near uwsgo_app_start_drain further below.
// uwsgo_app_stop needs to close it during shutdown.
static struct us_timer_t *g_drain_timer;

static size_t copy_string_view(std::string_view value, char *buffer, size_t buffer_len) {
    if (buffer != nullptr && buffer_len > 0) {
        std::memcpy(buffer, value.data(), std::min(buffer_len, value.size()));
    }

    return value.size();
}

extern "C" uwsgo_app_t *uwsgo_app_new(void) {
    auto *a = new uwsgo_app_t{std::make_unique<uWS::App>()};
    // Capture the loop pointer at app creation time. uWS::App() binds to the
    // current thread's loop; later teardown calls from any thread defer through
    // this captured loop rather than asking for the *caller's* loop.
    a->loop = uWS::Loop::get();
    return a;
}

extern "C" void uwsgo_app_free(uwsgo_app_t *app) {
    delete app;
}

extern "C" void uwsgo_app_get(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    app->app->get(pattern, [handler_id](auto *res, auto *req) {
        uwsgoHandleHTTP(handler_id, reinterpret_cast<uwsgo_res_t *>(res), reinterpret_cast<uwsgo_req_t *>(req));
    });
}

extern "C" void uwsgo_app_get_static(uwsgo_app_t *app, const char *pattern,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *body, size_t body_len) {
    auto stored = std::make_unique<StaticResponse>();
    stored->status.assign(status, status_len);
    stored->content_type.assign(content_type, content_type_len);
    stored->body.assign(body, body_len);
    auto *raw = stored.get();
    app->static_responses.push_back(std::move(stored));

    app->app->get(pattern, [raw](auto *res, auto *req) {
        (void)req;
        res->writeStatus(std::string_view(raw->status));
        if (!raw->content_type.empty()) {
            res->writeHeader(std::string_view("Content-Type", 12),
                             std::string_view(raw->content_type));
        }
        res->end(std::string_view(raw->body));
    });
}

extern "C" void uwsgo_app_post(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    app->app->post(pattern, [handler_id](auto *res, auto *req) {
        uwsgoHandleHTTP(handler_id, reinterpret_cast<uwsgo_res_t *>(res), reinterpret_cast<uwsgo_req_t *>(req));
    });
}

extern "C" void uwsgo_app_any(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    app->app->any(pattern, [handler_id](auto *res, auto *req) {
        uwsgoHandleHTTP(handler_id, reinterpret_cast<uwsgo_res_t *>(res), reinterpret_cast<uwsgo_req_t *>(req));
    });
}

extern "C" void uwsgo_app_ws(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    uWS::App::WebSocketBehavior<uwsgo_ws_data_t> behavior = {};

    behavior.open = [handler_id](auto *ws) {
        uwsgoHandleWSOpen(handler_id, reinterpret_cast<uwsgo_ws_t *>(ws));
    };

    behavior.message = [handler_id](auto *ws, std::string_view message, uWS::OpCode opcode) {
        uwsgoHandleWSMessage(
            handler_id,
            reinterpret_cast<uwsgo_ws_t *>(ws),
            message.data(),
            message.size(),
            static_cast<int>(opcode));
    };

    behavior.close = [handler_id](auto *ws, int code, std::string_view message) {
        uwsgoHandleWSClose(
            handler_id,
            reinterpret_cast<uwsgo_ws_t *>(ws),
            code,
            message.data(),
            message.size());
    };

    app->app->ws<uwsgo_ws_data_t>(pattern, std::move(behavior));
}

extern "C" int uwsgo_app_listen(uwsgo_app_t *app, int port) {
    bool ok = false;

    app->app->listen(port, [&ok, app](auto *listen_socket) {
        app->listen_socket = listen_socket;
        ok = listen_socket != nullptr;
    });

    return ok ? 1 : 0;
}

extern "C" void uwsgo_app_run(uwsgo_app_t *app) {
    app->app->run();
}

// Closes the App (which closes the listen socket and all active connection
// sockets via uWS::App::close), then closes the drain timer so the only
// remaining loop refs drop and us_loop_run can return. The actual close calls
// are dispatched via Loop::defer because they mutate loop state and must run
// on the loop thread. Safe to call from any goroutine. Idempotent.
extern "C" void uwsgo_app_stop(uwsgo_app_t *app) {
    if (app->loop == nullptr) {
        return;
    }
    app->loop->defer([app]() {
        if (app->app != nullptr) {
            app->app->close();
            app->listen_socket = nullptr;
        }
        if (g_drain_timer != nullptr) {
            us_timer_close(g_drain_timer);
            g_drain_timer = nullptr;
        }
    });
}

extern "C" void uwsgo_res_write_status(uwsgo_res_t *res, const char *status, size_t status_len) {
    reinterpret_cast<uWS::HttpResponse<false> *>(res)->writeStatus(std::string_view(status, status_len));
}

extern "C" void uwsgo_res_write_header(uwsgo_res_t *res, const char *key, size_t key_len, const char *value, size_t value_len) {
    reinterpret_cast<uWS::HttpResponse<false> *>(res)->writeHeader(
        std::string_view(key, key_len),
        std::string_view(value, value_len));
}

extern "C" void uwsgo_res_write(uwsgo_res_t *res, const char *body, size_t body_len) {
    reinterpret_cast<uWS::HttpResponse<false> *>(res)->write(std::string_view(body, body_len));
}

extern "C" void uwsgo_res_end(uwsgo_res_t *res, const char *body, size_t body_len) {
    reinterpret_cast<uWS::HttpResponse<false> *>(res)->end(std::string_view(body, body_len));
}

extern "C" void uwsgo_res_send(
    uwsgo_res_t *res,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *body, size_t body_len) {
    auto *r = reinterpret_cast<uWS::HttpResponse<false> *>(res);
    r->writeStatus(std::string_view(status, status_len));
    if (content_type_len > 0) {
        r->writeHeader(std::string_view("Content-Type", 12), std::string_view(content_type, content_type_len));
    }
    r->end(std::string_view(body, body_len));
}

extern "C" uwsgo_loop_t *uwsgo_res_get_loop(uwsgo_res_t * /*res*/) {
    // uWS loops are thread-local; calling from a route handler returns the
    // loop that runs the response.
    return reinterpret_cast<uwsgo_loop_t *>(uWS::Loop::get());
}

extern "C" void uwsgo_loop_defer(uwsgo_loop_t *loop, uintptr_t callback_id) {
    auto *l = reinterpret_cast<uWS::Loop *>(loop);
    GoHandle wrap{callback_id};
    l->defer([wrap = std::move(wrap)]() mutable {
        auto id = wrap.consume();
        if (id) {
            uwsgoHandleDefer(id);
        }
    });
}

extern "C" void uwsgo_res_on_aborted(uwsgo_res_t *res, uintptr_t callback_id) {
    auto *r = reinterpret_cast<uWS::HttpResponse<false> *>(res);
    GoHandle wrap{callback_id};
    r->onAborted([wrap = std::move(wrap)]() mutable {
        auto id = wrap.consume();
        if (id) {
            uwsgoHandleAborted(id);
        }
    });
}

extern "C" void uwsgo_res_cork(uwsgo_res_t *res, uintptr_t callback_id) {
    auto *r = reinterpret_cast<uWS::HttpResponse<false> *>(res);
    r->cork([callback_id]() {
        uwsgoHandleCork(callback_id);
    });
}

// uWS invokes the onData callback once per body chunk it receives. On the
// final chunk is_last == 1 — Go releases the handle after that call so the
// lambda's GoHandle wrapper must hand ownership off before then. We capture
// the handle in a shared_ptr so multi-chunk bodies don't accidentally release
// it mid-stream.
extern "C" void uwsgo_res_on_data(uwsgo_res_t *res, uintptr_t callback_id) {
    auto *r = reinterpret_cast<uWS::HttpResponse<false> *>(res);
    auto handle = std::make_shared<GoHandle>(callback_id);
    r->onData([handle](std::string_view chunk, bool is_last) {
        // uwsgoHandleData copies the chunk into Go memory before returning so
        // it's safe for uWS to reuse the buffer once the call completes.
        uwsgoHandleData(handle->id, chunk.data(), chunk.size(), is_last ? 1 : 0);
        if (is_last) {
            handle->consume(); // Go side released the handle.
        }
    });
}

namespace {

// Maximum inline body bytes for shared-memory responses. Sized to cover
// most JSON API responses; larger responses fall back to the cgo defer path.
constexpr size_t INLINE_STATUS_CAP = 32;
constexpr size_t INLINE_CT_CAP = 64;
constexpr size_t INLINE_BODY_CAP = 8192;

// Request-snapshot caps. uWS's HttpRequest becomes invalid the moment the
// C++ handler returns; for async handlers (which run on a goroutine later)
// we must copy the fields the user might want into the ctx up front.
constexpr size_t SNAP_METHOD_CAP = 8;
constexpr size_t SNAP_URL_CAP = 256;
constexpr size_t SNAP_QUERY_CAP = 512;
constexpr size_t SNAP_PARAM_CAP = 64;
constexpr size_t SNAP_PARAM_MAX = 8;
// Headers are encoded as "name\0value\0..." back-to-back so Go can parse on
// access without knowing the count up front. 4 KB fits the typical request.
constexpr size_t SNAP_HEADERS_CAP = 4096;

// AsyncCtx is a reference-counted handle that tracks an in-flight async
// response. Refs are held by:
//   1. Go-side state, until uwsgo_res_defer_send or uwsgo_async_ctx_release transfers it
//   2. The onAborted lambda registered with uWS, until the response is destroyed
//   3. The defer lambda (if a response is sent), until that lambda runs
// When the last ref drops, the ctx is deleted.
//
// The inline_* fields exist so Go can write the response directly into ctx
// memory (which lives in the C heap) and enqueue the ctx on the shared
// pending ring without any cgo call. The uWS loop drains the ring on every
// timer tick and serves the response from these buffers.
struct AsyncCtx {
    std::atomic<int> refcount{1};
    std::atomic<int32_t> aborted{0};
    uWS::HttpResponse<false> *response;
    uWS::Loop *loop = nullptr;  // The loop that owns this response (set at creation time)
    uint32_t handler_id = 0;  // Used by shared-dispatch path to pick which Go handler runs

    // Inline response slots populated by Go via shared-memory writes.
    uint32_t inline_status_len = 0;
    uint32_t inline_ct_len = 0;
    uint32_t inline_body_len = 0;
    char inline_status[INLINE_STATUS_CAP];
    char inline_content_type[INLINE_CT_CAP];
    char inline_body[INLINE_BODY_CAP];

    // Request snapshot. Captured in C++ before the route handler returns so
    // the async goroutine can read URL/query/params/headers after uWS has
    // freed the original HttpRequest.
    uint32_t method_len = 0;
    uint32_t url_len = 0;
    uint32_t query_len = 0;
    uint32_t param_count = 0;
    uint32_t headers_len = 0;
    uint32_t param_lens[SNAP_PARAM_MAX] = {0};
    char method[SNAP_METHOD_CAP];
    char url[SNAP_URL_CAP];
    char query[SNAP_QUERY_CAP];
    char params[SNAP_PARAM_MAX][SNAP_PARAM_CAP];
    char headers[SNAP_HEADERS_CAP];

    void retain() { refcount.fetch_add(1, std::memory_order_relaxed); }
    void release() {
        if (refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete this;
        }
    }
};

// Vyukov-style MPMC ring buffer used as MPSC: many goroutines push ready
// AsyncCtx pointers; the uWS loop is the sole consumer. Each slot carries a
// sequence number so producers and consumer don't trample each other without
// locks. POOL_SIZE must be a power of two.
constexpr uint64_t RING_SIZE = 4096;
constexpr uint64_t RING_MASK = RING_SIZE - 1;

struct PendingSlot {
    std::atomic<uint64_t> sequence;
    AsyncCtx *ctx;
};

struct PendingRing {
    PendingSlot slots[RING_SIZE];
    std::atomic<uint64_t> head;  // consumer index (loop thread only)
    std::atomic<uint64_t> tail;  // producer index (any thread)

    void init() {
        for (uint64_t i = 0; i < RING_SIZE; i++) {
            slots[i].sequence.store(i, std::memory_order_relaxed);
            slots[i].ctx = nullptr;
        }
        head.store(0, std::memory_order_relaxed);
        tail.store(0, std::memory_order_relaxed);
    }
};

// Single global ring shared between all uWS loops in the process. Aligned to a
// cache line to avoid false sharing between head and tail.
alignas(128) static PendingRing g_pending;

// RequestRing mirrors PendingRing but goes the other way: C++ enqueues incoming
// requests (as AsyncCtx*), Go worker goroutines dequeue and dispatch. Same
// Vyukov MPMC layout so both sides can read/write with plain atomics.
alignas(128) static PendingRing g_request;

// CtxHold is a smart-pointer-like wrapper that retains/releases AsyncCtx,
// suitable for capturing into uWS MoveOnlyFunction lambdas.
struct CtxHold {
    AsyncCtx *ctx;
    explicit CtxHold(AsyncCtx *c) : ctx(c) { c->retain(); }
    CtxHold(const CtxHold &o) : ctx(o.ctx) { ctx->retain(); }
    CtxHold(CtxHold &&o) noexcept : ctx(o.ctx) { o.ctx = nullptr; }
    CtxHold &operator=(const CtxHold &) = delete;
    CtxHold &operator=(CtxHold &&) = delete;
    ~CtxHold() { if (ctx) ctx->release(); }
};

// SendBuffer owns C-heap copies of status/content_type/body so the defer
// lambda can outlive the originating Go string.
struct SendBuffer {
    char *status = nullptr;
    size_t status_len = 0;
    char *content_type = nullptr;
    size_t content_type_len = 0;
    char *body = nullptr;
    size_t body_len = 0;

    SendBuffer() = default;
    SendBuffer(const SendBuffer &) = delete;
    SendBuffer &operator=(const SendBuffer &) = delete;
    SendBuffer(SendBuffer &&o) noexcept :
        status(o.status), status_len(o.status_len),
        content_type(o.content_type), content_type_len(o.content_type_len),
        body(o.body), body_len(o.body_len) {
        o.status = o.content_type = o.body = nullptr;
    }
    SendBuffer &operator=(SendBuffer &&) = delete;
    ~SendBuffer() {
        std::free(status);
        std::free(content_type);
        std::free(body);
    }
};

inline char *dup_to_c_heap(const char *src, size_t n) {
    if (n == 0) return nullptr;
    char *p = static_cast<char *>(std::malloc(n));
    if (p != nullptr) std::memcpy(p, src, n);
    return p;
}

}  // namespace

extern "C" uwsgo_loop_t *uwsgo_res_begin_async(uwsgo_res_t *res, void **out_ctx) {
    auto *r = reinterpret_cast<uWS::HttpResponse<false> *>(res);
    auto *loop = uWS::Loop::get();
    auto *ctx = new AsyncCtx;
    ctx->response = r;
    ctx->loop = loop;

    r->onAborted([hold = CtxHold(ctx)]() {
        hold.ctx->aborted.store(1, std::memory_order_release);
    });

    *out_ctx = ctx;
    return reinterpret_cast<uwsgo_loop_t *>(loop);
}

extern "C" void uwsgo_async_ctx_release(void *ctx_handle) {
    auto *ctx = static_cast<AsyncCtx *>(ctx_handle);
    ctx->release();
}

extern "C" void uwsgo_shared_layout(uwsgo_shared_layout_t *out) {
    g_pending.init();
    g_request.init();
    out->ring = &g_pending;
    out->request_ring = &g_request;
    out->ring_size = RING_SIZE;
    out->ring_mask = RING_MASK;
    out->ring_slots_offset = offsetof(PendingRing, slots);
    out->ring_slot_stride = sizeof(PendingSlot);
    out->ring_slot_seq_offset = offsetof(PendingSlot, sequence);
    out->ring_slot_ctx_offset = offsetof(PendingSlot, ctx);
    out->ring_head_offset = offsetof(PendingRing, head);
    out->ring_tail_offset = offsetof(PendingRing, tail);

    out->ctx_status_len_offset = offsetof(AsyncCtx, inline_status_len);
    out->ctx_ct_len_offset = offsetof(AsyncCtx, inline_ct_len);
    out->ctx_body_len_offset = offsetof(AsyncCtx, inline_body_len);
    out->ctx_status_offset = offsetof(AsyncCtx, inline_status);
    out->ctx_ct_offset = offsetof(AsyncCtx, inline_content_type);
    out->ctx_body_offset = offsetof(AsyncCtx, inline_body);
    out->ctx_handler_id_offset = offsetof(AsyncCtx, handler_id);
    out->ctx_response_offset = offsetof(AsyncCtx, response);
    out->ctx_loop_offset = offsetof(AsyncCtx, loop);
    out->ctx_inline_status_cap = INLINE_STATUS_CAP;
    out->ctx_inline_ct_cap = INLINE_CT_CAP;
    out->ctx_inline_body_cap = INLINE_BODY_CAP;

    out->ctx_method_len_offset = offsetof(AsyncCtx, method_len);
    out->ctx_url_len_offset = offsetof(AsyncCtx, url_len);
    out->ctx_query_len_offset = offsetof(AsyncCtx, query_len);
    out->ctx_param_count_offset = offsetof(AsyncCtx, param_count);
    out->ctx_headers_len_offset = offsetof(AsyncCtx, headers_len);
    out->ctx_param_lens_offset = offsetof(AsyncCtx, param_lens);
    out->ctx_method_offset = offsetof(AsyncCtx, method);
    out->ctx_url_offset = offsetof(AsyncCtx, url);
    out->ctx_query_offset = offsetof(AsyncCtx, query);
    out->ctx_params_offset = offsetof(AsyncCtx, params);
    out->ctx_headers_offset = offsetof(AsyncCtx, headers);
    out->ctx_snap_method_cap = SNAP_METHOD_CAP;
    out->ctx_snap_url_cap = SNAP_URL_CAP;
    out->ctx_snap_query_cap = SNAP_QUERY_CAP;
    out->ctx_snap_param_cap = SNAP_PARAM_CAP;
    out->ctx_snap_param_max = SNAP_PARAM_MAX;
    out->ctx_snap_headers_cap = SNAP_HEADERS_CAP;
}

// uwsgo_app_get_shared registers a route whose dispatch path skips the
// Go-side cgo callback entirely. When a request matches, C++ builds an
// AsyncCtx with handler_id stamped on it and pushes it onto the shared
// request ring. Go worker goroutines (started by the binding at startup)
// drain the ring with plain atomic ops and run the handler.
// snapshot_request copies the fields of the live uWS HttpRequest into the
// AsyncCtx so the async goroutine can read them after uWS frees the request.
// Anything that doesn't fit the fixed buffers is truncated.
static void snapshot_request(AsyncCtx *ctx, uWS::HttpRequest *req) {
    auto copy_view = [](char *dst, size_t cap, std::string_view src) -> uint32_t {
        size_t n = std::min(cap, src.size());
        if (n > 0) std::memcpy(dst, src.data(), n);
        return static_cast<uint32_t>(n);
    };

    ctx->method_len = copy_view(ctx->method, SNAP_METHOD_CAP, req->getMethod());
    // getUrl returns the path; getQuery returns query string sans '?'.
    ctx->url_len = copy_view(ctx->url, SNAP_URL_CAP, req->getUrl());
    ctx->query_len = copy_view(ctx->query, SNAP_QUERY_CAP, req->getQuery());

    // Route parameters: walk indices until uWS returns empty.
    uint32_t param_count = 0;
    for (uint32_t i = 0; i < SNAP_PARAM_MAX; i++) {
        auto v = req->getParameter(i);
        if (v.empty()) break;
        ctx->param_lens[i] = copy_view(ctx->params[i], SNAP_PARAM_CAP, v);
        param_count = i + 1;
    }
    ctx->param_count = param_count;

    // Headers: encode as "name\0value\0..." back-to-back. Stop when the next
    // pair won't fit so we don't half-write a value.
    uint32_t hpos = 0;
    for (auto it = req->begin(); it != req->end(); ++it) {
        auto kv = *it;
        std::string_view name = kv.first;
        std::string_view value = kv.second;
        size_t need = name.size() + 1 + value.size() + 1;
        if (hpos + need > SNAP_HEADERS_CAP) break;
        std::memcpy(ctx->headers + hpos, name.data(), name.size());
        hpos += name.size();
        ctx->headers[hpos++] = '\0';
        std::memcpy(ctx->headers + hpos, value.data(), value.size());
        hpos += value.size();
        ctx->headers[hpos++] = '\0';
    }
    ctx->headers_len = hpos;
}

extern "C" void uwsgo_app_get_shared(uwsgo_app_t *app, const char *pattern, uint32_t handler_id) {
    app->app->get(pattern, [handler_id](auto *res, auto *req) {
        // CAS-based bounded MPMC enqueue. If the ring is full (next slot's
        // sequence is behind our intended position), we reject the request
        // with 503 instead of spinning — that would block the loop thread.
        uint64_t tail = g_request.tail.load(std::memory_order_relaxed);
        for (int spin = 0;; ++spin) {
            PendingSlot *slot = &g_request.slots[tail & RING_MASK];
            uint64_t seq = slot->sequence.load(std::memory_order_acquire);
            int64_t diff = (int64_t)(seq - tail);
            if (diff == 0) {
                // Slot is empty for this tail; try to claim it.
                if (g_request.tail.compare_exchange_weak(
                        tail, tail + 1,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    auto *ctx = new AsyncCtx;
                    ctx->response = res;
                    ctx->loop = uWS::Loop::get();
                    ctx->handler_id = handler_id;
                    // Snapshot before any cgo / Go work — uWS HttpRequest is
                    // live only inside this lambda.
                    snapshot_request(ctx, req);
                    res->onAborted([hold = CtxHold(ctx)]() {
                        hold.ctx->aborted.store(1, std::memory_order_release);
                    });
                    slot->ctx = ctx;
                    slot->sequence.store(tail + 1, std::memory_order_release);
                    return;
                }
                // CAS lost; retry with new tail (already updated by CAS).
            } else if (diff < 0) {
                // Ring is full — consumer is RING_SIZE slots behind. Reject.
                res->writeStatus("503 Service Unavailable");
                res->writeHeader("Content-Type", "text/plain; charset=utf-8");
                res->end("Server overloaded\n");
                return;
            } else {
                // Another producer just claimed this slot; reload tail and retry.
                tail = g_request.tail.load(std::memory_order_relaxed);
            }
            // Defensive cap: huge contention shouldn't happen, but bail out
            // before the loop thread is locked indefinitely.
            if (spin > 100000) {
                res->writeStatus("503 Service Unavailable");
                res->writeHeader("Content-Type", "text/plain; charset=utf-8");
                res->end("Enqueue contention\n");
                return;
            }
        }
    });
}

// drain_pending runs on the loop thread (invoked from a periodic timer). It
// consumes every ready slot from the ring and sends the response. Single
// consumer so no CAS needed on head.
static void drain_pending() {
    uint64_t h = g_pending.head.load(std::memory_order_relaxed);
    while (true) {
        PendingSlot *slot = &g_pending.slots[h & RING_MASK];
        uint64_t seq = slot->sequence.load(std::memory_order_acquire);
        if (seq != h + 1) break;  // slot not ready

        AsyncCtx *ctx = slot->ctx;
        slot->sequence.store(h + RING_SIZE, std::memory_order_release);

        if (!ctx->aborted.load(std::memory_order_acquire)) {
            auto *r = ctx->response;
            r->cork([r, ctx]() {
                if (ctx->inline_status_len > 0) {
                    r->writeStatus(std::string_view(ctx->inline_status, ctx->inline_status_len));
                }
                if (ctx->inline_ct_len > 0) {
                    r->writeHeader(std::string_view("Content-Type", 12),
                                   std::string_view(ctx->inline_content_type, ctx->inline_ct_len));
                }
                r->end(std::string_view(ctx->inline_body, ctx->inline_body_len));
            });
        }
        ctx->release();
        h++;
    }
    g_pending.head.store(h, std::memory_order_relaxed);
}

// uwsgo_app_start_drain installs a periodic timer on the current loop that
// invokes drain_pending. interval_us is the polling interval; 50-200 us
// trades wake latency vs CPU.
// uwsgo_wake_drain schedules a single drain run on the loop thread. Callable
// from any goroutine via Go: after pushing onto the response ring, this
// wakes the loop immediately instead of waiting up to ~1 ms for the
// periodic drain timer to fire. Costs one cgo crossing per response in
// exchange for sub-millisecond response latency.
extern "C" void uwsgo_wake_drain(uwsgo_loop_t *loop) {
    auto *l = reinterpret_cast<uWS::Loop *>(loop);
    l->defer([]() { drain_pending(); });
}

extern "C" void uwsgo_app_start_drain(int interval_us) {
    auto *loop = reinterpret_cast<struct us_loop_t *>(uWS::Loop::get());
    g_drain_timer = us_create_timer(loop, 0, 0);
    int ms = interval_us / 1000;
    if (ms < 1) ms = 1;
    us_timer_set(g_drain_timer, [](struct us_timer_t * /*t*/) {
        drain_pending();
    }, ms, ms);
}

extern "C" void uwsgo_res_defer_send(
    uwsgo_loop_t *loop,
    void *ctx_handle,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *body, size_t body_len) {
    auto *l = reinterpret_cast<uWS::Loop *>(loop);
    auto *ctx = static_cast<AsyncCtx *>(ctx_handle);

    SendBuffer buf;
    buf.status = dup_to_c_heap(status, status_len);
    buf.status_len = status_len;
    buf.content_type = dup_to_c_heap(content_type, content_type_len);
    buf.content_type_len = content_type_len;
    buf.body = dup_to_c_heap(body, body_len);
    buf.body_len = body_len;

    // The defer lambda takes ownership of Go's ctx ref (no extra retain).
    l->defer([ctx, sb = std::move(buf)]() mutable {
        if (ctx->aborted.load(std::memory_order_acquire)) {
            ctx->release();
            return;
        }
        auto *r = ctx->response;
        r->cork([r, &sb]() {
            r->writeStatus(std::string_view(sb.status, sb.status_len));
            if (sb.content_type_len > 0) {
                r->writeHeader(std::string_view("Content-Type", 12),
                               std::string_view(sb.content_type, sb.content_type_len));
            }
            r->end(std::string_view(sb.body, sb.body_len));
        });
        ctx->release();
    });
}

extern "C" size_t uwsgo_req_method(uwsgo_req_t *req, char *buffer, size_t buffer_len) {
    auto value = reinterpret_cast<uWS::HttpRequest *>(req)->getMethod();
    return copy_string_view(value, buffer, buffer_len);
}

extern "C" size_t uwsgo_req_url(uwsgo_req_t *req, char *buffer, size_t buffer_len) {
    auto value = reinterpret_cast<uWS::HttpRequest *>(req)->getUrl();
    return copy_string_view(value, buffer, buffer_len);
}

extern "C" size_t uwsgo_req_header(uwsgo_req_t *req, const char *name, size_t name_len, char *buffer, size_t buffer_len) {
    auto value = reinterpret_cast<uWS::HttpRequest *>(req)->getHeader(std::string_view(name, name_len));
    return copy_string_view(value, buffer, buffer_len);
}

extern "C" size_t uwsgo_req_parameter(uwsgo_req_t *req, unsigned long index, char *buffer, size_t buffer_len) {
    auto value = reinterpret_cast<uWS::HttpRequest *>(req)->getParameter(index);
    return copy_string_view(value, buffer, buffer_len);
}

extern "C" size_t uwsgo_req_query(uwsgo_req_t *req, char *buffer, size_t buffer_len) {
    auto value = reinterpret_cast<uWS::HttpRequest *>(req)->getQuery();
    return copy_string_view(value, buffer, buffer_len);
}

extern "C" size_t uwsgo_req_query_param(uwsgo_req_t *req, const char *name, size_t name_len, char *buffer, size_t buffer_len) {
    auto value = reinterpret_cast<uWS::HttpRequest *>(req)->getQuery(std::string_view(name, name_len));
    return copy_string_view(value, buffer, buffer_len);
}

extern "C" size_t uwsgo_req_headers_all(uwsgo_req_t *req_ptr, char *buffer, size_t buffer_len) {
    auto *req = reinterpret_cast<uWS::HttpRequest *>(req_ptr);
    // Two-pass: first compute the total size, then write if there's room.
    size_t total = 0;
    for (auto it = req->begin(); it != req->end(); ++it) {
        auto kv = *it;
        total += kv.first.size() + 1 + kv.second.size() + 1;
    }
    if (buffer == nullptr || buffer_len < total) {
        return total;
    }
    size_t pos = 0;
    for (auto it = req->begin(); it != req->end(); ++it) {
        auto kv = *it;
        std::string_view name = kv.first;
        std::string_view value = kv.second;
        std::memcpy(buffer + pos, name.data(), name.size());
        pos += name.size();
        buffer[pos++] = '\0';
        std::memcpy(buffer + pos, value.data(), value.size());
        pos += value.size();
        buffer[pos++] = '\0';
    }
    return pos;
}

extern "C" int uwsgo_ws_send(uwsgo_ws_t *ws, const char *message, size_t message_len, int opcode) {
    bool ok = reinterpret_cast<GoWebSocket *>(ws)->send(std::string_view(message, message_len), static_cast<uWS::OpCode>(opcode));
    return ok ? 1 : 0;
}

extern "C" void uwsgo_ws_end(uwsgo_ws_t *ws, int code, const char *message, size_t message_len) {
    reinterpret_cast<GoWebSocket *>(ws)->end(code, std::string_view(message, message_len));
}
