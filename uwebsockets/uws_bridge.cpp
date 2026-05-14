//go:build cgo && uwebsockets

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
extern "C" void uwsgoHandleHTTPAsync(uintptr_t handler_id, uwsgo_res_t *res, void *ctx, uwsgo_loop_t *loop);
extern "C" void uwsgoHandleWSOpen(uintptr_t handler_id, uwsgo_ws_t *ws);
extern "C" void uwsgoHandleWSMessage(uintptr_t handler_id, uwsgo_ws_t *ws, const char *message, size_t message_len, int opcode);
extern "C" void uwsgoHandleWSClose(uintptr_t handler_id, uwsgo_ws_t *ws, int code, const char *message, size_t message_len);
extern "C" void uwsgoHandleDefer(uintptr_t callback_id);
extern "C" void uwsgoHandleAborted(uintptr_t callback_id);
extern "C" void uwsgoHandleCork(uintptr_t callback_id);
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
    us_listen_socket_t *listen_socket = nullptr;
    std::vector<std::unique_ptr<StaticResponse>> static_responses;
};

static size_t copy_string_view(std::string_view value, char *buffer, size_t buffer_len) {
    if (buffer != nullptr && buffer_len > 0) {
        std::memcpy(buffer, value.data(), std::min(buffer_len, value.size()));
    }

    return value.size();
}

extern "C" uwsgo_app_t *uwsgo_app_new(void) {
    return new uwsgo_app_t{std::make_unique<uWS::App>()};
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

namespace {

// Maximum inline body bytes for shared-memory responses. Sized to cover
// most JSON API responses; larger responses fall back to the cgo defer path.
constexpr size_t INLINE_STATUS_CAP = 32;
constexpr size_t INLINE_CT_CAP = 64;
constexpr size_t INLINE_BODY_CAP = 8192;

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
    uint32_t handler_id = 0;  // Used by shared-dispatch path to pick which Go handler runs

    // Inline response slots populated by Go via shared-memory writes.
    uint32_t inline_status_len = 0;
    uint32_t inline_ct_len = 0;
    uint32_t inline_body_len = 0;
    char inline_status[INLINE_STATUS_CAP];
    char inline_content_type[INLINE_CT_CAP];
    char inline_body[INLINE_BODY_CAP];

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
    auto *ctx = new AsyncCtx;
    ctx->response = r;

    r->onAborted([hold = CtxHold(ctx)]() {
        hold.ctx->aborted.store(1, std::memory_order_release);
    });

    *out_ctx = ctx;
    return reinterpret_cast<uwsgo_loop_t *>(uWS::Loop::get());
}

extern "C" void uwsgo_app_get_async(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    app->app->get(pattern, [handler_id](auto *res, auto *req) {
        (void)req;  // Async handlers don't receive the request; it would be
                    // invalid by the time the goroutine reads it.
        auto *ctx = new AsyncCtx;
        ctx->response = res;
        res->onAborted([hold = CtxHold(ctx)]() {
            hold.ctx->aborted.store(1, std::memory_order_release);
        });
        uwsgoHandleHTTPAsync(handler_id,
                             reinterpret_cast<uwsgo_res_t *>(res),
                             ctx,
                             reinterpret_cast<uwsgo_loop_t *>(uWS::Loop::get()));
    });
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
    out->ctx_inline_status_cap = INLINE_STATUS_CAP;
    out->ctx_inline_ct_cap = INLINE_CT_CAP;
    out->ctx_inline_body_cap = INLINE_BODY_CAP;
}

// uwsgo_app_get_shared registers a route whose dispatch path skips the
// Go-side cgo callback entirely. When a request matches, C++ builds an
// AsyncCtx with handler_id stamped on it and pushes it onto the shared
// request ring. Go worker goroutines (started by the binding at startup)
// drain the ring with plain atomic ops and run the handler.
extern "C" void uwsgo_app_get_shared(uwsgo_app_t *app, const char *pattern, uint32_t handler_id) {
    app->app->get(pattern, [handler_id](auto *res, auto *req) {
        (void)req;  // Shared-dispatch handlers can't read the request (uWS
                    // recycles it after this lambda returns, before Go reads).
        auto *ctx = new AsyncCtx;
        ctx->response = res;
        ctx->handler_id = handler_id;
        res->onAborted([hold = CtxHold(ctx)]() {
            hold.ctx->aborted.store(1, std::memory_order_release);
        });

        // MPMC push onto request ring. Producer = uWS loop thread (single),
        // consumers = Go worker goroutines (multiple).
        uint64_t idx = g_request.tail.fetch_add(1, std::memory_order_relaxed);
        PendingSlot *slot = &g_request.slots[idx & RING_MASK];
        while (slot->sequence.load(std::memory_order_acquire) != idx) {
            // Ring full; spin briefly — shouldn't happen at normal load.
        }
        slot->ctx = ctx;
        slot->sequence.store(idx + 1, std::memory_order_release);
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
// trades wake latency vs CPU. Returns nothing; the timer lives for the
// lifetime of the loop.
extern "C" void uwsgo_app_start_drain(int interval_us) {
    auto *loop = reinterpret_cast<struct us_loop_t *>(uWS::Loop::get());
    auto *timer = us_create_timer(loop, 0, 0);
    int ms = interval_us / 1000;
    if (ms < 1) ms = 1;
    us_timer_set(timer, [](struct us_timer_t * /*t*/) {
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

extern "C" int uwsgo_ws_send(uwsgo_ws_t *ws, const char *message, size_t message_len, int opcode) {
    bool ok = reinterpret_cast<GoWebSocket *>(ws)->send(std::string_view(message, message_len), static_cast<uWS::OpCode>(opcode));
    return ok ? 1 : 0;
}

extern "C" void uwsgo_ws_end(uwsgo_ws_t *ws, int code, const char *message, size_t message_len) {
    reinterpret_cast<GoWebSocket *>(ws)->end(code, std::string_view(message, message_len));
}
