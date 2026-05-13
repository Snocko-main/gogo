//go:build cgo && uwebsockets

#include "uws_bridge.h"

#include <App.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

extern "C" void uwsgoHandleHTTP(uintptr_t handler_id, uwsgo_res_t *res, uwsgo_req_t *req);
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

struct uwsgo_app_t {
    std::unique_ptr<uWS::App> app;
    us_listen_socket_t *listen_socket = nullptr;
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

// AsyncCtx is a reference-counted handle that tracks an in-flight async
// response. Refs are held by:
//   1. Go-side state, until uwsgo_res_defer_send or uwsgo_async_ctx_release transfers it
//   2. The onAborted lambda registered with uWS, until the response is destroyed
//   3. The defer lambda (if a response is sent), until that lambda runs
// When the last ref drops, the ctx is deleted.
struct AsyncCtx {
    std::atomic<int> refcount{1};
    std::atomic<int32_t> aborted{0};
    uWS::HttpResponse<false> *response;

    void retain() { refcount.fetch_add(1, std::memory_order_relaxed); }
    void release() {
        if (refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete this;
        }
    }
};

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

extern "C" void uwsgo_async_ctx_release(void *ctx_handle) {
    auto *ctx = static_cast<AsyncCtx *>(ctx_handle);
    ctx->release();
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
