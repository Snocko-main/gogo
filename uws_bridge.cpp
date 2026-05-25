//go:build cgo && gogo

#include "uws_bridge.h"

#include <App.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

extern "C" void uwsgoHandleHTTP(uintptr_t handler_id, uwsgo_res_t *res, uwsgo_req_t *req,
    const char *method, size_t method_len,
    const char *url, size_t url_len,
    const char *query, size_t query_len,
    const char *headers_blob, size_t headers_len, int headers_complete,
    const char *p0, size_t p0_len,
    const char *p1, size_t p1_len,
    const char *p2, size_t p2_len,
    const char *p3, size_t p3_len);
extern "C" void uwsgoHandleWSOpen(uintptr_t handler_id, uwsgo_ws_t *ws);
extern "C" void uwsgoHandleWSMessage(uintptr_t handler_id, uwsgo_ws_t *ws, const char *message, size_t message_len, int opcode);
extern "C" void uwsgoHandleWSClose(uintptr_t handler_id, uwsgo_ws_t *ws, int code, const char *message, size_t message_len);
extern "C" void uwsgoHandleWSUpgrade(
    uintptr_t handler_id,
    void *ctx_ptr,
    const char *method, size_t method_len,
    const char *url, size_t url_len,
    const char *query, size_t query_len,
    const char *ip, size_t ip_len,
    const char *headers_blob, size_t headers_len,
    const char *sec_protocol_offered, size_t sec_protocol_len);
extern "C" void uwsgoHandleDefer(uintptr_t callback_id);
extern "C" void uwsgoHandleAborted(uintptr_t callback_id);
extern "C" void uwsgoHandleCork(uintptr_t callback_id);
extern "C" void uwsgoHandleDrain(uintptr_t callback_id);
extern "C" void uwsgoHandleData(uintptr_t callback_id, const char *data, size_t len, int is_last);
extern "C" void uwsgoReleaseHandle(uintptr_t callback_id);
extern "C" void us_internal_free_closed_sockets(us_loop_t *loop);

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

struct uwsgo_ws_data_t {
    // go_user_data holds a Go cgo.Handle (cast to uintptr) that the
    // Go-side upgrade callback attached to this WebSocket. Read by
    // ws.UserData(), released by the Go close handler when the
    // socket goes away.
    uintptr_t go_user_data = 0;
};

using GoWebSocket = uWS::WebSocket<false, true, uwsgo_ws_data_t>;

// UpgradeCtx is shared between the C++ upgrade lambda and the Go
// callback. Lifetime is the duration of the lambda invocation —
// pointers must NOT outlive the synchronous call into Go.
struct UpgradeCtx {
    uWS::HttpResponse<false> *res;
    struct us_socket_context_t *context;
    std::string sec_key;
    std::string sec_protocol_offered;
    std::string sec_extensions;
    int done;  // 0 = pending, 1 = accept/reject already invoked
};

// UpgradeSnapshot holds the request-side strings the Go callback
// reads via UpgradeContext methods. Built in the C++ lambda before
// crossing into Go so the underlying HttpRequest is free to be
// freed when the lambda returns.
struct UpgradeSnapshot {
    std::string method;
    std::string url;
    std::string query;
    std::string ip;
    std::string headers_blob;  // packed name\0value\0...
};

// StaticResponse holds the captured bytes for a route that the C++ event loop
// can serve without crossing back into Go. Owned by the app for its lifetime.
struct StaticResponse {
    std::string status;
    std::string content_type;
    std::string body;
};

// Shared-dispatch ring types — defined up front so uwsgo_app_t can hold a
// per-App PendingRing pointer without forward-decl gymnastics. AsyncCtx is
// fully defined later (it has lots of fields); we forward-declare it here
// so PendingSlot can reference it. Each App allocates one PendingRing on
// the heap in uwsgo_app_new.
struct AsyncCtx;

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
    // wake_pending is 0 when no Go producer has yet called wake_drain
    // since the last loop-thread drain pass began. Producers CAS(0,1)
    // to claim the right to call wake_drain — the CAS loser knows the
    // drain is already scheduled and skips the cgo crossing. The drain
    // handler clears this back to 0 the moment it begins, so any newly
    // arrived ctx after that point will get a fresh wake.
    std::atomic<uint32_t> wake_pending;

    void init() {
        for (uint64_t i = 0; i < RING_SIZE; i++) {
            slots[i].sequence.store(i, std::memory_order_relaxed);
            slots[i].ctx = nullptr;
        }
        head.store(0, std::memory_order_relaxed);
        tail.store(0, std::memory_order_relaxed);
        wake_pending.store(0, std::memory_order_relaxed);
    }
};

// AsyncCtx pool sizing: enough to absorb realistic burst-concurrency
// recycle traffic. 256 ctxs at ~13 KiB each = ~3.3 MiB per App, well
// under the per-App request_ring + pending_ring storage. Above this
// cap producers fall back to delete (and consumers fall back to new),
// so the pool degrades gracefully under sustained over-allocation
// rather than blocking.
constexpr uint64_t CTX_POOL_SIZE = 256;
constexpr uint64_t CTX_POOL_MASK = CTX_POOL_SIZE - 1;

struct CtxPoolSlot {
    std::atomic<uint64_t> sequence;
    AsyncCtx *ctx;
};

// CtxPool is a per-App MPMC bounded queue of recycled AsyncCtx pointers.
// Producers: multiple — loop-thread drain (drain_pending → release)
// and worker goroutines (asyncCtxRelease) both push when the last ref
// drops. Consumer: loop thread only (uwsgo_app_get_shared lambda pops
// one ctx per incoming request before falling back to new AsyncCtx).
// Uses Vyukov-style sequenced slots, identical machinery to PendingRing.
struct CtxPool {
    CtxPoolSlot slots[CTX_POOL_SIZE];
    alignas(64) std::atomic<uint64_t> head;
    alignas(64) std::atomic<uint64_t> tail;

    void init() {
        for (uint64_t i = 0; i < CTX_POOL_SIZE; i++) {
            slots[i].sequence.store(i, std::memory_order_relaxed);
            slots[i].ctx = nullptr;
        }
        head.store(0, std::memory_order_relaxed);
        tail.store(0, std::memory_order_relaxed);
    }

    // push attempts to enqueue ctx for reuse. Returns false if the pool
    // is full — caller should delete instead.
    bool push(AsyncCtx *ctx) {
        uint64_t pos = tail.load(std::memory_order_relaxed);
        for (;;) {
            CtxPoolSlot *slot = &slots[pos & CTX_POOL_MASK];
            uint64_t seq = slot->sequence.load(std::memory_order_acquire);
            int64_t diff = (int64_t)(seq - pos);
            if (diff == 0) {
                if (tail.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                    slot->ctx = ctx;
                    slot->sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
            } else if (diff < 0) {
                return false;  // pool full — caller will delete
            } else {
                pos = tail.load(std::memory_order_relaxed);
            }
        }
    }

    // pop returns a recycled ctx or nullptr if the pool is empty.
    // Single-consumer (loop thread) — no CAS on head needed.
    AsyncCtx *pop() {
        uint64_t pos = head.load(std::memory_order_relaxed);
        CtxPoolSlot *slot = &slots[pos & CTX_POOL_MASK];
        uint64_t seq = slot->sequence.load(std::memory_order_acquire);
        int64_t diff = (int64_t)(seq - (pos + 1));
        if (diff != 0) {
            return nullptr;  // empty
        }
        AsyncCtx *ctx = slot->ctx;
        slot->ctx = nullptr;
        slot->sequence.store(pos + CTX_POOL_SIZE, std::memory_order_release);
        head.store(pos + 1, std::memory_order_relaxed);
        return ctx;
    }
};

struct uwsgo_app_t {
    std::unique_ptr<uWS::App> app;
    uWS::Loop *loop = nullptr;
    us_listen_socket_t *listen_socket = nullptr;
    std::vector<std::unique_ptr<StaticResponse>> static_responses;
    std::mutex app_mu;
    std::atomic<bool> accepting_work = true;
    bool has_websocket = false;

    // Per-App response ring + drain timer. Each native App owns its own
    // pending ring so SendShared can safely write from a worker thread and
    // the drain timer on this App's loop can cork+send without crossing
    // threads. Allocated on the heap so multiple App instances within the
    // same process don't share state and don't contend on a global ring.
    PendingRing *pending_ring = nullptr;
    struct us_timer_t *drain_timer = nullptr;

    // ctx_pool recycles AsyncCtx blocks so the shared-dispatch hot path
    // doesn't pay a fresh ~13 KiB allocation per request. See CtxPool
    // for the producer/consumer threading model.
    CtxPool *ctx_pool = nullptr;

    // body_limit is enforced for Post / Any routes by checking the
    // Content-Length header at request arrival before dispatching to Go.
    // 0 disables the check.
    size_t body_limit = 0;

    // capture_peer_ip toggles whether snapshot_request copies the
    // formatted peer IP into the AsyncCtx ip[] buffer. Off by default
    // — Go's Config.CapturePeerIP flips it. Skipping the copy saves
    // a string_view format + ~50-byte memcpy per shared-dispatch
    // request, which measures at ~2-3% on small-response routes.
    bool capture_peer_ip = false;
};

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
    // Allocate this App's response ring up front so multiple App instances
    // in the same process don't share state and don't contend on a global.
    a->pending_ring = new PendingRing;
    a->pending_ring->init();
    // Allocate the per-App AsyncCtx recycle pool. Empty at start; fills
    // as requests complete and ctx::release pushes back into it.
    a->ctx_pool = new CtxPool;
    a->ctx_pool->init();
    return a;
}

// uwsgo_app_free is defined below the AsyncCtx struct (the pool drain
// needs the complete type to delete recycled ctxs).
extern "C" void uwsgo_app_free(uwsgo_app_t *app);

// HEADERS_SCRATCH_SIZE caps how many request-header bytes the
// dispatcher copies into the stack-allocated blob before falling
// back to the cgo lookup path. 8 KB covers the realistic upper
// bound for a browser request (≈ 20 headers × ≈ 400 bytes
// each); requests that overflow simply lose the pre-pack benefit
// on the trailing headers — Request.Header still works via the
// cgo helper for any name that wasn't packed.
static constexpr size_t HEADERS_SCRATCH_SIZE = 8 * 1024;

// dispatch_sync invokes uwsgoHandleHTTP with method / URL / query / the
// first four route parameters already pulled out of the uWS request,
// plus a single packed `name\0value\0name\0value\0…` headers blob
// built in a stack scratch buffer. uWS keeps method / url / query /
// params / per-header views as std::string_view pointers into its own
// request buffer; the buffer is alive for the duration of the C++
// callback, which is exactly the lifetime of the Go Request wrapper,
// so passing the raw (data, len) pairs to Go is safe and lets
// Request's accessors materialize lazily without a cgo round-trip
// back into uWS.
//
// Four params covers the realistic ceiling — uWS itself supports more,
// but routes with more than four named params are extremely rare. Reads
// past index 3 fall through to the cgo getParameter helper.
//
// The packed headers blob is a single contiguous buffer the Go side
// can scan for any header by name without going back across cgo — the
// dominant per-request cost for middleware that reads Origin / Cookie
// / Authorization / User-Agent in series.
static inline void dispatch_sync(uintptr_t handler_id, uWS::HttpResponse<false> *res, uWS::HttpRequest *req) {
    auto method = req->getMethod();
    auto url = req->getUrl();
    auto query = req->getQuery();
    auto p0 = req->getParameter(0);
    auto p1 = req->getParameter(1);
    auto p2 = req->getParameter(2);
    auto p3 = req->getParameter(3);

    // Pack headers into a per-call stack scratch buffer. Walks uWS's
    // already-parsed pair list; stops cleanly on the first header
    // that won't fit, leaving the partial blob valid (every entry
    // ends with a NUL pair).
    char headers_buf[HEADERS_SCRATCH_SIZE];
    size_t headers_len = 0;
    int headers_complete = 1;
    for (auto it = req->begin(); it != req->end(); ++it) {
        auto pair = *it;
        auto name = pair.first;
        auto value = pair.second;
        size_t need = name.size() + 1 + value.size() + 1;
        if (headers_len + need > sizeof(headers_buf)) {
            // Don't truncate mid-pair — leave the buffer at the last
            // complete entry so the Go-side scanner never sees a
            // dangling key with no terminator.
            headers_complete = 0;
            break;
        }
        std::memcpy(headers_buf + headers_len, name.data(), name.size());
        headers_len += name.size();
        headers_buf[headers_len++] = 0;
        std::memcpy(headers_buf + headers_len, value.data(), value.size());
        headers_len += value.size();
        headers_buf[headers_len++] = 0;
    }

    uwsgoHandleHTTP(handler_id,
        reinterpret_cast<uwsgo_res_t *>(res),
        reinterpret_cast<uwsgo_req_t *>(req),
        method.data(), method.size(),
        url.data(), url.size(),
        query.data(), query.size(),
        headers_buf, headers_len, headers_complete,
        p0.data(), p0.size(),
        p1.data(), p1.size(),
        p2.data(), p2.size(),
        p3.data(), p3.size());
}

extern "C" void uwsgo_app_get(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    app->app->get(pattern, [handler_id](auto *res, auto *req) {
        dispatch_sync(handler_id, res, req);
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

// body_limit_rejects checks the declared Content-Length against the app's
// body_limit and writes a 413 directly without dispatching to Go if it
// exceeds. Returns true when the request was rejected.
//
// Coverage:
//   - Content-Length present: enforced here at request arrival,
//     before any cgo crossing. Cheapest gate.
//   - Content-Length absent (chunked transfer-encoded): handled on
//     the Go side. Response.OnData and Response.Body both
//     accumulate chunk sizes against the same body_limit and emit
//     413 + close as soon as the total exceeds the cap. So bypass
//     here is not a bypass overall — just a different layer.
static bool body_limit_rejects(uwsgo_app_t *app, uWS::HttpResponse<false> *res, uWS::HttpRequest *req) {
    if (app->body_limit == 0) return false;
    auto cl = req->getHeader("content-length");
    if (cl.empty()) return false;
    // strtoull-style parse; if the header is malformed we conservatively
    // reject too — clients that send junk Content-Length deserve a 400/413.
    unsigned long long n = 0;
    for (char c : cl) {
        if (c < '0' || c > '9') { n = ~0ULL; break; }
        n = n * 10 + static_cast<unsigned long long>(c - '0');
        if (n > app->body_limit) break;
    }
    if (n <= app->body_limit) return false;
    res->writeStatus("413 Payload Too Large");
    res->writeHeader("Content-Type", "text/plain; charset=utf-8");
    res->end("payload too large\n");
    return true;
}

extern "C" void uwsgo_app_post(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    app->app->post(pattern, [app, handler_id](auto *res, auto *req) {
        if (body_limit_rejects(app, res, req)) return;
        dispatch_sync(handler_id, res, req);
    });
}

extern "C" void uwsgo_app_any(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    app->app->any(pattern, [app, handler_id](auto *res, auto *req) {
        if (body_limit_rejects(app, res, req)) return;
        dispatch_sync(handler_id, res, req);
    });
}

extern "C" void uwsgo_app_put(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    app->app->put(pattern, [app, handler_id](auto *res, auto *req) {
        if (body_limit_rejects(app, res, req)) return;
        dispatch_sync(handler_id, res, req);
    });
}

extern "C" void uwsgo_app_patch(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    app->app->patch(pattern, [app, handler_id](auto *res, auto *req) {
        if (body_limit_rejects(app, res, req)) return;
        dispatch_sync(handler_id, res, req);
    });
}

extern "C" void uwsgo_app_delete(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    app->app->del(pattern, [app, handler_id](auto *res, auto *req) {
        if (body_limit_rejects(app, res, req)) return;
        dispatch_sync(handler_id, res, req);
    });
}

extern "C" void uwsgo_app_options(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    app->app->options(pattern, [handler_id](auto *res, auto *req) {
        dispatch_sync(handler_id, res, req);
    });
}

extern "C" void uwsgo_app_head(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id) {
    app->app->head(pattern, [handler_id](auto *res, auto *req) {
        dispatch_sync(handler_id, res, req);
    });
}

extern "C" void uwsgo_app_set_body_limit(uwsgo_app_t *app, size_t limit) {
    app->body_limit = limit;
}

extern "C" void uwsgo_app_set_capture_peer_ip(uwsgo_app_t *app, int enable) {
    app->capture_peer_ip = enable != 0;
}

extern "C" void uwsgo_app_ws(uwsgo_app_t *app, const char *pattern, uintptr_t handler_id,
    size_t max_payload, int idle_seconds, size_t max_backpressure,
    int send_pings_automatically, int with_upgrade) {
    app->has_websocket = true;
    uWS::App::WebSocketBehavior<uwsgo_ws_data_t> behavior = {};

    behavior.maxPayloadLength = static_cast<unsigned int>(max_payload);
    behavior.idleTimeout = static_cast<unsigned short>(idle_seconds);
    behavior.maxBackpressure = static_cast<unsigned int>(max_backpressure);
    behavior.sendPingsAutomatically = send_pings_automatically != 0;

    if (with_upgrade != 0) {
        behavior.upgrade = [handler_id](auto *res, auto *req, struct us_socket_context_t *context) {
            // Snapshot every field the Go callback might read,
            // including the headers blob, BEFORE we hand control
            // off — uWS frees the underlying HttpRequest the
            // moment the callback returns and Go runs synchronously
            // here on the loop thread.
            UpgradeCtx ctx;
            ctx.res = res;
            ctx.context = context;
            ctx.sec_key = std::string(req->getHeader("sec-websocket-key"));
            ctx.sec_protocol_offered = std::string(req->getHeader("sec-websocket-protocol"));
            ctx.sec_extensions = std::string(req->getHeader("sec-websocket-extensions"));
            ctx.done = 0;

            // Pack the rest of the request snapshot the Go side
            // exposes through UpgradeContext methods.
            UpgradeSnapshot snap;
            snap.method = std::string(req->getMethod());
            snap.url = std::string(req->getUrl());
            snap.query = std::string(req->getQuery());
            // Headers as packed name\0value\0... blob to mirror the
            // representation requestSnapshot uses for HTTP routes.
            std::string headers_blob;
            for (auto it = req->begin(); it != req->end(); ++it) {
                auto pair = *it;
                headers_blob.append(pair.first.data(), pair.first.size());
                headers_blob.push_back(0);
                headers_blob.append(pair.second.data(), pair.second.size());
                headers_blob.push_back(0);
            }
            snap.headers_blob = std::move(headers_blob);

            // Peer IP — uWS exposes a binary representation; mirror
            // the format goRequestSnapshot uses elsewhere by going
            // through getRemoteAddressAsText.
            snap.ip = std::string(res->getRemoteAddressAsText());

            uwsgoHandleWSUpgrade(
                static_cast<uintptr_t>(handler_id),
                &ctx,
                snap.method.data(), snap.method.size(),
                snap.url.data(), snap.url.size(),
                snap.query.data(), snap.query.size(),
                snap.ip.data(), snap.ip.size(),
                snap.headers_blob.data(), snap.headers_blob.size(),
                ctx.sec_protocol_offered.data(), ctx.sec_protocol_offered.size());

            if (!ctx.done) {
                // Defensive: the Go callback failed to call accept
                // or reject. Refuse with 500 so the socket isn't
                // leaked — better to fail loudly than hang.
                res->writeStatus("500 Internal Server Error");
                res->end("upgrade callback returned without accept/reject");
            }
        };
    }

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

// UpgradeCtx + UpgradeSnapshot live next to uwsgo_app_ws because
// they only exist for that function's upgrade callback. UpgradeCtx
// is pointer-shared with Go; UpgradeSnapshot is just a scratch
// buffer copied into the cgo call arguments and discarded when the
// lambda returns.
extern "C" void uwsgo_res_upgrade_accept(void *ctx_ptr,
    const char *sec_protocol, size_t sec_protocol_len,
    uintptr_t user_data) {
    auto *ctx = static_cast<UpgradeCtx *>(ctx_ptr);
    if (ctx->done) {
        return;
    }
    ctx->done = 1;

    uwsgo_ws_data_t user;
    user.go_user_data = user_data;

    // res->upgrade<UserData>(userData, secKey, secProtocol,
    //                        secExtensions, context) consumes the
    // response and produces a uWS::WebSocket; from here on the
    // open/message/close handlers fire.
    ctx->res->template upgrade<uwsgo_ws_data_t>(
        std::move(user),
        ctx->sec_key,
        std::string_view(sec_protocol, sec_protocol_len),
        ctx->sec_extensions,
        ctx->context);
}

extern "C" void uwsgo_res_upgrade_reject(void *ctx_ptr,
    const char *status, size_t status_len,
    const char *body, size_t body_len) {
    auto *ctx = static_cast<UpgradeCtx *>(ctx_ptr);
    if (ctx->done) {
        return;
    }
    ctx->done = 1;

    ctx->res->writeStatus(std::string_view(status, status_len));
    ctx->res->writeHeader("Content-Type", "text/plain; charset=utf-8");
    ctx->res->end(std::string_view(body, body_len));
}

extern "C" uintptr_t uwsgo_ws_user_data(uwsgo_ws_t *ws) {
    auto *socket = reinterpret_cast<GoWebSocket *>(ws);
    return socket->getUserData()->go_user_data;
}

extern "C" void uwsgo_ws_set_user_data(uwsgo_ws_t *ws, uintptr_t user_data) {
    auto *socket = reinterpret_cast<GoWebSocket *>(ws);
    socket->getUserData()->go_user_data = user_data;
}

extern "C" int uwsgo_app_listen(uwsgo_app_t *app, const char *host, int port) {
    bool ok = false;
    auto cb = [&ok, app](auto *listen_socket) {
        app->listen_socket = listen_socket;
        ok = listen_socket != nullptr;
    };
    if (host != nullptr && host[0] != '\0') {
        app->app->listen(std::string(host), port, std::move(cb));
    } else {
        app->app->listen(port, std::move(cb));
    }
    return ok ? 1 : 0;
}

extern "C" int uwsgo_app_add_child(uwsgo_app_t *parent, uwsgo_app_t *child) {
    if (parent == nullptr || child == nullptr || parent->app == nullptr || child->app == nullptr) {
        return 0;
    }
    parent->app->addChildApp(child->app.get());
    return 1;
}

extern "C" void uwsgo_app_run(uwsgo_app_t *app) {
    if (app == nullptr || app->app == nullptr) {
        return;
    }
    app->app->run();
    app->accepting_work.store(false, std::memory_order_release);
    us_internal_free_closed_sockets(reinterpret_cast<us_loop_t *>(app->loop));
    // uWS::App owns the WebSocket TopicTree and unregisters its loop
    // pre/post handlers in the App destructor via Loop::get(). That must run
    // on the loop thread; deleting the App later from Go's caller goroutine can
    // leave dangling TopicTree handlers on this loop and crash the next run.
    std::lock_guard<std::mutex> lock(app->app_mu);
    if (app->app != nullptr) {
        app->app.reset();
        app->listen_socket = nullptr;
    }
    if (app->has_websocket && app->loop != nullptr) {
        app->loop->free();
        app->loop = nullptr;
    }
}

// Closes the App (which closes the listen socket and all active connection
// sockets via uWS::App::close), then closes the drain timer so the only
// remaining loop refs drop and us_loop_run can return. The actual close calls
// are dispatched via Loop::defer because they mutate loop state and must run
// on the loop thread. Safe to call from any goroutine. Idempotent.
extern "C" void uwsgo_app_stop(uwsgo_app_t *app) {
    if (app == nullptr || app->loop == nullptr) {
        return;
    }
    app->accepting_work.store(false, std::memory_order_release);
    app->loop->defer([app]() {
        std::lock_guard<std::mutex> lock(app->app_mu);
        if (app->app != nullptr) {
            app->app->close();
            app->listen_socket = nullptr;
        }
        if (app->drain_timer != nullptr) {
            us_timer_close(app->drain_timer);
            app->drain_timer = nullptr;
        }
    });
}

// uwsgo_app_close_listen closes only the listen socket and the drain
// timer. Active sockets stay open until they finish their own response
// and the client (or HTTP keep-alive timeout) closes them, at which
// point the uWS loop's fd count drops to zero and run() returns. Calling
// uwsgo_app_stop afterwards force-closes any remaining stragglers.
extern "C" void uwsgo_app_close_listen(uwsgo_app_t *app) {
    if (app == nullptr || app->loop == nullptr) {
        return;
    }
    app->accepting_work.store(false, std::memory_order_release);
    app->loop->defer([app]() {
        std::lock_guard<std::mutex> lock(app->app_mu);
        if (app->app == nullptr) {
            return;
        }
        if (app->listen_socket) {
            us_listen_socket_close(0, app->listen_socket);
            app->listen_socket = nullptr;
        }
        if (app->drain_timer != nullptr) {
            us_timer_close(app->drain_timer);
            app->drain_timer = nullptr;
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

// Batch variant: walks the packed `key\0value\0key\0value\0` blob
// emitting each pair as a single writeHeader. memchr keeps the
// scan bounded by headers_len even if a malformed blob ever omits
// a trailing NUL — same defensive walk as the async defer path's
// header parser.
extern "C" void uwsgo_res_write_headers_batch(uwsgo_res_t *res,
    const char *headers_blob, size_t headers_len, size_t count) {
    auto *r = reinterpret_cast<uWS::HttpResponse<false> *>(res);
    const char *p = headers_blob;
    const char *end = headers_blob + headers_len;
    for (size_t i = 0; i < count && p < end; ++i) {
        const char *name_end = static_cast<const char *>(
            std::memchr(p, 0, static_cast<size_t>(end - p)));
        if (name_end == nullptr) break;
        std::string_view name(p, static_cast<size_t>(name_end - p));
        p = name_end + 1;
        if (p >= end) break;
        const char *value_end = static_cast<const char *>(
            std::memchr(p, 0, static_cast<size_t>(end - p)));
        if (value_end == nullptr) break;
        std::string_view value(p, static_cast<size_t>(value_end - p));
        p = value_end + 1;
        r->writeHeader(name, value);
    }
}

extern "C" void uwsgo_res_write(uwsgo_res_t *res, const char *body, size_t body_len) {
    reinterpret_cast<uWS::HttpResponse<false> *>(res)->write(std::string_view(body, body_len));
}

// HttpResponseAccess is the C++ pattern for reaching a protected
// base-class method: derive a class that publicly re-exports
// AsyncSocket::getBufferedAmount and reinterpret-cast through it.
// uWS deliberately scopes the getter to friend WebSocketContext +
// the response itself; for our backpressure hook we need it
// readable from the streaming caller's side.
class HttpResponseAccess : public uWS::HttpResponse<false> {
public:
    using uWS::AsyncSocket<false>::getBufferedAmount;
};

// uwsgo_res_buffered_amount returns uWS's own send-queue depth in
// bytes. Used by Go-side streaming callers to detect backpressure
// (queue grows when the client can't drain fast enough).
extern "C" size_t uwsgo_res_buffered_amount(uwsgo_res_t *res) {
    return reinterpret_cast<HttpResponseAccess *>(res)->getBufferedAmount();
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

extern "C" void uwsgo_res_send_split(
    uwsgo_res_t *res,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *headers_blob, size_t headers_len,
    const char *prefix, size_t prefix_len,
    const char *body, size_t body_len) {
    auto *r = reinterpret_cast<uWS::HttpResponse<false> *>(res);
    r->cork([r, status, status_len, content_type, content_type_len,
             headers_blob, headers_len, prefix, prefix_len, body, body_len]() {
        r->writeStatus(std::string_view(status, status_len));
        const char *p = headers_len > 0 ? headers_blob : nullptr;
        const char *end = headers_len > 0 ? headers_blob + headers_len : nullptr;
        while (p != nullptr && p < end) {
            const char *name_end = static_cast<const char *>(
                std::memchr(p, 0, static_cast<size_t>(end - p)));
            if (name_end == nullptr) break;
            std::string_view name(p, static_cast<size_t>(name_end - p));
            p = name_end + 1;
            if (p >= end) break;
            const char *value_end = static_cast<const char *>(
                std::memchr(p, 0, static_cast<size_t>(end - p)));
            if (value_end == nullptr) break;
            std::string_view value(p, static_cast<size_t>(value_end - p));
            p = value_end + 1;
            r->writeHeader(name, value);
        }
        if (content_type_len > 0) {
            r->writeHeader(std::string_view("Content-Type", 12), std::string_view(content_type, content_type_len));
        }
        if (prefix_len > 0) {
            r->write(std::string_view(prefix, prefix_len));
        }
        r->end(std::string_view(body, body_len));
    });
}

extern "C" size_t uwsgo_res_remote_addr(uwsgo_res_t *res, char *buffer, size_t buffer_len) {
    auto *r = reinterpret_cast<uWS::HttpResponse<false> *>(res);
    auto addr = r->getRemoteAddressAsText();
    return copy_string_view(addr, buffer, buffer_len);
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

// The shared-dispatch internals (AsyncCtx, PendingRing, etc.) live at file
// scope rather than in an anonymous namespace so uwsgo_app_t (declared near
// the top of the file) can hold a PendingRing* without a forward-decl tug
// of war. They remain internal to this translation unit since the C ABI
// only exposes opaque pointers.

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
// IPv4 needs ~15 chars; IPv6 ~45 chars; 64 leaves headroom for bracketed
// forms and the trailing null without bloating AsyncCtx.
constexpr size_t SNAP_IP_CAP = 64;
// Headers are encoded as "name\0value\0..." back-to-back so Go can parse on
// access without knowing the count up front. 8 KB matches the sync
// HEADERS_SCRATCH_SIZE — both paths now have the same upper bound on
// how many bytes of headers Go gets to see without falling back to
// cgo (the async path has no cgo fallback because the request is
// freed by the time the worker runs, so this cap is also the cap on
// non-truncated lookups for async requests).
constexpr size_t SNAP_HEADERS_CAP = 8192;

// Request body slot for the zero-cgo shared-dispatch PostAsync
// path. Bodies up to this size are collected on the loop side into
// the per-request AsyncCtx, then handed to the Go worker via
// shared memory — same path GetAsync uses, no per-chunk cgo. Above
// this cap the framework falls back to the cgo-mediated async
// path. 8 KiB matches the sync scratch buffer and covers the
// realistic ceiling for typical JSON / form / webhook POST bodies.
constexpr size_t SNAP_BODY_CAP = 8192;

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
    std::atomic<size_t> stream_pending_bytes{0};
    uWS::HttpResponse<false> *response;
    uWS::Loop *loop = nullptr;  // The loop that owns this response (set at creation time)
    PendingRing *pending_ring = nullptr;  // The response ring this ctx must be pushed onto
    CtxPool *pool = nullptr;  // Per-App pool to push back into on release; null = always delete
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
    uint32_t truncated = 0;
    uint32_t ip_len = 0;
    uint32_t param_lens[SNAP_PARAM_MAX] = {0};
    char method[SNAP_METHOD_CAP];
    char url[SNAP_URL_CAP];
    char query[SNAP_QUERY_CAP];
    char ip[SNAP_IP_CAP];
    char params[SNAP_PARAM_MAX][SNAP_PARAM_CAP];
    char headers[SNAP_HEADERS_CAP];

    // Request body — only populated for POST routes registered
    // via uwsgo_app_post_shared. body_len carries the byte count;
    // body_overflow is set when the incoming payload exceeded
    // SNAP_BODY_CAP (or the route's max), so Go can surface a 413
    // without re-reading the size out of band.
    uint32_t body_len = 0;
    uint32_t body_overflow = 0;
    char body[SNAP_BODY_CAP];

    void retain() { refcount.fetch_add(1, std::memory_order_relaxed); }

    // reset_for_pool wipes mutable state so a recycled ctx is
    // indistinguishable from a freshly-constructed one. Only the
    // non-array members need resetting — char arrays carry length
    // prefixes that are reset here, so any stale bytes are unreachable
    // through the documented accessors. Refcount goes back to 1 so the
    // next consumer sees the same starting state as `new AsyncCtx`.
    void reset_for_pool() {
        refcount.store(1, std::memory_order_relaxed);
        aborted.store(0, std::memory_order_relaxed);
        stream_pending_bytes.store(0, std::memory_order_relaxed);
        response = nullptr;
        loop = nullptr;
        pending_ring = nullptr;
        handler_id = 0;
        inline_status_len = 0;
        inline_ct_len = 0;
        inline_body_len = 0;
        method_len = 0;
        url_len = 0;
        query_len = 0;
        param_count = 0;
        headers_len = 0;
        truncated = 0;
        ip_len = 0;
        body_len = 0;
        body_overflow = 0;
        for (uint32_t i = 0; i < SNAP_PARAM_MAX; i++) param_lens[i] = 0;
        // pool field is sticky across recycles — it points at the same
        // App's pool for the entire lifetime of this object.
    }

    void release() {
        if (refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            CtxPool *p = pool;  // load before reset clobbers anything
            if (p) {
                reset_for_pool();
                if (p->push(this)) {
                    return;  // recycled into pool
                }
            }
            delete this;
        }
    }
};

extern "C" void uwsgo_app_free(uwsgo_app_t *app) {
    if (app->ctx_pool) {
        // Drain whatever's left in the pool. Pop until empty and delete
        // each ctx — at app teardown there are no producers, so no
        // races. Bypass the recycle path (set pool=nullptr) so each
        // delete really frees.
        for (;;) {
            AsyncCtx *ctx = app->ctx_pool->pop();
            if (!ctx) break;
            ctx->pool = nullptr;
            delete ctx;
        }
        delete app->ctx_pool;
    }
    if (app->pending_ring) {
        delete app->pending_ring;
    }
    delete app;
}

// RequestRing is shared across all App instances: C++ enqueues incoming
// requests (as AsyncCtx*), Go worker goroutines dequeue and dispatch. Single
// global ring + single worker pool is fine because workers are stateless —
// they look up the per-ctx pending_ring at response time. Aligned to a cache
// line to avoid false sharing between head and tail.
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

extern "C" void uwsgo_async_ctx_retain(void *ctx_handle) {
    auto *ctx = static_cast<AsyncCtx *>(ctx_handle);
    if (ctx != nullptr) {
        ctx->retain();
    }
}

extern "C" int uwsgo_async_ctx_aborted(void *ctx_handle) {
    auto *ctx = static_cast<AsyncCtx *>(ctx_handle);
    if (ctx == nullptr) {
        return 1;
    }
    return ctx->aborted.load(std::memory_order_acquire) ? 1 : 0;
}

extern "C" size_t uwsgo_async_ctx_stream_pending_bytes(void *ctx_handle) {
    auto *ctx = static_cast<AsyncCtx *>(ctx_handle);
    if (ctx == nullptr) {
        return 0;
    }
    return ctx->stream_pending_bytes.load(std::memory_order_acquire);
}

extern "C" void uwsgo_shared_layout(uwsgo_shared_layout_t *out) {
    g_request.init();
    // ring is no longer a global pointer; each AsyncCtx carries its App's
    // pending_ring via the ctx_pending_ring_offset field. Go reads it from
    // the ctx in asyncSendShared so multiple App instances each use their
    // own ring.
    out->ring = nullptr;
    out->request_ring = &g_request;
    out->ring_size = RING_SIZE;
    out->ring_mask = RING_MASK;
    out->ring_slots_offset = offsetof(PendingRing, slots);
    out->ring_slot_stride = sizeof(PendingSlot);
    out->ring_slot_seq_offset = offsetof(PendingSlot, sequence);
    out->ring_slot_ctx_offset = offsetof(PendingSlot, ctx);
    out->ring_head_offset = offsetof(PendingRing, head);
    out->ring_tail_offset = offsetof(PendingRing, tail);
    out->ring_wake_pending_offset = offsetof(PendingRing, wake_pending);

    out->ctx_status_len_offset = offsetof(AsyncCtx, inline_status_len);
    out->ctx_ct_len_offset = offsetof(AsyncCtx, inline_ct_len);
    out->ctx_body_len_offset = offsetof(AsyncCtx, inline_body_len);
    out->ctx_status_offset = offsetof(AsyncCtx, inline_status);
    out->ctx_ct_offset = offsetof(AsyncCtx, inline_content_type);
    out->ctx_body_offset = offsetof(AsyncCtx, inline_body);
    out->ctx_handler_id_offset = offsetof(AsyncCtx, handler_id);
    out->ctx_aborted_offset = offsetof(AsyncCtx, aborted);
    out->ctx_response_offset = offsetof(AsyncCtx, response);
    out->ctx_loop_offset = offsetof(AsyncCtx, loop);
    out->ctx_pending_ring_offset = offsetof(AsyncCtx, pending_ring);
    out->ctx_inline_status_cap = INLINE_STATUS_CAP;
    out->ctx_inline_ct_cap = INLINE_CT_CAP;
    out->ctx_inline_body_cap = INLINE_BODY_CAP;

    out->ctx_method_len_offset = offsetof(AsyncCtx, method_len);
    out->ctx_url_len_offset = offsetof(AsyncCtx, url_len);
    out->ctx_query_len_offset = offsetof(AsyncCtx, query_len);
    out->ctx_param_count_offset = offsetof(AsyncCtx, param_count);
    out->ctx_headers_len_offset = offsetof(AsyncCtx, headers_len);
    out->ctx_truncated_offset = offsetof(AsyncCtx, truncated);
    out->ctx_param_lens_offset = offsetof(AsyncCtx, param_lens);
    out->ctx_method_offset = offsetof(AsyncCtx, method);
    out->ctx_url_offset = offsetof(AsyncCtx, url);
    out->ctx_query_offset = offsetof(AsyncCtx, query);
    out->ctx_ip_len_offset = offsetof(AsyncCtx, ip_len);
    out->ctx_ip_offset = offsetof(AsyncCtx, ip);
    out->ctx_params_offset = offsetof(AsyncCtx, params);
    out->ctx_headers_offset = offsetof(AsyncCtx, headers);
    out->ctx_req_body_len_offset = offsetof(AsyncCtx, body_len);
    out->ctx_req_body_overflow_offset = offsetof(AsyncCtx, body_overflow);
    out->ctx_req_body_offset = offsetof(AsyncCtx, body);
    out->ctx_snap_method_cap = SNAP_METHOD_CAP;
    out->ctx_snap_url_cap = SNAP_URL_CAP;
    out->ctx_snap_query_cap = SNAP_QUERY_CAP;
    out->ctx_snap_ip_cap = SNAP_IP_CAP;
    out->ctx_snap_param_cap = SNAP_PARAM_CAP;
    out->ctx_snap_param_max = SNAP_PARAM_MAX;
    out->ctx_snap_headers_cap = SNAP_HEADERS_CAP;
    out->ctx_snap_req_body_cap = SNAP_BODY_CAP;
}

// uwsgo_app_get_shared registers a route whose dispatch path skips the
// Go-side cgo callback entirely. When a request matches, C++ builds an
// AsyncCtx with handler_id stamped on it and pushes it onto the shared
// request ring. Go worker goroutines (started by the binding at startup)
// drain the ring with plain atomic ops and run the handler.
// snapshot_request copies the fields of the live uWS HttpRequest into the
// AsyncCtx so the async goroutine can read them after uWS frees the request.
// Anything that doesn't fit the fixed buffers is truncated.
static void snapshot_request(uwsgo_app_t *app, AsyncCtx *ctx, uWS::HttpResponse<false> *res, uWS::HttpRequest *req) {
    bool truncated = false;
    auto copy_view = [&truncated](char *dst, size_t cap, std::string_view src) -> uint32_t {
        size_t n = std::min(cap, src.size());
        if (n > 0) std::memcpy(dst, src.data(), n);
        if (src.size() > cap) truncated = true;
        return static_cast<uint32_t>(n);
    };

    ctx->method_len = copy_view(ctx->method, SNAP_METHOD_CAP, req->getMethod());
    // getUrl returns the path; getQuery returns query string sans '?'.
    ctx->url_len = copy_view(ctx->url, SNAP_URL_CAP, req->getUrl());
    ctx->query_len = copy_view(ctx->query, SNAP_QUERY_CAP, req->getQuery());
    // Peer IP is opt-in (Config.CapturePeerIP). Skipping the format +
    // memcpy saves ~2-3% on small-response shared-dispatch routes;
    // when off, snap.ip is empty and req.IP() returns "" in the
    // worker.
    if (app->capture_peer_ip) {
        ctx->ip_len = copy_view(ctx->ip, SNAP_IP_CAP, res->getRemoteAddressAsText());
    } else {
        ctx->ip_len = 0;
    }

    // Route parameters: walk indices until uWS returns empty.
    uint32_t param_count = 0;
    for (uint32_t i = 0; i < SNAP_PARAM_MAX; i++) {
        auto v = req->getParameter(i);
        if (v.empty()) break;
        ctx->param_lens[i] = copy_view(ctx->params[i], SNAP_PARAM_CAP, v);
        param_count = i + 1;
    }
    if (!req->getParameter(SNAP_PARAM_MAX).empty()) truncated = true;
    ctx->param_count = param_count;

    // Headers: encode as "name\0value\0..." back-to-back. Stop when the next
    // pair won't fit so we don't half-write a value.
    uint32_t hpos = 0;
    for (auto it = req->begin(); it != req->end(); ++it) {
        auto kv = *it;
        std::string_view name = kv.first;
        std::string_view value = kv.second;
        size_t need = name.size() + 1 + value.size() + 1;
        if (hpos + need > SNAP_HEADERS_CAP) {
            truncated = true;
            break;
        }
        std::memcpy(ctx->headers + hpos, name.data(), name.size());
        hpos += name.size();
        ctx->headers[hpos++] = '\0';
        std::memcpy(ctx->headers + hpos, value.data(), value.size());
        hpos += value.size();
        ctx->headers[hpos++] = '\0';
    }
    ctx->headers_len = hpos;
    ctx->truncated = truncated ? 1 : 0;
}

// enqueue_ctx pushes a fully-populated AsyncCtx onto the request
// ring. Returns true on success; on failure (ring full or pool
// contention) it has already written a 503 to res and released
// the ctx — callers must NOT touch ctx or res after a false
// return. Used by both the GET and POST shared-dispatch paths so
// the enqueue-and-recover logic stays in one place.
static bool enqueue_ctx(AsyncCtx *ctx, uWS::HttpResponse<false> *res) {
    uint64_t tail = g_request.tail.load(std::memory_order_relaxed);
    for (int spin = 0;; ++spin) {
        PendingSlot *slot = &g_request.slots[tail & RING_MASK];
        uint64_t seq = slot->sequence.load(std::memory_order_acquire);
        int64_t diff = (int64_t)(seq - tail);
        if (diff == 0) {
            if (g_request.tail.compare_exchange_weak(
                    tail, tail + 1,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                res->onAborted([hold = CtxHold(ctx)]() {
                    hold.ctx->aborted.store(1, std::memory_order_release);
                });
                slot->ctx = ctx;
                slot->sequence.store(tail + 1, std::memory_order_release);
                return true;
            }
        } else if (diff < 0) {
            ctx->release();
            res->writeStatus("503 Service Unavailable");
            res->writeHeader("Content-Type", "text/plain; charset=utf-8");
            res->end("Server overloaded\n");
            return false;
        } else {
            tail = g_request.tail.load(std::memory_order_relaxed);
        }
        if (spin > 100000) {
            ctx->release();
            res->writeStatus("503 Service Unavailable");
            res->writeHeader("Content-Type", "text/plain; charset=utf-8");
            res->end("Enqueue contention\n");
            return false;
        }
    }
}

// acquire_shared_ctx pulls an AsyncCtx from the App's recycle pool
// (or allocates one on miss) and stamps it with the per-request
// fields the worker needs. Used by both GET and POST shared-
// dispatch handlers — keeps the pool / handler_id / loop wiring
// in one place.
static AsyncCtx *acquire_shared_ctx(uwsgo_app_t *app, uWS::HttpResponse<false> *res, uint32_t handler_id) {
    AsyncCtx *ctx = app->ctx_pool ? app->ctx_pool->pop() : nullptr;
    if (!ctx) {
        ctx = new AsyncCtx;
        ctx->pool = app->ctx_pool;
    }
    ctx->response = res;
    ctx->loop = uWS::Loop::get();
    ctx->pending_ring = app->pending_ring;
    ctx->handler_id = handler_id;
    return ctx;
}

extern "C" void uwsgo_app_get_shared(uwsgo_app_t *app, const char *pattern, uint32_t handler_id) {
    app->app->get(pattern, [app, handler_id](auto *res, auto *req) {
        AsyncCtx *ctx = acquire_shared_ctx(app, res, handler_id);
        // Snapshot before any cgo / Go work — uWS HttpRequest is
        // live only inside this lambda.
        snapshot_request(app, ctx, res, req);
        if (ctx->truncated) {
            ctx->release();
            res->writeStatus("431 Request Header Fields Too Large");
            res->writeHeader("Content-Type", "text/plain; charset=utf-8");
            res->end("Request snapshot too large\n");
            return;
        }
        enqueue_ctx(ctx, res);
    });
}

// uwsgo_app_post_shared is the POST counterpart to
// uwsgo_app_get_shared. The lambda snapshots the request the same
// way, then collects body chunks into ctx->body until either
// isLast=true (push to ring) or the cap is exceeded (413 + release).
// max_body is clamped to SNAP_BODY_CAP — Go-side registration
// validates the user's requested cap before reaching here.
extern "C" void uwsgo_app_post_shared(uwsgo_app_t *app, const char *pattern,
    uint32_t handler_id, size_t max_body) {
    if (max_body == 0 || max_body > SNAP_BODY_CAP) {
        max_body = SNAP_BODY_CAP;
    }
    app->app->post(pattern, [app, handler_id, max_body](auto *res, auto *req) {
        AsyncCtx *ctx = acquire_shared_ctx(app, res, handler_id);
        snapshot_request(app, ctx, res, req);
        if (ctx->truncated) {
            ctx->release();
            res->writeStatus("431 Request Header Fields Too Large");
            res->writeHeader("Content-Type", "text/plain; charset=utf-8");
            res->end("Request snapshot too large\n");
            return;
        }

        // Body collection runs after the lambda returns — uWS calls
        // onData per chunk. We accumulate into ctx->body until
        // isLast or overflow. ctx is captured by reference into the
        // closure; the onAborted hook flips the aborted flag so a
        // disconnected client mid-stream doesn't enqueue garbage.
        res->onAborted([hold = CtxHold(ctx)]() {
            hold.ctx->aborted.store(1, std::memory_order_release);
        });
        res->onData([ctx, res, max_body](std::string_view chunk, bool isLast) mutable {
            if (ctx->aborted.load(std::memory_order_acquire)) {
                // Client gone — drop the ctx, no enqueue. release()
                // here mirrors the onAborted increment so refcount
                // stays balanced.
                if (isLast) ctx->release();
                return;
            }
            if (!ctx->body_overflow) {
                size_t room = max_body - ctx->body_len;
                if (chunk.size() > room) {
                    // Capture what fits then flag overflow; we'll
                    // 413 on the next isLast.
                    std::memcpy(ctx->body + ctx->body_len, chunk.data(), room);
                    ctx->body_len += static_cast<uint32_t>(room);
                    ctx->body_overflow = 1;
                } else {
                    if (chunk.size() > 0) {
                        std::memcpy(ctx->body + ctx->body_len, chunk.data(), chunk.size());
                        ctx->body_len += static_cast<uint32_t>(chunk.size());
                    }
                }
            }
            if (!isLast) return;

            if (ctx->body_overflow) {
                ctx->release();
                res->writeStatus("413 Payload Too Large");
                res->writeHeader("Content-Type", "text/plain; charset=utf-8");
                res->end("payload too large\n");
                return;
            }
            // Body complete and within cap. Push to the ring so a
            // worker goroutine can run the user handler. The
            // enqueue_ctx helper also re-registers onAborted on
            // success — that's the canonical hook for the post-
            // enqueue abort signal, matching the GET path.
            enqueue_ctx(ctx, res);
        });
    });
}

// drain_pending walks a specific App's response ring on its loop thread.
// Single consumer (the loop) so no CAS needed on head. Takes the ring as a
// parameter so the same routine serves multiple App instances each with its
// own ring + loop.
//
// Clears ring->wake_pending at the start so any producer that publishes a
// slot AFTER this point will succeed in CAS(0,1) and call wake_drain for
// the next pass. Producers that publish BEFORE this clear are already
// in the slots we're about to walk; no wake needed.
static void drain_pending(PendingRing *ring) {
    ring->wake_pending.store(0, std::memory_order_release);
    uint64_t h = ring->head.load(std::memory_order_relaxed);
    while (true) {
        PendingSlot *slot = &ring->slots[h & RING_MASK];
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
    ring->head.store(h, std::memory_order_relaxed);
}

// uwsgo_wake_drain schedules a single drain pass on the given loop, draining
// the given ring. Callable from any goroutine via Go: after pushing onto the
// response ring, this wakes the loop immediately instead of waiting up to
// ~1 ms for the periodic drain timer to fire. Costs one cgo crossing per
// response in exchange for sub-millisecond response latency.
extern "C" void uwsgo_wake_drain(uwsgo_loop_t *loop, void *ring) {
    auto *l = reinterpret_cast<uWS::Loop *>(loop);
    auto *r = reinterpret_cast<PendingRing *>(ring);
    l->defer([r]() { drain_pending(r); });
}

// uwsgo_app_start_drain installs a periodic safety-net drain timer on this
// App's loop. The wake-on-write path normally fires drains immediately;
// this timer just catches anything that slips through.
extern "C" void uwsgo_app_start_drain(uwsgo_app_t *app, int interval_us) {
    auto *loop = reinterpret_cast<struct us_loop_t *>(app->loop);
    // Allocate ext_size = sizeof(PendingRing*) so we can stash the ring
    // pointer inline with the timer; libuS gives us back the timer in the
    // callback and us_timer_ext recovers the trailing user data.
    app->drain_timer = us_create_timer(loop, 0, sizeof(PendingRing *));
    int ms = interval_us / 1000;
    if (ms < 1) ms = 1;
    *reinterpret_cast<PendingRing **>(us_timer_ext(app->drain_timer)) = app->pending_ring;
    us_timer_set(app->drain_timer, [](struct us_timer_t *t) {
        auto *r = *reinterpret_cast<PendingRing **>(us_timer_ext(t));
        drain_pending(r);
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

// SendBufferWithHeaders carries the same fields as SendBuffer plus a
// packed name\0value\0... blob of extra response headers written between
// the status line and the Content-Type header.
struct SendBufferWithHeaders {
    char *status = nullptr;
    size_t status_len = 0;
    char *content_type = nullptr;
    size_t content_type_len = 0;
    char *headers_blob = nullptr;
    size_t headers_len = 0;
    char *body = nullptr;
    size_t body_len = 0;

    SendBufferWithHeaders() = default;
    SendBufferWithHeaders(const SendBufferWithHeaders &) = delete;
    SendBufferWithHeaders &operator=(const SendBufferWithHeaders &) = delete;
    SendBufferWithHeaders(SendBufferWithHeaders &&o) noexcept :
        status(o.status), status_len(o.status_len),
        content_type(o.content_type), content_type_len(o.content_type_len),
        headers_blob(o.headers_blob), headers_len(o.headers_len),
        body(o.body), body_len(o.body_len) {
        o.status = o.content_type = o.headers_blob = o.body = nullptr;
    }
    SendBufferWithHeaders &operator=(SendBufferWithHeaders &&) = delete;
    ~SendBufferWithHeaders() {
        std::free(status);
        std::free(content_type);
        std::free(headers_blob);
        std::free(body);
    }
};

extern "C" void uwsgo_res_defer_send_with_headers(
    uwsgo_loop_t *loop,
    void *ctx_handle,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *headers_blob, size_t headers_len,
    const char *body, size_t body_len) {
    auto *l = reinterpret_cast<uWS::Loop *>(loop);
    auto *ctx = static_cast<AsyncCtx *>(ctx_handle);

    SendBufferWithHeaders buf;
    buf.status = dup_to_c_heap(status, status_len);
    buf.status_len = status_len;
    buf.content_type = dup_to_c_heap(content_type, content_type_len);
    buf.content_type_len = content_type_len;
    buf.headers_blob = dup_to_c_heap(headers_blob, headers_len);
    buf.headers_len = headers_len;
    buf.body = dup_to_c_heap(body, body_len);
    buf.body_len = body_len;

    l->defer([ctx, sb = std::move(buf)]() mutable {
        if (ctx->aborted.load(std::memory_order_acquire)) {
            ctx->release();
            return;
        }
        auto *r = ctx->response;
        r->cork([r, &sb]() {
            r->writeStatus(std::string_view(sb.status, sb.status_len));
            // Walk the packed name\0value\0... blob and emit each pair as
            // a header. memchr keeps the scan bounded by headers_len even
            // if a malformed blob ever lacks a trailing NUL.
            const char *p = sb.headers_blob;
            const char *end = sb.headers_blob + sb.headers_len;
            while (p < end) {
                const char *name_end = static_cast<const char *>(
                    std::memchr(p, 0, static_cast<size_t>(end - p)));
                if (name_end == nullptr) break;
                std::string_view name(p, static_cast<size_t>(name_end - p));
                p = name_end + 1;
                if (p >= end) break;
                const char *value_end = static_cast<const char *>(
                    std::memchr(p, 0, static_cast<size_t>(end - p)));
                if (value_end == nullptr) break;
                std::string_view value(p, static_cast<size_t>(value_end - p));
                p = value_end + 1;
                r->writeHeader(name, value);
            }
            if (sb.content_type_len > 0) {
                r->writeHeader(std::string_view("Content-Type", 12),
                               std::string_view(sb.content_type, sb.content_type_len));
            }
            r->end(std::string_view(sb.body, sb.body_len));
        });
        ctx->release();
    });
}

// uwsgo_res_defer_stream_start opens a streaming response. The
// status line + headers go out together inside a cork so they land
// as a single TCP packet; the lambda intentionally does NOT call
// end(), leaving the response open for follow-up stream_write
// chunks. The ctx must outlive every pending defer — we retain
// here so the close-out (stream_end) can release symmetrically.
extern "C" void uwsgo_res_defer_stream_start(
    uwsgo_loop_t *loop,
    void *ctx_handle,
    const char *status, size_t status_len,
    const char *content_type, size_t content_type_len,
    const char *headers_blob, size_t headers_len) {
    auto *l = reinterpret_cast<uWS::Loop *>(loop);
    auto *ctx = static_cast<AsyncCtx *>(ctx_handle);
    ctx->retain();

    SendBufferWithHeaders buf;
    buf.status = dup_to_c_heap(status, status_len);
    buf.status_len = status_len;
    buf.content_type = dup_to_c_heap(content_type, content_type_len);
    buf.content_type_len = content_type_len;
    buf.headers_blob = dup_to_c_heap(headers_blob, headers_len);
    buf.headers_len = headers_len;
    buf.body = nullptr;
    buf.body_len = 0;

    l->defer([ctx, sb = std::move(buf)]() mutable {
        if (ctx->aborted.load(std::memory_order_acquire)) {
            ctx->release();
            return;
        }
        auto *r = ctx->response;
        r->cork([r, &sb]() {
            r->writeStatus(std::string_view(sb.status, sb.status_len));
            const char *p = sb.headers_blob;
            const char *end = sb.headers_blob + sb.headers_len;
            while (p < end) {
                const char *name_end = static_cast<const char *>(
                    std::memchr(p, 0, static_cast<size_t>(end - p)));
                if (name_end == nullptr) break;
                std::string_view name(p, static_cast<size_t>(name_end - p));
                p = name_end + 1;
                if (p >= end) break;
                const char *value_end = static_cast<const char *>(
                    std::memchr(p, 0, static_cast<size_t>(end - p)));
                if (value_end == nullptr) break;
                std::string_view value(p, static_cast<size_t>(value_end - p));
                p = value_end + 1;
                r->writeHeader(name, value);
            }
            if (sb.content_type_len > 0) {
                r->writeHeader(std::string_view("Content-Type", 12),
                               std::string_view(sb.content_type, sb.content_type_len));
            }
            // Intentionally NO r->end(): defer_stream_end closes
            // the response after all chunks have been written.
        });
        ctx->release();
    });
}

// uwsgo_res_defer_stream_write queues a single body chunk. uWS's
// HttpResponse::write() emits the chunk with HTTP/1.1
// transfer-encoding: chunked framing whenever no Content-Length was
// declared — which is the normal streaming case.
extern "C" void uwsgo_res_defer_stream_write(
    uwsgo_loop_t *loop,
    void *ctx_handle,
    const char *chunk, size_t chunk_len) {
    auto *l = reinterpret_cast<uWS::Loop *>(loop);
    auto *ctx = static_cast<AsyncCtx *>(ctx_handle);
    ctx->retain();
    ctx->stream_pending_bytes.fetch_add(chunk_len, std::memory_order_acq_rel);

    char *chunk_copy = dup_to_c_heap(chunk, chunk_len);
    size_t copy_len = chunk_len;

    l->defer([ctx, chunk_copy, copy_len]() mutable {
        if (ctx->aborted.load(std::memory_order_acquire)) {
            if (chunk_copy) std::free(chunk_copy);
            ctx->stream_pending_bytes.fetch_sub(copy_len, std::memory_order_acq_rel);
            ctx->release();
            return;
        }
        auto *r = ctx->response;
        r->write(std::string_view(chunk_copy ? chunk_copy : "", copy_len));
        if (chunk_copy) std::free(chunk_copy);
        ctx->stream_pending_bytes.fetch_sub(copy_len, std::memory_order_acq_rel);
        ctx->release();
    });
}

// uwsgo_res_defer_stream_end closes the streaming response. uWS
// emits the chunked-encoding terminator (0\r\n\r\n) and tears down
// the HttpResponse; subsequent writes against this ctx would target
// freed memory and are forbidden.
extern "C" void uwsgo_res_defer_stream_end(
    uwsgo_loop_t *loop,
    void *ctx_handle) {
    auto *l = reinterpret_cast<uWS::Loop *>(loop);
    auto *ctx = static_cast<AsyncCtx *>(ctx_handle);
    ctx->retain();

    l->defer([ctx]() mutable {
        if (ctx->aborted.load(std::memory_order_acquire)) {
            ctx->release();
            return;
        }
        auto *r = ctx->response;
        r->end(std::string_view());
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

extern "C" int uwsgo_ws_subscribe(uwsgo_ws_t *ws, const char *topic, size_t topic_len) {
    return reinterpret_cast<GoWebSocket *>(ws)
        ->subscribe(std::string_view(topic, topic_len)) ? 1 : 0;
}

extern "C" int uwsgo_ws_unsubscribe(uwsgo_ws_t *ws, const char *topic, size_t topic_len) {
    return reinterpret_cast<GoWebSocket *>(ws)
        ->unsubscribe(std::string_view(topic, topic_len)) ? 1 : 0;
}

extern "C" int uwsgo_ws_publish(uwsgo_ws_t *ws, const char *topic, size_t topic_len,
        const char *message, size_t message_len, int opcode) {
    return reinterpret_cast<GoWebSocket *>(ws)->publish(
        std::string_view(topic, topic_len),
        std::string_view(message, message_len),
        static_cast<uWS::OpCode>(opcode)) ? 1 : 0;
}

extern "C" void uwsgo_app_publish(uwsgo_app_t *app, const char *topic, size_t topic_len,
        const char *message, size_t message_len, int opcode) {
    if (app == nullptr) {
        return;
    }
    // Topic + message copied onto the heap because the cgo caller's
    // buffers go out of scope as soon as this function returns; the
    // deferred publish runs on the loop later. uWS::Loop::defer is
    // thread-safe, so this function can be called from any goroutine.
    //
    // Perf note: two std::string copies + a heap-spilled std::function
    // sounds wasteful, but BenchmarkAppPublishNoSubs measures ~700
    // ns/op end-to-end (cgo crossing + defer mutex + wakeup included)
    // and beat a flex-array single-allocation alternative by ~40 %.
    // The libstdc++ slab allocator's hot path for sub-128-byte
    // allocations is faster than one larger general-purpose alloc;
    // don't "optimize" without re-running the benchmark.
    std::string topic_copy(topic, topic_len);
    std::string message_copy(message, message_len);
    auto op = static_cast<uWS::OpCode>(opcode);
    std::lock_guard<std::mutex> lock(app->app_mu);
    if (app->app == nullptr || app->loop == nullptr ||
            !app->accepting_work.load(std::memory_order_acquire)) {
        return;
    }
    app->loop->defer([app, t = std::move(topic_copy), m = std::move(message_copy), op]() {
        std::lock_guard<std::mutex> lock(app->app_mu);
        if (app->app != nullptr && app->accepting_work.load(std::memory_order_acquire)) {
            app->app->publish(t, m, op);
        }
    });
}

extern "C" void uwsgo_app_publish_batch(
        uwsgo_app_t *app,
        const char *bytes, size_t bytes_len,
        const uwsgo_batch_item_t *items, size_t count) {
    if (app == nullptr) {
        return;
    }
    if (count == 0) {
        return;
    }
    // One alloc owns the items array + byte blob. The per-publish
    // overhead the single-message path pays (mutex, wakeup, lambda
    // heap-spill) gets amortized across `count` items, which is the
    // whole point of this entry point — the slab-vs-large-alloc
    // wash that pessimized the single-message rewrite doesn't apply
    // here because we're saving N-1 of EVERY other cost too.
    size_t items_bytes = count * sizeof(uwsgo_batch_item_t);
    size_t total = items_bytes + bytes_len;
    char *buf = static_cast<char *>(::operator new(total));
    memcpy(buf, items, items_bytes);
    if (bytes_len > 0) {
        memcpy(buf + items_bytes, bytes, bytes_len);
    }

    std::lock_guard<std::mutex> lock(app->app_mu);
    if (app->app == nullptr || app->loop == nullptr ||
            !app->accepting_work.load(std::memory_order_acquire)) {
        ::operator delete(buf);
        return;
    }
    app->loop->defer([app, buf, count]() {
        const auto *items = reinterpret_cast<const uwsgo_batch_item_t *>(buf);
        const char *bytes = buf + count * sizeof(uwsgo_batch_item_t);
        std::lock_guard<std::mutex> lock(app->app_mu);
        if (app->app != nullptr && app->accepting_work.load(std::memory_order_acquire)) {
            for (size_t i = 0; i < count; i++) {
                app->app->publish(
                    std::string_view(bytes + items[i].topic_off, items[i].topic_len),
                    std::string_view(bytes + items[i].message_off, items[i].message_len),
                    static_cast<uWS::OpCode>(items[i].opcode));
            }
        }
        ::operator delete(buf);
    });
}
