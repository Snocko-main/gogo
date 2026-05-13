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

int uwsgo_app_listen(uwsgo_app_t *app, int port);
void uwsgo_app_run(uwsgo_app_t *app);

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

size_t uwsgo_req_url(uwsgo_req_t *req, char *buffer, size_t buffer_len);
size_t uwsgo_req_header(uwsgo_req_t *req, const char *name, size_t name_len, char *buffer, size_t buffer_len);
size_t uwsgo_req_parameter(uwsgo_req_t *req, unsigned long index, char *buffer, size_t buffer_len);

int uwsgo_ws_send(uwsgo_ws_t *ws, const char *message, size_t message_len, int opcode);
void uwsgo_ws_end(uwsgo_ws_t *ws, int code, const char *message, size_t message_len);

#ifdef __cplusplus
}
#endif
