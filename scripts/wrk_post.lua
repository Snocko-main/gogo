-- wrk POST script for the /echo benchmark.
-- Sends a fixed 64-byte JSON body so all frameworks process the
-- same payload. Keeps the body small so the bottleneck stays on
-- the framework's body-collection + response path, not on syscall
-- copy bandwidth.

wrk.method = "POST"
wrk.body = '{"name":"benchmark","value":42,"items":[1,2,3,4,5]}'
wrk.headers["Content-Type"] = "application/json"
