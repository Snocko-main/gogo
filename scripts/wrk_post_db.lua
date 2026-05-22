-- wrk POST script for the /query benchmark.
-- Sends a fixed integer body so each framework parses it, queries the
-- 1000-row SQLite table, and returns the row. Keep the body tiny so
-- the bottleneck stays on body parse + db query, not bandwidth.

wrk.method = "POST"
wrk.body = "42"
wrk.headers["Content-Type"] = "text/plain"
