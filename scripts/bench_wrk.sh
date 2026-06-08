#!/usr/bin/env bash
#
# wrk benchmark matrix across gogo, fiber, nethttp, node+uWebSockets.js,
# bun+elysia, and actix-web.
#
# Route sets:
#   BENCH_ROUTE_SET=legacy
#     Six-framework comparison for the checked-in snapshot:
#     /hello, /hello/:name, /db (GET) + /echo, /query (POST)
#   BENCH_ROUTE_SET=v07-http
#     Local HTTP baseline coverage for gogo/fiber/nethttp:
#     /plain, /hello/:name, /json, /middleware, /async (GET)
# Threads:     1, 2, 4, 8     (wrk -t)
# Connections: 500            (wrk -c)
# Modes:       single (1 worker) and multi (MULTI_WORKERS workers)
#
# POST endpoints use scripts/wrk_post*.lua so each selected server gets
# the same body shape. Each server is started, warmed, benchmarked,
# then killed before moving to the next. Per-run stdout (request rate,
# latency) is appended to
# benchmark/results/wrk-<framework>-<mode>.log.
#
# Env knobs:
#   BENCH_ROUTE_SET legacy|v07-http route/framework defaults (default legacy)
#   DURATION       seconds per wrk run                  (default 15)
#   THREADS        space-separated thread counts        (default "1 2 4 8")
#   CONN           connections                          (default 500)
#   ENDPOINTS      GET endpoints to hit                 (route-set default)
#   POST_ENDPOINTS POST endpoints to hit                (route-set default)
#   FRAMEWORKS     subset to run                        (route-set default)
#   MODES          subset to run                        (default "single multi")
#   MULTI_WORKERS  server workers/processes for multi   (default min(NumCPU, 4))
#   GOGO_WORKERS   shared-dispatch workers for gogo     (default: mode workers)
#   WARMUP         seconds of warmup hits before timing (default 2)
#   RESULTS_DIR    where to write logs                  (default benchmark/results)
#
# Prereqs: wrk, go, node (+ npm install in benchmark/node-uwebsockets),
# bun (+ bun install in benchmark/bun-elysia), and the Actix release
# binary (cargo build --release --manifest-path benchmark/actix/Cargo.toml).
#
# gogo cgo budget for the v0.7 hot HTTP baselines:
#   - Static Reply/string routes: 0 per-request cgo callbacks/calls.
#   - Dynamic sync routes such as /plain, /json, and /hello/:name:
#     <=1 C++->Go handler callback + <=1 Go->C Send call.
#   - Sync middleware route /middleware: <=1 C++->Go handler callback
#     + <=3 Go->C response calls (status, batched headers, end). Adding
#     more middleware headers must keep the batched header crossing.
#   - Shared async route /async: 0 per-request cgo callbacks/calls while
#     the route has no sync middleware and the response fits shared-send
#     limits with Content-Type as the only response header.

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

BENCH_ROUTE_SET="${BENCH_ROUTE_SET:-legacy}"
case "$BENCH_ROUTE_SET" in
legacy)
	DEFAULT_FRAMEWORKS="gogo fiber nethttp uwsjs bun actix"
	DEFAULT_ENDPOINTS="/hello /hello/inon /db"
	DEFAULT_POST_ENDPOINTS="/echo /query"
	;;
v07-http)
	DEFAULT_FRAMEWORKS="gogo fiber nethttp"
	DEFAULT_ENDPOINTS="/plain /hello/inon /json /middleware /async"
	DEFAULT_POST_ENDPOINTS=""
	;;
*)
	echo "unknown BENCH_ROUTE_SET: $BENCH_ROUTE_SET (want legacy or v07-http)" >&2
	exit 1
	;;
esac

DURATION="${DURATION:-15}"
THREADS="${THREADS:-1 2 4 8}"
CONN="${CONN:-500}"
ENDPOINTS="${ENDPOINTS:-$DEFAULT_ENDPOINTS}"
POST_ENDPOINTS="${POST_ENDPOINTS:-$DEFAULT_POST_ENDPOINTS}"
POST_LUA="${POST_LUA:-$REPO_ROOT/scripts/wrk_post.lua}"
POST_LUA_QUERY="${POST_LUA_QUERY:-$REPO_ROOT/scripts/wrk_post_db.lua}"
FRAMEWORKS="${FRAMEWORKS:-$DEFAULT_FRAMEWORKS}"
MODES="${MODES:-single multi}"
WARMUP="${WARMUP:-2}"
RESULTS_DIR="${RESULTS_DIR:-benchmark/results}"

mkdir -p "$RESULTS_DIR"

if ! command -v wrk >/dev/null 2>&1; then
	echo "wrk is required. Install with: brew install wrk  (or apt install wrk)" >&2
	exit 1
fi

NCPU="$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu)"
if [ "$NCPU" -lt 4 ]; then
	DEFAULT_MULTI_WORKERS="$NCPU"
else
	DEFAULT_MULTI_WORKERS=4
fi
MULTI_WORKERS="${MULTI_WORKERS:-$DEFAULT_MULTI_WORKERS}"

SERVER_PID=""
SERVER_PORT=""
BENCH_BIN_DIR="${BENCH_BIN_DIR:-${TMPDIR:-/tmp}/gogo-bench-bin}"

# kill_tree walks down the descendant tree from a PID and signals each one.
# Used because macOS lacks `setsid`, so we can't rely on process groups.
kill_tree() {
	local pid="$1"
	local sig="${2:-TERM}"
	local kids
	kids="$(pgrep -P "$pid" 2>/dev/null || true)"
	for k in $kids; do
		kill_tree "$k" "$sig"
	done
	kill -"$sig" "$pid" 2>/dev/null || true
}

cleanup() {
	if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
		kill_tree "$SERVER_PID" TERM
		sleep 1
		kill_tree "$SERVER_PID" KILL
	fi
	if [ -n "$SERVER_PORT" ]; then
		local pids
		pids="$(lsof -tiTCP:"$SERVER_PORT" -sTCP:LISTEN 2>/dev/null || true)"
		for p in $pids; do
			kill_tree "$p" TERM
		done
		if [ -n "$pids" ]; then
			sleep 1
			for p in $pids; do
				kill_tree "$p" KILL
			done
		fi
	fi
	SERVER_PID=""
	SERVER_PORT=""
}
trap cleanup EXIT INT TERM

wait_port() {
	port="$1"
	for _ in $(seq 1 50); do
		if (echo >/dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.2
	done
	echo "server on port $port did not come up" >&2
	return 1
}

port_is_open() {
	port="$1"
	(echo >/dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1
}

build_go_binary() {
	name="$1"
	tags="$2"
	pkg="$3"
	mkdir -p "$BENCH_BIN_DIR"
	if [ -n "$tags" ]; then
		( cd benchmark && go build -tags "$tags" -o "$BENCH_BIN_DIR/$name" "$pkg" )
	else
		( cd benchmark && go build -o "$BENCH_BIN_DIR/$name" "$pkg" )
	fi
}

start_server() {
	fw="$1"
	mode="$2"
	case "$fw" in
	gogo) PORT=3002 ;;
	fiber) PORT=3004 ;;
	nethttp) PORT=3001 ;;
	uwsjs) PORT=3003 ;;
	bun) PORT=3005 ;;
	actix) PORT=3007 ;;
	*)
		echo "unknown framework: $fw" >&2
		return 1
		;;
	esac
	if port_is_open "$PORT"; then
		echo "port $PORT is already in use before starting $fw/$mode" >&2
		return 1
	fi
	SERVER_PORT="$PORT"
	case "$fw:$mode" in
	gogo:single)
		build_go_binary gogo-bench gogo ./gogo
		( GOGO_CORES=1 GOGO_WORKERS="${GOGO_WORKERS:-1}" "$BENCH_BIN_DIR/gogo-bench" >/tmp/bench-gogo.log 2>&1 ) &
		SERVER_PID=$!
		;;
	gogo:multi)
		build_go_binary gogo-bench gogo ./gogo
		( GOMAXPROCS="$MULTI_WORKERS" GOGO_CORES="$MULTI_WORKERS" GOGO_WORKERS="${GOGO_WORKERS:-$MULTI_WORKERS}" "$BENCH_BIN_DIR/gogo-bench" >/tmp/bench-gogo.log 2>&1 ) &
		SERVER_PID=$!
		;;
	fiber:single)
		build_go_binary fiber-bench "" ./fiber
		( GOMAXPROCS=1 FIBER_PREFORK=0 "$BENCH_BIN_DIR/fiber-bench" >/tmp/bench-fiber.log 2>&1 ) &
		SERVER_PID=$!
		;;
	fiber:multi)
		build_go_binary fiber-bench "" ./fiber
		( GOMAXPROCS="$MULTI_WORKERS" FIBER_PREFORK=1 "$BENCH_BIN_DIR/fiber-bench" >/tmp/bench-fiber.log 2>&1 ) &
		SERVER_PID=$!
		;;
	nethttp:single)
		build_go_binary nethttp-bench "" ./nethttp
		( GOMAXPROCS=1 "$BENCH_BIN_DIR/nethttp-bench" >/tmp/bench-nethttp.log 2>&1 ) &
		SERVER_PID=$!
		;;
	nethttp:multi)
		build_go_binary nethttp-bench "" ./nethttp
		( GOMAXPROCS="$MULTI_WORKERS" "$BENCH_BIN_DIR/nethttp-bench" >/tmp/bench-nethttp.log 2>&1 ) &
		SERVER_PID=$!
		;;
	uwsjs:single)
		( NODE_WORKERS=1 node benchmark/node-uwebsockets/server.cjs >/tmp/bench-uwsjs.log 2>&1 ) &
		SERVER_PID=$!
		;;
	uwsjs:multi)
		( NODE_WORKERS="$MULTI_WORKERS" node benchmark/node-uwebsockets/server.cjs >/tmp/bench-uwsjs.log 2>&1 ) &
		SERVER_PID=$!
		;;
	bun:single)
		( BUN_WORKERS=1 bun run benchmark/bun-elysia/server.ts >/tmp/bench-bun.log 2>&1 ) &
		SERVER_PID=$!
		;;
	bun:multi)
		( BUN_WORKERS="$MULTI_WORKERS" bun run benchmark/bun-elysia/server.ts >/tmp/bench-bun.log 2>&1 ) &
		SERVER_PID=$!
		;;
	actix:single)
		# Release binary must be pre-built: cargo build --release --manifest-path benchmark/actix/Cargo.toml
		( ACTIX_WORKERS=1 PORT="$PORT" benchmark/actix/target/release/actix-bench >/tmp/bench-actix.log 2>&1 ) &
		SERVER_PID=$!
		;;
	actix:multi)
		( ACTIX_WORKERS="$MULTI_WORKERS" PORT="$PORT" benchmark/actix/target/release/actix-bench >/tmp/bench-actix.log 2>&1 ) &
		SERVER_PID=$!
		;;
	*)
		echo "unknown framework/mode: $fw/$mode" >&2
		return 1
		;;
	esac
	if ! wait_port "$PORT"; then
		echo "---- server log ($fw $mode) ----" >&2
		tail -n 40 /tmp/bench-"$fw".log >&2 || true
		cleanup
		return 1
	fi
}

bench_one() {
	fw="$1"
	mode="$2"
	port="$3"
	endpoint="$4"
	threads="$5"
	post="${6:-0}"
	log="$RESULTS_DIR/wrk-${fw}-${mode}.log"

	method="GET"
	wrk_extra=()
	if [ "$post" = 1 ]; then
		method="POST"
		# Pick the right lua per endpoint: /query sends a plain
		# integer body so each server can do an apples-to-apples
		# strconv-then-DB-lookup; everything else uses the generic
		# 50-byte JSON echo body.
		if [ "$endpoint" = "/query" ]; then
			wrk_extra=(-s "$POST_LUA_QUERY")
		else
			wrk_extra=(-s "$POST_LUA")
		fi
	fi

	header="== $fw [$mode] $method $endpoint  t=$threads c=$CONN d=${DURATION}s =="
	echo
	echo "$header"
	echo "$header" >> "$log"

	# warmup
	if [ "$WARMUP" -gt 0 ]; then
		if [ "$post" = 1 ]; then
			wrk -t1 -c10 -d"${WARMUP}s" "${wrk_extra[@]}" "http://127.0.0.1:$port$endpoint" >/dev/null 2>&1 || true
		else
			wrk -t1 -c10 -d"${WARMUP}s" "http://127.0.0.1:$port$endpoint" >/dev/null 2>&1 || true
		fi
	fi

	if [ "$post" = 1 ]; then
		wrk -t"$threads" -c"$CONN" -d"${DURATION}s" --latency "${wrk_extra[@]}" "http://127.0.0.1:$port$endpoint" | tee -a "$log"
	else
		wrk -t"$threads" -c"$CONN" -d"${DURATION}s" --latency "http://127.0.0.1:$port$endpoint" | tee -a "$log"
	fi
}

for fw in $FRAMEWORKS; do
	for mode in $MODES; do
		echo
		echo "############ $fw [$mode] ############"
		if ! start_server "$fw" "$mode"; then
			echo "skip $fw/$mode (failed to start)" >&2
			continue
		fi
		# Settle.
		sleep 1
		for endpoint in $ENDPOINTS; do
			for t in $THREADS; do
				bench_one "$fw" "$mode" "$PORT" "$endpoint" "$t" 0
			done
		done
		for endpoint in $POST_ENDPOINTS; do
			for t in $THREADS; do
				bench_one "$fw" "$mode" "$PORT" "$endpoint" "$t" 1
			done
		done
		cleanup
		sleep 1
	done
done

echo
echo "Done. Results in $RESULTS_DIR/wrk-<fw>-<mode>.log"
