#!/usr/bin/env bash
#
# wrk benchmark matrix across gogo, fiber, node+uWebSockets.js, bun+elysia.
#
# Endpoints:   /hello, /hello/:name, /db
# Threads:     1, 2, 4, 8     (wrk -t)
# Connections: 500            (wrk -c)
# Modes:       single (1 worker) and multi (NumCPU workers)
#
# Each server is started, warmed, benchmarked, then killed before moving
# to the next. Per-run stdout (request rate, latency) is appended to
# benchmark/results/wrk-<framework>-<mode>.log.
#
# Env knobs:
#   DURATION  seconds per wrk run                 (default 15)
#   THREADS   space-separated thread counts       (default "1 2 4 8")
#   CONN      connections                         (default 500)
#   ENDPOINTS endpoints to hit                    (default "/hello /hello/inon /db")
#   FRAMEWORKS subset to run                      (default "gogo fiber uwsjs bun")
#   MODES     subset to run                       (default "single multi")
#   WARMUP    seconds of warmup hits before timing (default 2)
#   RESULTS_DIR  where to write logs              (default benchmark/results)
#
# Prereqs: wrk, go, node (+ npm install in benchmark/node-uwebsockets),
# bun (+ bun install in benchmark/bun-elysia).

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

DURATION="${DURATION:-15}"
THREADS="${THREADS:-1 2 4 8}"
CONN="${CONN:-500}"
ENDPOINTS="${ENDPOINTS:-/hello /hello/inon /db}"
FRAMEWORKS="${FRAMEWORKS:-gogo fiber nethttp uwsjs bun}"
MODES="${MODES:-single multi}"
WARMUP="${WARMUP:-2}"
RESULTS_DIR="${RESULTS_DIR:-benchmark/results}"

mkdir -p "$RESULTS_DIR"

if ! command -v wrk >/dev/null 2>&1; then
	echo "wrk is required. Install with: brew install wrk  (or apt install wrk)" >&2
	exit 1
fi

NCPU="$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu)"

SERVER_PID=""

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
	SERVER_PID=""
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

start_server() {
	fw="$1"
	mode="$2"
	# Each server is launched in a subshell so we have a clean parent PID
	# to walk children from (cleanup uses kill_tree to TERM the whole tree).
	case "$fw:$mode" in
	gogo:single)
		PORT=3002
		( GOGO_CORES=1 go run -tags gogo ./benchmark/gogo >/tmp/bench-gogo.log 2>&1 ) &
		SERVER_PID=$!
		;;
	gogo:multi)
		PORT=3002
		( GOGO_CORES="$NCPU" go run -tags gogo ./benchmark/gogo >/tmp/bench-gogo.log 2>&1 ) &
		SERVER_PID=$!
		;;
	fiber:single)
		PORT=3004
		( GOMAXPROCS=1 FIBER_PREFORK=0 go run ./benchmark/fiber >/tmp/bench-fiber.log 2>&1 ) &
		SERVER_PID=$!
		;;
	fiber:multi)
		PORT=3004
		( FIBER_PREFORK=1 go run ./benchmark/fiber >/tmp/bench-fiber.log 2>&1 ) &
		SERVER_PID=$!
		;;
	nethttp:single)
		PORT=3001
		( GOMAXPROCS=1 go run ./benchmark/nethttp >/tmp/bench-nethttp.log 2>&1 ) &
		SERVER_PID=$!
		;;
	nethttp:multi)
		PORT=3001
		( go run ./benchmark/nethttp >/tmp/bench-nethttp.log 2>&1 ) &
		SERVER_PID=$!
		;;
	uwsjs:single)
		PORT=3003
		( NODE_WORKERS=1 node benchmark/node-uwebsockets/server.cjs >/tmp/bench-uwsjs.log 2>&1 ) &
		SERVER_PID=$!
		;;
	uwsjs:multi)
		PORT=3003
		( NODE_WORKERS="$NCPU" node benchmark/node-uwebsockets/server.cjs >/tmp/bench-uwsjs.log 2>&1 ) &
		SERVER_PID=$!
		;;
	bun:single)
		PORT=3005
		( BUN_WORKERS=1 bun run benchmark/bun-elysia/server.ts >/tmp/bench-bun.log 2>&1 ) &
		SERVER_PID=$!
		;;
	bun:multi)
		PORT=3005
		( BUN_WORKERS="$NCPU" bun run benchmark/bun-elysia/server.ts >/tmp/bench-bun.log 2>&1 ) &
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
	log="$RESULTS_DIR/wrk-${fw}-${mode}.log"

	header="== $fw [$mode] $endpoint  t=$threads c=$CONN d=${DURATION}s =="
	echo
	echo "$header"
	echo "$header" >> "$log"

	# warmup
	if [ "$WARMUP" -gt 0 ]; then
		wrk -t1 -c10 -d"${WARMUP}s" "http://127.0.0.1:$port$endpoint" >/dev/null 2>&1 || true
	fi

	wrk -t"$threads" -c"$CONN" -d"${DURATION}s" --latency "http://127.0.0.1:$port$endpoint" | tee -a "$log"
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
				bench_one "$fw" "$mode" "$PORT" "$endpoint" "$t"
			done
		done
		cleanup
		sleep 1
	done
done

echo
echo "Done. Results in $RESULTS_DIR/wrk-<fw>-<mode>.log"
