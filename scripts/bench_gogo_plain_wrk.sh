#!/usr/bin/env bash
#
# Focused gogo /plain wrk bench.
#
# This avoids the full benchmark matrix and starts benchmark/gogo in
# GOGO_BENCH_PLAIN_ONLY mode, so sync/plain routes are measured without the
# async DB/file routes waking the shared worker pool.
#
# On Linux, set SERVER_CPUSET and WRK_CPUSET to keep the server and wrk from
# fighting over the same CPUs:
#
#   SERVER_CPUSET=0-3 WRK_CPUSET=4-7 MODE=multi WORKERS=4 \
#     THREADS="1 2 4" WRK_PROCESSES=1 ./scripts/bench_gogo_plain_wrk.sh
#
# For very small routes, one wrk process can bottleneck before four event
# loops saturate. Use WRK_PROCESSES=2 or more to run several wrk instances in
# parallel and sum their Requests/sec.
#
# Set SAMPLE_CPU=1 to write per-thread CPU samples next to the wrk log. On
# Linux this includes the CPU core (PSR) each gogo thread was running on.

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

MODE="${MODE:-multi}"
WORKERS="${WORKERS:-4}"
THREADS="${THREADS:-1 2 4 8}"
CONN="${CONN:-500}"
DURATION="${DURATION:-10}"
WARMUP="${WARMUP:-2}"
ROUTES="${ROUTES:-/plain /plain-static /health}"
PORT="${PORT:-3002}"
RESULTS_DIR="${RESULTS_DIR:-benchmark/results/plain-wrk}"
BINARY="${BINARY:-/tmp/gogo-plain-bench-bin}"
WRK_PROCESSES="${WRK_PROCESSES:-1}"
SERVER_CPUSET="${SERVER_CPUSET:-}"
WRK_CPUSET="${WRK_CPUSET:-}"
SAMPLE_CPU="${SAMPLE_CPU:-0}"
CPU_SAMPLE_INTERVAL="${CPU_SAMPLE_INTERVAL:-1}"

mkdir -p "$RESULTS_DIR"

if ! command -v wrk >/dev/null 2>&1; then
	echo "wrk is required. Install with: brew install wrk  (or apt install wrk)" >&2
	exit 1
fi

if [ "$WRK_PROCESSES" -lt 1 ]; then
	echo "WRK_PROCESSES must be >= 1" >&2
	exit 1
fi

if [ "$CONN" -lt "$WRK_PROCESSES" ]; then
	echo "CONN must be >= WRK_PROCESSES" >&2
	exit 1
fi

ulimit -n 1048576 >/dev/null 2>&1 || true

run_with_affinity() {
	local cpuset="$1"
	shift
	if [ -n "$cpuset" ] && command -v taskset >/dev/null 2>&1; then
		taskset -c "$cpuset" "$@"
	else
		if [ -n "$cpuset" ]; then
			echo "warning: taskset not found; ignoring CPU set $cpuset" >&2
		fi
		"$@"
	fi
}

port_is_open() {
	local port="$1"
	(echo >/dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1
}

wait_port() {
	local port="$1"
	for _ in $(seq 1 50); do
		if port_is_open "$port"; then
			return 0
		fi
		sleep 0.2
	done
	return 1
}

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

pid_tree_csv() {
	local pid="$1"
	local kids
	echo "$pid"
	kids="$(pgrep -P "$pid" 2>/dev/null || true)"
	for k in $kids; do
		pid_tree_csv "$k"
	done
}

sample_thread_cpu() {
	local outfile="$1"
	{
		echo "== gogo thread CPU samples pid=$SERVER_PID interval=${CPU_SAMPLE_INTERVAL}s =="
		while kill -0 "$SERVER_PID" 2>/dev/null; do
			local pids
			pids="$(pid_tree_csv "$SERVER_PID" | paste -sd, -)"
			date '+-- %Y-%m-%d %H:%M:%S --'
			case "$(uname -s)" in
			Linux)
				ps -L -p "$pids" -o pid,tid,psr,pcpu,comm
				;;
			Darwin)
				ps -M -p "$pids" -o pid,pcpu,comm
				;;
			*)
				ps -p "$pids" -o pid,pcpu,comm
				;;
			esac
			sleep "$CPU_SAMPLE_INTERVAL"
		done
	} >"$outfile" 2>&1
}

SERVER_PID=""
cleanup() {
	if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
		kill_tree "$SERVER_PID" TERM
		sleep 1
		kill_tree "$SERVER_PID" KILL
	fi
	SERVER_PID=""
}
trap cleanup EXIT INT TERM

echo "building $BINARY"
go build -tags gogo -o "$BINARY" ./benchmark/gogo

if port_is_open "$PORT"; then
	echo "port $PORT is already in use" >&2
	exit 1
fi

case "$MODE" in
single)
	server_env=(GOGO_BENCH_PLAIN_ONLY=1 GOGO_CORES=1 GOMAXPROCS=1)
	;;
multi)
	server_env=(GOGO_BENCH_PLAIN_ONLY=1 GOGO_CORES="$WORKERS" GOMAXPROCS="$WORKERS")
	;;
*)
	echo "MODE must be single or multi, got $MODE" >&2
	exit 1
	;;
esac

echo "starting gogo $MODE on :$PORT workers=$WORKERS server_cpuset=${SERVER_CPUSET:-none}"
(
	for kv in "${server_env[@]}"; do
		export "$kv"
	done
	run_with_affinity "$SERVER_CPUSET" "$BINARY"
) >/tmp/bench-gogo-plain.log 2>&1 &
SERVER_PID=$!

if ! wait_port "$PORT"; then
	echo "server on port $PORT did not come up" >&2
	tail -n 40 /tmp/bench-gogo-plain.log >&2 || true
	exit 1
fi
sleep 1
tail -n 5 /tmp/bench-gogo-plain.log || true

bench_one() {
	local route="$1"
	local threads="$2"
	local conn_per_proc=$((CONN / WRK_PROCESSES))
	local remainder=$((CONN % WRK_PROCESSES))
	local log="$RESULTS_DIR/wrk-gogo-${MODE}-plain.log"
	local tmpdir="$RESULTS_DIR/tmp-${route//\//_}-t${threads}-$$"
	local cpulog="$RESULTS_DIR/cpu-gogo-${MODE}-${route//\//_}-t${threads}.log"
	local sampler_pid=""

	mkdir -p "$tmpdir"
	echo
	echo "== gogo [$MODE] GET $route t=$threads c=$CONN d=${DURATION}s wrk_processes=$WRK_PROCESSES =="
	echo "== gogo [$MODE] GET $route t=$threads c=$CONN d=${DURATION}s wrk_processes=$WRK_PROCESSES ==" >>"$log"

	if [ "$WARMUP" -gt 0 ]; then
		run_with_affinity "$WRK_CPUSET" wrk -t1 -c10 -d"${WARMUP}s" "http://127.0.0.1:$PORT$route" >/dev/null 2>&1 || true
	fi

	if [ "$SAMPLE_CPU" = "1" ]; then
		sample_thread_cpu "$cpulog" &
		sampler_pid="$!"
	fi

	pids=""
	for i in $(seq 1 "$WRK_PROCESSES"); do
		local c="$conn_per_proc"
		if [ "$i" -le "$remainder" ]; then
			c=$((c + 1))
		fi
		(
			run_with_affinity "$WRK_CPUSET" wrk -t"$threads" -c"$c" -d"${DURATION}s" --latency "http://127.0.0.1:$PORT$route"
		) >"$tmpdir/wrk-$i.log" 2>&1 &
		pids="$pids $!"
	done

	local failed=0
	for pid in $pids; do
		if ! wait "$pid"; then
			failed=1
		fi
	done
	if [ -n "$sampler_pid" ]; then
		kill "$sampler_pid" 2>/dev/null || true
		wait "$sampler_pid" 2>/dev/null || true
		echo "CPU samples: $cpulog" | tee -a "$log"
	fi

	cat "$tmpdir"/wrk-*.log | tee -a "$log"
	awk '
		/Requests\/sec:/ { r += $2 }
		/Transfer\/sec:/ { transfer[++n] = $2 " " $3 }
		END {
			if (r > 0) {
				printf("Aggregate Requests/sec: %.2f\n", r)
			}
		}
	' "$tmpdir"/wrk-*.log | tee -a "$log"

	rm -rf "$tmpdir"
	return "$failed"
}

for route in $ROUTES; do
	for t in $THREADS; do
		bench_one "$route" "$t"
	done
done

echo
echo "Done. Results in $RESULTS_DIR/wrk-gogo-${MODE}-plain.log"
