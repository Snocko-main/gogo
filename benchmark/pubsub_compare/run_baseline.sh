#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
PORT="${PORT:-17100}"
CONNS="${CONNS:-1000}"
DURATION="${DURATION:-5s}"
BATCH="${BATCH:-100}"
BENCH_BIN_DIR="${BENCH_BIN_DIR:-${TMPDIR:-/tmp}/gogo-pubsub-baseline}"

SERVER_BIN="$BENCH_BIN_DIR/gogo_pubsub_server"
LOADGEN_BIN="$BENCH_BIN_DIR/pubsub_loadgen"
SERVER_LOG=""
SERVER_PID=""

cleanup() {
	if [ -n "$SERVER_PID" ]; then
		kill "$SERVER_PID" 2>/dev/null || true
		wait "$SERVER_PID" 2>/dev/null || true
		SERVER_PID=""
	fi
}
trap cleanup EXIT INT TERM

if ! command -v curl >/dev/null 2>&1; then
	echo "curl is required for the server readiness check" >&2
	exit 1
fi

mkdir -p "$BENCH_BIN_DIR"
cd "$ROOT"

echo "# WebSocket pub/sub baseline"
echo "# date_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "# root=$ROOT"
echo "# first_port=$PORT conns=$CONNS duration=$DURATION batch=$BATCH"
echo "# go=$(go version)"
echo "# uname=$(uname -a)"
echo "# ulimit_n=$(ulimit -n)"
echo

(cd "$ROOT/benchmark" && CGO_ENABLED="${CGO_ENABLED:-1}" go build -tags gogo -o "$SERVER_BIN" ./pubsub_compare)
(cd "$ROOT/benchmark" && go build -o "$LOADGEN_BIN" ./pubsub_compare/loadgen)

start_server() {
	label="$1"
	case_port="$2"
	SERVER_LOG="$BENCH_BIN_DIR/gogo_pubsub_server-$label.log"

	if command -v lsof >/dev/null 2>&1; then
		if lsof -nP -iTCP:"$case_port" -sTCP:LISTEN >/dev/null 2>&1; then
			echo "port $case_port is already listening; choose PORT=... or stop the stale server" >&2
			lsof -nP -iTCP:"$case_port" -sTCP:LISTEN >&2 || true
			exit 1
		fi
	fi

	PORT="$case_port" "$SERVER_BIN" >"$SERVER_LOG" 2>&1 &
	SERVER_PID="$!"

	ready=0
	for _ in 1 2 3 4 5 6 7 8 9 10; do
		if curl -fsS "http://127.0.0.1:$case_port/stat" >/dev/null 2>&1; then
			ready=1
			break
		fi
		if ! kill -0 "$SERVER_PID" 2>/dev/null; then
			echo "server exited before becoming ready; log follows:" >&2
			cat "$SERVER_LOG" >&2
			exit 1
		fi
		sleep 0.2
	done

	if [ "$ready" -ne 1 ]; then
		echo "server did not become ready on 127.0.0.1:$case_port; log follows:" >&2
		cat "$SERVER_LOG" >&2
		exit 1
	fi
}

case_index=0

run_case() {
	label="$1"
	mode="$2"
	case_batch="$3"
	case_port=$((PORT + case_index))
	case_index=$((case_index + 1))
	echo
	echo "## $label"
	start_server "$label" "$case_port"
	"$LOADGEN_BIN" \
		-target "127.0.0.1:$case_port" \
		-conns "$CONNS" \
		-duration "$DURATION" \
		-batch "$case_batch" \
		-mode "$mode" \
		-label "$label"
	echo "# server_log=$SERVER_LOG"
	cleanup
	sleep 0.5
}

run_case "publish-single" "publish" "1"
run_case "publish-loop-$BATCH" "publish" "$BATCH"
run_case "publishbatch-$BATCH" "publishbatch" "$BATCH"
