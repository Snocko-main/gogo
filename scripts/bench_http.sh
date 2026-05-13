#!/usr/bin/env sh
set -eu

AUTOCANNON="${AUTOCANNON:-autocannon}"

if [ -x ".tools/npm/node_modules/.bin/autocannon" ]; then
	AUTOCANNON=".tools/npm/node_modules/.bin/autocannon"
fi

if ! command -v "$AUTOCANNON" >/dev/null 2>&1; then
	echo "autocannon is required. Install with: npm install -g autocannon" >&2
	exit 1
fi

DURATION="${DURATION:-30}"
CONNECTIONS="${CONNECTIONS:-100}"
PIPELINING="${PIPELINING:-1}"

run() {
	name="$1"
	url="$2"

	echo
	echo "== $name =="
	echo "url=$url connections=$CONNECTIONS duration=${DURATION}s pipelining=$PIPELINING"
	"$AUTOCANNON" \
		--connections "$CONNECTIONS" \
		--duration "$DURATION" \
		--pipelining "$PIPELINING" \
		"$url"
}

run "go net/http plain" "http://127.0.0.1:3001/plain"
run "uWebSockets-Go plain" "http://127.0.0.1:3002/plain"
run "uWebSockets.js plain" "http://127.0.0.1:3003/plain"

run "go net/http json" "http://127.0.0.1:3001/json"
run "uWebSockets-Go json" "http://127.0.0.1:3002/json"
run "uWebSockets.js json" "http://127.0.0.1:3003/json"

run "go net/http param" "http://127.0.0.1:3001/hello/inon"
run "uWebSockets-Go param" "http://127.0.0.1:3002/hello/inon"
run "uWebSockets.js param" "http://127.0.0.1:3003/hello/inon"
