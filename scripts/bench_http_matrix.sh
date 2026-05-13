#!/usr/bin/env sh
set -eu

for c in 1 10 50 100 500; do
	CONNECTIONS="$c" DURATION="${DURATION:-20}" PIPELINING="${PIPELINING:-1}" sh scripts/bench_http.sh
done
