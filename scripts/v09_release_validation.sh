#!/usr/bin/env sh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

GO="${GO:-go}"
RACE_COUNT="${GOGO_V09_RACE_COUNT:-1}"
STRESS_COUNT="${GOGO_V09_STRESS_COUNT:-3}"
RACE_TIMEOUT="${GOGO_V09_RACE_TIMEOUT:-10m}"
STRESS_TIMEOUT="${GOGO_V09_STRESS_TIMEOUT:-10m}"

PURE_RACE_RE='^(TestFastRequestIDGeneratorConcurrentPure)$'
NATIVE_RACE_RE='^(TestConcurrentLoad|TestWebSocketPublishRaceWithClose|TestWebSocketPublishConcurrent)$'
NATIVE_STRESS_RE='^(TestStressShortBursts|TestConcurrentLoad|TestWebSocketPublishRaceWithClose|TestWebSocketPublishConcurrent)$'

run() {
	printf '\n==> %s\n' "$*"
	"$@"
}

run "$GO" test ./...
run env CGO_ENABLED=1 "$GO" test -tags gogo ./...
run "$GO" test -race ./middleware -run "$PURE_RACE_RE" -count "$RACE_COUNT" -timeout "$RACE_TIMEOUT"
run env CGO_ENABLED=1 "$GO" test -race -tags gogo . -run "$NATIVE_RACE_RE" -count "$RACE_COUNT" -timeout "$RACE_TIMEOUT"
run env CGO_ENABLED=1 "$GO" test -tags gogo . -run "$NATIVE_STRESS_RE" -count "$STRESS_COUNT" -timeout "$STRESS_TIMEOUT"

echo "v0.9 release validation passed"
