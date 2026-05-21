#!/usr/bin/env sh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="$ROOT/third_party/uWebSockets"
USOCKETS_PATCH="$ROOT/patches/uSockets-kqueue-ready-polls.patch"

if [ ! -d "$TARGET/.git" ]; then
	mkdir -p "$(dirname "$TARGET")"
	git clone --recursive https://github.com/uNetworking/uWebSockets.git "$TARGET"
else
	git -C "$TARGET" submodule update --init --recursive
fi

if [ -f "$USOCKETS_PATCH" ]; then
	if git -C "$TARGET/uSockets" apply --check "$USOCKETS_PATCH" >/dev/null 2>&1; then
		git -C "$TARGET/uSockets" apply "$USOCKETS_PATCH"
	elif grep -q "internal_cb->cb = 0;" "$TARGET/uSockets/src/eventing/epoll_kqueue.c" &&
		grep -q "if (cb->cb == 0)" "$TARGET/uSockets/src/loop.c"; then
		:
	else
		echo "failed to apply uSockets patch: $USOCKETS_PATCH" >&2
		exit 1
	fi
fi

make -C "$TARGET/uSockets"
