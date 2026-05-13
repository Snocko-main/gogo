#!/usr/bin/env sh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="$ROOT/uwebsockets/third_party/uWebSockets"

if [ ! -d "$TARGET/.git" ]; then
	mkdir -p "$(dirname "$TARGET")"
	git clone --recursive https://github.com/uNetworking/uWebSockets.git "$TARGET"
else
	git -C "$TARGET" submodule update --init --recursive
fi

make -C "$TARGET/uSockets"
