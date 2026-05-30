#!/usr/bin/env sh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="$ROOT/.tools/uWebSockets"
VENDOR="$ROOT/internal/native/uwebsockets"
USOCKETS_PATCH="$ROOT/patches/uSockets-kqueue-ready-polls.patch"
UWEBSOCKETS_REPO="${UWEBSOCKETS_REPO:-https://github.com/uNetworking/uWebSockets.git}"
UWEBSOCKETS_REF="${UWEBSOCKETS_REF:-34809c2eb8210f15369b251c4405eb2f494a334e}"

if [ ! -d "$TARGET/.git" ]; then
	mkdir -p "$(dirname "$TARGET")"
	git clone "$UWEBSOCKETS_REPO" "$TARGET"
else
	git -C "$TARGET" fetch --tags origin "$UWEBSOCKETS_REF"
fi
git -C "$TARGET" checkout --detach "$UWEBSOCKETS_REF"
if [ -e "$TARGET/uSockets/.git" ]; then
	git -C "$TARGET/uSockets" reset --hard
	git -C "$TARGET/uSockets" clean -fd
fi
git -C "$TARGET" submodule update --init --recursive

if [ -f "$USOCKETS_PATCH" ]; then
	if git -C "$TARGET/uSockets" apply --check "$USOCKETS_PATCH" >/dev/null 2>&1; then
		git -C "$TARGET/uSockets" apply "$USOCKETS_PATCH"
	elif git -C "$TARGET/uSockets" apply --reverse --check "$USOCKETS_PATCH" >/dev/null 2>&1; then
		:
	else
		echo "failed to apply uSockets patch: $USOCKETS_PATCH" >&2
		exit 1
	fi
fi

rm -rf "$VENDOR/src" "$VENDOR/uSockets/src"
mkdir -p "$VENDOR/uSockets"
cp -R "$TARGET/src" "$VENDOR/src"
cp -R "$TARGET/uSockets/src" "$VENDOR/uSockets/src"
cp "$TARGET/LICENSE" "$VENDOR/LICENSE"
cp "$TARGET/uSockets/LICENSE" "$VENDOR/uSockets/LICENSE"
find "$VENDOR" -name .DS_Store -delete

echo "synced vendored uWebSockets sources to $VENDOR"
