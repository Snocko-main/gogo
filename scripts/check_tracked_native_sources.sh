#!/usr/bin/env sh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

native_dir="internal/native/uwebsockets"

if ! command -v git >/dev/null 2>&1; then
	echo "git is required" >&2
	exit 1
fi

if [ ! -d "$native_dir" ]; then
	echo "missing native source directory: $native_dir" >&2
	exit 1
fi

tmp_base="${TMPDIR:-/tmp}/gogo-native-files.$$"
tmp="$tmp_base"
n=0
while ! mkdir "$tmp" 2>/dev/null; do
	n=$((n + 1))
	tmp="$tmp_base.$n"
done

cleanup() {
	rm -rf "$tmp"
}
trap cleanup EXIT HUP INT TERM

find "$native_dir" -type f ! -name .DS_Store | LC_ALL=C sort > "$tmp/files"

missing=0
while IFS= read -r path; do
	if ! git ls-files --error-unmatch "$path" >/dev/null 2>&1; then
		echo "native source is present but not tracked by git: $path" >&2
		missing=1
	fi
done < "$tmp/files"

if [ "$missing" -ne 0 ]; then
	exit 1
fi

required_files="
$native_dir/LICENSE
$native_dir/README.md
$native_dir/src/App.h
$native_dir/src/HttpContext.h
$native_dir/src/WebSocket.h
$native_dir/uSockets/LICENSE
$native_dir/uSockets/src/libusockets.h
$native_dir/uSockets/src/socket.c
$native_dir/uSockets/src/eventing/epoll_kqueue.c
"

for path in $required_files; do
	if ! git ls-files --error-unmatch "$path" >/dev/null 2>&1; then
		echo "required native source is not tracked by git: $path" >&2
		exit 1
	fi
done

if git ls-files 'third_party/**' | grep . >/dev/null 2>&1; then
	echo "tracked third_party files found; downstream builds must use module-tracked native sources" >&2
	git ls-files 'third_party/**' >&2
	exit 1
fi

if git grep -n 'third_party' -- \
	'*.go' '*.c' '*.cc' '*.cpp' '*.h' '*.hpp' '*.m' '*.mm' \
	go.mod .github/workflows >/dev/null 2>&1; then
	echo "build inputs reference third_party; downstream builds must not depend on local third_party paths" >&2
	git grep -n 'third_party' -- \
		'*.go' '*.c' '*.cc' '*.cpp' '*.h' '*.hpp' '*.m' '*.mm' \
		go.mod .github/workflows >&2
	exit 1
fi

echo "native source tracking check passed"
