#!/usr/bin/env sh
set -eu

GO="${GO:-go}"
MODULE="${GOGO_SMOKE_MODULE:-github.com/Snocko-main/gogo}"
TAGS="${GOGO_SMOKE_TAGS:-gogo}"
LOCAL_PATH=""

usage() {
	cat <<EOF
usage: $0 [--local PATH] [REF ...]

Builds a clean downstream module that imports $MODULE with:
  CGO_ENABLED=1 $GO build -tags "$TAGS" .

Release-prep examples:
  $0 \$(git rev-parse HEAD) v0.8.0 latest
  GOGO_SMOKE_REFS="\$(git rev-parse HEAD) v0.8.0 latest" $0

Use --local PATH in pull-request CI to smoke-build the checked-out workspace.

Environment:
  GOGO_SMOKE_MODULE   module path to import; defaults to $MODULE
  GOGO_SMOKE_TAGS     build tags; defaults to "$TAGS"
  GOGO_SMOKE_REFS     space-separated refs used when no REF args are given
  KEEP_TMP=1          keep the temporary downstream module for inspection
EOF
}

while [ "$#" -gt 0 ]; do
	case "$1" in
		--help|-h)
			usage
			exit 0
			;;
		--local)
			if [ "$#" -lt 2 ]; then
				echo "--local requires a path" >&2
				exit 1
			fi
			LOCAL_PATH="$2"
			shift 2
			;;
		--)
			shift
			break
			;;
		-*)
			echo "unknown option: $1" >&2
			usage >&2
			exit 1
			;;
		*)
			break
			;;
	esac
done

if ! command -v "$GO" >/dev/null 2>&1; then
	echo "go command not found: $GO" >&2
	exit 1
fi

tmp_base="${TMPDIR:-/tmp}/gogo-downstream-smoke.$$"
tmp_root="$tmp_base"
n=0
while ! mkdir "$tmp_root" 2>/dev/null; do
	n=$((n + 1))
	tmp_root="$tmp_base.$n"
done

cleanup() {
	if [ "${KEEP_TMP:-0}" = "1" ]; then
		echo "kept smoke workspace: $tmp_root" >&2
	else
		rm -rf "$tmp_root"
	fi
}
trap cleanup EXIT HUP INT TERM

write_main() {
	dir="$1"
	{
		printf '%s\n' 'package main'
		printf '%s\n' ''
		printf '%s\n' 'import ('
		printf '%s\n' '	"log"'
		printf '	gogo "%s"\n' "$MODULE"
		printf '%s\n' ')'
		printf '%s\n' ''
		printf '%s\n' 'func main() {'
		printf '%s\n' '	app, err := gogo.NewApp()'
		printf '%s\n' '	if err != nil {'
		printf '%s\n' '		log.Fatal(err)'
		printf '%s\n' '	}'
		printf '%s\n' '	defer app.Close()'
		printf '%s\n' '	app.Get("/health", gogo.Reply{'
		printf '%s\n' '		Status:      200,'
		printf '%s\n' '		ContentType: "text/plain; charset=utf-8",'
		printf '%s\n' '		Body:        "ok\n",'
		printf '%s\n' '	})'
		printf '%s\n' '}'
	} > "$dir/main.go"
}

start_smoke() {
	label="$1"
	dir="$2"

	echo "== downstream smoke: $label =="
	cd "$dir"
	"$GO" mod init example.com/gogo-downstream-smoke >/dev/null
	write_main "$dir"
}

finish_smoke() {
	printf 'resolved module: '
	"$GO" list -m "$MODULE"
	echo "build command: CGO_ENABLED=1 $GO build -tags \"$TAGS\" ."
	CGO_ENABLED=1 "$GO" build -tags "$TAGS" .
}

if [ -n "$LOCAL_PATH" ]; then
	case "$LOCAL_PATH" in
		/*) ;;
		*) LOCAL_PATH="$(cd "$LOCAL_PATH" && pwd)" ;;
	esac
	if [ ! -d "$LOCAL_PATH" ]; then
		echo "local path does not exist: $LOCAL_PATH" >&2
		exit 1
	fi
	dir="$tmp_root/local"
	mkdir "$dir"
	(
		start_smoke "local checkout $LOCAL_PATH" "$dir"
		"$GO" mod edit -require "$MODULE@v0.0.0"
		"$GO" mod edit -replace "$MODULE=$LOCAL_PATH"
		"$GO" mod tidy
		finish_smoke
	)
fi

refs="$*"
if [ -z "$refs" ] && [ -n "${GOGO_SMOKE_REFS:-}" ]; then
	refs="$GOGO_SMOKE_REFS"
fi

if [ -z "$LOCAL_PATH" ] && [ -z "$refs" ]; then
	usage >&2
	exit 2
fi

i=0
for ref in $refs; do
	i=$((i + 1))
	dir="$tmp_root/ref-$i"
	mkdir "$dir"
	(
		start_smoke "$MODULE@$ref" "$dir"
		"$GO" get "$MODULE@$ref"
		finish_smoke
	)
done

echo "downstream smoke build passed"
