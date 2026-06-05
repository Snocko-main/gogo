#!/usr/bin/env sh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
README="$ROOT/README.md"
GO="${GO:-go}"
TAGS="${GOGO_README_TAGS:-gogo}"

if ! command -v "$GO" >/dev/null 2>&1; then
	echo "go command not found: $GO" >&2
	exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
	echo "python3 is required" >&2
	exit 1
fi

tmp_base="${TMPDIR:-/tmp}/gogo-readme-examples.$$"
tmp="$tmp_base"
n=0
while ! mkdir "$tmp" 2>/dev/null; do
	n=$((n + 1))
	tmp="$tmp_base.$n"
done

cleanup() {
	if [ "${KEEP_TMP:-0}" = "1" ]; then
		echo "kept README example workspace: $tmp" >&2
	else
		rm -rf "$tmp"
	fi
}
trap cleanup EXIT HUP INT TERM

python3 - "$README" "$tmp" <<'PY'
from pathlib import Path
import re
import sys

readme = Path(sys.argv[1])
out = Path(sys.argv[2])
text = readme.read_text()

blocks = []
in_block = False
lang = ""
start = 0
buf = []
for lineno, line in enumerate(text.splitlines(), 1):
    if line.startswith("```"):
        if not in_block:
            in_block = True
            lang = line[3:].strip()
            start = lineno + 1
            buf = []
        else:
            blocks.append((lang, start, lineno - 1, "\n".join(buf)))
            in_block = False
        continue
    if in_block:
        buf.append(line)

go_blocks = [(start, end, body) for lang, start, end, body in blocks if lang == "go"]
runnable = []
for start, end, body in go_blocks:
    if re.search(r"(?m)^package\s+main\b", body):
        runnable.append((start, end, body))

if not runnable:
    raise SystemExit("README has no runnable package main Go examples")

manifest = out / "manifest"
with manifest.open("w") as mf:
    for i, (start, end, body) in enumerate(runnable, 1):
        d = out / f"readme-go-block-{i}-line-{start}"
        d.mkdir()
        (d / "main.go").write_text(body + "\n")
        mf.write(str(d) + "\n")

print(f"README Go fences: {len(go_blocks)}")
print(f"README runnable package main examples: {len(runnable)}")
print(f"README snippet Go fences documented as fragments: {len(go_blocks) - len(runnable)}")
PY

while IFS= read -r dir; do
	echo "== README example: $(basename "$dir") =="
	(
		cd "$dir"
		"$GO" mod init example.com/gogo-readme-example >/dev/null
		"$GO" mod edit -replace github.com/Snocko-main/gogo="$ROOT"
		"$GO" mod tidy
		CGO_ENABLED=1 "$GO" test -tags "$TAGS" .
	)
done < "$tmp/manifest"

echo "== examples packages: normal build =="
"$GO" test ./examples/...

echo "== examples packages: native build =="
CGO_ENABLED=1 "$GO" test -tags "$TAGS" ./examples/...

echo "README runnable examples compile"
