#!/usr/bin/env sh
set -eu

usage() {
	cat <<EOF
usage: $0 patch|minor [LATEST_TAG]

Prints the next stable semver tag. If LATEST_TAG is omitted, the script reads
the latest stable vX.Y.Z tag from the local git repository.
EOF
}

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
	usage >&2
	exit 2
fi

bump="$1"
case "$bump" in
	patch|minor) ;;
	*)
		echo "unknown bump type: $bump" >&2
		usage >&2
		exit 2
		;;
esac

if [ "$#" -eq 2 ]; then
	latest="$2"
else
	latest="$(
		git tag --list 'v*' |
			grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' |
			sort -V |
			tail -n 1
	)"
fi

if [ -z "$latest" ]; then
	echo "no stable vX.Y.Z tag found" >&2
	exit 1
fi

case "$latest" in
	v[0-9]*.[0-9]*.[0-9]*) ;;
	*)
		echo "latest tag must look like vX.Y.Z: $latest" >&2
		exit 1
		;;
esac

version="${latest#v}"
major="${version%%.*}"
rest="${version#*.}"
minor="${rest%%.*}"
patch="${rest#*.}"

case "$major$minor$patch" in
	*[!0-9]*)
		echo "latest tag must be a stable numeric vX.Y.Z tag: $latest" >&2
		exit 1
		;;
esac

case "$bump" in
	patch)
		patch=$((patch + 1))
		;;
	minor)
		minor=$((minor + 1))
		patch=0
		;;
esac

echo "v$major.$minor.$patch"
