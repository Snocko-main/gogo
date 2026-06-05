# Release Checklist

Use this checklist before cutting any public tag.

## Before Tagging

- Confirm `ROAD_TO_V1.md` reflects the intended release scope.
- Confirm `CHANGELOG.md` has a dated entry for the release.
- Confirm the repository has an explicit root `LICENSE` before presenting the
  project as open source.
- Confirm `SECURITY.md` lists the release line as supported or unsupported.
- Confirm native source tracking is clean:
  `scripts/check_tracked_native_sources.sh`.
- Confirm downstream install behavior from a clean module:
  `scripts/downstream_smoke_build.sh <sha> vX.Y.Z latest`.

## Required Checks

- `go test ./...`
- `CGO_ENABLED=1 go test -tags gogo ./...`
- `go vet ./...`
- `govulncheck ./...`
- `govulncheck -tags gogo ./...`

`CGO_ENABLED=1 go vet -tags gogo ./...` is not a release gate yet. It is a
known native baseline because `native_enabled.go` uses pointer arithmetic and
cgo shared-memory layouts that `go vet` reports as possible unsafe pointer
misuse. Treat native vet as advisory until those findings are either fixed or
suppressed with a narrow, documented pattern.

## Tagging

1. Update `CHANGELOG.md` with the release date and any breaking changes.
2. Run the required checks locally or confirm the matching CI run is green.
3. Tag with an annotated tag:
   `git tag -a vX.Y.Z -m "vX.Y.Z"`.
4. Push the tag:
   `git push origin vX.Y.Z`.
5. Run the downstream smoke build against `@vX.Y.Z` and `@latest` after the
   tag is visible to the Go proxy.

## After Tagging

- Confirm GitHub release notes match `CHANGELOG.md`.
- Confirm no security checklist item became a release blocker.
- Confirm install docs still describe the required Go version, cgo build tag,
  C/C++ toolchain, C++20, and zlib requirements.
- If the release changes public APIs, add migration notes or examples.
