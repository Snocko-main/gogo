# Release Checklist

Use this checklist before cutting any public tag.

## Current Release Candidate

`v0.9.0` and `v1.0.0-rc.1` are annotated tags cut on 2026-06-10 from commit
`8c36f0b21ac22569879072200184839c1bb30adf`. Re-run this checklist before
`v1.0.0` or any later release candidate.

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

## Stress and Race Validation

Run the v0.9 validation script before cutting a release candidate:

```sh
scripts/v09_release_validation.sh
```

The script runs the baseline suites plus targeted race and stress/count checks:

```sh
go test ./...
CGO_ENABLED=1 go test -tags gogo ./...
go test -race ./middleware -run '^(TestFastRequestIDGeneratorConcurrentPure)$' -count 1 -timeout 10m
CGO_ENABLED=1 go test -race -tags gogo . -run '^(TestConcurrentLoad|TestWebSocketPublishRaceWithClose|TestWebSocketPublishConcurrent)$' -count 1 -timeout 10m
CGO_ENABLED=1 go test -tags gogo . -run '^(TestStressShortBursts|TestConcurrentLoad|TestWebSocketPublishRaceWithClose|TestWebSocketPublishConcurrent)$' -count 3 -timeout 10m
```

Raise `GOGO_V09_RACE_COUNT`, `GOGO_V09_STRESS_COUNT`,
`GOGO_V09_RACE_TIMEOUT`, or `GOGO_V09_STRESS_TIMEOUT` in CI or on a release
host when deeper soak coverage is needed. If a platform cannot run native race
tests, record the toolchain error in the release notes and run the pure Go race
command plus the tagged native stress/count command as the supported fallback.

`CGO_ENABLED=1 go vet -tags gogo ./...` is not a release gate yet. It is a
known native baseline because `native_enabled.go` uses pointer arithmetic and
cgo shared-memory layouts that `go vet` reports as possible unsafe pointer
misuse. Treat native vet as advisory until those findings are either fixed or
suppressed with a narrow, documented pattern.

## Tagging

The GitHub Actions `Release` workflow runs automatically after a PR is merged
to `main`.

- `fix/*` branches create the next patch tag, for example `v1.0.1` to
  `v1.0.2`.
- `feat/*` and `feature/*` branches create the next minor tag, for example
  `v1.0.1` to `v1.1.0`.
- `doc/*`, `chore/*`, direct pushes, and other branch prefixes do not create a
  release.

Before tagging, the workflow runs the release gates, creates an annotated tag,
publishes a GitHub release, and smoke-builds the published tag and `latest`.
Release runs share one concurrency group with a `queue: max` policy, so a burst
of merges is processed one release at a time instead of replacing older pending
release runs.

Use the manual commands below only as a fallback when GitHub Actions cannot be
used or when a one-off prerelease tag is needed.

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
- Confirm `docs/install-build.md` still describes the required Go version, cgo
  build tag, C/C++ toolchain, C++20, and zlib requirements.
- Confirm `docs/production-examples.md` still maps the runnable examples to
  HTTP, middleware, WebSocket, graceful shutdown, and install/build coverage.
- If the release changes public APIs, add migration notes or examples.
