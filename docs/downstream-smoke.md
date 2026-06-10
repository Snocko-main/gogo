# Downstream Smoke Builds

Use the downstream smoke build before release-candidate tags and final release
tags to prove a clean external module can install and build gogo with the native
`gogo` build tag.

## Procedure

From a clean checkout on the release candidate commit, run:

```sh
scripts/downstream_smoke_build.sh "$(git rev-parse HEAD)" v0.8.0 latest
```

The script creates temporary modules outside the repository, imports
`github.com/Snocko-main/gogo`, runs `go get` for each requested ref, prints the
resolved module version, and builds with:

```sh
CGO_ENABLED=1 go build -tags "gogo" .
```

Use `KEEP_TMP=1` when you need to inspect the generated downstream module after
a failure.

## v0.9 Evidence

Evidence collected on 2026-06-10 from branch `chore/v09-downstream-smoke`:

- `origin/main` / `HEAD`: `4f0605a920461a19170a92a69503e08f18abf9e2`
- Latest released tag: `v0.8.0`
- `v0.8.0` points at the same commit as `HEAD`
- Go toolchain: `go version go1.26.3 darwin/arm64`
- Command:
  `scripts/downstream_smoke_build.sh "$(git rev-parse HEAD)" v0.8.0 latest`
- Result: passed for the exact commit ref, `v0.8.0`, and `latest`

## v1.0.0-rc.1 Evidence

Evidence collected on 2026-06-10 from branch `chore/v1-release-smoke`:

- `origin/main` / `HEAD`: `8c36f0b21ac22569879072200184839c1bb30adf`
- Remote tag refs:
  - `v0.9.0`: `fe605091029955a83e33cc6f9f0592773a1f933a`
  - `v1.0.0-rc.1`: `340c88459de1af1a9f046bc023222e5ff50e3329`
- Dereferenced tag commit for both refs: `8c36f0b21ac22569879072200184839c1bb30adf`
- Go toolchain: `go version go1.26.3 darwin/arm64`
- Command:
  `scripts/downstream_smoke_build.sh v0.9.0 v1.0.0-rc.1 latest`
- Result: passed for `v0.9.0`, `v1.0.0-rc.1`, and `latest`

The `latest` ref resolved to the stable `v0.9.0` module version during this
run, so the release candidate was covered explicitly through `v1.0.0-rc.1`.

Output:

```text
== downstream smoke: github.com/Snocko-main/gogo@v0.9.0 ==
go: creating new go.mod: module example.com/gogo-downstream-smoke
go: downloading github.com/Snocko-main/gogo v0.9.0
go: added github.com/Snocko-main/gogo v0.9.0
resolved module: github.com/Snocko-main/gogo v0.9.0
build command: CGO_ENABLED=1 go build -tags "gogo" .
== downstream smoke: github.com/Snocko-main/gogo@v1.0.0-rc.1 ==
go: creating new go.mod: module example.com/gogo-downstream-smoke
go: downloading github.com/Snocko-main/gogo v1.0.0-rc.1
go: added github.com/Snocko-main/gogo v1.0.0-rc.1
resolved module: github.com/Snocko-main/gogo v1.0.0-rc.1
build command: CGO_ENABLED=1 go build -tags "gogo" .
== downstream smoke: github.com/Snocko-main/gogo@latest ==
go: creating new go.mod: module example.com/gogo-downstream-smoke
go: added github.com/Snocko-main/gogo v0.9.0
resolved module: github.com/Snocko-main/gogo v0.9.0
build command: CGO_ENABLED=1 go build -tags "gogo" .
downstream smoke build passed
```
