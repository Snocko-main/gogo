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
