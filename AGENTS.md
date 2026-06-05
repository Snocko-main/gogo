# Agent Guide

This file is the working agreement for humans and agents contributing to this
repository.

## Source of Truth

- `ROAD_TO_V1.md` is the active roadmap.
- `ROADMAP.md` was removed and should not be recreated.
- Keep roadmap work small enough to review as separate PRs.

## Branch Names

Use one of these prefixes:

- `feat/...` for user-visible features or new framework APIs.
- `fix/...` for bugs, regressions, security fixes, or correctness changes.
- `chore/...` for CI, tooling, dependency, and maintenance work.
- `doc/...` for documentation-only work.

Do not use a `codex/` branch prefix.

## Parallel Work

- Use one branch per agent lane.
- Do not edit files outside your assigned ownership scope.
- Do not revert or overwrite changes made by other agents or by the user.
- If a task needs cross-lane edits, state the dependency in the PR and keep one
  agent as the integrator.
- Security-gate work should usually review and file issues, not make broad
  cross-lane edits.

## Agent Lanes

- Go API: `types.go`, `testing.go`, public Go docs, API tests.
- C++ / Native: `native_enabled.go`, `uws_bridge.*`, `usockets_vendor.c`,
  `internal/native/**`, build scripts.
- Middleware: `middleware/**`, `adapters/**`, middleware docs/tests.
- WebSocket: `ws_*.go`, `ws_hub*.go`, Redis WS adapter/tests.
- Docs / DX: `README.md`, `doc.go`, examples, docs.
- Release Engineering: `.github/**`, `scripts/**`, release docs.
- Security Gate: review across lanes; keep edits narrow unless assigned as
  integrator.

## Required Validation

For code changes, run at least:

```sh
go test ./...
CGO_ENABLED=1 go test -tags gogo ./...
```

For native, security, or release changes, also consider:

```sh
go vet ./...
CGO_ENABLED=1 go vet -tags gogo ./...
govulncheck ./...
govulncheck -tags gogo ./...
```

When testing downstream install behavior, use a clean temporary module that
imports `github.com/Snocko-main/gogo`, then build with:

```sh
go get github.com/Snocko-main/gogo@<sha-or-tag>
CGO_ENABLED=1 go build -tags gogo .
```

## Review Format

Reviews should lead with findings ordered by severity, then list open
questions, tests run, and residual risk. If there are no findings, say so
clearly and mention any remaining test gaps.
