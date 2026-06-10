# Package Docs Audit

This v0.9 docs pass checked the README, root package docs from `doc.go`, and
all example packages against the current public API.

## Commands

```sh
go doc github.com/Snocko-main/gogo
GOFLAGS=-tags=gogo go doc github.com/Snocko-main/gogo
go test ./examples/...
CGO_ENABLED=1 go test -tags gogo ./examples/...
scripts/check_readme_examples.sh
git diff --check
```

## Findings

- README runnable `package main` snippets and every `examples/...` package
  compile in normal and native example test modes.
- Root package docs render through `go doc` without missing or broken package
  documentation.
- The README table of contents was missing the top-level Examples and C++
  bridge sections.
- The README `examples/authmw` summary still described an older bearer-auth
  focused demo instead of the current production-oriented middleware stack.
