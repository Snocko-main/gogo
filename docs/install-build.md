# Install And Build Requirements

This page is the v1 install/build checklist for applications that consume
gogo from a clean module.

## Install

Add gogo to an application module with a pinned version for repeatable builds:

```sh
go get github.com/Snocko-main/gogo@vX.Y.Z
```

For preview or smoke testing the current default branch:

```sh
go get github.com/Snocko-main/gogo@latest
```

The module vendors the required uWebSockets/uSockets source under
`internal/native/uwebsockets`, so consumers do not need a `third_party`
checkout, a system uWebSockets install, or a prebuilt `uSockets.a`.

## Native Build Prerequisites

The real server is behind the `gogo` build tag. Native builds need:

- Go 1.24 or newer
- cgo enabled with `CGO_ENABLED=1`
- a C compiler
- a C++20-capable compiler, such as modern `clang` or `g++`
- zlib headers and library from the host system

Common package installs:

```sh
# macOS
xcode-select --install

# Debian / Ubuntu
sudo apt-get update
sudo apt-get install -y build-essential zlib1g-dev

# Fedora
sudo dnf install -y gcc gcc-c++ zlib-devel

# Alpine
sudo apk add build-base zlib-dev
```

Native builds are intended for macOS and Linux. Without `-tags gogo`, the
package still compiles a stub so normal Go tooling can inspect packages, but
`NewApp` returns a setup error instead of starting uWebSockets.

## Command Matrix

Use these commands from the application module:

```sh
CGO_ENABLED=1 go build -tags gogo ./...
CGO_ENABLED=1 go test -tags gogo ./...
CGO_ENABLED=1 go run -tags gogo .
```

For a production binary:

```sh
CGO_ENABLED=1 go build -tags gogo -o my-server .
```

For repository examples:

```sh
go test ./examples/...
CGO_ENABLED=1 go test -tags gogo ./examples/...
CGO_ENABLED=1 go run -tags gogo ./examples/hello
```

`scripts/check_readme_examples.sh` validates runnable README snippets and both
normal and native example package builds.

## Downstream Smoke Build

Before release-candidate tags and final release tags, prove install behavior
from a clean external module:

```sh
scripts/downstream_smoke_build.sh "$(git rev-parse HEAD)" vX.Y.Z latest
```

The script creates a temporary module, imports
`github.com/Snocko-main/gogo`, runs `go get` for each requested ref, and builds
with:

```sh
CGO_ENABLED=1 go build -tags "gogo" .
```

Run it after a release tag is visible to the Go proxy to catch packaging,
module, or vendored-native-source mistakes that local replaces can hide.

## Maintainer Notes

`scripts/bootstrap_uwebsockets.sh` is maintainer-only. It refreshes the
vendored uWebSockets source from the pinned upstream commit and reapplies the
repository patch. Application consumers should not run it to build or install
gogo.

If a build fails, first check:

- `go version` is 1.24 or newer
- `CGO_ENABLED=1` is set for the build
- the `gogo` build tag is present
- `clang++` or `g++` is available on `PATH`
- zlib headers are installed for the active compiler target
