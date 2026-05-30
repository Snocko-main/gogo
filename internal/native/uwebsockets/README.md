# Vendored uWebSockets

This directory contains the minimal uWebSockets/uSockets source needed by
gogo's native cgo build. It is committed into the Go module so downstream
users can build `-tags gogo` after `go get` without a `third_party` checkout or
prebuilt `uSockets.a`.

Refresh these files with:

```sh
sh scripts/bootstrap_uwebsockets.sh
```

The refresh script checks out the pinned upstream uWebSockets ref, initializes
uSockets, applies `patches/uSockets-kqueue-ready-polls.patch`, and copies only
the source and license files used by the module.
