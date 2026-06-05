# Third-party Notices

gogo vendors native source code from uWebSockets and uSockets in:

- `internal/native/uwebsockets`
- `internal/native/uwebsockets/uSockets`

Those vendored sources are distributed under the Apache License, Version 2.0.
Their upstream license texts are preserved at:

- `internal/native/uwebsockets/LICENSE`
- `internal/native/uwebsockets/uSockets/LICENSE`

gogo links against the host system's zlib when building the native server.
zlib is not vendored in this repository.
