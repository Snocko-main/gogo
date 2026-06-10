# File Serving And Upload Safety

This guide covers safe use of `Response.SendFile`, `Response.Download`, and
multipart file saves in production routes.

The short version: treat file paths and filenames as application policy.
gogo streams files efficiently once you choose a path, but it intentionally
does not decide which paths a client is allowed to reach.

## SendFile And Download Contract

`Response.SendFile(req, path)` opens exactly the path you pass to it. It adds
content type detection, `Last-Modified`, a weak ETag, conditional requests,
single byte-range responses, a size cap, and streamed backpressure. It returns
an error without touching the response when the path is missing, unreadable, a
directory, or larger than `MaxSendFileBytes`.

`Response.Download(req, path, filename)` is the same file streaming path with
`Content-Disposition: attachment`. If `filename` is empty, gogo uses
`filepath.Base(path)`. The header builder drops control characters, escapes
quotes and backslashes in the quoted `filename` parameter, and emits a
`filename*` parameter for non-ASCII names.

These helpers do not provide:

- route authorization or object ownership checks
- path traversal protection for request-derived paths
- symlink, hardlink, or user-writable directory policy
- hidden-file or dotfile filtering
- extension, MIME type, or content validation
- cache-control policy
- safe download names beyond header encoding

Put those checks in the route before calling `SendFile` or `Download`.

## Root Request Paths

Never concatenate a URL segment directly onto a filesystem directory. Root the
request path, clean it, and then verify the candidate path still lives below
the root.

```go
func safePublicPath(root, userPath string) (string, error) {
    cleanRoot, err := filepath.Abs(root)
    if err != nil {
        return "", err
    }

    cleaned := filepath.Clean("/" + userPath)
    candidate, err := filepath.Abs(filepath.Join(cleanRoot, cleaned))
    if err != nil {
        return "", err
    }

    rel, err := filepath.Rel(cleanRoot, candidate)
    if err != nil {
        return "", err
    }
    if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
        return "", os.ErrPermission
    }

    return candidate, nil
}
```

Use this only for routes that are meant to expose nested public files. If the
route serves named objects such as avatars, invoices, reports, or exports,
prefer an allow-list:

```go
var publicFiles = map[string]string{
    "terms":   "terms-2026-01.pdf",
    "privacy": "privacy-2026-01.pdf",
}

func allowListedPath(root, key string) (string, error) {
    name, ok := publicFiles[key]
    if !ok {
        return "", os.ErrNotExist
    }
    return safePublicPath(root, name)
}
```

Allow-lists are less flexible, but they avoid turning URL syntax into
filesystem policy.

## Symlinks And Writable Roots

The rooted join above blocks `../` traversal. It does not make a symlink safe.
`os.Open`, and therefore `SendFile` and `Download`, follows symlinks.

For a static asset tree that is created during deploy and is not writable by
the app or users, the usual policy is:

- keep the root outside upload, cache, and temp directories
- make the root read-only to the server process when possible
- review or disallow symlinks during build/deploy

For a tree that may contain symlinks, resolve both the root and candidate path
after cleaning and then check containment again:

```go
func safePublicPathNoSymlinkEscape(root, userPath string) (string, error) {
    candidate, err := safePublicPath(root, userPath)
    if err != nil {
        return "", err
    }

    cleanRoot, err := filepath.Abs(root)
    if err != nil {
        return "", err
    }
    realRoot, err := filepath.EvalSymlinks(cleanRoot)
    if err != nil {
        return "", err
    }
    realCandidate, err := filepath.EvalSymlinks(candidate)
    if err != nil {
        return "", err
    }

    rel, err := filepath.Rel(realRoot, realCandidate)
    if err != nil {
        return "", err
    }
    if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
        return "", os.ErrPermission
    }

    return realCandidate, nil
}
```

Do not rely on symlink resolution alone when attackers can write inside the
served tree. They may be able to swap paths between validation and open. For
untrusted writes, keep uploads in a separate private directory, generate stored
filenames yourself, and serve them through an object lookup that checks
authorization before resolving the stored path.

## Download Filenames

`Download` safely encodes the `Content-Disposition` header, but the route still
chooses the suggested filename. Do not pass a raw path parameter as the
download name.

Good download names are leaf names, not paths. They should not contain slashes,
backslashes, drive prefixes, control characters, or application-specific
separators that a downstream client might interpret.

```go
func safeDownloadName(name string) string {
    name = filepath.Base(name)
    name = strings.Map(func(r rune) rune {
        switch {
        case r < 0x20 || r == 0x7f:
            return -1
        case r == '/' || r == '\\' || r == ':':
            return '_'
        default:
            return r
        }
    }, name)
    name = strings.TrimSpace(name)
    if name == "" || name == "." || name == ".." {
        return "download"
    }
    return name
}
```

When the download is for a known object, prefer a server-side display name such
as `invoice-1234.pdf` over the original client filename.

## SendFile Limits And Backpressure

File streaming has three process-wide knobs:

- `SetMaxSendFileBytes(n)` caps the largest file `SendFile` and `Download`
  will serve. The default is 100 MiB. `gogo.NoSendFileLimit` disables the cap
  and should be reserved for trusted, authorized file routes that are bounded
  by another layer.
- `SetSendFileChunkBytes(n)` sets the disk-read and write buffer size for each
  streaming iteration. Values at or below zero restore the default 64 KiB.
- `SetSendFileBackpressureBytes(n)` sets the per-socket buffered-byte
  high-water mark before file streaming waits for drain. Zero restores the
  default 1 MiB.

Configure these at startup. The setters are atomic for runtime changes, but
keeping file-serving limits stable makes production behavior easier to reason
about.

The rough memory budget for concurrent file responses is:

```text
concurrent file responses * (SendFileChunkBytes + SendFileBackpressureBytes)
```

That is separate from kernel buffers, filesystem cache, and any application
metadata you load before serving the file.

## Multipart Save Safety

Multipart parsing is bounded by both the request body limit and per-part
limits. For body-async routes, the body limit is the second argument:

```go
app.PostAsync("/upload", 50<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
    err := req.MultipartWithOptions(gogo.MultipartOptions{
        MaxPartBytes: 8 << 20,
    }, func(p *gogo.MultipartPart) error {
        // ...
        return nil
    })
    if err != nil {
        res.Send(400, "text/plain", "bad multipart\n")
        return
    }
    res.Send(200, "text/plain", "ok\n")
})
```

`MultipartOptions{MaxPartBytes: n}` caps each part. A zero value uses
`GetDefaultMultipartPartLimit()`; the default is 8 MiB. Use
`gogo.NoMultipartPartLimit` only when another total-size cap still bounds the
request. Also count parts in your callback when a route should accept only a
small number of files or form fields.

For files, prefer `SaveInto` or generated destination paths:

- `MultipartPart.SaveInto(dir)` uses the basename of `FileName`, rejects empty
  names and `.` / `..`, and writes with `O_CREATE|O_EXCL` so it will not
  overwrite an existing file or final-path symlink.
- `MultipartStreamPart.SaveInto(dir)` does the same while copying from the
  multipart reader instead of retaining another `Data` allocation.
- `MultipartPart.SaveAtNew(dst)` is useful with a generated safe destination.
- `MultipartPart.SaveAt(dst)` overwrites and follows symlinks. Use it only for
  trusted destinations that are not derived from client input.

Create upload directories as trusted application state, not from request
fields. A common pattern is a private parent directory plus per-request temp
directories:

```go
uploadRoot := "/var/lib/myapp/uploads"

tmpDir, err := os.MkdirTemp(uploadRoot, "incoming-*")
if err != nil {
    return err
}
defer os.RemoveAll(tmpDir)
```

Keep temp and upload roots separate from public static roots. After saving,
validate the content you care about, generate the stored filename or object ID,
set restrictive permissions if needed, and then move or copy into the final
private location. Serve it later only after authorization and a rooted path
check.

Do not trust `MultipartPart.FileName` as identity. Use it as display metadata
after sanitizing, or discard it and generate names server-side. Validate
extensions and content signatures according to the route, not according to the
client's `Content-Type` alone.
