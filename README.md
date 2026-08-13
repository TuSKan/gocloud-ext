# gocloud-ext

Drivers for the [Go Cloud Development Kit](https://gocloud.dev) that live
outside the go-cloud repository.

go-cloud is [not generally accepting new
drivers](https://github.com/google/go-cloud/issues/3683), but its driver
interfaces are public and so is its conformance suite, so drivers work exactly
the same from out here: blank-import one and its URL schemes register with
`blob.DefaultURLMux()` as usual.

Every driver in this repo passes `gocloud.dev/blob/drivertest`, the same
conformance suite the in-tree drivers pass.

## Drivers

| module | schemes | what it does |
|---|---|---|
| [`blob/httpblob`](blob/httpblob) | `http`, `https`, `webdav`, `webdavs` | Blob storage over plain HTTP (read-only) or WebDAV (read-write) |

## Usage

```go
import (
    "gocloud.dev/blob"
    _ "github.com/TuSKan/gocloud-ext/blob/httpblob"
)

// Read-only, against any web server.
b, err := blob.OpenBucket(ctx, "https://example.com/files")

// Read-write, against a WebDAV server.
b, err := blob.OpenBucket(ctx, "webdavs://user:pass@example.com/dav/my-bucket")
```

```
go get github.com/TuSKan/gocloud-ext/blob/httpblob
```

## Layout

One repository, several modules, following go-cloud's own `allmodules`
convention. The root module holds only shared internals and has **no external
dependencies**; each driver is its own module so that it versions independently
and no consumer pulls a dependency it did not ask for.

```
.                        root module: internal/escape, internal/useragent
blob/httpblob            module github.com/TuSKan/gocloud-ext/blob/httpblob
```

Release tags are per-module and path-prefixed: `v0.1.0` for the root,
`blob/httpblob/v0.1.0` for the driver.

## Versioning against go-cloud

`gocloud.dev/blob/driver` carries no compatibility guarantee, and go-cloud is
still v0.x. Drivers here pin a specific go-cloud version, and CI additionally
builds against `gocloud.dev@master` so that an upstream interface change shows
up as a red build here rather than as a broken `go get` for you.

If a `go get -u` ever leaves you with a go-cloud version this driver has not
caught up to, pin go-cloud until a new driver release lands.

## License

Apache 2.0. Portions are derived from
[go-cloud](https://github.com/google/go-cloud); see [NOTICE](NOTICE).
