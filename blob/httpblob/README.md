# httpblob

[![pkg.go.dev](https://pkg.go.dev/badge/github.com/TuSKan/gocloud-ext/blob/httpblob.svg)](https://pkg.go.dev/github.com/TuSKan/gocloud-ext/blob/httpblob)

A [Go CDK](https://gocloud.dev) `blob` driver backed by an HTTP server.

Point it at a web server and get a `*blob.Bucket`. Two standard protocols, and
you pick which one by the URL scheme:

| scheme | protocol | bucket is |
|---|---|---|
| `http`, `https` | plain HTTP ([RFC 9110](https://www.rfc-editor.org/rfc/rfc9110)) | **read-only** |
| `webdav`, `webdavs` | WebDAV ([RFC 4918](https://www.rfc-editor.org/rfc/rfc4918)) | **read-write** |

`webdav` connects over http, `webdavs` over https.

httpblob never probes the server to guess which protocol it speaks. The scheme
decides — so behaviour is predictable and nothing silently changes when a server
is reconfigured.

Tested in CI against `rclone serve webdav` and Apache `mod_dav`, in addition to
`gocloud.dev/blob/drivertest`, the same conformance suite the in-tree go-cloud
drivers pass.

## Install

```bash
go get github.com/TuSKan/gocloud-ext/blob/httpblob
```

Requires Go 1.25 or later.

## Read from any web server

Nothing is required of the server. If `curl` can fetch it, httpblob can read it
— static file servers, artifact repositories, release pages, a bucket fronted by
a CDN.

```go
package main

import (
	"context"
	"fmt"

	"gocloud.dev/blob"
	_ "github.com/TuSKan/gocloud-ext/blob/httpblob"
)

func main() {
	ctx := context.Background()

	b, err := blob.OpenBucket(ctx, "https://example.com/files")
	if err != nil {
		panic(err)
	}
	defer b.Close()

	data, err := b.ReadAll(ctx, "reports/2026-q1.pdf")
	if err != nil {
		panic(err)
	}
	fmt.Println(len(data))
}
```

The bucket's base URL is the scheme, host and path from the URL; blob keys are
appended to it. The example above reads
`https://example.com/files/reports/2026-q1.pdf`.

Reads use `HEAD`, `GET` and `Range` requests, so range reads and seeks cost only
the bytes you ask for, provided the server honours `Range`.

## Read and write over WebDAV

WebDAV adds `PUT`, `DELETE`, `COPY`, `MOVE`, `MKCOL` and `PROPFIND`, which is
everything a read-write bucket needs. Works against Apache `mod_dav`, the nginx
`dav` module, Nextcloud/ownCloud, `golang.org/x/net/webdav` and `rclone serve
webdav`.

```go
b, err := blob.OpenBucket(ctx, "webdavs://user:pass@example.com/dav/my-bucket")
if err != nil {
	return err
}
defer b.Close()

if err := b.WriteAll(ctx, "notes.txt", []byte("hello"), nil); err != nil {
	return err
}

iter := b.List(nil)
for {
	obj, err := iter.Next(ctx)
	if err == io.EOF {
		break
	}
	if err != nil {
		return err
	}
	fmt.Println(obj.Key, obj.Size)
}
```

## What works under which protocol

| `blob.Bucket` method | `http` / `https` | `webdav` / `webdavs` |
|---|:--:|:--:|
| `NewReader`, `NewRangeReader`, `ReadAll`, `Download` | ✅ | ✅ |
| `Attributes`, `Exists` | ✅ | ✅ |
| `List`, `ListPage` | — | ✅ |
| `NewWriter`, `WriteAll`, `Upload` | — | ✅ |
| `Copy`, `Delete` | — | ✅ |
| `SignedURL` | — | — |

Unsupported operations return an error for which `gcerrors.Code` reports
`Unimplemented`, so you can branch on it rather than string-match:

```go
if gcerrors.Code(err) == gcerrors.Unimplemented {
	// This bucket is read-only; fall back to something else.
}
```

## URL parameters

| parameter | meaning |
|---|---|
| `metadata=skip` | Do not read or write metadata sidecars |
| `auth_token=…` | Sent as `Authorization: Bearer …` on every request |
| `max_retries=N` | Retry budget for retryable requests; default `3` |

Credentials in the URL userinfo — `webdavs://user:pass@host/path` — are used for
HTTP Basic Authentication ([RFC 7617](https://www.rfc-editor.org/rfc/rfc7617)).
`auth_token` takes precedence over them.

**Any query parameter not listed here is an error.** A misspelled parameter that
was silently ignored would be a setting you believe is on and isn't.

## Opening a bucket in code

Use `OpenBucket` when you need a custom `*http.Client`, extra headers, or
options that have no URL equivalent:

```go
import "github.com/TuSKan/gocloud-ext/blob/httpblob"

b, err := httpblob.OpenBucket(ctx, myClient, "https://example.com/dav/bucket", &httpblob.Options{
	Protocol:  httpblob.ProtocolWebDAV,
	AuthToken: os.Getenv("DAV_TOKEN"),
	Headers: http.Header{
		"X-Tenant": {"acme"},
	},
	MaxRetries: 5,
})
```

| field | meaning |
|---|---|
| `Protocol` | `ProtocolHTTP` (read-only, default) or `ProtocolWebDAV` (read-write) |
| `Metadata` | `MetadataInSidecar` (default) or `MetadataDontWrite` |
| `AuthToken` | Bearer token; takes precedence over basic auth |
| `BasicAuthUser`, `BasicAuthPassword` | HTTP Basic Authentication |
| `Headers` | Sent on every request, after the auth headers |
| `MaxRetries` | Retries for retryable requests; `<= 0` means the default of 3 |

Passing a `nil` client uses `http.DefaultClient`.

## Metadata

Most web servers store bytes and nothing else — there is nowhere to record a
blob's `ContentType`, `Metadata` or MD5. Under WebDAV, httpblob keeps these in
**sidecar** objects beside each blob, at the same key plus a `.attrs` suffix.

The format is identical to `fileblob`'s and `sftpblob`'s, so a bucket written by
any of the three is readable by the others. That is what lets you stage files
locally with `fileblob` and serve the same directory over WebDAV without a
conversion step.

Set `Options.Metadata = MetadataDontWrite`, or `?metadata=skip` in the URL, to
suppress them. Without stored metadata many `blob.Attributes` fields take
default values.

Under plain HTTP sidecars are **never** fetched — doing so would double the
request count against servers that know nothing about them — so attributes come
from response headers only, and `Attributes.Metadata` is always empty.

## Retries

Safe methods (`GET`, `HEAD`, `PROPFIND`, `OPTIONS`) are retried on transport
errors, 5xx and 429, honouring `Retry-After`.

Mutating requests are **never** retried. A `PUT` body is a one-shot stream that
cannot be replayed, and a retried `DELETE` or `MOVE` that actually succeeded the
first time comes back as a spurious 404.

## Worth knowing before production

- **Writes are staged.** A write `PUT`s to a temporary key and `MOVE`s it into
  place on `Close`, so a failed or canceled write leaves any previous blob
  intact. The `MOVE` and the sidecar write cannot be made atomic with respect to
  each other; if the last step fails you get the new blob with the previous
  blob's metadata, and the error is returned to you.
- **Sweep abandoned temporaries.** A canceled write removes its own temporary
  object, but a process killed mid-write cannot. Those objects carry a
  `.gocdktmp.` infix and are hidden from `List`, so they accumulate unnoticed. A
  bucket written by long-lived processes should be swept periodically.
- **Empty collections outlive their keys.** Deleting a blob does not delete the
  collection that held it. `List` will not report an empty collection as a
  directory, but skipping one still costs a request.
- **Reserved key names.** `.attrs` as a suffix and `.gocdktmp.` anywhere are
  rejected with `gcerrors.InvalidArgument`, so a blob can never be mistaken for
  httpblob's own bookkeeping.

## Scheme registration

`http` and `https` are generic enough that another package may want them too,
and `blob.URLMux` panics on duplicate registration with no way to unregister.
httpblob therefore claims a scheme **only if it is still free**, so a collision
costs you the scheme rather than crashing the process at `init`.

Which package wins is decided by init order, which Go does not define across
packages. If you need certainty, don't rely on the default mux — register with
your own `blob.URLMux`, or call `httpblob.OpenBucket` directly.

## Escaping

The Go CDK supports all UTF-8 strings. To work with services that don't,
strings are escaped on write and unescaped on read. For httpblob:

- ASCII 0–31 become `__0x<hex>__`.
- So do the `/` in `../`, the trailing `/` in `//`, and a trailing `/` in a key.
- `\ < > : " | ? *` are escaped too, because WebDAV servers are usually backed
  by a filesystem that cannot represent them.

## `As`

For [driver-specific access](https://gocloud.dev/concepts/as/):

| type | concrete type |
|---|---|
| `Bucket` | `*http.Client` |
| `Error` | `*httpblob.Error` |
| `Reader` | `*http.Response` |
| `Attributes` | `http.Header` |
| `ListObject` | `http.Header` |
| `BeforeList`, `BeforeRead`, `BeforeWrite`, `BeforeCopy`, `BeforeDelete` | `*http.Request` |

## License

Apache 2.0. Portions are derived from
[go-cloud](https://github.com/google/go-cloud); see [NOTICE](../../NOTICE).
