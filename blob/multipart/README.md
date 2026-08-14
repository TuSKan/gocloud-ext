# multipart

[![pkg.go.dev](https://pkg.go.dev/badge/github.com/TuSKan/gocloud-ext/blob/multipart.svg)](https://pkg.go.dev/github.com/TuSKan/gocloud-ext/blob/multipart)

Assemble one blob from parts uploaded **in any order, concurrently, or from
separate processes**, for any [Go CDK](https://gocloud.dev) `blob` driver.

## Do you actually need this?

Probably not, if you just want a large object to upload quickly. `blob.Writer`
already does that: `s3blob` fans out through `transfermanager` and `azureblob`
through `UploadStream`, both tuned by `WriterOptions.BufferSize` and
`MaxConcurrency`. Nothing here makes a single-process upload faster.

Reach for this package when the **upload itself must outlive the process that
started it**:

- Start an upload on one machine, write parts from a fleet of others, commit
  from a third.
- Resume after a crash instead of re-sending everything.
- Let parts arrive out of order from independent producers.

A `Writer` cannot express any of that — it is one sequential stream owned by one
goroutine. This gives you a durable `UploadID` instead.

## Install

```bash
go get github.com/TuSKan/gocloud-ext/blob/multipart
```

Requires Go 1.25 or later.

## Quick start

```go
import (
	"gocloud.dev/blob"
	"github.com/TuSKan/gocloud-ext/blob/multipart"
)

u, err := multipart.NewUploader(ctx, bucket, "big/object.bin", nil)
if err != nil {
	return err
}

// Parts may be uploaded concurrently, and in any order.
p2, err := u.UploadPart(ctx, 2, secondReader)
p1, err := u.UploadPart(ctx, 1, firstReader)

if err := u.Commit(ctx, []multipart.Part{p1, p2}); err != nil {
	return err
}

// The object is now readable through the same bucket, at the same key.
data, err := bucket.ReadAll(ctx, "big/object.bin")
```

Nothing is readable at the key until `Commit` succeeds.

## Committing from another process

`Part` is JSON-serializable and `UploadID` is durable, which is the whole point:

```go
// Process A
u, _ := multipart.NewUploader(ctx, bucket, key, nil)
id := u.UploadID()          // hand this to the others
p, _ := u.UploadPart(ctx, 1, r)
send(p)                     // encoding/json round-trips a Part unchanged

// Process B, later, maybe on another machine
u, err := multipart.Open(ctx, bucket, key, id, nil)
parts, _ := u.ListParts(ctx)   // what already arrived
u.Commit(ctx, parts)
```

`Open` recovers the options the upload was created with, so process B commits
with the content type and metadata process A chose without having to be told
what they were.

## Two implementations

| | `multipart.NewUploader` | `s3mp` / `gcsmp` / `azmp` |
|---|---|---|
| works with | **every driver** | S3 / GCS / Azure only |
| needs an SDK client | no | yes |
| needs a bucket name | no | **yes** |
| needs the prefix | no | **yes** |
| key escaping | done by the driver | reproduced by the package |
| `Commit` | re-reads and rewrites each part | **server side** |

**Start with `NewUploader`.** It talks only to `blob.Bucket`, so the driver
applies its own key escaping and any `blob.PrefixedBucket` wrapping, and the
committed object always lands where `bucket.NewReader` will find it. That one
implementation covers `fileblob`, `sftpblob`, `httpblob` over WebDAV, `memblob`
and the cloud drivers alike.

Its cost is that `Commit` reads every staged part back and rewrites it, roughly
doubling bytes transferred. When that matters, use the native package for your
backend:

- **[`s3mp`](s3mp)** — S3's own multipart API
- **[`gcsmp`](gcsmp)** — GCS object composition
- **[`azmp`](azmp)** — Azure block blobs

Those bypass the driver, which is why they need a `Location{Bucket, Prefix}`.
**A `PrefixedBucket` is invisible to them** — `blob.Bucket.As` exposes only the
client — so an unset `Prefix` silently writes where `blob.Bucket` will not look.

## Backend limits

Ask, rather than discovering them at `Commit`:

```go
c := u.Constraints()
if c.MinPartSize > 0 {
	// S3 reports 5 MiB here and rejects any smaller non-final part.
}
```

| field | meaning |
|---|---|
| `MinPartSize` | smallest allowed non-final part; 0 for no minimum |
| `MaxPartSize` | largest single part; 0 for no limit |
| `MaxParts` | most parts in one upload |
| `Resumable` | whether `Open` can continue this upload |

Part numbers run from 1 to 10000 — S3's ceiling, and therefore the portable one.

## There is no offset

`UploadPart` takes a part **number** and a reader. It never takes a byte offset,
and that is deliberate.

The rejected upstream proposal ([google/go-cloud#3769](https://github.com/google/go-cloud/pull/3769))
carried an `Offset` on its part struct. One driver assembled by it while another
sorted by part number, so portable code passing only a number **silently
produced corrupt objects**. Its conformance suite never caught this because the
suite always supplied `Offset` and `Size`.

With no offset in the signature, that failure cannot be written.

## Abandoned uploads cost money

Parts are billed from the moment they are written, and nothing can clean up
after a process that was killed. `Abort` is best-effort by nature.

Set a lifecycle rule:

- **generic** — staged parts are ordinary objects under a `.gocdkmpu.` infix
- **S3** — `AbortIncompleteMultipartUpload`
- **GCS** — an Object Lifecycle rule matching the `.gocdkmpu.` prefix
- **Azure** — uncommitted blocks expire on their own after about seven days

Staged parts are also visible to `blob.Bucket.List` while an upload is in
flight; filter on the infix if that matters.

## Testing

`mptest` is a conformance suite every backend passes. It deliberately supplies
**only what the documented API requires**, so a backend that secretly depends on
more fails rather than passing quietly.

Checking any driver takes a few lines:

```go
mptest.RunConformanceTests(t, mptest.BucketHarness(mptest.BucketOpener{
	Open:            func() (*blob.Bucket, error) { return fileblob.OpenBucket(dir, nil) },
	SeparateHandles: true,
}))
```

CI runs it against `memblob`, `fileblob`, a prefixed bucket, real **OpenSSH**,
real **Apache mod_dav** and **rclone**, plus **MinIO**, **Azurite** and
**fake-gcs-server** for the native backends — no cloud credentials required.

## License

Apache 2.0. Portions are derived from
[go-cloud](https://github.com/google/go-cloud); see [NOTICE](../../NOTICE).
