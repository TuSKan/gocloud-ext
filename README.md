# gocloud-ext

Drivers for the [Go Cloud Development Kit](https://gocloud.dev) that live
outside the go-cloud repository.

go-cloud is [not generally accepting new
drivers](https://github.com/google/go-cloud/issues/3683), but its driver
interfaces are public and so is its conformance suite, so drivers work exactly
the same from out here. You blank-import one, its URL schemes register with
`blob.DefaultURLMux()`, and from then on it is just a `*blob.Bucket` — the same
type, the same methods, no wrapper and no fork.

Every driver here passes `gocloud.dev/blob/drivertest`, the same conformance
suite the in-tree drivers pass, and is additionally tested against real servers
in CI (rclone and Apache `mod_dav` for WebDAV, OpenSSH for SFTP).

## Drivers

| module | schemes | read | write | use it for |
|---|---|:--:|:--:|---|
| [`blob/httpblob`](#blobhttpblob) | `http`, `https` | ✅ | — | Reading blobs from any ordinary web server |
| [`blob/httpblob`](#blobhttpblob) | `webdav`, `webdavs` | ✅ | ✅ | Nextcloud, Apache `mod_dav`, nginx `dav`, any RFC 4918 server |
| [`blob/sftpblob`](#blobsftpblob) | `sftp` | ✅ | ✅ | Any SSH server — the storage you already have on every Linux box |

There is also **[`blob/multipart`](blob/multipart/README.md)**, which is not a
driver: it assembles one blob from parts uploaded in any order, concurrently, or
from separate processes, against *any* driver. Use it when an upload has to
outlive the process that started it — start on one machine, write parts from
others, commit from a third, resume after a crash. Native S3, GCS and Azure
backends are included for server-side assembly.

Each module is versioned and installed independently. Take only the one you
need; installing `sftpblob` does not pull in `httpblob`, or vice versa.

Summaries follow below. Each module also has its own full documentation:
**[httpblob](blob/httpblob/README.md)** · **[sftpblob](blob/sftpblob/README.md)**.

---

# `blob/httpblob`

[![pkg.go.dev](https://pkg.go.dev/badge/github.com/TuSKan/gocloud-ext/blob/httpblob.svg)](https://pkg.go.dev/github.com/TuSKan/gocloud-ext/blob/httpblob)

Blob storage over HTTP. It speaks two standard protocols, chosen by the URL
scheme:

- **`http` / `https`** — plain HTTP (RFC 9110) against *any* web server.
  Read-only.
- **`webdav` / `webdavs`** — WebDAV (RFC 4918), which adds the methods needed
  for a read-write bucket. Works against Apache `mod_dav`, the nginx `dav`
  module, Nextcloud/ownCloud, `golang.org/x/net/webdav`, and `rclone serve
  webdav`.

The driver does not probe the server to guess which one to use. You choose, via
the scheme.

### Install

```bash
go get github.com/TuSKan/gocloud-ext/blob/httpblob
```

### Read from any web server

No server-side support required — if `curl` can fetch it, this can read it.

```go
import (
	"gocloud.dev/blob"
	_ "github.com/TuSKan/gocloud-ext/blob/httpblob"
)

b, err := blob.OpenBucket(ctx, "https://example.com/files")
if err != nil {
	return err
}
defer b.Close()

data, err := b.ReadAll(ctx, "reports/2026-q1.pdf")
// fetches https://example.com/files/reports/2026-q1.pdf
```

### Read and write over WebDAV

```go
b, err := blob.OpenBucket(ctx, "webdavs://user:pass@example.com/dav/my-bucket")
if err != nil {
	return err
}
defer b.Close()

err = b.WriteAll(ctx, "notes.txt", []byte("hello"), nil)
```

### What each protocol supports

| `blob.Bucket` method | `http`/`https` | `webdav`/`webdavs` |
|---|:--:|:--:|
| `NewReader`, `NewRangeReader`, `ReadAll`, `Download` | ✅ | ✅ |
| `Attributes`, `Exists` | ✅ | ✅ |
| `List`, `ListPage` | — | ✅ |
| `NewWriter`, `WriteAll`, `Upload` | — | ✅ |
| `Copy`, `Delete` | — | ✅ |
| `SignedURL` | — | — |

Under plain HTTP the unsupported operations return an error for which
`gcerrors.Code` reports `Unimplemented`, so you can detect it rather than guess.

### URL parameters

| parameter | meaning |
|---|---|
| `metadata=skip` | Do not read or write metadata sidecars (see below) |
| `auth_token=…` | Sent as `Authorization: Bearer …` on every request |
| `max_retries=N` | Retries for retryable requests; default 3 |

Credentials in the URL userinfo (`webdavs://user:pass@…`) are used for HTTP
Basic Authentication. `auth_token` takes precedence over them.

Any query parameter not in this list is an **error**, not a silent no-op — a
misspelled security setting should fail loudly.

### Metadata

Most web servers store bytes and nothing else, with nowhere to record a blob's
`ContentType`, `Metadata` or MD5. Under WebDAV, httpblob stores these in
*sidecar* objects beside each blob, at the same key plus a `.attrs` suffix. The
format is identical to `fileblob`'s and `sftpblob`'s, so a bucket written by any
of the three is readable by the others.

Under plain HTTP sidecars are never fetched — that would double the request
count against servers that know nothing about them — so attributes come from
response headers only and `Attributes.Metadata` is always empty.

### Worth knowing before production

- **Writes are staged.** A write PUTs to a temporary key and MOVEs it into place
  on `Close`, so a failed or canceled write leaves any previous blob intact. The
  final MOVE and the sidecar write cannot be atomic with respect to each other;
  if the last step fails, the new blob is in place with the old metadata, and the
  error is returned to you.
- **Sweep abandoned temporaries.** A canceled write cleans up after itself, but a
  process that is killed mid-write cannot. Those objects carry a `.gocdktmp.`
  infix and are hidden from `List`, so they accumulate silently. Buckets written
  by long-lived processes should be swept periodically.
- **Empty collections outlive their keys.** Deleting a blob does not delete the
  collection that held it. `List` will not report them, but skipping one costs a
  request.
- `.attrs` as a suffix and `.gocdktmp.` anywhere are reserved key names.

📖 **[Full httpblob documentation →](blob/httpblob/README.md)** — retry policy,
scheme-registration collisions, key escaping, `As` types, and opening a bucket
in code with a custom `*http.Client`.

---

# `blob/sftpblob`

[![pkg.go.dev](https://pkg.go.dev/badge/github.com/TuSKan/gocloud-ext/blob/sftpblob.svg)](https://pkg.go.dev/github.com/TuSKan/gocloud-ext/blob/sftpblob)

Blob storage over SFTP — a full read-write bucket on top of any SSH server. No
object-storage service to provision; if you can `ssh` to the box, you have a
bucket.

### Install

```bash
go get github.com/TuSKan/gocloud-ext/blob/sftpblob
```

### Quick start

```go
import (
	"gocloud.dev/blob"
	_ "github.com/TuSKan/gocloud-ext/blob/sftpblob"
)

b, err := blob.OpenBucket(ctx,
	"sftp://deploy@example.com/srv/artifacts?private_key_path=/home/me/.ssh/id_ed25519")
if err != nil {
	return err
}
defer b.Close()

err = b.WriteAll(ctx, "build-1234.tar.gz", data, nil)
```

The URL host is the server (port defaults to 22) and the URL path is the
directory used as the bucket root.

### Authentication

Tried in this order, first one wins:

1. **A private key**, if named by `private_key_path` or `private_key_env`.
2. **A password**, from the URL userinfo (`sftp://user:pass@host/…`).
3. **The SSH agent** at `$SSH_AUTH_SOCK`.

Supplying none of these is an error up front, rather than a confusing rejection
from the server later.

```go
// Key from a file.
"sftp://user@example.com:2222/bucket?private_key_path=/path/to/id_rsa"

// Encrypted key from the environment — good for CI secrets.
"sftp://user@example.com/bucket?private_key_env=DEPLOY_KEY&private_key_passphrase_env=DEPLOY_KEY_PASS"

// SSH agent, IPv6 host.
"sftp://user@[2001:db8::1]/bucket"
```

`private_key_env` names the variable rather than fixing one, so a single process
can open several `sftp://` buckets under different identities and the key's
source is visible at the call site. Escaped `\n` newlines are accepted, since
key material passed through a shell or a CI secret usually arrives that way.

### URL parameters

| parameter | meaning |
|---|---|
| `private_key_path=…` | File holding the PEM-encoded private key |
| `private_key_env=…` | **Name** of an env var holding the PEM-encoded key |
| `private_key_passphrase_env=…` | **Name** of an env var holding that key's passphrase |
| `known_hosts_path=…` | `known_hosts` file for host key verification; defaults to `~/.ssh/known_hosts` |
| `insecure_skip_verify=true` | Disable host key verification — **tests only** |
| `create_dir=true` | `MkdirAll` the bucket root if it does not exist |
| `metadata=skip` | Do not read or write metadata sidecars |
| `timeout=15s` | Connection timeout, as a Go duration; otherwise from the context deadline, default 15s |

Any parameter not in this list is an **error**, and booleans are parsed as
booleans — `?insecure_skip_verify=false` means *false*. Both matter: a
misspelled `known_hosts_path` that quietly fell back to the default file, or a
`false` that read as true, would be a security setting failing open.

### Metadata

Blob metadata is stored in sidecar files at the same key plus a `.attrs` suffix,
in the same format as `fileblob` and `httpblob`. Set `metadata=skip` to suppress
them; without stored metadata, many `blob.Attributes` fields take default
values.

### Worth knowing before production

- **Writes are atomic.** sftpblob writes to a temporary file beside the
  destination — same remote filesystem, so the rename is atomic — and renames it
  into place on `Close`. `WriterOptions.IfNotExist` is enforced by the rename
  itself, since `SSH_FXP_RENAME` fails when the destination exists, leaving no
  check-then-act window.
- **Overwrites need an OpenSSH extension.** Where the server offers
  `posix-rename@openssh.com` it is used and the overwrite is atomic. Where it
  does not, the fallback removes the destination then renames, and a reader in
  between sees no blob at all.
- **`Copy` costs bandwidth.** SFTP has no server-side copy, so the blob is read
  down and written back up: expect twice the object's size in traffic.
- **Sweep abandoned temporaries**, same as httpblob — `.gocdktmp.` files from
  killed processes are hidden from `List` and accumulate.
- Keys are escaped for filesystem safety: ASCII 0–31 and `\<>:"|?*` become
  `__0x<hex>__`, as do the separators in `../` and `//` and a trailing `/`.

📖 **[Full sftpblob documentation →](blob/sftpblob/README.md)** — host key
verification, key escaping, `As` types, and bringing your own `*sftp.Client`
for jump hosts or connection pooling.

---

## Layout

One repository, several modules, following go-cloud's own `allmodules`
convention. The root module holds only shared internals and has **no external
dependencies**; each driver is its own module so it versions independently and
no consumer pulls a dependency it did not ask for.

```
.                  root module — internal/escape, internal/useragent (no external deps)
blob/httpblob      module github.com/TuSKan/gocloud-ext/blob/httpblob
blob/sftpblob      module github.com/TuSKan/gocloud-ext/blob/sftpblob
```

Release tags are per-module and path-prefixed:

```
v0.1.0                    # root
blob/httpblob/v0.1.0      # httpblob
blob/sftpblob/v0.1.0      # sftpblob
```

Requires Go 1.25 or later.

## Versioning against go-cloud

`gocloud.dev/blob/driver` carries no compatibility guarantee, and go-cloud is
still v0.x. Drivers here pin a specific go-cloud version, and CI additionally
builds against `gocloud.dev@master` daily, so an upstream interface change shows
up as a red build here rather than as a broken `go get` for you.

If a `go get -u` ever leaves you with a go-cloud version a driver has not caught
up to, pin go-cloud until a new driver release lands.

## License

Apache 2.0. Portions are derived from
[go-cloud](https://github.com/google/go-cloud); see [NOTICE](NOTICE).
