# sftpblob

[![pkg.go.dev](https://pkg.go.dev/badge/github.com/TuSKan/gocloud-ext/blob/sftpblob.svg)](https://pkg.go.dev/github.com/TuSKan/gocloud-ext/blob/sftpblob)

A [Go CDK](https://gocloud.dev) `blob` driver that speaks SFTP.

A full read-write bucket on top of any SSH server. There is no object-storage
service to provision, no credentials to rotate through a cloud console, and no
new port to open: if you can `ssh` to the machine, you already have a bucket.

Useful for build artifacts on a box you already run, for buckets that must stay
inside a network boundary, and as a portable fallback when a deployment has SSH
but no S3.

Registers for the `sftp` scheme.

Tested in CI against real OpenSSH — not just the `pkg/sftp` test server — in
addition to `gocloud.dev/blob/drivertest`, the same conformance suite the
in-tree go-cloud drivers pass.

## Install

```bash
go get github.com/TuSKan/gocloud-ext/blob/sftpblob
```

Requires Go 1.25 or later.

## Quick start

```go
package main

import (
	"context"
	"fmt"

	"gocloud.dev/blob"
	_ "github.com/TuSKan/gocloud-ext/blob/sftpblob"
)

func main() {
	ctx := context.Background()

	b, err := blob.OpenBucket(ctx,
		"sftp://deploy@example.com/srv/artifacts?private_key_path=/home/me/.ssh/id_ed25519")
	if err != nil {
		panic(err)
	}
	defer b.Close()

	if err := b.WriteAll(ctx, "build-1234.txt", []byte("hello"), nil); err != nil {
		panic(err)
	}

	got, err := b.ReadAll(ctx, "build-1234.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(got))
}
```

The URL's **host** is the server — port defaults to 22 — and the URL's **path**
is the directory used as the bucket root. The example writes
`/srv/artifacts/build-1234.tar.gz` on `example.com`.

## Authentication

Tried in this order; the first one that is configured wins:

1. **A private key**, if named by `private_key_path` or `private_key_env`.
2. **A password**, from the URL userinfo — `sftp://user:pass@host/…`.
3. **The SSH agent** at `$SSH_AUTH_SOCK`.

Supplying none of these is an error raised up front, rather than a confusing
rejection from the server later.

```go
// Key from a file on disk.
"sftp://user@example.com:2222/bucket?private_key_path=/path/to/id_rsa"

// Encrypted key from the environment — the shape you want in CI.
"sftp://user@example.com/bucket?private_key_env=DEPLOY_KEY&private_key_passphrase_env=DEPLOY_KEY_PASS"

// Password.
"sftp://user:password@example.com/a/directory"

// SSH agent, IPv6 host.
"sftp://user@[2001:db8::1]/bucket"
```

`private_key_env` names the variable rather than fixing one. That means a single
process can open several `sftp://` buckets under different identities, and the
key's source is visible at the call site instead of hidden in the environment.
Escaped `\n` newlines are accepted, since key material passed through a shell or
a CI secret store usually arrives that way.

## Host key verification

Host keys are verified against `~/.ssh/known_hosts` by default. Point
`known_hosts_path` somewhere else to override it — useful in a container where
the key is provisioned by `ssh-keyscan`:

```
sftp://user@host/bucket?known_hosts_path=/etc/ssh/known_hosts.deploy
```

`insecure_skip_verify=true` disables verification entirely. It exists for tests.
Turning it on in production removes the only protection against a
man-in-the-middle on the connection.

Both settings are parsed strictly — see below.

## URL parameters

| parameter | meaning |
|---|---|
| `private_key_path=…` | Path to a file holding the PEM-encoded SSH private key |
| `private_key_env=…` | **Name** of an env var holding the PEM-encoded key |
| `private_key_passphrase_env=…` | **Name** of an env var holding that key's passphrase |
| `known_hosts_path=…` | `known_hosts` file for host key verification; defaults to `~/.ssh/known_hosts` |
| `insecure_skip_verify=true` | Disable host key verification — **tests only** |
| `create_dir=true` | `MkdirAll` the bucket root if it does not exist |
| `metadata=skip` | Store no metadata sidecars |
| `timeout=15s` | Connection timeout as a Go duration; otherwise taken from the context deadline, default 15s |

**Any parameter not listed here is an error, and booleans are parsed as
booleans** — `?insecure_skip_verify=false` means *false*.

Both rules exist because these are security settings, and a security setting
must not fail open. A misspelled `known_hosts_path` that quietly fell back to
the default file, or a `false` that was read as "present, therefore true", would
each silently weaken the connection.

## Opening a bucket in code

`sftpblob.OpenBucket` takes a `*sftp.Client` you have already connected, so you
keep full control of the SSH configuration — jump hosts, custom auth methods,
certificate-based host keys, connection pooling:

```go
import (
	"github.com/pkg/sftp"
	"github.com/TuSKan/gocloud-ext/blob/sftpblob"
	"golang.org/x/crypto/ssh"
)

conn, err := ssh.Dial("tcp", "example.com:22", sshConfig)
if err != nil {
	return err
}
client, err := sftp.NewClient(conn)
if err != nil {
	return err
}

b, err := sftpblob.OpenBucket(client, "/srv/artifacts", &sftpblob.Options{
	CreateDir: true,
})
```

| field | meaning |
|---|---|
| `CreateDir` | Create the bucket root with `MkdirAll` if it does not exist |
| `Metadata` | `MetadataInSidecar` (default) or `MetadataDontWrite` |

A client you pass in is **yours**: `Bucket.Close` never closes it, so two
buckets may share one connection. A client dialed by the URL opener is owned by
the bucket and closed with it.

## Metadata

Blob metadata is stored in **sidecar** files, at the same key plus a `.attrs`
suffix. The format is identical to `fileblob`'s and `httpblob`'s, so a directory
written by any of the three is readable by the others — you can stage files
locally with `fileblob` and serve the same tree over SFTP with no conversion.

Set `Options.Metadata = MetadataDontWrite`, or `?metadata=skip`, to suppress
them. Without stored metadata many `blob.Attributes` fields take default values.

## Worth knowing before production

- **Writes are atomic.** sftpblob writes to a temporary file created *next to*
  its destination — same remote filesystem, so the rename is atomic — and
  renames it into place on `Close`. `WriterOptions.IfNotExist` is enforced by
  the rename itself: `SSH_FXP_RENAME` fails when the destination exists, which
  leaves no check-then-act window for a racing writer to slip through.

- **Atomic *overwrite* depends on an OpenSSH extension.** Where the server
  offers `posix-rename@openssh.com` it is used and the overwrite is atomic.
  Where it does not, the fallback removes the destination and then renames, and
  a reader in between sees no blob at all. The server is probed once per bucket,
  not once per write.

- **`Copy` costs bandwidth.** SFTP has no server-side copy, so the blob is read
  down and written back up. Budget twice the object's size in network traffic.

- **Sweep abandoned temporaries.** A canceled or failed write removes its own
  temporary file, but a process killed mid-write cannot. Those files carry a
  `.gocdktmp.` infix and are hidden from `List`, so they accumulate unnoticed. A
  bucket written by long-lived processes should be swept periodically.

- **Empty directories outlive their keys.** Deleting a blob does not remove the
  directory that held it. `List` will not report an empty directory, but
  skipping one still costs a request.

## Escaping

The Go CDK supports all UTF-8 strings. To work with services that don't, strings
are escaped on write and unescaped on read. For sftpblob:

- ASCII 0–31 become `__0x<hex>__`.
- So do the `/` in `../`, the trailing `/` in `//`, and a trailing `/` in a key.
- `\ < > : " | ? *` are escaped as well, for safety across the filesystems an
  SFTP server may sit on.

## `As`

For [driver-specific access](https://gocloud.dev/concepts/as/):

| type | concrete type |
|---|---|
| `Bucket` | `*sftp.Client` |
| `Error` | `*sftpblob.Error`, plus the underlying `*sftp.StatusError` or `*os.PathError` |
| `Reader` | `io.Reader` |
| `Attributes` | `fs.FileInfo` |
| `ListObject` | `fs.FileInfo` |
| `BeforeRead`, `BeforeWrite`, `BeforeCopy` | `*sftp.File` |
| `BeforeList`, `BeforeDelete` | `*sftp.Client` |

## License

Apache 2.0. Portions are derived from
[go-cloud](https://github.com/google/go-cloud); see [NOTICE](../../NOTICE).
