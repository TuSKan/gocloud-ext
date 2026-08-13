// Copyright 2026 The gocloud-ext Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package sftpblob provides a blob implementation that uses the SFTP protocol.
// Use OpenBucket to construct a *blob.Bucket.
//
// To avoid partial writes, sftpblob writes to a temporary file and then renames
// it onto the final path on Close. The temporary file is created next to its
// destination so that both are on the same remote filesystem and the rename is
// atomic. Conditional writes (WriterOptions.IfNotExist) are staged the same
// way, and the condition is enforced by the rename itself: SSH_FXP_RENAME
// fails when the destination exists, which leaves no check-then-act window.
//
// Overwriting uses the posix-rename@openssh.com extension where the server
// offers it. Where it does not, sftpblob falls back to removing the
// destination and then renaming, which is not atomic: a reader in between sees
// no blob at all.
//
// Two operational notes:
//
//   - A write that is canceled or fails removes its own temporary file, but a
//     process that dies mid-write cannot. Such files carry a ".gocdktmp."
//     infix and are hidden from List, so they accumulate unnoticed; a bucket
//     written to by long-lived processes should be swept for them.
//   - Deleting a blob does not remove the directory that held it, so an empty
//     directory can outlive its last key. List does not report empty
//     directories, but skipping one costs a request.
//
// Copy has no server-side equivalent in SFTP, so it reads the blob down and
// writes it back up: expect twice the object's size in network traffic.
//
// By default sftpblob stores blob metadata in "sidecar" files under the original
// filename with an additional ".attrs" suffix.
// This behaviour can be changed via Options.Metadata;
// writing of those metadata files can be suppressed by setting it to
// MetadataDontWrite or its equivalent "metadata=skip" in the URL for the opener.
// In either case, absent any stored metadata many blob.Attributes fields
// will be set to default values.
//
// # URLs
//
// For blob.OpenBucket, sftpblob registers for the scheme "sftp".
// To customize the URL opener, or for more details on the URL format,
// see URLOpener.
// See https://gocloud.dev/concepts/urls/ for background information.
//
// # Escaping
//
// Go CDK supports all UTF-8 strings; to make this work with services lacking
// full UTF-8 support, strings must be escaped (during writes) and unescaped
// (during reads). The following escapes are performed for sftpblob:
//   - Blob keys: ASCII characters 0-31 are escaped to "__0x<hex>__".
//     Additionally, the "/" in "../", the trailing "/" in "//", and a trailing
//     "/" in key names are escaped in the same way.
//     The characters "\<>:"|?*" are also escaped for safety.
//
// # As
//
// sftpblob exposes the following types for As:
//   - Bucket: *sftp.Client
//   - Error: *sftpblob.Error, and the underlying *sftp.StatusError or
//     *os.PathError for failures reported by the server
//   - ListObject: fs.FileInfo
//   - Reader: io.Reader
//   - Attributes: fs.FileInfo
//   - ReaderOptions.BeforeRead, WriterOptions.BeforeWrite,
//     CopyOptions.BeforeCopy: *sftp.File
//   - ListOptions.BeforeList, DeleteOptions.BeforeDelete: *sftp.Client
package sftpblob // import "github.com/TuSKan/gocloud-ext/blob/sftpblob"

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TuSKan/gocloud-ext/internal/escape"
	"github.com/pkg/sftp"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

const (
	defaultPageSize = 1000
	attrsExt        = ".attrs"

	// tempInfix marks a staged write so it can be excluded from List and
	// rejected as a key.
	tempInfix = ".gocdktmp."

	// defaultDialTimeout applies when the URL sets no timeout and the context
	// carries no deadline.
	defaultDialTimeout = 15 * time.Second
)

func init() {
	blob.DefaultURLMux().RegisterBucket(Scheme, &URLOpener{})
}

// Scheme is the URL scheme sftpblob registers its URLOpener under on blob.DefaultMux.
const Scheme = "sftp"

// URLOpener opens sftp bucket URLs like "sftp://user:pass@host:port/foo/bar/baz".
//
// The URL's host is the SFTP server to connect to. If the port is omitted, it
// defaults to 22. The URL's path is the directory to use as the bucket root.
//
// Authentication is attempted in this order: a private key, if one is named by
// private_key_path or private_key_env; otherwise a password from the URL's
// userinfo; otherwise the ssh agent at SSH_AUTH_SOCK. Supplying none of these
// is an error rather than a confusing rejection from the server.
//
// Any query parameter not listed below is an error, and boolean parameters are
// parsed as booleans: "?insecure_skip_verify=false" means false. Both matter
// because these are security settings, and a security setting that is
// misspelled or misread must not fail open.
//
// The following query parameters are supported:
//
//   - private_key_path: path to a file holding the PEM-encoded SSH private key
//     to authenticate with.
//   - private_key_env: name of an environment variable holding the PEM-encoded
//     private key. The variable is named in the URL rather than fixed, so that
//     one process can open several sftp:// buckets under different identities
//     and so that the key's source is visible at the call site. Escaped
//     newlines ("\n") are accepted, since key material carried through a shell
//     or a CI secret usually arrives that way.
//   - private_key_passphrase_env: name of an environment variable holding the
//     passphrase for an encrypted private key.
//   - create_dir: boolean; create the bucket root with MkdirAll if it does not
//     already exist.
//   - insecure_skip_verify: boolean; disable SSH host key verification. Leave
//     this off outside of tests.
//   - known_hosts_path: path to the known_hosts file used to verify the host
//     key. Defaults to ~/.ssh/known_hosts.
//   - metadata: "skip" to store no metadata; see the package documentation.
//   - timeout: maximum time to establish the connection, as a Go duration. If
//     unset it is taken from the context's deadline, defaulting to 15s.
//
// Here are some example URLs:
//
//   - sftp://user:password@example.com/a/directory
//     -> Connects to example.com:22 with password authentication, using
//     "/a/directory" as the bucket root.
//   - sftp://user@example.com:2222/a/directory?private_key_path=/path/to/id_rsa
//     -> Connects to example.com:2222 with public key authentication, reading
//     the private key from "/path/to/id_rsa".
//   - sftp://user@example.com/bucket?private_key_env=DEPLOY_KEY&private_key_passphrase_env=DEPLOY_KEY_PASS
//     -> Reads an encrypted private key and its passphrase from the named
//     environment variables.
//   - sftp://user@[2001:db8::1]/
//     -> Connects to the IPv6 address with agent authentication (if SSH_AUTH_SOCK
//     is set).
type URLOpener struct {
	Options Options
}

type MetadataOption string

const (
	MetadataInSidecar MetadataOption = ""
	MetadataDontWrite MetadataOption = "skip"
)

// OpenBucketURL opens a blob.Bucket based on u.
func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	opts := new(Options)
	*opts = o.Options

	var (
		insecureSkipVerify bool
		knownHostsPath     string
		privateKeyPath     string
		privateKeyEnv      string
		passphraseEnv      string
		timeout            time.Duration
		haveTimeout        bool
	)

	// Every parameter is parsed strictly, and anything unrecognized is an
	// error. A silently ignored parameter here is a silently ignored security
	// setting: a misspelled known_hosts_path would quietly fall back to the
	// default file, and a "false" that reads as true would disable host key
	// checking altogether.
	for param, values := range u.Query() {
		value := values[0]
		var err error
		switch param {
		case "create_dir":
			opts.CreateDir, err = parseBoolParam(param, value)
		case "insecure_skip_verify":
			insecureSkipVerify, err = parseBoolParam(param, value)
		case "known_hosts_path":
			knownHostsPath = value
		case "private_key_path":
			privateKeyPath = value
		case "private_key_env":
			privateKeyEnv = value
		case "private_key_passphrase_env":
			passphraseEnv = value
		case "metadata":
			switch MetadataOption(value) {
			case MetadataDontWrite:
				opts.Metadata = MetadataDontWrite
			case MetadataInSidecar:
				opts.Metadata = MetadataInSidecar
			default:
				err = fmt.Errorf("invalid value %q", value)
			}
		case "timeout":
			timeout, err = time.ParseDuration(value)
			haveTimeout = err == nil
		default:
			return nil, fmt.Errorf("open bucket %v: invalid query parameter %q", u, param)
		}
		if err != nil {
			return nil, fmt.Errorf("open bucket %v: query parameter %q: %w", u, param, err)
		}
	}

	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "22"
	}

	var hostKeyCallback ssh.HostKeyCallback
	if insecureSkipVerify {
		hostKeyCallback = ssh.InsecureIgnoreHostKey()
	} else {
		if knownHostsPath == "" {
			homeDir, err := os.UserHomeDir()
			if err != nil {
				return nil, fmt.Errorf("sftpblob: cannot locate home directory for default known_hosts: %v", err)
			}
			knownHostsPath = filepath.Join(homeDir, ".ssh", "known_hosts")
		}
		cb, err := knownhosts.New(knownHostsPath)
		if err != nil {
			return nil, fmt.Errorf("sftpblob: failed to parse known_hosts file %q: %v", knownHostsPath, err)
		}
		hostKeyCallback = cb
	}

	if !haveTimeout {
		if deadline, ok := ctx.Deadline(); ok {
			timeout = time.Until(deadline)
			if timeout <= 0 {
				return nil, context.DeadlineExceeded
			}
		} else {
			timeout = defaultDialTimeout
		}
	}

	config := &ssh.ClientConfig{
		User:            u.User.Username(),
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: hostKeyCallback,
		Timeout:         timeout,
	}

	var closers closerList
	defer func() {
		_ = closers.Close()
	}()

	// Key material, in order of precedence: an explicit file, then an
	// explicitly named environment variable. The environment variable has to
	// be named in the URL rather than read from a fixed name, so that one
	// process can open two sftp:// buckets under different identities and so
	// that the key's source is visible at the call site.
	var keyBytes []byte
	switch {
	case privateKeyPath != "":
		b, err := os.ReadFile(privateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("sftpblob: failed to read private key from path %q: %v", privateKeyPath, err)
		}
		keyBytes = b
	case privateKeyEnv != "":
		v := os.Getenv(privateKeyEnv)
		if v == "" {
			return nil, fmt.Errorf("sftpblob: environment variable %q named by private_key_env is empty", privateKeyEnv)
		}
		// A PEM key carried through a shell or a CI secret often arrives with
		// its newlines escaped.
		keyBytes = []byte(strings.ReplaceAll(v, "\n", "\n"))
	}

	if len(keyBytes) > 0 {
		var signer ssh.Signer
		var err error
		if passphraseEnv != "" {
			passphrase := os.Getenv(passphraseEnv)
			if passphrase == "" {
				return nil, fmt.Errorf("sftpblob: environment variable %q named by private_key_passphrase_env is empty", passphraseEnv)
			}
			signer, err = ssh.ParsePrivateKeyWithPassphrase(keyBytes, []byte(passphrase))
		} else {
			signer, err = ssh.ParsePrivateKey(keyBytes)
		}
		if err != nil {
			// Deliberately does not wrap err: x/crypto/ssh error text for an
			// encrypted key is safe, but never risk echoing key material.
			return nil, fmt.Errorf("sftpblob: failed to parse private key (encrypted keys need private_key_passphrase_env)")
		}
		config.Auth = append(config.Auth, ssh.PublicKeys(signer))
	} else if pass, hasPass := u.User.Password(); hasPass {
		config.Auth = append(config.Auth, ssh.Password(pass))
	} else if authSock := os.Getenv("SSH_AUTH_SOCK"); authSock != "" {
		agentConn, err := net.Dial("unix", authSock)
		if err != nil {
			return nil, fmt.Errorf("sftpblob: failed to reach the ssh agent at %s: %v", authSock, err)
		}
		closers = append(closers, agentConn)
		config.Auth = append(config.Auth, ssh.PublicKeysCallback(agent.NewClient(agentConn).Signers))
	}
	if len(config.Auth) == 0 {
		return nil, fmt.Errorf("open bucket %v: no authentication available; supply a password in the URL, private_key_path, private_key_env, or run an ssh agent", u)
	}

	// Dial through the context so that a canceled ctx aborts connection setup;
	// ssh.Dial would honor only config.Timeout.
	addr := net.JoinHostPort(host, port)
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("sftpblob: failed to dial %s: %v", addr, err)
	}
	closers = append(closers, conn)
	sshConn, chans, reqs, err := ssh.NewClientConn(conn, addr, config)
	if err != nil {
		return nil, fmt.Errorf("sftpblob: ssh handshake with %s failed: %v", addr, err)
	}
	sshClient := ssh.NewClient(sshConn, chans, reqs)
	closers = append(closers, sshClient)

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return nil, fmt.Errorf("sftpblob: failed to start the sftp subsystem: %v", err)
	}
	// The bucket owns this client because this function dialed it; see
	// bucket.Close.
	closers = append(closers, sftpClient)

	bucketPath := u.Path
	if bucketPath == "" {
		bucketPath = "/"
	}

	drv, err := openBucket(sftpClient, bucketPath, opts)
	if err != nil {
		return nil, err
	}
	drv.closers = closers
	closers = nil

	return blob.NewBucket(drv), nil
}

// parseBoolParam parses a URL query parameter that must be a boolean. The
// value matters: "?insecure_skip_verify=false" has to mean false.
func parseBoolParam(param, value string) (bool, error) {
	if value == "" {
		return false, fmt.Errorf("expected a boolean, got an empty value")
	}
	b, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("expected a boolean, got %q", value)
	}
	return b, nil
}

// Options sets options for constructing a *blob.Bucket backed by sftpblob.
type Options struct {
	// CreateDir specifies whether the bucket root directory should be created
	// if it does not already exist.
	CreateDir bool

	Metadata MetadataOption
}

type bucket struct {
	client *sftp.Client
	dir    string
	opts   *Options

	// closers holds everything this package dialed and therefore owns. A
	// client handed to OpenBucket by the caller is not in here and is never
	// closed; two buckets may share one.
	closers closerList

	// posixRenameUnsupported latches a server's refusal of the
	// posix-rename@openssh.com extension so it is probed once, not per write.
	posixRenameUnsupported atomic.Bool

	// readDir, when set, replaces client.ReadDir during listing. It exists so
	// that a test can count the round trips a page costs; production code
	// leaves it nil.
	readDir func(dir string) ([]fs.FileInfo, error)
}

// openBucket creates a driver.Bucket backed by an sftp.Client.
func openBucket(client *sftp.Client, dir string, opts *Options) (*bucket, error) {
	if opts == nil {
		opts = &Options{}
	}
	// normalize path to absolute remote path using path.Clean
	absdir := path.Clean(dir)

	if opts.CreateDir {
		if err := client.MkdirAll(absdir); err != nil {
			return nil, err
		}
	}

	info, err := client.Stat(absdir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", absdir)
	}

	return &bucket{client: client, dir: absdir, opts: opts}, nil
}

// OpenBucket creates a *blob.Bucket backed by an sftp.Client and rooted at dir.
func OpenBucket(client *sftp.Client, dir string, opts *Options) (*blob.Bucket, error) {
	drv, err := openBucket(client, dir, opts)
	if err != nil {
		return nil, err
	}
	return blob.NewBucket(drv), nil
}

// Close releases only what this package dialed. A *sftp.Client passed to
// OpenBucket belongs to the caller and is left open: closing it would break
// every other bucket sharing that connection.
func (b *bucket) Close() error {
	return b.closers.Close()
}

type closerList []io.Closer

func (l closerList) Close() error {
	var errs []error
	for i := len(l) - 1; i >= 0; i-- {
		if c := l[i]; c != nil {
			errs = append(errs, c.Close())
		}
	}
	return errors.Join(errs...)
}

// Error is the error type sftpblob raises for conditions it detects itself —
// an invalid key, a failed precondition, an unsupported operation — as opposed
// to errors surfaced from the SSH or SFTP layers, which are returned as-is and
// classified by ErrorCode. It is reachable via Bucket.ErrorAs.
type Error struct {
	// Code is the portable error code for this failure.
	Code gcerrors.ErrorCode
	// Key is the blob key the operation was for, if any.
	Key string

	msg string
	err error
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("sftpblob: ")
	b.WriteString(e.msg)
	if e.Key != "" {
		fmt.Fprintf(&b, " (key %q)", e.Key)
	}
	if e.err != nil {
		b.WriteString(": ")
		b.WriteString(e.err.Error())
	}
	return b.String()
}

func (e *Error) Unwrap() error { return e.err }

// errorf builds an Error carrying an explicit portable code.
func errorf(code gcerrors.ErrorCode, err error, format string, args ...any) *Error {
	return &Error{Code: code, err: err, msg: fmt.Sprintf(format, args...)}
}

func (b *bucket) ErrorCode(err error) gcerrors.ErrorCode {
	// Errors raised by this driver carry their code directly.
	var sftpErr *Error
	if errors.As(err, &sftpErr) {
		return sftpErr.Code
	}
	if errors.Is(err, context.Canceled) {
		return gcerrors.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return gcerrors.DeadlineExceeded
	}
	if os.IsNotExist(err) || errors.Is(err, fs.ErrNotExist) {
		return gcerrors.NotFound
	}
	if os.IsPermission(err) || errors.Is(err, fs.ErrPermission) {
		return gcerrors.PermissionDenied
	}
	if os.IsExist(err) || errors.Is(err, fs.ErrExist) {
		return gcerrors.AlreadyExists
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return gcerrors.Internal
	}

	var statusErr *sftp.StatusError
	if errors.As(err, &statusErr) {
		switch statusErr.Code {
		case 2: // SSH_FX_NO_SUCH_FILE
			return gcerrors.NotFound
		case 3, 27: // SSH_FX_PERMISSION_DENIED, SSH_FX_WRITE_PROTECT
			return gcerrors.PermissionDenied
		case 8: // SSH_FX_OP_UNSUPPORTED
			return gcerrors.Unimplemented
		case 11: // SSH_FX_FILE_ALREADY_EXISTS
			return gcerrors.AlreadyExists
		case 6, 7: // SSH_FX_NO_CONNECTION, SSH_FX_CONNECTION_LOST
			return gcerrors.Internal
		case 21, 22, 24: // DIR_NOT_EMPTY, NOT_A_DIRECTORY, FILE_IS_A_DIRECTORY
			return gcerrors.FailedPrecondition
		}
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return gcerrors.DeadlineExceeded
		}
		return gcerrors.Internal
	}

	return gcerrors.Unknown
}

// commitRename moves a staged file onto its final path.
//
// When ifNotExist is set the destination must not already exist. A plain SFTP
// rename is exactly that primitive — SSH_FXP_RENAME fails if the destination
// is taken — so the condition is enforced atomically, with no check-then-act
// window. Otherwise PosixRename is used, which replaces the destination.
func (b *bucket) commitRename(src, dst string, ifNotExist bool) error {
	if ifNotExist {
		// Not every server refuses to clobber on rename, so check first as
		// well. The rename is what makes it race-free where it is honored;
		// this is what makes it correct where it is not.
		if _, err := b.client.Stat(dst); err == nil {
			return errorf(gcerrors.FailedPrecondition, nil, "blob already exists")
		}
		if err := b.client.Rename(src, dst); err != nil {
			if b.ErrorCode(err) == gcerrors.AlreadyExists {
				return errorf(gcerrors.FailedPrecondition, err, "blob already exists")
			}
			return err
		}
		return nil
	}

	// PosixRename is the posix-rename@openssh.com extension. OpenSSH has it;
	// plenty of other servers do not, and without a fallback every write and
	// copy against them fails.
	if !b.posixRenameUnsupported.Load() {
		err := b.client.PosixRename(src, dst)
		if err == nil {
			return nil
		}
		if b.ErrorCode(err) != gcerrors.Unimplemented {
			return err
		}
		b.posixRenameUnsupported.Store(true)
	}
	// Fallback: remove-then-rename. This is not atomic — a reader between the
	// two steps sees no blob at all — but it is the only option the base
	// protocol offers.
	if err := b.client.Remove(dst); err != nil && b.ErrorCode(err) != gcerrors.NotFound {
		return err
	}
	return b.client.Rename(src, dst)
}

func (b *bucket) fullPath(key string) (string, error) {
	if key == "" {
		return "", errorf(gcerrors.InvalidArgument, nil, "key is required")
	}
	ek := escape.KeyEscape(key)
	p := path.Join(b.dir, ek)
	dirPrefix := b.dir
	if !strings.HasSuffix(dirPrefix, "/") {
		dirPrefix += "/"
	}
	if !strings.HasPrefix(p+"/", dirPrefix) {
		return "", errorf(gcerrors.InvalidArgument, nil, "key %q escapes the bucket root", key)
	}
	if strings.HasSuffix(p, attrsExt) {
		return "", errorf(gcerrors.InvalidArgument, nil, "file extension %q is reserved", attrsExt)
	}
	if strings.Contains(p, tempInfix) {
		return "", errorf(gcerrors.InvalidArgument, nil, "%q is reserved and may not appear in a key", tempInfix)
	}
	return p, nil
}

// xattrs stores extended attributes
type xattrs struct {
	CacheControl       string            `json:"user.cache_control"`
	ContentDisposition string            `json:"user.content_disposition"`
	ContentEncoding    string            `json:"user.content_encoding"`
	ContentLanguage    string            `json:"user.content_language"`
	ContentType        string            `json:"user.content_type"`
	Metadata           map[string]string `json:"user.metadata"`
	MD5                []byte            `json:"md5"`
}

func getAttrs(ctx context.Context, client *sftp.Client, fullPath string) (xattrs, error) {
	if err := ctx.Err(); err != nil {
		return xattrs{}, err
	}
	f, err := client.Open(fullPath + attrsExt)
	if err != nil {
		if os.IsNotExist(err) || errors.Is(err, fs.ErrNotExist) {
			return xattrs{ContentType: "application/octet-stream"}, nil
		}
		return xattrs{}, err
	}
	defer func() { _ = f.Close() }()
	xa := new(xattrs)
	err = json.NewDecoder(f).Decode(xa)
	return *xa, err
}

func setAttrs(ctx context.Context, client *sftp.Client, fullPath string, xa xattrs) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := client.Create(fullPath + attrsExt)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(f).Encode(xa); err != nil {
		_ = f.Close()
		_ = client.Remove(f.Name())
		return err
	}
	return f.Close()
}

func (b *bucket) forKey(ctx context.Context, key string) (string, fs.FileInfo, *xattrs, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, nil, err
	}
	p, err := b.fullPath(key)
	if err != nil {
		return "", nil, nil, err
	}
	info, err := b.client.Stat(p)
	if err != nil {
		return "", nil, nil, err
	}
	if info.IsDir() {
		return "", nil, nil, os.ErrNotExist
	}
	xa, err := getAttrs(ctx, b.client, p)
	if err != nil {
		return "", nil, nil, err
	}
	return p, info, &xa, nil
}

func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	_, info, xa, err := b.forKey(ctx, key)
	if err != nil {
		return nil, err
	}
	return &driver.Attributes{
		CacheControl:       xa.CacheControl,
		ContentDisposition: xa.ContentDisposition,
		ContentEncoding:    xa.ContentEncoding,
		ContentLanguage:    xa.ContentLanguage,
		ContentType:        xa.ContentType,
		Metadata:           xa.Metadata,
		ModTime:            info.ModTime(),
		Size:               info.Size(),
		MD5:                xa.MD5,
		ETag:               fmt.Sprintf("\"%x-%x\"", info.ModTime().UnixNano(), info.Size()),
		AsFunc: func(i any) bool {
			if p, ok := i.(*fs.FileInfo); ok {
				*p = info
				return true
			}
			return false
		},
	}, nil
}

func (b *bucket) As(i any) bool {
	if p, ok := i.(**sftp.Client); ok {
		*p = b.client
		return true
	}
	return false
}

func (b *bucket) ErrorAs(err error, i any) bool {
	switch v := err.(type) {
	case *sftp.StatusError:
		if p, ok := i.(**sftp.StatusError); ok {
			*p = v
			return true
		}
	case *os.PathError:
		if p, ok := i.(**os.PathError); ok {
			*p = v
			return true
		}
	}
	return false
}

func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	p, info, xa, err := b.forKey(ctx, key)
	if err != nil {
		return nil, err
	}
	f, err := b.client.Open(p)
	if err != nil {
		return nil, err
	}

	if opts.BeforeRead != nil {
		if err := opts.BeforeRead(func(i any) bool {
			if p, ok := i.(**sftp.File); ok {
				*p = f
				return true
			}
			return false
		}); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	r := io.Reader(f)
	if length >= 0 {
		r = io.LimitReader(r, length)
	}
	return &reader{
		ctx: ctx,
		r:   r,
		c:   f,
		attrs: driver.ReaderAttributes{
			ContentType: xa.ContentType,
			ModTime:     info.ModTime(),
			Size:        info.Size(),
		},
	}, nil
}

type reader struct {
	ctx   context.Context
	r     io.Reader
	c     io.Closer
	attrs driver.ReaderAttributes
}

func (r *reader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.r == nil {
		return 0, io.EOF
	}
	return r.r.Read(p)
}

func (r *reader) Close() error {
	if r.c == nil {
		return nil
	}
	return r.c.Close()
}

func (r *reader) Attributes() *driver.ReaderAttributes {
	return &r.attrs
}

func (r *reader) As(i any) bool {
	if p, ok := i.(*io.Reader); ok {
		*p = r.r
		return true
	}
	return false
}

func (b *bucket) NewTypedWriter(ctx context.Context, key, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	if key == "" {
		return nil, errors.New("sftpblob: invalid key (empty string)")
	}
	p, err := b.fullPath(key)
	if err != nil {
		return nil, err
	}
	if err := b.client.MkdirAll(path.Dir(p)); err != nil {
		return nil, err
	}

	var f *sftp.File
	var tempPath string

	// Every write is staged to a temporary file and renamed into place, so a
	// canceled or failed write cannot leave a partial blob at the real key.
	// IfNotExist used to be the exception -- it opened the final path with
	// O_EXCL and wrote in place -- which meant precisely the conditional
	// writes were the ones that could be torn in half. The condition is
	// enforced at the rename instead: a non-clobbering rename fails when the
	// destination exists, which is both atomic and free of a check-then-act
	// race.
	{
		tempPath = p + tempInfix + strconv.FormatInt(time.Now().UnixNano(), 16)
		f, err = b.client.OpenFile(tempPath, os.O_RDWR|os.O_CREATE|os.O_EXCL)
		if err != nil {
			return nil, err
		}
	}

	if opts.BeforeWrite != nil {
		if err := opts.BeforeWrite(func(i any) bool {
			if p, ok := i.(**sftp.File); ok {
				*p = f
				return true
			}
			return false
		}); err != nil {
			_ = f.Close()
			if tempPath != "" {
				_ = b.client.Remove(tempPath)
			}
			return nil, err
		}
	}

	if b.opts.Metadata == MetadataDontWrite {
		return &writer{
			ctx:        ctx,
			b:          b,
			client:     b.client,
			f:          f,
			path:       p,
			tmp:        tempPath,
			contentMD5: opts.ContentMD5,
			md5hash:    md5.New(),
			ifNotExist: opts.IfNotExist,
			mu:         &sync.Mutex{},
		}, nil
	}

	var metadata map[string]string
	if len(opts.Metadata) > 0 {
		metadata = opts.Metadata
	}
	attrs := xattrs{
		CacheControl:       opts.CacheControl,
		ContentDisposition: opts.ContentDisposition,
		ContentEncoding:    opts.ContentEncoding,
		ContentLanguage:    opts.ContentLanguage,
		ContentType:        contentType,
		Metadata:           metadata,
	}

	return &writerWithSidecar{
		ctx:        ctx,
		b:          b,
		client:     b.client,
		f:          f,
		path:       p,
		tmp:        tempPath,
		attrs:      attrs,
		contentMD5: opts.ContentMD5,
		md5hash:    md5.New(),
		ifNotExist: opts.IfNotExist,
		mu:         &sync.Mutex{},
	}, nil
}

type writer struct {
	ctx        context.Context
	b          *bucket
	client     *sftp.Client
	f          *sftp.File
	path       string
	tmp        string
	contentMD5 []byte
	md5hash    hash.Hash
	mu         *sync.Mutex
	closed     bool
	ifNotExist bool
	failed     bool
}

func (w *writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, errors.New("sftpblob: already closed")
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := w.f.Write(p)
	if err != nil {
		w.md5hash.Write(p[:n])
		w.failed = true
		return n, err
	}
	if _, err := w.md5hash.Write(p); err != nil {
		w.failed = true
		return n, err
	}
	return n, nil
}

func (w *writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("sftpblob: already closed")
	}
	w.closed = true

	// Release the remote handle on every return path. Closing it inline used
	// to be skipped whenever an earlier check returned first, and the server
	// held the handle open until the connection dropped.
	closeErr := w.f.Close()

	// The staged file is removed unless the commit below succeeds.
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = w.client.Remove(w.tmp)
		}
	}()

	if w.failed {
		return errors.New("sftpblob: refusing to commit incomplete file write")
	}
	if closeErr != nil {
		return closeErr
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}

	if len(w.contentMD5) > 0 && !bytes.Equal(w.contentMD5, w.md5hash.Sum(nil)) {
		return errorf(gcerrors.FailedPrecondition, nil, "sftpblob: MD5 checksum mismatch")
	}

	err := w.b.commitRename(w.tmp, w.path, w.ifNotExist)
	if err == nil {
		removeTmp = false
	}
	return err
}

type writerWithSidecar struct {
	ctx        context.Context
	b          *bucket
	client     *sftp.Client
	f          *sftp.File
	path       string
	tmp        string
	attrs      xattrs
	contentMD5 []byte
	md5hash    hash.Hash
	mu         *sync.Mutex
	closed     bool
	ifNotExist bool
	failed     bool
}

func (w *writerWithSidecar) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, errors.New("sftpblob: already closed")
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := w.f.Write(p)
	if err != nil {
		w.md5hash.Write(p[:n])
		w.failed = true
		return n, err
	}
	if _, err := w.md5hash.Write(p); err != nil {
		w.failed = true
		return n, err
	}
	return n, nil
}

func (w *writerWithSidecar) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("sftpblob: already closed")
	}
	w.closed = true

	// Release the remote handle on every return path; see writer.Close.
	closeErr := w.f.Close()

	removeTmp := true
	defer func() {
		if removeTmp {
			_ = w.client.Remove(w.tmp + attrsExt)
			_ = w.client.Remove(w.tmp)
		}
	}()

	if w.failed {
		return errors.New("sftpblob: refusing to commit incomplete file write")
	}
	if closeErr != nil {
		return closeErr
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}

	sum := w.md5hash.Sum(nil)
	if len(w.contentMD5) > 0 && !bytes.Equal(w.contentMD5, sum) {
		return errorf(gcerrors.FailedPrecondition, nil, "sftpblob: MD5 checksum mismatch")
	}
	w.attrs.MD5 = sum

	// Stage the sidecar next to the staged blob, so that a conditional write
	// refused at the rename below leaves the existing blob's metadata alone.
	if err := setAttrs(w.ctx, w.client, w.tmp, w.attrs); err != nil {
		return err
	}
	if err := w.b.commitRename(w.tmp, w.path, w.ifNotExist); err != nil {
		return err
	}
	// The blob is in place; only its metadata is at risk from here.
	removeTmp = false
	return w.b.commitRename(w.tmp+attrsExt, w.path+attrsExt, false)
}

func (b *bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {
	if dstKey == "" || srcKey == "" {
		return errors.New("sftpblob: empty key")
	}
	srcPath, err := b.fullPath(srcKey)
	if err != nil {
		return err
	}
	dstPath, err := b.fullPath(dstKey)
	if err != nil {
		return err
	}

	srcFile, err := b.client.Open(srcPath)
	if err != nil {
		return err
	}
	defer func() { _ = srcFile.Close() }()

	if opts.BeforeCopy != nil {
		if err := opts.BeforeCopy(func(i any) bool {
			if p, ok := i.(**sftp.File); ok {
				*p = srcFile
				return true
			}
			return false
		}); err != nil {
			return err
		}
	}

	if err := b.client.MkdirAll(path.Dir(dstPath)); err != nil {
		return err
	}
	tempPath := dstPath + tempInfix + strconv.FormatInt(time.Now().UnixNano(), 16)
	dstFile, err := b.client.OpenFile(tempPath, os.O_RDWR|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		_ = dstFile.Close()
		_ = b.client.Remove(tempPath)
		return err
	}

	_, err = io.Copy(dstFile, srcFile)
	errClose := dstFile.Close()
	if err == nil {
		err = errClose
	}
	if err != nil {
		_ = b.client.Remove(tempPath)
		return err
	}

	var wroteSidecar bool
	if b.opts.Metadata != MetadataDontWrite {
		xa, err := getAttrs(ctx, b.client, srcPath)
		if err != nil {
			_ = b.client.Remove(tempPath)
			return err
		}
		if err := setAttrs(ctx, b.client, tempPath, xa); err != nil {
			_ = b.client.Remove(tempPath)
			return err
		}
		wroteSidecar = true
	}

	err = b.commitRename(tempPath, dstPath, false)
	if err != nil {
		if wroteSidecar {
			_ = b.client.Remove(tempPath + attrsExt)
		}
		_ = b.client.Remove(tempPath)
		return err
	}

	if wroteSidecar {
		err = b.commitRename(tempPath+attrsExt, dstPath+attrsExt, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *bucket) Delete(ctx context.Context, key string, opts *driver.DeleteOptions) error {
	p, err := b.fullPath(key)
	if err != nil {
		return err
	}
	if opts != nil && opts.BeforeDelete != nil {
		if err := opts.BeforeDelete(func(i any) bool {
			if c, ok := i.(**sftp.Client); ok {
				*c = b.client
				return true
			}
			return false
		}); err != nil {
			return err
		}
	}
	if err := b.client.Remove(p); err != nil {
		return err
	}
	// A missing sidecar is not an error: the blob may have been written with
	// metadata=skip, or by something other than the Go CDK.
	_ = b.client.Remove(p + attrsExt)
	return nil
}

func (b *bucket) SignedURL(ctx context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	return "", errorf(gcerrors.Unimplemented, nil, "sftpblob: SignedURL not implemented")
}
