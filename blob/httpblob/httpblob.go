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

// Package httpblob provides a blob implementation backed by an HTTP server.
// Use OpenBucket to construct a *blob.Bucket.
//
// # Protocols
//
// httpblob speaks two standard protocols, selected by Options.Protocol:
//
//   - ProtocolHTTP talks plain HTTP (RFC 9110) to any web server. It is
//     read-only: Attributes, NewRangeReader and Download are supported, using
//     HEAD, GET and Range requests. ListPaged, NewTypedWriter, Copy, Delete and
//     SignedURL return an error for which gcerrors.Code returns Unimplemented.
//   - ProtocolWebDAV talks WebDAV (RFC 4918), which adds the PUT, DELETE, COPY,
//     MOVE, MKCOL and PROPFIND methods needed for a read-write bucket. It works
//     against any conforming WebDAV server, including Apache mod_dav, the nginx
//     dav module, and golang.org/x/net/webdav.
//
// httpblob does not probe the server to decide which to use; the protocol comes
// from Options.Protocol or from the URL scheme.
//
// # Attributes
//
// Most HTTP servers store only bytes, with no place to record a blob's
// ContentType, Metadata or MD5. Under ProtocolWebDAV, httpblob therefore stores
// them in "sidecar" objects alongside each blob, under the same key with an
// additional ".attrs" suffix. The format is identical to fileblob's and
// sftpblob's, so a bucket written by any of the three is readable by the others.
// Sidecars can be suppressed with Options.Metadata = MetadataDontWrite, or
// "metadata=skip" in the URL; absent stored metadata, many blob.Attributes
// fields will be set to default values.
//
// Under ProtocolHTTP, sidecars are never fetched — that would double the number
// of requests against servers that know nothing about them — so attributes come
// only from response headers and Attributes.Metadata is always empty.
//
// # Writes
//
// A write under ProtocolWebDAV PUTs to a temporary key, MOVEs it into place on
// Close, and then writes the sidecar, so that a canceled or failed write leaves
// any previous blob intact.
//
// The MOVE and the sidecar write cannot be made atomic with respect to each
// other, so a write that fails at the last step leaves the new blob in place
// with the previous blob's metadata. The error is reported to the caller.
//
// Two operational consequences are worth planning for:
//
//   - A write that is canceled or fails deletes its own temporary object, but a
//     process that dies mid-write cannot. Such objects are named with a
//     ".gocdktmp." infix and are hidden from List, so they accumulate
//     unnoticed; a bucket written to by long-lived processes should be swept
//     for them periodically.
//   - Deleting a blob does not delete the collection that held it, so an empty
//     collection can outlive its last key. List does not report empty
//     collections as "directories", but they do cost a request to skip.
//
// Both ".attrs" as a suffix and ".gocdktmp." anywhere are reserved: keys
// containing them are rejected with gcerrors.InvalidArgument, so that a blob
// can never be confused with httpblob's own bookkeeping.
//
// # Retries
//
// Requests using a safe method (GET, HEAD, PROPFIND, OPTIONS) are retried on
// transport errors, 5xx responses and 429 responses, honoring Retry-After.
// Requests that mutate the bucket are never retried: a PUT body is a one-shot
// stream that cannot be replayed, and a retried DELETE or MOVE that actually
// succeeded the first time reports a spurious 404.
//
// # URLs
//
// For blob.OpenBucket, httpblob registers for the schemes "http" and "https"
// (read-only) and "webdav" and "webdavs" (read-write); "webdav" connects over
// http and "webdavs" over https. To customize the URL opener, or for more
// details on the URL format, see URLOpener.
// See https://gocloud.dev/concepts/urls/ for background information.
//
// "http" and "https" are generic enough that another package may want them
// too, and blob.URLMux panics on a duplicate registration with no way to
// unregister. httpblob therefore claims a scheme only if it is still free, so
// a collision costs you the scheme rather than crashing the process at init.
// Which package wins is decided by init order, which Go does not define across
// packages. If you need certainty, do not rely on the default mux: register
// with your own blob.URLMux, or call OpenBucket directly.
//
// # Escaping
//
// Go CDK supports all UTF-8 strings; to make this work with services lacking
// full UTF-8 support, strings must be escaped (during writes) and unescaped
// (during reads). The following escapes are performed for httpblob:
//   - Blob keys: ASCII characters 0-31 are escaped to "__0x<hex>__".
//     Additionally, the "/" in "../", the trailing "/" in "//", and a trailing
//     "/" in key names are escaped in the same way.
//     The characters "\<>:"|?*" are also escaped, because WebDAV servers are
//     commonly backed by a filesystem that cannot represent them.
//
// # As
//
// httpblob exposes the following types for As:
//   - Bucket: *http.Client
//   - Error: *httpblob.Error
//   - Reader: *http.Response
//   - Attributes: http.Header
//   - ListObject: http.Header
//   - ListOptions.BeforeList, ReaderOptions.BeforeRead,
//     WriterOptions.BeforeWrite, CopyOptions.BeforeCopy,
//     DeleteOptions.BeforeDelete: *http.Request
package httpblob // import "github.com/TuSKan/gocloud-ext/blob/httpblob"

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/TuSKan/gocloud-ext/internal/escape"
	"github.com/TuSKan/gocloud-ext/internal/useragent"
	gax "github.com/googleapis/gax-go/v2"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"
)

// Scheme constants for the URL openers registered by this package.
const (
	// SchemeHTTP and SchemeHTTPS open a read-only bucket over plain HTTP.
	SchemeHTTP  = "http"
	SchemeHTTPS = "https"
	// SchemeWebDAV and SchemeWebDAVS open a read-write bucket over WebDAV,
	// connecting over http and https respectively.
	SchemeWebDAV  = "webdav"
	SchemeWebDAVS = "webdavs"
)

const (
	defaultPageSize   = 1000
	defaultMaxRetries = 3

	// tempInfix marks in-progress writes so they can be excluded from List.
	tempInfix = ".gocdktmp."

	// maxErrorBodySize bounds how much of an error response body is captured.
	maxErrorBodySize = 1024

	// maxResumes bounds mid-stream resume attempts for a single Reader.
	maxResumes = 3

	// cleanupTimeout bounds the deletion of a staged object after a failed or
	// canceled write. It runs inside Close, so it must not be long.
	cleanupTimeout = 5 * time.Second
)

func init() { registerSchemes() }

// registerSchemes claims this package's URL schemes on the default mux,
// skipping any that are already taken.
//
// RegisterBucket panics if the scheme is taken, and nothing can unregister it.
// "http" and "https" are generic enough that another package may reasonably
// want them, and an unrecoverable panic in someone else's binary is a worse
// outcome than not claiming the scheme. See the package documentation.
func registerSchemes() {
	o := new(URLOpener)
	for _, scheme := range []string{SchemeHTTP, SchemeHTTPS, SchemeWebDAV, SchemeWebDAVS} {
		if !blob.DefaultURLMux().ValidBucketScheme(scheme) {
			blob.DefaultURLMux().RegisterBucket(scheme, o)
		}
	}
}

// Protocol selects which HTTP-based protocol httpblob speaks to the server.
type Protocol int

const (
	// ProtocolHTTP uses plain HTTP (RFC 9110). Buckets are read-only.
	ProtocolHTTP Protocol = iota
	// ProtocolWebDAV uses WebDAV (RFC 4918). Buckets are read-write.
	ProtocolWebDAV
)

func (p Protocol) String() string {
	switch p {
	case ProtocolHTTP:
		return "http"
	case ProtocolWebDAV:
		return "webdav"
	default:
		return fmt.Sprintf("Protocol(%d)", int(p))
	}
}

type metadataOption string

// Settings for Options.Metadata.
const (
	// MetadataInSidecar stores blob metadata in a ".attrs" object alongside
	// each blob. This is the default under ProtocolWebDAV.
	MetadataInSidecar metadataOption = ""
	// MetadataDontWrite does not write metadata at all, and reports default
	// values when reading it.
	MetadataDontWrite metadataOption = "skip"
)

// Options sets options for constructing a *blob.Bucket backed by an HTTP server.
type Options struct {
	// Protocol selects plain HTTP (read-only) or WebDAV (read-write).
	// Defaults to ProtocolHTTP.
	Protocol Protocol

	// Metadata controls how blob metadata is stored. It is ignored under
	// ProtocolHTTP, which never reads or writes sidecars.
	// If left unchanged, MetadataInSidecar is used.
	Metadata metadataOption

	// AuthToken is sent as an "Authorization: Bearer" header on every request.
	// It takes precedence over BasicAuthUser/BasicAuthPassword.
	AuthToken string

	// BasicAuthUser and BasicAuthPassword are sent using HTTP Basic
	// Authentication (RFC 7617) on every request.
	BasicAuthUser     string
	BasicAuthPassword string

	// Headers are sent on every request, after authentication headers are set.
	Headers http.Header

	// MaxRetries is the number of times a retryable request is retried before
	// giving up; see the package documentation for which requests are
	// retryable. If <= 0, a default of 3 is used.
	MaxRetries int
}

// URLOpener opens HTTP and WebDAV URLs like "https://example.com/bucket" or
// "webdavs://user:pass@example.com/bucket".
//
// The URL's host and path give the bucket's base URL; blob keys are appended to
// it. Credentials in the URL's userinfo are used for HTTP Basic Authentication.
//
// The following query parameters are supported:
//
//   - metadata: "skip" to set Options.Metadata to MetadataDontWrite.
//   - auth_token: sets Options.AuthToken.
//   - max_retries: sets Options.MaxRetries.
//
// Any other query parameter is an error.
type URLOpener struct {
	// Client is the http.Client used to make requests. If nil,
	// http.DefaultClient is used.
	Client *http.Client

	// Options specifies the default options for opened buckets. Protocol is
	// always overridden by the URL's scheme.
	Options Options
}

// OpenBucketURL opens a blob.Bucket based on u.
func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	opts := o.Options

	var transport string
	switch scheme := strings.ToLower(u.Scheme); scheme {
	case SchemeHTTP:
		opts.Protocol, transport = ProtocolHTTP, "http"
	case SchemeHTTPS:
		opts.Protocol, transport = ProtocolHTTP, "https"
	case SchemeWebDAV:
		opts.Protocol, transport = ProtocolWebDAV, "http"
	case SchemeWebDAVS:
		opts.Protocol, transport = ProtocolWebDAV, "https"
	default:
		return nil, fmt.Errorf("open bucket %v: unsupported scheme %q", u, scheme)
	}

	if u.User != nil {
		opts.BasicAuthUser = u.User.Username()
		opts.BasicAuthPassword, _ = u.User.Password()
	}

	for param, values := range u.Query() {
		value := values[0]
		switch param {
		case "metadata":
			switch metadataOption(value) {
			case MetadataDontWrite:
				opts.Metadata = MetadataDontWrite
			case MetadataInSidecar:
				opts.Metadata = MetadataInSidecar
			default:
				return nil, fmt.Errorf("open bucket %v: invalid value %q for query parameter %q", u, value, param)
			}
		case "auth_token":
			opts.AuthToken = value
		case "max_retries":
			n, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("open bucket %v: invalid value %q for query parameter %q: %w", u, value, param, err)
			}
			opts.MaxRetries = n
		default:
			return nil, fmt.Errorf("open bucket %v: invalid query parameter %q", u, param)
		}
	}

	baseURL := &url.URL{Scheme: transport, Host: u.Host, Path: u.Path}
	return OpenBucket(ctx, o.Client, baseURL.String(), &opts)
}

// OpenBucket creates a *blob.Bucket backed by the HTTP server at baseURL. Blob
// keys are appended to baseURL to form the URL of each object.
//
// baseURL must already exist on the server; like fileblob, httpblob does not
// create the bucket itself. It does create the collections for keys written
// beneath it.
//
// If client is nil, http.DefaultClient is used. The bucket does not take
// ownership of client; closing the bucket leaves it usable.
func OpenBucket(ctx context.Context, client *http.Client, baseURL string, opts *Options) (*blob.Bucket, error) {
	drv, err := openBucket(ctx, client, baseURL, opts)
	if err != nil {
		return nil, err
	}
	return blob.NewBucket(drv), nil
}

func openBucket(_ context.Context, client *http.Client, baseURL string, opts *Options) (driver.Bucket, error) {
	if opts == nil {
		opts = &Options{}
	}
	if client == nil {
		client = http.DefaultClient
	}
	if baseURL == "" {
		return nil, errors.New("httpblob.OpenBucket: baseURL is required")
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("httpblob.OpenBucket: parsing baseURL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("httpblob.OpenBucket: baseURL scheme must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("httpblob.OpenBucket: baseURL must include a host")
	}
	// Normalize away a trailing slash so that joining keys is unambiguous.
	u.Path = strings.TrimSuffix(u.Path, "/")
	u.RawQuery, u.Fragment = "", ""

	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultMaxRetries
	}

	// Go follows a 301/302/303 by reissuing the request as a GET. That is
	// correct for browsers and catastrophic for us: Apache redirects a
	// collection URL to its trailing-slash form, so a PROPFIND silently
	// becomes a directory read, and a redirected DELETE would silently become
	// a GET — reporting success while deleting nothing. Only GET and HEAD may
	// follow redirects.
	client = withSafeRedirects(client)
	return &bucket{
		client:     useragent.HTTPClient(client, "httpblob"),
		baseURL:    u,
		opts:       *opts,
		maxRetries: maxRetries,
		backoff: gax.Backoff{
			Initial:    100 * time.Millisecond,
			Max:        5 * time.Second,
			Multiplier: 2,
		},
		knownCollections: map[string]bool{},
	}, nil
}

type bucket struct {
	client     *http.Client
	baseURL    *url.URL
	opts       Options
	maxRetries int
	backoff    gax.Backoff

	// mkcolMu serializes collection creation. WebDAV servers lock the target
	// of a MKCOL, so concurrent writers racing to create the same ancestors
	// make each other fail with 423 Locked.
	mkcolMu sync.Mutex

	mu sync.Mutex
	// knownCollections caches WebDAV collections we have already created, so
	// that writing many blobs into one "directory" doesn't re-MKCOL its
	// ancestors every time.
	knownCollections map[string]bool
}

// Error is the error type returned for a non-2xx HTTP response. It is exposed
// via Bucket.ErrorAs.
type Error struct {
	// Method and URL identify the request that failed. They are empty for an
	// error the driver raised on its own, without an HTTP exchange.
	Method string
	URL    string
	// Key is the blob key the request was for, if any.
	Key string
	// StatusCode and Status are the HTTP response status, e.g. 404 and
	// "404 Not Found". They are zero for an error the driver raised on its own.
	StatusCode int
	Status     string
	// Body is the response body, truncated to a reasonable length.
	Body string

	// code is set for errors the driver raises itself — an invalid key, an
	// operation the protocol cannot support — where there is no status to
	// derive a code from. When unset, ErrorCode maps StatusCode instead.
	code gcerrors.ErrorCode
	msg  string
	err  error
}

func (e *Error) Unwrap() error { return e.err }

func (e *Error) Error() string {
	if e.StatusCode == 0 {
		// Raised by the driver; there is no request to describe.
		if e.err != nil {
			return e.msg + ": " + e.err.Error()
		}
		return e.msg
	}
	var b strings.Builder
	b.WriteString("httpblob: ")
	b.WriteString(e.Method)
	b.WriteString(" ")
	b.WriteString(e.URL)
	if e.Key != "" {
		fmt.Fprintf(&b, " (key %q)", e.Key)
	}
	b.WriteString(": ")
	if e.Status != "" {
		b.WriteString(e.Status)
	} else {
		b.WriteString(strconv.Itoa(e.StatusCode))
	}
	if e.Body != "" {
		b.WriteString(": ")
		b.WriteString(strings.TrimSpace(e.Body))
	}
	return b.String()
}

// errorf builds an error the driver is raising on its own, carrying the
// gcerrors code explicitly because there is no HTTP status to derive one from.
func errorf(code gcerrors.ErrorCode, err error, format string, args ...any) *Error {
	return &Error{code: code, err: err, msg: fmt.Sprintf(format, args...)}
}

// newError builds an Error from a failed response, consuming and closing its
// body.
func newError(req *http.Request, resp *http.Response) *Error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	_ = resp.Body.Close()
	return &Error{
		Method:     req.Method,
		URL:        req.URL.String(),
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		Body:       string(body),
	}
}

// maxRedirects matches net/http's own default limit.
const maxRedirects = 10

// withSafeRedirects returns a copy of client that only follows redirects for
// GET and HEAD. For every other method Go would reissue the request as a GET,
// changing what the request means; the caller sees the redirect response
// instead and decides.
func withSafeRedirects(client *http.Client) *http.Client {
	c := *client
	c.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		switch via[0].Method {
		case http.MethodGet, http.MethodHead:
		default:
			return http.ErrUseLastResponse
		}
		if len(via) >= maxRedirects {
			return fmt.Errorf("stopped after %d redirects", maxRedirects)
		}
		return nil
	}
	return &c
}

// isRedirect reports whether err carries a 3xx status.
func isRedirect(err error) bool {
	status := statusOf(err)
	return status >= 300 && status < 400
}

// statusOf returns the HTTP status carried by err, or 0 if it isn't an *Error.
// headRefused reports whether a status means the server will not answer HEAD,
// as opposed to meaning the object is not there.
//
// The distinction decides whether falling back to a ranged GET is worth a
// round trip. Three statuses say the method rather than the object is the
// problem:
//
//   - 405 Method Not Allowed is the canonical one, and what RFC 9110 section
//     15.5.6 describes for a target that does not support the method.
//   - 501 Not Implemented is what a server sends when it does not implement
//     the method at all.
//   - 403 Forbidden is what a good deal of deployed infrastructure sends in
//     practice: reverse proxies, API gateways and WAFs that route only GET
//     commonly reject HEAD as forbidden rather than as unsupported. Harvard
//     Dataverse, which serves a large share of the world's archived research
//     data, does exactly this - HEAD is 403 while ranged GET is served
//     normally, with Content-Range and an ETag.
//
// 403 is the one that could be read either way, and including it costs at most
// one wasted request: when the refusal is genuine the ranged GET fails too,
// and headObject then reports the original HEAD error rather than the
// fallback's. So a real refusal stays a real refusal.
//
// 404 is deliberately absent. An object that is not there must fail on the
// HEAD, or every missing key costs a pointless GET and reports a confusing
// error.
func headRefused(status int) bool {
	switch status {
	case http.StatusForbidden,
		http.StatusMethodNotAllowed,
		http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

func statusOf(err error) int {
	var httpErr *Error
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode
	}
	return 0
}

// notReadable rewrites a 405 on a read as a 404. Both HTTP and WebDAV use 405
// to say "you can't GET this", which for a blob bucket means the key does not
// name a readable object — most often because it is a WebDAV collection.
func notReadable(err error) error {
	var httpErr *Error
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusMethodNotAllowed {
		return err
	}
	return &Error{
		Method:     httpErr.Method,
		URL:        httpErr.URL,
		Key:        httpErr.Key,
		StatusCode: http.StatusNotFound,
		Status:     "404 Not Found",
		Body:       "the key does not name a readable object",
	}
}

// explainConflict expands the bare 409 a WebDAV server returns for a PUT or
// MKCOL whose parent collection is missing (RFC 4918 §9.7.1). By far the most
// common cause is that the bucket's base URL doesn't exist on the server, which
// is worth saying out loud. It wraps rather than replaces, so ErrorCode and
// ErrorAs still see the underlying *Error.
func explainConflict(err error, baseURL string) error {
	if statusOf(err) != http.StatusConflict {
		return err
	}
	return fmt.Errorf("%w (a parent collection does not exist; check that the bucket's base URL %q exists on the server, as httpblob does not create it)", err, baseURL)
}

// withKey annotates err with key, so that "not found" errors name the blob that
// was missing.
func withKey(err error, key string) error {
	var httpErr *Error
	if errors.As(err, &httpErr) && httpErr.Key == "" {
		httpErr.Key = key
	}
	return err
}

func (b *bucket) ErrorCode(err error) gcerrors.ErrorCode {
	if err == nil {
		return gcerrors.OK
	}
	var httpErr *Error
	if errors.As(err, &httpErr) {
		// Errors the driver raised itself carry their code directly; there is
		// no status to map.
		if httpErr.code != gcerrors.OK {
			return httpErr.code
		}
		switch httpErr.StatusCode {
		case http.StatusNotFound, http.StatusGone:
			return gcerrors.NotFound
		case http.StatusUnauthorized, http.StatusForbidden:
			return gcerrors.PermissionDenied
		case http.StatusPreconditionFailed, http.StatusConflict:
			// 412 is what a WebDAV server returns for MOVE with
			// "Overwrite: F" when the destination exists, which is how
			// WriterOptions.IfNotExist is enforced.
			return gcerrors.FailedPrecondition
		case http.StatusMethodNotAllowed, http.StatusNotImplemented:
			return gcerrors.Unimplemented
		case http.StatusBadRequest, http.StatusUnprocessableEntity,
			http.StatusRequestedRangeNotSatisfiable:
			return gcerrors.InvalidArgument
		case http.StatusRequestTimeout, http.StatusGatewayTimeout:
			return gcerrors.DeadlineExceeded
		case http.StatusTooManyRequests, http.StatusInsufficientStorage:
			return gcerrors.ResourceExhausted
		}
		if httpErr.StatusCode >= 500 {
			return gcerrors.Internal
		}
		return gcerrors.Unknown
	}
	if errors.Is(err, context.Canceled) {
		return gcerrors.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return gcerrors.DeadlineExceeded
	}
	return gcerrors.Unknown
}

func (b *bucket) As(i any) bool {
	p, ok := i.(**http.Client)
	if !ok {
		return false
	}
	*p = b.client
	return true
}

func (b *bucket) ErrorAs(err error, i any) bool {
	switch p := i.(type) {
	case **Error:
		var httpErr *Error
		if errors.As(err, &httpErr) {
			*p = httpErr
			return true
		}
	}
	return false
}

// Close implements driver.Bucket. The http.Client passed to OpenBucket is owned
// by the caller, so there is nothing to release.
func (b *bucket) Close() error { return nil }

// escapedPath returns the bucket-relative, escaped path for key: the form used
// on the wire, before per-segment URL escaping.
func (b *bucket) escapedPath(key string) (string, error) {
	if key == "" {
		return "", errorf(gcerrors.InvalidArgument, nil, "httpblob: key is required")
	}
	if strings.HasSuffix(key, attrsExt) {
		return "", errorf(gcerrors.InvalidArgument, errAttrsExt, "httpblob: invalid key %q", key)
	}
	if strings.Contains(key, tempInfix) {
		return "", errorf(gcerrors.InvalidArgument, nil, "httpblob: %q is reserved and may not appear in a key", tempInfix)
	}
	p := escape.KeyEscape(key)
	// escapeKey neutralizes "../" and leading/duplicate slashes, but a leading
	// "/" would still reset the path when joined; reject rather than silently
	// reinterpret.
	if strings.HasPrefix(p, "/") {
		return "", errorf(gcerrors.InvalidArgument, nil, "httpblob: key %q may not start with %q", key, "/")
	}
	return p, nil
}

// objectURL returns the absolute URL for key.
func (b *bucket) objectURL(key string) (string, error) {
	p, err := b.escapedPath(key)
	if err != nil {
		return "", err
	}
	return b.pathURL(p), nil
}

// pathURL turns a bucket-relative escaped path into an absolute URL.
// escapedPath is the literal path we want on the wire; leaving RawPath empty
// lets url.URL.String percent-encode whatever still needs it (spaces, "#",
// non-ASCII) while keeping "/" as a separator.
func (b *bucket) pathURL(escapedPath string) string {
	u := *b.baseURL
	if escapedPath == "" {
		return u.String()
	}
	u.Path = b.baseURL.Path + "/" + escapedPath
	u.RawPath = ""
	return u.String()
}

// collectionURL is pathURL for something known to be a collection. A
// collection's canonical URL ends in "/", and Apache answers the slashless
// form with a redirect, so asking for it directly saves a round trip and
// avoids depending on how redirects are handled.
func (b *bucket) collectionURL(escapedPath string) string {
	if escapedPath == "" {
		u := *b.baseURL
		u.Path = b.baseURL.Path + "/"
		u.RawPath = ""
		return u.String()
	}
	return b.pathURL(escapedPath) + "/"
}

// newRequest builds a request with authentication and custom headers applied.
func (b *bucket) newRequest(ctx context.Context, method, rawURL string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, err
	}
	if b.opts.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+b.opts.AuthToken)
	} else if b.opts.BasicAuthUser != "" || b.opts.BasicAuthPassword != "" {
		req.SetBasicAuth(b.opts.BasicAuthUser, b.opts.BasicAuthPassword)
	}
	for k, vs := range b.opts.Headers {
		for i, v := range vs {
			if i == 0 {
				req.Header.Set(k, v)
			} else {
				req.Header.Add(k, v)
			}
		}
	}
	return req, nil
}

// isRetryableMethod reports whether a request using method may be replayed.
// Only safe methods qualify, plus MKCOL, whose replay is harmless because an
// already-created collection answers 405 and mkcol treats that as success.
// A PUT body is a one-shot stream, and a retried DELETE or MOVE that actually
// succeeded the first time reports a spurious 404.
func isRetryableMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, methodPropfind, methodMkcol:
		return true
	}
	return false
}

func isRetryableStatus(code int) bool {
	// 423 Locked is transient: another client holds a WebDAV lock on the
	// resource, typically while creating the same parent collection.
	return code >= 500 || code == http.StatusTooManyRequests || code == http.StatusLocked
}

// retryAfter parses a Retry-After header, in either delta-seconds or HTTP-date
// form. It returns 0 if the header is absent or unparseable.
func retryAfter(h http.Header) time.Duration {
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// do issues the request built by prepare, retrying retryable failures. On
// success the caller owns the response body and must close it. On a non-2xx
// response, the body is consumed and an *Error is returned.
//
// prepare may be called more than once, and must produce an equivalent request
// each time.
func (b *bucket) do(ctx context.Context, prepare func() (*http.Request, error)) (*http.Response, error) {
	backoff := b.backoff
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, err
		}
		req, err := prepare()
		if err != nil {
			return nil, err
		}

		var pause time.Duration
		resp, err := b.client.Do(req)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return nil, err
			}
			lastErr = err
		case resp.StatusCode >= 400:
			pause = retryAfter(resp.Header)
			lastErr = newError(req, resp)
		case resp.StatusCode >= 300:
			// Only reachable for methods withSafeRedirects declines to follow.
			// The caller has to interpret it: for a WebDAV collection this is
			// how Apache asks for the trailing-slash form of the URL.
			lastErr = newError(req, resp)
		default:
			return resp, nil
		}

		// A transport error is retryable for any safe method; a response we can
		// see is retryable only if its status says the failure is transient.
		retryable := isRetryableMethod(req.Method)
		if status := statusOf(lastErr); status != 0 {
			retryable = retryable && isRetryableStatus(status)
		}
		if !retryable || attempt >= b.maxRetries {
			return nil, lastErr
		}
		// Advance the backoff even when the server told us how long to wait,
		// so that a server repeatedly returning "Retry-After: 0" still backs off.
		next := backoff.Pause()
		if pause <= 0 {
			pause = next
		}
		if err := gax.Sleep(ctx, pause); err != nil {
			return nil, lastErr
		}
	}
}

// callBefore invokes a Before* hook, exposing req via As.
func callBefore(before func(asFunc func(any) bool) error, req *http.Request) error {
	if before == nil {
		return nil
	}
	return before(func(i any) bool {
		p, ok := i.(**http.Request)
		if !ok {
			return false
		}
		*p = req
		return true
	})
}

// headerAsFunc builds an As function exposing h.
func headerAsFunc(h http.Header) func(any) bool {
	return func(i any) bool {
		p, ok := i.(*http.Header)
		if !ok {
			return false
		}
		*p = h
		return true
	}
}

// unimplemented reports that an operation needs ProtocolWebDAV.
func (b *bucket) unimplemented(op string) error {
	return errorf(gcerrors.Unimplemented, nil,
		"httpblob: %s is not supported with Protocol=%v; use Protocol=%v", op, b.opts.Protocol, ProtocolWebDAV)
}

// Attributes implements driver.Bucket.
func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	objURL, err := b.objectURL(key)
	if err != nil {
		return nil, err
	}
	waitAttrs := b.fetchAttrs(ctx, objURL)
	header, size, err := b.statObject(ctx, objURL)
	xa, xaErr := waitAttrs()
	if err != nil {
		return nil, withKey(err, key)
	}
	if xaErr != nil {
		return nil, withKey(xaErr, key)
	}

	attrs := &driver.Attributes{
		CacheControl:       firstNonEmpty(xa.CacheControl, header.Get("Cache-Control")),
		ContentDisposition: firstNonEmpty(xa.ContentDisposition, header.Get("Content-Disposition")),
		ContentEncoding:    firstNonEmpty(xa.ContentEncoding, header.Get("Content-Encoding")),
		ContentLanguage:    firstNonEmpty(xa.ContentLanguage, header.Get("Content-Language")),
		ContentType:        contentTypeFor(xa, header),
		Metadata:           copyMetadata(xa.Metadata),
		ModTime:            parseModTime(header),
		Size:               size,
		MD5:                firstNonEmptyBytes(xa.MD5, parseMD5(header)),
		ETag:               header.Get("ETag"),
		AsFunc:             headerAsFunc(header),
	}
	return attrs, nil
}

// statObject returns the headers and size describing the object at objURL.
//
// Under WebDAV it uses PROPFIND, which costs the same as HEAD and additionally
// reports whether the key names a collection — something no HEAD response can
// tell you, and which servers otherwise signal in mutually contradictory ways.
// Plain HTTP has no such method, so it falls back to HEAD.
func (b *bucket) statObject(ctx context.Context, objURL string) (http.Header, int64, error) {
	if b.opts.Protocol != ProtocolWebDAV {
		return b.headObject(ctx, objURL)
	}
	e, err := b.stat(ctx, objURL)
	if err != nil {
		return nil, 0, err
	}
	return e.header(), e.size, nil
}

// headObject returns the response headers and size for the object at objURL.
// Servers that disallow HEAD are handled by falling back to a one-byte ranged
// GET, which reports the full size in Content-Range. See headRefused for which
// statuses count as disallowing it.
func (b *bucket) headObject(ctx context.Context, objURL string) (http.Header, int64, error) {
	var headHeader http.Header
	resp, err := b.do(ctx, func() (*http.Request, error) {
		return b.newRequest(ctx, http.MethodHead, objURL, nil)
	})
	if err == nil {
		defer func() { _ = resp.Body.Close() }()
		if size := contentLength(resp); size >= 0 {
			return resp.Header, size, nil
		}
		// The server answered but would not say how big the object is. Fall
		// through to the ranged GET, whose Content-Range carries the total
		// even when Content-Length is absent.
		headHeader = resp.Header
	} else if !headRefused(statusOf(err)) {
		return nil, 0, err
	}

	rangeResp, rangeErr := b.do(ctx, func() (*http.Request, error) {
		req, err := b.newRequest(ctx, http.MethodGet, objURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Range", "bytes=0-0")
		return req, nil
	})
	if rangeErr != nil {
		if headHeader != nil {
			// HEAD worked; only the size is unknown. Report what we have
			// rather than failing the whole call.
			return headHeader, 0, nil
		}
		if statusOf(rangeErr) == http.StatusMethodNotAllowed {
			// Neither HEAD nor GET is allowed, which is how a WebDAV server
			// reports that the key names a collection rather than a blob.
			return nil, 0, notReadable(err)
		}
		// Otherwise report the original HEAD failure; the fallback is an
		// implementation detail.
		return nil, 0, err
	}
	defer func() { _ = rangeResp.Body.Close() }()
	_, _ = io.Copy(io.Discard, rangeResp.Body)

	header := rangeResp.Header
	if headHeader != nil {
		// Prefer the HEAD headers: the ranged response describes one byte.
		header = headHeader
	}
	if total, ok := parseContentRangeTotal(rangeResp.Header.Get("Content-Range")); ok {
		return header, total, nil
	}
	if size := contentLength(rangeResp); size >= 0 {
		return header, size, nil
	}
	return header, 0, nil
}

// contentLength returns the body length the response declares, or -1 when it
// declares none. A server streaming with chunked transfer encoding sends no
// Content-Length at all — rclone's WebDAV server does exactly this — so
// "unknown" has to be distinguishable from "empty".
func contentLength(resp *http.Response) int64 {
	if resp.ContentLength >= 0 {
		return resp.ContentLength
	}
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		if n, err := strconv.ParseInt(cl, 10, 64); err == nil {
			return n
		}
	}
	return -1
}

// parseContentRangeTotal extracts the total size from a Content-Range header
// like "bytes 0-0/1234". It returns false for an unknown ("*") total.
func parseContentRangeTotal(v string) (int64, bool) {
	i := strings.LastIndex(v, "/")
	if i < 0 {
		return 0, false
	}
	total, err := strconv.ParseInt(strings.TrimSpace(v[i+1:]), 10, 64)
	if err != nil {
		return 0, false
	}
	return total, true
}

// parseModTime returns the Last-Modified time, or the zero time if the server
// didn't send one. It deliberately does not substitute the current time.
func parseModTime(h http.Header) time.Time {
	if lm := h.Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			return t
		}
	}
	return time.Time{}
}

// parseMD5 reads an MD5 digest from Content-MD5 (RFC 1864) or from a
// Digest header carrying an md5 value (RFC 3230).
func parseMD5(h http.Header) []byte {
	if v := h.Get("Content-MD5"); v != "" {
		if d, err := base64.StdEncoding.DecodeString(v); err == nil && len(d) == md5.Size {
			return d
		}
		if d, err := hex.DecodeString(v); err == nil && len(d) == md5.Size {
			return d
		}
	}
	for _, v := range h.Values("Digest") {
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			name, value, ok := strings.Cut(part, "=")
			if !ok || !strings.EqualFold(strings.TrimSpace(name), "md5") {
				continue
			}
			if d, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value)); err == nil && len(d) == md5.Size {
				return d
			}
		}
	}
	return nil
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

func firstNonEmptyBytes(vs ...[]byte) []byte {
	for _, v := range vs {
		if len(v) > 0 {
			return v
		}
	}
	return nil
}

func copyMetadata(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// NewRangeReader implements driver.Bucket.
func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	if offset < 0 {
		return nil, errorf(gcerrors.InvalidArgument, nil, "httpblob: offset must be non-negative, got %d", offset)
	}
	objURL, err := b.objectURL(key)
	if err != nil {
		return nil, err
	}

	// A zero-length read still has to report the blob's real size and mod time,
	// so it needs a round trip even though no bytes are wanted.
	if length == 0 {
		waitAttrs := b.fetchAttrs(ctx, objURL)
		header, size, err := b.headObject(ctx, objURL)
		xa, xaErr := waitAttrs()
		if err != nil {
			return nil, withKey(err, key)
		}
		if xaErr != nil {
			return nil, withKey(xaErr, key)
		}
		if err := callBefore(opts.BeforeRead, mustRequest(ctx, b, http.MethodGet, objURL)); err != nil {
			return nil, err
		}
		return &reader{
			body: http.NoBody,
			rc:   http.NoBody,
			resp: &http.Response{Header: header},
			attrs: driver.ReaderAttributes{
				ContentType: contentTypeFor(xa, header),
				ModTime:     parseModTime(header),
				Size:        size,
			},
		}, nil
	}

	base, err := b.newRequest(ctx, http.MethodGet, objURL, nil)
	if err != nil {
		return nil, err
	}
	if rng := rangeHeader(offset, length); rng != "" {
		base.Header.Set("Range", rng)
	}
	if err := callBefore(opts.BeforeRead, base); err != nil {
		return nil, err
	}

	waitAttrs := b.fetchAttrs(ctx, objURL)
	resp, err := b.do(ctx, func() (*http.Request, error) { return base.Clone(ctx), nil })
	xa, xaErr := waitAttrs()
	if err != nil {
		return nil, withKey(notReadable(err), key)
	}
	if xaErr != nil {
		_ = resp.Body.Close()
		return nil, withKey(xaErr, key)
	}

	body, size, err := sliceBody(resp, offset, length)
	if err != nil {
		_ = resp.Body.Close()
		return nil, withKey(err, key)
	}
	if size < 0 {
		// The response did not say how big the object is, which happens
		// whenever a server streams with chunked transfer encoding. Size is
		// part of the Reader's contract, so pay for one more round trip to
		// learn it rather than reporting zero.
		if _, total, headErr := b.headObject(ctx, objURL); headErr == nil {
			size = total
		} else {
			size = 0
		}
	}

	remaining := int64(-1)
	if length > 0 {
		remaining = length
	} else if size >= 0 {
		remaining = size - offset
	}
	return &reader{
		ctx:       ctx,
		b:         b,
		key:       key,
		url:       objURL,
		base:      base,
		validator: rangeValidator(resp.Header),
		offset:    offset,
		remaining: remaining,
		resp:      resp,
		body:      body,
		rc:        resp.Body,
		attrs: driver.ReaderAttributes{
			ContentType: contentTypeFor(xa, resp.Header),
			ModTime:     parseModTime(resp.Header),
			// Size is the size of the whole object, not of this range.
			Size: size,
		},
	}, nil
}

// mustRequest builds a request for exposure via a Before hook; the URL has
// already been validated, so an error here is not possible in practice.
func mustRequest(ctx context.Context, b *bucket, method, rawURL string) *http.Request {
	req, err := b.newRequest(ctx, method, rawURL, nil)
	if err != nil {
		return &http.Request{Method: method, Header: http.Header{}}
	}
	return req
}

func rangeHeader(offset, length int64) string {
	switch {
	case length > 0:
		return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	case offset > 0:
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return ""
}

// rangeValidator returns the value to send in If-Range when resuming: a strong
// validator if the server gave us one, otherwise nothing. A weak ETag must not
// be used with If-Range (RFC 9110 §13.1.5).
func rangeValidator(h http.Header) string {
	if etag := h.Get("ETag"); etag != "" && !strings.HasPrefix(etag, "W/") {
		return etag
	}
	return h.Get("Last-Modified")
}

// sliceBody adapts a response to the requested range, and reports the size of
// the whole object. A server is allowed to ignore Range and return the entire
// entity with a 200; when that happens we do the slicing ourselves rather than
// passing the wrong bytes to the caller.
func sliceBody(resp *http.Response, offset, length int64) (io.Reader, int64, error) {
	var body io.Reader = resp.Body
	size := contentLength(resp)

	switch resp.StatusCode {
	case http.StatusPartialContent:
		if total, ok := parseContentRangeTotal(resp.Header.Get("Content-Range")); ok {
			size = total
		} else if size >= 0 {
			size = offset + size
		}
	case http.StatusOK:
		if offset > 0 {
			if n, err := io.CopyN(io.Discard, body, offset); err != nil {
				if errors.Is(err, io.EOF) {
					return nil, 0, errorf(gcerrors.InvalidArgument, nil,
						"httpblob: offset %d is past the end of the blob (size %d)", offset, n)
				}
				return nil, 0, err
			}
		}
	default:
		return nil, 0, fmt.Errorf("httpblob: unexpected status %s for a range request", resp.Status)
	}
	if length > 0 {
		body = io.LimitReader(body, length)
	}
	return body, size, nil
}

type reader struct {
	ctx  context.Context
	b    *bucket
	key  string
	url  string
	base *http.Request

	// validator is sent as If-Range when resuming, so that a resumed read can
	// never splice together two different versions of the object.
	validator string
	// offset is the position in the object of the next byte to be read.
	offset int64
	// remaining is the number of bytes still expected, or -1 if unknown.
	remaining int64
	resumes   int

	resp  *http.Response
	body  io.Reader
	rc    io.Closer
	attrs driver.ReaderAttributes
}

func (r *reader) Attributes() *driver.ReaderAttributes { return &r.attrs }

func (r *reader) As(i any) bool {
	p, ok := i.(**http.Response)
	if !ok {
		return false
	}
	*p = r.resp
	return true
}

func (r *reader) Read(p []byte) (int, error) {
	for {
		n, err := r.body.Read(p)
		if n > 0 {
			r.offset += int64(n)
			if r.remaining > 0 {
				r.remaining -= int64(n)
			}
			r.resumes = 0
		}
		if err == nil || errors.Is(err, io.EOF) {
			return n, err
		}
		if !r.canResume() {
			return n, err
		}
		if resumeErr := r.resume(); resumeErr != nil {
			return n, err
		}
		if n > 0 {
			// Hand back what we have; the caller will read again from the new
			// stream.
			return n, nil
		}
	}
}

// canResume reports whether a failed read may be retried against a fresh
// request.
func (r *reader) canResume() bool {
	return r.b != nil &&
		r.ctx != nil && r.ctx.Err() == nil &&
		r.remaining != 0 &&
		r.resumes < maxResumes &&
		// Without a validator we cannot prove the object didn't change between
		// requests, and silently splicing two versions is worse than an error.
		r.validator != ""
}

// resume re-issues the request for the not-yet-read remainder of the object.
func (r *reader) resume() error {
	r.resumes++
	prev := r.rc

	req := r.base.Clone(r.ctx)
	req.Header.Set("Range", rangeHeader(r.offset, r.remaining))
	req.Header.Set("If-Range", r.validator)

	resp, err := r.b.do(r.ctx, func() (*http.Request, error) { return req.Clone(r.ctx), nil })
	if err != nil {
		return err
	}
	// If-Range makes the server answer 200 with the whole entity when the
	// object has changed. Continuing would splice two versions together.
	if resp.StatusCode != http.StatusPartialContent {
		_ = resp.Body.Close()
		return fmt.Errorf("httpblob: cannot resume read of %q: object changed", r.key)
	}
	_ = prev.Close()

	var body io.Reader = resp.Body
	if r.remaining > 0 {
		body = io.LimitReader(body, r.remaining)
	}
	r.resp, r.body, r.rc = resp, body, resp.Body
	return nil
}

func (r *reader) Close() error { return r.rc.Close() }

// NewTypedWriter implements driver.Bucket.
func (b *bucket) NewTypedWriter(ctx context.Context, key, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	if b.opts.Protocol != ProtocolWebDAV {
		return nil, b.unimplemented("writing")
	}
	escaped, err := b.escapedPath(key)
	if err != nil {
		return nil, err
	}
	tempPath, err := tempPathFor(escaped)
	if err != nil {
		return nil, err
	}
	if err := b.ensureCollections(ctx, escaped); err != nil {
		return nil, explainConflict(withKey(err, key), b.baseURL.String())
	}

	tempURL := b.pathURL(tempPath)
	pr, pw := io.Pipe()
	req, err := b.newRequest(ctx, http.MethodPut, tempURL, pr)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if opts.CacheControl != "" {
		req.Header.Set("Cache-Control", opts.CacheControl)
	}
	if opts.ContentDisposition != "" {
		req.Header.Set("Content-Disposition", opts.ContentDisposition)
	}
	if opts.ContentEncoding != "" {
		req.Header.Set("Content-Encoding", opts.ContentEncoding)
	}
	if opts.ContentLanguage != "" {
		req.Header.Set("Content-Language", opts.ContentLanguage)
	}
	if len(opts.ContentMD5) > 0 {
		req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(opts.ContentMD5))
	}
	if err := callBefore(opts.BeforeWrite, req); err != nil {
		return nil, err
	}

	w := &writer{
		ctx:        ctx,
		b:          b,
		key:        key,
		objURL:     b.pathURL(escaped),
		tempURL:    tempURL,
		ifNotExist: opts.IfNotExist,
		md5hash:    md5.New(),
		pw:         pw,
		donec:      make(chan struct{}),
		attrs: xattrs{
			CacheControl:       opts.CacheControl,
			ContentDisposition: opts.ContentDisposition,
			ContentEncoding:    opts.ContentEncoding,
			ContentLanguage:    opts.ContentLanguage,
			ContentType:        contentType,
			Metadata:           copyMetadata(opts.Metadata),
		},
	}

	go func() {
		defer close(w.donec)
		// PUT is never retried: the body is a one-shot pipe.
		resp, err := b.client.Do(req)
		if err != nil {
			w.putErr = err
			// Unblock any writer still pushing into the pipe.
			pr.CloseWithError(err)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode >= 400 {
			w.putErr = newError(req, resp)
			pr.CloseWithError(w.putErr)
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
	}()
	return w, nil
}

// tempPathFor returns a sibling path used to stage a write.
func tempPathFor(escapedPath string) (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("httpblob: generating a temporary key: %w", err)
	}
	return escapedPath + tempInfix + hex.EncodeToString(buf[:]), nil
}

type writer struct {
	ctx     context.Context
	b       *bucket
	key     string
	objURL  string
	tempURL string

	ifNotExist bool
	attrs      xattrs
	md5hash    hash.Hash

	pw     *io.PipeWriter
	donec  chan struct{}
	putErr error

	closed bool
}

func (w *writer) Write(p []byte) (int, error) {
	n, err := w.pw.Write(p)
	if n > 0 {
		w.md5hash.Write(p[:n])
	}
	return n, err
}

func (w *writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// Finish the PUT before deciding what to do.
	if err := w.ctx.Err(); err != nil {
		w.pw.CloseWithError(err)
		<-w.donec
		w.cleanup()
		return err
	}
	_ = w.pw.Close()
	<-w.donec

	if err := w.ctx.Err(); err != nil {
		w.cleanup()
		return err
	}
	if w.putErr != nil {
		w.cleanup()
		return explainConflict(withKey(w.putErr, w.key), w.b.baseURL.String())
	}

	// MOVE with "Overwrite: F" is how IfNotExist is enforced: the server
	// answers 412 if the destination exists, atomically and without a
	// check-then-act race.
	if err := w.b.move(w.ctx, w.tempURL, w.objURL, !w.ifNotExist); err != nil {
		w.cleanup()
		return withKey(err, w.key)
	}

	// The sidecar is written only once the object is in place, so that a
	// refused conditional write never disturbs existing metadata. Nothing is
	// staged any more, so there is nothing left to clean up: the blob is
	// written, and only its metadata is at risk if this last request fails.
	w.attrs.MD5 = w.md5hash.Sum(nil)
	if err := w.b.putAttrs(w.ctx, w.objURL, w.attrs); err != nil {
		return withKey(err, w.key)
	}
	return nil
}

// cleanup removes the staged object. It runs even when w.ctx is already done,
// since otherwise a canceled write would leak its temporary object. The timeout
// is short because this runs inside Close, and a caller that has just canceled
// should not be made to wait on an unresponsive server.
func (w *writer) cleanup() {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), cleanupTimeout)
	defer cancel()
	w.b.deleteURL(ctx, w.tempURL)
}

// Copy implements driver.Bucket.
func (b *bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {
	if b.opts.Protocol != ProtocolWebDAV {
		return b.unimplemented("copying")
	}
	srcURL, err := b.objectURL(srcKey)
	if err != nil {
		return err
	}
	dstURL, err := b.objectURL(dstKey)
	if err != nil {
		return err
	}
	escapedDst, err := b.escapedPath(dstKey)
	if err != nil {
		return err
	}
	if err := b.ensureCollections(ctx, escapedDst); err != nil {
		return withKey(err, dstKey)
	}
	if err := b.copyOne(ctx, srcURL, dstURL, opts.BeforeCopy); err != nil {
		return withKey(err, srcKey)
	}
	if b.useSidecar() {
		// A missing sidecar is not an error; the source may have been written
		// with metadata=skip, or by something other than the Go CDK.
		if err := b.copyOne(ctx, srcURL+attrsExt, dstURL+attrsExt, nil); err != nil &&
			b.ErrorCode(err) != gcerrors.NotFound {
			return withKey(err, srcKey)
		}
	}
	return nil
}

// Delete implements driver.Bucket.
func (b *bucket) Delete(ctx context.Context, key string, opts *driver.DeleteOptions) error {
	if b.opts.Protocol != ProtocolWebDAV {
		return b.unimplemented("deleting")
	}
	objURL, err := b.objectURL(key)
	if err != nil {
		return err
	}
	req, err := b.newRequest(ctx, http.MethodDelete, objURL, nil)
	if err != nil {
		return err
	}
	if err := callBefore(opts.BeforeDelete, req); err != nil {
		return err
	}
	resp, err := b.do(ctx, func() (*http.Request, error) { return req.Clone(ctx), nil })
	if err != nil {
		return withKey(err, key)
	}
	_ = resp.Body.Close()

	if b.useSidecar() {
		b.deleteURL(ctx, objURL+attrsExt)
	}
	return nil
}

// deleteURL deletes rawURL, ignoring failures. It is used for cleanup paths
// where the caller has nothing useful to do with an error.
func (b *bucket) deleteURL(ctx context.Context, rawURL string) {
	resp, err := b.do(ctx, func() (*http.Request, error) {
		return b.newRequest(ctx, http.MethodDelete, rawURL, nil)
	})
	if err == nil {
		_ = resp.Body.Close()
	}
}

// SignedURL implements driver.Bucket. Neither HTTP nor WebDAV defines a way to
// mint a pre-authorized URL, so this is always unsupported.
func (b *bucket) SignedURL(ctx context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	return "", errorf(gcerrors.Unimplemented, nil, "httpblob: SignedURL is not supported")
}
