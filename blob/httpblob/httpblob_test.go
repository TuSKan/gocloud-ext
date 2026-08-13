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

package httpblob

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/webdav"

	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/blob/drivertest"
	"gocloud.dev/gcerrors"
)

// newWebDAVServer starts an httptest server backed by golang.org/x/net/webdav,
// a third-party WebDAV implementation. Conformance is run against it so that
// the tests exercise interop rather than a mock written to match this driver.
func newWebDAVServer(t *testing.T, wrap func(http.Handler) http.Handler) *httptest.Server {
	t.Helper()
	var h http.Handler = &webdav.Handler{
		FileSystem: webdav.Dir(t.TempDir()),
		LockSystem: webdav.NewMemLS(),
	}
	if wrap != nil {
		h = wrap(h)
	}
	return httptest.NewServer(h)
}

type harness struct {
	server *httptest.Server
	prefix string
}

func (h *harness) MakeDriver(ctx context.Context) (driver.Bucket, error) {
	drv, err := openBucket(ctx, h.server.Client(), h.server.URL, &Options{Protocol: ProtocolWebDAV})
	if err != nil {
		return nil, err
	}
	if h.prefix == "" {
		return drv, nil
	}
	return driver.NewPrefixedBucket(drv, h.prefix), nil
}

// MakeDriverForNonexistentBucket returns nil: an HTTP base URL that doesn't
// resolve is a connection failure, not a "missing bucket".
func (h *harness) MakeDriverForNonexistentBucket(ctx context.Context) (driver.Bucket, error) {
	return nil, nil
}

func (h *harness) HTTPClient() *http.Client { return h.server.Client() }

func (h *harness) Close() { h.server.Close() }

func webdavHarnessMaker(prefix string, wrap func(http.Handler) http.Handler) drivertest.HarnessMaker {
	return func(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
		t.Helper()
		return &harness{server: newWebDAVServer(t, wrap), prefix: prefix}, nil
	}
}

func TestConformanceWebDAV(t *testing.T) {
	drivertest.RunConformanceTests(t, webdavHarnessMaker("", nil), []drivertest.AsTest{verifyAs{}})
}

func TestConformanceWebDAVWithPrefix(t *testing.T) {
	drivertest.RunConformanceTests(t, webdavHarnessMaker("some/prefix/dir/", nil), nil)
}

// TestConformanceWebDAVApacheStyle runs the whole suite against a server
// configured the way Apache mod_dav ships: "Depth: infinity" PROPFIND refused.
// Nothing may depend on it. See also TestNeverSendsDepthInfinity.
func TestConformanceWebDAVApacheStyle(t *testing.T) {
	refuseInfinity := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == methodPropfind && r.Header.Get("Depth") == "infinity" {
				http.Error(w, "propfind-finite-depth", http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	drivertest.RunConformanceTests(t, webdavHarnessMaker("", refuseInfinity), nil)
}

// Environment variables that point the conformance suite at a real WebDAV
// server instead of the in-process one. CI sets them; see .github/workflows.
const (
	envWebDAVURL      = "HTTPBLOB_TEST_WEBDAV_URL"
	envWebDAVUser     = "HTTPBLOB_TEST_WEBDAV_USER"
	envWebDAVPassword = "HTTPBLOB_TEST_WEBDAV_PASSWORD"
)

// externalHarness runs the conformance suite against a WebDAV server this
// process did not start, in a collection of its own that it removes afterwards.
type externalHarness struct {
	baseURL string
	opts    Options
}

func newExternalHarness(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
	t.Helper()
	root := os.Getenv(envWebDAVURL)
	if root == "" {
		t.Skipf("set %s to run conformance against a real WebDAV server", envWebDAVURL)
	}

	opts := Options{
		Protocol:          ProtocolWebDAV,
		BasicAuthUser:     os.Getenv(envWebDAVUser),
		BasicAuthPassword: os.Getenv(envWebDAVPassword),
	}
	// Each harness gets its own collection, so concurrent runs and leftovers
	// from a previous run cannot interfere.
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return nil, err
	}
	baseURL := strings.TrimSuffix(root, "/") + "/httpblob-test-" + hex.EncodeToString(buf[:])

	h := &externalHarness{baseURL: baseURL, opts: opts}
	// httpblob does not create the bucket root, so make it here.
	if err := h.request(ctx, methodMkcol, baseURL); err != nil {
		return nil, fmt.Errorf("creating test collection: %w", err)
	}
	return h, nil
}

func (h *externalHarness) request(ctx context.Context, method, url string) error {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return err
	}
	if h.opts.BasicAuthUser != "" {
		req.SetBasicAuth(h.opts.BasicAuthUser, h.opts.BasicAuthPassword)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s %s: %s", method, url, resp.Status)
	}
	return nil
}

func (h *externalHarness) MakeDriver(ctx context.Context) (driver.Bucket, error) {
	return openBucket(ctx, nil, h.baseURL, &h.opts)
}

func (h *externalHarness) MakeDriverForNonexistentBucket(ctx context.Context) (driver.Bucket, error) {
	return nil, nil
}

func (h *externalHarness) HTTPClient() *http.Client { return http.DefaultClient }

func (h *externalHarness) Close() {
	// Best effort: a leftover collection is untidy, not incorrect.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = h.request(ctx, http.MethodDelete, h.baseURL)
}

// TestConformanceWebDAVExternal is the interop test. golang.org/x/net/webdav is
// one implementation, and passing against it does not prove much about Apache
// mod_dav, nginx or rclone, which differ in Depth handling, MOVE semantics and
// what they do with a missing collection. CI points this at real servers;
// locally it skips.
func TestConformanceWebDAVExternal(t *testing.T) {
	drivertest.RunConformanceTests(t, newExternalHarness, nil)
}

// verifyAs verifies the driver-specific types exposed via As.
type verifyAs struct{}

func (verifyAs) Name() string { return "verify As" }

func (verifyAs) BucketCheck(b *blob.Bucket) error {
	var client *http.Client
	if !b.As(&client) {
		return errors.New("Bucket.As failed for *http.Client")
	}
	return nil
}

func (verifyAs) ErrorCheck(b *blob.Bucket, err error) error {
	var e *Error
	if !b.ErrorAs(err, &e) {
		return errors.New("Bucket.ErrorAs failed for *httpblob.Error")
	}
	if e.StatusCode == 0 {
		return fmt.Errorf("got zero StatusCode in %v", e)
	}
	return nil
}

func (verifyAs) BeforeRead(as func(any) bool) error   { return wantRequest(as) }
func (verifyAs) BeforeWrite(as func(any) bool) error  { return wantRequest(as) }
func (verifyAs) BeforeCopy(as func(any) bool) error   { return wantRequest(as) }
func (verifyAs) BeforeList(as func(any) bool) error   { return wantRequest(as) }
func (verifyAs) BeforeDelete(as func(any) bool) error { return wantRequest(as) }

// BeforeSign is never reached: SignedURL is unimplemented.
func (verifyAs) BeforeSign(as func(any) bool) error { return nil }

func wantRequest(as func(any) bool) error {
	var req *http.Request
	if !as(&req) {
		return errors.New("As failed for *http.Request")
	}
	if req.Method == "" {
		return errors.New("got a request with no method")
	}
	return nil
}

func (verifyAs) AttributesCheck(attrs *blob.Attributes) error {
	var h http.Header
	if !attrs.As(&h) {
		return errors.New("Attributes.As failed for http.Header")
	}
	return nil
}

func (verifyAs) ReaderCheck(r *blob.Reader) error {
	var resp *http.Response
	if !r.As(&resp) {
		return errors.New("Reader.As failed for *http.Response")
	}
	return nil
}

func (verifyAs) ListObjectCheck(o *blob.ListObject) error {
	if o.IsDir {
		// "Directories" are synthesized by the driver and have no server-side
		// representation.
		return nil
	}
	var h http.Header
	if !o.As(&h) {
		return errors.New("ListObject.As failed for http.Header")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Read-only plain HTTP.
// ---------------------------------------------------------------------------

// newFileServer serves files from a temp dir using net/http's ordinary file
// server: an arbitrary web server that knows nothing about the Go CDK.
func newFileServer(t *testing.T, files map[string]string) *httptest.Server {
	t.Helper()
	dir := t.TempDir()
	for name, contents := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o777); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o666); err != nil {
			t.Fatal(err)
		}
	}
	return httptest.NewServer(http.FileServer(http.Dir(dir)))
}

func openReadOnly(t *testing.T, srv *httptest.Server) *blob.Bucket {
	t.Helper()
	b, err := OpenBucket(context.Background(), srv.Client(), srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestReadOnlyRead(t *testing.T) {
	const content = "abcdefghijklmnopqurstuvwxyz"
	srv := newFileServer(t, map[string]string{"dir/blob.txt": content})
	defer srv.Close()
	b := openReadOnly(t, srv)
	ctx := context.Background()

	got, err := b.ReadAll(ctx, "dir/blob.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("got %q want %q", got, content)
	}

	// The same offset/length matrix drivertest uses for reads.
	for _, tc := range []struct {
		name           string
		offset, length int64
		want           string
	}{
		{"length 0", 0, 0, ""},
		{"positive offset to end", 10, -1, content[10:]},
		{"part in the middle", 10, 5, content[10:15]},
		{"in full", 0, -1, content},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := b.NewRangeReader(ctx, "dir/blob.txt", tc.offset, tc.length, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Close() }()
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Errorf("got %q want %q", got, tc.want)
			}
			// Size is always the size of the whole blob, never of the range.
			if r.Size() != int64(len(content)) {
				t.Errorf("got Size %d want %d", r.Size(), len(content))
			}
			if r.ModTime().IsZero() {
				t.Error("got zero ModTime")
			}
		})
	}
}

func TestReadOnlyAttributes(t *testing.T) {
	const content = "hello world"
	srv := newFileServer(t, map[string]string{"blob.txt": content})
	defer srv.Close()
	b := openReadOnly(t, srv)

	attrs, err := b.Attributes(context.Background(), "blob.txt")
	if err != nil {
		t.Fatal(err)
	}
	if attrs.Size != int64(len(content)) {
		t.Errorf("got Size %d want %d", attrs.Size, len(content))
	}
	if !strings.HasPrefix(attrs.ContentType, "text/plain") {
		t.Errorf("got ContentType %q want text/plain", attrs.ContentType)
	}
	if attrs.ModTime.IsZero() {
		t.Error("got zero ModTime")
	}
}

func TestReadOnlyNotFound(t *testing.T) {
	srv := newFileServer(t, nil)
	defer srv.Close()
	b := openReadOnly(t, srv)
	ctx := context.Background()

	_, err := b.Attributes(ctx, "no-such-key")
	if gcerrors.Code(err) != gcerrors.NotFound {
		t.Errorf("Attributes: got %v (code %v) want NotFound", err, gcerrors.Code(err))
	}
	if !strings.Contains(err.Error(), "no-such-key") {
		t.Errorf("error %q does not name the missing key", err)
	}
	_, err = b.ReadAll(ctx, "no-such-key")
	if gcerrors.Code(err) != gcerrors.NotFound {
		t.Errorf("ReadAll: got %v (code %v) want NotFound", err, gcerrors.Code(err))
	}
}

// TestReadOnlyWritesUnimplemented checks that the read-only protocol says so
// clearly rather than issuing a request the server will reject.
func TestReadOnlyWritesUnimplemented(t *testing.T) {
	srv := newFileServer(t, map[string]string{"blob.txt": "hi"})
	defer srv.Close()
	b := openReadOnly(t, srv)
	ctx := context.Background()

	for name, err := range map[string]error{
		"WriteAll": b.WriteAll(ctx, "k", []byte("v"), nil),
		"Copy":     b.Copy(ctx, "dst", "blob.txt", nil),
		"Delete":   b.Delete(ctx, "blob.txt"),
	} {
		if gcerrors.Code(err) != gcerrors.Unimplemented {
			t.Errorf("%s: got %v (code %v) want Unimplemented", name, err, gcerrors.Code(err))
		}
	}
	iter := b.List(nil)
	if _, err := iter.Next(ctx); gcerrors.Code(err) != gcerrors.Unimplemented {
		t.Errorf("List: got %v (code %v) want Unimplemented", err, gcerrors.Code(err))
	}
	_, err := b.SignedURL(ctx, "blob.txt", &blob.SignedURLOptions{Expiry: time.Minute})
	if gcerrors.Code(err) != gcerrors.Unimplemented {
		t.Errorf("SignedURL: got %v (code %v) want Unimplemented", err, gcerrors.Code(err))
	}
}

// ---------------------------------------------------------------------------
// Regressions against servers that behave in awkward but legal ways.
// ---------------------------------------------------------------------------

// TestServerIgnoresRange covers a server that answers a Range request with 200
// and the whole entity, which RFC 9110 permits. Handing those bytes back as if
// they were the requested range would silently corrupt the caller's data.
func TestServerIgnoresRange(t *testing.T) {
	const content = "abcdefghijklmnopqurstuvwxyz"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, content)
	}))
	defer srv.Close()
	b := openReadOnly(t, srv)
	ctx := context.Background()

	for _, tc := range []struct {
		offset, length int64
		want           string
	}{
		{10, 5, content[10:15]},
		{10, -1, content[10:]},
		{0, 3, content[:3]},
	} {
		r, err := b.NewRangeReader(ctx, "blob.txt", tc.offset, tc.length, nil)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tc.want {
			t.Errorf("offset=%d length=%d: got %q want %q", tc.offset, tc.length, got, tc.want)
		}
		if r.Size() != int64(len(content)) {
			t.Errorf("offset=%d length=%d: got Size %d want %d", tc.offset, tc.length, r.Size(), len(content))
		}
	}
}

// TestHeadNotAllowed covers servers that reject HEAD; Attributes falls back to
// a one-byte ranged GET and reads the size from Content-Range.
func TestHeadNotAllowed(t *testing.T) {
	const content = "hello world"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			http.Error(w, "HEAD not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(content)))
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, content[:1])
	}))
	defer srv.Close()
	b := openReadOnly(t, srv)

	attrs, err := b.Attributes(context.Background(), "blob.txt")
	if err != nil {
		t.Fatal(err)
	}
	if attrs.Size != int64(len(content)) {
		t.Errorf("got Size %d want %d", attrs.Size, len(content))
	}
}

// TestResumeAfterDroppedConnection covers a connection that dies mid-body. The
// read resumes from the byte after the last one delivered.
func TestResumeAfterDroppedConnection(t *testing.T) {
	const content = "abcdefghijklmnopqurstuvwxyz"
	const etag = `"v1"`
	var requests atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		if n == 1 {
			// Promise the whole body, then die after a few bytes.
			w.Header().Set("Content-Length", strconv.Itoa(len(content)))
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, content[:5])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}
		start := parseRangeStart(t, r.Header.Get("Range"))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(content)-1, len(content)))
		w.Header().Set("Content-Length", strconv.Itoa(len(content)-int(start)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, content[start:])
	}))
	srv.Config.ErrorLog = quietLogger()
	defer srv.Close()
	b := openReadOnly(t, srv)

	got, err := b.ReadAll(context.Background(), "blob.txt")
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != content {
		t.Errorf("got %q want %q", got, content)
	}
	if n := requests.Load(); n != 2 {
		t.Errorf("got %d requests, want 2 (initial + resume)", n)
	}
}

// TestResumeRefusedWhenObjectChanged is the other half: If-Range makes the
// server return the whole new entity when the object has changed. Splicing that
// onto the bytes already read would produce a corrupt result, so the read fails.
func TestResumeRefusedWhenObjectChanged(t *testing.T) {
	const content = "abcdefghijklmnopqurstuvwxyz"
	var requests atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		if n == 1 {
			w.Header().Set("ETag", `"v1"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(content)))
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, content[:5])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}
		// The object changed, so If-Range does not match: answer 200 with the
		// whole (new) entity, as RFC 9110 requires.
		w.Header().Set("ETag", `"v2"`)
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, strings.ToUpper(content))
	}))
	srv.Config.ErrorLog = quietLogger()
	defer srv.Close()
	b := openReadOnly(t, srv)

	if _, err := b.ReadAll(context.Background(), "blob.txt"); err == nil {
		t.Fatal("got nil error, want a failure rather than spliced content")
	}
}

func parseRangeStart(t *testing.T, header string) int64 {
	t.Helper()
	if !strings.HasPrefix(header, "bytes=") {
		t.Fatalf("expected a Range header, got %q", header)
	}
	start, _, _ := strings.Cut(strings.TrimPrefix(header, "bytes="), "-")
	n, err := strconv.ParseInt(start, 10, 64)
	if err != nil {
		t.Fatalf("parsing Range %q: %v", header, err)
	}
	return n
}

// TestRetrySafeMethod checks that a transient 503 on a GET is retried.
func TestRetrySafeMethod(t *testing.T) {
	const content = "hello"
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "try later", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		_, _ = io.WriteString(w, content)
	}))
	defer srv.Close()
	b := openReadOnly(t, srv)

	got, err := b.ReadAll(context.Background(), "blob.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("got %q want %q", got, content)
	}
	if n := requests.Load(); n != 2 {
		t.Errorf("got %d requests, want 2", n)
	}
}

// TestRetryExhausted checks that retries stop and surface the server's error.
func TestRetryExhausted(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "0")
		http.Error(w, "still broken", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx := context.Background()
	b, err := OpenBucket(ctx, srv.Client(), srv.URL, &Options{MaxRetries: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	_, err = b.ReadAll(ctx, "blob.txt")
	if gcerrors.Code(err) != gcerrors.Internal {
		t.Errorf("got %v (code %v) want Internal", err, gcerrors.Code(err))
	}
	// The initial attempt plus MaxRetries.
	if n := requests.Load(); n != 3 {
		t.Errorf("got %d requests, want 3", n)
	}
}

// TestWritesAreNotRetried checks that a PUT is attempted exactly once: its body
// is a one-shot stream that cannot be replayed.
func TestWritesAreNotRetried(t *testing.T) {
	var puts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			http.Error(w, "broken", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	ctx := context.Background()
	b, err := OpenBucket(ctx, srv.Client(), srv.URL, &Options{Protocol: ProtocolWebDAV, MaxRetries: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	if err := b.WriteAll(ctx, "blob.txt", []byte("data"), nil); err == nil {
		t.Fatal("got nil error, want the server's 503")
	}
	if n := puts.Load(); n != 1 {
		t.Errorf("got %d PUTs, want exactly 1", n)
	}
}

// ---------------------------------------------------------------------------
// WebDAV behaviors not covered by the conformance suite.
// ---------------------------------------------------------------------------

func openWebDAV(t *testing.T, srv *httptest.Server, opts *Options) *blob.Bucket {
	t.Helper()
	if opts == nil {
		opts = &Options{}
	}
	opts.Protocol = ProtocolWebDAV
	b, err := OpenBucket(context.Background(), srv.Client(), srv.URL, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// TestIfNotExist checks that a conditional write is refused with
// FailedPrecondition, which is what drivertest and the portable type expect.
func TestIfNotExist(t *testing.T) {
	srv := newWebDAVServer(t, nil)
	defer srv.Close()
	b := openWebDAV(t, srv, nil)
	ctx := context.Background()

	opts := &blob.WriterOptions{IfNotExist: true}
	if err := b.WriteAll(ctx, "dir/blob.txt", []byte("first"), opts); err != nil {
		t.Fatal(err)
	}
	err := b.WriteAll(ctx, "dir/blob.txt", []byte("second"), opts)
	if gcerrors.Code(err) != gcerrors.FailedPrecondition {
		t.Errorf("got %v (code %v) want FailedPrecondition", err, gcerrors.Code(err))
	}
	// The original content must survive the refused write.
	got, err := b.ReadAll(ctx, "dir/blob.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "first" {
		t.Errorf("got %q want %q", got, "first")
	}
}

// TestCanceledWriteLeavesNoTemp checks that an interrupted write neither
// replaces the previous blob nor leaves its staging object behind in List.
func TestCanceledWriteLeavesNoTemp(t *testing.T) {
	srv := newWebDAVServer(t, nil)
	defer srv.Close()
	b := openWebDAV(t, srv, nil)
	ctx := context.Background()

	if err := b.WriteAll(ctx, "blob.txt", []byte("original"), nil); err != nil {
		t.Fatal(err)
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	w, err := b.NewWriter(cancelCtx, "blob.txt", &blob.WriterOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("replacement")); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := w.Close(); err == nil {
		t.Error("got nil error from Close after cancel")
	}

	got, err := b.ReadAll(ctx, "blob.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "original" {
		t.Errorf("got %q want the write to have left the original in place", got)
	}
	for _, key := range listKeys(ctx, t, b) {
		if strings.Contains(key, tempInfix) || strings.HasSuffix(key, attrsExt) {
			t.Errorf("List returned internal object %q", key)
		}
	}
}

// TestMissingBucketRoot checks that the bare 409 a WebDAV server returns when
// the bucket's base URL doesn't exist is explained rather than passed through.
func TestMissingBucketRoot(t *testing.T) {
	srv := newWebDAVServer(t, nil)
	defer srv.Close()

	ctx := context.Background()
	b, err := OpenBucket(ctx, srv.Client(), srv.URL+"/no-such-bucket", &Options{Protocol: ProtocolWebDAV})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	err = b.WriteAll(ctx, "blob.txt", []byte("hello"), nil)
	if err == nil {
		t.Fatal("got nil error writing into a nonexistent bucket")
	}
	if !strings.Contains(err.Error(), "no-such-bucket") {
		t.Errorf("error %q does not name the missing base URL", err)
	}
	if !strings.Contains(err.Error(), "parent collection") {
		t.Errorf("error %q does not explain the cause", err)
	}
}

// TestWriteRoundTrips pins the number of requests a simple write costs, so that
// a future change adding one is a deliberate decision rather than an accident.
func TestWriteRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		name  string
		key   string
		opts  *Options
		want  map[string]int
		total int
	}{
		{
			name:  "flat key with sidecar",
			key:   "blob.txt",
			want:  map[string]int{http.MethodPut: 2, methodMove: 1},
			total: 3,
		},
		{
			name:  "flat key without sidecar",
			key:   "blob.txt",
			opts:  &Options{Metadata: MetadataDontWrite},
			want:  map[string]int{http.MethodPut: 1, methodMove: 1},
			total: 2,
		},
		{
			name: "nested key also creates collections",
			key:  "a/b/blob.txt",
			want: map[string]int{http.MethodPut: 2, methodMove: 1, methodMkcol: 2},
			// The MKCOLs are memoized, so only the first write to a
			// collection pays for them.
			total: 5,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counts := map[string]int{}
			var mu sync.Mutex
			inner := newWebDAVServer(t, nil)
			defer inner.Close()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				counts[r.Method]++
				mu.Unlock()
				proxyTo(t, w, r, inner.URL)
			}))
			defer srv.Close()

			b := openWebDAV(t, srv, tc.opts)
			if err := b.WriteAll(context.Background(), tc.key, []byte("hello"), nil); err != nil {
				t.Fatal(err)
			}

			mu.Lock()
			defer mu.Unlock()
			total := 0
			for _, n := range counts {
				total += n
			}
			if total != tc.total {
				t.Errorf("got %d requests %v, want %d", total, counts, tc.total)
			}
			for method, want := range tc.want {
				if counts[method] != want {
					t.Errorf("got %d %s requests, want %d (all: %v)", counts[method], method, want, counts)
				}
			}
		})
	}
}

// proxyTo forwards r to the server at target, so a test can count requests
// without reimplementing WebDAV.
func proxyTo(t *testing.T, w http.ResponseWriter, r *http.Request, target string) {
	t.Helper()
	outURL := target + r.URL.RequestURI()
	req, err := http.NewRequestWithContext(r.Context(), r.Method, outURL, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for k, vs := range r.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	// Destination points back at the front server; rewrite it for the backend.
	if dst := req.Header.Get("Destination"); dst != "" {
		if u, err := url.Parse(dst); err == nil {
			req.Header.Set("Destination", target+u.RequestURI())
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// countingProxy forwards to inner while tallying requests by method, so tests
// can assert how much network work an operation costs.
type countingProxy struct {
	*httptest.Server
	mu     sync.Mutex
	counts map[string]int
	depths []string
}

func newCountingProxy(t *testing.T, inner *httptest.Server) *countingProxy {
	t.Helper()
	p := &countingProxy{counts: map[string]int{}}
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		p.counts[r.Method]++
		if d := r.Header.Get("Depth"); d != "" {
			p.depths = append(p.depths, d)
		}
		p.mu.Unlock()
		proxyTo(t, w, r, inner.URL)
	}))
	return p
}

func (p *countingProxy) count(method string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts[method]
}

func (p *countingProxy) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counts = map[string]int{}
	p.depths = nil
}

func (p *countingProxy) seenDepths() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.depths...)
}

// TestListCostIsBoundedByPage is the regression test for listing scalability.
// A page must cost work proportional to the page, not to the size of the
// bucket: an implementation that walks the whole tree for every page turns a
// full listing into O(pages x tree).
func TestListCostIsBoundedByPage(t *testing.T) {
	const (
		dirs         = 10
		filesPerDir  = 20
		totalObjects = dirs * filesPerDir
	)
	inner := newWebDAVServer(t, nil)
	defer inner.Close()
	proxy := newCountingProxy(t, inner)
	defer proxy.Close()

	b := openWebDAV(t, proxy.Server, nil)
	ctx := context.Background()
	for d := range dirs {
		for f := range filesPerDir {
			key := fmt.Sprintf("dir%02d/file%02d.txt", d, f)
			if err := b.WriteAll(ctx, key, []byte("x"), nil); err != nil {
				t.Fatal(err)
			}
		}
	}

	// A small first page must not touch the whole tree: the root collection,
	// plus the one subtree the page's keys come from.
	proxy.reset()
	objs, _, err := b.ListPage(ctx, blob.FirstPageToken, 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 5 {
		t.Fatalf("got %d objects, want 5", len(objs))
	}
	if got := proxy.count(methodPropfind); got > 3 {
		t.Errorf("first page cost %d PROPFINDs, want <= 3 (walking the whole tree would be %d)", got, dirs+1)
	}

	// A full paged iteration must stay far below re-walking per page, which
	// would be pages * (dirs+1) requests.
	proxy.reset()
	var seen int
	token := blob.FirstPageToken
	for {
		objs, next, err := b.ListPage(ctx, token, 5, nil)
		if err != nil {
			t.Fatal(err)
		}
		seen += len(objs)
		if len(next) == 0 {
			break
		}
		token = next
	}
	if seen != totalObjects {
		t.Errorf("listed %d objects, want %d", seen, totalObjects)
	}
	pages := totalObjects / 5
	if got, budget := proxy.count(methodPropfind), pages*(dirs+1)/2; got > budget {
		t.Errorf("full listing cost %d PROPFINDs, want < %d (re-walking each page would be %d)",
			got, budget, pages*(dirs+1))
	}

	// Browsing with a delimiter must probe each directory, not enumerate it.
	proxy.reset()
	objs, _, err = b.ListPage(ctx, blob.FirstPageToken, 1000, &blob.ListOptions{Delimiter: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != dirs {
		t.Fatalf("got %d directories, want %d", len(objs), dirs)
	}
	if got, budget := proxy.count(methodPropfind), dirs+1; got > budget {
		t.Errorf("delimiter listing cost %d PROPFINDs, want <= %d", got, budget)
	}
}

// TestNeverSendsDepthInfinity pins the choice to walk with Depth: 1. RFC 4918
// lets a server refuse Depth: infinity, and Apache mod_dav does by default, so
// depending on it would break against real servers.
func TestNeverSendsDepthInfinity(t *testing.T) {
	inner := newWebDAVServer(t, nil)
	defer inner.Close()
	proxy := newCountingProxy(t, inner)
	defer proxy.Close()

	b := openWebDAV(t, proxy.Server, nil)
	ctx := context.Background()
	for _, key := range []string{"a.txt", "dir/b.txt", "dir/sub/c.txt"} {
		if err := b.WriteAll(ctx, key, []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}
	listKeys(ctx, t, b)
	if _, _, err := b.ListPage(ctx, blob.FirstPageToken, 100, &blob.ListOptions{Delimiter: "/"}); err != nil {
		t.Fatal(err)
	}
	for _, d := range proxy.seenDepths() {
		if d == "infinity" {
			t.Fatalf("sent Depth: infinity; got depths %v", proxy.seenDepths())
		}
	}
}

// TestEmptyCollectionIsNotADirectory covers the case that deleting a blob
// leaves its collection behind: an empty collection must not be reported as a
// "directory", because no key lives under it.
func TestEmptyCollectionIsNotADirectory(t *testing.T) {
	srv := newWebDAVServer(t, nil)
	defer srv.Close()
	b := openWebDAV(t, srv, nil)
	ctx := context.Background()

	for _, key := range []string{"keep/a.txt", "gone/b.txt", "gone/deep/c.txt"} {
		if err := b.WriteAll(ctx, key, []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"gone/b.txt", "gone/deep/c.txt"} {
		if err := b.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
	}

	objs, _, err := b.ListPage(ctx, blob.FirstPageToken, 100, &blob.ListOptions{Delimiter: "/"})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, o := range objs {
		got = append(got, o.Key)
	}
	if len(got) != 1 || got[0] != "keep/" {
		t.Errorf("got %v, want [keep/] (the emptied collection must not be listed)", got)
	}
}

// ---------------------------------------------------------------------------
// Authentication and TLS.
// ---------------------------------------------------------------------------

func TestAuth(t *testing.T) {
	var mu sync.Mutex
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = r.Header.Clone()
		mu.Unlock()
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		_, _ = io.WriteString(w, "hi")
	}))
	defer srv.Close()
	ctx := context.Background()

	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pa@ss"))

	for _, tc := range []struct {
		name  string
		opts  *Options
		check func(t *testing.T, h http.Header)
	}{
		{
			name: "bearer token",
			opts: &Options{AuthToken: "tok123"},
			check: func(t *testing.T, h http.Header) {
				if got, want := h.Get("Authorization"), "Bearer tok123"; got != want {
					t.Errorf("got Authorization %q want %q", got, want)
				}
			},
		},
		{
			name: "basic auth",
			opts: &Options{BasicAuthUser: "user", BasicAuthPassword: "pa@ss"},
			check: func(t *testing.T, h http.Header) {
				if got := h.Get("Authorization"); got != basic {
					t.Errorf("got Authorization %q want %q", got, basic)
				}
			},
		},
		{
			name: "token wins over basic",
			opts: &Options{AuthToken: "tok123", BasicAuthUser: "user", BasicAuthPassword: "pa@ss"},
			check: func(t *testing.T, h http.Header) {
				if got, want := h.Get("Authorization"), "Bearer tok123"; got != want {
					t.Errorf("got Authorization %q want %q", got, want)
				}
			},
		},
		{
			name: "custom headers",
			opts: &Options{Headers: http.Header{"X-Custom": {"one", "two"}}},
			check: func(t *testing.T, h http.Header) {
				if got, want := strings.Join(h.Values("X-Custom"), ","), "one,two"; got != want {
					t.Errorf("got X-Custom %q want %q", got, want)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := OpenBucket(ctx, srv.Client(), srv.URL, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = b.Close() }()
			if _, err := b.ReadAll(ctx, "blob.txt"); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			tc.check(t, got)
		})
	}

	// Credentials in the URL become HTTP Basic Authentication, with the
	// userinfo percent-decoding applied.
	t.Run("URL userinfo", func(t *testing.T) {
		o := &URLOpener{Client: srv.Client()}
		u, err := url.Parse("webdav://user:pa%40ss@" + srv.Listener.Addr().String() + "/?metadata=skip")
		if err != nil {
			t.Fatal(err)
		}
		b, err := o.OpenBucketURL(ctx, u)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = b.Close() }()
		if _, err := b.ReadAll(ctx, "blob.txt"); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if got := got.Get("Authorization"); got != basic {
			t.Errorf("got Authorization %q want %q", got, basic)
		}
	})
}

// TestTLS exercises the https and webdavs schemes end to end, which are
// otherwise never covered: every other test runs over plaintext loopback.
func TestTLS(t *testing.T) {
	srv := httptest.NewTLSServer(&webdav.Handler{
		FileSystem: webdav.Dir(t.TempDir()),
		LockSystem: webdav.NewMemLS(),
	})
	defer srv.Close()
	ctx := context.Background()
	host := srv.Listener.Addr().String()
	o := &URLOpener{Client: srv.Client()}

	rw, err := o.OpenBucketURL(ctx, mustParseURL(t, "webdavs://"+host+"/"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rw.Close() }()
	if err := rw.WriteAll(ctx, "dir/blob.txt", []byte("over TLS"), nil); err != nil {
		t.Fatal(err)
	}
	got, err := rw.ReadAll(ctx, "dir/blob.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "over TLS" {
		t.Errorf("got %q want %q", got, "over TLS")
	}

	// The https scheme opens the same server read-only.
	ro, err := o.OpenBucketURL(ctx, mustParseURL(t, "https://"+host+"/"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	if got, err = ro.ReadAll(ctx, "dir/blob.txt"); err != nil {
		t.Fatal(err)
	}
	if string(got) != "over TLS" {
		t.Errorf("got %q want %q", got, "over TLS")
	}
	if err := ro.WriteAll(ctx, "k", []byte("v"), nil); gcerrors.Code(err) != gcerrors.Unimplemented {
		t.Errorf("got %v (code %v) want Unimplemented", err, gcerrors.Code(err))
	}
}

// TestChunkedResponseSize covers a server that streams the body with chunked
// transfer encoding and therefore sends no Content-Length. rclone's WebDAV
// server does this, and reporting Size 0 for a 27-byte blob broke every caller
// that trusts Reader.Size.
func TestChunkedResponseSize(t *testing.T) {
	const content = "abcdefghijklmnopqurstuvwxyz"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(content)))
			return
		}
		// Flushing commits the headers before the body is known, which is what
		// makes net/http fall back to chunked encoding with no Content-Length.
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, content)
	}))
	defer srv.Close()
	b := openReadOnly(t, srv)
	ctx := context.Background()

	r, err := b.NewRangeReader(ctx, "blob.txt", 0, -1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("got %q want %q", got, content)
	}
	if r.Size() != int64(len(content)) {
		t.Errorf("got Size %d want %d", r.Size(), len(content))
	}
}

// TestCollectionIsNotABlob covers Apache mod_dav, which serves a collection as
// a readable HTTP resource — an autoindex with 200, or 403 under
// "Options -Indexes". Neither means the key names a blob, and only
// resourcetype settles it, which is why Attributes stats with PROPFIND.
func TestCollectionIsNotABlob(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == methodPropfind {
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:"><D:response><D:href>%s</D:href><D:propstat><D:prop>
<D:resourcetype><D:collection/></D:resourcetype>
</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>`, r.URL.Path)
			return
		}
		// Apache would serve the directory index here, with a 200.
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, "<html><body>Index of /someDir</body></html>")
	}))
	defer srv.Close()
	b := openWebDAV(t, srv, nil)

	_, err := b.Attributes(context.Background(), "someDir")
	if gcerrors.Code(err) != gcerrors.NotFound {
		t.Errorf("got %v (code %v) want NotFound", err, gcerrors.Code(err))
	}
}

// TestCollectionRedirect covers Apache's DirectorySlash behavior: a request on
// a collection URL without its trailing slash is answered with a 301 to the
// canonical form. Go's client follows that by reissuing the request as a GET,
// so a PROPFIND would silently become a directory read — and a DELETE would
// silently become a read too, reporting success while deleting nothing.
func TestCollectionRedirect(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path)
		mu.Unlock()
		// Only the WebDAV methods are redirected, so any request that arrives
		// at the trailing-slash form proves the client followed one — and,
		// since Go rewrites the method on the way, arrived as a GET.
		switch r.Method {
		case http.MethodGet, http.MethodHead:
		default:
			if !strings.HasSuffix(r.URL.Path, "/") {
				http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
				return
			}
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, "<html><body>Index of /someDir</body></html>")
	}))
	defer srv.Close()
	b := openWebDAV(t, srv, nil)
	ctx := context.Background()

	if _, err := b.Attributes(ctx, "someDir"); gcerrors.Code(err) != gcerrors.NotFound {
		t.Errorf("Attributes: got %v (code %v) want NotFound", err, gcerrors.Code(err))
	}
	if err := b.Delete(ctx, "someDir"); err == nil {
		t.Error("Delete: got nil error for a redirected DELETE, want a failure")
	}

	mu.Lock()
	defer mu.Unlock()
	for _, s := range seen {
		if strings.HasSuffix(s, "/") {
			t.Errorf("followed a redirect for a WebDAV method; requests seen: %v", seen)
			break
		}
	}
}

// writeMultiStatus answers a PROPFIND with a minimal RFC 4918 multistatus for
// one non-collection resource, so that a hand-written stub server is WebDAV
// enough for the driver to stat against.
func writeMultiStatus(w http.ResponseWriter, href string, size int, contentType string) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:"><D:response><D:href>%s</D:href><D:propstat><D:prop>
<D:resourcetype/>
<D:getcontentlength>%d</D:getcontentlength>
<D:getlastmodified>%s</D:getlastmodified>
<D:getetag>"stub"</D:getetag>
<D:getcontenttype>%s</D:getcontenttype>
</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>`,
		href, size, time.Now().UTC().Format(http.TimeFormat), contentType)
}

// TestUnreadableSidecar covers a server that answers a missing path with a
// friendly HTML page and a 200 instead of a 404, which is common. The blob
// itself is fine, so the read must succeed with metadata taken from the
// response headers rather than failing.
func TestUnreadableSidecar(t *testing.T) {
	const content = "the actual blob"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == methodPropfind {
			writeMultiStatus(w, r.URL.Path, len(content), "text/plain")
			return
		}
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		if strings.HasSuffix(r.URL.Path, attrsExt) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, "<html><body>Sorry, page not found!</body></html>")
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		_, _ = io.WriteString(w, content)
	}))
	defer srv.Close()
	b := openWebDAV(t, srv, nil)
	ctx := context.Background()

	got, err := b.ReadAll(ctx, "blob.txt")
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != content {
		t.Errorf("got %q want %q", got, content)
	}
	attrs, err := b.Attributes(ctx, "blob.txt")
	if err != nil {
		t.Fatalf("Attributes: %v", err)
	}
	if attrs.ContentType != "text/plain" {
		t.Errorf("got ContentType %q, want it to fall back to the header", attrs.ContentType)
	}
}

// ---------------------------------------------------------------------------
// Streaming at size, and concurrency.
// ---------------------------------------------------------------------------

// bigContent builds a deterministic payload large enough to span many network
// reads and writes, so the streaming paths are exercised for real. Every test
// elsewhere uses a few bytes.
func bigContent(n int) []byte {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte(i*7 + i/251)
	}
	return buf
}

func TestLargeBlob(t *testing.T) {
	const size = 8 << 20
	content := bigContent(size)
	srv := newWebDAVServer(t, nil)
	defer srv.Close()
	b := openWebDAV(t, srv, nil)
	ctx := context.Background()

	// Write through the streaming path rather than WriteAll.
	w, err := b.NewWriter(ctx, "dir/big.bin", &blob.WriterOptions{ContentType: "application/octet-stream"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(w, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := b.ReadAll(ctx, "dir/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("read back %d bytes, want %d matching", len(got), len(content))
	}

	attrs, err := b.Attributes(ctx, "dir/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if attrs.Size != size {
		t.Errorf("got Size %d want %d", attrs.Size, size)
	}
	if sum := md5.Sum(content); !bytes.Equal(attrs.MD5, sum[:]) {
		t.Errorf("got MD5 %x want %x", attrs.MD5, sum)
	}

	// A range read from the middle of a large object.
	const off, length = 3 << 20, 1 << 20
	r, err := b.NewRangeReader(ctx, "dir/big.bin", off, length, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	part, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(part, content[off:off+length]) {
		t.Errorf("range read mismatch at offset %d", off)
	}
	if r.Size() != size {
		t.Errorf("got Size %d want the whole object %d", r.Size(), size)
	}
}

// TestLargeBlobResume drops the connection partway through a multi-megabyte
// read; the resumed request must stitch the remainder on exactly.
func TestLargeBlobResume(t *testing.T) {
	const size = 4 << 20
	const cut = 1 << 20
	content := bigContent(size)
	const etag = `"big"`
	var requests atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		if n == 1 {
			w.Header().Set("Content-Length", strconv.Itoa(size))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(content[:cut])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}
		start := parseRangeStart(t, r.Header.Get("Range"))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, size-1, size))
		w.Header().Set("Content-Length", strconv.FormatInt(int64(size)-start, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[start:])
	}))
	srv.Config.ErrorLog = quietLogger()
	defer srv.Close()
	b := openReadOnly(t, srv)

	got, err := b.ReadAll(context.Background(), "big.bin")
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("resumed read mismatch: got %d bytes, want %d", len(got), size)
	}
}

// TestConcurrentOperations hammers one bucket from many goroutines. Its real
// value is under -race, where it covers the driver's shared mutable state:
// the created-collection cache and the writer's goroutine handoff.
func TestConcurrentOperations(t *testing.T) {
	const goroutines = 16
	srv := newWebDAVServer(t, nil)
	defer srv.Close()
	b := openWebDAV(t, srv, nil)
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*4)
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Deliberately overlapping collections, so the goroutines race to
			// create the same ancestors.
			key := fmt.Sprintf("shared/dir%d/blob%d.txt", g%3, g)
			content := []byte(strings.Repeat("x", g+1))
			if err := b.WriteAll(ctx, key, content, nil); err != nil {
				errs <- fmt.Errorf("write %s: %w", key, err)
				return
			}
			got, err := b.ReadAll(ctx, key)
			if err != nil {
				errs <- fmt.Errorf("read %s: %w", key, err)
				return
			}
			if !bytes.Equal(got, content) {
				errs <- fmt.Errorf("read %s: got %d bytes want %d", key, len(got), len(content))
				return
			}
			if _, err := b.Attributes(ctx, key); err != nil {
				errs <- fmt.Errorf("attributes %s: %w", key, err)
				return
			}
			if _, _, err := b.ListPage(ctx, blob.FirstPageToken, 100, &blob.ListOptions{Prefix: "shared/"}); err != nil {
				errs <- fmt.Errorf("list: %w", err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	if got := len(listKeys(ctx, t, b)); got != goroutines {
		t.Errorf("got %d keys after %d concurrent writes, want %d", got, goroutines, goroutines)
	}
}

// TestListHidesInternalObjects checks that sidecars never surface as blobs.
func TestListHidesInternalObjects(t *testing.T) {
	srv := newWebDAVServer(t, nil)
	defer srv.Close()
	b := openWebDAV(t, srv, nil)
	ctx := context.Background()

	for _, key := range []string{"a.txt", "dir/b.txt", "dir/sub/c.txt"} {
		if err := b.WriteAll(ctx, key, []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}
	got := listKeys(ctx, t, b)
	want := []string{"a.txt", "dir/b.txt", "dir/sub/c.txt"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("got %v want %v", got, want)
	}
}

// TestMetadataDontWrite checks that sidecars can be suppressed entirely.
func TestMetadataDontWrite(t *testing.T) {
	srv := newWebDAVServer(t, nil)
	defer srv.Close()
	b := openWebDAV(t, srv, &Options{Metadata: MetadataDontWrite})
	ctx := context.Background()

	if err := b.WriteAll(ctx, "blob.txt", []byte("hello"), &blob.WriterOptions{
		ContentType: "text/plain",
		Metadata:    map[string]string{"foo": "bar"},
	}); err != nil {
		t.Fatal(err)
	}
	attrs, err := b.Attributes(ctx, "blob.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(attrs.Metadata) != 0 {
		t.Errorf("got Metadata %v, want none to be stored", attrs.Metadata)
	}
	if attrs.Size != 5 {
		t.Errorf("got Size %d want 5", attrs.Size)
	}
	for _, key := range listKeys(ctx, t, b) {
		if strings.HasSuffix(key, attrsExt) {
			t.Errorf("got sidecar %q despite MetadataDontWrite", key)
		}
	}
}

func listKeys(ctx context.Context, t *testing.T, b *blob.Bucket) []string {
	t.Helper()
	var keys []string
	iter := b.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			return keys
		}
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, obj.Key)
	}
}

// ---------------------------------------------------------------------------
// Keys and URLs.
// ---------------------------------------------------------------------------

func TestReservedKeys(t *testing.T) {
	srv := newWebDAVServer(t, nil)
	defer srv.Close()
	b := openWebDAV(t, srv, nil)
	ctx := context.Background()

	for _, key := range []string{"blob" + attrsExt, "blob" + tempInfix + "abcd", "/leading-slash"} {
		if err := b.WriteAll(ctx, key, []byte("x"), nil); gcerrors.Code(err) != gcerrors.InvalidArgument {
			t.Errorf("key %q: got %v (code %v) want InvalidArgument", key, err, gcerrors.Code(err))
		}
	}
}

func TestOpenBucketFromURL(t *testing.T) {
	srv := newWebDAVServer(t, nil)
	defer srv.Close()
	host := srv.Listener.Addr().String()

	for _, tc := range []struct {
		url     string
		wantErr bool
	}{
		{"http://" + host + "/bucket", false},
		{"https://" + host + "/bucket", false},
		{"webdav://" + host + "/bucket", false},
		{"webdavs://" + host + "/bucket", false},
		{"webdav://user:pass@" + host + "/bucket", false},
		{"webdav://" + host + "/bucket?metadata=skip", false},
		{"webdav://" + host + "/bucket?auth_token=t", false},
		{"webdav://" + host + "/bucket?max_retries=7", false},
		// Invalid parameter values.
		{"webdav://" + host + "/bucket?metadata=nope", true},
		{"webdav://" + host + "/bucket?max_retries=lots", true},
		// Unknown parameters are rejected rather than silently ignored.
		{"webdav://" + host + "/bucket?secret_key=hunter2", true},
	} {
		b, err := blob.OpenBucket(context.Background(), tc.url)
		if b != nil {
			_ = b.Close()
		}
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: got error %v, want error %v", tc.url, err, tc.wantErr)
		}
	}
}

// TestDefaultClient covers opening a bucket without supplying a client, whose
// implicit transport is nil.
func TestDefaultClient(t *testing.T) {
	const content = "hello"
	srv := newFileServer(t, map[string]string{"blob.txt": content})
	defer srv.Close()

	ctx := context.Background()
	b, err := OpenBucket(ctx, nil, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	got, err := b.ReadAll(ctx, "blob.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("got %q want %q", got, content)
	}
}

// TestRegisterSchemesIsIdempotent covers the collision case for the generic
// "http" and "https" schemes. blob.URLMux panics on a duplicate registration
// and offers no way to unregister, so another package claiming those schemes
// must cost us the scheme rather than crash the process at init.
func TestRegisterSchemesIsIdempotent(t *testing.T) {
	// init() has already registered; doing it again must be a no-op.
	registerSchemes()
	registerSchemes()

	for _, scheme := range []string{SchemeHTTP, SchemeHTTPS, SchemeWebDAV, SchemeWebDAVS} {
		if !blob.DefaultURLMux().ValidBucketScheme(scheme) {
			t.Errorf("scheme %q is not registered", scheme)
		}
	}
}

func TestOpenBucketValidatesBaseURL(t *testing.T) {
	ctx := context.Background()
	for _, baseURL := range []string{"", "ftp://example.com/x", "/no-scheme", "http://"} {
		if _, err := OpenBucket(ctx, nil, baseURL, nil); err == nil {
			t.Errorf("baseURL %q: got nil error, want a failure", baseURL)
		}
	}
}

func TestURLPathEscaping(t *testing.T) {
	b := &bucket{baseURL: mustParseURL(t, "http://example.com/base")}
	for _, tc := range []struct{ key, want string }{
		{"simple.txt", "http://example.com/base/simple.txt"},
		{"dir/simple.txt", "http://example.com/base/dir/simple.txt"},
		{"with space.txt", "http://example.com/base/with%20space.txt"},
		{"with#hash.txt", "http://example.com/base/with%23hash.txt"},
		{"unicode/☺.txt", "http://example.com/base/unicode/%E2%98%BA.txt"},
	} {
		got, err := b.objectURL(tc.key)
		if err != nil {
			t.Errorf("key %q: %v", tc.key, err)
			continue
		}
		if got != tc.want {
			t.Errorf("key %q: got %q want %q", tc.key, got, tc.want)
		}
	}
}

func TestRelativePath(t *testing.T) {
	b := &bucket{baseURL: mustParseURL(t, "http://example.com/base")}
	for _, tc := range []struct {
		href   string
		want   string
		wantOK bool
	}{
		{"/base/foo.txt", "foo.txt", true},
		{"/base/dir/", "dir", true},
		{"http://example.com/base/dir/foo.txt", "dir/foo.txt", true},
		{"/base/with%20space.txt", "with space.txt", true},
		{"/base", "", false},
		{"/base/", "", false},
		{"/elsewhere/foo.txt", "", false},
	} {
		got, ok := b.relativePath(tc.href)
		if ok != tc.wantOK || got != tc.want {
			t.Errorf("href %q: got (%q, %v) want (%q, %v)", tc.href, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"", 0},
		{"5", 5 * time.Second},
		{"-1", 0},
		{"garbage", 0},
		{time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat), 0},
	} {
		h := http.Header{}
		if tc.value != "" {
			h.Set("Retry-After", tc.value)
		}
		if got := retryAfter(h); got != tc.want {
			t.Errorf("Retry-After %q: got %v want %v", tc.value, got, tc.want)
		}
	}
}

func TestParseMD5(t *testing.T) {
	sum := []byte{0x9a, 0x03, 0x60, 0x80, 0xd3, 0xad, 0x69, 0x36, 0xd0, 0xef, 0xc5, 0x7d, 0x66, 0x4d, 0x38, 0x1e}
	for _, tc := range []struct {
		name   string
		header http.Header
		want   []byte
	}{
		{"none", http.Header{}, nil},
		{"base64", http.Header{"Content-Md5": {"mgNggNOtaTbQ78V9Zk04Hg=="}}, sum},
		{"hex", http.Header{"Content-Md5": {"9a036080d3ad6936d0efc57d664d381e"}}, sum},
		{"digest", http.Header{"Digest": {"sha-256=xyz, md5=mgNggNOtaTbQ78V9Zk04Hg=="}}, sum},
		{"wrong length", http.Header{"Content-Md5": {"YWJj"}}, nil},
	} {
		if got := parseMD5(tc.header); !bytes.Equal(got, tc.want) {
			t.Errorf("%s: got %x want %x", tc.name, got, tc.want)
		}
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// quietLogger silences the "connection reset" noise from the tests that abort a
// handler on purpose.
func quietLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}
