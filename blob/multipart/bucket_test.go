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

package multipart_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/TuSKan/gocloud-ext/blob/multipart"
	"github.com/TuSKan/gocloud-ext/blob/multipart/mptest"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/memblob"
)

// bucketOpener returns a *blob.Bucket. Whether a second call shares storage
// with the first depends on the driver: two fileblob buckets over one directory
// do, two memblob buckets do not, since each OpenBucket allocates its own map.
//
// separateHandles records that difference. Where it is false, the harness hands
// out one bucket for everything, which is the only way memblob can be tested at
// all; where it is true, resuming genuinely goes through an independent handle
// and exercises the cross-process path this package exists for.
type bucketOpener struct {
	open            func() (*blob.Bucket, error)
	separateHandles bool
}

type harness struct {
	opener  bucketOpener
	primary *blob.Bucket
	extra   []*blob.Bucket
}

func newHarness(opener bucketOpener) mptest.HarnessMaker {
	return func(ctx context.Context, t *testing.T) (mptest.Harness, error) {
		b, err := opener.open()
		if err != nil {
			return nil, err
		}
		return &harness{opener: opener, primary: b}, nil
	}
}

// handle returns a bucket for reads or for a resumed upload: an independent one
// where the driver supports it, otherwise the primary.
func (h *harness) handle() (*blob.Bucket, error) {
	if !h.opener.separateHandles {
		return h.primary, nil
	}
	b, err := h.opener.open()
	if err != nil {
		return nil, err
	}
	h.extra = append(h.extra, b)
	return b, nil
}

func (h *harness) NewUploader(ctx context.Context, t *testing.T, key string, opts *multipart.Options) (multipart.Uploader, error) {
	return multipart.NewUploader(ctx, h.primary, key, opts)
}

func (h *harness) Open(ctx context.Context, t *testing.T, key, uploadID string) (multipart.Uploader, error) {
	b, err := h.handle()
	if err != nil {
		return nil, err
	}
	return multipart.Open(ctx, b, key, uploadID, nil)
}

func (h *harness) Bucket(ctx context.Context, t *testing.T) (*blob.Bucket, error) {
	return h.handle()
}

func (h *harness) Close() {
	for _, b := range h.extra {
		_ = b.Close()
	}
	_ = h.primary.Close()
}

func memOpener() bucketOpener {
	return bucketOpener{
		open:            func() (*blob.Bucket, error) { return memblob.OpenBucket(nil), nil },
		separateHandles: false,
	}
}

func fileOpener(dir string) bucketOpener {
	return bucketOpener{
		open:            func() (*blob.Bucket, error) { return fileblob.OpenBucket(dir, nil) },
		separateHandles: true,
	}
}

func TestConformanceMem(t *testing.T) {
	mptest.RunConformanceTests(t, newHarness(memOpener()))
}

// TestConformanceFile is the one that matters for the cross-process claim: each
// bucket handle is independent, so Resume really does commit an upload through
// a handle that did not create it.
func TestConformanceFile(t *testing.T) {
	mptest.RunConformanceTests(t, newHarness(fileOpener(t.TempDir())))
}

// TestConformancePrefixed runs the suite through a prefixed bucket. The generic
// uploader writes through blob.Bucket, so the driver applies the prefix without
// any cooperation from this package; this proves that rather than assuming it.
func TestConformancePrefixed(t *testing.T) {
	dir := t.TempDir()
	opener := bucketOpener{
		open: func() (*blob.Bucket, error) {
			b, err := fileblob.OpenBucket(dir, nil)
			if err != nil {
				return nil, err
			}
			return blob.PrefixedBucket(b, "some/prefix/"), nil
		},
		separateHandles: true,
	}
	mptest.RunConformanceTests(t, newHarness(opener))
}

// TestStagedPartsRemovedAfterCommit checks that a committed upload leaves no
// staging objects behind. Abandoned parts cost storage, so "it worked" is not
// the whole contract.
func TestStagedPartsRemovedAfterCommit(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	defer func() { _ = b.Close() }()

	u, err := multipart.NewUploader(ctx, b, "tidy.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	var parts []multipart.Part
	for i, body := range []string{"a", "b", "c"} {
		p, err := u.UploadPart(ctx, int64(i+1), strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, p)
	}
	if err := u.Commit(ctx, parts); err != nil {
		t.Fatal(err)
	}

	if leftover := keysWithInfix(ctx, t, b, multipart.StagingInfix); len(leftover) != 0 {
		t.Errorf("staging objects survived Commit: %v", leftover)
	}
}

// TestStagedPartsRemovedAfterAbort is the same guarantee for the abort path.
func TestStagedPartsRemovedAfterAbort(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	defer func() { _ = b.Close() }()

	u, err := multipart.NewUploader(ctx, b, "discarded.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u.UploadPart(ctx, 1, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := u.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	if leftover := keysWithInfix(ctx, t, b, multipart.StagingInfix); len(leftover) != 0 {
		t.Errorf("staging objects survived Abort: %v", leftover)
	}
}

func keysWithInfix(ctx context.Context, t *testing.T, b *blob.Bucket, infix string) []string {
	t.Helper()
	var found []string
	iter := b.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err != nil {
			break
		}
		if strings.Contains(obj.Key, infix) {
			found = append(found, obj.Key)
		}
	}
	return found
}

// TestOpenUnknownUploadID checks that resuming a nonexistent upload is
// distinguishable from resuming one that merely has no parts yet.
func TestOpenUnknownUploadID(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	defer func() { _ = b.Close() }()

	_, err := multipart.Open(ctx, b, "some.txt", "0123456789abcdef0123456789abcdef", nil)
	if !errors.Is(err, multipart.ErrUploadNotFound) {
		t.Errorf("Open with an unknown upload ID returned %v, want ErrUploadNotFound", err)
	}
}

// TestOpenWrongKey guards the cross-process case: an upload ID is only
// meaningful with the key it was created for, and committing it against
// another key would write the wrong object.
func TestOpenWrongKey(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	defer func() { _ = b.Close() }()

	u, err := multipart.NewUploader(ctx, b, "right.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = u.Abort(ctx) }()

	if _, err := multipart.Open(ctx, b, "wrong.txt", u.UploadID(), nil); !errors.Is(err, multipart.ErrUploadNotFound) {
		t.Errorf("Open with a mismatched key returned %v, want ErrUploadNotFound", err)
	}
}

// TestCommitRejectsForeignPart covers parts arriving from another upload, which
// is possible once parts are passed between processes.
func TestCommitRejectsForeignPart(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	defer func() { _ = b.Close() }()

	first, err := multipart.NewUploader(ctx, b, "first.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Abort(ctx) }()
	foreign, err := first.UploadPart(ctx, 1, strings.NewReader("from another upload"))
	if err != nil {
		t.Fatal(err)
	}

	second, err := multipart.NewUploader(ctx, b, "second.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Abort(ctx) }()

	if err := second.Commit(ctx, []multipart.Part{foreign}); err == nil {
		t.Error("Commit accepted a part from a different upload, want an error")
	}
}

// TestUploaderRejectsReservedKey stops a caller creating an upload whose key
// would collide with this package's own staging objects.
func TestUploaderRejectsReservedKey(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	defer func() { _ = b.Close() }()

	key := "bad" + multipart.StagingInfix + "key.txt"
	if _, err := multipart.NewUploader(ctx, b, key, nil); err == nil {
		t.Errorf("NewUploader(%q) succeeded, want it refused for containing %q", key, multipart.StagingInfix)
	}
}
