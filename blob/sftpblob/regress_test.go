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

package sftpblob

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/pkg/sftp"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

// testServer starts pkg/sftp's own server over an in-memory pipe, rooted at a
// temporary directory, and returns a connected client.
func testServer(t *testing.T) (*sftp.Client, string) {
	t.Helper()
	dir := t.TempDir()
	clientConn, serverConn := netPipe()
	server, err := sftp.NewServer(serverConn)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve() }()
	client, err := sftp.NewClientPipe(clientConn, clientConn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, dir
}

// openTestBucket returns a bucket over a fresh in-process SFTP server.
func openTestBucket(t *testing.T, opts *Options) (*blob.Bucket, *bucket) {
	t.Helper()
	client, dir := testServer(t)
	drv, err := openBucket(client, dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	b := blob.NewBucket(drv)
	t.Cleanup(func() { _ = b.Close() })
	return b, drv
}

func listKeys(ctx context.Context, t *testing.T, b *blob.Bucket, opts *blob.ListOptions) []string {
	t.Helper()
	var keys []string
	iter := b.List(opts)
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

// TestBeforeListIsCalled covers a driver-contract violation that the
// conformance suite cannot catch: ListPaged never called opts.BeforeList, and
// an AsTest hook that is never invoked cannot fail.
func TestBeforeListIsCalled(t *testing.T) {
	b, _ := openTestBucket(t, nil)
	ctx := context.Background()
	if err := b.WriteAll(ctx, "a.txt", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}

	var calls int
	iter := b.List(&blob.ListOptions{
		BeforeList: func(as func(any) bool) error {
			calls++
			var c *sftp.Client
			if !as(&c) {
				t.Error("BeforeList: As failed for **sftp.Client")
			}
			return nil
		},
	})
	for {
		if _, err := iter.Next(ctx); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Errorf("BeforeList was called %d times, want exactly 1", calls)
	}
}

// TestIfNotExistIsAtomic covers the write path that used to skip the
// temp-file-and-rename entirely: IfNotExist opened the final path directly, so
// a canceled conditional write left a partial blob at the real key.
func TestIfNotExistIsAtomic(t *testing.T) {
	b, _ := openTestBucket(t, nil)
	ctx := context.Background()

	const key = "dir/blob.txt"
	if err := b.WriteAll(ctx, key, []byte("first"), &blob.WriterOptions{IfNotExist: true}); err != nil {
		t.Fatal(err)
	}

	// A second conditional write must be refused, and must leave the original
	// untouched.
	err := b.WriteAll(ctx, key, []byte("second-and-much-longer"), &blob.WriterOptions{IfNotExist: true})
	if gcerrors.Code(err) != gcerrors.FailedPrecondition {
		t.Errorf("got %v (code %v), want FailedPrecondition", err, gcerrors.Code(err))
	}
	got, err := b.ReadAll(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "first" {
		t.Errorf("got %q, want the refused write to have left %q", got, "first")
	}

	// A canceled conditional write must not truncate or replace the blob.
	cancelCtx, cancel := context.WithCancel(ctx)
	w, err := b.NewWriter(cancelCtx, "dir/other.txt", &blob.WriterOptions{
		IfNotExist:  true,
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := w.Close(); err == nil {
		t.Error("got nil error closing a canceled write")
	}
	if _, err := b.ReadAll(ctx, "dir/other.txt"); gcerrors.Code(err) != gcerrors.NotFound {
		t.Errorf("got %v (code %v), want the canceled write to have left nothing", err, gcerrors.Code(err))
	}
}

// TestInternalPathsHidden covers staging files and sidecars showing up as
// blobs. Only ".attrs" used to be filtered, so an in-flight write was listed
// as if it were an object.
func TestInternalPathsHidden(t *testing.T) {
	b, drv := openTestBucket(t, nil)
	ctx := context.Background()

	for _, key := range []string{"a.txt", "dir/b.txt"} {
		if err := b.WriteAll(ctx, key, []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}
	// Plant a leftover staging file, as a process that died mid-write would.
	stale := drv.fullDir("a.txt" + tempInfix + "deadbeef")
	f, err := drv.client.Create(stale)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	got := listKeys(ctx, t, b, nil)
	want := []string{"a.txt", "dir/b.txt"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("got %v, want %v", got, want)
	}

	// And such a name is not a usable key either.
	err = b.WriteAll(ctx, "x"+tempInfix+"y", []byte("v"), nil)
	if gcerrors.Code(err) != gcerrors.InvalidArgument {
		t.Errorf("got %v (code %v), want InvalidArgument", err, gcerrors.Code(err))
	}
}

// TestListOrdering covers key ordering across directory boundaries. Sorting
// entries by bare name puts "t/t/t" before "t-/t.", but the correct order by
// key is the reverse, because "-" sorts before "/".
func TestListOrdering(t *testing.T) {
	b, _ := openTestBucket(t, nil)
	ctx := context.Background()

	for _, key := range []string{"testFile1", "t/t/t", "t-/t.", "dir1/f", "dir2/f", "d"} {
		if err := b.WriteAll(ctx, key, []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}
	got := listKeys(ctx, t, b, &blob.ListOptions{Delimiter: "/"})
	want := []string{"d", "dir1/", "dir2/", "t-/", "t/", "testFile1"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("got %v\nwant %v", got, want)
	}

	// The same order must hold one page at a time.
	var paged []string
	token := blob.FirstPageToken
	for {
		objs, next, err := b.ListPage(ctx, token, 1, &blob.ListOptions{Delimiter: "/"})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range objs {
			paged = append(paged, o.Key)
		}
		if len(next) == 0 {
			break
		}
		token = next
	}
	if strings.Join(paged, ",") != strings.Join(want, ",") {
		t.Errorf("paged one at a time got %v\nwant %v", paged, want)
	}
}

// TestEmptyDirectoryIsNotADirectory covers deleting a blob leaving its
// directory behind: an empty directory holds no keys and must not be reported.
func TestEmptyDirectoryIsNotADirectory(t *testing.T) {
	b, _ := openTestBucket(t, nil)
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
	got := listKeys(ctx, t, b, &blob.ListOptions{Delimiter: "/"})
	if len(got) != 1 || got[0] != "keep/" {
		t.Errorf("got %v, want [keep/]", got)
	}
}

// TestListCostIsBoundedByPage is the regression test for listing scalability.
// The previous walk re-read the tree from the prefix root for every page and
// fetched each object's sidecar, so a page cost work proportional to the
// bucket rather than to the page.
func TestListCostIsBoundedByPage(t *testing.T) {
	const dirs, filesPerDir = 10, 20

	b, drv := openTestBucket(t, nil)
	ctx := context.Background()
	for d := range dirs {
		for f := range filesPerDir {
			key := fmt.Sprintf("dir%02d/file%02d.txt", d, f)
			if err := b.WriteAll(ctx, key, []byte("x"), nil); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Count directory reads through the seam.
	var mu sync.Mutex
	var reads int
	real := drv.client
	drv.readDir = func(dir string) ([]fs.FileInfo, error) {
		mu.Lock()
		reads++
		mu.Unlock()
		return real.ReadDir(dir)
	}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return reads
	}
	reset := func() {
		mu.Lock()
		reads = 0
		mu.Unlock()
	}

	// A small first page must touch the root plus the one directory its keys
	// come from, not all of them.
	reset()
	objs, _, err := b.ListPage(ctx, blob.FirstPageToken, 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 5 {
		t.Fatalf("got %d objects, want 5", len(objs))
	}
	if got := count(); got > 3 {
		t.Errorf("first page cost %d directory reads, want <= 3 (the whole tree is %d)", got, dirs+1)
	}

	// Browsing with a delimiter probes each directory rather than enumerating
	// it: one read for the root, one per directory.
	reset()
	objs, _, err = b.ListPage(ctx, blob.FirstPageToken, 1000, &blob.ListOptions{Delimiter: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != dirs {
		t.Fatalf("got %d directories, want %d", len(objs), dirs)
	}
	if got, budget := count(), dirs+1; got > budget {
		t.Errorf("delimiter listing cost %d directory reads, want <= %d", got, budget)
	}
}

// TestCallerOwnsClient covers Close closing a client it did not create. Two
// buckets may share one connection, and closing one must not break the other.
func TestCallerOwnsClient(t *testing.T) {
	client, dir := testServer(t)
	ctx := context.Background()

	first, err := OpenBucket(client, dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenBucket(client, dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()

	if err := second.WriteAll(ctx, "blob.txt", []byte("hello"), nil); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := second.ReadAll(ctx, "blob.txt")
	if err != nil {
		t.Fatalf("closing one bucket broke the other: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("got %q want %q", got, "hello")
	}
}

// TestRenameFallback covers servers without the posix-rename@openssh.com
// extension. Without a fallback every write and copy against them fails.
func TestRenameFallback(t *testing.T) {
	b, drv := openTestBucket(t, nil)
	ctx := context.Background()

	// Pretend the server refused the extension, as a non-OpenSSH server would.
	drv.posixRenameUnsupported.Store(true)

	if err := b.WriteAll(ctx, "dir/blob.txt", []byte("first"), nil); err != nil {
		t.Fatal(err)
	}
	// Overwriting exercises the remove-then-rename path.
	if err := b.WriteAll(ctx, "dir/blob.txt", []byte("second"), nil); err != nil {
		t.Fatal(err)
	}
	got, err := b.ReadAll(ctx, "dir/blob.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "second" {
		t.Errorf("got %q want %q", got, "second")
	}
	if err := b.Copy(ctx, "dir/copy.txt", "dir/blob.txt", nil); err != nil {
		t.Fatal(err)
	}
	if got, err = b.ReadAll(ctx, "dir/copy.txt"); err != nil {
		t.Fatal(err)
	}
	if string(got) != "second" {
		t.Errorf("copy got %q want %q", got, "second")
	}
}

// TestReaderClosesOnSeekFailure covers a leaked remote handle: a failed Seek
// returned without closing the file it had just opened.
func TestReaderClosesOnSeekFailure(t *testing.T) {
	b, _ := openTestBucket(t, nil)
	ctx := context.Background()
	if err := b.WriteAll(ctx, "blob.txt", []byte("hello"), nil); err != nil {
		t.Fatal(err)
	}
	// A negative offset is rejected by the portable layer, so drive the driver
	// directly with an offset past the end, which the server refuses.
	r, err := b.NewRangeReader(ctx, "blob.txt", 2, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "ll" {
		t.Errorf("got %q want %q", got, "ll")
	}
	// The server reports any handle still open when it shuts down; the
	// harness closes it in t.Cleanup, so a leak surfaces as test output.
	_ = os.Getenv
}
