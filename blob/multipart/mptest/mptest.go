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

// Package mptest provides a conformance suite for multipart.Uploader
// implementations.
//
// The suite passes only what the documented API requires — a part number and a
// reader, never a byte offset or a precomputed size. That is deliberate. The
// equivalent suite in gocloud.dev/blob/drivertest always supplied Offset and
// Size, which hid a backend that silently assembled corrupt objects when a
// caller passed neither. A conformance suite that over-specifies its input
// tests the backend it was written against rather than the contract.
package mptest // import "github.com/TuSKan/gocloud-ext/blob/multipart/mptest"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/TuSKan/gocloud-ext/blob/multipart"
	"gocloud.dev/blob"
)

// Harness creates the pieces one backend needs for a conformance run.
type Harness interface {
	// NewUploader starts an upload targeting key.
	NewUploader(ctx context.Context, t *testing.T, key string, opts *multipart.Options) (multipart.Uploader, error)

	// Open resumes the upload identified by uploadID for key. Implementations
	// whose Constraints report Resumable false may return an error; the suite
	// skips the resume test for them.
	Open(ctx context.Context, t *testing.T, key, uploadID string) (multipart.Uploader, error)

	// Bucket returns a blob.Bucket reading the same storage the uploader
	// writes to, so the suite can verify that a committed object is readable
	// through the portable API at the key it was given.
	//
	// The harness owns the returned bucket and closes it in Close; the suite
	// never does. Backends differ in whether a second handle even shares
	// storage — two memblob.OpenBucket calls do not — so lifetime has to be
	// the harness's decision.
	Bucket(ctx context.Context, t *testing.T) (*blob.Bucket, error)

	// Close releases anything the harness allocated.
	Close()
}

// HarnessMaker creates a Harness for one subtest.
type HarnessMaker func(ctx context.Context, t *testing.T) (Harness, error)

// RunConformanceTests runs the full suite against a backend.
func RunConformanceTests(t *testing.T, newHarness HarnessMaker) {
	t.Helper()

	t.Run("OutOfOrder", func(t *testing.T) { testOutOfOrder(t, newHarness) })
	t.Run("SinglePart", func(t *testing.T) { testSinglePart(t, newHarness) })
	t.Run("PartsSurviveJSON", func(t *testing.T) { testPartsSurviveJSON(t, newHarness) })
	t.Run("Abort", func(t *testing.T) { testAbort(t, newHarness) })
	t.Run("NotVisibleBeforeCommit", func(t *testing.T) { testNotVisibleBeforeCommit(t, newHarness) })
	t.Run("Resume", func(t *testing.T) { testResume(t, newHarness) })
	t.Run("ListParts", func(t *testing.T) { testListParts(t, newHarness) })
	t.Run("Attributes", func(t *testing.T) { testAttributes(t, newHarness) })
	t.Run("EscapingKeys", func(t *testing.T) { testEscapingKeys(t, newHarness) })
	t.Run("RejectsBadPartNumbers", func(t *testing.T) { testRejectsBadPartNumbers(t, newHarness) })
	t.Run("RejectsDuplicateParts", func(t *testing.T) { testRejectsDuplicateParts(t, newHarness) })
	t.Run("RejectsEmptyCommit", func(t *testing.T) { testRejectsEmptyCommit(t, newHarness) })
	t.Run("ClosedAfterCommit", func(t *testing.T) { testClosedAfterCommit(t, newHarness) })
}

// setup builds a harness and registers its cleanup.
func setup(ctx context.Context, t *testing.T, newHarness HarnessMaker) Harness {
	t.Helper()
	h, err := newHarness(ctx, t)
	if err != nil {
		t.Fatalf("newHarness: %v", err)
	}
	t.Cleanup(h.Close)
	return h
}

// partBodies returns n distinct part bodies sized for the backend.
//
// Every part but the last is padded to Constraints.MinPartSize, because S3
// rejects an undersized non-final part and would fail every multi-part case
// here. Backends with no minimum keep short bodies so their runs stay fast.
// The last part is exempt from the minimum by definition.
//
// Each body is a distinct repeated character, so a backend that assembles in
// the wrong order produces visibly wrong output rather than something that
// happens to compare equal.
func partBodies(c multipart.Constraints, n int) []string {
	const shortLen = 16
	bodies := make([]string, n)
	for i := range bodies {
		length := shortLen
		if i < n-1 && c.MinPartSize > int64(shortLen) {
			length = int(c.MinPartSize)
		}
		bodies[i] = strings.Repeat(string(rune('a'+i%26)), length)
	}
	return bodies
}

// uploadParts sends each body as its own part, in the order given by order,
// and returns the resulting parts.
//
// Only the part number and a reader are supplied, which is exactly what the
// documented API requires. A backend needing anything more fails here.
func uploadParts(ctx context.Context, t *testing.T, u multipart.Uploader, bodies []string, order []int) []multipart.Part {
	t.Helper()
	parts := make([]multipart.Part, len(bodies))
	for _, i := range order {
		p, err := u.UploadPart(ctx, int64(i+1), strings.NewReader(bodies[i]))
		if err != nil {
			t.Fatalf("UploadPart(%d): %v", i+1, err)
		}
		if p.Number != int64(i+1) {
			t.Errorf("UploadPart(%d) returned Number %d, want %d", i+1, p.Number, i+1)
		}
		if want := int64(len(bodies[i])); p.Size != want {
			t.Errorf("UploadPart(%d) returned Size %d, want %d", i+1, p.Size, want)
		}
		parts[i] = p
	}
	return parts
}

// verify reads key back through blob.Bucket and compares it with want. Reading
// through the portable API is the contract that matters: an object a backend
// wrote but blob.Bucket cannot find at the same key is useless.
func verify(ctx context.Context, t *testing.T, h Harness, key, want string) {
	t.Helper()
	b, err := h.Bucket(ctx, t)
	if err != nil {
		t.Fatalf("Bucket: %v", err)
	}

	got, err := b.ReadAll(ctx, key)
	if err != nil {
		t.Fatalf("ReadAll(%q): %v", key, err)
	}
	if string(got) != want {
		t.Errorf("object at %q is %q, want %q", key, got, want)
	}
}

func testOutOfOrder(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h := setup(ctx, t, newHarness)

	const key = "out-of-order.txt"

	u, err := h.NewUploader(ctx, t, key, nil)
	if err != nil {
		t.Fatalf("NewUploader: %v", err)
	}
	bodies := partBodies(u.Constraints(), 3)
	// Deliberately not ascending: a backend that assembles in arrival order,
	// or that needs the caller to supply offsets, produces the wrong bytes.
	parts := uploadParts(ctx, t, u, bodies, []int{2, 0, 1})

	if err := u.Commit(ctx, parts); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	verify(ctx, t, h, key, strings.Join(bodies, ""))
}

func testSinglePart(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h := setup(ctx, t, newHarness)

	const key = "single.txt"
	const body = "only part"

	u, err := h.NewUploader(ctx, t, key, nil)
	if err != nil {
		t.Fatalf("NewUploader: %v", err)
	}
	parts := uploadParts(ctx, t, u, []string{body}, []int{0})
	if err := u.Commit(ctx, parts); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	verify(ctx, t, h, key, body)
}

func testPartsSurviveJSON(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h := setup(ctx, t, newHarness)

	const key = "json-parts.txt"

	u, err := h.NewUploader(ctx, t, key, nil)
	if err != nil {
		t.Fatalf("NewUploader: %v", err)
	}
	bodies := partBodies(u.Constraints(), 2)
	parts := uploadParts(ctx, t, u, bodies, []int{0, 1})

	// The point of this package is that the process uploading a part need not
	// be the one committing. That only works if a Part survives serialization,
	// so commit with values that have been through encoding/json.
	encoded, err := json.Marshal(parts)
	if err != nil {
		t.Fatalf("marshaling parts: %v", err)
	}
	var decoded []multipart.Part
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshaling parts: %v", err)
	}
	if err := u.Commit(ctx, decoded); err != nil {
		t.Fatalf("Commit with round-tripped parts: %v", err)
	}
	verify(ctx, t, h, key, strings.Join(bodies, ""))
}

func testAbort(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h := setup(ctx, t, newHarness)

	const key = "aborted.txt"

	u, err := h.NewUploader(ctx, t, key, nil)
	if err != nil {
		t.Fatalf("NewUploader: %v", err)
	}
	uploadParts(ctx, t, u, []string{"discarded"}, []int{0})

	if err := u.Abort(ctx); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	// Abort is documented as safe to repeat.
	if err := u.Abort(ctx); err != nil {
		t.Errorf("second Abort: %v", err)
	}

	b, err := h.Bucket(ctx, t)
	if err != nil {
		t.Fatalf("Bucket: %v", err)
	}
	exists, err := b.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Errorf("object exists at %q after Abort, want it absent", key)
	}
}

func testNotVisibleBeforeCommit(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h := setup(ctx, t, newHarness)

	const key = "not-yet.txt"

	u, err := h.NewUploader(ctx, t, key, nil)
	if err != nil {
		t.Fatalf("NewUploader: %v", err)
	}
	uploadParts(ctx, t, u, []string{"pending"}, []int{0})

	b, err := h.Bucket(ctx, t)
	if err != nil {
		t.Fatalf("Bucket: %v", err)
	}

	exists, err := b.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Errorf("object is readable at %q before Commit, want nothing until Commit", key)
	}
	_ = u.Abort(ctx)
}

func testResume(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h := setup(ctx, t, newHarness)

	const key = "resumed.txt"

	u, err := h.NewUploader(ctx, t, key, nil)
	if err != nil {
		t.Fatalf("NewUploader: %v", err)
	}
	bodies := partBodies(u.Constraints(), 2)
	if !u.Constraints().Resumable {
		t.Skip("backend reports Resumable false")
	}
	first := uploadParts(ctx, t, u, bodies, []int{0})[0]
	uploadID := u.UploadID()
	if uploadID == "" {
		t.Fatal("UploadID is empty on a backend reporting Resumable true")
	}

	// A second uploader stands in for another process, which is the case this
	// package exists to serve.
	resumed, err := h.Open(ctx, t, key, uploadID)
	if err != nil {
		t.Fatalf("Open(%q): %v", uploadID, err)
	}
	second, err := resumed.UploadPart(ctx, 2, strings.NewReader(bodies[1]))
	if err != nil {
		t.Fatalf("UploadPart(2) on the resumed uploader: %v", err)
	}
	if err := resumed.Commit(ctx, []multipart.Part{first, second}); err != nil {
		t.Fatalf("Commit on the resumed uploader: %v", err)
	}
	verify(ctx, t, h, key, strings.Join(bodies, ""))
}

func testListParts(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h := setup(ctx, t, newHarness)

	const key = "listed.txt"

	u, err := h.NewUploader(ctx, t, key, nil)
	if err != nil {
		t.Fatalf("NewUploader: %v", err)
	}
	bodies := partBodies(u.Constraints(), 3)
	uploadParts(ctx, t, u, bodies, []int{1, 2, 0})

	listed, err := u.ListParts(ctx)
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(listed) != len(bodies) {
		t.Fatalf("ListParts returned %d parts, want %d", len(listed), len(bodies))
	}
	for i, p := range listed {
		if want := int64(i + 1); p.Number != want {
			t.Errorf("ListParts[%d].Number is %d, want %d (results must be ascending)", i, p.Number, want)
		}
		if want := int64(len(bodies[i])); p.Size != want {
			t.Errorf("ListParts[%d].Size is %d, want %d", i, p.Size, want)
		}
	}

	// The parts ListParts reports must be usable for Commit, so that a process
	// which resumed an upload can finish it without the original's bookkeeping.
	if err := u.Commit(ctx, listed); err != nil {
		t.Fatalf("Commit with parts from ListParts: %v", err)
	}
	verify(ctx, t, h, key, strings.Join(bodies, ""))
}

func testAttributes(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h := setup(ctx, t, newHarness)

	const key = "with-attrs.txt"
	opts := &multipart.Options{
		ContentType:  "text/plain",
		CacheControl: "max-age=60",
		Metadata:     map[string]string{"origin": "mptest"},
	}

	u, err := h.NewUploader(ctx, t, key, opts)
	if err != nil {
		t.Fatalf("NewUploader: %v", err)
	}
	parts := uploadParts(ctx, t, u, []string{"body"}, []int{0})
	if err := u.Commit(ctx, parts); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	b, err := h.Bucket(ctx, t)
	if err != nil {
		t.Fatalf("Bucket: %v", err)
	}

	attrs, err := b.Attributes(ctx, key)
	if err != nil {
		t.Fatalf("Attributes: %v", err)
	}
	if attrs.ContentType != opts.ContentType {
		t.Errorf("ContentType is %q, want %q", attrs.ContentType, opts.ContentType)
	}
	if attrs.CacheControl != opts.CacheControl {
		t.Errorf("CacheControl is %q, want %q", attrs.CacheControl, opts.CacheControl)
	}
	// Drivers configured without metadata storage report none; only check the
	// value when something came back.
	if len(attrs.Metadata) > 0 && attrs.Metadata["origin"] != "mptest" {
		t.Errorf("Metadata[origin] is %q, want %q", attrs.Metadata["origin"], "mptest")
	}
}

func testEscapingKeys(t *testing.T, newHarness HarnessMaker) {
	// Keys that the gocloud.dev drivers escape differently from one another.
	// A backend that bypasses the driver and gets escaping wrong writes an
	// object blob.Bucket cannot find, which verify catches.
	for _, key := range []string{
		"nested/dir/object.txt",
		"with space.txt",
		"with-dot-dot/../resolved.txt",
		"unicode-ü-ñ.txt",
	} {
		t.Run(key, func(t *testing.T) {
			ctx := context.Background()
			h := setup(ctx, t, newHarness)

			u, err := h.NewUploader(ctx, t, key, nil)
			if err != nil {
				t.Fatalf("NewUploader(%q): %v", key, err)
			}
			body := fmt.Sprintf("contents of %s", key)
			parts := uploadParts(ctx, t, u, []string{body}, []int{0})
			if err := u.Commit(ctx, parts); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			verify(ctx, t, h, key, body)
		})
	}
}

func testRejectsBadPartNumbers(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h := setup(ctx, t, newHarness)

	u, err := h.NewUploader(ctx, t, "bad-numbers.txt", nil)
	if err != nil {
		t.Fatalf("NewUploader: %v", err)
	}
	defer func() { _ = u.Abort(ctx) }()

	max := u.Constraints().MaxParts
	if max <= 0 || max > multipart.MaxPartNumber {
		max = multipart.MaxPartNumber
	}
	for _, number := range []int64{0, -1, max + 1} {
		_, err := u.UploadPart(ctx, number, strings.NewReader("x"))
		if err == nil {
			t.Errorf("UploadPart(%d) succeeded, want an error", number)
			continue
		}
		var invalid *multipart.InvalidPartNumberError
		if !errors.As(err, &invalid) {
			t.Errorf("UploadPart(%d) returned %v, want an *InvalidPartNumberError", number, err)
		}
	}
}

func testRejectsDuplicateParts(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h := setup(ctx, t, newHarness)

	u, err := h.NewUploader(ctx, t, "duplicate.txt", nil)
	if err != nil {
		t.Fatalf("NewUploader: %v", err)
	}
	defer func() { _ = u.Abort(ctx) }()

	p, err := u.UploadPart(ctx, 1, strings.NewReader("body"))
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	// Two parts claiming the same number would make the object depend on the
	// order Commit iterated in, so it must be refused rather than resolved.
	err = u.Commit(ctx, []multipart.Part{p, p})
	if err == nil {
		t.Fatal("Commit with a duplicate part number succeeded, want an error")
	}
	var dup *multipart.DuplicatePartError
	if !errors.As(err, &dup) {
		t.Errorf("Commit returned %v, want a *DuplicatePartError", err)
	}
}

func testRejectsEmptyCommit(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h := setup(ctx, t, newHarness)

	u, err := h.NewUploader(ctx, t, "empty-commit.txt", nil)
	if err != nil {
		t.Fatalf("NewUploader: %v", err)
	}
	defer func() { _ = u.Abort(ctx) }()

	if err := u.Commit(ctx, nil); !errors.Is(err, multipart.ErrNoParts) {
		t.Errorf("Commit(nil) returned %v, want ErrNoParts", err)
	}
}

func testClosedAfterCommit(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h := setup(ctx, t, newHarness)

	const key = "closed.txt"
	u, err := h.NewUploader(ctx, t, key, nil)
	if err != nil {
		t.Fatalf("NewUploader: %v", err)
	}
	parts := uploadParts(ctx, t, u, []string{"done"}, []int{0})
	if err := u.Commit(ctx, parts); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if _, err := u.UploadPart(ctx, 2, strings.NewReader("late")); !errors.Is(err, multipart.ErrClosed) {
		t.Errorf("UploadPart after Commit returned %v, want ErrClosed", err)
	}
	if err := u.Commit(ctx, parts); !errors.Is(err, multipart.ErrClosed) {
		t.Errorf("second Commit returned %v, want ErrClosed", err)
	}
}
