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

// Package gcsmp implements multipart uploads on Google Cloud Storage using
// object composition, so assembly happens server side.
//
// GCS has no multipart session like S3's. Each part is uploaded as its own
// object and Commit composes them into the destination. The generic uploader in
// the parent package also stages parts as objects, but its Commit reads every
// one back and rewrites it; here the bytes never leave GCS.
//
// # Compose limits
//
// A single compose call takes at most 32 sources, so an upload with more parts
// is composed as a tree: parts are composed in groups of 32 into intermediate
// objects, those into further intermediates, and so on until one object
// remains. That costs ceil(n/32) + ... calls rather than one, which is still
// far cheaper than moving the bytes twice.
//
// # Interoperating with gcsblob
//
// An object committed here is meant to be read back through a
// gocloud.dev/blob.Bucket opened by gcsblob, so it must land on exactly the
// object name gcsblob would have used. The bucket name and any prefix have to
// be supplied in Location: blob.Bucket.As exposes the client and nothing else,
// and a blob.PrefixedBucket is invisible through it. Key escaping is handled
// here.
//
// # Abandoned uploads
//
// Staged parts are ordinary objects and are billed as storage. Abort deletes
// them, but nothing can clean up after a process that died. They are named
// under a ".gocdkmpu." infix; set an Object Lifecycle Management rule matching
// that prefix to expire them.
package gcsmp // import "github.com/TuSKan/gocloud-ext/blob/multipart/gcsmp"

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/TuSKan/gocloud-ext/blob/multipart"
	"github.com/TuSKan/gocloud-ext/internal/escape"
	"google.golang.org/api/iterator"
)

const (
	// maxComposeSources is the number of objects one compose call accepts.
	maxComposeSources = 32

	// StagingInfix marks the objects an upload stages. It matches the parent
	// package's marker so one lifecycle rule covers both.
	StagingInfix = ".gocdkmpu."

	// partNameWidth zero-pads staged part names so lexical listing order
	// matches numeric part order.
	partNameWidth = 5
)

// EscapeKey applies gcsblob's key escaping.
//
// It is exported because getting escaping wrong is silent: the upload succeeds
// and the object is simply not where blob.Bucket looks. The rules mirror
// gocloud.dev/blob/gcsblob.escapeKey and are pinned by a golden test.
func EscapeKey(key string) string {
	return escape.HexEscape(key, func(r []rune, i int) bool {
		switch {
		// GCS does not handle these.
		case r[i] == 10 || r[i] == 13:
			return true
		// For "../", escape the trailing slash.
		case i > 1 && r[i] == '/' && r[i-1] == '.' && r[i-2] == '.':
			return true
		}
		return false
	})
}

type uploader struct {
	bucket   *storage.BucketHandle
	name     string
	uploadID string
	opts     *multipart.Options
	finished bool
}

var _ multipart.Uploader = (*uploader)(nil)

// NewUploader starts an upload that will produce the object for key under loc
// when committed.
func NewUploader(ctx context.Context, client *storage.Client, loc multipart.Location, key string, opts *multipart.Options) (multipart.Uploader, error) {
	if client == nil {
		return nil, errors.New("gcsmp: nil client")
	}
	if loc.Bucket == "" {
		return nil, errors.New("gcsmp: Location.Bucket is required; blob.Bucket.As cannot supply it")
	}
	if key == "" {
		return nil, errors.New("gcsmp: empty key")
	}
	if strings.Contains(key, StagingInfix) {
		return nil, fmt.Errorf("gcsmp: key %q contains the reserved infix %q", key, StagingInfix)
	}
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return nil, fmt.Errorf("gcsmp: generating upload ID: %w", err)
	}
	return &uploader{
		bucket:   client.Bucket(loc.Bucket),
		name:     multipart.ObjectName(loc, key, EscapeKey),
		uploadID: hex.EncodeToString(buf[:]),
		opts:     opts,
	}, nil
}

// Open resumes an upload.
//
// GCS has no upload session, so there is nothing to look up; the staged part
// objects are the upload. Whether any exist yet is not checked, because an
// upload with no parts is legitimate. Use ListParts to see what arrived.
func Open(ctx context.Context, client *storage.Client, loc multipart.Location, key, uploadID string, opts *multipart.Options) (multipart.Uploader, error) {
	if client == nil {
		return nil, errors.New("gcsmp: nil client")
	}
	if loc.Bucket == "" {
		return nil, errors.New("gcsmp: Location.Bucket is required")
	}
	if key == "" || uploadID == "" {
		return nil, fmt.Errorf("%w: key and uploadID are both required", multipart.ErrUploadNotFound)
	}
	return &uploader{
		bucket:   client.Bucket(loc.Bucket),
		name:     multipart.ObjectName(loc, key, EscapeKey),
		uploadID: uploadID,
		opts:     opts,
	}, nil
}

func (u *uploader) stagingPrefix() string {
	return u.name + StagingInfix + u.uploadID + "/"
}

// partName is derived from the number rather than stored, so a Part that has
// been through encoding/json and across a process boundary still resolves.
func (u *uploader) partName(number int64) string {
	return fmt.Sprintf("%s%0*d", u.stagingPrefix(), partNameWidth, number)
}

func (u *uploader) UploadID() string { return u.uploadID }

func (u *uploader) Constraints() multipart.Constraints {
	return multipart.Constraints{
		// A staged part is an ordinary object, so GCS imposes no minimum.
		MinPartSize: 0,
		MaxPartSize: 0,
		MaxParts:    multipart.MaxPartNumber,
		Resumable:   true,
	}
}

func (u *uploader) UploadPart(ctx context.Context, number int64, r io.Reader) (multipart.Part, error) {
	if u.finished {
		return multipart.Part{}, multipart.ErrClosed
	}
	if err := multipart.ValidatePartNumber(number, u.Constraints()); err != nil {
		return multipart.Part{}, err
	}

	name := u.partName(number)
	w := u.bucket.Object(name).NewWriter(ctx)
	n, err := io.Copy(w, r)
	if err != nil {
		_ = w.Close()
		// Remove the partial object so a retry of the same number does not
		// compose stale bytes.
		_ = u.bucket.Object(name).Delete(ctx)
		return multipart.Part{}, fmt.Errorf("gcsmp: uploading part %d: %w", number, err)
	}
	if err := w.Close(); err != nil {
		_ = u.bucket.Object(name).Delete(ctx)
		return multipart.Part{}, fmt.Errorf("gcsmp: closing part %d: %w", number, err)
	}
	return multipart.Part{Number: number, Size: n, Tag: name}, nil
}

func (u *uploader) ListParts(ctx context.Context) ([]multipart.Part, error) {
	prefix := u.stagingPrefix()
	it := u.bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	var parts []multipart.Part
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		number, err := strconv.ParseInt(strings.TrimPrefix(attrs.Name, prefix), 10, 64)
		if err != nil {
			// Not one of ours; leave it alone rather than guess.
			continue
		}
		parts = append(parts, multipart.Part{Number: number, Size: attrs.Size, Tag: attrs.Name})
	}
	if len(parts) == 0 {
		return nil, nil
	}
	return multipart.SortParts(parts)
}

func (u *uploader) Commit(ctx context.Context, parts []multipart.Part) error {
	if u.finished {
		return multipart.ErrClosed
	}
	ordered, err := multipart.SortParts(parts)
	if err != nil {
		return err
	}

	sources := make([]*storage.ObjectHandle, len(ordered))
	for i, p := range ordered {
		sources[i] = u.bucket.Object(u.partName(p.Number))
	}

	final, err := u.composeTree(ctx, sources)
	if err != nil {
		return err
	}

	// Compose the last (or only) group straight onto the destination, applying
	// the object's attributes as it goes.
	if len(final) == 1 && final[0].ObjectName() != u.name {
		// A single source cannot be "composed" onto the destination with a
		// one-element compose in every emulator, so copy it instead.
		copier := u.bucket.Object(u.name).CopierFrom(final[0])
		u.applyAttrs(&copier.ObjectAttrs)
		if _, err := copier.Run(ctx); err != nil {
			return fmt.Errorf("gcsmp: finalising %q: %w", u.name, err)
		}
	} else if len(final) > 1 {
		composer := u.bucket.Object(u.name).ComposerFrom(final...)
		u.applyAttrs(&composer.ObjectAttrs)
		if _, err := composer.Run(ctx); err != nil {
			return fmt.Errorf("gcsmp: composing %q: %w", u.name, err)
		}
	}

	u.finished = true
	// The object exists now, so cleanup failures must not fail the Commit.
	u.deleteStaged(ctx)
	return nil
}

// composeTree reduces sources to at most maxComposeSources objects, composing
// intermediates as needed, and returns what remains.
func (u *uploader) composeTree(ctx context.Context, sources []*storage.ObjectHandle) ([]*storage.ObjectHandle, error) {
	round := 0
	for len(sources) > maxComposeSources {
		var next []*storage.ObjectHandle
		for i := 0; i < len(sources); i += maxComposeSources {
			end := min(i+maxComposeSources, len(sources))
			group := sources[i:end]
			if len(group) == 1 {
				// Nothing to merge; carry it into the next round untouched.
				next = append(next, group[0])
				continue
			}
			inter := u.bucket.Object(fmt.Sprintf("%sinter-%d-%d", u.stagingPrefix(), round, i/maxComposeSources))
			if _, err := inter.ComposerFrom(group...).Run(ctx); err != nil {
				return nil, fmt.Errorf("gcsmp: composing intermediate %d/%d: %w", round, i, err)
			}
			next = append(next, inter)
		}
		sources = next
		round++
	}
	return sources, nil
}

// applyAttrs copies Options onto the attributes of the object being produced.
func (u *uploader) applyAttrs(attrs *storage.ObjectAttrs) {
	if u.opts == nil {
		return
	}
	attrs.ContentType = u.opts.ContentType
	attrs.CacheControl = u.opts.CacheControl
	attrs.ContentDisposition = u.opts.ContentDisposition
	attrs.ContentEncoding = u.opts.ContentEncoding
	attrs.ContentLanguage = u.opts.ContentLanguage
	if len(u.opts.Metadata) > 0 {
		md := make(map[string]string, len(u.opts.Metadata))
		for k, v := range u.opts.Metadata {
			// blob.Bucket lowercases metadata keys; match it so the object
			// reads back with the keys it was given.
			md[strings.ToLower(k)] = v
		}
		attrs.Metadata = md
	}
}

func (u *uploader) Abort(ctx context.Context) error {
	if u.finished {
		return nil
	}
	u.finished = true
	return u.deleteStagedErr(ctx)
}

func (u *uploader) deleteStaged(ctx context.Context) { _ = u.deleteStagedErr(ctx) }

// deleteStagedErr removes every staged object, including any compose
// intermediates, and reports the first failure.
func (u *uploader) deleteStagedErr(ctx context.Context) error {
	prefix := u.stagingPrefix()
	it := u.bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	var firstErr error
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}
		if err := u.bucket.Object(attrs.Name).Delete(ctx); err != nil &&
			!errors.Is(err, storage.ErrObjectNotExist) {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
