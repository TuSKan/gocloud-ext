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

package multipart

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

// StagingInfix marks the objects a generic upload stages in the bucket. It is
// deliberately not the ".gocdktmp." infix that fileblob, sftpblob and httpblob
// reserve, because those drivers reject keys containing theirs.
//
// Staged parts are ordinary objects and therefore appear in blob.Bucket.List
// while an upload is in flight. Nothing in the public blob API can hide them.
// Filter on this infix if a listing must exclude uploads in progress, and
// expire them with a lifecycle rule so an abandoned upload does not accumulate
// storage; see the package documentation.
const StagingInfix = ".gocdkmpu."

// manifestName is the object recording an upload's existence and options. It
// is what lets Open tell "no such upload" apart from "an upload with no parts
// yet", and it carries the options forward so a process that resumes an upload
// commits with the content type and metadata the upload was created with,
// rather than with whatever the resuming caller happened to pass.
const manifestName = "manifest.json"

// partNameWidth zero-pads staged part names so that lexical order, which is
// what blob.Bucket.List guarantees, matches numeric part order. MaxPartNumber
// is 10000, so five digits is always enough.
const partNameWidth = 5

// manifest is the JSON stored at manifestName.
type manifest struct {
	Key     string   `json:"key"`
	Options *Options `json:"options,omitempty"`
}

// bucketUploader implements Uploader over the public blob.Bucket API.
type bucketUploader struct {
	b        *blob.Bucket
	key      string
	uploadID string
	opts     *Options

	// finished is set by Commit and Abort. It is not guarded by a mutex:
	// UploadPart is documented as concurrency-safe but Commit and Abort are
	// not, so a caller racing them against UploadPart has already broken the
	// contract, and a mutex here would imply a safety this type cannot give.
	finished bool
}

var _ Uploader = (*bucketUploader)(nil)

// NewUploader starts a multipart upload that will produce the object at key in
// b when it is committed.
//
// It works with any bucket, including one wrapped by blob.PrefixedBucket: every
// read and write goes through b, so the driver applies its own key escaping and
// prefix, and the committed object lands exactly where b.NewReader will find
// it.
//
// Commit reads each staged part back and rewrites it into the destination, so
// the bytes cross the network roughly twice. Where that matters, use the
// native package for the backend — s3mp, gcsmp or azmp — which assembles server
// side.
func NewUploader(ctx context.Context, b *blob.Bucket, key string, opts *Options) (Uploader, error) {
	if b == nil {
		return nil, errors.New("multipart: nil bucket")
	}
	if key == "" {
		return nil, errors.New("multipart: empty key")
	}
	if strings.Contains(key, StagingInfix) {
		return nil, fmt.Errorf("multipart: key %q contains the reserved infix %q", key, StagingInfix)
	}

	id, err := newUploadID()
	if err != nil {
		return nil, err
	}
	u := &bucketUploader{b: b, key: key, uploadID: id, opts: opts}

	// Write the manifest first. Until it exists the upload does not exist, so
	// a failure here leaves nothing behind to clean up.
	body, err := json.Marshal(manifest{Key: key, Options: opts})
	if err != nil {
		return nil, fmt.Errorf("multipart: marshaling manifest: %w", err)
	}
	if err := b.WriteAll(ctx, u.manifestKey(), body, &blob.WriterOptions{
		ContentType: "application/json",
	}); err != nil {
		return nil, fmt.Errorf("multipart: writing manifest: %w", err)
	}
	return u, nil
}

// Open resumes the upload identified by uploadID, which must have come from
// UploadID on an uploader created by NewUploader for the same bucket and key.
//
// The options the upload was created with are recovered from the bucket, so a
// process that did not create the upload still commits with the right content
// type and metadata. Use ListParts to discover which parts already arrived.
//
// It returns an error matching ErrUploadNotFound if no such upload exists,
// including when it was already committed or aborted.
func Open(ctx context.Context, b *blob.Bucket, key, uploadID string, _ *Options) (Uploader, error) {
	if b == nil {
		return nil, errors.New("multipart: nil bucket")
	}
	if key == "" || uploadID == "" {
		return nil, fmt.Errorf("%w: key and uploadID are both required", ErrUploadNotFound)
	}
	u := &bucketUploader{b: b, key: key, uploadID: uploadID}

	body, err := b.ReadAll(ctx, u.manifestKey())
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, fmt.Errorf("%w: %q for key %q", ErrUploadNotFound, uploadID, key)
		}
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("multipart: manifest for upload %q is unreadable: %w", uploadID, err)
	}
	if m.Key != key {
		// The upload ID is real but belongs to a different key; committing
		// would write to the wrong object.
		return nil, fmt.Errorf("%w: upload %q belongs to key %q, not %q", ErrUploadNotFound, uploadID, m.Key, key)
	}
	u.opts = m.Options
	return u, nil
}

// newUploadID returns a random identifier. It is hex so that it is safe in a
// key for every backend without escaping.
func newUploadID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("multipart: generating upload ID: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// stagingPrefix is the key prefix holding this upload's manifest and parts.
func (u *bucketUploader) stagingPrefix() string {
	return u.key + StagingInfix + u.uploadID + "/"
}

func (u *bucketUploader) manifestKey() string {
	return u.stagingPrefix() + manifestName
}

// partKey is derived from the part number rather than stored, so a Part that
// has been through encoding/json and across a process boundary still resolves.
func (u *bucketUploader) partKey(number int64) string {
	return fmt.Sprintf("%s%0*d", u.stagingPrefix(), partNameWidth, number)
}

func (u *bucketUploader) UploadID() string { return u.uploadID }

func (u *bucketUploader) Constraints() Constraints {
	return Constraints{
		// A staged part is an ordinary object, so there is no minimum and no
		// per-part ceiling beyond whatever the driver imposes on any write.
		MinPartSize: 0,
		MaxPartSize: 0,
		MaxParts:    MaxPartNumber,
		// State lives in the bucket, so Open can always resume. Whether that
		// survives a process restart depends on the driver, not on this code.
		Resumable: true,
	}
}

func (u *bucketUploader) UploadPart(ctx context.Context, number int64, r io.Reader) (Part, error) {
	if u.finished {
		return Part{}, ErrClosed
	}
	if err := ValidatePartNumber(number, u.Constraints()); err != nil {
		return Part{}, err
	}

	key := u.partKey(number)
	w, err := u.b.NewWriter(ctx, key, nil)
	if err != nil {
		return Part{}, err
	}
	n, err := io.Copy(w, r)
	if err != nil {
		// Close to release the writer's resources, then remove the partial
		// part so a retry of the same number does not read stale bytes.
		_ = w.Close()
		_ = u.b.Delete(ctx, key)
		return Part{}, err
	}
	if err := w.Close(); err != nil {
		_ = u.b.Delete(ctx, key)
		return Part{}, err
	}
	return Part{Number: number, Size: n, Tag: key}, nil
}

func (u *bucketUploader) ListParts(ctx context.Context) ([]Part, error) {
	prefix := u.stagingPrefix()
	iter := u.b.List(&blob.ListOptions{Prefix: prefix})
	var parts []Part
	for {
		obj, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		name := strings.TrimPrefix(obj.Key, prefix)
		if name == manifestName {
			continue
		}
		number, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			// Not one of ours; leave it alone rather than guess.
			continue
		}
		parts = append(parts, Part{Number: number, Size: obj.Size, Tag: obj.Key})
	}
	if len(parts) == 0 {
		return nil, nil
	}
	// List returns keys in lexical order and part names are zero-padded, so
	// these are already ascending; sort anyway rather than depend on it.
	return SortParts(parts)
}

func (u *bucketUploader) Commit(ctx context.Context, parts []Part) error {
	if u.finished {
		return ErrClosed
	}
	ordered, err := SortParts(parts)
	if err != nil {
		return err
	}
	if err := CheckPartSizes(ordered, u.Constraints()); err != nil {
		return err
	}
	// Reject parts belonging to another upload before writing anything: with
	// parts arriving from other processes, a mixed-up Tag would otherwise
	// assemble an object out of the wrong bytes.
	for _, p := range ordered {
		if p.Tag != "" && p.Tag != u.partKey(p.Number) {
			return fmt.Errorf("multipart: part %d has tag %q, which does not belong to upload %q",
				p.Number, p.Tag, u.uploadID)
		}
	}

	w, err := u.b.NewWriter(ctx, u.key, u.writerOptions())
	if err != nil {
		return err
	}
	for _, p := range ordered {
		if err := u.copyPart(ctx, w, p); err != nil {
			_ = w.Close()
			return err
		}
	}
	// Close is what publishes the object; until it returns, nothing is
	// readable at the destination key.
	if err := w.Close(); err != nil {
		return err
	}

	u.finished = true
	// The object exists now, so cleanup failures must not fail the Commit.
	// Anything left behind is covered by the lifecycle guidance in the package
	// documentation.
	u.deleteStaged(ctx)
	return nil
}

// copyPart streams one staged part into w.
func (u *bucketUploader) copyPart(ctx context.Context, w io.Writer, p Part) error {
	r, err := u.b.NewReader(ctx, u.partKey(p.Number), nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return fmt.Errorf("multipart: part %d is missing from upload %q: %w", p.Number, u.uploadID, err)
		}
		return err
	}
	defer func() { _ = r.Close() }()
	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("multipart: copying part %d: %w", p.Number, err)
	}
	return nil
}

func (u *bucketUploader) Abort(ctx context.Context) error {
	if u.finished {
		// Aborting an aborted upload is documented as safe, and after a Commit
		// there is nothing left to discard.
		return nil
	}
	u.finished = true
	return u.deleteStagedErr(ctx)
}

// deleteStaged removes the staging objects, ignoring failures.
func (u *bucketUploader) deleteStaged(ctx context.Context) {
	_ = u.deleteStagedErr(ctx)
}

// deleteStagedErr removes every staged object, including the manifest, and
// reports the first failure. The manifest goes last so that a partial delete
// leaves an upload Open can still find and retry.
func (u *bucketUploader) deleteStagedErr(ctx context.Context) error {
	prefix := u.stagingPrefix()
	manifestKey := u.manifestKey()

	var firstErr error
	iter := u.b.List(&blob.ListOptions{Prefix: prefix})
	for {
		obj, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}
		if obj.Key == manifestKey {
			continue
		}
		if err := u.b.Delete(ctx, obj.Key); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if err := u.b.Delete(ctx, manifestKey); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// writerOptions maps Options onto blob.WriterOptions.
func (u *bucketUploader) writerOptions() *blob.WriterOptions {
	if u.opts == nil {
		return nil
	}
	return &blob.WriterOptions{
		ContentType:        u.opts.ContentType,
		CacheControl:       u.opts.CacheControl,
		ContentDisposition: u.opts.ContentDisposition,
		ContentEncoding:    u.opts.ContentEncoding,
		ContentLanguage:    u.opts.ContentLanguage,
		Metadata:           u.opts.Metadata,
		ContentMD5:         u.opts.ContentMD5,
		// blob.Writer sniffs the content type when it is empty, using the first
		// bytes written. Those bytes are part 1 here, which is not the caller's
		// choice of type and not what a native multipart backend would report,
		// so leave the type empty rather than let it be guessed inconsistently.
		DisableContentTypeDetection: u.opts.ContentType == "",
	}
}
