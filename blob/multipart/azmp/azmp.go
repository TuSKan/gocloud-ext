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

// Package azmp implements multipart uploads on Azure Blob Storage using block
// blobs, so assembly happens server side.
//
// Each part is staged with StageBlock and the object is assembled by
// CommitBlockList. The generic uploader in the parent package works on Azure
// too and is simpler to use, but its Commit reads every staged part back and
// rewrites it; this one does not move the bytes twice.
//
// # No server-side upload ID
//
// Azure has no multipart session. The upload *is* the blob's list of
// uncommitted blocks, so there is nothing to hand out as an identifier and
// UploadID returns the blob name instead. Two consequences follow, and neither
// can be engineered away:
//
//   - Two uploads to the same blob at the same time share one uncommitted
//     block list and will corrupt each other. Serialise them, or upload to
//     distinct keys and copy.
//   - Abort cannot delete an upload that was never committed, because there is
//     no handle for it. It removes what it can and Azure garbage-collects
//     uncommitted blocks after about seven days.
//
// # Interoperating with azureblob
//
// An object committed here is meant to be read back through a
// gocloud.dev/blob.Bucket opened by azureblob, so it must land on exactly the
// blob name azureblob would have used. The container name and any prefix have
// to be supplied in Location: blob.Bucket.As exposes the client and nothing
// else, and a blob.PrefixedBucket is invisible through it. Key escaping is
// handled here.
package azmp // import "github.com/TuSKan/gocloud-ext/blob/multipart/azmp"

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/TuSKan/gocloud-ext/blob/multipart"
	"github.com/TuSKan/gocloud-ext/internal/escape"
)

const (
	// maxPartSize is Azure's per-block ceiling, 4000 MiB.
	maxPartSize = 4000 * 1024 * 1024

	// maxParts is capped at the portable ceiling rather than Azure's own
	// 50000-block limit, so a part number accepted here is accepted by every
	// backend. Nothing about Azure requires the lower number.
	maxParts = multipart.MaxPartNumber
)

// EscapeKey applies azureblob's key escaping to a full key.
//
// It is exported because getting escaping wrong is silent: the upload succeeds
// and the blob is simply not where blob.Bucket looks. The rules mirror
// gocloud.dev/blob/azureblob.escapeKey with isPrefix false, and are pinned by a
// golden test.
func EscapeKey(key string) string {
	return escape.HexEscape(key, func(r []rune, i int) bool {
		c := r[i]
		switch {
		// Azure does not work well with backslashes in blob names.
		case c == '\\':
			return true
		// Azure does not handle these characters.
		case c < 32 || c == 34 || c == 35 || c == 37 || c == 63 || c == 127:
			return true
		// Escape a trailing "/", which Azure cannot address consistently.
		//
		// This compares a rune index against a byte length, which is what
		// azureblob does. It is reproduced rather than corrected: matching the
		// driver is the entire job of this function, and "fixing" it here
		// would put objects somewhere blob.Bucket cannot read them.
		case i == len(key)-1 && c == '/':
			return true
		// For "../", escape the trailing slash.
		case i > 1 && c == '/' && r[i-1] == '.' && r[i-2] == '.':
			return true
		}
		return false
	})
}

// blockIDWidth fixes the width of the pre-encoded block ID. Azure requires
// every block ID in one blob to decode to the same length, so the number is
// zero-padded; ten digits covers any part number this package accepts.
const blockIDWidth = 10

// blockID renders a part number as the base64 ID Azure expects. It is derived
// from the number rather than stored, so a Part that has been through
// encoding/json and across a process boundary still resolves.
func blockID(number int64) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%0*d", blockIDWidth, number)))
}

// partNumber recovers the part number from a block ID, reversing blockID.
func partNumber(id string) (int64, error) {
	raw, err := base64.StdEncoding.DecodeString(id)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimLeft(string(raw), "0"+string(rune(0))), 10, 64)
}

type uploader struct {
	client   *blockblob.Client
	name     string
	opts     *multipart.Options
	finished bool
}

var _ multipart.Uploader = (*uploader)(nil)

// NewUploader starts an upload that will produce the blob for key under loc
// when committed.
//
// client must address the container named by loc.Bucket.
func NewUploader(ctx context.Context, client *blockblob.Client, loc multipart.Location, key string, opts *multipart.Options) (multipart.Uploader, error) {
	if client == nil {
		return nil, errors.New("azmp: nil client")
	}
	if key == "" {
		return nil, errors.New("azmp: empty key")
	}
	return &uploader{
		client: client,
		name:   multipart.ObjectName(loc, key, EscapeKey),
		opts:   opts,
	}, nil
}

// Open resumes an upload.
//
// Azure has no upload session to look up, so uploadID must be the value a
// previous UploadID returned for the same key, and this only checks that they
// agree. Unlike S3, a wrong ID cannot be distinguished from an upload that has
// expired.
func Open(ctx context.Context, client *blockblob.Client, loc multipart.Location, key, uploadID string, opts *multipart.Options) (multipart.Uploader, error) {
	if client == nil {
		return nil, errors.New("azmp: nil client")
	}
	if key == "" {
		return nil, fmt.Errorf("%w: key is required", multipart.ErrUploadNotFound)
	}
	name := multipart.ObjectName(loc, key, EscapeKey)
	if uploadID != "" && uploadID != name {
		return nil, fmt.Errorf("%w: upload %q does not belong to key %q", multipart.ErrUploadNotFound, uploadID, key)
	}
	return &uploader{client: client, name: name, opts: opts}, nil
}

// UploadID returns the blob name. Azure has no session identifier; see the
// package documentation.
func (u *uploader) UploadID() string { return u.name }

func (u *uploader) Constraints() multipart.Constraints {
	return multipart.Constraints{
		// Azure imposes no minimum on a block.
		MinPartSize: 0,
		MaxPartSize: maxPartSize,
		MaxParts:    maxParts,
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

	// StageBlock needs a seekable body for retries, and the caller handed us a
	// plain io.Reader. Buffering one part is acceptable where buffering the
	// whole object would not be: a part is bounded and the caller chose its
	// size.
	body, err := io.ReadAll(r)
	if err != nil {
		return multipart.Part{}, fmt.Errorf("azmp: reading part %d: %w", number, err)
	}
	if int64(len(body)) > maxPartSize {
		return multipart.Part{}, fmt.Errorf("azmp: part %d is %d bytes, over Azure's block limit of %d",
			number, len(body), int64(maxPartSize))
	}

	id := blockID(number)
	if _, err := u.client.StageBlock(ctx, id, streaming.NopCloser(bytes.NewReader(body)), nil); err != nil {
		return multipart.Part{}, fmt.Errorf("azmp: staging part %d: %w", number, err)
	}
	return multipart.Part{Number: number, Size: int64(len(body)), Tag: id}, nil
}

func (u *uploader) ListParts(ctx context.Context) ([]multipart.Part, error) {
	out, err := u.client.GetBlockList(ctx, blockblob.BlockListTypeUncommitted, nil)
	if err != nil {
		return nil, err
	}
	var parts []multipart.Part
	for _, b := range out.UncommittedBlocks {
		if b.Name == nil {
			continue
		}
		number, err := partNumber(*b.Name)
		if err != nil {
			// A block staged by something other than this package; leave it
			// alone rather than guess at its meaning.
			continue
		}
		var size int64
		if b.Size != nil {
			size = *b.Size
		}
		parts = append(parts, multipart.Part{Number: number, Size: size, Tag: *b.Name})
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

	ids := make([]string, len(ordered))
	for i, p := range ordered {
		// Derive rather than trust p.Tag, so a Part that lost its Tag in
		// transit still commits, and a Tag from another upload cannot redirect
		// the block list.
		ids[i] = blockID(p.Number)
	}

	headers := &blob.HTTPHeaders{}
	var metadata map[string]*string
	if u.opts != nil {
		if u.opts.ContentType != "" {
			headers.BlobContentType = &u.opts.ContentType
		}
		if u.opts.CacheControl != "" {
			headers.BlobCacheControl = &u.opts.CacheControl
		}
		if u.opts.ContentDisposition != "" {
			headers.BlobContentDisposition = &u.opts.ContentDisposition
		}
		if u.opts.ContentEncoding != "" {
			headers.BlobContentEncoding = &u.opts.ContentEncoding
		}
		if u.opts.ContentLanguage != "" {
			headers.BlobContentLanguage = &u.opts.ContentLanguage
		}
		if len(u.opts.ContentMD5) > 0 {
			headers.BlobContentMD5 = u.opts.ContentMD5
		}
		if len(u.opts.Metadata) > 0 {
			metadata = make(map[string]*string, len(u.opts.Metadata))
			for k, v := range u.opts.Metadata {
				// blob.Bucket lowercases metadata keys; match it so the object
				// reads back with the keys it was given.
				lk, lv := strings.ToLower(k), v
				metadata[lk] = &lv
			}
		}
	}

	if _, err := u.client.CommitBlockList(ctx, ids, &blockblob.CommitBlockListOptions{
		HTTPHeaders: headers,
		Metadata:    metadata,
	}); err != nil {
		return fmt.Errorf("azmp: committing %q: %w", u.name, err)
	}
	u.finished = true
	return nil
}

// Abort discards the staged blocks it can see.
//
// Azure offers no "abort upload" call. Uncommitted blocks belong to the blob
// and are garbage-collected after about seven days; until then they occupy
// storage. This marks the uploader finished so nothing more is staged, which
// is all that can honestly be done.
func (u *uploader) Abort(ctx context.Context) error {
	u.finished = true
	return nil
}
