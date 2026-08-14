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

// Package multipart assembles one blob from parts that may be uploaded in any
// order, concurrently, or from separate processes.
//
// gocloud.dev/blob.Writer cannot express that. Writer is a single sequential
// stream owned by one goroutine; drivers may fan out underneath it — s3blob
// does, via transfermanager, and azureblob via UploadStream — but the upload
// belongs to one process and dies with it. This package gives the caller a
// durable UploadID instead, so an upload can be started on one machine, have
// its parts written from others, be committed by a third, and be resumed after
// a crash rather than restarted.
//
// If all you want is for a large object to upload quickly, you do not need this
// package: set blob.WriterOptions.BufferSize and MaxConcurrency and use an
// ordinary Writer. Reach for multipart when you need the upload itself to
// outlive the process that started it.
//
// # Relationship to gocloud.dev/blob
//
// This is not a blob driver and does not wrap one. blob.Bucket is a concrete
// struct, so no external package can add methods to it; multipart is therefore
// a free function over a bucket rather than a method on it. Use blob.Bucket for
// everything else and come here only for the write:
//
//	u, err := multipart.NewUploader(ctx, bucket, "big/object.bin", nil)
//	// ... upload parts from wherever, then Commit ...
//	r, err := bucket.NewReader(ctx, "big/object.bin", nil) // reads what was committed
//
// # Two implementations
//
// NewUploader, in this package, works with any *blob.Bucket using only the
// public blob API. It stages each part as an ordinary object and assembles them
// on Commit. Because every read and write goes through the bucket, the driver
// applies its own key escaping and any blob.PrefixedBucket wrapping, so the
// committed object always lands where blob.Bucket will find it. This is the
// implementation to use for fileblob, sftpblob, httpblob over WebDAV, memblob,
// and anything else.
//
// Its one cost is that Commit reads every part back and rewrites it, roughly
// doubling the bytes transferred. The sibling packages — s3mp, gcsmp, azmp —
// avoid that by driving each provider's native multipart, so assembly happens
// server side. They bypass the driver and therefore need a Location and their
// own key escaping; see those packages.
//
// # Portability
//
// Backends differ in ways that cannot be hidden, so this package makes them
// visible rather than surprising:
//
//   - Parts are identified by number alone. There is deliberately no way to
//     pass a byte offset, so no backend can quietly require the caller to
//     compute the finished object's layout.
//   - Size limits differ. S3 rejects a non-final part below 5 MiB; a filesystem
//     does not care. Ask the Uploader for its Constraints rather than
//     discovering the difference at Commit.
//   - A Part is opaque and serializable. Its contents mean something only to
//     the backend that produced it.
//
// # Abandoned uploads
//
// Parts occupy storage and are billed for from the moment they are written. An
// Uploader that is abandoned without Abort — because the process was killed,
// say — leaves them behind, and no client can clean up after a process that no
// longer exists. Every backend has this property, including the cloud ones.
// Set a lifecycle rule on the bucket to expire incomplete uploads; for the
// generic implementation those objects are ordinary blobs under a
// ".gocdkmpu." infix, and for S3 the rule is AbortIncompleteMultipartUpload.
package multipart // import "github.com/TuSKan/gocloud-ext/blob/multipart"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
)

// MaxPartNumber is the largest part number this package accepts. It matches
// S3's limit, which is the lowest among the supported backends, so a part
// number valid here is valid everywhere.
const MaxPartNumber = 10000

// Sentinel errors. Backends wrap these, so compare with errors.Is.
var (
	// ErrUploadNotFound reports that an UploadID does not name a live upload:
	// it was never created, it was already committed or aborted, or the
	// backend expired it.
	ErrUploadNotFound = errors.New("multipart: upload not found")

	// ErrNoParts is returned by Commit when given no parts. Committing nothing
	// is a caller mistake rather than a request for an empty object; use
	// blob.Bucket.WriteAll for that.
	ErrNoParts = errors.New("multipart: commit requires at least one part")

	// ErrPartTooSmall reports a non-final part below the backend's minimum
	// size. Backends that can detect this at UploadPart do; S3 cannot, because
	// it does not know which part is last until Commit.
	ErrPartTooSmall = errors.New("multipart: part is below the backend's minimum size")

	// ErrClosed reports that the Uploader has already been committed or
	// aborted.
	ErrClosed = errors.New("multipart: uploader is already finished")
)

// InvalidPartNumberError reports a part number outside the accepted range.
type InvalidPartNumberError struct {
	Number int64
	Max    int64
}

func (e *InvalidPartNumberError) Error() string {
	return fmt.Sprintf("multipart: part number %d out of range; must be 1 to %d", e.Number, e.Max)
}

// DuplicatePartError reports two parts sharing a number, which would make the
// assembled object depend on the order Commit happened to iterate in.
type DuplicatePartError struct {
	Number int64
}

func (e *DuplicatePartError) Error() string {
	return fmt.Sprintf("multipart: duplicate part number %d", e.Number)
}

// Constraints describes the limits a backend imposes. Query them rather than
// assuming; they are what differs most between backends.
type Constraints struct {
	// MinPartSize is the smallest allowed size, in bytes, for any part other
	// than the one with the highest number. Zero means no minimum.
	//
	// S3 enforces 5 MiB and rejects the upload at Commit rather than at
	// UploadPart, because until Commit it cannot know which part is last.
	MinPartSize int64

	// MaxPartSize is the largest allowed size for one part, in bytes. Zero
	// means no limit.
	MaxPartSize int64

	// MaxParts is the largest number of parts one upload may have. Zero means
	// MaxPartNumber.
	MaxParts int64

	// Resumable reports whether UploadID can be handed to the backend's Open
	// function to continue an upload.
	//
	// Whether that works from a *different process* depends on the underlying
	// storage, not on this package: an upload staged in memblob is resumable
	// only within the process that holds the store, while the same code over
	// fileblob or S3 survives a restart.
	Resumable bool
}

// Part identifies one uploaded part.
//
// Everything but Number is filled in by the backend and is opaque to the
// caller. Part is serializable so that a process which uploads a part can hand
// it to whichever process performs the Commit; that is the whole point of this
// package, and a Part that does not survive encoding/json is a bug in the
// backend rather than a limitation of the caller.
type Part struct {
	// Number is the part's position in the finished object, counting from 1.
	// Parts are assembled in ascending Number regardless of the order they
	// were uploaded in. Set by the caller.
	Number int64 `json:"number"`

	// Size is the number of bytes the backend stored for this part.
	Size int64 `json:"size"`

	// Tag is the backend's handle for the stored part — an S3 ETag, an Azure
	// block ID, the name of a staged object. Commit needs it; treat it as
	// opaque and do not construct one by hand.
	Tag string `json:"tag,omitempty"`
}

// Options set attributes on the object that Commit produces.
//
// These mirror the subset of blob.WriterOptions a multipart upload can honor.
// Content type is not sniffed: no backend sees the whole object before Commit,
// so an empty ContentType stays empty rather than being guessed from whichever
// part happened to arrive first.
type Options struct {
	// ContentType is the MIME type of the finished object.
	ContentType string

	// These are set on the finished object where the backend supports them.
	CacheControl       string
	ContentDisposition string
	ContentEncoding    string
	ContentLanguage    string

	// Metadata holds key/value pairs to associate with the finished object.
	// Keys are lowercased, matching blob.Bucket, so that an object written
	// here reads back through blob.Bucket with the metadata it was given.
	Metadata map[string]string

	// ContentMD5, if set, is checked against the finished object by backends
	// able to do so.
	ContentMD5 []byte
}

// Uploader assembles a single object from parts.
//
// UploadPart is safe to call concurrently from multiple goroutines; that is the
// primary reason to use this package. Commit and Abort are not concurrent-safe:
// call exactly one of them, once, after every UploadPart has returned.
type Uploader interface {
	// UploadID returns this upload's identifier. When Constraints.Resumable is
	// true it can be passed to the backend's Open function later to continue
	// the upload.
	UploadID() string

	// Constraints reports the backend's limits.
	Constraints() Constraints

	// UploadPart stores r as the part with the given number, which must be
	// between 1 and Constraints.MaxParts. It reads r to EOF and returns the
	// Part that Commit requires.
	//
	// There is deliberately no offset parameter. Assembling parts in ascending
	// Number is the backend's job, and a caller must never have to work out
	// where a part belongs in the finished object.
	UploadPart(ctx context.Context, number int64, r io.Reader) (Part, error)

	// ListParts returns the parts already stored for this upload, ordered by
	// Number.
	//
	// This is what makes resuming useful: after Open, it is how a process
	// discovers which parts it does not need to send again, and it supplies
	// the Part values Commit needs without the original uploader having to
	// hand them over.
	ListParts(ctx context.Context) ([]Part, error)

	// Commit assembles the parts into the final object. They may be given in
	// any order, and must have come from UploadPart on an upload with this
	// UploadID. Nothing is readable at the destination key until Commit
	// returns successfully.
	//
	// After Commit the Uploader is finished and further calls return ErrClosed.
	Commit(ctx context.Context, parts []Part) error

	// Abort discards the upload and any parts stored for it. It is safe to call
	// on an upload that was already aborted.
	//
	// Abort is best-effort cleanup, not a guarantee; see the package
	// documentation on abandoned uploads.
	Abort(ctx context.Context) error
}

// maxParts resolves the effective part-number ceiling for c.
func maxParts(c Constraints) int64 {
	if c.MaxParts <= 0 || c.MaxParts > MaxPartNumber {
		return MaxPartNumber
	}
	return c.MaxParts
}

// ValidatePartNumber reports whether number is usable under c. Backends call it
// so the error is identical whichever backend produced it.
func ValidatePartNumber(number int64, c Constraints) error {
	max := maxParts(c)
	if number < 1 || number > max {
		return &InvalidPartNumberError{Number: number, Max: max}
	}
	return nil
}

// SortParts returns parts ordered by Number, leaving the caller's slice
// untouched, and rejects duplicates.
//
// Backends use it so that "assembled in ascending part number" means the same
// thing everywhere, and so that a duplicate is an error rather than a silently
// order-dependent object.
func SortParts(parts []Part) ([]Part, error) {
	if len(parts) == 0 {
		return nil, ErrNoParts
	}
	out := make([]Part, len(parts))
	copy(out, parts)
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	for i := 1; i < len(out); i++ {
		if out[i].Number == out[i-1].Number {
			return nil, &DuplicatePartError{Number: out[i].Number}
		}
	}
	return out, nil
}

// CheckPartSizes reports the first non-final part smaller than
// c.MinPartSize. parts must already be sorted by SortParts.
//
// Backends that cannot check sizes until Commit use this to produce the same
// error the others produce at UploadPart.
func CheckPartSizes(parts []Part, c Constraints) error {
	if c.MinPartSize <= 0 || len(parts) < 2 {
		return nil
	}
	// The highest-numbered part is the last one and is exempt.
	for _, p := range parts[:len(parts)-1] {
		if p.Size < c.MinPartSize {
			return fmt.Errorf("%w: part %d is %d bytes, minimum is %d",
				ErrPartTooSmall, p.Number, p.Size, c.MinPartSize)
		}
	}
	return nil
}

// Location names the destination for the native backends — s3mp, gcsmp and
// azmp — which write through a provider SDK rather than through blob.Bucket.
//
// The generic Uploader from NewUploader does not use this: it goes through the
// bucket, which supplies both of these itself.
//
// Location exists because blob.Bucket.As exposes a backend's client and nothing
// else — not the bucket name, and not any prefix.
type Location struct {
	// Bucket is the S3 bucket, GCS bucket, or Azure container. Required.
	Bucket string

	// Prefix must match whatever was passed to blob.PrefixedBucket, or the
	// "prefix" query parameter of the URL the bucket was opened with. Leave it
	// empty when the bucket is not wrapped.
	//
	// This cannot be detected. A prefixed bucket is indistinguishable from an
	// unprefixed one through As, so an unset Prefix here writes the object to
	// the unprefixed key, where blob.Bucket will not look for it.
	Prefix string
}

// ObjectName returns the backend-level name for key under loc, applying the
// prefix and then esc, the backend's key escaping.
//
// The native backends route through this so an object lands on exactly the name
// the corresponding gocloud.dev driver would have used. Escaping is applied
// after the prefix is joined, matching the drivers, which escape the key they
// receive once a prefixed bucket has already prepended to it.
func ObjectName(loc Location, key string, esc func(string) string) string {
	full := loc.Prefix + key
	if esc == nil {
		return full
	}
	return esc(full)
}
