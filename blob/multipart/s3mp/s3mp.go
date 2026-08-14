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

// Package s3mp implements multipart uploads on S3 using S3's own multipart
// API, so assembly happens server side.
//
// The generic uploader in the parent package works on S3 too, and is simpler to
// use because it needs no bucket name and no SDK client. Its Commit reads every
// staged part back and rewrites it, though, which doubles the bytes
// transferred. Use this package when that matters, and the generic one
// otherwise.
//
// # Interoperating with s3blob
//
// An object committed here is meant to be read back through a
// gocloud.dev/blob.Bucket opened by s3blob, so it has to land on exactly the
// object name s3blob would have used. Two things are needed for that, and
// neither can be discovered from the SDK client:
//
//   - The bucket name, which blob.Bucket.As does not expose.
//   - Any prefix, because blob.PrefixedBucket is invisible through As. If the
//     bucket was wrapped, or opened from a URL with "?prefix=", that prefix
//     must be given in Location.Prefix or the object lands somewhere
//     blob.Bucket will not look.
//
// Key escaping is handled here, reproducing s3blob's rules.
//
// # Abandoned uploads
//
// An upload left neither committed nor aborted keeps its parts, and S3 bills
// for them. Abort cleans up, but nothing can clean up after a process that
// died. Set an S3 lifecycle rule with AbortIncompleteMultipartUpload on any
// bucket used for multipart uploads.
package s3mp // import "github.com/TuSKan/gocloud-ext/blob/multipart/s3mp"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/TuSKan/gocloud-ext/blob/multipart"
	"github.com/TuSKan/gocloud-ext/internal/escape"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3 limits, from
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html.
const (
	// minPartSize is the smallest S3 accepts for any part but the last.
	minPartSize = 5 * 1024 * 1024
	// maxPartSize is the largest S3 accepts for a single part.
	maxPartSize = 5 * 1024 * 1024 * 1024
	// maxParts is S3's ceiling on parts in one upload.
	maxParts = 10000
)

// API is the subset of the S3 client this package uses. Taking an interface
// rather than *s3.Client keeps the package testable and lets a caller wrap the
// client with their own middleware.
type API interface {
	CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, in *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	ListParts(ctx context.Context, in *s3.ListPartsInput, optFns ...func(*s3.Options)) (*s3.ListPartsOutput, error)
}

var _ API = (*s3.Client)(nil)

// EscapeKey applies s3blob's key escaping.
//
// It is exported because getting it wrong is silent: the object still uploads,
// but under a name blob.Bucket cannot find. Keeping it public makes it
// testable and lets a caller check where an object will land.
//
// The rules mirror gocloud.dev/blob/s3blob.escapeKey and are pinned by a
// golden test.
func EscapeKey(key string) string {
	return escape.HexEscape(key, func(r []rune, i int) bool {
		c := r[i]
		switch {
		// S3 does not handle these.
		case c < 32:
			return true
		// For "../", escape the trailing slash.
		case i > 1 && c == '/' && r[i-1] == '.' && r[i-2] == '.':
			return true
		}
		return false
	})
}

type uploader struct {
	client   API
	loc      multipart.Location
	key      string // the escaped object name
	uploadID string
	finished bool
}

var _ multipart.Uploader = (*uploader)(nil)

// NewUploader starts an S3 multipart upload that will produce the object for
// key under loc when committed.
func NewUploader(ctx context.Context, client API, loc multipart.Location, key string, opts *multipart.Options) (multipart.Uploader, error) {
	if client == nil {
		return nil, errors.New("s3mp: nil client")
	}
	if loc.Bucket == "" {
		return nil, errors.New("s3mp: Location.Bucket is required; blob.Bucket.As cannot supply it")
	}
	if key == "" {
		return nil, errors.New("s3mp: empty key")
	}
	name := multipart.ObjectName(loc, key, EscapeKey)

	in := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(loc.Bucket),
		Key:    aws.String(name),
	}
	if opts != nil {
		// Only set what the caller asked for: S3 records an empty ContentType
		// as a literal empty header rather than omitting it.
		if opts.ContentType != "" {
			in.ContentType = aws.String(opts.ContentType)
		}
		if opts.CacheControl != "" {
			in.CacheControl = aws.String(opts.CacheControl)
		}
		if opts.ContentDisposition != "" {
			in.ContentDisposition = aws.String(opts.ContentDisposition)
		}
		if opts.ContentEncoding != "" {
			in.ContentEncoding = aws.String(opts.ContentEncoding)
		}
		if opts.ContentLanguage != "" {
			in.ContentLanguage = aws.String(opts.ContentLanguage)
		}
		if len(opts.Metadata) > 0 {
			in.Metadata = lowercaseKeys(opts.Metadata)
		}
	}

	out, err := client.CreateMultipartUpload(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("s3mp: creating upload for %q: %w", name, err)
	}
	return &uploader{
		client:   client,
		loc:      loc,
		key:      name,
		uploadID: aws.ToString(out.UploadId),
	}, nil
}

// Open resumes an upload from its ID.
//
// The upload is confirmed to exist before returning, so a mistyped or expired
// ID fails here rather than much later at Commit with an error that says
// nothing about which of the two it was.
//
// S3 does not report the options an upload was created with, so a resumed
// upload commits with whatever CreateMultipartUpload recorded — the content
// type and metadata are already fixed and cannot be changed now.
func Open(ctx context.Context, client API, loc multipart.Location, key, uploadID string) (multipart.Uploader, error) {
	if client == nil {
		return nil, errors.New("s3mp: nil client")
	}
	if loc.Bucket == "" {
		return nil, errors.New("s3mp: Location.Bucket is required")
	}
	if key == "" || uploadID == "" {
		return nil, fmt.Errorf("%w: key and uploadID are both required", multipart.ErrUploadNotFound)
	}
	name := multipart.ObjectName(loc, key, EscapeKey)

	if _, err := client.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   aws.String(loc.Bucket),
		Key:      aws.String(name),
		UploadId: aws.String(uploadID),
		MaxParts: aws.Int32(1),
	}); err != nil {
		if isNoSuchUpload(err) {
			return nil, fmt.Errorf("%w: %q for key %q", multipart.ErrUploadNotFound, uploadID, key)
		}
		return nil, err
	}
	return &uploader{client: client, loc: loc, key: name, uploadID: uploadID}, nil
}

// isNoSuchUpload reports whether err is S3's "that upload does not exist".
func isNoSuchUpload(err error) bool {
	var nsu *types.NoSuchUpload
	if errors.As(err, &nsu) {
		return true
	}
	// Some S3-compatible stores return the code without the modelled type.
	return strings.Contains(err.Error(), "NoSuchUpload")
}

// lowercaseKeys matches what blob.Bucket does to metadata keys, so an object
// written here reads back through blob.Bucket with the keys it was given.
func lowercaseKeys(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[strings.ToLower(k)] = v
	}
	return out
}

func (u *uploader) UploadID() string { return u.uploadID }

func (u *uploader) Constraints() multipart.Constraints {
	return multipart.Constraints{
		MinPartSize: minPartSize,
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

	// S3 signs each part, which needs a seekable body or a known length, and
	// the caller handed us a plain io.Reader. Buffering a part is acceptable
	// where streaming an arbitrary object is not: a part is bounded by
	// maxPartSize and the caller chose its size.
	body, err := io.ReadAll(r)
	if err != nil {
		return multipart.Part{}, fmt.Errorf("s3mp: reading part %d: %w", number, err)
	}
	if int64(len(body)) > maxPartSize {
		return multipart.Part{}, fmt.Errorf("s3mp: part %d is %d bytes, over S3's limit of %d",
			number, len(body), int64(maxPartSize))
	}

	out, err := u.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(u.loc.Bucket),
		Key:           aws.String(u.key),
		UploadId:      aws.String(u.uploadID),
		PartNumber:    aws.Int32(int32(number)),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	})
	if err != nil {
		return multipart.Part{}, fmt.Errorf("s3mp: uploading part %d: %w", number, err)
	}
	return multipart.Part{
		Number: number,
		Size:   int64(len(body)),
		Tag:    aws.ToString(out.ETag),
	}, nil
}

func (u *uploader) ListParts(ctx context.Context) ([]multipart.Part, error) {
	var parts []multipart.Part
	var marker *string
	for {
		out, err := u.client.ListParts(ctx, &s3.ListPartsInput{
			Bucket:           aws.String(u.loc.Bucket),
			Key:              aws.String(u.key),
			UploadId:         aws.String(u.uploadID),
			PartNumberMarker: marker,
		})
		if err != nil {
			if isNoSuchUpload(err) {
				return nil, fmt.Errorf("%w: %q", multipart.ErrUploadNotFound, u.uploadID)
			}
			return nil, err
		}
		for _, p := range out.Parts {
			parts = append(parts, multipart.Part{
				Number: int64(aws.ToInt32(p.PartNumber)),
				Size:   aws.ToInt64(p.Size),
				Tag:    aws.ToString(p.ETag),
			})
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		marker = out.NextPartNumberMarker
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
	// S3 reports an undersized part as EntityTooSmall at Commit, naming only
	// the part. Checking here produces the same error every other backend
	// gives, before spending a round trip on a request that cannot succeed.
	if err := multipart.CheckPartSizes(ordered, u.Constraints()); err != nil {
		return err
	}

	completed := make([]types.CompletedPart, len(ordered))
	for i, p := range ordered {
		completed[i] = types.CompletedPart{
			PartNumber: aws.Int32(int32(p.Number)),
			ETag:       aws.String(p.Tag),
		}
	}
	if _, err := u.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(u.loc.Bucket),
		Key:             aws.String(u.key),
		UploadId:        aws.String(u.uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	}); err != nil {
		return fmt.Errorf("s3mp: completing upload %q: %w", u.uploadID, err)
	}
	u.finished = true
	return nil
}

func (u *uploader) Abort(ctx context.Context) error {
	if u.finished {
		return nil
	}
	u.finished = true
	if _, err := u.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(u.loc.Bucket),
		Key:      aws.String(u.key),
		UploadId: aws.String(u.uploadID),
	}); err != nil {
		if isNoSuchUpload(err) {
			// Already gone, which is what Abort was asking for.
			return nil
		}
		return fmt.Errorf("s3mp: aborting upload %q: %w", u.uploadID, err)
	}
	return nil
}
