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

package mptest

import (
	"context"
	"testing"

	"github.com/TuSKan/gocloud-ext/blob/multipart"
	"gocloud.dev/blob"
)

// BucketOpener returns a *blob.Bucket to run the suite against.
//
// SeparateHandles records whether a second call shares storage with the first.
// Two fileblob or sftpblob buckets over the same directory do; two
// memblob.OpenBucket calls do not, because each allocates its own map. When it
// is true the suite resumes an upload through a handle that did not create it,
// which is the cross-process path multipart exists for; when false, one bucket
// is reused throughout, which is the only way an in-memory driver can be tested.
type BucketOpener struct {
	Open            func() (*blob.Bucket, error)
	SeparateHandles bool
}

// BucketHarness returns a HarnessMaker that runs the suite against
// multipart.NewUploader, the generic implementation, over buckets from opener.
//
// Any blob driver can be checked with it in a few lines:
//
//	func TestMultipart(t *testing.T) {
//		mptest.RunConformanceTests(t, mptest.BucketHarness(mptest.BucketOpener{
//			Open:            func() (*blob.Bucket, error) { return fileblob.OpenBucket(dir, nil) },
//			SeparateHandles: true,
//		}))
//	}
func BucketHarness(opener BucketOpener) HarnessMaker {
	return func(ctx context.Context, t *testing.T) (Harness, error) {
		b, err := opener.Open()
		if err != nil {
			return nil, err
		}
		return &bucketHarness{opener: opener, primary: b}, nil
	}
}

type bucketHarness struct {
	opener  BucketOpener
	primary *blob.Bucket
	extra   []*blob.Bucket
}

// handle returns a bucket for reads or for a resumed upload: an independent one
// where the driver supports it, otherwise the primary.
func (h *bucketHarness) handle() (*blob.Bucket, error) {
	if !h.opener.SeparateHandles {
		return h.primary, nil
	}
	b, err := h.opener.Open()
	if err != nil {
		return nil, err
	}
	h.extra = append(h.extra, b)
	return b, nil
}

func (h *bucketHarness) NewUploader(ctx context.Context, t *testing.T, key string, opts *multipart.Options) (multipart.Uploader, error) {
	return multipart.NewUploader(ctx, h.primary, key, opts)
}

func (h *bucketHarness) Open(ctx context.Context, t *testing.T, key, uploadID string) (multipart.Uploader, error) {
	b, err := h.handle()
	if err != nil {
		return nil, err
	}
	return multipart.Open(ctx, b, key, uploadID, nil)
}

func (h *bucketHarness) Bucket(ctx context.Context, t *testing.T) (*blob.Bucket, error) {
	return h.handle()
}

func (h *bucketHarness) Close() {
	for _, b := range h.extra {
		_ = b.Close()
	}
	_ = h.primary.Close()
}
