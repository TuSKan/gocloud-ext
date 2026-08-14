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

package gcsmp_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/TuSKan/gocloud-ext/blob/multipart"
	"github.com/TuSKan/gocloud-ext/blob/multipart/gcsmp"
	"github.com/TuSKan/gocloud-ext/blob/multipart/mptest"
	"gocloud.dev/blob"
	"gocloud.dev/gcp"
	"google.golang.org/api/option"

	gcsblob "gocloud.dev/blob/gcsblob"
)

// Environment pointing the suite at a GCS endpoint. CI runs fake-gcs-server,
// which the Go client picks up from STORAGE_EMULATOR_HOST; see
// .github/workflows/ci.yml.
const envBucket = "GCSMP_TEST_BUCKET"

// TestConformanceGCS runs the suite against a real GCS implementation.
//
// Reads go through gocloud.dev/blob/gcsblob, so a mismatch between this
// package's key escaping and gcsblob's shows up as an object that cannot be
// found rather than as a silent success.
func TestConformanceGCS(t *testing.T) {
	bucket := os.Getenv(envBucket)
	if bucket == "" {
		t.Skipf("set %s (and STORAGE_EMULATOR_HOST) to run conformance against GCS", envBucket)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("building the storage client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	// A prefix per run, which also exercises the Prefix path — the one that
	// cannot be detected from the client and is easiest to get wrong.
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("mp-%s/", hex.EncodeToString(buf[:]))
	loc := multipart.Location{Bucket: bucket, Prefix: prefix}

	mptest.RunConformanceTests(t, func(ctx context.Context, t *testing.T) (mptest.Harness, error) {
		return &gcsHarness{client: client, loc: loc, prefix: prefix}, nil
	})
}

type gcsHarness struct {
	client  *storage.Client
	loc     multipart.Location
	prefix  string
	buckets []*blob.Bucket
}

func (h *gcsHarness) NewUploader(ctx context.Context, t *testing.T, key string, opts *multipart.Options) (multipart.Uploader, error) {
	return gcsmp.NewUploader(ctx, h.client, h.loc, key, opts)
}

func (h *gcsHarness) Open(ctx context.Context, t *testing.T, key, uploadID string) (multipart.Uploader, error) {
	return gcsmp.Open(ctx, h.client, h.loc, key, uploadID, nil)
}

func (h *gcsHarness) Bucket(ctx context.Context, t *testing.T) (*blob.Bucket, error) {
	// gcsblob is opened unauthenticated, like the uploader's client, so both
	// address the emulator identically and the read-back really does check
	// that the two agree on the object name.
	hc := gcp.NewAnonymousHTTPClient(gcp.DefaultTransport())
	b, err := gcsblob.OpenBucket(ctx, hc, h.loc.Bucket, nil)
	if err != nil {
		return nil, err
	}
	pb := blob.PrefixedBucket(b, h.prefix)
	h.buckets = append(h.buckets, pb)
	return pb, nil
}

func (h *gcsHarness) Close() {
	for _, b := range h.buckets {
		_ = b.Close()
	}
}
