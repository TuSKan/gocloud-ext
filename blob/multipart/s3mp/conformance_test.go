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

package s3mp_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"github.com/TuSKan/gocloud-ext/blob/multipart"
	"github.com/TuSKan/gocloud-ext/blob/multipart/mptest"
	"github.com/TuSKan/gocloud-ext/blob/multipart/s3mp"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob"

	_ "gocloud.dev/blob/s3blob"
)

// Environment pointing the suite at an S3-compatible endpoint. CI runs MinIO;
// see .github/workflows/ci.yml. Real AWS works too if these are set to it.
const (
	envEndpoint = "S3MP_TEST_ENDPOINT"
	envBucket   = "S3MP_TEST_BUCKET"
	envKeyID    = "S3MP_TEST_ACCESS_KEY_ID"
	envSecret   = "S3MP_TEST_SECRET_ACCESS_KEY"
	envRegion   = "S3MP_TEST_REGION"
)

// TestConformanceS3 runs the suite against a real S3 implementation.
//
// The suite pads non-final parts to Constraints.MinPartSize, so this uploads
// 5 MiB parts for real. That is the point: S3's minimum is the constraint most
// likely to be discovered late, and a backend that only ever saw a filesystem
// would not have exercised it.
//
// Reads go through gocloud.dev/blob/s3blob, so a mismatch between this
// package's key escaping and s3blob's shows up as an object that cannot be
// found rather than as a silent success.
func TestConformanceS3(t *testing.T) {
	endpoint := os.Getenv(envEndpoint)
	bucket := os.Getenv(envBucket)
	if endpoint == "" || bucket == "" {
		t.Skipf("set %s and %s to run conformance against an S3-compatible endpoint", envEndpoint, envBucket)
	}

	ctx := context.Background()
	client, err := newTestClient(ctx, endpoint)
	if err != nil {
		t.Fatalf("building the S3 client: %v", err)
	}

	// A prefix per run, so leftovers from an earlier run cannot be mistaken
	// for this one's. Exercising a non-empty Prefix also covers the case most
	// likely to be got wrong, since it cannot be detected from the client.
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("mp-%s/", hex.EncodeToString(buf[:]))
	loc := multipart.Location{Bucket: bucket, Prefix: prefix}

	// s3blob opens the same bucket with the matching prefix, which is what
	// makes the read-back a real check that both agree on the object name.
	bucketURL := fmt.Sprintf("s3://%s?prefix=%s&endpoint=%s&use_path_style=true&disable_https=true&region=%s",
		bucket, prefix, endpoint, testRegion())

	mptest.RunConformanceTests(t, func(ctx context.Context, t *testing.T) (mptest.Harness, error) {
		return &s3Harness{client: client, loc: loc, url: bucketURL}, nil
	})
}

type s3Harness struct {
	client  *s3.Client
	loc     multipart.Location
	url     string
	buckets []*blob.Bucket
}

func (h *s3Harness) NewUploader(ctx context.Context, t *testing.T, key string, opts *multipart.Options) (multipart.Uploader, error) {
	return s3mp.NewUploader(ctx, h.client, h.loc, key, opts)
}

func (h *s3Harness) Open(ctx context.Context, t *testing.T, key, uploadID string) (multipart.Uploader, error) {
	return s3mp.Open(ctx, h.client, h.loc, key, uploadID)
}

func (h *s3Harness) Bucket(ctx context.Context, t *testing.T) (*blob.Bucket, error) {
	b, err := blob.OpenBucket(ctx, h.url)
	if err != nil {
		return nil, err
	}
	h.buckets = append(h.buckets, b)
	return b, nil
}

func (h *s3Harness) Close() {
	for _, b := range h.buckets {
		_ = b.Close()
	}
}

func testRegion() string {
	if r := os.Getenv(envRegion); r != "" {
		return r
	}
	return "us-east-1"
}

// newTestClient builds a client for an S3-compatible endpoint. Path-style
// addressing is required because MinIO does not serve virtual-host buckets on
// a bare host.
func newTestClient(ctx context.Context, endpoint string) (*s3.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(testRegion()),
	}
	if id := os.Getenv(envKeyID); id != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(id, os.Getenv(envSecret), ""),
		))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	}), nil
}
