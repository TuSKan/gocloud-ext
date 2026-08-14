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

package azmp_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/TuSKan/gocloud-ext/blob/multipart"
	"github.com/TuSKan/gocloud-ext/blob/multipart/azmp"
	"github.com/TuSKan/gocloud-ext/blob/multipart/mptest"
	"gocloud.dev/blob"

	_ "gocloud.dev/blob/azureblob"
)

// Environment pointing the suite at an Azure Blob endpoint. CI runs Azurite;
// see .github/workflows/ci.yml.
const (
	envConnString = "AZMP_TEST_CONNECTION_STRING"
	envContainer  = "AZMP_TEST_CONTAINER"
)

// TestConformanceAzure runs the suite against a real Azure Blob implementation.
//
// Reads go through gocloud.dev/blob/azureblob, so a mismatch between this
// package's key escaping and azureblob's shows up as an object that cannot be
// found rather than as a silent success.
func TestConformanceAzure(t *testing.T) {
	conn := os.Getenv(envConnString)
	containerName := os.Getenv(envContainer)
	if conn == "" || containerName == "" {
		t.Skipf("set %s and %s to run conformance against an Azure Blob endpoint", envConnString, envContainer)
	}

	// Create the container here rather than in the CI script. Doing it from
	// the shell meant a second client — the Azure CLI — whose negotiated
	// x-ms-version had to agree with the emulator's, and that mismatch is what
	// broke first. This client is one the test already needs.
	ctx := context.Background()
	cc, err := container.NewClientFromConnectionString(conn, containerName, nil)
	if err != nil {
		t.Fatalf("building the container client: %v", err)
	}
	if _, err := cc.Create(ctx, nil); err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		t.Fatalf("creating container %q: %v", containerName, err)
	}

	// A prefix per run, which also exercises the Prefix path — the one that
	// cannot be detected from the client and is easiest to get wrong.
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("mp-%s/", hex.EncodeToString(buf[:]))
	loc := multipart.Location{Bucket: containerName, Prefix: prefix}

	bucketURL := fmt.Sprintf("azblob://%s?prefix=%s", containerName, prefix)

	mptest.RunConformanceTests(t, func(ctx context.Context, t *testing.T) (mptest.Harness, error) {
		return &azHarness{conn: conn, loc: loc, url: bucketURL}, nil
	})
}

type azHarness struct {
	conn    string
	loc     multipart.Location
	url     string
	buckets []*blob.Bucket
}

// blobClient builds a client for one blob. azmp works on a single blob at a
// time because Azure's block list belongs to the blob, not to a session.
func (h *azHarness) blobClient(key string) (*blockblob.Client, error) {
	name := multipart.ObjectName(h.loc, key, azmp.EscapeKey)
	return blockblob.NewClientFromConnectionString(h.conn, h.loc.Bucket, name, nil)
}

func (h *azHarness) NewUploader(ctx context.Context, t *testing.T, key string, opts *multipart.Options) (multipart.Uploader, error) {
	c, err := h.blobClient(key)
	if err != nil {
		return nil, err
	}
	return azmp.NewUploader(ctx, c, h.loc, key, opts)
}

func (h *azHarness) Open(ctx context.Context, t *testing.T, key, uploadID string) (multipart.Uploader, error) {
	c, err := h.blobClient(key)
	if err != nil {
		return nil, err
	}
	return azmp.Open(ctx, c, h.loc, key, uploadID, nil)
}

func (h *azHarness) Bucket(ctx context.Context, t *testing.T) (*blob.Bucket, error) {
	b, err := blob.OpenBucket(ctx, h.url)
	if err != nil {
		return nil, err
	}
	h.buckets = append(h.buckets, b)
	return b, nil
}

func (h *azHarness) Close() {
	for _, b := range h.buckets {
		_ = b.Close()
	}
}
