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
	"net/http"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/TuSKan/gocloud-ext/blob/multipart"
	"github.com/TuSKan/gocloud-ext/blob/multipart/azmp"
	"github.com/TuSKan/gocloud-ext/blob/multipart/mptest"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
)

// Environment pointing the suite at an Azure Blob endpoint. CI runs Azurite;
// see .github/workflows/ci.yml.
const (
	envConnString = "AZMP_TEST_CONNECTION_STRING"
	envContainer  = "AZMP_TEST_CONTAINER"

	// envAPIVersion pins the x-ms-version the SDK sends. Needed only for
	// emulators; see forceAPIVersion.
	envAPIVersion = "AZMP_TEST_API_VERSION"
)

// forceAPIVersion overrides the x-ms-version header the SDK negotiates.
//
// The Azure SDK advertises a service version newer than any released Azurite
// implements. Azurite rejects it outright, and starting it with
// --skipApiVersionCheck only gets the request past the version gate: the
// shared-key signature is then computed over a version the emulator does not
// model, and it answers 403 AuthorizationFailure. Pinning the header to a
// version Azurite knows makes the two agree.
//
// This is a per-call policy on purpose. Per-call policies run before the
// shared-key credential policy, so the signature is computed over the header
// as overridden here; a per-retry policy would run after signing and produce
// the same 403 it is meant to avoid.
//
// It is applied only when envAPIVersion is set, so a run against real Azure
// uses whatever the SDK would normally send.
type forceAPIVersion struct{ version string }

func (p forceAPIVersion) Do(req *policy.Request) (*http.Response, error) {
	req.Raw().Header.Set("x-ms-version", p.version)
	return req.Next()
}

// clientOptions returns the options every client in this test is built with, so
// that azmp's uploads and azureblob's read-back speak the same service version.
func clientOptions() azcore.ClientOptions {
	var opts azcore.ClientOptions
	if v := os.Getenv(envAPIVersion); v != "" {
		opts.PerCallPolicies = []policy.Policy{forceAPIVersion{version: v}}
	}
	return opts
}

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

	// One container client is the root of everything here: the container is
	// created through it, azmp's per-blob clients are derived from it, and
	// azureblob reads through it. Derived clients inherit its pipeline, so the
	// version policy above applies to every request the test makes rather than
	// only the ones built directly.
	ctx := context.Background()
	cc, err := container.NewClientFromConnectionString(conn, containerName,
		&container.ClientOptions{ClientOptions: clientOptions()})
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

	mptest.RunConformanceTests(t, func(ctx context.Context, t *testing.T) (mptest.Harness, error) {
		return &azHarness{cc: cc, loc: loc, prefix: prefix}, nil
	})
}

type azHarness struct {
	cc      *container.Client
	loc     multipart.Location
	prefix  string
	buckets []*blob.Bucket
}

// blobClient builds a client for one blob. azmp works on a single blob at a
// time because Azure's block list belongs to the blob, not to a session.
func (h *azHarness) blobClient(key string) *blockblob.Client {
	return h.cc.NewBlockBlobClient(multipart.ObjectName(h.loc, key, azmp.EscapeKey))
}

func (h *azHarness) NewUploader(ctx context.Context, t *testing.T, key string, opts *multipart.Options) (multipart.Uploader, error) {
	return azmp.NewUploader(ctx, h.blobClient(key), h.loc, key, opts)
}

func (h *azHarness) Open(ctx context.Context, t *testing.T, key, uploadID string) (multipart.Uploader, error) {
	return azmp.Open(ctx, h.blobClient(key), h.loc, key, uploadID, nil)
}

func (h *azHarness) Bucket(ctx context.Context, t *testing.T) (*blob.Bucket, error) {
	b, err := azureblob.OpenBucket(ctx, h.cc, nil)
	if err != nil {
		return nil, err
	}
	pb := blob.PrefixedBucket(b, h.prefix)
	h.buckets = append(h.buckets, pb)
	return pb, nil
}

func (h *azHarness) Close() {
	for _, b := range h.buckets {
		_ = b.Close()
	}
}
