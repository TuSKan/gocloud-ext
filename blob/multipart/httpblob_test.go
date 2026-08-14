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

package multipart_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"

	"github.com/TuSKan/gocloud-ext/blob/multipart/mptest"
	"gocloud.dev/blob"

	_ "github.com/TuSKan/gocloud-ext/blob/httpblob"
)

// TestConformanceWebDAV runs the suite over a real WebDAV server.
//
// httpblob needs no multipart code of its own; the generic uploader works
// entirely through blob.Bucket. WebDAV is the most demanding backend for that
// approach — staging parts creates collections the server has to materialise,
// and Commit reads every part back over HTTP — so it is where the generic
// design is likeliest to come apart. That it *should* work and that it *does*
// work are different claims.
//
// Only the WebDAV protocol is exercised: a plain http:// bucket is read-only
// and cannot support multipart at all.
//
// CI sets the environment; see .github/workflows/ci.yml.
func TestConformanceWebDAV(t *testing.T) {
	base := os.Getenv("HTTPBLOB_TEST_WEBDAV_URL")
	if base == "" {
		t.Skip("set HTTPBLOB_TEST_WEBDAV_URL to run multipart conformance against a real WebDAV server")
	}

	parsed, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parsing HTTPBLOB_TEST_WEBDAV_URL: %v", err)
	}
	// The variable names an http:// endpoint, but multipart needs the
	// read-write WebDAV protocol, which httpblob selects by scheme.
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "webdav"
	case "https":
		parsed.Scheme = "webdavs"
	}
	if user := os.Getenv("HTTPBLOB_TEST_WEBDAV_USER"); user != "" {
		parsed.User = url.UserPassword(user, os.Getenv("HTTPBLOB_TEST_WEBDAV_PASSWORD"))
	}

	ctx := context.Background()

	// A collection per run, so leftovers from an earlier run cannot be
	// mistaken for this one's.
	suffix, err := randomSuffix()
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = fmt.Sprintf("%s/mp-%s", parsed.Path, suffix)
	bucketURL := parsed.String()

	// The bucket root has to exist before anything is written into it.
	// httpblob creates collections for a key's own path components, but it
	// treats the bucket's base URL as given — reasonably, since creating it
	// would mean writing outside the bucket. WebDAV MKCOL also refuses when
	// the parent is missing, so without this every write fails with a 409 that
	// looks like a driver bug rather than a missing directory.
	if err := mkcol(ctx, base, parsed.Path, os.Getenv("HTTPBLOB_TEST_WEBDAV_USER"),
		os.Getenv("HTTPBLOB_TEST_WEBDAV_PASSWORD")); err != nil {
		t.Fatalf("creating the bucket collection: %v", err)
	}

	mptest.RunConformanceTests(t, mptest.BucketHarness(mptest.BucketOpener{
		Open: func() (*blob.Bucket, error) { return blob.OpenBucket(ctx, bucketURL) },
		// Each OpenBucket is an independent client against the same server, so
		// resuming really does cross a client boundary.
		SeparateHandles: true,
	}))
}

func randomSuffix() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

// mkcol creates one collection on a WebDAV server. The bucket's base
// collection has to exist before httpblob writes into it, and creating it is
// the caller's job rather than the driver's.
func mkcol(ctx context.Context, base, path, user, pass string) error {
	target, err := url.Parse(base)
	if err != nil {
		return err
	}
	target.Path = path
	req, err := http.NewRequestWithContext(ctx, "MKCOL", target.String(), nil)
	if err != nil {
		return err
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusOK:
		return nil
	case http.StatusMethodNotAllowed:
		// Already there, which is what was wanted.
		return nil
	default:
		return fmt.Errorf("MKCOL %s: %s", target, resp.Status)
	}
}
