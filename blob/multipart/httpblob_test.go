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

	// A collection per run, so leftovers from an earlier run cannot be
	// mistaken for this one's.
	suffix, err := randomSuffix()
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = fmt.Sprintf("%s/mp-%s", parsed.Path, suffix)
	bucketURL := parsed.String()

	ctx := context.Background()
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
