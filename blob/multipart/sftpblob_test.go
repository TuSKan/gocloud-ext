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
	"fmt"
	"os"
	"testing"

	"github.com/TuSKan/gocloud-ext/blob/multipart/mptest"
	"gocloud.dev/blob"

	_ "github.com/TuSKan/gocloud-ext/blob/sftpblob"
)

// TestConformanceSFTP runs the suite over a real SSH server.
//
// sftpblob needs no multipart code of its own. The generic uploader stages
// parts as ordinary objects and assembles them through blob.Bucket, so any
// driver that can write, read, list and delete supports multipart. This test
// settles that against OpenSSH rather than an in-process fake.
//
// CI sets the environment; see .github/workflows/ci.yml.
func TestConformanceSFTP(t *testing.T) {
	base := os.Getenv("SFTPBLOB_TEST_URL")
	if base == "" {
		t.Skip("set SFTPBLOB_TEST_URL to run multipart conformance against a real SSH server")
	}

	// A directory per run, so leftovers from an earlier run cannot be mistaken
	// for this one's.
	suffix, err := randomSuffix()
	if err != nil {
		t.Fatal(err)
	}
	bucketURL := fmt.Sprintf("%s/mp-%s?create_dir=true", base, suffix)
	if keyPath := os.Getenv("SFTPBLOB_TEST_KEY_PATH"); keyPath != "" {
		bucketURL += "&private_key_path=" + keyPath
	}
	if knownHosts := os.Getenv("SFTPBLOB_TEST_KNOWN_HOSTS"); knownHosts != "" {
		bucketURL += "&known_hosts_path=" + knownHosts
	}

	ctx := context.Background()
	mptest.RunConformanceTests(t, mptest.BucketHarness(mptest.BucketOpener{
		Open: func() (*blob.Bucket, error) { return blob.OpenBucket(ctx, bucketURL) },
		// Each OpenBucket dials its own SSH connection to the same remote
		// directory, so resuming genuinely crosses a connection boundary.
		SeparateHandles: true,
	}))
}
