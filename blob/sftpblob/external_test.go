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

package sftpblob

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"

	"github.com/pkg/sftp"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/blob/drivertest"
)

// Environment variables that point the conformance suite at a real SSH server
// instead of the in-process one. CI sets them; see .github/workflows/ci.yml.
const (
	envSFTPURL     = "SFTPBLOB_TEST_URL"
	envSFTPKeyPath = "SFTPBLOB_TEST_KEY_PATH"
	envSFTPKnown   = "SFTPBLOB_TEST_KNOWN_HOSTS"
)

// externalHarness runs the conformance suite against an SSH server this
// process did not start, in a directory of its own.
type externalHarness struct {
	// owner holds the connection; closing it tears everything down.
	owner  *blob.Bucket
	client *sftp.Client
	dir    string
}

func newExternalHarness(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
	t.Helper()
	base := os.Getenv(envSFTPURL)
	if base == "" {
		t.Skipf("set %s to run conformance against a real SSH server", envSFTPURL)
	}

	// Each harness gets its own directory, so concurrent runs and leftovers
	// from a previous run cannot interfere.
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return nil, err
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("parsing %s=%q: %w", envSFTPURL, base, err)
	}
	u.Path = u.Path + "/sftpblob-test-" + hex.EncodeToString(buf[:])

	q := u.Query()
	// create_dir makes the per-run directory; the driver does not create the
	// bucket root on its own.
	q.Set("create_dir", "true")
	if keyPath := os.Getenv(envSFTPKeyPath); keyPath != "" {
		q.Set("private_key_path", keyPath)
	}
	if known := os.Getenv(envSFTPKnown); known != "" {
		q.Set("known_hosts_path", known)
	}
	u.RawQuery = q.Encode()

	opener := &URLOpener{}
	b, err := opener.OpenBucketURL(ctx, u)
	if err != nil {
		return nil, fmt.Errorf("opening %v: %w", u, err)
	}
	// drivertest wants a fresh driver.Bucket per call, all seeing the same
	// storage. Reach the connection the opener dialed and build drivers over
	// it; each is closed independently, which only works because Close no
	// longer closes a client it did not create.
	var client *sftp.Client
	if !b.As(&client) {
		b.Close()
		return nil, fmt.Errorf("sftpblob: Bucket.As failed for **sftp.Client")
	}
	return &externalHarness{owner: b, client: client, dir: u.Path}, nil
}

func (h *externalHarness) MakeDriver(ctx context.Context) (driver.Bucket, error) {
	return openBucket(h.client, h.dir, nil)
}

func (h *externalHarness) MakeDriverForNonexistentBucket(ctx context.Context) (driver.Bucket, error) {
	return nil, nil
}

func (h *externalHarness) HTTPClient() *http.Client { return nil }

func (h *externalHarness) Close() { _ = h.owner.Close() }

// TestConformanceExternal is the interop test. pkg/sftp's own server is one
// implementation, and passing against it says little about OpenSSH, which is
// what almost every real deployment runs and which differs in rename
// semantics, extension support and error codes.
func TestConformanceExternal(t *testing.T) {
	drivertest.RunConformanceTests(t, newExternalHarness, nil)
}
