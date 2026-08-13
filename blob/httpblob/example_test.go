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

package httpblob_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"

	"golang.org/x/net/webdav"

	"github.com/TuSKan/gocloud-ext/blob/httpblob"
	"gocloud.dev/blob"
)

func ExampleOpenBucket() {
	ctx := context.Background()

	// A WebDAV bucket is read-write. Blob keys are appended to the base URL.
	bucket, err := httpblob.OpenBucket(ctx, http.DefaultClient, "https://example.com/dav/my-bucket",
		&httpblob.Options{
			Protocol:      httpblob.ProtocolWebDAV,
			BasicAuthUser: "user",
			// In real code, read the password from your secret store.
			BasicAuthPassword: os.Getenv("WEBDAV_PASSWORD"),
		})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = bucket.Close() }()
}

func ExampleOpenBucket_readOnly() {
	ctx := context.Background()

	// The default protocol is plain HTTP, which works against any web server
	// but is read-only: reads and Attributes are supported, and writes, deletes
	// and listing return an Unimplemented error.
	bucket, err := httpblob.OpenBucket(ctx, http.DefaultClient, "https://example.com/files", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = bucket.Close() }()
}

func Example_openBucketFromURL() {

	// Run a WebDAV server to talk to. In a real program this would be a
	// remote server, and the URL below would be its address.
	dir, err := os.MkdirTemp("", "go-cloud-httpblob-example")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	// Like the directory you pass to fileblob, the bucket's base URL must
	// already exist; httpblob does not create it.
	if err := os.Mkdir(filepath.Join(dir, "my-bucket"), 0o777); err != nil {
		log.Fatal(err)
	}
	srv := httptest.NewServer(&webdav.Handler{
		FileSystem: webdav.Dir(dir),
		LockSystem: webdav.NewMemLS(),
	})
	defer srv.Close()

	// blob.OpenBucket creates a *blob.Bucket from a URL. The "webdav" scheme
	// connects over http; use "webdavs" for https. The "http" and "https"
	// schemes open a read-only bucket against an ordinary web server.
	ctx := context.Background()
	b, err := blob.OpenBucket(ctx, "webdav://"+srv.Listener.Addr().String()+"/my-bucket")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	// Now we can use b to read or write blobs.
	if err := b.WriteAll(ctx, "my-key", []byte("hello world"), nil); err != nil {
		log.Fatal(err)
	}
	data, err := b.ReadAll(ctx, "my-key")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(data))

	// Output:
	// hello world
}
