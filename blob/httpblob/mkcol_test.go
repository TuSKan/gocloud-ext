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

package httpblob

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestMkcolAcceptsAlreadyExists covers the responses a server gives when the
// collection is already there.
//
// RFC 4918 §9.3.1 specifies 405, and most servers send it. Apache mod_dav
// sends 301 instead, redirecting to the trailing-slash form of the URL.
// httpblob does not follow redirects on a mutating method, because Go rewrites
// a redirected non-GET into a GET, so the 301 arrives as an error and has to be
// recognised here.
//
// This is a regression test: against Apache, every write into a collection made
// by a different bucket handle failed, because a fresh handle starts with an
// empty created-collection cache and so always issues the MKCOL.
func TestMkcolAcceptsAlreadyExists(t *testing.T) {
	for _, test := range []struct {
		name    string
		status  int
		wantErr bool
	}{
		{"created", http.StatusCreated, false},
		{"405 per RFC 4918", http.StatusMethodNotAllowed, false},
		{"301 as Apache mod_dav sends", http.StatusMovedPermanently, false},
		{"302", http.StatusFound, false},
		{"308", http.StatusPermanentRedirect, false},

		// Anything genuinely wrong must still surface.
		{"403", http.StatusForbidden, true},
		{"409 missing parent", http.StatusConflict, true},
		{"500", http.StatusInternalServerError, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var gotMethod string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod = r.Method
				// A redirect status without a Location header is what makes
				// this a useful test: if httpblob ever started following the
				// redirect, it would rewrite the method and land somewhere
				// unintended rather than treating it as "already exists".
				w.WriteHeader(test.status)
			}))
			defer srv.Close()

			ctx := context.Background()
			drv, err := openBucket(ctx, srv.Client(), srv.URL, &Options{Protocol: ProtocolWebDAV})
			if err != nil {
				t.Fatal(err)
			}
			b := drv.(*bucket)

			err = b.mkcol(ctx, srv.URL+"/some/collection")
			if test.wantErr && err == nil {
				t.Errorf("mkcol against %d returned nil, want an error", test.status)
			}
			if !test.wantErr && err != nil {
				t.Errorf("mkcol against %d returned %v, want nil", test.status, err)
			}
			if gotMethod != methodMkcol {
				t.Errorf("server saw method %q, want %q", gotMethod, methodMkcol)
			}
		})
	}
}
