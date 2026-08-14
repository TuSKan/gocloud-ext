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
	"testing"

	"github.com/TuSKan/gocloud-ext/blob/multipart"
	"github.com/TuSKan/gocloud-ext/blob/multipart/s3mp"
)

// TestEscapeKeyGolden pins the escaping to the exact output s3blob produces.
//
// This is the highest-value test in the package. Getting escaping wrong is
// silent: the upload succeeds, and the object simply is not where
// blob.Bucket looks for it. These values were taken from
// gocloud.dev/blob/s3blob's own escaping rules; if upstream changes them, the
// daily gocloud.dev@master CI job plus this test are what surface it.
func TestEscapeKeyGolden(t *testing.T) {
	for _, test := range []struct {
		key  string
		want string
	}{
		// Ordinary keys pass through untouched.
		{"foo.txt", "foo.txt"},
		{"nested/dir/object.bin", "nested/dir/object.bin"},
		{"with space.txt", "with space.txt"},
		{"unicode-ü-ñ.txt", "unicode-ü-ñ.txt"},
		{"", ""},

		// Control characters are escaped; S3 does not handle them.
		{"a\x00b", "a__0x0__b"},
		{"a\nb", "a__0xa__b"},
		{"a\x1fb", "a__0x1f__b"},

		// Only the slash of "../" is escaped, and only when preceded by two
		// dots. A lone ".." or a "..x" is left alone.
		{"a/../b", "a/..__0x2f__b"},
		{"..", ".."},
		{"..x", "..x"},
		{"a/..b", "a/..b"},
	} {
		if got := s3mp.EscapeKey(test.key); got != test.want {
			t.Errorf("EscapeKey(%q) = %q, want %q", test.key, got, test.want)
		}
	}
}

// TestObjectNameAppliesPrefixThenEscapes checks the order of operations.
//
// s3blob escapes the key it is handed *after* a prefixed bucket has prepended
// to it, so escaping the key first and then joining would produce a different
// name for any prefix ending in a way that interacts with the key.
func TestObjectNameAppliesPrefixThenEscapes(t *testing.T) {
	loc := multipart.Location{Bucket: "b", Prefix: "a/.."}
	// Joined first this is "a/../key", whose "../" slash is escaped. Escaping
	// the parts separately would leave the slash alone.
	const key = "/key"
	got := multipart.ObjectName(loc, key, s3mp.EscapeKey)
	want := s3mp.EscapeKey("a/../key")
	if got != want {
		t.Errorf("ObjectName = %q, want %q (prefix must be joined before escaping)", got, want)
	}
	if got == s3mp.EscapeKey(loc.Prefix)+s3mp.EscapeKey(key) {
		t.Error("ObjectName escaped the prefix and key separately, which does not match s3blob")
	}
}

// TestNewUploaderRequiresBucket makes the Location requirement an explicit
// failure rather than a request against an empty bucket name.
func TestNewUploaderRequiresBucket(t *testing.T) {
	if _, err := s3mp.NewUploader(t.Context(), stubAPI{}, multipart.Location{}, "key", nil); err == nil {
		t.Error("NewUploader with no bucket succeeded, want an error")
	}
}

func TestNewUploaderRejectsEmptyKey(t *testing.T) {
	if _, err := s3mp.NewUploader(t.Context(), stubAPI{}, multipart.Location{Bucket: "b"}, "", nil); err == nil {
		t.Error("NewUploader with an empty key succeeded, want an error")
	}
}

// stubAPI satisfies s3mp.API without reaching the network. The validation
// above rejects its input before any call is made.
type stubAPI struct{ s3mp.API }
