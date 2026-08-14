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

package gcsmp_test

import (
	"testing"

	"github.com/TuSKan/gocloud-ext/blob/multipart/gcsmp"
)

// TestEscapeKeyGolden pins the escaping to what gcsblob produces.
//
// Every case was run against gocloud.dev/blob/gcsblob's unexported escapeKey
// and produced identical output. Escaping drift is silent — the upload
// succeeds and the object is simply not where blob.Bucket looks.
//
// Note how much less GCS escapes than S3 or Azure: only the two line-ending
// characters and the slash of "../". A control character other than \n or \r
// passes straight through, which is correct here and would be wrong for the
// other two.
func TestEscapeKeyGolden(t *testing.T) {
	for _, test := range []struct {
		key  string
		want string
	}{
		{"foo.txt", "foo.txt"},
		{"nested/dir/object.bin", "nested/dir/object.bin"},
		{"with space.txt", "with space.txt"},
		{"unicode-ü-ñ.txt", "unicode-ü-ñ.txt"},
		{"", ""},

		// Only \n and \r are escaped.
		{"a\nb", "a__0xa__b"},
		{"a\rb", "a__0xd__b"},

		// Other control characters are left alone, unlike S3 and Azure.
		{"a\x00b", "a\x00b"},
		{"a\x1fb", "a\x1fb"},

		// The slash of "../".
		{"a/../b", "a/..__0x2f__b"},
		{"..", ".."},
		{"..x", "..x"},

		// A trailing slash is not escaped, unlike Azure.
		{"dir/", "dir/"},
	} {
		if got := gcsmp.EscapeKey(test.key); got != test.want {
			t.Errorf("EscapeKey(%q) = %q, want %q", test.key, got, test.want)
		}
	}
}
