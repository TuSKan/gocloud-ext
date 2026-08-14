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
	"testing"

	"github.com/TuSKan/gocloud-ext/blob/multipart/azmp"
)

// TestEscapeKeyGolden pins the escaping to what azureblob produces.
//
// Every case here was run against gocloud.dev/blob/azureblob's unexported
// escapeKey and produced identical output. Escaping drift is silent — the
// upload succeeds and the blob is simply not where blob.Bucket looks — so this
// is the most valuable test in the package.
func TestEscapeKeyGolden(t *testing.T) {
	for _, test := range []struct {
		key  string
		want string
	}{
		{"foo.txt", "foo.txt"},
		{"nested/dir/object.bin", "nested/dir/object.bin"},
		{"with space.txt", "with space.txt"},
		{"", ""},

		// Characters Azure cannot address.
		{"a" + string(rune(92)) + "b", "a__0x5c__b"}, // backslash
		{"a\"b", "a__0x22__b"},
		{"a#b", "a__0x23__b"},
		{"a%b", "a__0x25__b"},
		{"a?b", "a__0x3f__b"},
		{"a\x7fb", "a__0x7f__b"},
		{"a\x00b", "a__0x0__b"},

		// A trailing slash, and the slash of "../".
		{"dir/", "dir__0x2f__"},
		{"a/../b", "a/..__0x2f__b"},
		{"..", ".."},
	} {
		if got := azmp.EscapeKey(test.key); got != test.want {
			t.Errorf("EscapeKey(%q) = %q, want %q", test.key, got, test.want)
		}
	}
}

// TestBlockIDsAreFixedWidth covers Azure's requirement that every block ID in
// one blob decode to the same length. A variable-width ID is accepted for the
// first block and rejected for a later one, which would surface as a confusing
// mid-upload failure rather than as a bad ID.
func TestBlockIDsAreFixedWidth(t *testing.T) {
	first := azmp.BlockIDForTest(1)
	last := azmp.BlockIDForTest(10000)
	if len(first) != len(last) {
		t.Errorf("block IDs differ in length: %q (%d) vs %q (%d)", first, len(first), last, len(last))
	}
	if first == last {
		t.Error("block IDs for different part numbers are identical")
	}
}

// TestBlockIDRoundTrip checks that a part number survives the trip through a
// block ID, which is how ListParts recovers it from Azure.
func TestBlockIDRoundTrip(t *testing.T) {
	for _, number := range []int64{1, 2, 9, 10, 99, 100, 5000, 9999, 10000} {
		id := azmp.BlockIDForTest(number)
		got, err := azmp.PartNumberForTest(id)
		if err != nil {
			t.Errorf("PartNumber(%q) for part %d: %v", id, number, err)
			continue
		}
		if got != number {
			t.Errorf("part %d round-tripped to %d via %q", number, got, id)
		}
	}
}
