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

package escape

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the golden file instead of comparing against it")

const goldenPath = "testdata/keys.golden"

// TestKeyEscapeGolden pins the exact output of KeyEscape.
//
// escape.go is a vendored copy of gocloud.dev/internal/escape, and its
// escaping is not frozen upstream: commit 03617260 changed HexEscape's
// semantics after v0.46.0. The drivers here promise their keys and .attrs
// sidecars interoperate with fileblob and sftpblob, and a silent change to
// this output breaks that promise in a way no other test would notice — old
// keys simply stop resolving.
//
// Re-sync with `go test ./internal/escape -update` and read the diff. Every
// changed line is a key that existing buckets can no longer address.
func TestKeyEscapeGolden(t *testing.T) {
	names := make([]string, 0, len(WeirdStrings)+len(extraKeyCases))
	inputs := map[string]string{}
	for name, s := range WeirdStrings {
		names = append(names, name)
		inputs[name] = s
	}
	for name, s := range extraKeyCases {
		if _, dup := inputs[name]; dup {
			t.Fatalf("case %q is defined twice", name)
		}
		names = append(names, name)
		inputs[name] = s
	}
	sort.Strings(names)

	var got strings.Builder
	got.WriteString("# Golden output of KeyEscape. Regenerate with:\n")
	got.WriteString("#   go test ./internal/escape -update\n")
	got.WriteString("# Format: <case name> <TAB> <quoted input> <TAB> <quoted escaped>\n")
	for _, name := range names {
		fmt.Fprintf(&got, "%s\t%q\t%q\n", name, inputs[name], KeyEscape(inputs[name]))
	}

	if *update {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o777); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, []byte(got.String()), 0o666); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", goldenPath)
		return
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("%v; generate it with: go test ./internal/escape -update", err)
	}
	if got.String() != string(want) {
		t.Errorf("KeyEscape output no longer matches %s.\n"+
			"If this followed a re-sync of escape.go from upstream, every changed line is a key\n"+
			"that existing buckets can no longer address. Review carefully, then regenerate with:\n"+
			"  go test ./internal/escape -update\n\ngot:\n%s\nwant:\n%s",
			goldenPath, got.String(), want)
	}
}

// extraKeyCases covers the rules KeyEscape adds on top of the shared
// WeirdStrings corpus, which does not exercise all of them on its own.
var extraKeyCases = map[string]string{
	"trailing-slash":        "dir/",
	"double-slash-mid":      "a//b",
	"dotdot-slash":          "../etc/passwd",
	"dotdot-slash-mid":      "a/../b",
	"windows-reserved":      `a<b>c:d"e|f?g*h`,
	"backslash":             `a\b`,
	"control-chars":         "a\x00b\x1fc",
	"del-is-not-escaped":    "a\x7fb",
	"already-escaped":       "__0x2f__",
	"escaped-looking-plain": "__0xzz__",
	"plain":                 "dir/sub/file.txt",
	"empty":                 "",
}

// TestKeyRoundTrip checks that every case survives escape then unescape. This
// is the property that actually matters; the golden file only pins how it is
// spelled.
func TestKeyRoundTrip(t *testing.T) {
	for name, s := range WeirdStrings {
		if got := KeyUnescape(KeyEscape(s)); got != s {
			t.Errorf("%s: round trip gave %q, want %q", name, got, s)
		}
	}
	for name, s := range extraKeyCases {
		if got := KeyUnescape(KeyEscape(s)); got != s {
			t.Errorf("%s: round trip gave %q, want %q", name, got, s)
		}
	}
}

// TestKeyEscapeIsInjective is the property commit 03617260 added upstream: two
// different keys must never escape to the same string, or one caller's key can
// be made to collide with another's.
func TestKeyEscapeIsInjective(t *testing.T) {
	seen := map[string]string{}
	for _, s := range WeirdStrings {
		check(t, seen, s)
	}
	for _, s := range extraKeyCases {
		check(t, seen, s)
	}
	// The specific collision that motivated the upstream fix: a literal
	// "__0x2f__" versus a "/" that escapes into that same text.
	check(t, seen, "__0x2f__")
	check(t, seen, "/")
}

func check(t *testing.T, seen map[string]string, s string) {
	t.Helper()
	escaped := KeyEscape(s)
	if prev, ok := seen[escaped]; ok && prev != s {
		t.Errorf("keys %q and %q both escape to %q", prev, s, escaped)
	}
	seen[escaped] = s
}
