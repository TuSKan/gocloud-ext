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

// This file is not part of the upstream copy in escape.go; it holds the one
// key-escaping rule set shared by every driver in this repository.

package escape

// KeyEscape escapes a blob key for storage on a backend whose namespace
// behaves like a filesystem.
//
// The rule set is deliberately identical to the one gocloud.dev's fileblob and
// sftpblob use, so that a bucket written by any of them is readable by the
// others: ASCII control characters, the "/" in "../", the trailing "/" in
// "//", a trailing "/", and the characters \<>:"|?* are all escaped to
// "__0x<hex>__".
//
// Sharing one implementation across the drivers here means they cannot drift
// apart from each other; key_test.go pins the output so they cannot silently
// drift from upstream either.
func KeyEscape(s string) string {
	return HexEscape(s, func(r []rune, i int) bool {
		c := r[i]
		switch {
		case c < 32:
			return true
		case c == '\\':
			return true
		case i > 1 && c == '/' && r[i-1] == '.' && r[i-2] == '.':
			return true
		case i > 0 && c == '/' && r[i-1] == '/':
			return true
		case c == '/' && i == len(r)-1:
			return true
		case c == '>' || c == '<' || c == ':' || c == '"' || c == '|' || c == '?' || c == '*':
			return true
		}
		return false
	})
}

// KeyUnescape reverses KeyEscape.
func KeyUnescape(s string) string {
	return HexUnescape(s)
}
