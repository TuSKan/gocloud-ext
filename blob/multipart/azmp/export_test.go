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

package azmp

// Block IDs are an implementation detail — a caller never constructs one — but
// their fixed width is a correctness requirement Azure enforces, and the
// round trip is how ListParts recovers a part number. Exporting them to the
// test rather than to users keeps both testable without widening the API.

// BlockIDForTest exposes blockID.
func BlockIDForTest(number int64) string { return blockID(number) }

// PartNumberForTest exposes partNumber.
func PartNumberForTest(id string) (int64, error) { return partNumber(id) }
