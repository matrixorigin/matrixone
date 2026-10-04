// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggexec

import "bytes"

// compareUTF8mb4Bin implements the PAD SPACE contract shared by MySQL's
// legacy utf8_bin and utf8mb4_bin collations. Their sort order is bytewise for
// valid UTF-8, but trailing U+0020 characters are insignificant. Keep the
// binary character set and legacy catalog metadata on the raw-byte comparator.
func compareUTF8mb4Bin(a, b []byte) int {
	a = trimTrailingU0020(a)
	b = trimTrailingU0020(b)
	return bytes.Compare(a, b)
}

func trimTrailingU0020(value []byte) []byte {
	for len(value) > 0 && value[len(value)-1] == ' ' {
		value = value[:len(value)-1]
	}
	return value
}
