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

package identifier

import (
	"strings"
	"unicode/utf8"
)

// Fold returns the comparison identity of a database or table identifier.
// Invalid UTF-8 is kept byte-for-byte except for ASCII capitals, so a client
// encoding cannot silently be replaced with the Unicode replacement rune.
func Fold(value string) string {
	if utf8.ValidString(value) {
		return strings.ToLower(value)
	}
	for i := 0; i < len(value); i++ {
		if value[i] >= 'A' && value[i] <= 'Z' {
			lower := []byte(value)
			for j := i; j < len(lower); j++ {
				if lower[j] >= 'A' && lower[j] <= 'Z' {
					lower[j] += 'a' - 'A'
				}
			}
			return string(lower)
		}
	}
	return value
}
