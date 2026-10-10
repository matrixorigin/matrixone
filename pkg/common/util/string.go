// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"strings"
	"unicode/utf8"
)

// Abbreviate truncates diagnostic text and replaces each malformed UTF-8 byte
// in the retained prefix with '?'. Valid text and byte positions are preserved.
// Parameters:
//   - str: the input string
//   - length: the maximum length to truncate to
//     -1: return the complete string
//     0: return empty string
//     >0: return a complete UTF-8 prefix of at most 'length' bytes, appending "..." if truncated
func Abbreviate(str string, length int) string {
	if length == 0 || length < -1 {
		return ""
	}

	truncated := length > 0 && len(str) > length
	if truncated {
		str = str[:UTF8PrefixLen(str, length)]
	}
	if utf8.ValidString(str) {
		if truncated {
			return str + "..."
		}
		return str
	}
	var repaired strings.Builder
	repaired.Grow(len(str) + 3)
	start := 0
	for i, r := range str {
		if r != utf8.RuneError {
			continue
		}
		if _, width := utf8.DecodeRuneInString(str[i:]); width != 1 {
			continue
		}
		repaired.WriteString(str[start:i])
		repaired.WriteByte('?')
		start = i + 1
	}
	repaired.WriteString(str[start:])
	if truncated {
		repaired.WriteString("...")
	}
	return repaired.String()
}

// UTF8PrefixLen returns the longest complete UTF-8 prefix within a byte budget.
// It inspects only the cut boundary; malformed input is not repaired.
func UTF8PrefixLen(s string, budget int) int {
	if budget <= 0 {
		return 0
	}
	if budget >= len(s) {
		return len(s)
	}
	if utf8.RuneStart(s[budget]) {
		return budget
	}
	start := budget
	for start > 0 && budget-start < utf8.UTFMax-1 && !utf8.RuneStart(s[start]) {
		start--
	}
	_, width := utf8.DecodeRuneInString(s[start:])
	if width > 1 && start+width > budget {
		return start
	}
	return budget
}
