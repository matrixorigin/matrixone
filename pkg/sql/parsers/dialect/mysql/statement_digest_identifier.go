// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"sort"
	"strings"
	"unicode/utf8"
)

// Native function lookup skips whitespace, but never comments, in IGNORE_SPACE.
func (s *Scanner) digestFunctionCallAhead() bool {
	position := s.Pos
	if s.sqlMode.Has(SQLModeIgnoreSpace) {
		for position < len(s.buf) && strings.IndexByte(" \t\r\n\v\f", s.buf[position]) >= 0 {
			position++
		}
	}
	return position < len(s.buf) && s.buf[position] == '('
}

// digestIdentifierKey uses MySQL's identifier case table, not Go's evolving
// Unicode lowercase mapping. For example, capital sharp S is distinct from ß.
// The same key owns declaration uniqueness and reference-time CTE lookup.
func digestIdentifierKey(name string) string {
	var result strings.Builder
	changed := false
	for position := 0; position < len(name); {
		letter, size := utf8.DecodeRuneInString(name[position:])
		lower := letter
		if letter >= 'A' && letter <= 'Z' {
			lower += 'a' - 'A'
		} else if letter >= utf8.RuneSelf {
			index := sort.Search(len(digestIdentifierLower), func(i int) bool { return digestIdentifierLower[i][0] >= letter })
			if index < len(digestIdentifierLower) && digestIdentifierLower[index][0] == letter {
				lower = digestIdentifierLower[index][1]
			}
		}
		if lower != letter && !changed {
			result.Grow(len(name))
			result.WriteString(name[:position])
			changed = true
		}
		if changed {
			if lower == letter {
				// Preserve original client bytes, including invalid UTF-8.
				result.WriteString(name[position : position+size])
			} else {
				result.WriteRune(lower)
			}
		}
		position += size
	}
	if changed {
		return result.String()
	}
	return name
}

// SQL keywords are ASCII byte names. Unicode casing must not turn an identifier
// such as dotless-i + f into the IF keyword.
func digestKeywordUpper(word string) string {
	for i := range len(word) {
		if word[i] >= utf8.RuneSelf {
			return ""
		}
	}
	return strings.ToUpper(word)
}
