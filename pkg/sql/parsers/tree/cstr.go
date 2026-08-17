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

package tree

import (
	"strings"
	"unicode/utf8"
)

type CStrParts [4]*CStr
type CStr struct {
	// user origin input
	o string
	// use for compare
	c string
	// quote bool
}

func NewCStr(str string, lower int64) *CStr {
	cs := &CStr{o: str}
	if lower == 0 {
		cs.c = cs.o
		return cs
	}
	cs.c = lowerIdentifier(cs.o)
	return cs
}

// lowerIdentifier keeps bytes from single-byte client encodings intact. Go's
// Unicode lowercasing replaces malformed UTF-8 with utf8.RuneError, which can
// silently change a catalog key. Valid UTF-8 retains the existing Unicode
// case-folding behavior; malformed input receives ASCII-only folding.
func lowerIdentifier(value string) string {
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

func (cs *CStr) Origin() string {
	return cs.o
}

func (cs *CStr) Compare() string {
	return cs.c
}

func (cs *CStr) Empty() bool {
	return len(cs.o) == 0
}
