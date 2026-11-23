// Copyright 2026 Matrix Origin
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

package aggexec

import "unicode/utf8"

// jsonValidUTF8Len returns the length after replacing each invalid UTF-8 byte
// with U+FFFD, following the existing JSON text representation.
func jsonValidUTF8Len(value []byte) int {
	if utf8.Valid(value) {
		return len(value)
	}
	n := 0
	for i := 0; i < len(value); {
		r, size := utf8.DecodeRune(value[i:])
		if r == utf8.RuneError && size == 1 {
			n += utf8.RuneLen(utf8.RuneError)
		} else {
			n += size
		}
		i += size
	}
	return n
}

// putJSONValidUTF8 writes normalized text into a correctly sized destination.
func putJSONValidUTF8(dst, value []byte) int {
	if utf8.Valid(value) {
		return copy(dst, value)
	}
	n := 0
	for i := 0; i < len(value); {
		r, size := utf8.DecodeRune(value[i:])
		if r == utf8.RuneError && size == 1 {
			n += utf8.EncodeRune(dst[n:], utf8.RuneError)
		} else {
			n += copy(dst[n:], value[i:i+size])
		}
		i += size
	}
	return n
}
