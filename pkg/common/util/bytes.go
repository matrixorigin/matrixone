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

// CloneBytesIf conditionally copies a byte slice.
// If clone is true, it returns a new copy of the slice.
// If clone is false, it returns the original slice without copying.
// If the source slice is empty, it returns an empty slice.
func CloneBytesIf(src []byte, clone bool) []byte {
	if clone {
		if len(src) > 0 {
			dst := make([]byte, len(src))
			copy(dst, src)
			return dst
		}
		return []byte{}
	}
	return src
}

// new copy of the slice
func CloneBytes(src []byte) []byte {
	var ret []byte
	if len(src) > 0 {
		ret = make([]byte, len(src))
		copy(ret, src)
	}
	return ret
}
