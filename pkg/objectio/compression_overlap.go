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

package objectio

import (
	"runtime"
	"unsafe"
)

// compressionBuffersOverlap checks byte ranges without dereferencing or
// reconstructing pointers. Both slices and their owners must remain valid for
// the comparison; callers must not free or mutate the arena concurrently.
func compressionBuffersOverlap(left, right []byte) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	l := uintptr(unsafe.Pointer(unsafe.SliceData(left)))
	r := uintptr(unsafe.Pointer(unsafe.SliceData(right)))
	var overlap bool
	if l <= r {
		overlap = r-l < uintptr(len(left))
	} else {
		overlap = l-r < uintptr(len(right))
	}
	runtime.KeepAlive(left)
	runtime.KeepAlive(right)
	return overlap
}
