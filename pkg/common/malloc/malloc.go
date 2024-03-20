// Copyright 2022 Matrix Origin
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

package malloc

// #include <stdlib.h>
import "C"
import "unsafe"

//go:linkname throw runtime.throw
func throw(s string)

// alloc allocates a byte slice of size use cgo
func Alloc(n int) []byte {
	if n == 0 {
		return make([]byte, 0)
	}
	ptr := C.calloc(C.size_t(n), 1)
	if ptr == nil {
		// NB: throw is like panic, except it guarantees the process will be
		// terminated. The call below is exactly what the Go runtime invokes when
		// it cannot allocate memory.
		throw("out of memory")
	}
	return unsafe.Slice((*byte)(ptr), n)
}

func Free(b []byte) {
	if cap(b) != 0 {
		b = b[:cap(b)]
		C.free(unsafe.Pointer(&b[0]))
	}
}
