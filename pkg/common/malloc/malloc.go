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

import (
	"syscall"
	"unsafe"
)

var fd = -1

//go:linkname throw runtime.throw
func throw(s string)

// New allocates a slice of size n. The returned slice is from manually managed
// memory and MUST be released by calling Free. Failure to do so will result in
// a memory leak.
func Alloc(n int) []byte {
	if n == 0 {
		return make([]byte, 0)
	}
	size := rollup(n)
	r0, _, e1 := syscall.Syscall6(syscall.SYS_MMAP, 0, uintptr(size), uintptr(syscall.PROT_READ|syscall.PROT_WRITE),
		uintptr(syscall.MAP_ANON|syscall.MAP_PRIVATE), uintptr(fd), uintptr(0))
	if e1 != 0 {
		throw("out of memory")
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(r0)), n)
}

func Free(b []byte) {
	size := int64(rollup(cap(b)))
	syscall.Syscall(syscall.SYS_MUNMAP, uintptr(unsafe.Pointer(&b[0])), uintptr(size), 0)
}

func rollup(n int) int {
	return (n + 4095) & (^4095)
}
