// Copyright 2024 Matrix Origin
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
	"unsafe"

	"golang.org/x/sys/unix"
)

func reuseMem(ptr unsafe.Pointer, size int) {
	// no need to clear, since re-visiting a MADV_DONTNEED-advised page will zero it

	slice := unsafe.Slice((*byte)(ptr), size)
	if err := unix.Mprotect(slice, unix.PROT_READ|unix.PROT_WRITE); err != nil {
		panic(err)
	}
}

func freeMem(ptr unsafe.Pointer, size int) {
	slice := unsafe.Slice((*byte)(ptr), size)
	if err := unix.Madvise(slice, unix.MADV_DONTNEED); err != nil {
		panic(err)
	}
	if err := unix.Mprotect(slice, unix.PROT_NONE); err != nil {
		panic(err)
	}
}
