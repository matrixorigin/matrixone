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

const (
	madv_FREE_REUSABLE = 0x7
	madv_FREE_REUSE    = 0x8
)

func (f *fixedSizeMmapAllocator) reuseMem(ptr unsafe.Pointer, hints Hints) {
	if err := unix.Madvise(
		unsafe.Slice((*byte)(ptr), f.size),
		madv_FREE_REUSE,
	); err != nil {
		panic(err)
	}
	if hints&NoClear == 0 {
		clear(unsafe.Slice((*byte)(ptr), f.size))
	}
}

func (f *fixedSizeMmapAllocator) freeMem(ptr unsafe.Pointer) {
	if err := unix.Madvise(
		unsafe.Slice((*byte)(ptr), f.size),
		madv_FREE_REUSABLE,
	); err != nil {
		panic(err)
	}
}
