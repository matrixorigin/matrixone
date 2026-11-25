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

func adviseLargeMpoolMapping(data []byte) {
	if len(data) >= 2<<20 {
		// Under defer+madvise, this lets large MPool buffers obtain huge pages
		// through compaction instead of depending only on immediately free
		// contiguous blocks. Advice is optional; allocation remains valid if
		// the kernel does not support or accept it.
		_ = unix.Madvise(data, unix.MADV_HUGEPAGE)
	}
}

func (f *fixedSizeMmapAllocator) reuseMem(ptr unsafe.Pointer, hints Hints, clearSize uint64) {
	// no need to clear, since re-visiting a MADV_DONTNEED-advised page will zero it
}

func (f *fixedSizeMmapAllocator) freeMem(ptr unsafe.Pointer) {
	if err := unix.Madvise(
		unsafe.Slice((*byte)(ptr), f.size),
		unix.MADV_DONTNEED,
	); err != nil {
		panic(err)
	}
}
