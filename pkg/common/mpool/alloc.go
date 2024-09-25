// Copyright 2021 - 2022 Matrix Origin
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

package mpool

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

func alloc(sz, requiredSpaceWithoutHeader int, mp *MPool) []byte {
	allocateSize := requiredSpaceWithoutHeader + kMemHdrSz
	var bs []byte
	var err error
	if mp.useMalloc {
		bs, err = allocator().Allocate(uint64(allocateSize), malloc.NoHints)
		if err != nil {
			panic(err)
		}
	} else {
		bs = make([]byte, allocateSize)
	}

	hdr := unsafe.Pointer(&bs[0])
	pHdr := (*memHdr)(hdr)
	pHdr.poolId = mp.id
	pHdr.fixedPoolIdx = NumFixedPool
	pHdr.allocSz = int32(sz)
	pHdr.SetGuard()
	if mp.details != nil {
		mp.details.recordAlloc(int64(pHdr.allocSz))
	}
	if mp.useMalloc {
		pHdr.isMallocAllocated = true
	}
	return pHdr.ToSlice(sz, requiredSpaceWithoutHeader)
}
