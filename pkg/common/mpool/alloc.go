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

//go:build !race
// +build !race

package mpool

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

func alloc(sz, requiredSpaceWithoutHeader int, mp *MPool) []byte {
	bs := make([]byte, requiredSpaceWithoutHeader+kMemHdrSz)
	hdr := unsafe.Pointer(&bs[0])
	pHdr := (*memHdr)(hdr)
	pHdr.poolId = mp.id
	pHdr.fixedPoolIdx = NumFixedPool
	pHdr.allocSz = int32(sz)
	pHdr.SetGuard()
	if mp.details != nil {
		pHdr.allocateStacktraceID = uint64(malloc.GetStacktraceID(0))
		mp.details.recordAlloc(int64(pHdr.allocSz), pHdr.allocateStacktraceID)
	}
	return pHdr.ToSlice(sz, requiredSpaceWithoutHeader)
}
