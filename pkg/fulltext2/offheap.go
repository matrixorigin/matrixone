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

package fulltext2

import (
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/util"
)

// allocPostings allocates a LOADED segment's posting buffers OFF the Go heap (C
// allocator) so a multi-GB cached index is not GC-scanned and releases RSS
// deterministically on eviction (Free → the OS immediately, unlike Go's
// scavenger). Mirrors bm25's WandModel decode.
//
//   - docIDs:  totalP int64   (all terms' doc ords, concatenated; delta-decoded)
//   - tfs:     totalP uint8
//   - posBuf:  posBytes []byte (the COMPRESSED positions section, kept as-is; each
//     term's posRaw slices into it and is decoded on demand — positions
//     are never expanded into RAM at load)
//
// The deallocators are recorded on the segment and released by Free(); on a
// partial-allocation error Free() unwinds what was taken. Zero sizes return nil.
func (s *Segment) allocPostings(totalP, posBytes int64) (docIDs []int64, tfs []uint8, posBuf []byte, err error) {
	alloc := malloc.NewCAllocator()
	take := func(nbytes uint64) ([]byte, error) {
		b, dec, e := alloc.Allocate(nbytes, malloc.NoClear)
		if e != nil {
			return nil, e
		}
		s.deallocators = append(s.deallocators, dec)
		return b, nil
	}
	fail := func(e error) ([]int64, []uint8, []byte, error) {
		s.Free() // release whatever was already taken; idempotent
		return nil, nil, nil, e
	}

	if totalP > 0 {
		db, e := take(uint64(totalP) * uint64(util.UnsafeSizeOf[int64]()))
		if e != nil {
			return fail(e)
		}
		docIDs = util.UnsafeSliceCastToLength[int64](db, int(totalP))
		tb, e := take(uint64(totalP))
		if e != nil {
			return fail(e)
		}
		tfs = util.UnsafeSliceCastToLength[uint8](tb, int(totalP))
	}
	if posBytes > 0 {
		pb, e := take(uint64(posBytes))
		if e != nil {
			return fail(e)
		}
		posBuf = pb
	}
	return docIDs, tfs, posBuf, nil
}
