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
// allocator), in one contiguous buffer per field, so a multi-GB cached index is
// not GC-scanned (the `positions [][]int32` per-doc slice headers were the worst
// offender) and releases RSS deterministically on eviction (Free → the OS
// immediately, unlike Go's scavenger). Mirrors bm25's WandModel decode.
//
//   - docIDs: totalP int64        (all terms' doc ords, concatenated)
//   - tfs:    totalP uint8
//   - pos:    totalPos int32      (all docs' positions, flattened)
//   - off:    totalP+nterms int64 (per-term absolute offset run into pos, df+1 each)
//
// Each term slices views into these. The deallocators are recorded on the segment
// and released by Free(); on a partial-allocation error Free() unwinds what was
// taken. Zero totals return nil (callers slice zero-length into them harmlessly).
func (s *Segment) allocPostings(totalP, totalPos, nterms int64) (docIDs []int64, tfs []uint8, pos []int32, off []int64, err error) {
	alloc := malloc.NewCAllocator()
	take := func(nbytes uint64) ([]byte, error) {
		b, dec, e := alloc.Allocate(nbytes, malloc.NoClear)
		if e != nil {
			return nil, e
		}
		s.deallocators = append(s.deallocators, dec)
		return b, nil
	}
	fail := func(e error) ([]int64, []uint8, []int32, []int64, error) {
		s.Free() // release whatever was already taken; idempotent
		return nil, nil, nil, nil, e
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
	if totalPos > 0 {
		pb, e := take(uint64(totalPos) * uint64(util.UnsafeSizeOf[int32]()))
		if e != nil {
			return fail(e)
		}
		pos = util.UnsafeSliceCastToLength[int32](pb, int(totalPos))
	}
	if noff := totalP + nterms; noff > 0 {
		ob, e := take(uint64(noff) * uint64(util.UnsafeSizeOf[int64]()))
		if e != nil {
			return fail(e)
		}
		off = util.UnsafeSliceCastToLength[int64](ob, int(noff))
	}
	return docIDs, tfs, pos, off, nil
}
