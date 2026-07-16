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

// allocPostings allocates a LOADED segment's docID/tf buffers OFF the Go heap (C
// allocator) so a large cached index is not GC-scanned and releases RSS
// deterministically on eviction. docIDs are delta-decoded into these buffers;
// positions are NOT copied here — they stay compressed in the segment's backing
// bytes (mmap for a base, the read blob for a tail) and each term's posRaw slices
// into that. Mirrors bm25's WandModel decode. The deallocators are recorded on the
// segment and released by Free(); on a partial-allocation error Free() unwinds.
func (s *Segment) allocPostings(totalP int64) (docIDs []int64, tfs []uint8, err error) {
	if totalP <= 0 {
		return nil, nil, nil
	}
	alloc := malloc.NewCAllocator()
	take := func(nbytes uint64) ([]byte, error) {
		b, dec, e := alloc.Allocate(nbytes, malloc.NoClear)
		if e != nil {
			return nil, e
		}
		s.deallocators = append(s.deallocators, dec)
		return b, nil
	}
	db, e := take(uint64(totalP) * uint64(util.UnsafeSizeOf[int64]()))
	if e != nil {
		s.Free()
		return nil, nil, e
	}
	docIDs = util.UnsafeSliceCastToLength[int64](db, int(totalP))
	tb, e := take(uint64(totalP))
	if e != nil {
		s.Free()
		return nil, nil, e
	}
	tfs = util.UnsafeSliceCastToLength[uint8](tb, int(totalP))
	return docIDs, tfs, nil
}
