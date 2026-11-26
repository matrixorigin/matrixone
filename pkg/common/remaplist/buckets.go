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

package remaplist

import (
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// SplitBuckets is the one counting sort that splits rows into buckets. ids[i]
// is the bucket of row i. It writes rows 0..len(ids)-1 into rows grouped by
// bucket, in ascending order within each bucket, and the bucket boundaries
// into offs: bucket b is rows[offs[b]:offs[b+1]]. The number of buckets is
// len(offs)-1.
//
// It makes two passes over ids and needs no scratch beyond rows and offs, so
// callers that charge their buffers to an allocation account can pass their
// own. A List owner calls List.BuildBuckets instead.
func SplitBuckets(ids []uint64, rows []int64, offs []int64) error {
	if len(offs) == 0 {
		return moerr.NewInternalErrorNoCtx("remaplist: SplitBuckets needs at least one bucket")
	}
	if len(rows) < len(ids) {
		return moerr.NewInternalErrorNoCtxf("remaplist: %d rows do not fit in %d", len(ids), len(rows))
	}
	buckets := uint64(len(offs) - 1)
	// Count bucket b in offs[b+1]. The prefix sum then makes offs[b] the start
	// of bucket b, which the placement pass uses as its write cursor.
	clear(offs)
	for _, id := range ids {
		if id >= buckets {
			return moerr.NewInternalErrorNoCtxf("remaplist: bucket %d out of %d", id, buckets)
		}
		offs[id+1]++
	}
	for b := 1; b < len(offs); b++ {
		offs[b] += offs[b-1]
	}
	cursor := offs[:buckets]
	for row, id := range ids {
		rows[cursor[id]] = int64(row)
		cursor[id]++
	}
	// Each cursor now holds the end of its bucket, which is the start of the
	// next one. Shift them back into place.
	copy(offs[1:], offs[:buckets])
	offs[0] = 0
	return nil
}

// BuildBuckets sets the List to rows 0..len(ids)-1 grouped by bucket, as
// SplitBuckets does: ids[i] < buckets is the bucket of row i. Bucket(b)
// returns the rows of bucket b.
func (r *List) BuildBuckets(ids []uint64, buckets int) error {
	if buckets <= 0 || buckets >= math.MaxInt/8 {
		return moerr.NewInternalErrorNoCtxf("remaplist: %d buckets", buckets)
	}
	if r.mp == nil {
		return errNoInit
	}
	r.rows = r.rows[:0]
	if err := r.reserve(len(ids)); err != nil {
		return err
	}
	r.rows = r.rows[:len(ids)]
	if cap(r.offs) < buckets+1 {
		b, err := r.mp.Alloc((buckets+1)*8, true)
		if err != nil {
			return err
		}
		r.freeOffs()
		r.offs = unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(b))), buckets+1)
	}
	r.offs = r.offs[:buckets+1]
	if err := SplitBuckets(ids, r.rows, r.offs); err != nil {
		r.rows = r.rows[:0]
		r.offs = r.offs[:0]
		return err
	}
	return nil
}

// Buckets is the number of buckets of the last BuildBuckets, or zero.
func (r *List) Buckets() int {
	return max(len(r.offs)-1, 0)
}

// Bucket returns the source rows of bucket b, in ascending order. It is valid
// until the List next changes.
func (r *List) Bucket(b int) []int64 {
	return r.rows[r.offs[b]:r.offs[b+1]]
}

func (r *List) freeOffs() {
	if r.mp != nil && cap(r.offs) > 0 {
		r.mp.Free(int64Bytes(r.offs[:1]))
	}
	r.offs = nil
}
