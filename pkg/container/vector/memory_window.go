// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"math"
)

// WindowSize is the payload of a compact flat copy of [start,end). Window views
// share their parent's area; its full length is not the payload of the window.
// Constant values are counted once for every row of the expanded copy.
func (v *Vector) WindowSize(start, end int) int64 {
	if start < 0 || end < start || (!v.IsConst() && end > v.length) {
		panic("invalid vector memory window")
	}
	rows, width := int64(end-start), int64(v.typ.TypeSize())
	if width > 0 && rows > math.MaxInt64/width {
		return math.MaxInt64
	}
	total := rows * width
	if !v.typ.IsVarlen() || v.IsConstNull() {
		return total
	}
	col := MustFixedColNoTypeCheck[types.Varlena](v)
	if v.IsConst() {
		if v.nsp.Contains(0) || col[0].IsSmall() {
			return total
		}
		_, n := col[0].OffsetLen()
		if n > 0 && rows > (math.MaxInt64-total)/int64(n) {
			return math.MaxInt64
		}
		return total + rows*int64(n)
	}
	for row := start; row < end; row++ {
		if v.nsp.Contains(uint64(row)) || col[row].IsSmall() {
			continue
		}
		_, n := col[row].OffsetLen()
		if total > math.MaxInt64-int64(n) {
			return math.MaxInt64
		}
		total += int64(n)
	}
	return total
}

// AppendAllocation predicts complete new allocations made by appending addRows
// and extraAreaBytes using PreExtendWithArea. Existing storage remains charged
// until growth has finished. A zero result means both capacities suffice.
func (v *Vector) AppendAllocation(addRows, extraAreaBytes int) int64 {
	if addRows < 0 || extraAreaBytes < 0 {
		return math.MaxInt64
	}
	if v.IsConst() {
		return 0
	}
	rows := int64(v.length) + int64(addRows)
	width := int64(v.typ.TypeSize())
	if width > 0 && rows > math.MaxInt64/width {
		return math.MaxInt64
	}
	required := rows * width
	if required > int64(mpool.CapLimit) {
		return math.MaxInt64
	}
	data := mpool.GrowthAllocation(int64(cap(v.data)), required)
	areaRequired := int64(len(v.area)) + int64(extraAreaBytes)
	if areaRequired > int64(mpool.CapLimit) {
		return math.MaxInt64
	}
	area := mpool.GrowthAllocation(int64(cap(v.area)), areaRequired)
	if data > math.MaxInt64-area {
		return math.MaxInt64
	}
	return data + area
}

// UnionBatchAreaBytes is the area appended by a no-flags UnionBatch of this
// window. Constants share one copied value. Flat vectors preserve shared
// offsets when copying the whole physical area costs no more than compacting
// the referenced values; otherwise dead area outside the window is discarded.
func (v *Vector) UnionBatchAreaBytes(start, end int) int64 {
	if start < 0 || end < start || (!v.IsConst() && end > v.length) {
		panic("invalid vector memory window")
	}
	if start == end || !v.typ.IsVarlen() || v.IsConstNull() || len(v.area) == 0 {
		return 0
	}
	if v.IsConst() {
		value := MustFixedColNoTypeCheck[types.Varlena](v)[0]
		if value.IsSmall() {
			return 0
		}
		_, length := value.OffsetLen()
		return int64(length)
	}
	// Match 4.2's no-scan owned-vector copy exactly. Physical area may still
	// include payload under a null or an overwritten descriptor.
	if start == 0 && end == v.length && v.areaDisjoint && !v.cantFreeArea {
		return int64(len(v.area))
	}
	payload := v.WindowSize(start, end) - int64(end-start)*int64(v.typ.TypeSize())
	startArea, endArea, connected := v.connectedWindowArea(start, end)
	if connected {
		return int64(endArea - startArea)
	}
	return payload
}

func windowHasBitmapBits(bitmap *nulls.Nulls, start, end int) bool {
	if start == end || bitmap.EmptyByFlag() {
		return false
	}
	// Merge-sort forecasts one row at a time. Starting an iterator at zero
	// would rescan the entire preceding bitmap for every row, making dense
	// null/grouping inputs quadratic. Examine only the selected window.
	for row := start; row < end; row++ {
		if bitmap.Contains(uint64(row)) {
			return true
		}
	}
	return false
}

// AppendWindowAllocation predicts complete new backing stores for UnionBatch
// and GetUnionAllFunction, including null/grouping bitmaps. Existing storage
// remains live until each replacement is installed. Only bitmaps with a source
// bit in the selected window grow; stale source words are not copied.
func (v *Vector) AppendWindowAllocation(src *Vector, start, end int) int64 {
	area := src.UnionBatchAreaBytes(start, end)
	if start == end {
		return 0
	}
	if v.length > math.MaxInt-(end-start)-1 {
		return math.MaxInt64
	}
	if area > int64(math.MaxInt) {
		return math.MaxInt64
	}
	total := v.AppendAllocation(end-start, int(area))
	bitmaps := []struct {
		destination *nulls.Nulls
		copied      bool
	}{
		{&v.nsp, src.IsConstNull() || windowHasBitmapBits(&src.nsp, start, end)},
		{&v.gsp, windowHasBitmapBits(&src.gsp, start, end)},
	}
	for _, bitmap := range bitmaps {
		if !bitmap.copied {
			continue
		}
		growth := nulls.GrowthAllocation(bitmap.destination, v.length+end-start+1)
		if total > math.MaxInt64-growth {
			return math.MaxInt64
		}
		total += growth
	}
	return total
}

// connectedWindowArea recognizes a connected union of referenced payloads.
// Overlapping aliases can share one copied span; disconnected ranges use the
// ordinary compact-copy path instead of retaining unrelated parent payload.
func (v *Vector) connectedWindowArea(start, end int) (low, high int, connected bool) {
	seen := false
	col := MustFixedColNoTypeCheck[types.Varlena](v)
	for i := start; i < end; i++ {
		if v.nsp.Contains(uint64(i)) || col[i].IsSmall() {
			continue
		}
		off, size := col[i].OffsetLen()
		stop := uint64(off) + uint64(size)
		if stop > uint64(len(v.area)) {
			return 0, 0, false
		}
		if !seen {
			low, high, seen = int(off), int(stop), true
			continue
		}
		if int(off) > high || int(stop) < low {
			return 0, 0, false
		}
		low = min(low, int(off))
		high = max(high, int(stop))
	}
	return low, high, true
}
