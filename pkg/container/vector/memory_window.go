// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package vector

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"math"
)

// WindowSize is the payload of a compact flat copy of [start,end). Window views
// share their parent's area; its full length is not the payload of the window.
// Constant values are counted once for every row of the expanded copy.
func (v *Vector) WindowSize(start, end int) int64 {
	if start < 0 || end < start || end > v.length {
		panic("invalid vector memory window")
	}
	total := int64(end-start) * int64(v.typ.TypeSize())
	if !v.typ.IsVarlen() || v.IsConstNull() {
		return total
	}
	col := MustFixedColNoTypeCheck[types.Varlena](v)
	for row := start; row < end; row++ {
		index := row
		if v.IsConst() {
			index = 0
		}
		if v.nsp.Contains(uint64(index)) || col[index].IsSmall() {
			continue
		}
		_, n := col[index].OffsetLen()
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
	data := mpool.GrowCapacity(int64(cap(v.data)), required)
	areaRequired := int64(len(v.area)) + int64(extraAreaBytes)
	if areaRequired > int64(mpool.CapLimit) {
		return math.MaxInt64
	}
	area := mpool.GrowCapacity(int64(cap(v.area)), areaRequired)
	if data > math.MaxInt64-area {
		return math.MaxInt64
	}
	return data + area
}

// AppendWindowAllocation uses the same growth policy as the actual vector
// append, counting only referenced out-of-line values of the source window.
func (v *Vector) AppendWindowAllocation(src *Vector, start, end int) int64 {
	size := src.WindowSize(start, end)
	fixed := int64(end-start) * int64(src.typ.TypeSize())
	area := size - fixed
	if area > int64(math.MaxInt) {
		return math.MaxInt64
	}
	return v.AppendAllocation(end-start, int(area))
}
