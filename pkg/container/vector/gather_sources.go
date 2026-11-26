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

package vector

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// UnionRowsFrom appends one row from each of several source vectors: output
// row v.Length()+i is row rows[i] of srcs[i]. It is the many-source gather,
// used where a row list spans batches, such as the build side of a hash join
// whose matches lie in different build batches. Every source must have v's
// type. NULLs move with their rows and varlena payloads are copied into v.
//
// Flat sources take one pass per column; anything else (a constant source,
// a source that is v itself, a different type) is appended row by row with
// UnionOne, so the result is the same either way. A row outside its flat
// source is an error, and v is then unchanged.
func (v *Vector) UnionRowsFrom(srcs []*Vector, rows []int64, mp *mpool.MPool) error {
	if len(srcs) != len(rows) {
		return moerr.NewInternalErrorNoCtxf("UnionRowsFrom: %d sources for %d rows", len(srcs), len(rows))
	}
	if len(rows) == 0 {
		return nil
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	varlen := v.typ.IsVarlen()
	hasNulls := false
	area := 0
	fast := true
	for i, w := range srcs {
		r := rows[i]
		if w == nil {
			return moerr.NewInternalErrorNoCtx("UnionRowsFrom: nil source")
		}
		if w.class == FLAT && (r < 0 || r >= int64(w.length)) {
			return moerr.NewInternalErrorNoCtxf("UnionRowsFrom: row %d out of %d", r, w.length)
		}
		if fast && (w == v || w.class != FLAT || w.typ != v.typ) {
			fast = false
		}
		if !fast {
			continue
		}
		if !w.nsp.IsEmpty() && w.nsp.Contains(uint64(r)) {
			hasNulls = true
			continue
		}
		if varlen {
			if va := (*types.Varlena)(unsafe.Pointer(&w.data[int(r)*types.VarlenaSize])); !va.IsSmall() {
				_, l := va.OffsetLen()
				area += int(l)
			}
		}
	}
	if !fast {
		return v.unionRowsOneByOne(srcs, rows, mp)
	}
	if err := extendWithBitmaps(v, len(rows), mp, hasNulls); err != nil {
		return err
	}
	oldLen := v.length
	if hasNulls {
		if err := v.nsp.GetBitmap().TryExpandWithSize(oldLen + len(rows)); err != nil {
			return err
		}
		gatherNullsFrom(v, oldLen, srcs, rows)
	}
	if varlen {
		if err := pregrowVarlenaArea(v, area, mp); err != nil {
			return err
		}
		var vCol []types.Varlena
		v.setLengthAfterExtend(oldLen + len(rows))
		ToSliceNoTypeCheck(v, &vCol)
		out := vCol[oldLen:]
		for i, w := range srcs {
			r := rows[i]
			if hasNulls && v.nsp.Contains(uint64(oldLen+i)) {
				// A reused destination can hold a stale descriptor here.
				out[i] = types.Varlena{}
				continue
			}
			src := (*types.Varlena)(unsafe.Pointer(&w.data[int(r)*types.VarlenaSize]))
			if src.IsSmall() {
				out[i] = *src
				continue
			}
			// The area was grown for every long value above, so this append
			// never reallocates.
			off, l := src.OffsetLen()
			out[i].SetOffsetLen(uint32(len(v.area)), l)
			v.area = append(v.area, w.area[off:off+l]...)
		}
		return nil
	}
	// Fixed width: copy every value, NULL rows included; a NULL row's value is
	// never read.
	width := v.typ.TypeSize()
	target := v.data[oldLen*width : (oldLen+len(rows))*width]
	switch width {
	case 8:
		gatherFixedFrom[uint64](target, srcs, rows)
	case 4:
		gatherFixedFrom[uint32](target, srcs, rows)
	case 2:
		gatherFixedFrom[uint16](target, srcs, rows)
	case 1:
		gatherFixedFrom[uint8](target, srcs, rows)
	case 12:
		gatherFixedFrom[types.TS](target, srcs, rows)
	case 16:
		gatherFixedFrom[types.Decimal128](target, srcs, rows)
	case 20:
		gatherFixedFrom[types.Blockid](target, srcs, rows)
	case 24:
		gatherFixedFrom[types.Rowid](target, srcs, rows)
	case 32:
		gatherFixedFrom[types.Decimal256](target, srcs, rows)
	default:
		for i, w := range srcs {
			r := int(rows[i])
			copy(target[i*width:(i+1)*width], w.data[r*width:(r+1)*width])
		}
	}
	v.setLengthAfterExtend(oldLen + len(rows))
	return nil
}

// gatherFixedFrom sets value i of target to value rows[i] of srcs[i]. The
// rows were checked against their sources' lengths.
func gatherFixedFrom[T any](target []byte, srcs []*Vector, rows []int64) {
	dst := unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(target))), len(rows))
	for i, w := range srcs {
		dst[i] = *(*T)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(w.data)), uintptr(rows[i])*unsafe.Sizeof(dst[0])))
	}
}

// gatherNullsFrom sets v's row base+i when row rows[i] of srcs[i] is NULL,
// building each output word in a register, 64 rows at a time.
func gatherNullsFrom(v *Vector, base int, srcs []*Vector, rows []int64) {
	out := v.nsp.GetBitmap()
	for i := 0; i < len(rows); {
		p := base + i
		lo := p & 63
		end := min(len(rows), i+64-lo)
		var acc uint64
		for k := i; k < end; k++ {
			if w := srcs[k]; !w.nsp.IsEmpty() && w.nsp.Contains(uint64(rows[k])) {
				acc |= 1 << (uint(lo+k-i) & 63)
			}
		}
		if acc != 0 {
			out.OrWord(p>>6, acc)
		}
		i = end
	}
}

// unionRowsOneByOne is the general path of UnionRowsFrom.
func (v *Vector) unionRowsOneByOne(srcs []*Vector, rows []int64, mp *mpool.MPool) error {
	for i, w := range srcs {
		if err := v.UnionOne(w, rows[i], mp); err != nil {
			return err
		}
	}
	return nil
}
