// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"math"
)

// unionDictionaryBatch runs after ordinary destination preflight/admission.
// It gathers directly from the immutable backing into the existing destination;
// source metadata remains indexed by logical row, including repeated values.
func (v *Vector) unionDictionaryBatch(w *Vector, offset int64, count int, flags []uint8,
	overrides []types.StringSource, added int, areaWasDisjoint bool, mp *mpool.MPool) error {
	backing := w.dictionaryState()
	base, rows := backing.base.vec, backing.rows.storageFixed()
	oldLength := v.length
	v.setLengthAfterExtend(oldLength + added)
	appendBatchGrouping(v, w, oldLength, offset, count, flags)
	if !v.typ.IsVarlen() && copyDictionaryFixedBatch(v, w, int(offset), count, flags, oldLength, added) {
		// Capacity and grouping were admitted by the existing append preflight.
	} else {
		width := v.typ.TypeSize()
		var dstValues, srcValues []types.Varlena
		if v.typ.IsVarlen() {
			dstValues = toSliceOfLengthNoTypeCheck[types.Varlena](v, v.length)
			if !base.IsConstNull() {
				physicalLength := base.length
				if base.IsConst() {
					physicalLength = 1
				}
				srcValues = toSliceOfLengthNoTypeCheck[types.Varlena](base, physicalLength)
			}
		}
		sourceCount := count
		if flags != nil {
			sourceCount = len(flags)
		}
		next := oldLength
		for row := 0; row < sourceCount; row++ {
			if flags != nil && flags[row] == 0 {
				continue
			}
			logical := int(offset) + row
			if w.IsNull(uint64(logical)) {
				nulls.Add(&v.nsp, uint64(next))
				if v.typ.IsVarlen() {
					dstValues[next] = types.Varlena{}
				}
			} else {
				physical := int(rows[logical])
				if base.IsConst() {
					physical = 0
				}
				if v.typ.IsVarlen() {
					if err := BuildVarlenaFromVarlena(v, &dstValues[next], &srcValues[physical], &base.area, mp); err != nil {
						return err
					}
				} else {
					copy(v.data[next*width:(next+1)*width], base.data[physical*width:(physical+1)*width])
				}
			}
			next++
		}
	}
	if err := v.propagatePrepareParamKindsBatch(w, oldLength, offset, count, flags, overrides, mp); err != nil {
		return err
	}
	if err := v.propagateBinaryStringBatch(w, oldLength, offset, count, flags, mp); err != nil {
		return err
	}
	if v.typ.IsVarlen() {
		v.areaDisjoint = areaWasDisjoint
	}
	return nil
}

// gatherDictionarySelection uses the existing bulk append preflight once. It
// never reruns UnionOne admission/metadata transitions for every selected row.
func gatherDictionarySelection[S int32 | int64](v, w *Vector, sels []S, offset int, areaWasDisjoint bool, mp *mpool.MPool) error {
	base := w.dictionaryState().base.vec
	if base.IsConstNull() {
		nulls.AddRange(&v.nsp, uint64(offset), uint64(v.length))
		clear(v.data[offset*v.typ.TypeSize() : v.length*v.typ.TypeSize()])
		if v.typ.IsVarlen() {
			v.areaDisjoint = areaWasDisjoint
		}
		return nil
	}
	if !v.typ.IsVarlen() {
		switch v.typ.TypeSize() {
		case 1:
			gatherDictionaryFixed[uint8](v, w, sels, offset)
		case 2:
			gatherDictionaryFixed[uint16](v, w, sels, offset)
		case 4:
			gatherDictionaryFixed[uint32](v, w, sels, offset)
		case 8:
			gatherDictionaryFixed[uint64](v, w, sels, offset)
		case 12:
			gatherDictionaryFixed[[12]byte](v, w, sels, offset)
		case 16:
			gatherDictionaryFixed[[16]byte](v, w, sels, offset)
		case 20:
			gatherDictionaryFixed[[20]byte](v, w, sels, offset)
		case 24:
			gatherDictionaryFixed[[24]byte](v, w, sels, offset)
		case 32:
			gatherDictionaryFixed[[32]byte](v, w, sels, offset)
		default:
			view := ResolveFixedBytesReadView(w)
			for i, row := range sels {
				target := v.data[(offset+i)*view.Width : (offset+i+1)*view.Width]
				if w.nsp.Contains(uint64(row)) {
					clear(target)
					nulls.Add(&v.nsp, uint64(offset+i))
					continue
				}
				physical := int(view.Selection[row])
				if view.Constant {
					physical = 0
				}
				copy(target, view.Values[physical*view.Width:(physical+1)*view.Width])
			}
		}
		return nil
	}
	view := ResolveVarlenaReadView(w)
	total := 0
	for _, row := range sels {
		if view.Nulls.Contains(uint64(row)) {
			continue
		}
		physical := int(view.Selection[row])
		if view.Constant {
			physical = 0
		}
		value := view.Values[physical]
		if !value.IsSmall() {
			_, length := value.OffsetLen()
			if int(length) > math.MaxInt-total {
				return moerr.NewInvalidInputNoCtx("selected dictionary payload overflows")
			}
			total += int(length)
		}
	}
	if err := pregrowVarlenaArea(v, total, mp); err != nil {
		return err
	}
	target := toSliceOfLengthNoTypeCheck[types.Varlena](v, v.length)
	for i, row := range sels {
		if view.Nulls.Contains(uint64(row)) {
			target[offset+i] = types.Varlena{}
			nulls.Add(&v.nsp, uint64(offset+i))
			continue
		}
		physical := int(view.Selection[row])
		if view.Constant {
			physical = 0
		}
		if err := BuildVarlenaFromVarlena(v, &target[offset+i], &view.Values[physical], &view.Area, mp); err != nil {
			return err
		}
	}
	v.areaDisjoint = areaWasDisjoint
	return nil
}

func gatherDictionaryFixed[T any, S int32 | int64](v, w *Vector, sels []S, offset int) {
	backing := w.dictionaryState()
	base := backing.base.vec
	count := base.length
	if base.IsConst() {
		count = 1
	}
	source := toSliceOfLengthNoTypeCheck[T](base, count)
	target := toSliceOfLengthNoTypeCheck[T](v, v.length)
	mapping := backing.rows.storageFixed()
	constant := base.IsConst()
	if w.nsp.Any() {
		var zero T
		for i, row := range sels {
			if w.nsp.Contains(uint64(row)) {
				target[offset+i] = zero
				nulls.Add(&v.nsp, uint64(offset+i))
				continue
			}
			physical := int(mapping[row])
			if constant {
				physical = 0
			}
			target[offset+i] = source[physical]
		}
		return
	}
	for i, row := range sels {
		physical := int(mapping[row])
		if constant {
			physical = 0
		}
		target[offset+i] = source[physical]
	}
}

func dictionarySelectedAreaBytes(source *Vector, offset int, flags []uint8) (int, error) {
	view := ResolveVarlenaReadView(source)
	if view.AllNull {
		return 0, nil
	}
	total := 0
	for i, flag := range flags {
		row := offset + i
		if flag == 0 || view.Nulls.Contains(uint64(row)) {
			continue
		}
		physical := int(view.Selection[row])
		if view.Constant {
			physical = 0
		}
		value := view.Values[physical]
		if value.IsSmall() {
			continue
		}
		_, length := value.OffsetLen()
		if int(length) > math.MaxInt-total {
			return 0, mpool.ErrAllocationAllocatorLimit
		}
		total += int(length)
	}
	return total, nil
}

// Return false only for an unrecognized physical width, leaving the established
// generic path intact. Type-width, validity and constant decisions are per batch.
func copyDictionaryFixedBatch(dst, src *Vector, start, count int, flags []uint8, offset, added int) bool {
	switch src.typ.TypeSize() {
	case 1:
		copyDictionaryFixedRows[uint8](dst, src, start, count, flags, offset, added)
	case 2:
		copyDictionaryFixedRows[uint16](dst, src, start, count, flags, offset, added)
	case 4:
		copyDictionaryFixedRows[uint32](dst, src, start, count, flags, offset, added)
	case 8:
		copyDictionaryFixedRows[uint64](dst, src, start, count, flags, offset, added)
	case 12:
		copyDictionaryFixedRows[[12]byte](dst, src, start, count, flags, offset, added)
	case 16:
		copyDictionaryFixedRows[[16]byte](dst, src, start, count, flags, offset, added)
	case 20:
		copyDictionaryFixedRows[[20]byte](dst, src, start, count, flags, offset, added)
	case 24:
		copyDictionaryFixedRows[[24]byte](dst, src, start, count, flags, offset, added)
	case 32:
		copyDictionaryFixedRows[[32]byte](dst, src, start, count, flags, offset, added)
	default:
		return false
	}
	return true
}

func copyDictionaryFixedRows[T any](dst, src *Vector, start, count int, flags []uint8, offset, added int) {
	backing := src.dictionaryState()
	base := backing.base.vec
	target := types.DecodeSlice[T](dst.data[:dst.length*dst.typ.TypeSize()])[offset : offset+added]
	if base.IsConstNull() {
		clear(target)
		nulls.AddRange(&dst.nsp, uint64(offset), uint64(offset+added))
		return
	}
	physicalRows := base.length
	if base.IsConst() {
		physicalRows = 1
	}
	values := types.DecodeSlice[T](base.data[:physicalRows*base.typ.TypeSize()])
	mapping := backing.rows.storageFixed()
	if !src.HasNull() {
		if base.IsConst() {
			for i := range target {
				target[i] = values[0]
			}
			return
		}
		if flags == nil {
			for i, row := range mapping[start : start+count] {
				target[i] = values[row]
			}
			return
		}
		next := 0
		for i, flag := range flags {
			if flag != 0 {
				target[next] = values[mapping[start+i]]
				next++
			}
		}
		return
	}
	var zero T
	next := 0
	limit := count
	if flags != nil {
		limit = len(flags)
	}
	for i := 0; i < limit; i++ {
		if flags != nil && flags[i] == 0 {
			continue
		}
		logical := start + i
		if src.nsp.Contains(uint64(logical)) {
			target[next] = zero
			nulls.Add(&dst.nsp, uint64(offset+next))
		} else {
			physical := int(mapping[logical])
			if base.IsConst() {
				physical = 0
			}
			target[next] = values[physical]
		}
		next++
	}
}

// unionDictionaryMulti keeps selected repetition outside the flat broadcast
// frame, reusing UnionOne's logical metadata and allocation-failure contract.
func (v *Vector) unionDictionaryMulti(w *Vector, row int64, count int, mp *mpool.MPool) error {
	for i := 0; i < count; i++ {
		if err := v.UnionOne(w, row, mp); err != nil {
			return err
		}
	}
	return nil
}

// unionDictionaryOne reuses the selected bulk path for one logical row. It
// keeps dictionary preflight and mapping outside the original scalar flat
// frame without a complete input materialization. Only self-append needs an
// accounted one-row snapshot before the destination changes representation.
func (v *Vector) unionDictionaryOne(w *Vector, row int64, mp *mpool.MPool) error {
	if v == w {
		source, err := NewOffHeapVecWithTypeAndAllocation(v.typ, v.allocationAccount)
		if err != nil {
			return err
		}
		defer source.Free(mp)
		if err := source.UnionBatch(w, row, 1, nil, mp); err != nil {
			return err
		}
		return v.UnionOne(source, 0, mp)
	}
	return v.UnionBatch(w, row, 1, nil, mp)
}
