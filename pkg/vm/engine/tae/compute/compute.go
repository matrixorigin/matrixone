// Copyright 2021 Matrix Origin
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

package compute

import (
	"bytes"
	"fmt"
	"math"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

func LengthOfMoVector(vec *gvec.Vector) int { return gvec.Length(vec) }

func GenericUpdateFixedValue[T any](vec *gvec.Vector, row uint32, v any) {
	_, isNull := v.(types.Null)
	if isNull {
		if vec.Nsp.Np == nil {
			vec.Nsp.Np = roaring64.BitmapOf(uint64(row))
		} else {
			vec.Nsp.Np.Add(uint64(row))
		}
	} else {
		vvals := vec.Col.([]T)
		vvals[row] = v.(T)
		if vec.Nsp.Np != nil && vec.Nsp.Np.Contains(uint64(row)) {
			vec.Nsp.Np.Remove(uint64(row))
		}
	}
}

func AppendFixedValue[T any](vec *gvec.Vector, v any) {
	_, isNull := v.(types.Null)
	vvals := vec.Col.([]T)
	if isNull {
		row := len(vvals)
		vec.Col = append(vvals, types.DefaultVal[T]())
		if vec.Nsp.Np == nil {
			vec.Nsp.Np = roaring64.BitmapOf(uint64(row))
		} else {
			vec.Nsp.Np.Add(uint64(row))
		}
	} else {
		vec.Col = append(vvals, v.(T))
	}
	vec.Data = types.EncodeFixedSlice(vvals, int(vec.Typ.Size))
}

func AppendValue(vec *gvec.Vector, v any) {
	switch vec.Typ.Oid {
	case types.Type_BOOL:
		AppendFixedValue[bool](vec, v)
	case types.Type_INT8:
		AppendFixedValue[int8](vec, v)
	case types.Type_INT16:
		AppendFixedValue[int16](vec, v)
	case types.Type_INT32:
		AppendFixedValue[int32](vec, v)
	case types.Type_INT64:
		AppendFixedValue[int64](vec, v)
	case types.Type_UINT8:
		AppendFixedValue[uint8](vec, v)
	case types.Type_UINT16:
		AppendFixedValue[uint16](vec, v)
	case types.Type_UINT32:
		AppendFixedValue[uint32](vec, v)
	case types.Type_UINT64:
		AppendFixedValue[uint64](vec, v)
	case types.Type_DECIMAL64:
		AppendFixedValue[types.Decimal64](vec, v)
	case types.Type_DECIMAL128:
		AppendFixedValue[types.Decimal128](vec, v)
	case types.Type_FLOAT32:
		AppendFixedValue[float32](vec, v)
	case types.Type_FLOAT64:
		AppendFixedValue[float64](vec, v)
	case types.Type_DATE:
		AppendFixedValue[types.Date](vec, v)
	case types.Type_TIMESTAMP:
		AppendFixedValue[types.Timestamp](vec, v)
	case types.Type_DATETIME:
		AppendFixedValue[types.Datetime](vec, v)
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		vvals := vec.Col.(*types.Bytes)
		offset := len(vvals.Data)
		var val []byte
		if _, ok := v.(types.Null); ok {
			if vec.Nsp.Np == nil {
				vec.Nsp.Np = roaring64.BitmapOf(uint64(offset))
			} else {
				vec.Nsp.Np.Add(uint64(offset))
			}
		} else {
			val = v.([]byte)
		}
		length := len(val)
		vvals.Data = append(vvals.Data, val...)
		vvals.Offsets = append(vvals.Offsets, uint32(offset))
		vvals.Lengths = append(vvals.Lengths, uint32(length))
	default:
		panic(any("not expected"))
	}
}

func LengthOfBatch(bat *gbat.Batch) int {
	return gvec.Length(bat.Vecs[0])
}

func GetValue(col *gvec.Vector, row uint32) any {
	if col.Nsp.Np != nil && col.Nsp.Np.Contains(uint64(row)) {
		return types.Null{}
	}
	vals := col.Col
	switch col.Typ.Oid {
	case types.Type_BOOL:
		data := vals.([]bool)
		return data[row]
	case types.Type_INT8:
		data := vals.([]int8)
		return data[row]
	case types.Type_INT16:
		data := vals.([]int16)
		return data[row]
	case types.Type_INT32:
		data := vals.([]int32)
		return data[row]
	case types.Type_INT64:
		data := vals.([]int64)
		return data[row]
	case types.Type_UINT8:
		data := vals.([]uint8)
		return data[row]
	case types.Type_UINT16:
		data := vals.([]uint16)
		return data[row]
	case types.Type_UINT32:
		data := vals.([]uint32)
		return data[row]
	case types.Type_UINT64:
		data := vals.([]uint64)
		return data[row]
	case types.Type_DECIMAL64:
		data := vals.([]types.Decimal64)
		return data[row]
	case types.Type_DECIMAL128:
		data := vals.([]types.Decimal128)
		return data[row]
	case types.Type_FLOAT32:
		data := vals.([]float32)
		return data[row]
	case types.Type_FLOAT64:
		data := vals.([]float64)
		return data[row]
	case types.Type_DATE:
		data := vals.([]types.Date)
		return data[row]
	case types.Type_DATETIME:
		data := vals.([]types.Datetime)
		return data[row]
	case types.Type_TIMESTAMP:
		data := vals.([]types.Timestamp)
		return data[row]
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		data := vals.(*types.Bytes)
		s := data.Offsets[row]
		e := data.Lengths[row]
		// return string(data.Data[s : e+s])
		return data.Data[s : e+s]
	default:
		return vector.ErrVecTypeNotSupport
	}
}

func UpdateValue(col *gvec.Vector, row uint32, val any) {
	switch col.Typ.Oid {
	case types.Type_BOOL:
		GenericUpdateFixedValue[bool](col, row, val)
	case types.Type_INT8:
		GenericUpdateFixedValue[int8](col, row, val)
	case types.Type_INT16:
		GenericUpdateFixedValue[int16](col, row, val)
	case types.Type_INT32:
		GenericUpdateFixedValue[int32](col, row, val)
	case types.Type_INT64:
		GenericUpdateFixedValue[int64](col, row, val)
	case types.Type_UINT8:
		GenericUpdateFixedValue[uint8](col, row, val)
	case types.Type_UINT16:
		GenericUpdateFixedValue[uint16](col, row, val)
	case types.Type_UINT32:
		GenericUpdateFixedValue[uint32](col, row, val)
	case types.Type_UINT64:
		GenericUpdateFixedValue[uint64](col, row, val)
	case types.Type_DECIMAL64:
		GenericUpdateFixedValue[types.Decimal64](col, row, val)
	case types.Type_DECIMAL128:
		GenericUpdateFixedValue[types.Decimal128](col, row, val)
	case types.Type_FLOAT32:
		GenericUpdateFixedValue[float32](col, row, val)
	case types.Type_FLOAT64:
		GenericUpdateFixedValue[float64](col, row, val)
	case types.Type_DATE:
		GenericUpdateFixedValue[types.Date](col, row, val)
	case types.Type_DATETIME:
		GenericUpdateFixedValue[types.Datetime](col, row, val)
	case types.Type_TIMESTAMP:
		GenericUpdateFixedValue[types.Timestamp](col, row, val)
	case types.Type_VARCHAR, types.Type_CHAR, types.Type_JSON:
		v := val.([]byte)
		data := col.Col.(*types.Bytes)
		tail := data.Data[data.Offsets[row]+data.Lengths[row]:]
		data.Lengths[row] = uint32(len(v))
		v = append(v, tail...)
		data.Data = append(data.Data[:data.Offsets[row]], v...)
		if col.Nsp.Np != nil && col.Nsp.Np.Contains(uint64(row)) {
			col.Nsp.Np.Remove(uint64(row))
		}
	default:
		panic(fmt.Errorf("%v not supported", col.Typ))
	}
}

func DeleteFixSizeTypeValue(col *gvec.Vector, row uint32) error {
	vals := col.Col
	switch col.Typ.Oid {
	case types.Type_BOOL:
		data := vals.([]bool)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_INT8:
		data := vals.([]int8)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_INT16:
		data := vals.([]int16)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_INT32:
		data := vals.([]int32)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_INT64:
		data := vals.([]int64)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_UINT8:
		data := vals.([]uint8)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_UINT16:
		data := vals.([]uint16)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_UINT32:
		data := vals.([]uint32)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_UINT64:
		data := vals.([]uint64)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_DECIMAL64:
		data := vals.([]types.Decimal64)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_DECIMAL128:
		data := vals.([]types.Decimal128)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_FLOAT32:
		data := vals.([]float32)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_FLOAT64:
		data := vals.([]float64)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_DATE:
		data := vals.([]types.Date)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_DATETIME:
		data := vals.([]types.Datetime)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_TIMESTAMP:
		data := vals.([]types.Timestamp)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		// data := vals.(*types.Bytes)
		// s := data.Offsets[row]
		// e := data.Lengths[row]
		// return string(data.Data[s:e])
	default:
		return vector.ErrVecTypeNotSupport
	}
	return nil
}

func ForEachValue(col *gvec.Vector, reversed bool, op func(v any, row uint32) error) (err error) {
	if reversed {
		for i := gvec.Length(col) - 1; i >= 0; i-- {
			v := GetValue(col, uint32(i))
			if err = op(v, uint32(i)); err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < gvec.Length(col); i++ {
		v := GetValue(col, uint32(i))
		if err = op(v, uint32(i)); err != nil {
			return
		}
	}
	return
}

func UpdateOffsets(data *types.Bytes, start, end int) {
	if len(data.Offsets) == 0 {
		return
	}
	if start == -1 {
		data.Offsets[0] = 0
		start++
	}
	for i := start; i < end; i++ {
		data.Offsets[i+1] = data.Offsets[i] + data.Lengths[i]
	}
}

func BatchWindow(bat *gbat.Batch, start, end int) *gbat.Batch {
	window := gbat.New(true, bat.Attrs)
	window.Vecs = make([]*gvec.Vector, len(bat.Vecs))
	for i := range window.Vecs {
		vec := gvec.New(bat.Vecs[i].Typ)
		gvec.Window(bat.Vecs[i], start, end, vec)
		window.Vecs[i] = vec
	}
	return window
}

func SplitBatch(bat *gbat.Batch, cnt int) []*gbat.Batch {
	if cnt == 1 {
		return []*gbat.Batch{bat}
	}
	length := gvec.Length(bat.Vecs[0])
	rows := length / cnt
	if length%cnt == 0 {
		bats := make([]*gbat.Batch, 0, cnt)
		for i := 0; i < cnt; i++ {
			newBat := gbat.New(true, bat.Attrs)
			for j := 0; j < len(bat.Vecs); j++ {
				window := gvec.New(bat.Vecs[j].Typ)
				gvec.Window(bat.Vecs[j], i*rows, (i+1)*rows, window)
				newBat.Vecs[j] = window
			}
			bats = append(bats, newBat)
		}
		return bats
	}
	rowArray := make([]int, 0)
	if length/cnt == 0 {
		for i := 0; i < length; i++ {
			rowArray = append(rowArray, 1)
		}
	} else {
		left := length
		for i := 0; i < cnt; i++ {
			if left >= rows && i < cnt-1 {
				rowArray = append(rowArray, rows)
			} else {
				rowArray = append(rowArray, left)
			}
			left -= rows
		}
	}
	start := 0
	bats := make([]*gbat.Batch, 0, cnt)
	for _, row := range rowArray {
		newBat := gbat.New(true, bat.Attrs)
		for j := 0; j < len(bat.Vecs); j++ {
			window := gvec.New(bat.Vecs[j].Typ)
			gvec.Window(bat.Vecs[j], start, start+row, window)
			newBat.Vecs[j] = window
		}
		start += row
		bats = append(bats, newBat)
	}
	return bats
}

func EstimateSize(bat *containers.Batch, offset, length uint32) uint64 {
	size := uint64(0)
	for _, vec := range bat.Vecs {
		colSize := length * uint32(vec.GetType().Size)
		size += uint64(colSize)
	}
	return size
}

func CopyToIBatch(data *gbat.Batch, capacity uint64) (bat batch.IBatch, err error) {
	vecs := make([]vector.IVector, len(data.Vecs))
	attrs := make([]int, len(data.Vecs))
	for i, vec := range data.Vecs {
		attrs[i] = i
		vecs[i] = vector.NewVector(vec.Typ, capacity)
		_, err = vecs[i].AppendVector(vec, 0)
		if err != nil {
			return
		}
	}
	bat, err = batch.NewBatch(attrs, vecs)
	return
}

func ApplyDeleteToVector(vec *gvec.Vector, deletes *roaring.Bitmap) *gvec.Vector {
	if deletes == nil || deletes.IsEmpty() {
		return vec
	}
	col := vec.Col
	deletesIterator := deletes.Iterator()
	np := roaring64.New()
	var nspIterator roaring64.IntPeekable64
	if vec.Nsp != nil && vec.Nsp.Np != nil {
		nspIterator = vec.Nsp.Np.Iterator()
	}
	deleted := 0
	switch vec.Typ.Oid {
	case types.Type_BOOL, types.Type_INT8, types.Type_INT16, types.Type_INT32, types.Type_INT64,
		types.Type_UINT8, types.Type_UINT16, types.Type_UINT32, types.Type_UINT64,
		types.Type_DECIMAL64, types.Type_DECIMAL128, types.Type_FLOAT32, types.Type_FLOAT64,
		types.Type_DATE, types.Type_DATETIME, types.Type_TIMESTAMP:
		vec.Col = InplaceDeleteRows(vec.Col, deletesIterator)
		deletesIterator = deletes.Iterator()
		for deletesIterator.HasNext() {
			row := deletesIterator.Next()
			if nspIterator != nil {
				var n uint64
				if nspIterator.HasNext() {
					for nspIterator.HasNext() {
						n = nspIterator.PeekNext()
						if uint32(n) < row {
							nspIterator.Next()
						} else {
							if uint32(n) == row {
								nspIterator.Next()
							}
							break
						}
						np.Add(n - uint64(deleted))
					}
				}
			}
			deleted++
		}
		if nspIterator != nil {
			for nspIterator.HasNext() {
				n := nspIterator.Next()
				np.Add(n - uint64(deleted))
			}
		}
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		data := col.(*types.Bytes)
		pre := -1
		for deletesIterator.HasNext() {
			row := deletesIterator.Next()
			currRow := row - uint32(deleted)
			if pre != -1 {
				if int(currRow) == len(data.Lengths)-1 {
					UpdateOffsets(data, pre-1, int(currRow))
				} else {
					UpdateOffsets(data, pre-1, int(currRow)+1)
				}
			}
			if int(currRow) == len(data.Lengths)-1 {
				data.Data = data.Data[:data.Offsets[currRow]]
				data.Lengths = data.Lengths[:currRow]
				data.Offsets = data.Offsets[:currRow]
			} else {
				data.Data = append(data.Data[:data.Offsets[currRow]], data.Data[data.Offsets[currRow+1]:]...)
				data.Lengths = append(data.Lengths[:currRow], data.Lengths[currRow+1:]...)
				data.Offsets = append(data.Offsets[:currRow], data.Offsets[currRow+1:]...)
			}
			if nspIterator != nil {
				var n uint64
				if nspIterator.HasNext() {
					for nspIterator.HasNext() {
						n = nspIterator.PeekNext()
						if uint32(n) < row {
							nspIterator.Next()
						} else {
							if uint32(n) == row {
								nspIterator.Next()
							}
							break
						}
						np.Add(n - uint64(deleted))
					}
				}
			}
			deleted++
			pre = int(currRow)
		}
		if nspIterator != nil {
			for nspIterator.HasNext() {
				n := nspIterator.Next()
				np.Add(n - uint64(deleted))
			}
		}
		if pre != -1 {
			UpdateOffsets(data, pre-1, len(data.Lengths)-1)
		}
	}
	vec.Nsp.Np = np
	return vec
}

func ApplyUpdateToVector(vec *gvec.Vector, mask *roaring.Bitmap, vals map[uint32]any) *gvec.Vector {
	if mask == nil || mask.IsEmpty() {
		return vec
	}
	iterator := mask.Iterator()
	col := vec.Col
	switch vec.Typ.Oid {
	case types.Type_BOOL, types.Type_INT8, types.Type_INT16, types.Type_INT32, types.Type_INT64,
		types.Type_UINT8, types.Type_UINT16, types.Type_UINT32, types.Type_UINT64,
		types.Type_DECIMAL64, types.Type_DECIMAL128, types.Type_FLOAT32, types.Type_FLOAT64,
		types.Type_DATE, types.Type_DATETIME, types.Type_TIMESTAMP:
		for iterator.HasNext() {
			row := iterator.Next()
			UpdateValue(vec, row, vals[row])
		}
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		data := col.(*types.Bytes)
		pre := -1
		for iterator.HasNext() {
			row := iterator.Next()
			v := vals[row]
			if _, ok := v.(types.Null); ok {
				if vec.Nsp.Np == nil {
					vec.Nsp.Np = roaring64.BitmapOf(uint64(row))
				} else {
					vec.Nsp.Np.Add(uint64(row))
				}
				continue
			}
			if pre != -1 {
				UpdateOffsets(data, pre, int(row))
			}
			UpdateValue(vec, row, vals[row])
			pre = int(row)
		}
		if pre != -1 {
			UpdateOffsets(data, pre, len(data.Lengths)-1)
		}
	}
	return vec
}

func ApplyUpdateToIVector(vec vector.IVector, mask *roaring.Bitmap, vals map[uint32]any) vector.IVector {
	if mask == nil || mask.IsEmpty() {
		return vec
	}
	updateIterator := mask.Iterator()
	switch vec.GetType() {
	case container.StdVec:
		if vec.IsReadonly() {
			vec2 := vec
			vec = vec.(*vector.StdVector).Clone()
			vec.ResetReadonly()
			_ = vec2.Close()
		}
		for updateIterator.HasNext() {
			rowIdx := updateIterator.Next()
			err := vec.SetValue(int(rowIdx), vals[rowIdx])
			if err != nil {
				panic(err)
			}
		}
	case container.StrVec:
		strVec := vec.(*vector.StrVector)
		data := strVec.Data
		pre := -1
		if vec.IsReadonly() {
			strVec2 := vector.NewEmptyStrVector()
			if strVec.VMask != nil && strVec.VMask.Np != nil {
				strVec2.VMask.Np = strVec.VMask.Np.Clone()
			}
			pos2 := 0
			strVec2.Data.Lengths = make([]uint32, len(data.Lengths))
			strVec2.Data.Offsets = make([]uint32, len(data.Offsets))
			for updateIterator.HasNext() {
				row := updateIterator.Next()
				val := vals[row].([]byte)

				preOffset := int(data.Offsets[pre+1])
				length := int(data.Offsets[row] - uint32(preOffset))
				strVec2.Data.Data = append(strVec2.Data.Data, make([]byte, length+len(val))...)
				copy(strVec2.Data.Data[pos2:pos2+length], data.Data[preOffset:preOffset+length])
				pos2 += length

				for i := pre + 1; i < int(row); i++ {
					strVec2.Data.Lengths[i] = data.Lengths[i]
					strVec2.Data.Offsets[i+1] = strVec2.Data.Offsets[i] + strVec2.Data.Lengths[i]
				}
				copy(strVec2.Data.Data[pos2:pos2+len(val)], val)
				pos2 += len(val)

				if strVec.VMask != nil && strVec.VMask.Np != nil && strVec.VMask.Np.Contains(uint64(row)) {
					strVec.VMask.Np.Flip(uint64(row), uint64(row))
				}

				strVec2.Data.Lengths[row] = uint32(len(val))
				if int(row) != len(data.Offsets)-1 {
					strVec2.Data.Offsets[row+1] = strVec2.Data.Offsets[row] + strVec2.Data.Lengths[row]
				}
				pre = int(row)
			}
			preOffset := int(data.Offsets[pre] + data.Lengths[pre])
			row := len(data.Offsets)
			length := int(len(data.Data) - preOffset)
			strVec2.Data.Data = append(strVec2.Data.Data, make([]byte, length)...)
			copy(strVec2.Data.Data[pos2:pos2+length], data.Data[preOffset:preOffset+length])

			for i := pre + 1; i < int(row); i++ {
				strVec2.Data.Lengths[i] = data.Lengths[i]
				if i != len(data.Offsets)-1 {
					strVec2.Data.Offsets[i+1] = strVec2.Data.Offsets[i] + strVec2.Data.Lengths[i]
				}
			}
			strVec2.StatMask = strVec.StatMask
			strVec2.Type = types.Type{
				Oid:   strVec.Type.Oid,
				Size:  strVec.Type.Size,
				Width: strVec.Type.Width,
			}
			vec = strVec2
			vec.ResetReadonly()
		} else {
			for updateIterator.HasNext() {
				row := updateIterator.Next()
				if pre != -1 {
					UpdateOffsets(data, pre, int(row))
				}
				val := vals[row].([]byte)
				suffix := data.Data[data.Offsets[row]+data.Lengths[row]:]
				data.Lengths[row] = uint32(len(val))
				val = append(val, suffix...)
				data.Data = append(data.Data[:data.Offsets[row]], val...)
				pre = int(row)
				if strVec.VMask != nil && strVec.VMask.Np != nil && strVec.VMask.Np.Contains(uint64(row)) {
					strVec.VMask.Np.Flip(uint64(row), uint64(row))
				}
			}
			if pre != -1 {
				UpdateOffsets(data, pre, len(data.Offsets)-1)
			}
		}
	default:
		panic("not support")
	}
	return vec
}

type deleteRange struct {
	pos     uint32
	deleted uint32
}

func findDeleteRange(pos uint32, ranges []*deleteRange) *deleteRange {
	left, right := 0, len(ranges)-1
	var mid int
	for left <= right {
		mid = (left + right) / 2
		if ranges[mid].pos < pos {
			left = mid + 1
		} else if ranges[mid].pos > pos {
			right = mid - 1
		} else {
			break
		}
	}
	if mid == 0 && ranges[mid].pos < pos {
		mid = mid + 1
	}
	// logutil.Infof("pos=%d, mid=%d, range.pos=%d,range.deleted=%d", pos, mid, ranges[mid].pos, ranges[mid].deleted)
	return ranges[mid]
}

func ShuffleByDeletes(origMask *roaring.Bitmap, origVals map[uint32]any, deletes *roaring.Bitmap) (*roaring.Bitmap, map[uint32]any, *roaring.Bitmap) {
	if deletes == nil {
		return origMask, origVals, deletes
	}
	destDelets := roaring.New()
	ranges := make([]*deleteRange, 0, 10)
	deletesIt := deletes.Iterator()
	deletedCnt := uint32(0)
	for deletesIt.HasNext() {
		pos := deletesIt.Next()
		destDelets.Add(pos - deletedCnt)
		ranges = append(ranges, &deleteRange{pos: pos, deleted: deletedCnt})
		deletedCnt++
	}
	if origMask == nil || origMask.IsEmpty() {
		return origMask, origVals, destDelets
	}

	ranges = append(ranges, &deleteRange{pos: math.MaxUint32, deleted: deletedCnt})
	destMask := roaring.New()
	destVals := make(map[uint32]any)
	origIt := origMask.Iterator()
	for origIt.HasNext() {
		pos := origIt.Next()
		drange := findDeleteRange(pos, ranges)
		destMask.Add(pos - drange.deleted)
		destVals[pos-drange.deleted] = origVals[pos]
	}
	// for i, r := range ranges {
	// 	logutil.Infof("%d range.pos=%d,range.deleted=%d", i, r.pos, r.deleted)
	// }
	return destMask, destVals, destDelets
}

func GetOffsetWithFunc[T any](
	vals []T,
	val T,
	compare func(a, b T) int64,
	skipmask *roaring.Bitmap,
) (offset int, exist bool) {
	start, end := 0, len(vals)-1
	var mid int
	for start <= end {
		mid = (start + end) / 2
		res := compare(vals[mid], val)
		if res > 0 {
			end = mid - 1
		} else if res < 0 {
			start = mid + 1
		} else {
			if skipmask != nil && skipmask.Contains(uint32(mid)) {
				return
			}
			offset = mid
			exist = true
			return
		}
	}
	return
}

func GetOffsetOfOrdered[T types.OrderedT](vs, v any, skipmask *roaring.Bitmap) (offset int, exist bool) {
	column := vs.([]T)
	val := v.(T)
	start, end := 0, len(column)-1
	var mid int
	for start <= end {
		mid = (start + end) / 2
		if column[mid] > val {
			end = mid - 1
		} else if column[mid] < val {
			start = mid + 1
		} else {
			if skipmask != nil && skipmask.Contains(uint32(mid)) {
				return
			}
			offset = mid
			exist = true
			return
		}
	}
	return
}

func GetOffsetByVal(data containers.Vector, v any, skipmask *roaring.Bitmap) (offset int, exist bool) {
	switch data.GetType().Oid {
	case types.Type_BOOL:
		return GetOffsetWithFunc[bool](data.Slice().([]bool), v.(bool), CompareBool, skipmask)
	case types.Type_INT8:
		return GetOffsetOfOrdered[int8](data.Slice(), v, skipmask)
	case types.Type_INT16:
		return GetOffsetOfOrdered[int16](data.Slice(), v, skipmask)
	case types.Type_INT32:
		return GetOffsetOfOrdered[int32](data.Slice(), v, skipmask)
	case types.Type_INT64:
		return GetOffsetOfOrdered[int64](data.Slice(), v, skipmask)
	case types.Type_UINT8:
		return GetOffsetOfOrdered[uint8](data.Slice(), v, skipmask)
	case types.Type_UINT16:
		return GetOffsetOfOrdered[uint16](data.Slice(), v, skipmask)
	case types.Type_UINT32:
		return GetOffsetOfOrdered[uint32](data.Slice(), v, skipmask)
	case types.Type_UINT64:
		return GetOffsetOfOrdered[uint64](data.Slice(), v, skipmask)
	case types.Type_FLOAT32:
		return GetOffsetOfOrdered[float32](data.Slice(), v, skipmask)
	case types.Type_FLOAT64:
		return GetOffsetOfOrdered[float64](data.Slice(), v, skipmask)
	case types.Type_DATE:
		return GetOffsetOfOrdered[types.Date](data.Slice(), v, skipmask)
	case types.Type_DATETIME:
		return GetOffsetOfOrdered[types.Datetime](data.Slice(), v, skipmask)
	case types.Type_TIMESTAMP:
		return GetOffsetOfOrdered[types.Timestamp](data.Slice(), v, skipmask)
	case types.Type_DECIMAL64:
		return GetOffsetOfOrdered[types.Decimal64](data.Slice(), v, skipmask)
	case types.Type_DECIMAL128:
		return GetOffsetWithFunc[types.Decimal128](
			data.Slice().([]types.Decimal128),
			v.(types.Decimal128),
			types.CompareDecimal128Decimal128Aligned,
			skipmask)
	case types.Type_CHAR, types.Type_VARCHAR:
		// column := data.Slice().(*containers.Bytes)
		val := v.([]byte)
		start, end := 0, data.Length()-1
		var mid int
		for start <= end {
			mid = (start + end) / 2
			res := bytes.Compare(data.Get(mid).([]byte), val)
			if res > 0 {
				end = mid - 1
			} else if res < 0 {
				start = mid + 1
			} else {
				if skipmask != nil && skipmask.Contains(uint32(mid)) {
					return
				}
				offset = mid
				exist = true
				return
			}
		}
		return
	default:
		panic("unsupported type")
	}
}
