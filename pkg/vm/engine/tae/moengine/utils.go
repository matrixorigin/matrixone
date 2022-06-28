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

package moengine

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

func MockVec(typ types.Type, rows int, offset int) *vector.Vector {
	vec := vector.New(typ)
	switch typ.Oid {
	case types.Type_BOOL:
		data := make([]bool, 0)
		for i := 0; i < rows; i++ {
			if i%2 == 0 {
				data = append(data, true)
			} else {
				data = append(data, false)
			}
		}
		_ = vector.Append(vec, data)
	case types.Type_INT8:
		data := make([]int8, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int8(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_INT16:
		data := make([]int16, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int16(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_INT32:
		data := make([]int32, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int32(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_INT64:
		data := make([]int64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int64(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_UINT8:
		data := make([]uint8, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint8(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_UINT16:
		data := make([]uint16, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint16(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_UINT32:
		data := make([]uint32, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint32(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_UINT64:
		data := make([]uint64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint64(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_FLOAT32:
		data := make([]float32, 0)
		for i := 0; i < rows; i++ {
			data = append(data, float32(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_FLOAT64:
		data := make([]float64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, float64(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_DECIMAL64:
		data := make([]types.Decimal64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Decimal64(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_DECIMAL128:
		data := make([]types.Decimal128, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Decimal128{Lo: int64(i + offset)})
		}
		_ = vector.Append(vec, data)
	case types.Type_TIMESTAMP:
		data := make([]types.Timestamp, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Timestamp(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_DATE:
		data := make([]types.Date, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Date(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_DATETIME:
		data := make([]types.Datetime, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Datetime(i+offset))
		}
		_ = vector.Append(vec, data)
	case types.Type_CHAR, types.Type_VARCHAR:
		data := make([][]byte, 0)
		for i := 0; i < rows; i++ {
			data = append(data, []byte(strconv.Itoa(i+offset)))
		}
		_ = vector.Append(vec, data)
	default:
		panic("not support")
	}
	return vec
}

/*func BatchWindow(bat *batch.Batch, start, end int) *batch.Batch {
	window := batch.New(true, bat.Attrs)
	window.Vecs = make([]*vector.Vector, len(bat.Vecs))
	for i := range window.Vecs {
		vec := vector.New(bat.Vecs[i].Typ)
		vector.Window(bat.Vecs[i], start, end, vec)
		window.Vecs[i] = vec
	}
	return window
}

func SplitBatch(bat *batch.Batch, cnt int) []*batch.Batch {
	if cnt == 1 {
		return []*batch.Batch{bat}
	}
	length := vector.Length(bat.Vecs[0])
	rows := length / cnt
	if length%cnt == 0 {
		bats := make([]*batch.Batch, 0, cnt)
		for i := 0; i < cnt; i++ {
			newBat := batch.New(true, bat.Attrs)
			for j := 0; j < len(bat.Vecs); j++ {
				window := vector.New(bat.Vecs[j].Typ)
				vector.Window(bat.Vecs[j], i*rows, (i+1)*rows, window)
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
	bats := make([]*batch.Batch, 0, cnt)
	for _, row := range rowArray {
		newBat := batch.New(true, bat.Attrs)
		for j := 0; j < len(bat.Vecs); j++ {
			window := vector.New(bat.Vecs[j].Typ)
			vector.Window(bat.Vecs[j], start, start+row, window)
			newBat.Vecs[j] = window
		}
		start += row
		bats = append(bats, newBat)
	}
	return bats
}*/

func GenericUpdateFixedValue[T any](vec *vector.Vector, row uint32, v any) {
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

func AppendFixedValue[T any](vec *vector.Vector, v any) {
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

func AppendValue(vec *vector.Vector, v any) {
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

func GetValue(col *vector.Vector, row uint32) any {
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
		//return vector.ErrVecTypeNotSupport
		panic(any("No Support"))
	}
}

func UpdateValue(col *vector.Vector, row uint32, val any) {
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

func ForEachValue(col *vector.Vector, reversed bool, op func(v any, row uint32) error) (err error) {
	if reversed {
		for i := vector.Length(col) - 1; i >= 0; i-- {
			v := GetValue(col, uint32(i))
			if err = op(v, uint32(i)); err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < vector.Length(col); i++ {
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

func BatchWindow(bat *batch.Batch, start, end int) *batch.Batch {
	window := batch.New(true, bat.Attrs)
	window.Vecs = make([]*vector.Vector, len(bat.Vecs))
	for i := range window.Vecs {
		vec := vector.New(bat.Vecs[i].Typ)
		vector.Window(bat.Vecs[i], start, end, vec)
		window.Vecs[i] = vec
	}
	return window
}

func SplitBatch(bat *batch.Batch, cnt int) []*batch.Batch {
	if cnt == 1 {
		return []*batch.Batch{bat}
	}
	length := vector.Length(bat.Vecs[0])
	rows := length / cnt
	if length%cnt == 0 {
		bats := make([]*batch.Batch, 0, cnt)
		for i := 0; i < cnt; i++ {
			newBat := batch.New(true, bat.Attrs)
			for j := 0; j < len(bat.Vecs); j++ {
				window := vector.New(bat.Vecs[j].Typ)
				vector.Window(bat.Vecs[j], i*rows, (i+1)*rows, window)
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
	bats := make([]*batch.Batch, 0, cnt)
	for _, row := range rowArray {
		newBat := batch.New(true, bat.Attrs)
		for j := 0; j < len(bat.Vecs); j++ {
			window := vector.New(bat.Vecs[j].Typ)
			vector.Window(bat.Vecs[j], start, start+row, window)
			newBat.Vecs[j] = window
		}
		start += row
		bats = append(bats, newBat)
	}
	return bats
}

func ApplyDeleteToVector(vec *vector.Vector, deletes *roaring.Bitmap) *vector.Vector {
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
		vec.Col = compute.InplaceDeleteRows(vec.Col, deletesIterator)
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

func ApplyUpdateToVector(vec *vector.Vector, mask *roaring.Bitmap, vals map[uint32]any) *vector.Vector {
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

func MOToVector(v *vector.Vector, nullable bool) containers.Vector {
	vec := containers.MakeVector(v.Typ, nullable)
	bs := containers.NewBytes()
	switch v.Typ.Oid {
	case types.Type_BOOL:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]bool), 1)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_INT8:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]int8), 1)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_INT16:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]int16), 2)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_INT32:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]int32), 4)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_INT64:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]int64), 8)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_UINT8:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]uint8), 1)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_UINT16:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]uint16), 2)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_UINT32:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]uint32), 4)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_UINT64:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]uint64), 8)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_FLOAT32:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]float32), 4)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_FLOAT64:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]float64), 8)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_DATE:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Date), 4)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_DATETIME:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Datetime), 8)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_TIMESTAMP:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Timestamp), 8)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_DECIMAL64:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Decimal64), 8)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_DECIMAL128:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Decimal128), 16)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		vbs := v.Col.(*types.Bytes)
		bs.Data = vbs.Data
		bs.Offset = vbs.Offsets
		bs.Length = vbs.Lengths
		vec.ResetWithData(bs, v.Nsp.Np)
	default:
		panic(any(fmt.Errorf("%s not supported", v.Typ.String())))
	}
	return vec
}

func MOToVectorTmp(v *vector.Vector, nullable bool) containers.Vector {
	vec := containers.MakeVector(v.Typ, nullable)
	bs := containers.NewBytes()
	switch v.Typ.Oid {
	case types.Type_BOOL:
		if v.Col == nil || len(v.Col.([]bool)) == 0 {
			bs.Data = make([]byte, v.Length)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]bool), 1)
		}
	case types.Type_INT8:
		if v.Col == nil || len(v.Col.([]int8)) == 0 {
			bs.Data = make([]byte, v.Length)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]int8), 1)
		}
	case types.Type_INT16:
		if v.Col == nil || len(v.Col.([]int16)) == 0 {
			bs.Data = make([]byte, v.Length*2)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]int16), 2)
		}
	case types.Type_INT32:
		if v.Col == nil || len(v.Col.([]int32)) == 0 {
			bs.Data = make([]byte, v.Length*4)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]int32), 4)
		}
	case types.Type_INT64:
		if v.Col == nil || len(v.Col.([]int64)) == 0 {
			bs.Data = make([]byte, v.Length*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]int64), 8)
		}
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_UINT8:
		if v.Col == nil || len(v.Col.([]uint8)) == 0 {
			bs.Data = make([]byte, v.Length)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]uint8), 1)
		}
	case types.Type_UINT16:
		if v.Col == nil || len(v.Col.([]uint16)) == 0 {
			bs.Data = make([]byte, v.Length*2)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]uint16), 2)
		}
	case types.Type_UINT32:
		if v.Col == nil || len(v.Col.([]uint32)) == 0 {
			bs.Data = make([]byte, v.Length*4)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]uint32), 4)
		}
	case types.Type_UINT64:
		if v.Col == nil || len(v.Col.([]uint64)) == 0 {
			bs.Data = make([]byte, v.Length*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]uint64), 8)
		}
	case types.Type_FLOAT32:
		if v.Col == nil || len(v.Col.([]float32)) == 0 {
			bs.Data = make([]byte, v.Length*4)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]float32), 4)
		}
	case types.Type_FLOAT64:
		if v.Col == nil || len(v.Col.([]float64)) == 0 {
			bs.Data = make([]byte, v.Length*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]float64), 8)
		}
	case types.Type_DATE:
		if v.Col == nil || len(v.Col.([]types.Date)) == 0 {
			bs.Data = make([]byte, v.Length*4)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Date), 4)
		}
	case types.Type_DATETIME:
		if v.Col == nil || len(v.Col.([]types.Datetime)) == 0 {
			bs.Data = make([]byte, v.Length*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Datetime), 8)
		}
	case types.Type_TIMESTAMP:
		if v.Col == nil || len(v.Col.([]types.Timestamp)) == 0 {
			bs.Data = make([]byte, v.Length*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Timestamp), 8)
		}
	case types.Type_DECIMAL64:
		if v.Col == nil || len(v.Col.([]types.Decimal64)) == 0 {
			bs.Data = make([]byte, v.Length*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Decimal64), 8)
		}
	case types.Type_DECIMAL128:
		if v.Col == nil || len(v.Col.([]types.Decimal128)) == 0 {
			bs.Data = make([]byte, v.Length*16)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Decimal128), 16)
		}
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		vbs := v.Col.(*types.Bytes)
		bs.Data = vbs.Data
		bs.Offset = vbs.Offsets
		bs.Length = vbs.Lengths
	case types.Type_ANY:
		bs.Data = make([]byte, 0)
	default:
		panic(any(fmt.Errorf("%s not supported", v.Typ.String())))
	}
	vec.ResetWithData(bs, v.Nsp.Np)
	return vec
}

func CopyToMoVector(vec containers.Vector) *vector.Vector {
	mov := vector.New(vec.GetType())
	w := new(bytes.Buffer)
	_, _ = w.Write(types.EncodeType(vec.GetType()))
	if vec.HasNull() {
		var nullBuf []byte
		nullBuf, _ = vec.NullMask().ToBytes()
		_, _ = w.Write(types.EncodeFixed(uint32(len(nullBuf))))
		_, _ = w.Write(nullBuf)
	} else {
		_, _ = w.Write(types.EncodeFixed(uint32(0)))
	}
	switch vec.GetType().Oid {
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		_, _ = w.Write(types.EncodeFixed(uint32(vec.Length())))
		if vec.Length() > 0 {
			bs := vec.Bytes()
			_, _ = w.Write(bs.LengthBuf())
			_, _ = w.Write(bs.DataBuf())
		}
	default:
		bs := vec.Data()
		_, _ = w.Write(bs)
	}
	if err := mov.Read(w.Bytes()); err != nil {
		panic(err)
	}
	return mov
}

func VectorsToMO(vec containers.Vector) *vector.Vector {
	mov := vector.New(vec.GetType())
	data := vec.Data()
	typ := vec.GetType()
	mov.Typ = typ
	mov.Or = true
	if vec.HasNull() {
		mov.Nsp.Np = vec.NullMask()
	}
	mov.Data = data
	switch vec.GetType().Oid {
	case types.Type_BOOL:
		mov.Col = encoding.DecodeBoolSlice(data)
	case types.Type_INT8:
		mov.Col = encoding.DecodeInt8Slice(data)
	case types.Type_INT16:
		mov.Col = encoding.DecodeInt16Slice(data)
	case types.Type_INT32:
		mov.Col = encoding.DecodeInt32Slice(data)
	case types.Type_INT64:
		mov.Col = encoding.DecodeInt64Slice(data)
	case types.Type_UINT8:
		mov.Col = encoding.DecodeUint8Slice(data)
	case types.Type_UINT16:
		mov.Col = encoding.DecodeUint16Slice(data)
	case types.Type_UINT32:
		mov.Col = encoding.DecodeUint32Slice(data)
	case types.Type_UINT64:
		mov.Col = encoding.DecodeUint64Slice(data)
	case types.Type_FLOAT32:
		mov.Col = encoding.DecodeFloat32Slice(data)
	case types.Type_FLOAT64:
		mov.Col = encoding.DecodeFloat64Slice(data)
	case types.Type_DATE:
		mov.Col = encoding.DecodeDateSlice(data)
	case types.Type_DATETIME:
		mov.Col = encoding.DecodeDatetimeSlice(data)
	case types.Type_TIMESTAMP:
		mov.Col = encoding.DecodeTimestampSlice(data)
	case types.Type_DECIMAL64:
		mov.Col = encoding.DecodeDecimal64Slice(data)
	case types.Type_DECIMAL128:
		mov.Col = encoding.DecodeDecimal128Slice(data)
	case types.Type_TUPLE:
		cnt := encoding.DecodeInt32(data)
		if cnt == 0 {
			break
		}
		if err := encoding.Decode(data, &mov.Col); err != nil {
			panic(any(err))
		}
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		Col := mov.Col.(*types.Bytes)
		Col.Reset()
		bs := vec.Bytes()
		Col.Offsets = make([]uint32, vec.Length())
		if vec.Length() > 0 {
			Col.Lengths = bs.Length
			Col.Offsets = bs.Offset
			Col.Data = bs.Data
		}
	default:
		panic(any(fmt.Errorf("%s not supported", vec.GetType().String())))
	}
	return mov
}

func CopyToMoVectors(vecs []containers.Vector) []*vector.Vector {
	movecs := make([]*vector.Vector, len(vecs))
	for i := range movecs {
		movecs[i] = CopyToMoVector(vecs[i])
	}
	return movecs
}

func MOToVectors(movecs []*vector.Vector, nullables []bool) []containers.Vector {
	vecs := make([]containers.Vector, len(movecs))
	for i := range movecs {
		vecs[i] = MOToVector(movecs[i], nullables[i])
	}
	return vecs
}
