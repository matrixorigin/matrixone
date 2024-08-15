// Copyright 2022 Matrix Origin
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

package containers

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
)

// ### Shallow copy Functions

func ToCNBatch(tnBat *Batch) *batch.Batch {
	cnBat := batch.New(true, tnBat.Attrs)
	for i, vec := range tnBat.Vecs {
		cnBat.Vecs[i] = vec.GetDownstreamVector()
	}
	cnBat.SetRowCount(tnBat.Length())
	return cnBat
}

func ToTNBatch(cnBat *batch.Batch, mp *mpool.MPool) *Batch {
	tnBat := NewEmptyBatch()
	for i, vec := range cnBat.Vecs {
		v := ToTNVector(vec, mp)
		tnBat.AddVector(cnBat.Attrs[i], v)
	}
	return tnBat
}

func ToTNVector(v *movec.Vector, mp *mpool.MPool) Vector {
	vec := MakeVector(*v.GetType(), mp)
	vec.setDownstreamVector(v)
	return vec
}

func CloneVector(src *movec.Vector, mp *mpool.MPool, vp *VectorPool) (Vector, error) {
	var vec Vector
	if vp != nil {
		vec = vp.GetVector(src.GetType())
		mp = vp.GetMPool()
		if err := src.CloneWindowTo(
			vec.GetDownstreamVector(), 0, src.Length(), mp,
		); err != nil {
			vec.Close()
			return nil, err
		}
	} else {
		vec = MakeVector(*src.GetType(), mp)
		if v, err := src.CloneWindow(0, src.Length(), mp); err != nil {
			vec.Close()
			return nil, err
		} else {
			vec.setDownstreamVector(v)
		}
	}
	return vec, nil
}

// ### Get Functions

// getNonNullValue Please don't merge it with GetValue(). Used in Vector for getting NonNullValue.
func getNonNullValue(col *movec.Vector, row uint32) any {

	switch col.GetType().Oid {
	case types.T_bool:
		return movec.GetFixedAt[bool](col, int(row))
	case types.T_bit:
		return movec.GetFixedAt[uint64](col, int(row))
	case types.T_int8:
		return movec.GetFixedAt[int8](col, int(row))
	case types.T_int16:
		return movec.GetFixedAt[int16](col, int(row))
	case types.T_int32:
		return movec.GetFixedAt[int32](col, int(row))
	case types.T_int64:
		return movec.GetFixedAt[int64](col, int(row))
	case types.T_uint8:
		return movec.GetFixedAt[uint8](col, int(row))
	case types.T_uint16:
		return movec.GetFixedAt[uint16](col, int(row))
	case types.T_uint32:
		return movec.GetFixedAt[uint32](col, int(row))
	case types.T_uint64:
		return movec.GetFixedAt[uint64](col, int(row))
	case types.T_decimal64:
		return movec.GetFixedAt[types.Decimal64](col, int(row))
	case types.T_decimal128:
		return movec.GetFixedAt[types.Decimal128](col, int(row))
	case types.T_uuid:
		return movec.GetFixedAt[types.Uuid](col, int(row))
	case types.T_float32:
		return movec.GetFixedAt[float32](col, int(row))
	case types.T_float64:
		return movec.GetFixedAt[float64](col, int(row))
	case types.T_date:
		return movec.GetFixedAt[types.Date](col, int(row))
	case types.T_time:
		return movec.GetFixedAt[types.Time](col, int(row))
	case types.T_datetime:
		return movec.GetFixedAt[types.Datetime](col, int(row))
	case types.T_timestamp:
		return movec.GetFixedAt[types.Timestamp](col, int(row))
	case types.T_enum:
		return movec.GetFixedAt[types.Enum](col, int(row))
	case types.T_TS:
		return movec.GetFixedAt[types.TS](col, int(row))
	case types.T_Rowid:
		return movec.GetFixedAt[types.Rowid](col, int(row))
	case types.T_Blockid:
		return movec.GetFixedAt[types.Blockid](col, int(row))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		return col.GetBytesAt(int(row))
	default:
		//return vector.ErrVecTypeNotSupport
		panic(any("No Support"))
	}
}

// ### Update Function

func GenericUpdateFixedValue[T types.FixedSizeT](
	vec *movec.Vector, row uint32, v any, isNull bool, _ *mpool.MPool,
) {
	if isNull {
		nulls.Add(vec.GetNulls(), uint64(row))
	} else {
		err := movec.SetFixedAt(vec, int(row), v.(T))
		if err != nil {
			panic(err)
		}
		if vec.GetNulls().Contains(uint64(row)) {
			vec.GetNulls().Unset(uint64(row))
		}
	}
}

func GenericUpdateBytes(
	vec *movec.Vector, row uint32, v any, isNull bool, mp *mpool.MPool,
) {
	if isNull {
		nulls.Add(vec.GetNulls(), uint64(row))
	} else {
		err := movec.SetBytesAt(vec, int(row), v.([]byte), mp)
		if err != nil {
			panic(err)
		}
		if vec.GetNulls().Contains(uint64(row)) {
			vec.GetNulls().Unset(uint64(row))
		}
	}
}

func UpdateValue(col *movec.Vector, row uint32, val any, isNull bool, mp *mpool.MPool) {
	switch col.GetType().Oid {
	case types.T_bool:
		GenericUpdateFixedValue[bool](col, row, val, isNull, mp)
	case types.T_bit:
		GenericUpdateFixedValue[uint64](col, row, val, isNull, mp)
	case types.T_int8:
		GenericUpdateFixedValue[int8](col, row, val, isNull, mp)
	case types.T_int16:
		GenericUpdateFixedValue[int16](col, row, val, isNull, mp)
	case types.T_int32:
		GenericUpdateFixedValue[int32](col, row, val, isNull, mp)
	case types.T_int64:
		GenericUpdateFixedValue[int64](col, row, val, isNull, mp)
	case types.T_uint8:
		GenericUpdateFixedValue[uint8](col, row, val, isNull, mp)
	case types.T_uint16:
		GenericUpdateFixedValue[uint16](col, row, val, isNull, mp)
	case types.T_uint32:
		GenericUpdateFixedValue[uint32](col, row, val, isNull, mp)
	case types.T_uint64:
		GenericUpdateFixedValue[uint64](col, row, val, isNull, mp)
	case types.T_decimal64:
		GenericUpdateFixedValue[types.Decimal64](col, row, val, isNull, mp)
	case types.T_decimal128:
		GenericUpdateFixedValue[types.Decimal128](col, row, val, isNull, mp)
	case types.T_float32:
		GenericUpdateFixedValue[float32](col, row, val, isNull, mp)
	case types.T_float64:
		GenericUpdateFixedValue[float64](col, row, val, isNull, mp)
	case types.T_date:
		GenericUpdateFixedValue[types.Date](col, row, val, isNull, mp)
	case types.T_time:
		GenericUpdateFixedValue[types.Time](col, row, val, isNull, mp)
	case types.T_datetime:
		GenericUpdateFixedValue[types.Datetime](col, row, val, isNull, mp)
	case types.T_timestamp:
		GenericUpdateFixedValue[types.Timestamp](col, row, val, isNull, mp)
	case types.T_enum:
		GenericUpdateFixedValue[types.Enum](col, row, val, isNull, mp)
	case types.T_uuid:
		GenericUpdateFixedValue[types.Uuid](col, row, val, isNull, mp)
	case types.T_TS:
		GenericUpdateFixedValue[types.TS](col, row, val, isNull, mp)
	case types.T_Rowid:
		GenericUpdateFixedValue[types.Rowid](col, row, val, isNull, mp)
	case types.T_Blockid:
		GenericUpdateFixedValue[types.Blockid](col, row, val, isNull, mp)
	case types.T_varchar, types.T_char, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		GenericUpdateBytes(col, row, val, isNull, mp)
	default:
		panic(moerr.NewInternalErrorNoCtx("%v not supported", col.GetType()))
	}
}

// ### Only used in testcases

func SplitBatch(bat *batch.Batch, cnt int) []*batch.Batch {
	if cnt == 1 {
		return []*batch.Batch{bat}
	}
	length := bat.Vecs[0].Length()
	rows := length / cnt
	if length%cnt == 0 {
		bats := make([]*batch.Batch, 0, cnt)
		for i := 0; i < cnt; i++ {
			newBat := batch.New(true, bat.Attrs)
			for j := 0; j < len(bat.Vecs); j++ {
				window, _ := bat.Vecs[j].CloneWindow(i*rows, (i+1)*rows, nil)
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
			window, _ := bat.Vecs[j].CloneWindow(start, start+row, nil)
			newBat.Vecs[j] = window
		}
		start += row
		bats = append(bats, newBat)
	}
	return bats
}

func NewNonNullBatchWithSharedMemory(b *batch.Batch, mp *mpool.MPool) *Batch {
	bat := NewBatch()
	for i, attr := range b.Attrs {
		v := ToTNVector(b.Vecs[i], mp)
		bat.AddVector(attr, v)
	}
	return bat
}

func ForeachVector(vec Vector, op any, sel *nulls.Bitmap) (err error) {
	return ForeachVectorWindow(vec, 0, vec.Length(), op, nil, sel)
}

func ForeachVectorWindow(
	vec Vector,
	start, length int,
	op1 any,
	op2 ItOp,
	sel *nulls.Bitmap,
) (err error) {
	typ := vec.GetType()
	col := vec.GetDownstreamVector()
	if typ.IsVarlen() {
		var op func([]byte, bool, int) error
		if op1 != nil {
			op = op1.(func([]byte, bool, int) error)
		}
		return ForeachWindowVarlen(
			col,
			start,
			length,
			op,
			op2,
			sel)
	}
	switch typ.Oid {
	case types.T_bool:
		var op func(bool, bool, int) error
		if op1 != nil {
			op = op1.(func(bool, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_bit:
		var op func(uint64, bool, int) error
		if op1 != nil {
			op = op1.(func(uint64, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_int8:
		var op func(int8, bool, int) error
		if op1 != nil {
			op = op1.(func(int8, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_int16:
		var op func(int16, bool, int) error
		if op1 != nil {
			op = op1.(func(int16, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_int32:
		var op func(int32, bool, int) error
		if op1 != nil {
			op = op1.(func(int32, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_int64:
		var op func(int64, bool, int) error
		if op1 != nil {
			op = op1.(func(int64, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_uint8:
		var op func(uint8, bool, int) error
		if op1 != nil {
			op = op1.(func(uint8, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_uint16:
		var op func(uint16, bool, int) error
		if op1 != nil {
			op = op1.(func(uint16, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_uint32:
		var op func(uint32, bool, int) error
		if op1 != nil {
			op = op1.(func(uint32, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_uint64:
		var op func(uint64, bool, int) error
		if op1 != nil {
			op = op1.(func(uint64, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_decimal64:
		var op func(types.Decimal64, bool, int) error
		if op1 != nil {
			op = op1.(func(types.Decimal64, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_decimal128:
		var op func(types.Decimal128, bool, int) error
		if op1 != nil {
			op = op1.(func(types.Decimal128, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_decimal256:
		var op func(types.Decimal256, bool, int) error
		if op1 != nil {
			op = op1.(func(types.Decimal256, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_float32:
		var op func(float32, bool, int) error
		if op1 != nil {
			op = op1.(func(float32, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_float64:
		var op func(float64, bool, int) error
		if op1 != nil {
			op = op1.(func(float64, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_timestamp:
		var op func(types.Timestamp, bool, int) error
		if op1 != nil {
			op = op1.(func(types.Timestamp, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_date:
		var op func(types.Date, bool, int) error
		if op1 != nil {
			op = op1.(func(types.Date, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_time:
		var op func(types.Time, bool, int) error
		if op1 != nil {
			op = op1.(func(types.Time, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_datetime:
		var op func(types.Datetime, bool, int) error
		if op1 != nil {
			op = op1.(func(types.Datetime, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_enum:
		var op func(types.Enum, bool, int) error
		if op1 != nil {
			op = op1.(func(types.Enum, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_TS:
		var op func(types.TS, bool, int) error
		if op1 != nil {
			op = op1.(func(types.TS, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_Blockid:
		var op func(types.Blockid, bool, int) error
		if op1 != nil {
			op = op1.(func(types.Blockid, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_uuid:
		var op func(types.Uuid, bool, int) error
		if op1 != nil {
			op = op1.(func(types.Uuid, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	case types.T_Rowid:
		var op func(types.Rowid, bool, int) error
		if op1 != nil {
			op = op1.(func(types.Rowid, bool, int) error)
		}
		return ForeachWindowFixed(
			col,
			start,
			length,
			op,
			op2,
			sel)
	default:
		panic(fmt.Sprintf("unsupported type: %s", typ.String()))
	}
}

func ForeachWindowBytes(
	vec *movec.Vector,
	start, length int,
	op ItOpT[[]byte],
	sels *nulls.Bitmap,
) (err error) {
	typ := vec.GetType()
	if typ.IsVarlen() {
		return ForeachWindowVarlen(vec, start, length, op, nil, sels)
	}
	tsize := typ.TypeSize()
	data := vec.UnsafeGetRawData()[start*tsize : (start+length)*tsize]
	if sels.IsEmpty() {
		for i := 0; i < length; i++ {
			if err = op(data[i*tsize:(i+1)*tsize], vec.IsNull(uint64(i+start)), i+start); err != nil {
				break
			}
		}
	} else {
		end := start + length
		it := sels.GetBitmap().Iterator()
		for it.HasNext() {
			idx := uint32(it.Next())
			if int(idx) < start {
				continue
			} else if int(idx) >= end {
				break
			}
			i := int(idx)
			if err = op(data[i*tsize:(i+1)*tsize], vec.IsNull(uint64(i)), i); err != nil {
				break
			}
		}

	}
	return
}

func ForeachWindowFixed[T any](
	vec *movec.Vector,
	start, length int,
	op ItOpT[T],
	opAny ItOp,
	sels *nulls.Bitmap,
) (err error) {
	if vec.IsConst() {
		var v T
		isnull := false
		if vec.IsConstNull() {
			isnull = true
		} else {
			v = movec.GetFixedAt[T](vec, 0)
		}
		if sels.IsEmpty() {
			for i := 0; i < length; i++ {
				if op != nil {
					if err = op(v, isnull, i+start); err != nil {
						break
					}
				}
				if opAny != nil {
					if err = opAny(v, isnull, i+start); err != nil {
						break
					}
				}
			}
		} else {
			end := start + length
			it := sels.GetBitmap().Iterator()
			for it.HasNext() {
				idx := uint32(it.Next())
				if int(idx) < start {
					continue
				} else if int(idx) >= end {
					break
				}
				if op != nil {
					if err = op(v, isnull, int(idx)); err != nil {
						break
					}
				}
				if opAny != nil {
					if err = opAny(v, isnull, int(idx)); err != nil {
						break
					}
				}
			}
		}

		return
	}
	slice := movec.MustFixedCol[T](vec)[start : start+length]
	if sels.IsEmpty() {
		for i, v := range slice {
			if op != nil {
				if err = op(v, vec.IsNull(uint64(i+start)), i+start); err != nil {
					break
				}
			}
			if opAny != nil {
				if err = opAny(v, vec.IsNull(uint64(i+start)), i+start); err != nil {
					break
				}
			}
		}
	} else {
		end := start + length
		it := sels.GetBitmap().Iterator()
		for it.HasNext() {
			idx := uint32(it.Next())
			if int(idx) < start {
				continue
			} else if int(idx) >= end {
				break
			}
			v := slice[int(idx)-start]
			if op != nil {
				if err = op(v, vec.IsNull(uint64(idx)), int(idx)); err != nil {
					break
				}
			}
			if opAny != nil {
				if err = opAny(v, vec.IsNull(uint64(idx)), int(idx)); err != nil {
					break
				}
			}
		}
	}
	return
}

func ForeachWindowVarlen(
	vec *movec.Vector,
	start, length int,
	op ItOpT[[]byte],
	opAny ItOp,
	sels *nulls.Bitmap,
) (err error) {
	if vec.IsConst() {
		var v []byte
		isnull := false
		if vec.IsConstNull() {
			isnull = true
		} else {
			v = vec.GetBytesAt(0)
		}
		for i := 0; i < length; i++ {
			if op != nil {
				if err = op(v, isnull, i+start); err != nil {
					break
				}
			}
			if opAny != nil {
				if err = opAny(v, isnull, i+start); err != nil {
					break
				}
			}
		}
		return
	}
	slice, area := movec.MustVarlenaRawData(vec)
	slice = slice[start : start+length]
	if sels.IsEmpty() {
		for i := range slice {
			var val []byte
			isNull := vec.IsNull(uint64(i + start))
			if !isNull {
				val = slice[i].GetByteSlice(area)
			}
			if op != nil {
				if err = op(val, isNull, i+start); err != nil {
					break
				}
			}
			if opAny != nil {
				if err = opAny(val, isNull, i+start); err != nil {
					break
				}
			}
		}
	} else {
		end := start + length
		it := sels.GetBitmap().Iterator()
		for it.HasNext() {
			idx := uint32(it.Next())
			if int(idx) < start {
				continue
			} else if int(idx) >= end {
				break
			}
			v := slice[int(idx)-start]
			var val []byte
			isNull := vec.IsNull(uint64(idx))
			if !isNull {
				val = v.GetByteSlice(area)
			}
			if op != nil {
				if err = op(val, isNull, int(idx)); err != nil {
					break
				}
			}
			if opAny != nil {
				if err = opAny(val, isNull, int(idx)); err != nil {
					break
				}
			}
		}
	}
	return
}

func MakeForeachVectorOp(t types.T, overloads map[types.T]any, args ...any) any {
	if t.FixedLength() < 0 {
		overload := overloads[t].(func(...any) func([]byte, bool, int) error)
		return overload(args...)
	}
	switch t {
	case types.T_bool:
		overload := overloads[t].(func(...any) func(bool, bool, int) error)
		return overload(args...)
	case types.T_int8:
		overload := overloads[t].(func(...any) func(int8, bool, int) error)
		return overload(args...)
	case types.T_int16:
		overload := overloads[t].(func(...any) func(int16, bool, int) error)
		return overload(args...)
	case types.T_int32:
		overload := overloads[t].(func(...any) func(int32, bool, int) error)
		return overload(args...)
	case types.T_int64:
		overload := overloads[t].(func(...any) func(int64, bool, int) error)
		return overload(args...)
	case types.T_uint8:
		overload := overloads[t].(func(...any) func(uint8, bool, int) error)
		return overload(args...)
	case types.T_uint16:
		overload := overloads[t].(func(...any) func(uint16, bool, int) error)
		return overload(args...)
	case types.T_uint32:
		overload := overloads[t].(func(...any) func(uint32, bool, int) error)
		return overload(args...)
	case types.T_uint64:
		overload := overloads[t].(func(...any) func(uint64, bool, int) error)
		return overload(args...)
	case types.T_float32:
		overload := overloads[t].(func(...any) func(float32, bool, int) error)
		return overload(args...)
	case types.T_float64:
		overload := overloads[t].(func(...any) func(float64, bool, int) error)
		return overload(args...)
	case types.T_decimal64:
		overload := overloads[t].(func(...any) func(types.Decimal64, bool, int) error)
		return overload(args...)
	case types.T_decimal128:
		overload := overloads[t].(func(...any) func(types.Decimal128, bool, int) error)
		return overload(args...)
	case types.T_decimal256:
		overload := overloads[t].(func(...any) func(types.Decimal256, bool, int) error)
		return overload(args...)
	case types.T_timestamp:
		overload := overloads[t].(func(...any) func(types.Timestamp, bool, int) error)
		return overload(args...)
	case types.T_time:
		overload := overloads[t].(func(...any) func(types.Time, bool, int) error)
		return overload(args...)
	case types.T_date:
		overload := overloads[t].(func(...any) func(types.Date, bool, int) error)
		return overload(args...)
	case types.T_datetime:
		overload := overloads[t].(func(...any) func(types.Datetime, bool, int) error)
		return overload(args...)
	case types.T_enum:
		overload := overloads[t].(func(...any) func(types.Enum, bool, int) error)
		return overload(args...)
	case types.T_TS:
		overload := overloads[t].(func(...any) func(types.TS, bool, int) error)
		return overload(args...)
	case types.T_Rowid:
		overload := overloads[t].(func(...any) func(types.Rowid, bool, int) error)
		return overload(args...)
	case types.T_Blockid:
		overload := overloads[t].(func(...any) func(types.Blockid, bool, int) error)
		return overload(args...)
	case types.T_uuid:
		overload := overloads[t].(func(...any) func(types.Uuid, bool, int) error)
		return overload(args...)
	}
	panic(fmt.Sprintf("unsupported type: %s", t.String()))
}
