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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	cnNulls "github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

// ### Shallow copy Functions

func UnmarshalToMoVec(vec Vector) *movec.Vector {
	return vec.getDownstreamVector()
}

func UnmarshalToMoVecs(vecs []Vector) []*movec.Vector {
	movecs := make([]*movec.Vector, len(vecs))
	for i := range movecs {
		movecs[i] = UnmarshalToMoVec(vecs[i])
	}
	return movecs
}

func NewVectorWithSharedMemory(v *movec.Vector) Vector {
	vec := MakeVector(*v.GetType())
	vec.setDownstreamVector(v)
	return vec
}

// ### Deep copy Functions

func CopyToMoVec(vec Vector) (mov *movec.Vector) {
	//TODO: can be updated if Dup(nil) is supported by CN vector.
	vecLen := vec.getDownstreamVector().Length()
	res, err := vec.getDownstreamVector().CloneWindow(0, vecLen, nil)
	if err != nil {
		panic(err)
	}
	return res
}

func CopyToMoVecs(vecs []Vector) []*movec.Vector {
	movecs := make([]*movec.Vector, len(vecs))
	for i := range movecs {
		movecs[i] = CopyToMoVec(vecs[i])
	}
	return movecs
}

func CopyToMoBatch(bat *Batch) *batch.Batch {
	ret := batch.New(true, bat.Attrs)
	for i := range bat.Vecs {
		ret.Vecs[i] = CopyToMoVec(bat.Vecs[i])
	}
	return ret
}

// ### Get Functions

// getNonNullValue Please don't merge it with GetValue(). Used in Vector for getting NonNullValue.
func getNonNullValue(col *movec.Vector, row uint32) any {

	switch col.GetType().Oid {
	case types.T_bool:
		return movec.GetFixedAt[bool](col, int(row))
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
	case types.T_TS:
		return movec.GetFixedAt[types.TS](col, int(row))
	case types.T_Rowid:
		return movec.GetFixedAt[types.Rowid](col, int(row))
	case types.T_Blockid:
		return movec.GetFixedAt[types.Blockid](col, int(row))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text:
		return col.GetBytesAt(int(row))
	default:
		//return vector.ErrVecTypeNotSupport
		panic(any("No Support"))
	}
}

// ### Update Function

var mockMp = common.DefaultAllocator

func GenericUpdateFixedValue[T types.FixedSizeT](vec *movec.Vector, row uint32, v any) {
	_, isNull := v.(types.Null)
	if isNull {
		cnNulls.Add(vec.GetNulls(), uint64(row))
	} else {
		err := movec.SetFixedAt(vec, int(row), v.(T))
		if err != nil {
			panic(err)
		}
		if vec.GetNulls().Np != nil && vec.GetNulls().Np.Contains(uint64(row)) {
			vec.GetNulls().Np.Remove(uint64(row))
		}
	}
}

func GenericUpdateBytes(vec *movec.Vector, row uint32, v any) {
	_, isNull := v.(types.Null)
	if isNull {
		cnNulls.Add(vec.GetNulls(), uint64(row))
	} else {
		err := movec.SetBytesAt(vec, int(row), v.([]byte), mockMp)
		if err != nil {
			panic(err)
		}
		if vec.GetNulls().Np != nil && vec.GetNulls().Np.Contains(uint64(row)) {
			vec.GetNulls().Np.Remove(uint64(row))
		}
	}
}

func UpdateValue(col *movec.Vector, row uint32, val any) {
	switch col.GetType().Oid {
	case types.T_bool:
		GenericUpdateFixedValue[bool](col, row, val)
	case types.T_int8:
		GenericUpdateFixedValue[int8](col, row, val)
	case types.T_int16:
		GenericUpdateFixedValue[int16](col, row, val)
	case types.T_int32:
		GenericUpdateFixedValue[int32](col, row, val)
	case types.T_int64:
		GenericUpdateFixedValue[int64](col, row, val)
	case types.T_uint8:
		GenericUpdateFixedValue[uint8](col, row, val)
	case types.T_uint16:
		GenericUpdateFixedValue[uint16](col, row, val)
	case types.T_uint32:
		GenericUpdateFixedValue[uint32](col, row, val)
	case types.T_uint64:
		GenericUpdateFixedValue[uint64](col, row, val)
	case types.T_decimal64:
		GenericUpdateFixedValue[types.Decimal64](col, row, val)
	case types.T_decimal128:
		GenericUpdateFixedValue[types.Decimal128](col, row, val)
	case types.T_float32:
		GenericUpdateFixedValue[float32](col, row, val)
	case types.T_float64:
		GenericUpdateFixedValue[float64](col, row, val)
	case types.T_date:
		GenericUpdateFixedValue[types.Date](col, row, val)
	case types.T_time:
		GenericUpdateFixedValue[types.Time](col, row, val)
	case types.T_datetime:
		GenericUpdateFixedValue[types.Datetime](col, row, val)
	case types.T_timestamp:
		GenericUpdateFixedValue[types.Timestamp](col, row, val)
	case types.T_uuid:
		GenericUpdateFixedValue[types.Uuid](col, row, val)
	case types.T_TS:
		GenericUpdateFixedValue[types.TS](col, row, val)
	case types.T_Rowid:
		GenericUpdateFixedValue[types.Rowid](col, row, val)
	case types.T_Blockid:
		GenericUpdateFixedValue[types.Blockid](col, row, val)
	case types.T_varchar, types.T_char, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		GenericUpdateBytes(col, row, val)
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

func NewNonNullBatchWithSharedMemory(b *batch.Batch) *Batch {
	bat := NewBatch()
	for i, attr := range b.Attrs {
		v := NewVectorWithSharedMemory(b.Vecs[i])
		bat.AddVector(attr, v)
	}
	return bat
}

func GetValue(col *movec.Vector, row uint32) any {
	if col.GetNulls().Np != nil && col.GetNulls().Np.Contains(uint64(row)) {
		return types.Null{}
	}
	return getNonNullValue(col, row)
}
