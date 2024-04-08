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

package util

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var SerialWithCompacted = serialWithCompacted
var SerialWithoutCompacted = serialWithoutCompacted
var CompactSingleIndexCol = compactSingleIndexCol
var CompactPrimaryCol = compactPrimaryCol

func BuildIndexTableName(ctx context.Context, unique bool) (string, error) {
	var name string
	if unique {
		name = catalog.UniqueIndexTableNamePrefix
	} else {
		name = catalog.SecondaryIndexTableNamePrefix
	}
	id, err := uuid.NewV7()
	if err != nil {
		return "", moerr.NewInternalError(ctx, "newuuid failed")
	}
	name += id.String()
	return name, nil
}

// BuildUniqueKeyBatch used in test to validate
// serialWithCompacted(), compactSingleIndexCol() and compactPrimaryCol()
func BuildUniqueKeyBatch(vecs []*vector.Vector, attrs []string, parts []string, originTablePrimaryKey string, proc *process.Process) (*batch.Batch, int) {
	var b *batch.Batch
	if originTablePrimaryKey == "" {
		b = &batch.Batch{
			Attrs: make([]string, 1),
			Vecs:  make([]*vector.Vector, 1),
			Cnt:   1,
		}
		b.Attrs[0] = catalog.IndexTableIndexColName
	} else {
		b = &batch.Batch{
			Attrs: make([]string, 2),
			Vecs:  make([]*vector.Vector, 2),
			Cnt:   1,
		}
		b.Attrs[0] = catalog.IndexTableIndexColName
		b.Attrs[1] = catalog.IndexTablePrimaryColName
	}
	isCompoundIndex := false
	if len(parts) > 1 {
		isCompoundIndex = true
	}
	//bitMap := new(nulls.Nulls)
	var bitMap *nulls.Nulls
	if isCompoundIndex {
		cIndexVecMap := make(map[string]*vector.Vector)
		for num, attrName := range attrs {
			for _, name := range parts {
				if attrName == name {
					cIndexVecMap[name] = vecs[num]
				}
			}
		}
		vs := make([]*vector.Vector, 0)
		for _, part := range parts {
			v := cIndexVecMap[part]
			vs = append(vs, v)
		}
		b.Vecs[0], bitMap = serialWithCompacted(vs, proc)
	} else {
		var vec *vector.Vector
		for i, name := range attrs {
			if parts[0] == name {
				vec = vecs[i]
				break
			}
		}
		b.Vecs[0], bitMap = compactSingleIndexCol(vec, proc)
	}

	if len(b.Attrs) > 1 {
		var vec *vector.Vector
		for i, name := range attrs {
			if originTablePrimaryKey == name {
				vec = vecs[i]
			}
		}
		b.Vecs[1] = compactPrimaryCol(vec, bitMap, proc)
	}

	b.SetRowCount(b.Vecs[0].Length())
	return b, b.RowCount()
}

// SerialWithCompacted have a similar function named Serial
// SerialWithCompacted function is used by BuildUniqueKeyBatch
// when vs have null value, the function will ignore the row in
// the vs
// for example:
// input vec is [[1, 1, 1], [2, 2, null], [3, 3, 3]]
// result vec is [serial(1, 2, 3), serial(1, 2, 3)]
// result bitmap is [2]
func serialWithCompacted(vs []*vector.Vector, proc *process.Process) (*vector.Vector, *nulls.Nulls) {
	// resolve vs
	length := vs[0].Length()
	vct := types.T_varchar.ToType()
	//nsp := new(nulls.Nulls)
	val := make([][]byte, 0, length)
	ps := types.NewPackerArray(length, proc.Mp())
	bitMap := new(nulls.Nulls)

	for _, v := range vs {
		switch v.GetType().Oid {
		case types.T_bool:
			s := vector.MustFixedCol[bool](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeBool(b)
				}
			}
		case types.T_int8:
			s := vector.MustFixedCol[int8](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeInt8(b)
				}
			}
		case types.T_int16:
			s := vector.MustFixedCol[int16](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeInt16(b)
				}
			}
		case types.T_int32:
			s := vector.MustFixedCol[int32](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeInt32(b)
				}
			}
		case types.T_int64:
			s := vector.MustFixedCol[int64](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeInt64(b)
				}
			}
		case types.T_uint8:
			s := vector.MustFixedCol[uint8](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeUint8(b)
				}
			}
		case types.T_uint16:
			s := vector.MustFixedCol[uint16](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeUint16(b)
				}
			}
		case types.T_uint32:
			s := vector.MustFixedCol[uint32](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeUint32(b)
				}
			}
		case types.T_uint64:
			s := vector.MustFixedCol[uint64](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeUint64(b)
				}
			}
		case types.T_float32:
			s := vector.MustFixedCol[float32](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeFloat32(b)
				}
			}
		case types.T_float64:
			s := vector.MustFixedCol[float64](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeFloat64(b)
				}
			}
		case types.T_date:
			s := vector.MustFixedCol[types.Date](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeDate(b)
				}
			}
		case types.T_time:
			s := vector.MustFixedCol[types.Time](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeTime(b)
				}
			}
		case types.T_datetime:
			s := vector.MustFixedCol[types.Datetime](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeDatetime(b)
				}
			}
		case types.T_timestamp:
			s := vector.MustFixedCol[types.Timestamp](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeTimestamp(b)
				}
			}
		case types.T_enum:
			s := vector.MustFixedCol[types.Enum](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeEnum(b)
				}
			}
		case types.T_decimal64:
			s := vector.MustFixedCol[types.Decimal64](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeDecimal64(b)
				}
			}
		case types.T_decimal128:
			s := vector.MustFixedCol[types.Decimal128](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeDecimal128(b)
				}
			}
		case types.T_json, types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text,
			types.T_array_float32, types.T_array_float64:
			// NOTE 1: We will consider T_array as bytes here just like JSON, VARBINARY and BLOB.
			// If not, we need to define arrayType in types/tuple.go as arrayF32TypeCode, arrayF64TypeCode etc
			// NOTE 2: vs is []string and not []byte. vs[i] is not of form "[1,2,3]". It is binary string of []float32{1,2,3}
			// NOTE 3: This class is mainly used by PreInsertUnique which gets triggered before inserting into column having
			// Unique Key or Primary Key constraint. Vector cannot be UK or PK.
			vs := vector.MustStrCol(v)
			for i := range vs {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {

					ps[i].EncodeStringType([]byte(vs[i]))
				}
			}
		}
	}

	for i := range ps {
		if !nulls.Contains(bitMap, uint64(i)) {
			val = append(val, ps[i].GetBuf())
		}
	}

	vec := proc.GetVector(vct)
	vector.AppendBytesList(vec, val, nil, proc.Mp())

	return vec, bitMap
}

// serialWithoutCompacted is similar to serialWithCompacted and builtInSerial
// serialWithoutCompacted function is used by Secondary Index to support rows containing null entries
// for example:
// input vec is [[1, 1, 1], [2, 2, null], [3, 3, 3]]
// result vec is [serial(1, 2, 3), serial(1, 2, null), serial(1, 2, 3)]
// result bitmap is [] (empty)
// Here we are keeping the same function signature of serialWithCompacted so that we can duplicate the same code of
// `preinsertunique` in `preinsertsecondaryindex`
func serialWithoutCompacted(vs []*vector.Vector, proc *process.Process) (*vector.Vector, *nulls.Nulls, error) {
	if len(vs) == 0 {
		// return empty vector and empty bitmap
		return proc.GetVector(types.T_varchar.ToType()), new(nulls.Nulls), nil
	}

	result := vector.NewFunctionResultWrapper(proc.GetVector, proc.PutVector, types.T_varchar.ToType(), proc.Mp())
	rowCount := vs[0].Length()
	_ = function.BuiltInSerialFull(vs, result, proc, rowCount)
	resultVec := result.GetResultVector()
	return resultVec, new(nulls.Nulls), nil
}

func compactSingleIndexCol(v *vector.Vector, proc *process.Process) (*vector.Vector, *nulls.Nulls) {
	nsp := new(nulls.Nulls)
	vec := proc.GetVector(*v.GetType())
	length := v.Length()
	switch v.GetType().Oid {
	case types.T_bool:
		s := vector.MustFixedCol[bool](v)
		ns := make([]bool, 0, length)
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_int8:
		s := vector.MustFixedCol[int8](v)
		ns := make([]int8, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_int16:
		s := vector.MustFixedCol[int16](v)
		ns := make([]int16, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_int32:
		s := vector.MustFixedCol[int32](v)
		ns := make([]int32, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_int64:
		s := vector.MustFixedCol[int64](v)
		ns := make([]int64, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_uint8:
		s := vector.MustFixedCol[uint8](v)
		ns := make([]uint8, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_uint16:
		s := vector.MustFixedCol[uint16](v)
		ns := make([]uint16, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_uint32:
		s := vector.MustFixedCol[uint32](v)
		ns := make([]uint32, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewVec(*v.GetType())
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_uint64:
		s := vector.MustFixedCol[uint64](v)
		ns := make([]uint64, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_float32:
		s := vector.MustFixedCol[float32](v)
		ns := make([]float32, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_float64:
		s := vector.MustFixedCol[float64](v)
		ns := make([]float64, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewVec(*v.GetType())
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_date:
		s := vector.MustFixedCol[types.Date](v)
		ns := make([]types.Date, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewVec(*v.GetType())
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_time:
		s := vector.MustFixedCol[types.Time](v)
		ns := make([]types.Time, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_datetime:
		s := vector.MustFixedCol[types.Datetime](v)
		ns := make([]types.Datetime, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_timestamp:
		s := vector.MustFixedCol[types.Timestamp](v)
		ns := make([]types.Timestamp, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_enum:
		s := vector.MustFixedCol[types.Enum](v)
		ns := make([]types.Enum, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_decimal64:
		s := vector.MustFixedCol[types.Decimal64](v)
		ns := make([]types.Decimal64, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_decimal128:
		s := vector.MustFixedCol[types.Decimal128](v)
		ns := make([]types.Decimal128, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_json, types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob,
		types.T_array_float32, types.T_array_float64:
		s := vector.MustBytesCol(v)
		ns := make([][]byte, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.GetNulls(), uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendBytesList(vec, ns, nil, proc.Mp())
	}
	return vec, v.GetNulls()
}
func compactPrimaryCol(v *vector.Vector, bitMap *nulls.Nulls, proc *process.Process) *vector.Vector {
	//nsp := new(nulls.Nulls)
	vec := proc.GetVector(*v.GetType())
	length := v.Length()
	switch v.GetType().Oid {
	case types.T_bool:
		s := vector.MustFixedCol[bool](v)
		ns := make([]bool, 0, length)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_int8:
		s := vector.MustFixedCol[int8](v)
		ns := make([]int8, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_int16:
		s := vector.MustFixedCol[int16](v)
		ns := make([]int16, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_int32:
		s := vector.MustFixedCol[int32](v)
		ns := make([]int32, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_int64:
		s := vector.MustFixedCol[int64](v)
		ns := make([]int64, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_uint8:
		s := vector.MustFixedCol[uint8](v)
		ns := make([]uint8, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_uint16:
		s := vector.MustFixedCol[uint16](v)
		ns := make([]uint16, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_uint32:
		s := vector.MustFixedCol[uint32](v)
		ns := make([]uint32, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_uint64:
		s := vector.MustFixedCol[uint64](v)
		ns := make([]uint64, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_float32:
		s := vector.MustFixedCol[float32](v)
		ns := make([]float32, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_float64:
		s := vector.MustFixedCol[float64](v)
		ns := make([]float64, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_date:
		s := vector.MustFixedCol[types.Date](v)
		ns := make([]types.Date, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_time:
		s := vector.MustFixedCol[types.Time](v)
		ns := make([]types.Time, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_datetime:
		s := vector.MustFixedCol[types.Datetime](v)
		ns := make([]types.Datetime, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_timestamp:
		s := vector.MustFixedCol[types.Timestamp](v)
		ns := make([]types.Timestamp, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_enum:
		s := vector.MustFixedCol[types.Enum](v)
		ns := make([]types.Enum, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_decimal64:
		s := vector.MustFixedCol[types.Decimal64](v)
		ns := make([]types.Decimal64, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_decimal128:
		s := vector.MustFixedCol[types.Decimal128](v)
		ns := make([]types.Decimal128, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendFixedList(vec, ns, nil, proc.Mp())
	case types.T_json, types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob,
		types.T_array_float32, types.T_array_float64:
		s := vector.MustBytesCol(v)
		ns := make([][]byte, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vector.AppendBytesList(vec, ns, nil, proc.Mp())
	}
	return vec
}
