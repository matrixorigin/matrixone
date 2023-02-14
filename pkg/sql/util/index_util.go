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
var CompactSingleIndexCol = compactSingleIndexCol
var CompactPrimaryCol = compactPrimaryCol

func BuildIndexTableName(ctx context.Context, unique bool) (string, error) {
	var name string
	name = catalog.PrefixIndexTableName
	if unique {
		name += "unique_"
	} else {
		name += "secondary_"
	}
	id, err := uuid.NewUUID()
	if err != nil {
		return "", moerr.NewInternalError(ctx, "newuuid failed")
	}
	name += id.String()
	return name, nil
}

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

	b.SetZs(vector.Length(b.Vecs[0]), proc.Mp())
	return b, vector.Length(b.Vecs[0])
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
	length := vector.Length(vs[0])
	vct := types.T_varchar.ToType()
	nsp := new(nulls.Nulls)
	val := make([][]byte, 0, length)
	ps := types.NewPackerArray(length, proc.Mp())
	bitMap := new(nulls.Nulls)

	for _, v := range vs {
		switch v.Typ.Oid {
		case types.T_bool:
			s := vector.MustTCols[bool](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeBool(b)
				}
			}
		case types.T_int8:
			s := vector.MustTCols[int8](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeInt8(b)
				}
			}
		case types.T_int16:
			s := vector.MustTCols[int16](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeInt16(b)
				}
			}
		case types.T_int32:
			s := vector.MustTCols[int32](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeInt32(b)
				}
			}
		case types.T_int64:
			s := vector.MustTCols[int64](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeInt64(b)
				}
			}
		case types.T_uint8:
			s := vector.MustTCols[uint8](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeUint8(b)
				}
			}
		case types.T_uint16:
			s := vector.MustTCols[uint16](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeUint16(b)
				}
			}
		case types.T_uint32:
			s := vector.MustTCols[uint32](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeUint32(b)
				}
			}
		case types.T_uint64:
			s := vector.MustTCols[uint64](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeUint64(b)
				}
			}
		case types.T_float32:
			s := vector.MustTCols[float32](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeFloat32(b)
				}
			}
		case types.T_float64:
			s := vector.MustTCols[float64](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeFloat64(b)
				}
			}
		case types.T_date:
			s := vector.MustTCols[types.Date](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeDate(b)
				}
			}
		case types.T_time:
			s := vector.MustTCols[types.Time](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeTime(b)
				}
			}
		case types.T_datetime:
			s := vector.MustTCols[types.Datetime](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeDatetime(b)
				}
			}
		case types.T_timestamp:
			s := vector.MustTCols[types.Timestamp](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeTimestamp(b)
				}
			}
		case types.T_decimal64:
			s := vector.MustTCols[types.Decimal64](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeDecimal64(b)
				}
			}
		case types.T_decimal128:
			s := vector.MustTCols[types.Decimal128](v)
			for i, b := range s {
				if nulls.Contains(v.Nsp, uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeDecimal128(b)
				}
			}
		case types.T_json, types.T_char, types.T_varchar, types.T_blob, types.T_text:
			vs := vector.GetStrVectorValues(v)
			for i := range vs {
				if nulls.Contains(v.Nsp, uint64(i)) {
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

	vec := vector.NewWithBytes(vct, val, nsp, proc.Mp())

	return vec, bitMap
}

func compactSingleIndexCol(v *vector.Vector, proc *process.Process) (*vector.Vector, *nulls.Nulls) {
	nsp := new(nulls.Nulls)
	var vec *vector.Vector
	length := vector.Length(v)
	switch v.Typ.Oid {
	case types.T_bool:
		s := vector.MustTCols[bool](v)
		ns := make([]bool, 0, length)
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_int8:
		s := vector.MustTCols[int8](v)
		ns := make([]int8, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_int16:
		s := vector.MustTCols[int16](v)
		ns := make([]int16, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_int32:
		s := vector.MustTCols[int32](v)
		ns := make([]int32, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_int64:
		s := vector.MustTCols[int64](v)
		ns := make([]int64, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_uint8:
		s := vector.MustTCols[uint8](v)
		ns := make([]uint8, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_uint16:
		s := vector.MustTCols[uint16](v)
		ns := make([]uint16, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_uint32:
		s := vector.MustTCols[uint32](v)
		ns := make([]uint32, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_uint64:
		s := vector.MustTCols[uint64](v)
		ns := make([]uint64, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_float32:
		s := vector.MustTCols[float32](v)
		ns := make([]float32, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_float64:
		s := vector.MustTCols[float64](v)
		ns := make([]float64, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_date:
		s := vector.MustTCols[types.Date](v)
		ns := make([]types.Date, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_time:
		s := vector.MustTCols[types.Time](v)
		ns := make([]types.Time, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_datetime:
		s := vector.MustTCols[types.Datetime](v)
		ns := make([]types.Datetime, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_timestamp:
		s := vector.MustTCols[types.Timestamp](v)
		ns := make([]types.Timestamp, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_decimal64:
		s := vector.MustTCols[types.Decimal64](v)
		ns := make([]types.Decimal64, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_decimal128:
		s := vector.MustTCols[types.Decimal128](v)
		ns := make([]types.Decimal128, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_json, types.T_char, types.T_varchar, types.T_blob:
		s := vector.GetStrVectorValues(v)
		ns := make([]string, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithStrings(v.Typ, ns, nsp, proc.Mp())
	}
	return vec, v.Nsp
}
func compactPrimaryCol(v *vector.Vector, bitMap *nulls.Nulls, proc *process.Process) *vector.Vector {
	nsp := new(nulls.Nulls)
	var vec *vector.Vector
	length := vector.Length(v)
	switch v.Typ.Oid {
	case types.T_bool:
		s := vector.MustTCols[bool](v)
		ns := make([]bool, 0, length)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_int8:
		s := vector.MustTCols[int8](v)
		ns := make([]int8, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_int16:
		s := vector.MustTCols[int16](v)
		ns := make([]int16, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_int32:
		s := vector.MustTCols[int32](v)
		ns := make([]int32, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_int64:
		s := vector.MustTCols[int64](v)
		ns := make([]int64, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_uint8:
		s := vector.MustTCols[uint8](v)
		ns := make([]uint8, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_uint16:
		s := vector.MustTCols[uint16](v)
		ns := make([]uint16, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_uint32:
		s := vector.MustTCols[uint32](v)
		ns := make([]uint32, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_uint64:
		s := vector.MustTCols[uint64](v)
		ns := make([]uint64, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_float32:
		s := vector.MustTCols[float32](v)
		ns := make([]float32, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_float64:
		s := vector.MustTCols[float64](v)
		ns := make([]float64, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_date:
		s := vector.MustTCols[types.Date](v)
		ns := make([]types.Date, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_time:
		s := vector.MustTCols[types.Time](v)
		ns := make([]types.Time, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_datetime:
		s := vector.MustTCols[types.Datetime](v)
		ns := make([]types.Datetime, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_timestamp:
		s := vector.MustTCols[types.Timestamp](v)
		ns := make([]types.Timestamp, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_decimal64:
		s := vector.MustTCols[types.Decimal64](v)
		ns := make([]types.Decimal64, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_decimal128:
		s := vector.MustTCols[types.Decimal128](v)
		ns := make([]types.Decimal128, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_json, types.T_char, types.T_varchar, types.T_blob:
		s := vector.GetStrVectorValues(v)
		ns := make([]string, 0)
		for i, b := range s {
			if !nulls.Contains(bitMap, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithStrings(v.Typ, ns, nsp, proc.Mp())
	}
	return vec
}
