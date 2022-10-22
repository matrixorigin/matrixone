// Copyright 2021 - 2022 Matrix Origin
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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Serial(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	for _, v := range vectors {
		if v.IsScalar() {
			return nil, moerr.NewConstraintViolation("serial function don't support constant value")
		}
	}
	return SerialWithSomeCols(vectors, proc)
}

func SerialWithSomeCols(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	length := vector.Length(vectors[0])
	vct := types.T_varchar.ToType()
	nsp := new(nulls.Nulls)
	val := make([][]byte, 0, length)
	ps := types.NewPackerArray(length)
	bitMap := new(nulls.Nulls)

	for _, v := range vectors {
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

	return vec, nil
}
