// Copyright 2022 Matrix Origin
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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	CoalesceUint8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[uint8](vs, proc, types.Type{Oid: types.T_uint8})
	}

	CoalesceUint16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[uint16](vs, proc, types.Type{Oid: types.T_uint16})
	}

	CoalesceUint32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[uint32](vs, proc, types.Type{Oid: types.T_uint32})
	}

	CoalesceUint64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[uint64](vs, proc, types.Type{Oid: types.T_uint64})
	}

	CoalesceInt8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[int8](vs, proc, types.Type{Oid: types.T_int8})
	}

	CoalesceInt16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[int16](vs, proc, types.Type{Oid: types.T_int16})
	}

	CoalesceInt32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[int32](vs, proc, types.Type{Oid: types.T_int32})
	}

	CoalesceInt64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[int64](vs, proc, types.Type{Oid: types.T_int64})
	}

	CoalesceFloat32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[float32](vs, proc, types.Type{Oid: types.T_float32})
	}

	CoalesceFloat64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[float64](vs, proc, types.Type{Oid: types.T_float64})
	}

	CoalesceBool = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[bool](vs, proc, types.Type{Oid: types.T_bool})
	}

	CoalesceDate = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Date](vs, proc, types.Type{Oid: types.T_date})
	}

	CoalesceTime = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Time](vs, proc, types.Type{Oid: types.T_time})
	}

	CoalesceDateTime = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Datetime](vs, proc, types.Type{Oid: types.T_datetime})
	}

	CoalesceVarchar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceString(vs, proc, types.Type{Oid: types.T_varchar, Width: types.MaxVarcharLen})
	}

	CoalesceChar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceString(vs, proc, types.Type{Oid: types.T_char})
	}

	CoalesceJson = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceString(vs, proc, types.Type{Oid: types.T_json.ToType().Oid})
	}

	CoalesceBlob = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceString(vs, proc, types.Type{Oid: types.T_blob.ToType().Oid})
	}

	CoalesceText = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceString(vs, proc, types.Type{Oid: types.T_text.ToType().Oid})
	}

	CoalesceDecimal64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Decimal64](vs, proc, types.Type{Oid: types.T_decimal64})
	}

	CoalesceDecimal128 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Decimal128](vs, proc, types.Type{Oid: types.T_decimal128})
	}

	CoalesceTimestamp = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Timestamp](vs, proc, types.Type{Oid: types.T_timestamp})
	}

	CoalesceUuid = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Uuid](vs, proc, types.Type{Oid: types.T_uuid})
	}
)

// CoalesceTypeCheckFn is type check function for coalesce operator
func CoalesceTypeCheckFn(inputTypes []types.T, _ []types.T, ret types.T) bool {
	l := len(inputTypes)
	for i := 0; i < l; i++ {
		if inputTypes[i] != ret && inputTypes[i] != types.T_any {
			return false
		}
	}
	return true
}

// coalesceGeneral is a general evaluate function for coalesce operator
// when return type is uint / int / float / bool / date / datetime
func coalesceGeneral[T NormalType](vs []*vector.Vector, proc *process.Process, t types.Type) (*vector.Vector, error) {
	vecLen := vector.Length(vs[0])
	startIdx := 0
	for i := 0; i < len(vs); i++ {
		input := vs[i]
		if input.IsScalar() {
			if !input.IsScalarNull() {
				cols := vector.MustTCols[T](input)
				r := proc.AllocScalarVector(t)
				r.Typ.Width = input.Typ.Width
				r.Typ.Scale = input.Typ.Scale
				r.Col = make([]T, 1)
				r.Col.([]T)[0] = cols[0]
				return r, nil
			}
		} else {
			startIdx = i
			break
		}
	}

	rs, err := proc.AllocVector(t, int64(vecLen*t.Oid.TypeLen()))
	if err != nil {
		return nil, err
	}
	rs.Col = vector.DecodeFixedCol[T](rs, t.Oid.TypeLen())
	rs.Col = rs.Col.([]T)[:vecLen]
	rsCols := rs.Col.([]T)

	rs.Nsp = nulls.NewWithSize(vecLen)
	rs.Nsp.Np.AddRange(0, uint64(vecLen))

	for i := startIdx; i < len(vs); i++ {
		input := vs[i]
		if input.Typ.Oid != types.T_any {
			rs.Typ = input.Typ
		}
		cols := vector.MustTCols[T](input)
		if input.IsScalar() {
			if input.IsScalarNull() {
				continue
			}

			for j := 0; j < vecLen; j++ {
				if rs.Nsp.Contains(uint64(j)) {
					rsCols[j] = cols[0]
				}
			}
			rs.Nsp.Np = nil
			return rs, nil
		} else {
			nullsLength := nulls.Length(input.Nsp)
			if nullsLength == vecLen {
				// all null do nothing
				continue
			} else if nullsLength == 0 {
				// all not null
				for j := 0; j < vecLen; j++ {
					if rs.Nsp.Contains(uint64(j)) {
						rsCols[j] = cols[j]
					}
				}
				rs.Nsp.Np = nil
				return rs, nil
			} else {
				// some nulls
				for j := 0; j < vecLen; j++ {
					if rs.Nsp.Contains(uint64(j)) && !input.Nsp.Contains(uint64(j)) {
						rsCols[j] = cols[j]
						rs.Nsp.Np.Remove(uint64(j))
					}
				}

				if rs.Nsp.Np.IsEmpty() {
					rs.Nsp.Np = nil
					return rs, nil
				}
			}
		}
	}

	return rs, nil
}

// coalesceGeneral is a general evaluate function for coalesce operator
// when return type is char / varchar
func coalesceString(vs []*vector.Vector, proc *process.Process, typ types.Type) (*vector.Vector, error) {
	vecLen := vector.Length(vs[0])
	startIdx := 0

	// If leading expressions are non null scalar, return.   Otherwise startIdx
	// is positioned at the first non scalar vector.
	for i := 0; i < len(vs); i++ {
		input := vs[i]
		if input.IsScalar() {
			if !input.IsScalarNull() {
				cols := vector.MustStrCols(input)
				return vector.NewConstString(typ, input.Length(), cols[0], proc.Mp()), nil
			}
		} else {
			startIdx = i
			break
		}
	}

	rs := make([]string, vecLen)
	nsp := nulls.NewWithSize(vecLen)
	nsp.Np.AddRange(0, uint64(vecLen))

	for i := startIdx; i < len(vs); i++ {
		input := vs[i]
		cols := vector.MustStrCols(input)
		if input.IsScalar() {
			if input.IsScalarNull() {
				continue
			}
			for j := 0; j < vecLen; j++ {
				if nsp.Contains(uint64(j)) {
					rs[j] = cols[0]
				}
			}
			nsp = nil
			break
		} else {
			nullsLength := nulls.Length(input.Nsp)
			if nullsLength == vecLen {
				// all null do nothing
				continue
			} else if nullsLength == 0 {
				// all not null
				for j := 0; j < vecLen; j++ {
					if nsp.Contains(uint64(j)) {
						rs[j] = cols[j]
					}
				}
				nsp = nil
				break
			} else {
				// some nulls
				for j := 0; j < vecLen; j++ {
					if nsp.Contains(uint64(j)) && !input.Nsp.Contains(uint64(j)) {
						rs[j] = cols[j]
						nsp.Np.Remove(uint64(j))
					}
				}

				// now if is empty, break
				if nsp.Np.IsEmpty() {
					nsp = nil
					break
				}
			}
		}
	}

	return vector.NewWithStrings(typ, rs, nsp, proc.Mp()), nil
}
