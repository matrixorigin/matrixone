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

	CoalesceDateTime = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Datetime](vs, proc, types.Type{Oid: types.T_datetime})
	}

	CoalesceVarchar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwString(vs, proc, types.Type{Oid: types.T_varchar})
	}

	CoalesceChar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwString(vs, proc, types.Type{Oid: types.T_char})
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
)

// coalesceGeneral is a general evaluate function for case-when operator
// whose return type is uint / int / float / bool / date / datetime
func coalesceGeneral[T NormalType](vs []*vector.Vector, proc *process.Process, t types.Type) (*vector.Vector, error) {
	for i := 0; i < len(vs); i++ {
		input := vs[i]
		col := vector.MustTCols[T](input)
		if input.IsScalar() {
			if input.IsScalarNull() {
				continue
			}
			r := proc.AllocScalarVector(t)
			r.Typ.Precision = input.Typ.Precision
			r.Typ.Width = input.Typ.Width
			r.Typ.Scale = input.Typ.Scale
			r.Col = make([]T, 1)
			r.Col.([]T)[0] = col[0]
			return r, nil
		} else {
			l := vector.Length(vs[0])
			// all nulls
			if nulls.Length(input.Nsp) == l {
				continue
			}

			rs, err := proc.AllocVector(t, int64(l*t.Oid.TypeLen()))
			if err != nil {
				return nil, err
			}
			rs.Col = vector.DecodeFixedCol[T](rs, t.Oid.TypeLen())
			rs.Col = rs.Col.([]T)[:l]
			rs.Typ.Precision = input.Typ.Precision
			rs.Typ.Width = input.Typ.Width
			rs.Typ.Scale = input.Typ.Scale
			rs.Nsp.Or(input.Nsp)
			return rs, nil
		}
	}

	return proc.AllocScalarNullVector(t), nil
}
