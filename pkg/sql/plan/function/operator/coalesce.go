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
		return coalesceString(vs, proc, types.Type{Oid: types.T_varchar})
	}

	CoalesceChar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceString(vs, proc, types.Type{Oid: types.T_char})
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
				r.Typ.Precision = input.Typ.Precision
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
	for i := 0; i < len(vs); i++ {
		input := vs[i]
		if input.IsScalar() {
			if !input.IsScalarNull() {
				cols := vector.MustBytesCols(input)
				r := proc.AllocScalarVector(typ)
				r.Col = &types.Bytes{
					Data:    make([]byte, cols.Lengths[0]),
					Offsets: make([]uint32, 1),
					Lengths: make([]uint32, 1),
				}
				copy(r.Col.(*types.Bytes).Data, cols.Data)
				r.Col.(*types.Bytes).Lengths[0] = cols.Lengths[0]
				return r, nil
			}
		} else {
			startIdx = i
			break
		}
	}

	rs, err := proc.AllocVector(typ, 0)
	if err != nil {
		return nil, err
	}
	rs.Col = &types.Bytes{
		Data:    nil,
		Offsets: make([]uint32, vecLen),
		Lengths: make([]uint32, vecLen),
	}
	rsCols := rs.Col.(*types.Bytes)

	dataVec := make([][]byte, vecLen)

	rs.Nsp = nulls.NewWithSize(vecLen)
	rs.Nsp.Np.AddRange(0, uint64(vecLen))

	for i := startIdx; i < len(vs); i++ {
		input := vs[i]
		cols := vector.MustBytesCols(input)
		if input.IsScalar() {
			if input.IsScalarNull() {
				continue
			}

			for j := 0; j < vecLen; j++ {
				if rs.Nsp.Contains(uint64(j)) {
					copy(dataVec[j], cols.Data)
				}
			}
			rs.Nsp.Np = nil
			break
		} else {
			nullsLength := nulls.Length(input.Nsp)
			if nullsLength == vecLen {
				// all null do nothing
				continue
			} else if nullsLength == 0 {
				// all not null
				for j := 0; j < vecLen; j++ {
					if rs.Nsp.Contains(uint64(j)) {
						length := cols.Lengths[j]
						o := cols.Offsets[j]
						dataVec[j] = append(dataVec[j], cols.Data[o:o+length]...)
					}
				}
				rs.Nsp.Np = nil
				break
			} else {
				// some nulls
				for j := 0; j < vecLen; j++ {
					if rs.Nsp.Contains(uint64(j)) && !input.Nsp.Contains(uint64(j)) {
						length := cols.Lengths[j]
						o := cols.Offsets[j]
						dataVec[j] = append(dataVec[j], cols.Data[o:o+length]...)
						rs.Nsp.Np.Remove(uint64(j))
					}
				}
				// now if is empty, break
				if rs.Nsp.Np.IsEmpty() {
					rs.Nsp.Np = nil
					break
				}
			}
		}
	}

	var offset uint32 = 0
	for j := 0; j < vecLen; j++ {
		length := len(dataVec[j])
		if length > 0 {
			rsCols.Data = append(rsCols.Data, dataVec[j]...)
			rsCols.Offsets[j] = offset
			rsCols.Lengths[j] = uint32(length)
			offset = offset + uint32(length)
		}
	}

	rs.Data = rsCols.Data

	return rs, nil
}
