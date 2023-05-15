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
		return coalesceGeneral[uint8](vs, proc, types.T_uint8.ToType())
	}

	CoalesceUint16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[uint16](vs, proc, types.T_uint16.ToType())
	}

	CoalesceUint32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[uint32](vs, proc, types.T_uint32.ToType())
	}

	CoalesceUint64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[uint64](vs, proc, types.T_uint64.ToType())
	}

	CoalesceInt8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[int8](vs, proc, types.T_int8.ToType())
	}

	CoalesceInt16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[int16](vs, proc, types.T_int16.ToType())
	}

	CoalesceInt32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[int32](vs, proc, types.T_int32.ToType())
	}

	CoalesceInt64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[int64](vs, proc, types.T_int64.ToType())
	}

	CoalesceFloat32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[float32](vs, proc, types.T_float32.ToType())
	}

	CoalesceFloat64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[float64](vs, proc, types.T_float64.ToType())
	}

	CoalesceBool = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[bool](vs, proc, types.T_bool.ToType())
	}

	CoalesceDate = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Date](vs, proc, types.T_date.ToType())
	}

	CoalesceTime = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Time](vs, proc, types.T_time.ToType())
	}

	CoalesceDateTime = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Datetime](vs, proc, types.T_datetime.ToType())
	}

	CoalesceVarchar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceString(vs, proc, types.T_varchar.ToType())
	}

	CoalesceChar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceString(vs, proc, types.T_char.ToType())
	}

	CoalesceJson = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceString(vs, proc, types.T_json.ToType())
	}

	CoalesceBlob = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceString(vs, proc, types.T_blob.ToType())
	}

	CoalesceText = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceString(vs, proc, types.T_text.ToType())
	}

	CoalesceDecimal64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Decimal64](vs, proc, types.T_decimal64.ToType())
	}

	CoalesceDecimal128 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Decimal128](vs, proc, types.T_decimal128.ToType())
	}

	CoalesceTimestamp = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Timestamp](vs, proc, types.T_timestamp.ToType())
	}

	CoalesceUuid = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return coalesceGeneral[types.Uuid](vs, proc, types.T_uuid.ToType())
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
	vecLen := vs[0].Length()
	startIdx := 0
	for i := 0; i < len(vs); i++ {
		input := vs[i]
		if input.IsConst() {
			if !input.IsConstNull() {
				cols := vector.MustFixedCol[T](input)
				r := vector.NewConstFixed(t, cols[0], vecLen, proc.Mp())
				r.GetType().Width = input.GetType().Width
				r.GetType().Scale = input.GetType().Scale
				r.SetLength(1)
				return r, nil
			}
		} else {
			startIdx = i
			break
		}
	}

	rs, err := proc.AllocVectorOfRows(t, vecLen, nil)
	if err != nil {
		return nil, err
	}
	rsCols := vector.MustFixedCol[T](rs)

	rs.SetNulls(nulls.NewWithSize(vecLen))
	rs.GetNulls().AddRange(0, uint64(vecLen))

	for i := startIdx; i < len(vs); i++ {
		input := vs[i]
		if input.GetType().Oid != types.T_any {
			rs.SetType(*input.GetType())
		}
		if input.IsConstNull() {
			continue
		}
		cols := vector.MustFixedCol[T](input)
		if input.IsConst() {
			for j := 0; j < vecLen; j++ {
				if rs.GetNulls().Contains(uint64(j)) {
					rsCols[j] = cols[0]
				}
			}
			rs.GetNulls().Reset()
			return rs, nil
		} else {
			nullsLength := input.GetNulls().Count()
			if nullsLength == vecLen {
				// all null do nothing
				continue
			} else if nullsLength == 0 {
				// all not null
				for j := 0; j < vecLen; j++ {
					if rs.GetNulls().Contains(uint64(j)) {
						rsCols[j] = cols[j]
					}
				}
				rs.GetNulls().Reset()
				return rs, nil
			} else {
				// some nulls
				for j := 0; j < vecLen; j++ {
					if rs.GetNulls().Contains(uint64(j)) && !input.GetNulls().Contains(uint64(j)) {
						rsCols[j] = cols[j]
						rs.GetNulls().Unset(uint64(j))
					}
				}

				// XXX Is this really an optimization?
				if rs.GetNulls().IsEmpty() {
					rs.GetNulls().Reset()
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
	vecLen := vs[0].Length()
	startIdx := 0

	// If leading expressions are non null scalar, return.   Otherwise startIdx
	// is positioned at the first non scalar vector.
	for i := 0; i < len(vs); i++ {
		input := vs[i]
		if input.IsConst() {
			if !input.IsConstNull() {
				cols := vector.MustStrCol(input)
				vec := vector.NewConstBytes(typ, []byte(cols[0]), vecLen, proc.Mp())
				return vec, nil
			}
		} else {
			startIdx = i
			break
		}
	}

	rs := make([]string, vecLen)
	nsp := nulls.NewWithSize(vecLen)
	nsp.AddRange(0, uint64(vecLen))

	for i := startIdx; i < len(vs); i++ {
		input := vs[i]
		if input.IsConstNull() {
			continue
		}
		cols := vector.MustStrCol(input)
		if input.IsConst() {
			for j := 0; j < vecLen; j++ {
				if nsp.Contains(uint64(j)) {
					rs[j] = cols[0]
				}
			}
			nsp = nil
			break
		} else {
			nullsLength := input.GetNulls().Count()
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
					if nsp.Contains(uint64(j)) && !input.GetNulls().Contains(uint64(j)) {
						rs[j] = cols[j]
						nsp.Unset(uint64(j))
					}
				}

				// now if is empty, break
				if nsp.IsEmpty() {
					nsp = nil
					break
				}
			}
		}
	}
	vec := vector.NewVec(typ)
	vector.AppendStringList(vec, rs, nil, proc.Mp())
	vec.SetNulls(nsp)
	return vec, nil
}
