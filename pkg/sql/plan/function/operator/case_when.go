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
	"golang.org/x/exp/constraints"
)

// case-when operator only support format like that
//
//	`
//		case
//		when A = a1 then ...
//		when A = a2 then ...
//		when A = a3 then ...
//		(else ...)
//	`
//
// format `case A when a1 then ... when a2 then ...` should be converted to required format.
var (
	CaseWhenUint8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[uint8](vs, proc, types.Type{Oid: types.T_uint8})
	}

	CaseWhenUint16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[uint16](vs, proc, types.Type{Oid: types.T_uint16})
	}

	CaseWhenUint32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[uint32](vs, proc, types.Type{Oid: types.T_uint32})
	}

	CaseWhenUint64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[uint64](vs, proc, types.Type{Oid: types.T_uint64})
	}

	CaseWhenInt8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[int8](vs, proc, types.Type{Oid: types.T_int8})
	}

	CaseWhenInt16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[int16](vs, proc, types.Type{Oid: types.T_int16})
	}

	CaseWhenInt32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[int32](vs, proc, types.Type{Oid: types.T_int32})
	}

	CaseWhenInt64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[int64](vs, proc, types.Type{Oid: types.T_int64})
	}

	CaseWhenFloat32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[float32](vs, proc, types.Type{Oid: types.T_float32})
	}

	CaseWhenFloat64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[float64](vs, proc, types.Type{Oid: types.T_float64})
	}

	CaseWhenBool = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[bool](vs, proc, types.Type{Oid: types.T_bool})
	}

	CaseWhenDate = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[types.Date](vs, proc, types.Type{Oid: types.T_date})
	}

	CaseWhenTime = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[types.Time](vs, proc, types.Type{Oid: types.T_time})
	}

	CaseWhenDateTime = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[types.Datetime](vs, proc, types.Type{Oid: types.T_datetime})
	}

	CaseWhenVarchar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwString(vs, proc, types.Type{Oid: types.T_varchar})
	}

	CaseWhenChar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwString(vs, proc, types.Type{Oid: types.T_char})
	}

	CaseWhenDecimal64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[types.Decimal64](vs, proc, types.Type{Oid: types.T_decimal64})
	}

	CaseWhenDecimal128 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[types.Decimal128](vs, proc, types.Type{Oid: types.T_decimal128})
	}

	CaseWhenTimestamp = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[types.Timestamp](vs, proc, types.Type{Oid: types.T_timestamp})
	}

	CaseWhenUuid = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwGeneral[types.Uuid](vs, proc, types.Type{Oid: types.T_uuid})
	}

	CaseWhenBlob = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwString(vs, proc, types.Type{Oid: types.T_blob})
	}

	CaseWhenText = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return cwString(vs, proc, types.Type{Oid: types.T_text})
	}
)

// CwTypeCheckFn is type check function for case-when operator
func CwTypeCheckFn(inputTypes []types.T, _ []types.T, ret types.T) bool {
	l := len(inputTypes)
	if l >= 2 {
		for i := 0; i < l-1; i += 2 {
			if inputTypes[i] != types.T_bool {
				return false
			}
		}

		if l%2 == 1 {
			if inputTypes[l-1] != ret && inputTypes[l-1] != types.T_any {
				return false
			}
		}

		for i := 1; i < l; i += 2 {
			if inputTypes[i] != ret && inputTypes[i] != types.T_any {
				return false
			}
		}
		return true
	}
	return false
}

type OrderedValue interface {
	constraints.Integer | constraints.Float | types.Date | types.Datetime | types.Decimal64 | types.Timestamp
}

type NormalType interface {
	constraints.Integer | constraints.Float | bool | types.Date | types.Datetime |
		types.Decimal64 | types.Decimal128 | types.Timestamp | types.Uuid
}

// cwGeneral is a general evaluate function for case-when operator
// whose return type is uint / int / float / bool / date / datetime
func cwGeneral[T NormalType](vs []*vector.Vector, proc *process.Process, t types.Type) (*vector.Vector, error) {
	l := vector.Length(vs[0])

	rs, err := proc.AllocVector(t, int64(l*t.Oid.TypeLen()))
	if err != nil {
		return nil, err
	}
	rs.Col = vector.DecodeFixedCol[T](rs, t.Oid.TypeLen())
	rs.Col = rs.Col.([]T)[:l]
	rscols := rs.Col.([]T)

	flag := make([]bool, l) // if flag[i] is false, it couldn't adapt to any case

	for i := 0; i < len(vs)-1; i += 2 {
		whenv := vs[i]
		thenv := vs[i+1]
		whencols := vector.MustTCols[bool](whenv)
		thencols := vector.MustTCols[T](thenv)
		switch {
		case whenv.IsScalar() && thenv.IsScalar():
			if !whenv.IsScalarNull() && whencols[0] {
				if thenv.IsScalarNull() {
					return proc.AllocScalarNullVector(t), nil
				} else {
					r := proc.AllocScalarVector(t)
					r.Typ.Precision = thenv.Typ.Precision
					r.Typ.Width = thenv.Typ.Width
					r.Typ.Scale = thenv.Typ.Scale
					r.Col = make([]T, 1)
					r.Col.([]T)[0] = thencols[0]
					return r, nil
				}
			}
		case whenv.IsScalar() && !thenv.IsScalar():
			rs.Typ.Precision = thenv.Typ.Precision
			rs.Typ.Width = thenv.Typ.Width
			rs.Typ.Scale = thenv.Typ.Scale
			if !whenv.IsScalarNull() && whencols[0] {
				copy(rscols, thencols)
				rs.Nsp.Or(thenv.Nsp)
				return rs, nil
			}
		case !whenv.IsScalar() && thenv.IsScalar():
			rs.Typ.Precision = thenv.Typ.Precision
			rs.Typ.Width = thenv.Typ.Width
			rs.Typ.Scale = thenv.Typ.Scale
			if thenv.IsScalarNull() {
				var j uint64
				temp := make([]uint64, 0, l)
				for j = 0; j < uint64(l); j++ {
					if flag[j] {
						continue
					}
					if whencols[j] {
						temp = append(temp, j)
						flag[j] = true
					}
				}
				nulls.Add(rs.Nsp, temp...)
			} else {
				for j := 0; j < l; j++ {
					if flag[j] {
						continue
					}
					if whencols[j] {
						rscols[j] = thencols[0]
						flag[j] = true
					}
				}
			}
		case !whenv.IsScalar() && !thenv.IsScalar():
			rs.Typ.Precision = thenv.Typ.Precision
			rs.Typ.Width = thenv.Typ.Width
			rs.Typ.Scale = thenv.Typ.Scale
			if nulls.Any(thenv.Nsp) {
				var j uint64
				temp := make([]uint64, 0, l)
				for j = 0; j < uint64(l); j++ {
					if whencols[j] {
						if flag[j] {
							continue
						}
						if nulls.Contains(thenv.Nsp, j) {
							temp = append(temp, j)
						} else {
							rscols[j] = thencols[j]
						}
						flag[j] = true
					}
				}
				nulls.Add(rs.Nsp, temp...)
			} else {
				for j := 0; j < l; j++ {
					if whencols[j] {
						if flag[j] {
							continue
						}
						rscols[j] = thencols[j]
						flag[j] = true
					}
				}
			}
		}
	}

	// deal the ELSE part
	if len(vs)%2 == 0 || vs[len(vs)-1].IsScalarNull() {
		var i uint64
		temp := make([]uint64, 0, l)
		for i = 0; i < uint64(l); i++ {
			if !flag[i] {
				temp = append(temp, i)
			}
		}
		nulls.Add(rs.Nsp, temp...)
	} else {
		ev := vs[len(vs)-1]
		ecols := ev.Col.([]T)
		if ev.IsScalar() {
			for i := 0; i < l; i++ {
				if !flag[i] {
					rscols[i] = ecols[0]
				}
			}
		} else {
			if nulls.Any(ev.Nsp) {
				var i uint64
				temp := make([]uint64, 0, l)
				for i = 0; i < uint64(l); i++ {
					if !flag[i] {
						if nulls.Contains(ev.Nsp, i) {
							temp = append(temp, i)
						} else {
							rscols[i] = ecols[i]
						}
					}
				}
				nulls.Add(rs.Nsp, temp...)
			} else {
				for i := 0; i < l; i++ {
					if !flag[i] {
						rscols[i] = ecols[i]
					}
				}
			}
		}
	}

	return rs, nil
}

// cwString is an evaluate function for case-when operator
// whose return type is char / varchar
func cwString(vs []*vector.Vector, proc *process.Process, typ types.Type) (*vector.Vector, error) {
	nres := vector.Length(vs[0])
	results := make([]string, nres)
	nsp := nulls.NewWithSize(nres)
	flag := make([]bool, nres)

	for i := 0; i < len(vs)-1; i += 2 {
		whenv := vs[i]
		thenv := vs[i+1]
		whencols := vector.MustTCols[bool](whenv)
		thencols := vector.MustStrCols(thenv)
		switch {
		case whenv.IsScalar() && thenv.IsScalar():
			if !whenv.IsScalarNull() && whencols[0] {
				if thenv.IsScalarNull() {
					for idx := range results {
						if !flag[idx] {
							nsp.Np.Add(uint64(idx))
							flag[idx] = true
						}
					}
				} else {
					for idx := range results {
						if !flag[idx] {
							results[idx] = thencols[0]
							flag[idx] = true
						}
					}
				}
			}
		case whenv.IsScalar() && !thenv.IsScalar():
			if !whenv.IsScalarNull() && whencols[0] {
				for idx := range results {
					if !flag[idx] {
						if nulls.Contains(thenv.Nsp, uint64(idx)) {
							nsp.Np.Add(uint64(idx))
						} else {
							results[idx] = thencols[idx]
						}
						flag[idx] = true
					}
				}
			}
		case !whenv.IsScalar() && thenv.IsScalar():
			if thenv.IsScalarNull() {
				for idx := range results {
					if !flag[idx] {
						if !nulls.Contains(whenv.Nsp, uint64(idx)) && whencols[idx] {
							nsp.Np.Add(uint64(idx))
							flag[idx] = true
						}
					}
				}
			} else {
				for idx := range results {
					if !flag[idx] {
						if !nulls.Contains(whenv.Nsp, uint64(idx)) && whencols[idx] {
							results[idx] = thencols[0]
							flag[idx] = true
						}
					}
				}
			}
		case !whenv.IsScalar() && !thenv.IsScalar():
			for idx := range results {
				if !flag[idx] {
					if !nulls.Contains(whenv.Nsp, uint64(idx)) && whencols[idx] {
						if nulls.Contains(thenv.Nsp, uint64(idx)) {
							nsp.Np.Add(uint64(idx))
						} else {
							results[idx] = thencols[idx]
						}
						flag[idx] = true
					}
				}
			}
		}
	}

	// deal the ELSE part
	if len(vs)%2 == 0 || vs[len(vs)-1].IsScalarNull() {
		for idx := range results {
			if !flag[idx] {
				nulls.Add(nsp, uint64(idx))
				flag[idx] = true
			}
		}
	} else {
		ev := vs[len(vs)-1]
		ecols := vector.MustStrCols(ev)
		if ev.IsScalar() {
			for idx := range results {
				if !flag[idx] {
					results[idx] = ecols[0]
					flag[idx] = true
				}
			}
		} else {
			for idx := range results {
				if !flag[idx] {
					if nulls.Contains(ev.Nsp, uint64(idx)) {
						nulls.Add(nsp, uint64(idx))
					} else {
						results[idx] = ecols[idx]
					}
					flag[idx] = true
				}
			}
		}
	}

	return vector.NewWithStrings(typ, results, nsp, proc.Mp()), nil
}
