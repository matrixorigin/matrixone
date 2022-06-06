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
//	`
//		case
//		when A = a1 then ...
//		when A = a2 then ...
//		when A = a3 then ...
//		(else ...)
//	`
// format `case A when a1 then ... when a2 then ...` should be converted to required format.

type Ret interface {
	constraints.Integer | constraints.Float | bool | types.Date | types.Datetime
}

// CwTypeCheckFn is type check function for case-when operator
func CwTypeCheckFn(inputTypes []types.T, _ []types.T, ret types.T) bool {
	l := len(inputTypes)
	if l >= 2 {
		for i := 0; i < l-1; i += 2 {
			if inputTypes[i] != types.T_bool {
				return false
			}
		}

		allNull := true // result part (contains then part and else part) should have at least 1 owns exact type
		for i := 1; i < l; i += 2 {
			if inputTypes[i] == ret {
				allNull = false
			} else {
				if inputTypes[i] != types.T_any {
					return false
				}
			}
		}
		return !allNull
	}
	return false
}

// CwFn1 is fn for uint / int / float / bool
func CwFn1[T Ret](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var t = types.T_bool.ToType() // result vector's type
	l := vector.Length(vs[0])
	for i := 1; i < len(vs); i += 2 {
		if !vs[i].IsScalarNull() {
			t = vs[i].Typ
			break
		}
	}

	rs, err := proc.AllocVector(t, int64(l*t.Oid.TypeLen()))
	if err != nil {
		return nil, err
	}
	rs.Col = vector.DecodeFixedCol[T](rs, t.Oid.FixedLength())
	rs.Col = rs.Col.([]T)[:l]
	rscols := rs.Col.([]T)

	flag := make([]bool, l) // if flag[i] is false, it couldn't adapt to any case

	for i := 0; i < len(vs)-1; i += 2 {
		whenv := vs[i]
		thenv := vs[i+1]
		whencols := whenv.Col.([]bool)
		thencols := thenv.Col.([]T)
		switch {
		case whenv.IsScalar() && thenv.IsScalar():
			if whencols[0] {
				if thenv.IsScalarNull() {
					return proc.AllocScalarNullVector(t), nil
				} else {
					r := proc.AllocScalarVector(t)
					r.Col = make([]T, 1)
					r.Col.([]T)[0] = thencols[0]
					return r, nil
				}
			}
		case whenv.IsScalar() && !thenv.IsScalar():
			if whencols[0] {
				copy(rscols, thencols)
				rs.Nsp.Or(thenv.Nsp)
				return rs, nil
			}
		case !whenv.IsScalar() && thenv.IsScalar():
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
