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
	constraints.Integer | constraints.Float | bool
}

// CwTypeCheckFn is type check function for case-when operator
func CwTypeCheckFn(inputTypes []types.T, args []types.T) bool {
	rt := args[0]
	l := len(inputTypes)
	if l >= 2 {
		for i := 0; i < l-1; i += 2 {
			if inputTypes[i] != types.T_bool {
				return false
			}
		}
		for i := 1; i < l; i += 2 {
			if inputTypes[i] != rt && inputTypes[i] != types.T_any {
				return false
			}
		}
		return true
	}
	return false
}

func CwFn[T Ret](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	t := vs[1].Typ
	l := vector.Length(vs[0])
	rs, err := proc.AllocVector(t, int64(l*t.Oid.TypeLen()))
	if err != nil {
		return nil, err
	}
	rs.Col = vector.DecodeFixedCol[T](rs, t.Oid.FixedLength())
	rs.Col = rs.Col.([]T)[:l]
	rscols := rs.Col.([]T)

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
				for j := 0; j < l; j++ {
					rscols[j] = thencols[j]
				}
				rs.Nsp.Or(thenv.Nsp)
			}
		case !whenv.IsScalar() && thenv.IsScalar():
			if thenv.IsScalarNull() {
				var j uint64
				temp := make([]uint64, 0, l)
				for j = 0; j < uint64(l); j++ {
					if whencols[j] {
						temp = append(temp, j)
					}
				}
				nulls.Add(rs.Nsp, temp...)
			} else {
				for j := 0; j < l; j++ {
					if whencols[j] {
						rscols[j] = thencols[j]
					}
				}
			}
		case !whenv.IsScalar() && !thenv.IsScalar():
			for j := 0; j < l; j++ {
				if whencols[j] {
					rscols[j] = thencols[j]
				}
			}
		}
	}
	return rs, nil
}
