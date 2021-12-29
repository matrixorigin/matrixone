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

package multi

import (
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Round(xs, rs []float64, dec int) []float64 {
	for i, x := range xs {
		rs[i] = x
	}
	return rs
}

func init() {
	extend.FunctionRegistry["round"] = builtin.Round
	overload.AppendFunctionRets(builtin.Round, []types.T{types.T_float64, types.T_float64}, types.T_float64)
	extend.MultiReturnTypes[builtin.Round] = func(es []extend.Extend) types.T {
		return getMultiReturnType(builtin.Round, es)
	}
	extend.MultiStrings[builtin.Round] = func(es []extend.Extend) string {
		if len(es) > 1 {
			return fmt.Sprintf("round(%s, %s)", es[0], es[1])
		} else {
			return fmt.Sprintf("round(%s)", es[0])
		}
	}
	overload.OpTypes[builtin.Round] = overload.Multi
	overload.MultiOps[builtin.Round] = []*overload.MultiOp{
		{
			Min:        1,
			Max:        2,
			Typ:        types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(vecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				dec := int(0)
				vs := vecs[0].Col.([]float64)
				if len(vecs) > 1 {
					if !cs[1] && vecs[1].Typ.Oid != types.T_int64 {
						return nil, errors.New("The second argument of the round function must be an int64 constant")
					}
					dec = int(vecs[1].Col.([]int64)[0])
				}
				if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
					vecs[0].Ref = 0
					Round(vs, vs, dec)
					return vecs[0], nil
				}
				vec, err := process.Get(proc, 8*int64(len(vs)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, vecs[0].Nsp)
				vector.SetCol(vec, Round(vs, rs, dec))
				return vec, nil
			},
		},
	}
}
