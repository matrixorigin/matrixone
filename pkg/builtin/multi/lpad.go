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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lpad"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var ArgAndRets_Lpad = []argsAndRet{
	{[]types.T{types.T_varchar, types.T_int64, types.T_varchar}, types.T_varchar},
}

func init() {
	extend.FunctionRegistry["lpad"] = builtin.Lpad
	for _, item := range ArgAndRets_Lpad {
		overload.AppendFunctionRets(builtin.Lpad, item.args, item.ret)
	}
	extend.MultiReturnTypes[builtin.Lpad] = func(es []extend.Extend) types.T {
		return getMultiReturnType(builtin.Lpad, es)
	}

	extend.MultiStrings[builtin.Lpad] = func(es []extend.Extend) string {
		return fmt.Sprintf("lpad(%s, %s, %s)", es[0], es[1], es[2])
	}
	overload.OpTypes[builtin.Lpad] = overload.Multi
	overload.MultiOps[builtin.Lpad] = []*overload.MultiOp{
		{
			Min:        3,
			Max:        3,
			Typ:        types.T_varchar,
			ReturnType: types.T_varchar,
			Fn: func(vecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				vs := vecs[0].Col.(*types.Bytes) //Get the first arg

				if !cs[1] || vecs[1].Typ.Oid != types.T_int64 {
					return nil, errors.New("The second argument of the lpad function must be an int64 constant")
				} else if !cs[2] || vecs[2].Typ.Oid != types.T_varchar {
					return nil, errors.New("The third argument of the lpad function must be an string constant")
				}
				lens := vecs[1].Col.([]int64)
				padds := vecs[2].Col.(*types.Bytes)
				for _, num := range lens {
					if num < 0 {
						return nil, errors.New("The second argument cant't be negative")
					}
				}
				//use vecs[0] as return
				if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
					vecs[0].Ref = 0
					temp := lpad.LpadVarchar(vs, lens, padds)
					vs.Data = make([]byte, len(temp.Data))
					vs.Lengths = make([]uint32, len(temp.Lengths))
					vs.Offsets = make([]uint32, len(temp.Offsets))
					copy(vs.Data, temp.Data)
					copy(vs.Lengths, temp.Lengths)
					copy(vs.Offsets, temp.Offsets)
					return vecs[0], nil
				}

				vec, err := process.Get(proc, 24*int64(len(vs.Lengths)), types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(vec.Nsp, vecs[0].Nsp)
				vector.SetCol(vec, lpad.LpadVarchar(vs, lens, padds))
				return vec, nil
				// digits := int64(0)
				// vs := vecs[0].Col.([]uint8)
				// if len(vecs) > 1 {
				// 	if !cs[1] && vecs[1].Typ.Oid != types.T_int64 {
				// 		return nil, errors.New("The second argument of the round function must be an int64 constant")
				// 	}
				// 	digits = vecs[1].Col.([]int64)[0]
				// }
				// if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
				// 	vecs[0].Ref = 0
				// 	floor.FloorUint8(vs, vs, digits)
				// 	return vecs[0], nil
				// }
				// vec, err := process.Get(proc, int64(len(vs)), types.Type{Oid: types.T_uint8, Size: 1})
				// if err != nil {
				// 	return nil, err
				// }
				// rs := encoding.DecodeUint8Slice(vec.Data)
				// rs = rs[:len(vs)]
				// vec.Col = rs
				// nulls.Set(vec.Nsp, vecs[0].Nsp)
				// vector.SetCol(vec, floor.FloorUint8(vs, rs, digits))
				// return vec, nil
			},
		},
	}
}
