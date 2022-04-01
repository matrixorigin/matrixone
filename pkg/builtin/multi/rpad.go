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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/rpad"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	// register function name
	extend.FunctionRegistry["rpad"] = builtin.Rpad

	secondArgs := []types.T{types.T_int64, types.T_int32, types.T_int16, types.T_int8, types.T_uint64, types.T_uint32,
		types.T_uint16, types.T_uint8, types.T_float32, types.T_float64, types.T_varchar, types.T_char}
	thirdArgs := []types.T{types.T_varchar, types.T_char, types.T_int64, types.T_int32, types.T_int16, types.T_int8,
		types.T_uint64, types.T_uint32, types.T_uint16, types.T_uint8, types.T_float32, types.T_float64}
	for _, a1 := range []types.T{types.T_varchar, types.T_char} {
		for _, a2 := range secondArgs {
			for _, a3 := range thirdArgs {
				overload.AppendFunctionRets(builtin.Rpad, []types.T{a1, a2, a3}, a1)
			}
		}
	}

	// define a get return type function
	extend.MultiReturnTypes[builtin.Rpad] = func(extends []extend.Extend) types.T {
		return getMultiReturnType(builtin.Rpad, extends)
	}
	// define a stringify function
	extend.MultiStrings[builtin.Rpad] = func(es []extend.Extend) string {
		return fmt.Sprintf("rpad(%s,%s,%s)", es[0], es[1], es[2])
	}
	// register rpad function type
	overload.OpTypes[builtin.Rpad] = overload.Multi
	overload.MultiOps[builtin.Rpad] = []*overload.MultiOp{
		// T_varchar
		{
			Min:        3,
			Max:        3,
			Typ:        types.T_varchar,
			ReturnType: types.T_varchar,
			Fn: func(origVecs []*vector.Vector, proc *process.Process, isConst []bool) (*vector.Vector, error) {
				// gets all args
				strs, sizes, padstrs := origVecs[0].Col.(*types.Bytes), origVecs[1].Col, origVecs[2].Col
				oriNsps := []*nulls.Nulls{origVecs[0].Nsp, origVecs[1].Nsp, origVecs[2].Nsp}

				if origVecs[0].Ref == 1 || origVecs[1].Ref == 0 {
					// uses the original vector to store our result if it isn't needed anymore
					origVecs[0].Ref = 0
					result, nsp, err := rpad.Rpad(strs, sizes, padstrs, isConst, oriNsps)
					if err != nil {
						return nil, err
					}
					origVecs[0].Nsp = nsp
					vector.SetCol(origVecs[0], result)
					return origVecs[0], nil
				}
				// gets a new vector to store our result
				resultVec, err := process.Get(proc, 24*int64(len(strs.Lengths)), types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				result, nsp, err := rpad.Rpad(strs, sizes, padstrs, isConst, oriNsps)
				if err != nil {
					return nil, err
				}
				resultVec.Nsp = nsp
				vector.SetCol(resultVec, result)
				return resultVec, nil
			},
		},
		// T_char
		{
			Min:        3,
			Max:        3,
			Typ:        types.T_char,
			ReturnType: types.T_char,
			Fn: func(origVecs []*vector.Vector, proc *process.Process, isConst []bool) (*vector.Vector, error) {
				// gets all args
				strs, sizes, padstrs := origVecs[0].Col.(*types.Bytes), origVecs[1].Col, origVecs[2].Col
				oriNsps := []*nulls.Nulls{origVecs[0].Nsp, origVecs[1].Nsp, origVecs[2].Nsp}
				if origVecs[0].Ref == 1 || origVecs[1].Ref == 0 {
					// uses the original vector to store our result if it isn't needed anymore
					origVecs[0].Ref = 0
					result, nsp, err := rpad.Rpad(strs, sizes, padstrs, isConst, oriNsps)
					if err != nil {
						return nil, err
					}
					origVecs[0].Nsp = nsp
					vector.SetCol(origVecs[0], result)
					return origVecs[0], nil
				}
				// gets a new vector to store our result
				resultVec, err := process.Get(proc, 24*int64(len(strs.Lengths)), types.Type{Oid: types.T_char, Size: 24})
				if err != nil {
					return nil, err
				}
				result, nsp, err := rpad.Rpad(strs, sizes, padstrs, isConst, oriNsps)
				if err != nil {
					return nil, err
				}
				resultVec.Nsp = nsp
				vector.SetCol(resultVec, result)
				return resultVec, nil
			},
		},
	}
}
