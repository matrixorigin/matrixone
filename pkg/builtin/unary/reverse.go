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
package unary

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/reverse"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var ReverseArgAndRets = []argsAndRet{
	{[]types.T{types.T_char}, types.T_char},
	{[]types.T{types.T_varchar}, types.T_varchar},
}

func init() {
	// register built-in func and its args and rets
	extend.FunctionRegistry["reverse"] = builtin.Reverse
	overload.OpTypes[builtin.Reverse] = overload.Unary

	for _, item := range ReverseArgAndRets {
		overload.AppendFunctionRets(builtin.Reverse, item.args, item.ret)
	}

	extend.UnaryReturnTypes[builtin.Reverse] = func(extend extend.Extend) types.T {
		return getUnaryReturnType(builtin.Reverse, extend)
	}
	// add built-in stringify
	extend.UnaryStrings[builtin.Reverse] = func(extend extend.Extend) string {
		return fmt.Sprintf("reverse(%s)", extend)
	}

	overload.UnaryOps[builtin.Reverse] = []*overload.UnaryOp{
		{
			Typ:        types.T_varchar,
			ReturnType: types.T_varchar,
			Fn: func(inputVec *vector.Vector, proc *process.Process, b bool) (*vector.Vector, error) {
				inputVecCol := inputVec.Col.(*types.Bytes)

				if inputVec.Ref == 1 || inputVec.Ref == 0 { // reuse the original vector when we don't need the original one anymore
					inputVec.Ref = 0
					reverse.ReverseVarChar(inputVecCol, inputVecCol)
					return inputVec, nil
				}

				resultVec, err := process.Get(proc, int64(len(inputVecCol.Data)), types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVec.Data,
					Offsets: make([]uint32, len(inputVecCol.Offsets)),
					Lengths: make([]uint32, len(inputVecCol.Lengths)),
				}
				nulls.Set(resultVec.Nsp, inputVec.Nsp)
				vector.SetCol(resultVec, reverse.ReverseVarChar(inputVecCol, results))
				return resultVec, nil
			},
		},
		{
			Typ:        types.T_char,
			ReturnType: types.T_char,
			Fn: func(inputVec *vector.Vector, proc *process.Process, b bool) (*vector.Vector, error) {
				inputVecCol := inputVec.Col.(*types.Bytes)

				if inputVec.Ref == 1 || inputVec.Ref == 0 { // reuse the original vector when we don't need the original one anymore
					inputVec.Ref = 0
					reverse.ReverseChar(inputVecCol, inputVecCol)
					return inputVec, nil
				}

				resultVec, err := process.Get(proc, int64(len(inputVecCol.Data)), types.Type{Oid: types.T_char, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVec.Data,
					Offsets: make([]uint32, len(inputVecCol.Offsets)),
					Lengths: make([]uint32, len(inputVecCol.Lengths)),
				}
				nulls.Set(resultVec.Nsp, inputVec.Nsp)
				vector.SetCol(resultVec, reverse.ReverseChar(inputVecCol, results))
				return resultVec, nil
			},
		},
	}

}
