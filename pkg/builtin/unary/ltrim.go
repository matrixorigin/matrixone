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

package unary

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/ltrim"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["ltrim"] = builtin.Ltrim

	// register function args and returns
	overload.AppendFunctionRets(builtin.Ltrim, []types.T{types.T_char}, types.T_char)
	overload.AppendFunctionRets(builtin.Ltrim, []types.T{types.T_varchar}, types.T_varchar)

	extend.UnaryReturnTypes[builtin.Ltrim] = func(extend extend.Extend) types.T {
		return getUnaryReturnType(builtin.Ltrim, extend) // register a get return type function
	}

	extend.UnaryStrings[builtin.Ltrim] = func(extend extend.Extend) string {
		return fmt.Sprintf("ltrim(%s)", extend) // define a stringify function
	}

	overload.OpTypes[builtin.Ltrim] = overload.Unary // register function type

	overload.UnaryOps[builtin.Ltrim] = []*overload.UnaryOp{
		{
			Typ:        types.T_varchar,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.(*types.Bytes)

				// totalCount - spaceCount is the total bytes need for the ltrim-ed string
				spaceCount := ltrim.CountSpacesFromLeft(origVecCol)
				totalCount := int32(len(origVecCol.Data))

				resultVector, err := process.Get(proc, int64(totalCount-spaceCount), types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol.Offsets)),
					Lengths: make([]uint32, len(origVecCol.Lengths)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ltrim.LtrimVarChar(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_char,
			ReturnType: types.T_char,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.(*types.Bytes)

				// totalCount - spaceCount is the total bytes need for the ltrim-ed string
				spaceCount := ltrim.CountSpacesFromLeft(origVecCol)
				totalCount := int32(len(origVecCol.Data))

				resultVector, err := process.Get(proc, int64(totalCount-spaceCount), types.Type{Oid: types.T_char, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol.Offsets)),
					Lengths: make([]uint32, len(origVecCol.Lengths)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ltrim.LtrimChar(origVecCol, results))
				return resultVector, nil
			},
		},
	}
}
