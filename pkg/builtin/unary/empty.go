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
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/empty"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["empty"] = builtin.Empty

	// register function args and returns
	overload.AppendFunctionRets(builtin.Empty, []types.T{types.T_char}, types.T_uint8)
	overload.AppendFunctionRets(builtin.Empty, []types.T{types.T_varchar}, types.T_uint8)

	extend.UnaryReturnTypes[builtin.Empty] = func(extend extend.Extend) types.T {
		return getUnaryReturnType(builtin.Empty, extend) // register a get return type function
	}

	extend.UnaryStrings[builtin.Empty] = func(e extend.Extend) string {
		return fmt.Sprintf("empty(%s)", e) // define a stringify function
	}

	overload.OpTypes[builtin.Empty] = overload.Unary // register function type

	overload.UnaryOps[builtin.Empty] = []*overload.UnaryOp{
		{
			Typ:        types.T_varchar,
			ReturnType: types.T_uint8,
			Fn:         emptyFn,
		},
		{
			Typ:        types.T_char,
			ReturnType: types.T_uint8,
			Fn:         emptyFn,
		},
	}
}

func emptyFn(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
	col := origVec.Col.(*types.Bytes)
	resultLen := len(col.Lengths)
	resultVec, err := process.Get(proc, int64(resultLen), types.Type{Oid: types.T_uint8, Size: 1})
	if err != nil {
		return nil, err
	}
	result := encoding.DecodeUint8Slice(resultVec.Data)
	result = result[:resultLen]
	resultVec.Col = result
	// the new vector's nulls are the same as the original vector
	nulls.Set(resultVec.Nsp, origVec.Nsp)
	vector.SetCol(resultVec, empty.Empty(col, result))
	return resultVec, nil
}
