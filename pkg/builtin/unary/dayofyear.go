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
	"github.com/matrixorigin/matrixone/pkg/vectorize/dayofyear"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	// register built-in func and its args and rets
	extend.FunctionRegistry["dayofyear"] = builtin.DayOfYear
	overload.OpTypes[builtin.DayOfYear] = overload.Unary
	overload.AppendFunctionRets(builtin.DayOfYear, []types.T{types.T_date}, types.T_uint16)
	extend.UnaryReturnTypes[builtin.DayOfYear] = func(e extend.Extend) types.T {
		return getUnaryReturnType(builtin.DayOfYear, e)
	}

	// add built-in stringify
	extend.UnaryStrings[builtin.DayOfYear] = func(e extend.Extend) string {
		return fmt.Sprintf("DayOfYear(%s)", e)
	}

	overload.UnaryOps[builtin.DayOfYear] = []*overload.UnaryOp{
		{
			Typ:        types.T_date,
			ReturnType: types.T_uint16,
			Fn: func(inputVec *vector.Vector, proc *process.Process, b bool) (*vector.Vector, error) {
				inputVecCol := inputVec.Col.([]types.Date)
				resultVec, err := process.Get(proc, 2*int64(len(inputVecCol)), types.Type{Oid: types.T_uint16, Size: 2})
				if err != nil {
					return nil, err
				}

				results := encoding.DecodeUint16Slice(resultVec.Data)[:len(inputVecCol)]
				nulls.Set(resultVec.Nsp, inputVec.Nsp)
				vector.SetCol(resultVec, dayofyear.GetDayOfYear(inputVecCol, results))

				return resultVec, nil
			},
		},
	}

}
