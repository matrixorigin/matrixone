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
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	vnow "github.com/matrixorigin/matrixone/pkg/vectorize/now"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["now"] = builtin.Now
	extend.UnaryReturnTypes[builtin.Now] = func(extend extend.Extend) types.T {
		return types.T_datetime
	}

	extend.UnaryStrings[builtin.Now] = func(e extend.Extend) string {
		return fmt.Sprintf("Now(%s)", e)
	}

	overload.OpTypes[builtin.Now] = overload.Unary
	overload.UnaryOps[builtin.Now] = []*overload.UnaryOp{
		{ // T_int64
			Typ:        types.T_int64,
			ReturnType: types.T_datetime,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int64)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_datetime, Size: 8})
				if err != nil {
					return nil, err
				}

				results := encoding.DecodeDatetimeSlice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results

				svec, err := vnow.EvalNowWithFsp(proc, origVecCol, results)
				if err != nil {
					return nil, err
				}

				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, svec)
				return resultVector, err
			},
		},
	}
}
