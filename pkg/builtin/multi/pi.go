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
	"github.com/matrixorigin/matrixone/pkg/vectorize/pi"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["pi"] = builtin.Pi
	overload.OpTypes[builtin.Pi] = overload.Multi
	extend.MultiReturnTypes[builtin.Pi] = func(_ []extend.Extend) types.T {
		return types.T_float64
	}
	extend.MultiStrings[builtin.Pi] = func(e []extend.Extend) string {
		return fmt.Sprintf("pi()")
	}
	overload.AppendFunctionRets(builtin.Pi, []types.T{}, types.T_float64)
	//logutil.Warn("negative_number_error")
	overload.MultiOps[builtin.Pi] = []*overload.MultiOp{
		{
			Min: 0,
			Max: 0,
			//Typ:        types.T_any,
			ReturnType: types.T_float64,
			Fn: func(lv []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				if len(lv) != 0 {
					return nil, errors.New("pi() takes no arguments")
				}
				vec, err := process.Get(proc, 0, types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				//vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
				//nulls.Set(vec.Nsp, )
				//vec.Nsp = nil
				nulls.Set(vec.Nsp, new(nulls.Nulls))
				result := make([]float64, 0)
				result = append(result, pi.GetPi())
				vector.SetCol(vec, result)
				return vec, nil
			},
		},
	}
}
