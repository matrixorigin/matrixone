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
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/fromunixtime"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["from_unixtime"] = builtin.FromUnixTime
	overload.OpTypes[builtin.FromUnixTime] = overload.Multi
	extend.MultiStrings[builtin.FromUnixTime] = func(e []extend.Extend) string {
		return fmt.Sprintf("from_unixtime(%s)", e)
	}

	overload.AppendFunctionRets(builtin.FromUnixTime, []types.T{types.T_int64}, types.T_datetime)
	overload.AppendFunctionRets(builtin.FromUnixTime, []types.T{types.T_int64, types.T_varchar}, types.T_varchar)
	extend.MultiReturnTypes[builtin.FromUnixTime] = func(e []extend.Extend) types.T {
		return getMultiReturnType(builtin.FromUnixTime, e)
	}

	overload.MultiOps[builtin.FromUnixTime] = []*overload.MultiOp{
		{
			Min:        1,
			Max:        2,
			Typ:        types.T_int64,
			ReturnType: types.T_any,
			Fn: func(lv []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				if len(lv) >= 2 {
					return nil, errors.New("from_unixtime doesn't support custome format for now")
				}
				inVec := lv[0]
				times := inVec.Col.([]int64)
				size := types.T(types.T_datetime).TypeLen()

				vec, err := process.Get(proc, int64(size)*int64(len(times)), types.Type{Oid: types.T_datetime, Size: int32(size)})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeDatetimeSlice(vec.Data)
				rs = rs[:len(times)]
				nulls.Set(vec.Nsp, inVec.Nsp)
				vector.SetCol(vec, fromunixtime.UnixToDatetime(times, rs))
				return vec, nil
			},
		},
	}
}
