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
	"github.com/matrixorigin/matrixone/pkg/vectorize/get_timestamp"

	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["utc_timestamp"] = builtin.UTCTimestamp
	overload.OpTypes[builtin.UTCTimestamp] = overload.Multi
	extend.MultiReturnTypes[builtin.UTCTimestamp] = func(_ []extend.Extend) types.T {
		return types.T_datetime // return in 'YYYY-MM-DD hh:mm:ss' format
	}
	extend.MultiStrings[builtin.UTCTimestamp] = func(e []extend.Extend) string {
		return "utc_timestamp()"
	}
	overload.AppendFunctionRets(builtin.UTCTimestamp, []types.T{}, types.T_char)
	overload.MultiOps[builtin.UTCTimestamp] = []*overload.MultiOp{
		{
			Min:        0,
			Max:        0,
			ReturnType: types.T_datetime,
			Fn: func(lv []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				if len(lv) != 0 {
					return nil, errors.New("utc_timestamp() takes no arguments")
				}
				vec, err := process.Get(proc, 0, types.Type{
					Oid: types.T_datetime, Size: 8,
				})
				if err != nil {
					return nil, err
				}
				nulls.Set(vec.Nsp, new(nulls.Nulls))
				result := make([]types.Datetime, 0)
				result = append(result, get_timestamp.GetUTCTimestamp())
				vector.SetCol(vec, result)

				return vec, nil
			},
		},
	}

}
