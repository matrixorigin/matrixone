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
	"github.com/matrixorigin/matrixone/pkg/vectorize/utc_date"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["utc_date"] = builtin.UTCDate
	overload.OpTypes[builtin.UTCDate] = overload.Multi
	extend.MultiReturnTypes[builtin.UTCDate] = func(_ []extend.Extend) types.T {
		return types.T_date // return in 'YYYY-MM-DD hh:mm:ss' format
	}
	extend.MultiStrings[builtin.UTCDate] = func(e []extend.Extend) string {
		return fmt.Sprintf("utc_date()")
	}
	overload.AppendFunctionRets(builtin.UTCDate, []types.T{}, types.T_char)
	overload.MultiOps[builtin.UTCDate] = []*overload.MultiOp{
		{
			Min:        0,
			Max:        0,
			ReturnType: types.T_date,
			Fn: func(lv []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				if len(lv) != 0 {
					return nil, errors.New("utc_date() takes no arguments")
				}
				vec, err := process.Get(proc, 0, types.Type{
					Oid: types.T_date, Size: 8,
				})
				if err != nil {
					return nil, err
				}
				nulls.Set(vec.Nsp, new(nulls.Nulls))
				result := make([]types.Date, 0)
				result = append(result, utc_date.GetUTCDate())
				vector.SetCol(vec, result)

				return vec, nil
			},
		},
	}

}
