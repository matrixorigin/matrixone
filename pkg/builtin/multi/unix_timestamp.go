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
	"github.com/matrixorigin/matrixone/pkg/vectorize/unixtimestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["unix_timestamp"] = builtin.UnixTimestamp
	overload.OpTypes[builtin.UnixTimestamp] = overload.Multi
	extend.MultiStrings[builtin.UnixTimestamp] = func(e []extend.Extend) string {
		return fmt.Sprintf("unix_timestamp(%s)", e)
	}

	overload.AppendFunctionRets(builtin.UnixTimestamp, []types.T{}, types.T_int64)
	// TODO overload.AppendFunctionRets(builtin.UnixTimestamp, []types.T{types.T_date}, types.T_int64)
	overload.AppendFunctionRets(builtin.UnixTimestamp, []types.T{types.T_datetime}, types.T_int64)
	extend.MultiReturnTypes[builtin.UnixTimestamp] = func(e []extend.Extend) types.T {
		return getMultiReturnType(builtin.UnixTimestamp, e)
	}

	overload.MultiOps[builtin.UnixTimestamp] = []*overload.MultiOp{
		{
			Min:        0,
			Max:        0,
			ReturnType: types.T_int64,
			Fn: func(_ []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				return nil, errors.New("unix_timestamp must take one argument for now")
			},
		},
		{
			Min:        1,
			Max:        1,
			Typ:        types.T_datetime,
			ReturnType: types.T_int64,
			Fn: func(lv []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				if len(lv) != 1 {
					return nil, errors.New("unix_timestamp must take one argument for now 1/1")
				}
				inVec := lv[0]
				times := inVec.Col.([]types.Datetime)
				size := types.T(types.T_int64).TypeLen()

				vec, err := process.Get(proc, int64(size)*int64(len(times)), types.Type{Oid: types.T_int64, Size: int32(size)})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(times)]
				nulls.Set(vec.Nsp, inVec.Nsp)
				vector.SetCol(vec, unixtimestamp.UnixTimestamp(times, rs))
				return vec, nil
			},
		},
	}
}
