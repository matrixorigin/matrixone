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

package binary

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/startswith"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func fn(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
	lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)

	var resultsLen int
	if len(lvs.Lengths) > len(rvs.Lengths) {
		resultsLen = len(lvs.Lengths)
	} else {
		resultsLen = len(rvs.Lengths)
	}

	resultVector, err := process.Get(proc, 1*int64(resultsLen), types.Type{Oid: types.T_uint8, Size: 1})
	if err != nil {
		return nil, err
	}
	results := encoding.DecodeUint8Slice(resultVector.Data)
	results = results[:resultsLen]
	resultVector.Col = results
	nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)

	if lc && rc {
		vector.SetCol(resultVector, startswith.StartsWithAllConst(lvs, rvs, results))
	} else if !lc && rc {
		vector.SetCol(resultVector, startswith.StartsWithRightConst(lvs, rvs, results))
	} else if lc && !rc {
		vector.SetCol(resultVector, startswith.StartsWithLeftConst(lvs, rvs, results))
	} else {
		vector.SetCol(resultVector, startswith.StartsWith(lvs, rvs, results))
	}

	return resultVector, nil
}

func init() {
	extend.FunctionRegistry["startswith"] = builtin.StartsWith
	extend.BinaryReturnTypes[builtin.StartsWith] = func(e extend.Extend, e2 extend.Extend) types.T {
		return types.T_uint8
	}

	extend.BinaryStrings[builtin.StartsWith] = func(e extend.Extend, e2 extend.Extend) string {
		return fmt.Sprintf("startsWith(%s, %s)", e, e2)
	}

	overload.OpTypes[builtin.StartsWith] = overload.Binary

	overload.BinOps[builtin.StartsWith] = []*overload.BinOp{
		{
			LeftType:   types.T_char,
			RightType:  types.T_char,
			ReturnType: types.T_uint8,
			Fn:         fn,
		},
		{
			LeftType:   types.T_char,
			RightType:  types.T_varchar,
			ReturnType: types.T_uint8,
			Fn:         fn,
		},
		{
			LeftType:   types.T_varchar,
			RightType:  types.T_char,
			ReturnType: types.T_uint8,
			Fn:         fn,
		},
		{
			LeftType:   types.T_varchar,
			RightType:  types.T_varchar,
			ReturnType: types.T_uint8,
			Fn:         fn,
		},
	}

}
