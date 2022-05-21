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
	"github.com/matrixorigin/matrixone/pkg/vectorize/findinset"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	// register built-in func and its args and rets
	extend.FunctionRegistry["find_in_set"] = builtin.FindInSet
	overload.OpTypes[builtin.FindInSet] = overload.Binary

	extend.BinaryReturnTypes[builtin.FindInSet] = func(e extend.Extend, e2 extend.Extend) types.T {
		return types.T_uint64
	}
	extend.BinaryStrings[builtin.FindInSet] = func(e extend.Extend, e2 extend.Extend) string {
		return fmt.Sprintf("find_in_set(%s, %s)", e, e2)
	}
	overload.BinOps[builtin.FindInSet] = []*overload.BinOp{
		{
			LeftType:   types.T_varchar,
			RightType:  types.T_varchar,
			ReturnType: types.T_uint64,
			Fn:         findInSetFn,
		},
		{
			LeftType:   types.T_char,
			RightType:  types.T_char,
			ReturnType: types.T_uint64,
			Fn:         findInSetFn,
		},
		{
			LeftType:   types.T_varchar,
			RightType:  types.T_char,
			ReturnType: types.T_uint64,
			Fn:         findInSetFn,
		},
		{
			LeftType:   types.T_char,
			RightType:  types.T_varchar,
			ReturnType: types.T_uint64,
			Fn:         findInSetFn,
		},
	}
}

func findInSetFn(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
	lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)

	var resultsLen int
	if len(lvs.Lengths) > len(rvs.Lengths) {
		resultsLen = len(lvs.Lengths)
	} else {
		resultsLen = len(rvs.Lengths)
	}

	retVec, err := process.Get(proc, 8*int64(resultsLen), types.Type{Oid: types.T_uint64, Size: 8})
	if err != nil {
		return nil, err
	}
	results := encoding.DecodeUint64Slice(retVec.Data)
	results = results[:resultsLen]
	retVec.Col = results

	nulls.Or(lv.Nsp, rv.Nsp, retVec.Nsp)

	switch {
	case lc && rc:
		vector.SetCol(retVec, findinset.FindInSetWithAllConst(lvs, rvs, results))
	case lc:
		vector.SetCol(retVec, findinset.FindInSetWithLeftConst(lvs, rvs, results))
	case rc:
		vector.SetCol(retVec, findinset.FindInSetWithRightConst(lvs, rvs, results))
	default:
		vector.SetCol(retVec, findinset.FindInSet(lvs, rvs, results))
	}

	return retVec, nil
}
