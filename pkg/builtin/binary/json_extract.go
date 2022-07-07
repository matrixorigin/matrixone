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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func json_extract_fn(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
	lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
	var resultsLen int
	if len(lvs.Lengths) > len(rvs.Lengths) {
		resultsLen = len(lvs.Lengths)
	} else {
		resultsLen = len(rvs.Lengths)
	}

	resultVector, err := process.Get(proc, int64(resultsLen), types.Type{Oid: types.T_varchar, Size: 255})
	if err != nil {
		return nil, err
	}
	results := encoding.DecodeStringSlice(resultVector.Data)
	results = results[:resultsLen]
	resultVector.Col = results
	nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)

	return resultVector, nil
}

func init() {
	extend.FunctionRegistry["json_extract"] = builtin.JsonExtract
	extend.BinaryReturnTypes[builtin.JsonExtract] = func(e extend.Extend, e2 extend.Extend) types.T {
		return types.T_varchar
	}
	extend.BinaryStrings[builtin.JsonExtract] = func(e extend.Extend, e2 extend.Extend) string {
		return fmt.Sprintf("json_extract(%s, %s)", e, e2)
	}

	overload.OpTypes[builtin.JsonExtract] = overload.Binary
	overload.BinOps[builtin.JsonExtract] = []*overload.BinOp{
		{
			LeftType:   types.T_varchar,
			RightType:  types.T_varchar,
			ReturnType: types.T_varchar,
			Fn:         json_extract_fn,
		},
	}
}
