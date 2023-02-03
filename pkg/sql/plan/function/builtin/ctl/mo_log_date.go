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

package ctl

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func MOLogDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if !vectors[1].IsScalar() {
		return nil, moerr.NewInvalidArg(proc.Ctx, "the second parameter of function to_date", "not constant")
	}
	inputBytes0 := vector.MustStrCols(vectors[0])
	inputBytes1 := vector.MustStrCols(vectors[1])
	resultType := types.Type{Oid: types.T_varchar, Size: 24, Width: types.MaxVarcharLen}
	if vectors[0].IsScalar() && vectors[1].IsScalar() {
		results := make([]string, 1)
		format := inputBytes1[0]
		inputNsp := vectors[0].Nsp
		result, resultNsp, err := binary.ToDateInputBytes(proc.Ctx, inputBytes0, format, inputNsp, results)
		if err != nil {
			return nil, err
		}
		resultVector := vector.NewConstString(resultType, 1, result[0], proc.Mp())
		nulls.Set(resultVector.Nsp, resultNsp)
		return resultVector, nil
	} else {
		results := make([]string, len(inputBytes0))
		format := inputBytes1[0]
		inputNsp := vectors[0].Nsp
		results, resultNsp, err := binary.ToDateInputBytes(proc.Ctx, inputBytes0, format, inputNsp, results)
		if err != nil {
			return nil, err
		}
		resultVector := vector.NewWithStrings(resultType, results, resultNsp, proc.Mp())
		return resultVector, nil
	}
}
