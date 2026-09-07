// Copyright 2026 Matrix Origin
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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// jsonStorageTypeMatch accepts the JSON domain and every MySQL string domain.
// T_any is only accepted as the planner's NULL/text placeholder and is bound
// to VARCHAR; numeric and other scalar types must not be implicitly stringified.
func jsonStorageTypeMatch(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) != 1 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	switch inputs[0].Oid {
	case types.T_json:
		return newCheckResultWithSuccess(0)
	case types.T_any:
		return newCheckResultWithCast(1, []types.Type{types.T_varchar.ToType()})
	default:
		if inputs[0].Oid.IsMySQLString() {
			return newCheckResultWithSuccess(1)
		}
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
}

func jsonStorageInvalidArg(proc *process.Process, functionName string) error {
	return moerr.NewInvalidArg(proc.Ctx, functionName, "invalid JSON document")
}

func JsonStorageSize(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	if ivecs[0].GetType().Oid == types.T_json {
		return opUnaryBytesToFixed[int64](ivecs, result, proc, length,
			func(value []byte) int64 { return int64(len(value)) }, selectList)
	}
	return opUnaryBytesToFixedWithErrorCheck[int64](ivecs, result, proc, length,
		func(value []byte) (int64, error) {
			bj, err := types.ParseSliceToByteJson(value)
			if err != nil {
				return 0, jsonStorageInvalidArg(proc, "json_storage_size")
			}
			return int64(1 + len(bj.Data)), nil
		}, selectList)
}

func JsonStorageFree(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	return opUnaryBytesToFixedWithErrorCheck[int64](ivecs, result, proc, length,
		func(value []byte) (int64, error) {
			if ivecs[0].GetType().Oid == types.T_json {
				return 0, nil
			}
			if _, err := types.ParseSliceToByteJson(value); err != nil {
				return 0, jsonStorageInvalidArg(proc, "json_storage_free")
			}
			return 0, nil
		}, selectList)
}
