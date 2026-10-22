// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func jsonParseExactCheckFn(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) == 1 && (inputs[0].Oid.IsMySQLString() || inputs[0].Oid == types.T_any) {
		return newCheckResultWithSuccess(0)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

// JsonParseExact preserves numeric JSON lexemes at the text ingestion boundary.
// It cannot recover precision already lost in a floating point or JSON value.
func JsonParseExact(parameters []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(parameters, result, proc, length,
		func(text []byte) ([]byte, error) {
			bj, err := bytejson.ParseFromByteSliceExact(text)
			if err != nil {
				return nil, err
			}
			return types.EncodeJson(bj)
		}, selectList)
}
