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

package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/log"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func Log[T constraints.Integer | constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0] // so these kinds of indexing are always guaranteed success because all the checks are done in the plan?
	resultType := types.Type{Oid: types.T_float64, Size: 8}
	resultElementSize := int(resultType.Size)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		inputValues := inputVector.Col.([]T)
		resultVector := vector.NewConst(resultType)
		resultValues := make([]float64, 1)

		logResult := log.Log[T](inputValues, resultValues)
		if nulls.Any(logResult.Nsp) {
			return proc.AllocScalarNullVector(resultType), nil
		}
		vector.SetCol(resultVector, logResult.Result)
		return resultVector, nil
	} else {
		inputValues := inputVector.Col.([]T)
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		lnResult := log.Log[T](inputValues, resultValues)
		nulls.Or(inputVector.Nsp, lnResult.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	}
}
