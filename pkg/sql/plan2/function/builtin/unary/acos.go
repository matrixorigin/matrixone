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
	"github.com/matrixorigin/matrixone/pkg/vectorize/acos"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func AcosUint64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_float64, Size: 8}
	resultElementSize := int(resultType.Size)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		inputValues := inputVector.Col.([]uint64)
		resultVector := vector.NewConst(resultType)
		resultValues := make([]float64, 1)
		results := acos.AcosUint64(inputValues, resultValues)
		if nulls.Any(results.Nsp) {
			return proc.AllocScalarNullVector(resultType), nil
		}
		vector.SetCol(resultVector, results.Result)
		return resultVector, nil
	} else {
		inputValues := inputVector.Col.([]uint64)
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		results := acos.AcosUint64(inputValues, resultValues)
		nulls.Or(inputVector.Nsp, results.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, results.Result)
		return resultVector, nil
	}
}

func AcosInt64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_float64, Size: 8}
	resultElementSize := int(resultType.Size)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		inputValues := inputVector.Col.([]int64)
		resultVector := vector.NewConst(resultType)
		resultValues := make([]float64, 1)
		results := acos.AcosInt64(inputValues, resultValues)
		if nulls.Any(results.Nsp) {
			return proc.AllocScalarNullVector(resultType), nil
		}
		vector.SetCol(resultVector, results.Result)
		return resultVector, nil
	} else {
		inputValues := inputVector.Col.([]int64)
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		results := acos.AcosInt64(inputValues, resultValues)
		nulls.Or(inputVector.Nsp, results.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, results.Result)
		return resultVector, nil
	}
}

func AcosFloat64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_float64, Size: 8}
	resultElementSize := int(resultType.Size)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		inputValues := inputVector.Col.([]float64)
		resultVector := vector.NewConst(resultType)
		resultValues := make([]float64, 1)
		results := acos.AcosFloat64(inputValues, resultValues)
		if nulls.Any(results.Nsp) {
			return proc.AllocScalarNullVector(resultType), nil
		}
		vector.SetCol(resultVector, results.Result)
		return resultVector, nil
	} else {
		inputValues := inputVector.Col.([]float64)
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		results := acos.AcosFloat64(inputValues, resultValues)
		nulls.Or(inputVector.Nsp, results.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, results.Result)
		return resultVector, nil
	}
}
