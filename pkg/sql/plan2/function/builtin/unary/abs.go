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
	"github.com/matrixorigin/matrixone/pkg/vectorize/abs"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// abs function's evaluation for arguments: [uint64]
func AbsUInt64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_uint64, Size: 8}
	resultElementSize := int(resultType.Size)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		inputValues := inputVector.Col.([]uint64)
		resultVector := vector.NewConst(resultType)
		resultValues := make([]uint64, 1)
		vector.SetCol(resultVector, abs.AbsUint64(inputValues, resultValues))
		return resultVector, nil
	} else {
		inputValues := inputVector.Col.([]uint64)
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeUint64Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, abs.AbsUint64(inputValues, resultValues))
		return resultVector, nil
	}
}

// abs function's evaluation for arguments: [int64]
func AbsInt64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_int64, Size: 8}
	resultElementSize := int(resultType.Size)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		inputValues := inputVector.Col.([]int64)
		resultVector := vector.NewConst(resultType)
		resultValues := make([]int64, 1)
		vector.SetCol(resultVector, abs.AbsInt64(inputValues, resultValues))
		return resultVector, nil
	} else {
		inputValues := inputVector.Col.([]int64)
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeInt64Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, abs.AbsInt64(inputValues, resultValues))
		return resultVector, nil
	}
}

// abs function's evaluation for arguments: [float64]
func AbsFloat64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
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
		vector.SetCol(resultVector, abs.AbsFloat64(inputValues, resultValues))
		return resultVector, nil
	} else {
		inputValues := inputVector.Col.([]float64)
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, abs.AbsFloat64(inputValues, resultValues))
		return resultVector, nil
	}
}
