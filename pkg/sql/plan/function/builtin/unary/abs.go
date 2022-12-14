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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/abs"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// abs function's evaluation for arguments: [uint64]
func AbsUInt64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_uint64, Size: 8}
	inputValues := vector.MustTCols[uint64](inputVector)
	if inputVector.IsConst() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]uint64, 1)
		vector.SetCol(resultVector, abs.AbsUint64(inputValues, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[uint64](resultVector)
		abs.AbsUint64(inputValues, resultValues)
		return resultVector, nil
	}
}

// abs function's evaluation for arguments: [int64]
func AbsInt64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_int64, Size: 8}
	inputValues := vector.MustTCols[int64](inputVector)
	if inputVector.IsConst() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]int64, 1)
		vector.SetCol(resultVector, abs.AbsInt64(inputValues, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[int64](resultVector)
		abs.AbsInt64(inputValues, resultValues)
		return resultVector, nil
	}
}

// abs function's evaluation for arguments: [float64]
func AbsFloat64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_float64, Size: 8}
	inputValues := vector.MustTCols[float64](inputVector)
	if inputVector.IsConst() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]float64, 1)
		vector.SetCol(resultVector, abs.AbsFloat64(inputValues, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[float64](resultVector)
		abs.AbsFloat64(inputValues, resultValues)
		return resultVector, nil
	}
}

func AbsDecimal128(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_decimal128, Size: 16, Scale: inputVector.GetType().Scale}
	inputValues := vector.MustTCols[types.Decimal128](inputVector)
	if inputVector.IsConst() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Decimal128, 1)
		vector.SetCol(resultVector, abs.AbsDecimal128(inputValues, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Decimal128](resultVector)
		abs.AbsDecimal128(inputValues, resultValues)
		return resultVector, nil
	}
}
