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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/oct"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func Oct[T constraints.Unsigned | constraints.Signed](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_decimal128, Size: 16}
	inputValues := vector.MustTCols[T](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Decimal128, 1)
		col, err := oct.Oct(inputValues, resultValues)
		if err != nil {
			return nil, err
		}
		vector.SetCol(resultVector, col)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Decimal128](resultVector)
		_, err = oct.Oct(inputValues, resultValues)
		if err != nil {
			return nil, err
		}
		return resultVector, nil
	}
}

func OctFloat[T constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_decimal128, Size: 16}
	inputValues := vector.MustTCols[T](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Decimal128, 1)
		col, err := oct.OctFloat(inputValues, resultValues)
		if err != nil {
			return nil, moerr.NewInternalError(proc.Ctx, "the input value is out of integer range")
		}
		vector.SetCol(resultVector, col)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Decimal128](resultVector)
		_, err = oct.OctFloat(inputValues, resultValues)
		if err != nil {
			return nil, moerr.NewInternalError(proc.Ctx, "the input value is out of integer range")
		}
		return resultVector, nil
	}
}
