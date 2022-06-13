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
	"github.com/matrixorigin/matrixone/pkg/vectorize/date"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateToDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_date, Size: 4}
	resultElementSize := int(resultType.Size)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		inputValues := inputVector.Col.([]types.Date)
		resultVector := vector.NewConst(resultType)
		resultValues := make([]types.Date, 1)
		copy(resultValues, inputValues)
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	} else {
		inputValues := inputVector.Col.([]types.Date)
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeDateSlice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		copy(resultValues, inputValues)
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	}
}

func DatetimeToDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_date, Size: 4}
	resultElementSize := int(resultType.Size)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		inputValues := inputVector.Col.([]types.Datetime)
		resultVector := vector.NewConst(resultType)
		resultValues := make([]types.Date, 1)
		vector.SetCol(resultVector, date.DatetimeToDate(inputValues, resultValues))
		return resultVector, nil
	} else {
		inputValues := inputVector.Col.([]types.Datetime)
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeDateSlice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, date.DatetimeToDate(inputValues, resultValues))
		return resultVector, nil
	}
}
