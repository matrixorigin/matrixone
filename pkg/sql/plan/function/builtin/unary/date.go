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
	"github.com/matrixorigin/matrixone/pkg/vectorize/date"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateToDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// XXX Why do we ever need this one?  Isn't this a noop?
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_date, Size: 4}
	inputValues := vector.MustTCols[types.Date](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Date, 1)
		copy(resultValues, inputValues)
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Date](resultVector)
		copy(resultValues, inputValues)
		return resultVector, nil
	}
}

func DatetimeToDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_date, Size: 4}
	inputValues := vector.MustTCols[types.Datetime](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Date, 1)
		vector.SetCol(resultVector, date.DatetimeToDate(inputValues, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Date](resultVector)
		date.DatetimeToDate(inputValues, resultValues)
		return resultVector, nil
	}
}

func TimeToDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_date, Size: 4}
	inputValues := vector.MustTCols[types.Time](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Date, 1)
		vector.SetCol(resultVector, date.TimeToDate(inputValues, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Date](resultVector)
		date.TimeToDate(inputValues, resultValues)
		return resultVector, nil
	}
}

func DateStringToDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_date, Size: 4}
	inputValues := vector.MustStrCols(inputVector)

	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Date, 1)
		result, err := date.DateStringToDate(inputValues, resultValues)
		vector.SetCol(resultVector, result)
		return resultVector, err
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Date](resultVector)
		_, err = date.DateStringToDate(inputValues, resultValues)
		return resultVector, err
	}
}

func TimesToDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_date, Size: 4}
	inputValues := vector.MustStrCols(inputVector)

	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Date, 1)
		result, err := date.DateStringToDate(inputValues, resultValues)
		vector.SetCol(resultVector, result)
		return resultVector, err
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Date](resultVector)
		_, err = date.DateStringToDate(inputValues, resultValues)
		return resultVector, err
	}
}
