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
	"github.com/matrixorigin/matrixone/pkg/vectorize/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateToTimestamp(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_timestamp, Scale: 6, Size: 8}
	inputValues := vector.MustTCols[types.Date](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Timestamp, 1)
		vector.SetCol(resultVector, timestamp.DateToTimestamp(proc.SessionInfo.TimeZone, inputValues, resultVector.Nsp, resultValues))
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Timestamp](resultVector)
		timestamp.DateToTimestamp(proc.SessionInfo.TimeZone, inputValues, resultVector.Nsp, resultValues)
		return resultVector, nil
	}
}

func DatetimeToTimestamp(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_timestamp, Scale: inputVector.Typ.Scale, Size: 8}
	inputValues := vector.MustTCols[types.Datetime](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Timestamp, 1)
		vector.SetCol(resultVector, timestamp.DatetimeToTimestamp(proc.SessionInfo.TimeZone, inputValues, resultVector.Nsp, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Timestamp](resultVector)
		timestamp.DatetimeToTimestamp(proc.SessionInfo.TimeZone, inputValues, resultVector.Nsp, resultValues)
		return resultVector, nil
	}
}

func TimestampToTimestamp(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// XXX should this be an Noop?
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_timestamp, Scale: inputVector.Typ.Scale, Size: 8}
	inputValues := vector.MustTCols[types.Timestamp](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Timestamp, 1)
		copy(resultValues, inputValues)
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Timestamp](resultVector)
		copy(resultValues, inputValues)
		return resultVector, nil
	}
}

func DateStringToTimestamp(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_timestamp, Scale: 6, Size: 8}
	inputValues := vector.MustStrCols(inputVector)

	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Timestamp, 1)
		vector.SetCol(resultVector, timestamp.DateStringToTimestamp(proc.SessionInfo.TimeZone, inputValues, resultVector.Nsp, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Timestamp](resultVector)
		timestamp.DateStringToTimestamp(proc.SessionInfo.TimeZone, inputValues, resultVector.Nsp, resultValues)
		return resultVector, nil
	}
}
