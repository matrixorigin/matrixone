// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DatetimeToHour(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.T_uint8.ToType()
	inputValues := vector.MustTCols[types.Datetime](inputVector)
	if inputVector.IsScalar() {
		if inputVector.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConstFixed(resultType, 1, uint8(inputValues[0].Hour()))
		return resultVector, nil
	} else {
		resultVector := vector.New(resultType)
		for i, v := range inputValues {
			if inputVector.GetNulls().Contains(uint64(i)) {
				resultVector.GetNulls().Set(uint64(i))
				if err := resultVector.Append(uint8(0), true, proc.GetMheap()); err != nil {
					return nil, err
				}
				continue
			}
			if err := resultVector.Append(uint8(v.Hour()), false, proc.GetMheap()); err != nil {
				return nil, err
			}
		}
		return resultVector, nil
	}
}

func TimestampToHour(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.T_uint8.ToType()
	inputValues := vector.MustTCols[types.Timestamp](inputVector)
	convertedInputValues := make([]types.Datetime, len(inputValues))
	if _, err := types.TimestampToDatetime(proc.SessionInfo.TimeZone, inputValues, convertedInputValues); err != nil {
		return nil, err
	}
	if inputVector.IsScalar() {
		if inputVector.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConstFixed(resultType, 1, uint8(convertedInputValues[0].Hour()))
		return resultVector, nil
	} else {
		resultVector := vector.New(resultType)
		for i, v := range convertedInputValues {
			if inputVector.GetNulls().Contains(uint64(i)) {
				resultVector.GetNulls().Set(uint64(i))
				if err := resultVector.Append(uint8(0), true, proc.GetMheap()); err != nil {
					return nil, err
				}
				continue
			}
			if err := resultVector.Append(uint8(v.Hour()), false, proc.GetMheap()); err != nil {
				return nil, err
			}
		}
		return resultVector, nil
	}
}

func DatetimeToMinute(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.T_uint8.ToType()
	inputValues := vector.MustTCols[types.Datetime](inputVector)

	if inputVector.IsScalar() {
		if inputVector.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConstFixed(resultType, 1, uint8(inputValues[0].Minute()))
		return resultVector, nil
	} else {
		resultVector := vector.New(resultType)
		for i, v := range inputValues {
			if inputVector.GetNulls().Contains(uint64(i)) {
				resultVector.GetNulls().Set(uint64(i))
				if err := resultVector.Append(uint8(0), true, proc.GetMheap()); err != nil {
					return nil, err
				}
				continue
			}
			if err := resultVector.Append(uint8(v.Minute()), false, proc.GetMheap()); err != nil {
				return nil, err
			}
		}
		return resultVector, nil
	}
}

func TimestampToMinute(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.T_uint8.ToType()
	inputValues := vector.MustTCols[types.Timestamp](inputVector)
	convertedInputValues := make([]types.Datetime, len(inputValues))
	if _, err := types.TimestampToDatetime(proc.SessionInfo.TimeZone, inputValues, convertedInputValues); err != nil {
		return nil, err
	}
	if inputVector.IsScalar() {
		if inputVector.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConstFixed(resultType, 1, uint8(convertedInputValues[0].Minute()))
		return resultVector, nil
	} else {
		resultVector := vector.New(resultType)
		for i, v := range convertedInputValues {
			if inputVector.GetNulls().Contains(uint64(i)) {
				resultVector.GetNulls().Set(uint64(i))
				if err := resultVector.Append(uint8(0), true, proc.GetMheap()); err != nil {
					return nil, err
				}
				continue
			}
			if err := resultVector.Append(uint8(v.Minute()), false, proc.GetMheap()); err != nil {
				return nil, err
			}
		}
		return resultVector, nil
	}
}

func DatetimeToSecond(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.T_uint8.ToType()
	inputValues := vector.MustTCols[types.Datetime](inputVector)
	if inputVector.IsScalar() {
		if inputVector.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConstFixed(resultType, 1, uint8(inputValues[0].Sec()))
		return resultVector, nil
	} else {
		resultVector := vector.New(resultType)
		for i, v := range inputValues {
			if inputVector.GetNulls().Contains(uint64(i)) {
				resultVector.GetNulls().Set(uint64(i))
				if err := resultVector.Append(uint8(0), true, proc.GetMheap()); err != nil {
					return nil, err
				}
				continue
			}
			if err := resultVector.Append(uint8(v.Sec()), false, proc.GetMheap()); err != nil {
				return nil, err
			}
		}
		return resultVector, nil
	}
}

func TimestampToSecond(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.T_uint8.ToType()
	inputValues := vector.MustTCols[types.Timestamp](inputVector)
	convertedInputValues := make([]types.Datetime, len(inputValues))
	if _, err := types.TimestampToDatetime(proc.SessionInfo.TimeZone, inputValues, convertedInputValues); err != nil {
		return nil, err
	}
	if inputVector.IsScalar() {
		if inputVector.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConstFixed(resultType, 1, uint8(convertedInputValues[0].Sec()))
		return resultVector, nil
	} else {
		resultVector := vector.New(resultType)
		for i, v := range convertedInputValues {
			if inputVector.GetNulls().Contains(uint64(i)) {
				resultVector.GetNulls().Set(uint64(i))
				if err := resultVector.Append(uint8(0), true, proc.GetMheap()); err != nil {
					return nil, err
				}
				continue
			}
			if err := resultVector.Append(uint8(v.Sec()), false, proc.GetMheap()); err != nil {
				return nil, err
			}
		}
		return resultVector, nil
	}
}
