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
	"github.com/matrixorigin/matrixone/pkg/vectorize/space"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// the function registeration for generics functions may have some problem now, change this to generics later
func SpaceInt64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	inputValues := vector.MustTCols[int64](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		bytesNeed, err := space.CountSpacesSigned(inputValues)
		if err != nil {
			return nil, err
		}
		results := &types.Bytes{
			Data:    make([]byte, bytesNeed),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		result := space.FillSpacesSigned(inputValues, results)
		nulls.Or(inputVector.Nsp, result.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, result.Result)
		return resultVector, nil
	}
	bytesNeed, err := space.CountSpacesSigned(inputValues)
	if err != nil {
		return nil, err
	}
	resultVector, err := proc.AllocVector(resultType, bytesNeed)
	if err != nil {
		return nil, err
	}
	resultValues := &types.Bytes{
		Data:    resultVector.Data,
		Offsets: make([]uint32, len(inputValues)),
		Lengths: make([]uint32, len(inputValues)),
	}
	result := space.FillSpacesSigned(inputValues, resultValues)
	nulls.Or(inputVector.Nsp, result.Nsp, resultVector.Nsp)
	vector.SetCol(resultVector, result.Result)
	return resultVector, nil
}

func SpaceUint64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	inputValues := vector.MustTCols[uint64](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		bytesNeed, err := space.CountSpacesUnsigned(inputValues)
		if err != nil {
			return nil, err
		}
		results := &types.Bytes{
			Data:    make([]byte, bytesNeed),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		result := space.FillSpacesUnsigned(inputValues, results)
		nulls.Or(inputVector.Nsp, result.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, result.Result)
		return resultVector, nil
	}
	bytesNeed, err := space.CountSpacesUnsigned(inputValues)
	if err != nil {
		return nil, err
	}
	resultVector, err := proc.AllocVector(resultType, bytesNeed)
	if err != nil {
		return nil, err
	}
	resultValues := &types.Bytes{
		Data:    resultVector.Data,
		Offsets: make([]uint32, len(inputValues)),
		Lengths: make([]uint32, len(inputValues)),
	}
	result := space.FillSpacesUnsigned(inputValues, resultValues)
	nulls.Or(inputVector.Nsp, result.Nsp, resultVector.Nsp)
	vector.SetCol(resultVector, result.Result)
	return resultVector, nil
}

func SpaceFloat[T constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	inputValues := vector.MustTCols[T](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		bytesNeed, err := space.CountSpacesFloat(inputValues)
		if err != nil {
			return nil, err
		}
		results := &types.Bytes{
			Data:    make([]byte, bytesNeed),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		result := space.FillSpacesFloat(inputValues, results)
		nulls.Or(inputVector.Nsp, result.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, result.Result)
		return resultVector, nil
	}
	bytesNeed, err := space.CountSpacesFloat(inputValues)
	if err != nil {
		return nil, err
	}
	resultVector, err := proc.AllocVector(resultType, bytesNeed)
	if err != nil {
		return nil, err
	}
	resultValues := &types.Bytes{
		Data:    resultVector.Data,
		Offsets: make([]uint32, len(inputValues)),
		Lengths: make([]uint32, len(inputValues)),
	}
	result := space.FillSpacesFloat(inputValues, resultValues)
	nulls.Or(inputVector.Nsp, result.Nsp, resultVector.Nsp)
	vector.SetCol(resultVector, result.Result)
	return resultVector, nil
}
