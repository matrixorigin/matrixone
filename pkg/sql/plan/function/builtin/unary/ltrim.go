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
	"github.com/matrixorigin/matrixone/pkg/vectorize/ltrim"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Ltrim(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	// totalCount - spaceCount is the total bytes need for the ltrim-ed string
	inputValues := vector.MustBytesCols(inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		spaceCount := ltrim.CountSpacesFromLeft(inputValues)
		totalCount := int32(len(inputValues.Data))
		resultVector := vector.NewConst(resultType, 1)
		resultValues := &types.Bytes{
			Data:    make([]byte, totalCount-spaceCount),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		vector.SetCol(resultVector, ltrim.LtrimChar(inputValues, resultValues))
		return resultVector, nil
	} else {
		spaceCount := ltrim.CountSpacesFromLeft(inputValues)
		totalCount := int32(len(inputValues.Data))
		resultVector, err := proc.AllocVector(resultType, int64(totalCount-spaceCount))
		if err != nil {
			return nil, err
		}
		resultValues := &types.Bytes{
			Data:    resultVector.Data,
			Offsets: make([]uint32, len(inputValues.Offsets)),
			Lengths: make([]uint32, len(inputValues.Lengths)),
		}
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, ltrim.LtrimChar(inputValues, resultValues))
		return resultVector, nil
	}
}
