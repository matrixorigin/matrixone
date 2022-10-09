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

package binary

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/endswith"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Endswith(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	// XXX Why result type is uint8, not bool?
	resultType := types.Type{Oid: types.T_uint8, Size: 1}
	leftValues, rightValues := vector.MustStrCols(left), vector.MustStrCols(right)
	switch {
	case left.IsScalar() && right.IsScalar():
		if left.ConstVectorIsNull() || right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]uint8, 1)
		endswith.EndsWithAllConst(leftValues, rightValues, resultValues)
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	case left.IsScalar() && !right.IsScalar():
		if left.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(rightValues)), right.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[uint8](resultVector)
		endswith.EndsWithLeftConst(leftValues, rightValues, resultValues)
		return resultVector, nil
	case !left.IsScalar() && right.IsScalar():
		if right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(leftValues)), left.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[uint8](resultVector)
		endswith.EndsWithRightConst(leftValues, rightValues, resultValues)
		return resultVector, nil
	}

	resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(rightValues)), nil)
	if err != nil {
		return nil, err
	}
	nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
	resultValues := vector.MustTCols[uint8](resultVector)
	endswith.EndsWith(leftValues, rightValues, resultValues)
	return resultVector, nil
}
