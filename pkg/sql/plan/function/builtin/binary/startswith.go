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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/startswith"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Startswith(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	resultType := types.T_uint8.ToType()
	leftValues, rightValues := vector.MustStrCols(left), vector.MustStrCols(right)
	switch {
	case left.IsScalar() && right.IsScalar():
		if left.ConstVectorIsNull() || right.ConstVectorIsNull() {
			return nil, moerr.NewInvalidArg("StartsWith input", "empty string")
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := vector.MustTCols[uint8](resultVector)
		err := startswith.StartsWithAllConst(leftValues[0], rightValues[0], resultValues)
		if err != nil {
			return nil, err
		}
		return resultVector, nil
	case left.IsScalar() && !right.IsScalar():
		if left.ConstVectorIsNull() {
			return nil, moerr.NewInvalidArg("StartsWith input", "empty string")
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(rightValues)), right.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[uint8](resultVector)
		err = startswith.StartsWithLeftConst(leftValues[0], rightValues, resultValues)
		if err != nil {
			return nil, err
		}
		return resultVector, nil
	case !left.IsScalar() && right.IsScalar():
		if right.ConstVectorIsNull() {
			return nil, moerr.NewInvalidArg("StartsWith input", "empty string")
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(leftValues)), left.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[uint8](resultVector)
		err = startswith.StartsWithRightConst(leftValues, rightValues[0], resultValues)
		if err != nil {
			return nil, err
		}
		return resultVector, nil
	}
	resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(leftValues)), nil)
	if err != nil {
		return nil, err
	}
	resultValues := vector.MustTCols[uint8](resultVector)
	nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
	err = startswith.StartsWith(leftValues, rightValues, resultValues)
	if err != nil {
		return nil, err
	}

	return resultVector, nil
}
