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
	"github.com/matrixorigin/matrixone/pkg/vectorize/datediff"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateDiff(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left := vectors[0]
	right := vectors[1]
	leftValues := vector.MustTCols[types.Date](vectors[0])
	rightValues := vector.MustTCols[types.Date](vectors[1])

	resultType := types.T_int64.ToType()
	switch {
	case left.IsScalar() && right.IsScalar():
		if left.ConstVectorIsNull() || right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := vector.MustTCols[int64](resultVector)
		datediff.DateDiff(leftValues, rightValues, resultValues)
		return resultVector, nil
	case left.IsScalar() && !right.IsScalar():
		if left.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(rightValues)), right.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[int64](resultVector)
		datediff.DateDiffLeftConst(leftValues[0], rightValues, resultValues)
		return resultVector, nil
	case !left.IsScalar() && right.IsScalar():
		if right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(leftValues)), left.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[int64](resultVector)
		datediff.DateDiffRightConst(leftValues, rightValues[0], resultValues)
		return resultVector, nil
	}
	resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(leftValues)), nil)
	if err != nil {
		return nil, err
	}
	resultValues := vector.MustTCols[int64](resultVector)
	nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
	datediff.DateDiff(leftValues, rightValues, resultValues)
	return resultVector, nil
}
