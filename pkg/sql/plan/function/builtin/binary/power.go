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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/power"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Power(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	resultType := types.T_float64.ToType()
	leftValues, rightValues := vector.MustTCols[float64](left), vector.MustTCols[float64](right)
	switch {
	case left.IsConst() && right.IsConst():
		if left.ConstVectorIsNull() || right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]float64, 1)
		vector.SetCol(resultVector, power.Power(leftValues, rightValues, resultValues))
		return resultVector, nil
	case left.IsConst() && !right.IsConst():
		if left.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(rightValues)), right.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[float64](resultVector)
		power.PowerScalarLeftConst(leftValues[0], rightValues, resultValues)
		return resultVector, nil
	case !left.IsConst() && right.IsConst():
		if right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(leftValues)), left.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[float64](resultVector)
		power.PowerScalarRightConst(rightValues[0], leftValues, resultValues)
		return resultVector, nil
	}
	resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(rightValues)), nil)
	if err != nil {
		return nil, err
	}
	resultValues := vector.MustTCols[float64](resultVector)
	power.Power(leftValues, rightValues, resultValues)
	return resultVector, nil
}
