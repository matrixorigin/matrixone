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
	"github.com/matrixorigin/matrixone/pkg/vectorize/findinset"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func FindInSet(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	resultType := types.Type{Oid: types.T_uint64, Size: 8}
	leftValues, rightValues := vector.MustStrCols(left), vector.MustStrCols(right)
	switch {
	case left.IsConst() && right.IsConst():
		if left.IsConstNull() || right.IsConstNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.New(vector.CONSTANT, resultType)
		resultValues := vector.MustTCols[uint64](resultVector)
		findinset.FindInSetWithAllConst(leftValues[0], rightValues[0], resultValues)
		return resultVector, nil
	case left.IsConst() && !right.IsConst():
		if left.IsConstNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		rlen := len(rightValues)
		resultVector := vector.New(vector.FLAT, resultType)
		resultVector.PreExtend(rlen, proc.Mp())
		resultValues := vector.MustTCols[uint64](resultVector)
		nulls.Set(resultVector.GetNulls(), right.GetNulls())
		findinset.FindInSetWithLeftConst(leftValues[0], rightValues, resultValues)
		return resultVector, nil
	case !left.IsConst() && right.IsConst():
		if right.IsConstNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resLen := len(leftValues)
		resultVector := vector.New(vector.FLAT, resultType)
		resultVector.PreExtend(resLen, proc.Mp())
		resultValues := vector.MustTCols[uint64](resultVector)
		nulls.Set(resultVector.GetNulls(), left.GetNulls())
		findinset.FindInSetWithRightConst(leftValues, rightValues[0], resultValues)
		return resultVector, nil
	}
	resLen := len(leftValues)
	resultVector := vector.New(vector.FLAT, resultType)
	resultVector.PreExtend(resLen, proc.Mp())
	resultValues := vector.MustTCols[uint64](resultVector)
	nulls.Or(left.GetNulls(), right.GetNulls(), resultVector.GetNulls())
	findinset.FindInSet(leftValues, rightValues, resultValues)
	return resultVector, nil
}
