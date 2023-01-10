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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/left"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Left(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	leftVec := vs[0]
	rightVec := vs[1]

	if leftVec.IsConstNull() || rightVec.IsConstNull() {
		return proc.AllocScalarNullVector(*leftVec.GetType()), nil
	}
	strValues := vector.MustStrCols(leftVec)
	lengthValues := vector.MustTCols[int64](rightVec)

	if leftVec.IsConst() && rightVec.IsConst() {
		resultValues := make([]string, 1)
		left.LeftAllConst(strValues, lengthValues, resultValues)

		resultVector := vector.New(vector.FLAT, *leftVec.GetType())
		vector.AppendString(resultVector, resultValues[0], resultValues[0] == "", proc.Mp())
		return resultVector, nil
	} else if leftVec.IsConst() && !rightVec.IsConst() {
		resultValues := make([]string, len(lengthValues))
		left.LeftLeftConst(strValues, lengthValues, resultValues)
		resultVector := vector.New(vector.FLAT, *leftVec.GetType())
		vector.AppendStringList(resultVector, resultValues, nil, proc.Mp())
		return resultVector, nil
	} else if !leftVec.IsConst() && rightVec.IsConst() {
		resultValues := make([]string, len(strValues))
		left.LeftRightConst(strValues, lengthValues, resultValues)
		resultVector := vector.New(vector.FLAT, *leftVec.GetType())
		vector.AppendStringList(resultVector, resultValues, nil, proc.Mp())
		return resultVector, nil
	} else {
		resultValues := make([]string, len(strValues))
		left.Left(strValues, lengthValues, resultValues)
		resultNsp := nulls.NewWithSize(len(strValues))
		nulls.Or(leftVec.GetNulls(), rightVec.GetNulls(), resultNsp)
		resultVector := vector.New(vector.FLAT, *leftVec.GetType())
		vector.AppendStringList(resultVector, resultValues, nil, proc.Mp())
		return resultVector, nil
	}
}
