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
		return vector.NewConstNull(*leftVec.GetType(), leftVec.Length(), proc.Mp()), nil
	}
	strValues := vector.MustStrCol(leftVec)
	lengthValues := vector.MustFixedCol[int64](rightVec)

	if leftVec.IsConst() && rightVec.IsConst() {
		var rvals [1]string
		left.LeftAllConst(strValues, lengthValues, rvals[:])
		return vector.NewConstBytes(*leftVec.GetType(), []byte(rvals[0]), leftVec.Length(), proc.Mp()), nil
	} else if leftVec.IsConst() && !rightVec.IsConst() {
		rvals := make([]string, len(lengthValues))
		left.LeftLeftConst(strValues, lengthValues, rvals)
		rvec := vector.NewVec(*leftVec.GetType())
		vector.AppendStringList(rvec, rvals, nil, proc.Mp())
		nulls.Set(rvec.GetNulls(), rightVec.GetNulls())
		return rvec, nil
	} else if !leftVec.IsConst() && rightVec.IsConst() {
		rvals := make([]string, len(strValues))
		left.LeftRightConst(strValues, lengthValues, rvals)
		rvec := vector.NewVec(*leftVec.GetType())
		vector.AppendStringList(rvec, rvals, nil, proc.Mp())
		nulls.Set(rvec.GetNulls(), leftVec.GetNulls())
		return rvec, nil
	} else {
		rvals := make([]string, len(strValues))
		left.Left(strValues, lengthValues, rvals)
		resultNsp := nulls.NewWithSize(len(strValues))
		nulls.Or(leftVec.GetNulls(), rightVec.GetNulls(), resultNsp)
		rvec := vector.NewVec(*leftVec.GetType())
		vector.AppendStringList(rvec, rvals, nil, proc.Mp())
		rvec.SetNulls(resultNsp)
		return rvec, nil
	}
}
