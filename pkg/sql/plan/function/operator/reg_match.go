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

package operator

import (
	"regexp"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func RegMatch(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalRegMatch(vectors, proc, true)
}

func NotRegMatch(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalRegMatch(vectors, proc, false)
}

func generalRegMatch(vectors []*vector.Vector, proc *process.Process, isReg bool) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	resultType := types.T_bool.ToType()
	leftValues, rightValues := vector.MustStrCols(left), vector.MustStrCols(right)
	switch {
	case left.IsConst() && right.IsConst():
		if left.IsConstNull() || right.IsConstNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.New(vector.CONSTANT, resultType)
		resultValues := vector.MustTCols[bool](resultVector)
		err := RegMatchWithAllConst(leftValues, rightValues, resultValues, isReg)
		if err != nil {
			return nil, moerr.NewInvalidInputNoCtx("The Regular Expression have invalid parameter")
		}
		return resultVector, nil
	case left.IsConst() && !right.IsConst():
		if left.IsConstNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(rightValues)), right.GetNulls())
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[bool](resultVector)
		err = RegMatchWithLeftConst(leftValues, rightValues, resultValues, isReg)
		if err != nil {
			return nil, moerr.NewInvalidInputNoCtx("The Regular Expression have invalid parameter")
		}
		return resultVector, nil
	case !left.IsConst() && right.IsConst():
		if right.IsConstNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(leftValues)), left.GetNulls())
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[bool](resultVector)
		err = RegMatchWithRightConst(leftValues, rightValues, resultValues, isReg)
		if err != nil {
			return nil, moerr.NewInvalidInputNoCtx("The Regular Expression have invalid parameter")
		}
		return resultVector, nil
	}
	resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(leftValues)), nil)
	if err != nil {
		return nil, err
	}
	resultValues := vector.MustTCols[bool](resultVector)
	nulls.Or(left.GetNulls(), right.GetNulls(), resultVector.GetNulls())
	err = RegMatchWithALL(leftValues, rightValues, resultValues, isReg)
	if err != nil {
		return nil, moerr.NewInvalidInputNoCtx("The Regular Expression have invalid parameter")
	}
	return resultVector, nil
}

func RegMatchWithAllConst(lv, rv []string, rs []bool, isReg bool) error {
	res, err := regexp.MatchString(rv[0], lv[0])
	if err != nil {
		return err
	}

	rs[0] = BoolResult(res, isReg)
	return nil
}

func RegMatchWithLeftConst(lv, rv []string, rs []bool, isReg bool) error {
	for i := range rv {
		res, err := regexp.MatchString(rv[i], lv[0])
		if err != nil {
			return err
		}
		rs[i] = BoolResult(res, isReg)
	}
	return nil
}

func RegMatchWithRightConst(lv, rv []string, rs []bool, isReg bool) error {
	for i := range lv {
		res, err := regexp.MatchString(rv[0], lv[i])
		if err != nil {
			return err
		}
		rs[i] = BoolResult(res, isReg)
	}
	return nil
}

func RegMatchWithALL(lv, rv []string, rs []bool, isReg bool) error {
	for i := range lv {
		res, err := regexp.MatchString(rv[i], lv[i])
		if err != nil {
			return err
		}
		rs[i] = BoolResult(res, isReg)
	}
	return nil
}

func BoolResult(isMatch bool, isReg bool) bool {
	return (isMatch && isReg) || !(isMatch || isReg)
}
