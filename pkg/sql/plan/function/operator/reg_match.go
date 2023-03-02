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

func RegMatch(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalRegMatch(ivecs, proc, true)
}

func NotRegMatch(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalRegMatch(ivecs, proc, false)
}

func generalRegMatch(ivecs []*vector.Vector, proc *process.Process, isReg bool) (*vector.Vector, error) {
	left, right := ivecs[0], ivecs[1]
	rtyp := types.T_bool.ToType()
	leftValues, rightValues := vector.MustStrCol(left), vector.MustStrCol(right)
	switch {
	case left.IsConstNull() || right.IsConstNull():
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	case left.IsConst() && right.IsConst():
		var rvals [1]bool
		err := regMatchWithAllConst(leftValues, rightValues, rvals[:], isReg)
		if err != nil {
			return nil, moerr.NewInvalidInput(proc.Ctx, "The Regular Expression have invalid parameter")
		}
		return vector.NewConstFixed(rtyp, rvals[0], ivecs[0].Length(), proc.Mp()), nil
	case left.IsConst() && !right.IsConst():
		rvec, err := proc.AllocVectorOfRows(rtyp, len(rightValues), right.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[bool](rvec)
		err = regMatchWithLeftConst(leftValues, rightValues, rvals, isReg)
		if err != nil {
			return nil, moerr.NewInvalidInput(proc.Ctx, "The Regular Expression have invalid parameter")
		}
		return rvec, nil
	case !left.IsConst() && right.IsConst():
		rvec, err := proc.AllocVectorOfRows(rtyp, len(leftValues), left.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[bool](rvec)
		err = regMatchWithRightConst(leftValues, rightValues, rvals, isReg)
		if err != nil {
			return nil, moerr.NewInvalidInput(proc.Ctx, "The Regular Expression have invalid parameter")
		}
		return rvec, nil
	}
	rvec, err := proc.AllocVectorOfRows(rtyp, len(leftValues), nil)
	if err != nil {
		return nil, err
	}
	rvals := vector.MustFixedCol[bool](rvec)
	nulls.Or(left.GetNulls(), right.GetNulls(), rvec.GetNulls())
	err = regMatchWithAll(leftValues, rightValues, rvals, isReg)
	if err != nil {
		return nil, moerr.NewInvalidInput(proc.Ctx, "The Regular Expression have invalid parameter")
	}
	return rvec, nil
}

func regMatchWithAllConst(lv, rv []string, rs []bool, isReg bool) error {
	res, err := regexp.MatchString(rv[0], lv[0])
	if err != nil {
		return err
	}

	rs[0] = boolResult(res, isReg)
	return nil
}

func regMatchWithLeftConst(lv, rv []string, rs []bool, isReg bool) error {
	for i := range rv {
		res, err := regexp.MatchString(rv[i], lv[0])
		if err != nil {
			return err
		}
		rs[i] = boolResult(res, isReg)
	}
	return nil
}

func regMatchWithRightConst(lv, rv []string, rs []bool, isReg bool) error {
	for i := range lv {
		res, err := regexp.MatchString(rv[0], lv[i])
		if err != nil {
			return err
		}
		rs[i] = boolResult(res, isReg)
	}
	return nil
}

func regMatchWithAll(lv, rv []string, rs []bool, isReg bool) error {
	for i := range lv {
		res, err := regexp.MatchString(rv[i], lv[i])
		if err != nil {
			return err
		}
		rs[i] = boolResult(res, isReg)
	}
	return nil
}

func boolResult(isMatch bool, isReg bool) bool {
	return (isMatch && isReg) || !(isMatch || isReg)
}
