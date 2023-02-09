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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateToYear(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_uint16.ToType()
	ivals := vector.MustTCols[types.Date](inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		return vector.NewConst(rtyp, ivals[0].Year(), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustTCols[uint16](rvec)
		doDateToYear(ivals, rvals)
		return rvec, nil
	}
}

func DatetimeToYear(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_uint16.ToType()
	ivals := vector.MustTCols[types.Datetime](inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		return vector.NewConst(rtyp, ivals[0].Year(), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustTCols[uint16](rvec)
		doDatetimeToYear(ivals, rvals)
		return rvec, nil
	}
}

func DateStringToYear(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_uint16.ToType()
	ivals := vector.MustStrCols(inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		var rvals [1]uint16
		doDateStringToYear(ivals, rvals[:])
		return vector.NewConst(rtyp, rvals[0], ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustTCols[uint16](rvec)
		doDateStringToYear(ivals, rvals)
		return rvec, nil
	}
}

func doDateToYear(xs []types.Date, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x.Year()
	}
	return rs
}

func doDatetimeToYear(xs []types.Datetime, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x.Year()
	}
	return rs
}

func doDateStringToYear(xs []string, rs []uint16) []uint16 {
	for i, str := range xs {
		d, e := types.ParseDateCast(str)
		if e != nil {
			panic(e)
		}
		rs[i] = d.Year()
	}
	return rs
}
