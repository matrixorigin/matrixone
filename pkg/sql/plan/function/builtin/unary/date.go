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
	"github.com/matrixorigin/matrixone/pkg/vectorize/date"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateToDate(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ivecs[0].Dup(proc.Mp())
}

func DatetimeToDate(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_date.ToType()
	if inputVector.IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	}

	ivals := vector.MustFixedCol[types.Datetime](inputVector)
	if inputVector.IsConst() {
		var rvals [1]types.Date
		date.DatetimeToDate(ivals, rvals[:])
		return vector.NewConstFixed(rtyp, rvals[0], ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[types.Date](rvec)
		date.DatetimeToDate(ivals, rvals)
		return rvec, nil
	}
}

func TimeToDate(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_date.ToType()
	if inputVector.IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	}

	ivals := vector.MustFixedCol[types.Time](inputVector)
	if inputVector.IsConst() {
		var rvals [1]types.Date
		date.TimeToDate(ivals, rvals[:])
		return vector.NewConstFixed(rtyp, rvals[0], ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[types.Date](rvec)
		date.TimeToDate(ivals, rvals)
		return rvec, nil
	}
}

func DateStringToDate(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_date.ToType()
	if inputVector.IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	}

	ivals := vector.MustStrCol(inputVector)
	if inputVector.IsConst() {
		var rvals [1]types.Date
		_, err := date.DateStringToDate(ivals, rvals[:])
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, rvals[0], ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[types.Date](rvec)
		_, err = date.DateStringToDate(ivals, rvals)
		return rvec, err
	}
}

func TimesToDate(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_date.ToType()
	ivals := vector.MustStrCol(inputVector)

	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		var rvals [1]types.Date
		_, err := date.DateStringToDate(ivals, rvals[:])
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, rvals[0], ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[types.Date](rvec)
		_, err = date.DateStringToDate(ivals, rvals)
		return rvec, err
	}
}
