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
	"github.com/matrixorigin/matrixone/pkg/vectorize/day"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateToDay(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.Type{Oid: types.T_uint8, Size: 1}
	ivals := vector.MustFixedCol[types.Date](inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		var rvals [1]uint8
		day.DateToDay(ivals, rvals[:])
		return vector.NewConstFixed(rtyp, rvals[0], ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[uint8](rvec)
		day.DateToDay(ivals, rvals)
		return rvec, nil
	}
}

func DatetimeToDay(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.Type{Oid: types.T_uint8, Size: 1}
	ivals := vector.MustFixedCol[types.Datetime](inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		var rvals [1]uint8
		day.DatetimeToDay(ivals, rvals[:])
		return vector.NewConstFixed(rtyp, rvals[0], ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[uint8](rvec)
		day.DatetimeToDay(ivals, rvals)
		return rvec, nil
	}
}
