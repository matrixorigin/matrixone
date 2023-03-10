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

func DateToTimestamp(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.New(types.T_timestamp, 0, 6)
	ivals := vector.MustFixedCol[types.Date](inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		return vector.NewConstFixed(rtyp, ivals[0].ToTimestamp(proc.SessionInfo.TimeZone), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[types.Timestamp](rvec)
		for i := range ivals {
			rvals[i] = ivals[i].ToTimestamp(proc.SessionInfo.TimeZone)
		}
		return rvec, nil
	}
}

func DatetimeToTimestamp(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.New(types.T_timestamp, 0, inputVector.GetType().Scale)
	ivals := vector.MustFixedCol[types.Datetime](inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		return vector.NewConstFixed(rtyp, ivals[0].ToTimestamp(proc.SessionInfo.TimeZone), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[types.Timestamp](rvec)
		for i := range ivals {
			rvals[i] = ivals[i].ToTimestamp(proc.SessionInfo.TimeZone)
		}
		return rvec, nil
	}
}

func TimestampToTimestamp(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ivecs[0].Dup(proc.Mp())
}

func DateStringToTimestamp(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.New(types.T_timestamp, 0, 6)
	ivals := vector.MustStrCol(inputVector)

	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		ts, err := types.ParseTimestamp(proc.SessionInfo.TimeZone, ivals[0], 6)
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, ts, ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[types.Timestamp](rvec)
		for i := range ivals {
			rvals[i], err = types.ParseTimestamp(proc.SessionInfo.TimeZone, ivals[i], 6)
			if err != nil {
				return nil, err
			}
		}
		return rvec, nil
	}
}
