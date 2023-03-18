// Copyright 2021 - 2022 Matrix Origin
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

func DatetimeToHour(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_uint8.ToType()
	ivals := vector.MustFixedCol[types.Datetime](inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		return vector.NewConstFixed(rtyp, uint8(ivals[0].Hour()), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, ivecs[0].Length(), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rs := vector.MustFixedCol[uint8](rvec)
		for i, v := range ivals {
			if !rvec.GetNulls().Contains(uint64(i)) {
				rs[i] = uint8(v.Hour())
			}
		}
		return rvec, nil
	}
}

func TimestampToHour(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_uint8.ToType()
	ivals := vector.MustFixedCol[types.Timestamp](inputVector)
	dtvals := make([]types.Datetime, len(ivals))
	if _, err := types.TimestampToDatetime(proc.SessionInfo.TimeZone, ivals, dtvals); err != nil {
		return nil, err
	}
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		return vector.NewConstFixed(rtyp, uint8(dtvals[0].Hour()), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, ivecs[0].Length(), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rs := vector.MustFixedCol[uint8](rvec)
		for i, v := range dtvals {
			if !rvec.GetNulls().Contains(uint64(i)) {
				rs[i] = uint8(v.Hour())
			}
		}
		return rvec, nil
	}
}

func DatetimeToMinute(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_uint8.ToType()
	ivals := vector.MustFixedCol[types.Datetime](inputVector)

	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		return vector.NewConstFixed(rtyp, uint8(ivals[0].Minute()), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, ivecs[0].Length(), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rs := vector.MustFixedCol[uint8](rvec)
		for i, v := range ivals {
			if !rvec.GetNulls().Contains(uint64(i)) {
				rs[i] = uint8(v.Minute())
			}
		}
		return rvec, nil
	}
}

func TimestampToMinute(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_uint8.ToType()
	ivals := vector.MustFixedCol[types.Timestamp](inputVector)
	dtvals := make([]types.Datetime, len(ivals))
	if _, err := types.TimestampToDatetime(proc.SessionInfo.TimeZone, ivals, dtvals); err != nil {
		return nil, err
	}
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		return vector.NewConstFixed(rtyp, uint8(dtvals[0].Minute()), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, ivecs[0].Length(), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rs := vector.MustFixedCol[uint8](rvec)
		for i, v := range dtvals {
			if !rvec.GetNulls().Contains(uint64(i)) {
				rs[i] = uint8(v.Minute())
			}
		}
		return rvec, nil
	}
}

func DatetimeToSecond(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_uint8.ToType()
	ivals := vector.MustFixedCol[types.Datetime](inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		return vector.NewConstFixed(rtyp, uint8(ivals[0].Sec()), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, ivecs[0].Length(), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rs := vector.MustFixedCol[uint8](rvec)
		for i, v := range ivals {
			if !rvec.GetNulls().Contains(uint64(i)) {
				rs[i] = uint8(v.Sec())
			}
		}
		return rvec, nil
	}
}

func TimestampToSecond(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_uint8.ToType()
	ivals := vector.MustFixedCol[types.Timestamp](inputVector)
	dtvals := make([]types.Datetime, len(ivals))
	if _, err := types.TimestampToDatetime(proc.SessionInfo.TimeZone, ivals, dtvals); err != nil {
		return nil, err
	}
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		return vector.NewConstFixed(rtyp, uint8(dtvals[0].Sec()), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, ivecs[0].Length(), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rs := vector.MustFixedCol[uint8](rvec)
		for i, v := range dtvals {
			if !rvec.GetNulls().Contains(uint64(i)) {
				rs[i] = uint8(v.Sec())
			}
		}
		return rvec, nil
	}
}
