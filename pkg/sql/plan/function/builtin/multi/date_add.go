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

package multi

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateAdd(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	startVec := ivecs[0]
	diffVec := ivecs[1]
	starts := vector.MustFixedCol[types.Date](ivecs[0])
	diffs := vector.MustFixedCol[int64](ivecs[1])
	unit := vector.MustFixedCol[int64](ivecs[2])[0]

	rtyp := types.T_date.ToType()
	if startVec.IsConstNull() || diffVec.IsConstNull() {
		return vector.NewConstNull(rtyp, startVec.Length(), proc.Mp()), nil
	} else if startVec.IsConst() && diffVec.IsConst() {
		rval, err := doDateAdd(starts[0], diffs[0], unit)
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, rval, startVec.Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, startVec.Length(), nil)
		if err != nil {
			return nil, err
		}
		nulls.Or(startVec.GetNulls(), diffVec.GetNulls(), rvec.GetNulls())
		rvals := vector.MustFixedCol[types.Date](rvec)
		if startVec.IsConst() && !diffVec.IsConst() {
			for i := range diffs {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doDateAdd(starts[0], diffs[i], unit)
				if err != nil {
					return nil, err
				}
			}
		} else if !startVec.IsConst() && diffVec.IsConst() {
			for i := range starts {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doDateAdd(starts[i], diffs[0], unit)
				if err != nil {
					return nil, err
				}
			}
		} else {
			for i := range starts {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doDateAdd(starts[i], diffs[i], unit)
				if err != nil {
					return nil, err
				}
			}
		}
		return rvec, nil
	}
}

func TimeAdd(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	startVec := ivecs[0]
	diffVec := ivecs[1]
	starts := vector.MustFixedCol[types.Time](ivecs[0])
	diffs := vector.MustFixedCol[int64](ivecs[1])
	unit := vector.MustFixedCol[int64](ivecs[2])[0]

	scale := startVec.GetType().Scale
	switch types.IntervalType(unit) {
	case types.MicroSecond:
		scale = 6
	}

	rtyp := types.New(types.T_time, 0, scale)

	if startVec.IsConstNull() || diffVec.IsConstNull() {
		return vector.NewConstNull(rtyp, startVec.Length(), proc.Mp()), nil
	} else if startVec.IsConst() && diffVec.IsConst() {
		rval, err := doTimeAdd(starts[0], diffs[0], unit)
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, rval, startVec.Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, startVec.Length(), nil)
		if err != nil {
			return nil, err
		}
		nulls.Or(startVec.GetNulls(), diffVec.GetNulls(), rvec.GetNulls())
		rvals := vector.MustFixedCol[types.Time](rvec)
		if startVec.IsConst() && !diffVec.IsConst() {
			for i := range diffs {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doTimeAdd(starts[0], diffs[i], unit)
				if err != nil {
					return nil, err
				}
			}
		} else if !startVec.IsConst() && diffVec.IsConst() {
			for i := range starts {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doTimeAdd(starts[i], diffs[0], unit)
				if err != nil {
					return nil, err
				}
			}
		} else {
			for i := range starts {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doTimeAdd(starts[i], diffs[i], unit)
				if err != nil {
					return nil, err
				}
			}
		}
		return rvec, nil
	}
}

func DatetimeAdd(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	startVec := ivecs[0]
	diffVec := ivecs[1]
	starts := vector.MustFixedCol[types.Datetime](ivecs[0])
	diffs := vector.MustFixedCol[int64](ivecs[1])
	unit := vector.MustFixedCol[int64](ivecs[2])[0]

	scale := startVec.GetType().Scale
	switch types.IntervalType(unit) {
	case types.MicroSecond:
		scale = 6
	}

	rtyp := types.New(types.T_datetime, 0, scale)

	if startVec.IsConstNull() || diffVec.IsConstNull() {
		return vector.NewConstNull(rtyp, startVec.Length(), proc.Mp()), nil
	} else if startVec.IsConst() && diffVec.IsConst() {
		rval, err := doDatetimeAdd(starts[0], diffs[0], unit)
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, rval, startVec.Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, startVec.Length(), nil)
		if err != nil {
			return nil, err
		}
		nulls.Or(startVec.GetNulls(), diffVec.GetNulls(), rvec.GetNulls())
		rvals := vector.MustFixedCol[types.Datetime](rvec)
		if startVec.IsConst() && !diffVec.IsConst() {
			for i := range diffs {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doDatetimeAdd(starts[0], diffs[i], unit)
				if err != nil {
					return nil, err
				}
			}
		} else if !startVec.IsConst() && diffVec.IsConst() {
			for i := range starts {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doDatetimeAdd(starts[i], diffs[0], unit)
				if err != nil {
					return nil, err
				}
			}
		} else {
			for i := range starts {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doDatetimeAdd(starts[i], diffs[i], unit)
				if err != nil {
					return nil, err
				}
			}
		}
		return rvec, nil
	}
}

func DateStringAdd(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	startVec := ivecs[0]
	diffVec := ivecs[1]
	starts := vector.MustStrCol(ivecs[0])
	diffs := vector.MustFixedCol[int64](ivecs[1])
	unit := vector.MustFixedCol[int64](ivecs[2])[0]

	rtyp := types.New(types.T_datetime, 0, 6)

	if startVec.IsConstNull() || diffVec.IsConstNull() {
		return vector.NewConstNull(rtyp, startVec.Length(), proc.Mp()), nil
	} else if startVec.IsConst() && diffVec.IsConst() {
		rval, err := doDateStringAdd(starts[0], diffs[0], unit)
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, rval, startVec.Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, startVec.Length(), nil)
		if err != nil {
			return nil, err
		}
		nulls.Or(startVec.GetNulls(), diffVec.GetNulls(), rvec.GetNulls())
		rvals := vector.MustFixedCol[types.Datetime](rvec)
		if startVec.IsConst() && !diffVec.IsConst() {
			for i := range diffs {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doDateStringAdd(starts[0], diffs[i], unit)
				if err != nil {
					return nil, err
				}
			}
		} else if !startVec.IsConst() && diffVec.IsConst() {
			for i := range starts {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doDateStringAdd(starts[i], diffs[0], unit)
				if err != nil {
					return nil, err
				}
			}
		} else {
			for i := range starts {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doDateStringAdd(starts[i], diffs[i], unit)
				if err != nil {
					return nil, err
				}
			}
		}
		return rvec, nil
	}
}

func TimestampAdd(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	startVec := ivecs[0]
	diffVec := ivecs[1]
	starts := vector.MustFixedCol[types.Timestamp](ivecs[0])
	diffs := vector.MustFixedCol[int64](ivecs[1])
	unit := vector.MustFixedCol[int64](ivecs[2])[0]

	scale := startVec.GetType().Scale
	switch types.IntervalType(unit) {
	case types.MicroSecond:
		scale = 6
	}

	rtyp := types.New(types.T_timestamp, 0, scale)

	if startVec.IsConstNull() || diffVec.IsConstNull() {
		return vector.NewConstNull(rtyp, startVec.Length(), proc.Mp()), nil
	} else if startVec.IsConst() && diffVec.IsConst() {
		rval, err := doTimestampAdd(proc.SessionInfo.TimeZone, starts[0], diffs[0], unit)
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, rval, startVec.Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, startVec.Length(), nil)
		if err != nil {
			return nil, err
		}
		nulls.Or(startVec.GetNulls(), diffVec.GetNulls(), rvec.GetNulls())
		rvals := vector.MustFixedCol[types.Timestamp](rvec)
		if startVec.IsConst() && !diffVec.IsConst() {
			for i := range diffs {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doTimestampAdd(proc.SessionInfo.TimeZone, starts[0], diffs[i], unit)
				if err != nil {
					return nil, err
				}
			}
		} else if !startVec.IsConst() && diffVec.IsConst() {
			for i := range starts {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doTimestampAdd(proc.SessionInfo.TimeZone, starts[i], diffs[0], unit)
				if err != nil {
					return nil, err
				}
			}
		} else {
			for i := range starts {
				if rvec.GetNulls().Contains(uint64(i)) {
					continue
				}
				rvals[i], err = doTimestampAdd(proc.SessionInfo.TimeZone, starts[i], diffs[i], unit)
				if err != nil {
					return nil, err
				}
			}
		}
		return rvec, nil
	}
}

func doDateAdd(start types.Date, diff int64, unit int64) (types.Date, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime().AddInterval(diff, types.IntervalType(unit), types.DateType)
	if success {
		return dt.ToDate(), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
}

func doTimeAdd(start types.Time, diff int64, unit int64) (types.Time, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	t, success := start.AddInterval(diff, types.IntervalType(unit))
	if success {
		return t, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
}

func doDatetimeAdd(start types.Datetime, diff int64, unit int64) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doDateStringAdd(startStr string, diff int64, unit int64) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	start, err := types.ParseDatetime(startStr, 6)
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(diff, types.IntervalType(unit), types.DateType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doTimestampAdd(loc *time.Location, start types.Timestamp, diff int64, unit int64) (types.Timestamp, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime(loc).AddInterval(diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt.ToTimestamp(loc), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
}
