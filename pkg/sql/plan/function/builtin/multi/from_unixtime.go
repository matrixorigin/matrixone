// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package multi

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/vectorize/fromunixtime"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	max_unix_timestamp_int = 32536771199
)

func FromUnixTimeInt64(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := ivecs[0]
	if inVec.IsConstNull() {
		return vector.NewConstNull(types.T_datetime.ToType(), inVec.Length(), proc.Mp()), nil
	}

	times := vector.MustFixedCol[int64](inVec)
	if inVec.IsConst() {
		if times[0] < 0 || times[0] > max_unix_timestamp_int {
			return vector.NewConstNull(types.T_datetime.ToType(), inVec.Length(), proc.Mp()), nil
		}

		var rs [1]types.Datetime
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, times, rs[:])
		rvec := vector.NewConstFixed(types.T_datetime.ToType(), rs[0], inVec.Length(), proc.Mp())
		return rvec, nil
	} else {
		rs := make([]types.Datetime, len(times))
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, times, rs)
		vec := vector.NewVec(types.T_datetime.ToType())
		nulls.Set(vec.GetNulls(), inVec.GetNulls())
		vector.AppendFixedList(vec, rs, nil, proc.Mp())
		for i := 0; i < len(times); i++ {
			if times[i] < 0 || times[i] > max_unix_timestamp_int {
				nulls.Add(vec.GetNulls(), uint64(i))
			}
		}
		return vec, nil
	}
}

func FromUnixTimeFloat64(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := ivecs[0]
	if inVec.IsConstNull() {
		return vector.NewConstNull(types.T_datetime.ToType(), inVec.Length(), proc.Mp()), nil
	}

	times := vector.MustFixedCol[float64](inVec)
	if inVec.IsConst() {
		if times[0] < 0 || times[0] > max_unix_timestamp_int {
			return vector.NewConstNull(types.T_datetime.ToType(), inVec.Length(), proc.Mp()), nil
		}

		var rs [1]types.Datetime
		ints, fracs := splitDecimalToIntAndFrac(times)
		fromunixtime.UnixToDateTimeWithNsec(proc.SessionInfo.TimeZone, ints, fracs, rs[:])

		rtyp := types.New(types.T_datetime, 0, 6)
		rvec := vector.NewConstFixed(rtyp, rs[0], inVec.Length(), proc.Mp())
		return rvec, nil
	} else {
		rs := make([]types.Datetime, len(times))
		ints, fracs := splitDecimalToIntAndFrac(times)
		fromunixtime.UnixToDateTimeWithNsec(proc.SessionInfo.TimeZone, ints, fracs, rs)
		rtyp := types.New(types.T_datetime, 0, 6)
		rvec := vector.NewVec(rtyp)
		nulls.Set(rvec.GetNulls(), inVec.GetNulls())
		vector.AppendFixedList(rvec, rs, nil, proc.Mp())
		for i := 0; i < len(times); i++ {
			if times[i] < 0 || times[i] > max_unix_timestamp_int {
				nulls.Add(rvec.GetNulls(), uint64(i))
			}
		}
		return rvec, nil
	}
}

func FromUnixTimeInt64Format(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := ivecs[0]
	formatVec := ivecs[1]
	rtyp := types.T_varchar.ToType()
	if !formatVec.IsConst() {
		return nil, moerr.NewInvalidArg(proc.Ctx, "from_unixtime format", "not constant")
	}
	if inVec.IsConstNull() || formatVec.IsConstNull() {
		return vector.NewConstNull(rtyp, inVec.Length(), proc.Mp()), nil
	}

	formatMask := formatVec.GetStringAt(0)
	if inVec.IsConst() {
		times := vector.MustFixedCol[int64](inVec)
		rs := make([]types.Datetime, 1)
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, times, rs)
		resCol, err := binary.CalcDateFromat(proc.Ctx, rs, formatMask, inVec.GetNulls())
		if err != nil {
			return nil, err
		}
		vec := vector.NewConstBytes(rtyp, []byte(resCol[0]), inVec.Length(), proc.Mp())
		nulls.Set(vec.GetNulls(), inVec.GetNulls())
		if times[0] < 0 || times[0] > max_unix_timestamp_int {
			nulls.Add(vec.GetNulls(), 0)
		}
		return vec, nil
	} else {
		times := vector.MustFixedCol[int64](inVec)
		rs := make([]types.Datetime, len(times))
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, times, rs)
		resCol, err := binary.CalcDateFromat(proc.Ctx, rs, formatMask, inVec.GetNulls())
		if err != nil {
			return nil, err
		}
		rvec := vector.NewVec(rtyp)
		vector.AppendStringList(rvec, resCol, nil, proc.Mp())
		nulls.Set(rvec.GetNulls(), inVec.GetNulls())
		for i := 0; i < len(times); i++ {
			if times[i] < 0 || times[i] > max_unix_timestamp_int {
				nulls.Add(rvec.GetNulls(), uint64(i))
			}
		}
		return rvec, nil
	}
}

func FromUnixTimeFloat64Format(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := ivecs[0]
	formatVec := ivecs[1]
	rtyp := types.T_varchar.ToType()
	if !formatVec.IsConst() {
		return nil, moerr.NewInvalidArg(proc.Ctx, "from_unixtime format", "not constant")
	}
	if inVec.IsConstNull() || formatVec.IsConstNull() {
		return vector.NewConstNull(rtyp, inVec.Length(), proc.Mp()), nil
	}

	formatMask := formatVec.GetStringAt(0)
	if inVec.IsConst() {
		times := vector.MustFixedCol[float64](inVec)
		rs := make([]types.Datetime, 1)
		ints, fracs := splitDecimalToIntAndFrac(times)
		fromunixtime.UnixToDateTimeWithNsec(proc.SessionInfo.TimeZone, ints, fracs, rs)
		resCol, err := binary.CalcDateFromat(proc.Ctx, rs, formatMask, inVec.GetNulls())
		if err != nil {
			return nil, err
		}
		vec := vector.NewConstBytes(rtyp, []byte(resCol[0]), inVec.Length(), proc.Mp())
		nulls.Set(vec.GetNulls(), inVec.GetNulls())
		if times[0] < 0 || times[0] > max_unix_timestamp_int {
			nulls.Add(vec.GetNulls(), 0)
		}
		return vec, nil
	} else {
		times := vector.MustFixedCol[float64](inVec)
		rs := make([]types.Datetime, len(times))
		ints, fracs := splitDecimalToIntAndFrac(times)
		fromunixtime.UnixToDateTimeWithNsec(proc.SessionInfo.TimeZone, ints, fracs, rs)
		resCol, err := binary.CalcDateFromat(proc.Ctx, rs, formatMask, inVec.GetNulls())
		if err != nil {
			return nil, err
		}

		vec := vector.NewVec(rtyp)
		vector.AppendStringList(vec, resCol, nil, proc.Mp())
		nulls.Set(vec.GetNulls(), inVec.GetNulls())
		for i := 0; i < len(times); i++ {
			if times[i] < 0 || times[i] > max_unix_timestamp_int {
				nulls.Add(vec.GetNulls(), uint64(i))
			}
		}
		return vec, nil
	}
}

func splitDecimalToIntAndFrac(floats []float64) ([]int64, []int64) {
	ints := make([]int64, len(floats))
	fracs := make([]int64, len(floats))
	for i, f := range floats {
		intPart := int64(f)
		nano := (f - float64(intPart)) * math.Pow10(9)
		fracPart := int64(nano)
		ints[i] = intPart
		fracs[i] = fracPart
	}
	return ints, fracs
}

func FromUnixTimeUint64(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	uint64ToInt64 := func(from []uint64) []int64 {
		to := make([]int64, len(from))
		for i := range from {
			to[i] = int64(from[i])
		}
		return to
	}
	inVec := ivecs[0]
	times := vector.MustFixedCol[uint64](inVec)
	if inVec.IsConstNull() {
		return vector.NewConstNull(types.T_datetime.ToType(), inVec.Length(), proc.Mp()), nil
	}
	if inVec.IsConst() {
		var rs [1]types.Datetime
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, uint64ToInt64(times), rs[:])
		rvec := vector.NewConstFixed(types.T_datetime.ToType(), rs[0], inVec.Length(), proc.Mp())
		nulls.Set(rvec.GetNulls(), inVec.GetNulls())

		if times[0] > max_unix_timestamp_int {
			nulls.Add(rvec.GetNulls(), 0)
		}
		return rvec, nil
	} else {
		rs := make([]types.Datetime, len(times))
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, uint64ToInt64(times), rs)
		rvec := vector.NewVec(types.T_datetime.ToType())
		vector.AppendFixedList(rvec, rs, nil, proc.Mp())
		nulls.Set(rvec.GetNulls(), inVec.GetNulls())

		for i := 0; i < len(times); i++ {
			if times[i] > max_unix_timestamp_int {
				nulls.Add(rvec.GetNulls(), uint64(i))
			}
		}
		return rvec, nil
	}
}

func FromUnixTimeUint64Format(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	uint64ToInt64 := func(from []uint64) []int64 {
		to := make([]int64, len(from))
		for i := range from {
			to[i] = int64(from[i])
		}
		return to
	}

	inVec := ivecs[0]
	formatVec := ivecs[1]
	rtyp := types.T_varchar.ToType()
	if !formatVec.IsConst() {
		return nil, moerr.NewInvalidArg(proc.Ctx, "from_unixtime format", "not constant")
	}
	if inVec.IsConstNull() || formatVec.IsConstNull() {
		return vector.NewConstNull(rtyp, inVec.Length(), proc.Mp()), nil
	}

	formatMask := formatVec.GetStringAt(0)
	if inVec.IsConst() {
		times := vector.MustFixedCol[uint64](inVec)
		rs := make([]types.Datetime, 1)
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, uint64ToInt64(times), rs)
		resCol, err := binary.CalcDateFromat(proc.Ctx, rs, formatMask, inVec.GetNulls())
		if err != nil {
			return nil, err
		}
		rvec := vector.NewConstBytes(rtyp, []byte(resCol[0]), inVec.Length(), proc.Mp())
		nulls.Set(rvec.GetNulls(), inVec.GetNulls())
		if times[0] > max_unix_timestamp_int {
			nulls.Add(rvec.GetNulls(), 0)
		}
		return rvec, nil
	} else {
		times := vector.MustFixedCol[uint64](inVec)
		rs := make([]types.Datetime, len(times))
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, uint64ToInt64(times), rs)
		resCol, err := binary.CalcDateFromat(proc.Ctx, rs, formatMask, inVec.GetNulls())
		if err != nil {
			return nil, err
		}
		vec := vector.NewVec(rtyp)
		vector.AppendStringList(vec, resCol, nil, proc.Mp())
		nulls.Set(vec.GetNulls(), inVec.GetNulls())
		for i := 0; i < len(times); i++ {
			if times[i] > max_unix_timestamp_int {
				nulls.Add(vec.GetNulls(), uint64(i))
			}
		}
		return vec, nil
	}
}
