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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/vectorize/fromunixtime"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
)

const (
	max_unix_timestamp_int = 32536771199
)

func FromUnixTimeInt64(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := lv[0]
	times := vector.MustTCols[int64](inVec)
	if inVec.IsScalarNull() {
		return proc.AllocScalarNullVector(types.T_datetime.ToType()), nil
	}

	if inVec.IsScalar() {
		rs := make([]types.Datetime, 1)
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, times, rs)

		vec := vector.NewConstFixed(types.T_datetime.ToType(), 1, rs[0])
		if times[0] < 0 || times[0] > max_unix_timestamp_int {
			nulls.Add(vec.Nsp, 0)
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		return vec, nil
	} else {
		rs := make([]types.Datetime, len(times))
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, times, rs)
		vec := vector.NewWithFixed(types.T_datetime.ToType(), rs, nulls.NewWithSize(len(rs)), proc.GetMheap())
		for i := 0; i < len(times); i++ {
			if times[i] < 0 || times[i] > max_unix_timestamp_int {
				nulls.Add(vec.Nsp, uint64(i))
			}
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		return vec, nil
	}
}

func FromUnixTimeFloat64(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := lv[0]
	times := vector.MustTCols[float64](inVec)
	size := types.T(types.T_datetime).TypeLen()
	if inVec.IsScalarNull() {
		return proc.AllocScalarNullVector(types.T_datetime.ToType()), nil
	}
	if inVec.IsScalar() {
		rs := make([]types.Datetime, 1)
		ints, fracs := splitDecimalToIntAndFrac(times)
		fromunixtime.UnixToDateTimeWithNsec(proc.SessionInfo.TimeZone, ints, fracs, rs)

		t := types.Type{Oid: types.T_datetime, Size: int32(size), Precision: 6}
		vec := vector.NewConstFixed(t, 1, rs[0])
		if times[0] < 0 || times[0] > max_unix_timestamp_int {
			nulls.Add(vec.Nsp, 0)
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		return vec, nil
	} else {
		rs := make([]types.Datetime, len(times))
		ints, fracs := splitDecimalToIntAndFrac(times)
		fromunixtime.UnixToDateTimeWithNsec(proc.SessionInfo.TimeZone, ints, fracs, rs)
		t := types.Type{Oid: types.T_datetime, Size: int32(size), Precision: 6}
		vec := vector.NewWithFixed(t, rs, nulls.NewWithSize(len(rs)), proc.Mp())
		for i := 0; i < len(times); i++ {
			if times[i] < 0 || times[i] > max_unix_timestamp_int {
				nulls.Add(vec.Nsp, uint64(i))
			}
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		return vec, nil
	}
}

func FromUnixTimeInt64Format(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := vs[0]
	formatVec := vs[1]
	resultType := types.T_varchar.ToType()
	if !formatVec.IsScalar() {
		return nil, moerr.NewInvalidArg("from_unixtime format", "not constant")
	}
	if inVec.IsScalarNull() || formatVec.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	formatMask := formatVec.GetString(0)
	if inVec.IsScalar() {
		times := vector.MustTCols[int64](inVec)
		rs := make([]types.Datetime, 1)
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, times, rs)
		resCol, err := binary.CalcDateFromat(rs, formatMask, inVec.Nsp)
		if err != nil {
			return nil, err
		}
		vec := vector.NewConstString(resultType, 1, resCol[0])
		if times[0] < 0 || times[0] > max_unix_timestamp_int {
			nulls.Add(vec.Nsp, 0)
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		return vec, nil
	} else {
		times := vector.MustTCols[int64](inVec)
		rs := make([]types.Datetime, len(times))
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, times, rs)
		resCol, err := binary.CalcDateFromat(rs, formatMask, inVec.Nsp)
		if err != nil {
			return nil, err
		}
		vec := vector.NewWithStrings(resultType, resCol, inVec.Nsp, proc.Mp())
		for i := 0; i < len(times); i++ {
			if times[i] < 0 || times[i] > max_unix_timestamp_int {
				nulls.Add(vec.Nsp, uint64(i))
			}
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		return vec, nil
	}
}

func FromUnixTimeFloat64Format(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := vs[0]
	formatVec := vs[1]
	resultType := types.T_varchar.ToType()
	if !formatVec.IsScalar() {
		return nil, moerr.NewInvalidArg("from_unixtime format", "not constant")
	}
	if inVec.IsScalarNull() || formatVec.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	formatMask := formatVec.GetString(0)
	if inVec.IsScalar() {
		times := vector.MustTCols[float64](inVec)
		rs := make([]types.Datetime, 1)
		ints, fracs := splitDecimalToIntAndFrac(times)
		fromunixtime.UnixToDateTimeWithNsec(proc.SessionInfo.TimeZone, ints, fracs, rs)
		resCol, err := binary.CalcDateFromat(rs, formatMask, inVec.Nsp)
		if err != nil {
			return nil, err
		}
		vec := vector.NewConstString(resultType, 1, resCol[0])
		if times[0] < 0 || times[0] > max_unix_timestamp_int {
			nulls.Add(vec.Nsp, 0)
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		return vec, nil
	} else {
		times := vector.MustTCols[float64](inVec)
		rs := make([]types.Datetime, len(times))
		ints, fracs := splitDecimalToIntAndFrac(times)
		fromunixtime.UnixToDateTimeWithNsec(proc.SessionInfo.TimeZone, ints, fracs, rs)
		resCol, err := binary.CalcDateFromat(rs, formatMask, inVec.Nsp)
		if err != nil {
			return nil, err
		}

		vec := vector.NewWithStrings(resultType, resCol, inVec.Nsp, proc.Mp())
		for i := 0; i < len(times); i++ {
			if times[i] < 0 || times[i] > max_unix_timestamp_int {
				nulls.Add(vec.Nsp, uint64(i))
			}
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
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

func FromUnixTimeUint64(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	uint64ToInt64 := func(from []uint64) []int64 {
		to := make([]int64, len(from))
		for i, _ := range from {
			to[i] = int64(from[i])
		}
		return to
	}
	inVec := lv[0]
	times := vector.MustTCols[uint64](inVec)
	if inVec.IsScalarNull() {
		return proc.AllocScalarNullVector(types.T_datetime.ToType()), nil
	}
	if inVec.IsScalar() {
		rs := make([]types.Datetime, 1)
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, uint64ToInt64(times), rs)
		vec := vector.NewConstFixed(types.T_datetime.ToType(), 1, rs[0])

		if times[0] > max_unix_timestamp_int {
			nulls.Add(vec.Nsp, 0)
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		return vec, nil
	} else {
		rs := make([]types.Datetime, len(times))
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, uint64ToInt64(times), rs)
		vec := vector.NewWithFixed(types.T_datetime.ToType(), rs, nulls.NewWithSize(len(rs)), proc.Mp())

		for i := 0; i < len(times); i++ {
			if times[i] > max_unix_timestamp_int {
				nulls.Add(vec.Nsp, uint64(i))
			}
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		return vec, nil
	}
}

func FromUnixTimeUint64Format(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	uint64ToInt64 := func(from []uint64) []int64 {
		to := make([]int64, len(from))
		for i, _ := range from {
			to[i] = int64(from[i])
		}
		return to
	}

	inVec := vs[0]
	formatVec := vs[1]
	resultType := types.T_varchar.ToType()
	if !formatVec.IsScalar() {
		return nil, moerr.NewInvalidArg("from_unixtime format", "not constant")
	}
	if inVec.IsScalarNull() || formatVec.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	formatMask := formatVec.GetString(0)
	if inVec.IsScalar() {
		times := vector.MustTCols[uint64](inVec)
		rs := make([]types.Datetime, 1)
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, uint64ToInt64(times), rs)
		resCol, err := binary.CalcDateFromat(rs, formatMask, inVec.Nsp)
		if err != nil {
			return nil, err
		}
		vec := vector.NewConstString(resultType, 1, resCol[0])
		if times[0] < 0 || times[0] > max_unix_timestamp_int {
			nulls.Add(vec.Nsp, 0)
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		return vec, nil
	} else {
		times := vector.MustTCols[uint64](inVec)
		rs := make([]types.Datetime, len(times))
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, uint64ToInt64(times), rs)
		resCol, err := binary.CalcDateFromat(rs, formatMask, inVec.Nsp)
		if err != nil {
			return nil, err
		}
		vec := vector.NewWithStrings(resultType, resCol, inVec.Nsp, proc.Mp())
		for i := 0; i < len(times); i++ {
			if times[i] < 0 || times[i] > max_unix_timestamp_int {
				nulls.Add(vec.Nsp, uint64(i))
			}
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		return vec, nil
	}
}
