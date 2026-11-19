// Copyright 2026 Matrix Origin
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

package function

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Go owns the independent Gregorian calendar. UnixMicro differences remain
// representable through year9999; time.Sub(epoch) would overflow Duration.
func fixedTruncDatetime(value time.Time) types.Datetime {
	return types.Datetime(value.UnixMicro() - time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro())
}

func fixedTruncCalendar(unit string, value time.Time) types.Datetime {
	year, month, day := value.Date()
	hour, minute, second := value.Clock()
	switch unit {
	case "day":
		hour, minute, second = 0, 0, 0
	case "hour":
		minute, second = 0, 0
	case "minute":
		second = 0
	case "second":
	default:
		panic("fixed-unit test oracle received unsupported unit")
	}
	return fixedTruncDatetime(time.Date(year, month, day, hour, minute, second, 0, time.UTC))
}

func TestDateTruncFixedUnitsCalendarOracle(t *testing.T) {
	for _, text := range []string{
		"0001-01-01 00:00:00", "0001-01-01 00:00:00.000001",
		"0004-02-29 23:59:59.999999", "1600-02-29 12:34:56.123456",
		"1700-03-01 00:00:00.000001", "1900-03-01 00:00:00.000001",
		"1923-12-31 23:59:59.999999", "1924-01-01 00:00:00.000001",
		"2000-02-29 12:34:56.123456", "2099-12-31 23:59:59.999999",
		"2100-03-01 00:00:00.000001", "2024-02-29 12:34:59.999999",
		"2024-02-29 12:35:00", "2024-02-29 23:59:59.999999",
		"2024-03-01 00:00:00", "2026-01-01 12:34:56.123456",
		"9999-12-31 23:59:59.999999",
	} {
		value, err := time.ParseInLocation("2006-01-02 15:04:05.999999", text, time.UTC)
		require.NoError(t, err)
		for _, unit := range []string{"second", "minute", "hour", "day"} {
			t.Run(text+"/"+unit, func(t *testing.T) {
				got, err := dateTruncCore(unit, fixedTruncDatetime(value))
				require.NoError(t, err)
				require.Equal(t, fixedTruncCalendar(unit, value), got)
			})
		}
	}
}

func TestDateTruncFixedUnitsKeepRawFallback(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input types.Datetime
		// Existing raw-negative behavior includes uint8 clock-field wrapping.
		// These literals are compatibility goldens, not new SQL input support.
		want [4]types.Datetime // second, minute, hour, day
	}{
		{"negative_microseconds", -2, [4]types.Datetime{0, 0, 0, 0}},
		{"negative_second", -1_000_000, [4]types.Datetime{255_000_000, 0, 0, 0}},
		{"negative_minute", -60_000_000, [4]types.Datetime{15_300_000_000, 15_300_000_000, 0, 0}},
		{"above_max", types.Datetime(types.TimestampMaxValue) + 1, [4]types.Datetime{
			types.Datetime(types.TimestampMaxValue) + 1, types.Datetime(types.TimestampMaxValue) + 1,
			types.Datetime(types.TimestampMaxValue) + 1, types.Datetime(types.TimestampMaxValue) + 1}},
	} {
		for index, unit := range []string{"second", "minute", "hour", "day"} {
			t.Run(tc.name+"/"+unit, func(t *testing.T) {
				got, err := dateTruncCore(unit, tc.input)
				require.NoError(t, err)
				require.Equal(t, tc.want[index], got)
			})
		}
	}
	// Date.Year returns uint16. The legacy raw year65537 wraps to year1;
	// dropping the upper range guard would incorrectly keep year65537.
	input := fixedTruncDatetime(time.Date(65537, 5, 19, 12, 34, 56, 123456000, time.UTC))
	legacy := time.Date(1, 5, 19, 12, 34, 56, 0, time.UTC)
	for _, unit := range []string{"second", "minute", "hour", "day"} {
		got, err := dateTruncCore(unit, input)
		require.NoError(t, err)
		require.Equal(t, fixedTruncCalendar(unit, legacy), got, unit)
	}
	for _, tc := range []struct{ unit, want string }{
		{"year", "2024-01-01 00:00:00"}, {"quarter", "2024-04-01 00:00:00"},
		{"month", "2024-05-01 00:00:00"}, {"week", "2024-05-13 00:00:00"},
	} {
		got, err := dateTruncCore(tc.unit, fixedTruncDatetime(time.Date(2024, 5, 19, 12, 34, 56, 123456000, time.UTC)))
		require.NoError(t, err)
		want, err := time.ParseInLocation("2006-01-02 15:04:05", tc.want, time.UTC)
		require.NoError(t, err)
		require.Equal(t, fixedTruncDatetime(want), got, tc.unit)
	}
}

func fixedTruncProcess(t testing.TB) *process.Process {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
	t.Cleanup(proc.Free)
	return proc
}

func fixedTruncValues(t testing.TB, proc *process.Process, values []types.Datetime, nulls []bool) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(types.T_datetime.ToTypeWithScale(6))
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	require.NoError(t, vector.AppendFixedList(vec, values, nulls, proc.Mp()))
	return vec
}

func TestDateTruncFixedUnitsVectorContract(t *testing.T) {
	modern := time.Date(2026, 1, 1, 12, 34, 56, 123456000, time.UTC)
	for _, unit := range []string{"second", "minute", "hour", "day"} {
		t.Run("constant/"+unit, func(t *testing.T) {
			proc := fixedTruncProcess(t)
			values := fixedTruncValues(t, proc, []types.Datetime{types.DatetimeEpoch, types.ZeroDatetime, fixedTruncDatetime(modern), 0}, []bool{false, false, false, true})
			units, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(strings.ToUpper(unit)), 4, proc.Mp())
			require.NoError(t, err)
			t.Cleanup(func() { units.Free(proc.Mp()) })
			result := vector.NewFunctionResultWrapper(types.T_datetime.ToTypeWithScale(6), proc.Mp())
			t.Cleanup(result.Free)
			require.NoError(t, result.PreExtendAndReset(4))
			require.NoError(t, DateTrunc([]*vector.Vector{units, values}, result, proc, 4, nil))
			vec := result.GetResultVector()
			require.False(t, vec.IsNull(0))
			require.Equal(t, types.DatetimeEpoch, vector.GetFixedAtWithTypeCheck[types.Datetime](vec, 0))
			require.True(t, vec.IsNull(1))
			require.False(t, vec.IsNull(2))
			require.Equal(t, fixedTruncCalendar(unit, modern), vector.GetFixedAtWithTypeCheck[types.Datetime](vec, 2))
			require.True(t, vec.IsNull(3))
		})
	}
	t.Run("per_row_units_and_null_error_order", func(t *testing.T) {
		proc := fixedTruncProcess(t)
		units := vector.NewVec(types.T_varchar.ToType())
		t.Cleanup(func() { units.Free(proc.Mp()) })
		names := []string{"second", "MINUTE", "hour", "DAY", "bogus", "bogus", "bogus"}
		for index, unit := range names {
			require.NoError(t, vector.AppendBytes(units, []byte(unit), index == 6, proc.Mp()))
		}
		value := fixedTruncDatetime(modern)
		values := fixedTruncValues(t, proc, []types.Datetime{value, value, value, value, value, types.ZeroDatetime, value}, []bool{false, false, false, false, true, false, false})
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToTypeWithScale(6), proc.Mp())
		t.Cleanup(result.Free)
		require.NoError(t, result.PreExtendAndReset(len(names)))
		require.NoError(t, DateTrunc([]*vector.Vector{units, values}, result, proc, len(names), nil))
		vec := result.GetResultVector()
		for index := range names {
			if index >= 4 {
				require.True(t, vec.IsNull(uint64(index)))
				continue
			}
			require.False(t, vec.IsNull(uint64(index)))
			require.Equal(t, fixedTruncCalendar(strings.ToLower(names[index]), modern), vector.GetFixedAtWithTypeCheck[types.Datetime](vec, index))
		}
	})
	for _, unit := range []string{"bogus", ""} {
		t.Run(fmt.Sprintf("invalid_unit/%q", unit), func(t *testing.T) {
			proc := fixedTruncProcess(t)
			units, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(unit), 1, proc.Mp())
			require.NoError(t, err)
			t.Cleanup(func() { units.Free(proc.Mp()) })
			values := fixedTruncValues(t, proc, []types.Datetime{types.DatetimeEpoch}, nil)
			result := vector.NewFunctionResultWrapper(types.T_datetime.ToTypeWithScale(6), proc.Mp())
			t.Cleanup(result.Free)
			require.NoError(t, result.PreExtendAndReset(1))
			err = DateTrunc([]*vector.Vector{units, values}, result, proc, 1, nil)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		})
	}
}
