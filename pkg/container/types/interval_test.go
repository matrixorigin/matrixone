// Copyright 2021 Matrix Origin
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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIntervalType(t *testing.T) {
	var it IntervalType
	var err error
	it, err = IntervalTypeOf("Microsecond")
	require.Equal(t, it, MicroSecond, "type failed, us")
	require.Equal(t, err, nil, "type failed, us")

	it, err = IntervalTypeOf("second")
	require.Equal(t, it, Second, "type failed, s")
	require.Equal(t, err, nil, "type failed, s")

	it, err = IntervalTypeOf("MiNuTe")
	require.Equal(t, it, Minute, "type failed, m")
	require.Equal(t, err, nil, "type failed, m")

	it, err = IntervalTypeOf("HOUR")
	require.Equal(t, it, Hour, "type failed, h")
	require.Equal(t, err, nil, "type failed, h")

	it, err = IntervalTypeOf("daY")
	require.Equal(t, it, Day, "type failed, d")
	require.Equal(t, err, nil, "type failed, d")

	it, err = IntervalTypeOf("month")
	require.Equal(t, it, Month, "type failed, mn")
	require.Equal(t, err, nil, "type failed, mn")

	it, err = IntervalTypeOf("quarter")
	require.Equal(t, it, Quarter, "type failed, q")
	require.Equal(t, err, nil, "type failed, q")

	it, err = IntervalTypeOf("year")
	require.Equal(t, it, Year, "type failed, y")
	require.Equal(t, err, nil, "type failed, y")

	it, err = IntervalTypeOf("Second_Microsecond")
	require.Equal(t, it, Second_MicroSecond, "type failed, s_us")
	require.Equal(t, err, nil, "type failed, s_us")

	it, err = IntervalTypeOf("Hour_Minute")
	require.Equal(t, it, Hour_Minute, "type failed, h_m")
	require.Equal(t, err, nil, "type failed, h_m")

	_, err = IntervalTypeOf("Invalid_interval_type")
	require.NotEqual(t, err, nil, "type failed, invalid_internal_type")

	_, err = IntervalTypeOf("IntervalTypeMax")
	require.NotEqual(t, err, nil, "type failed, invalid_internal_type")

	_, err = IntervalTypeOf("YEAR_DAY")
	require.NotEqual(t, err, nil, "type failed, y_d")

	// Parser should resolve this
	_, err = IntervalTypeOf("DAYS")
	require.NotEqual(t, err, nil, "type failed, days")

	_, err = IntervalTypeOf("hour__second")
	require.NotEqual(t, err, nil, "type failed, h__s")

	_, err = IntervalTypeOf("day minute")
	require.NotEqual(t, err, nil, "type failed, d m")
}

func TestConv(t *testing.T) {
	var val int64
	var vt IntervalType
	var err error

	val, vt, err = NormalizeInterval("1 2", Hour_Minute)
	require.Equal(t, val, int64(62), "HM error")
	require.Equal(t, vt, Minute, "HM error")
	require.Equal(t, err, nil, "HM error")

	val, vt, err = NormalizeInterval("1-2", Hour_Minute)
	require.Equal(t, val, int64(62), "HM error")
	require.Equal(t, vt, Minute, "HM error")
	require.Equal(t, err, nil, "HM error")

	val, vt, err = NormalizeInterval("1-2-foo", Hour_Minute)
	require.Equal(t, val, int64(62), "HM error")
	require.Equal(t, vt, Minute, "HM error")
	require.Equal(t, err, nil, "HM error")

	val, vt, err = NormalizeInterval("", Hour_Minute)
	require.Equal(t, val, int64(0), "HM error")
	require.Equal(t, vt, Minute, "HM error")
	require.Equal(t, err, nil, "HM error")

	val, vt, err = NormalizeInterval("foo", Hour_Minute)
	require.Equal(t, val, int64(0), "HM error")
	require.Equal(t, vt, Minute, "HM error")
	require.Equal(t, err, nil, "HM error")

	val, vt, err = NormalizeInterval("1 01:02:03.4", Day_MicroSecond)
	val2, vt2, _ := NormalizeInterval("1 01:02:03", Day_MicroSecond)
	val3, vt3, _ := NormalizeInterval("0 00:00:00.4", Day_MicroSecond)
	require.Equal(t, err, nil, "HM error")
	require.Equal(t, vt, MicroSecond, "D_US error")
	require.Equal(t, vt, vt2, "D_US error")
	require.Equal(t, vt, vt3, "D_US error")
	require.Equal(t, val3, int64(400000), "D_US error")
	require.Equal(t, val2, int64(24*60*60*1000000+1*60*60*1000000+2*60*1000000+3*1000000), "D_US error")
	require.Equal(t, val, val2+val3, "D_US error")

	val, vt, err = NormalizeInterval("1 -1", Year_Month)
	// Yes, 13.  -1 is parsed as - delim 1
	require.Equal(t, val, int64(13), "YM error")
	require.Equal(t, vt, Month, "YM error")
	require.Equal(t, err, nil, "YM error")

	val, vt, err = NormalizeInterval("2 +1", Year_Month)
	require.Equal(t, val, int64(25), "YM error")
	require.Equal(t, vt, Month, "YM error")
	require.Equal(t, err, nil, "YM error")

	val, vt, err = NormalizeInterval("-1 +13", Year_Month)
	// -1,
	require.Equal(t, val, int64(-25), "YM error")
	require.Equal(t, vt, Month, "YM error")
	require.Equal(t, err, nil, "YM error")

	// Some failure cases
	_, _, err = NormalizeInterval("-1 +13 14", Year_Month)
	require.NotEqual(t, err, nil, "YM error")

	_, _, err = NormalizeInterval("-1 +130000000000000000000000000", Year_Month)
	require.NotEqual(t, err, nil, "YM error")

	_, _, err = NormalizeInterval("123400000000000000000000 0", Year_Month)
	require.NotEqual(t, err, nil, "YM error")
}
