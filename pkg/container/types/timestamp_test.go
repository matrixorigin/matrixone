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
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestTimestamp_String(t *testing.T) {
	a, err := ParseTimestamp(time.UTC, "2012-01-01 11:11:11", 6)
	require.NoError(t, err)
	resultStr := a.String()
	require.Equal(t, "2012-01-01 11:11:11.000000 UTC", resultStr)
	a, err = ParseTimestamp(time.UTC, "20120101111111", 6)
	require.NoError(t, err)
	resultStr = a.String()
	require.Equal(t, "2012-01-01 11:11:11.000000 UTC", resultStr)

	a, err = ParseTimestamp(time.UTC, "2012-01-01 11:11:11.123", 6)
	resultStr3 := a.String()
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11.123000 UTC", resultStr3)
	a, err = ParseTimestamp(time.UTC, "20120101111111.123", 6)
	resultStr3 = a.String()
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11.123000 UTC", resultStr3)

}

func TestTimestamp_String2(t *testing.T) {
	a, err := ParseTimestamp(time.UTC, "2012-01-01 11:11:11", 6)
	require.NoError(t, err)
	resultStr := a.String2(time.UTC, 6)
	require.Equal(t, "2012-01-01 11:11:11.000000", resultStr)
	a, err = ParseTimestamp(time.UTC, "20120101111111", 6)
	require.NoError(t, err)
	resultStr = a.String2(time.UTC, 6)
	require.Equal(t, "2012-01-01 11:11:11.000000", resultStr)

	resultStr1 := a.String2(time.UTC, 3)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11.000", resultStr1)

	resultStr2 := a.String2(time.UTC, 0)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11", resultStr2)

	a, err = ParseTimestamp(time.UTC, "2012-01-01 11:11:11.123", 6)
	resultStr3 := a.String2(time.UTC, 0)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11", resultStr3)
	a, err = ParseTimestamp(time.UTC, "20120101111111.123", 6)
	resultStr3 = a.String2(time.UTC, 0)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11", resultStr3)

	resultStr4 := a.String2(time.UTC, 3)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11.123", resultStr4)

	resultStr5 := a.String2(time.UTC, 6)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11.123000", resultStr5)

	a, err = ParseTimestamp(time.UTC, "2012-01-01 11:11:11.123456", 3)
	resultStr6 := a.String2(time.UTC, 0)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11", resultStr6)

	resultStr7 := a.String2(time.UTC, 3)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11.123", resultStr7)

	resultStr8 := a.String2(time.UTC, 6)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11.123000", resultStr8)
}

func TestParseTimestamp(t *testing.T) {
	a, err := ParseTimestamp(time.UTC, "1970-01-01 00:00:01", 6)
	require.NoError(t, err)
	require.Equal(t, int64(TimestampMinValue), int64(a))

	a, err = ParseTimestamp(time.UTC, "1970-01-01 00:00:01.123", 6)
	require.NoError(t, err)
	require.Equal(t, int64(TimestampMinValue+123000), int64(a))

	a, err = ParseTimestamp(time.UTC, "1970-01-01 00:00:01.123456", 6)
	require.NoError(t, err)
	require.Equal(t, int64(TimestampMinValue+123456), int64(a))

	a, err = ParseTimestamp(time.UTC, "1970-01-01 00:00:01.123456", 3)
	require.NoError(t, err)
	require.Equal(t, int64(a), int64(TimestampMinValue+123000))

	a, err = ParseTimestamp(time.UTC, "1970-01-01 00:00:01.12356", 3)
	require.NoError(t, err)
	require.Equal(t, int64(TimestampMinValue+124000), int64(a))

	a, err = ParseTimestamp(time.UTC, "1970-01-01 00:00:01.12345", 0)
	require.NoError(t, err)
	require.Equal(t, int64(TimestampMinValue), int64(a))

	a, err = ParseTimestamp(time.UTC, "1970-01-01 00:00:01.52345", 0)
	require.NoError(t, err)
	require.Equal(t, int64(TimestampMinValue+MicroSecsPerSec), int64(a))

	a, err = ParseTimestamp(time.UTC, "1966-01-01 00:00:01.52345", 0)
	require.NoError(t, err)
	require.Equal(t, int64(62009366402000000), int64(a))

	//ts, err := ParseTimestamp(time.UTC, "9999-12-31 23:59:59.5", 0)
	//fmt.Println(int64(ts))
	//require.Error(t, err)
}

func TestLocation(t *testing.T) {
	loc := time.FixedZone("test", 8*3600)
	locPtr := (*unsafeLoc)(unsafe.Pointer(loc))
	require.Equal(t, len(locPtr.zone), 1)

	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	locPtr = (*unsafeLoc)(unsafe.Pointer(loc))
	require.Greater(t, len(locPtr.zone), 1)
}

func TestTimestamp_TruncateToScale(t *testing.T) {
	// Test timestamp with full microsecond precision: 1970-01-01 00:00:01.123456
	ts := TimestampMinValue + 123456

	// Test scale 0 (seconds, no fractional part)
	truncated := ts.TruncateToScale(0)
	require.Equal(t, int64(TimestampMinValue), int64(truncated))

	// Test scale 3 (milliseconds) - should truncate to .123000
	truncated = ts.TruncateToScale(3)
	require.Equal(t, int64(TimestampMinValue+123000), int64(truncated))

	// Test scale 6 (microseconds) - should not change
	truncated = ts.TruncateToScale(6)
	require.Equal(t, int64(ts), int64(truncated))

	// Test rounding up: 1970-01-01 00:00:01.123500 with scale 3 should round to .124000
	ts2 := TimestampMinValue + 123500
	truncated = ts2.TruncateToScale(3)
	require.Equal(t, int64(TimestampMinValue+124000), int64(truncated))

	// Test rounding up to next second: 1970-01-01 00:00:01.999999 with scale 0 should round to 2 seconds
	ts3 := TimestampMinValue + 999999
	truncated = ts3.TruncateToScale(0)
	require.Equal(t, int64(TimestampMinValue+MicroSecsPerSec), int64(truncated))

	// Test scale 1 (0.1 seconds)
	ts4 := TimestampMinValue + 156789
	truncated = ts4.TruncateToScale(1)
	require.Equal(t, int64(TimestampMinValue+200000), int64(truncated)) // rounds up to .2

	// Test scale 2 (0.01 seconds)
	ts5 := TimestampMinValue + 125678
	truncated = ts5.TruncateToScale(2)
	require.Equal(t, int64(TimestampMinValue+130000), int64(truncated)) // rounds up to .13
}
