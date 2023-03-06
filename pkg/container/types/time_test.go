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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestTime_StringAndString2(t *testing.T) {
	testCases := []struct {
		name       string
		input      Time
		strExpect  string
		str2Expect string
		scale      int32
	}{
		{
			name: "TestString-NoPrecision",
			// 11:22:33
			input:      TimeFromClock(false, 11, 22, 33, 0),
			strExpect:  "11:22:33",
			str2Expect: "11:22:33",
			scale:      0,
		},
		{
			name: "TestString-Precision",
			// 11:22:33.123
			input:      TimeFromClock(false, 11, 22, 33, 123000),
			strExpect:  "11:22:33",
			str2Expect: "11:22:33.12300",
			scale:      5,
		},
		{
			name: "TestString-ShortterPrecision",
			// 11:22:33.123
			input:      TimeFromClock(false, 11, 22, 33, 123000),
			strExpect:  "11:22:33",
			str2Expect: "11:22:33.12",
			scale:      2,
		},
		{
			name: "TestString-Minus",
			// 11:22:33.125000
			input:      TimeFromClock(true, 11, 22, 33, 125000),
			strExpect:  "-11:22:33",
			str2Expect: "-11:22:33.12",
			scale:      2,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			// only 1 input
			strActual := c.input.String()
			require.Equal(t, strActual, c.strExpect)
			str2Actual := c.input.String2(c.scale)
			require.Equal(t, str2Actual, c.str2Expect)
		})
	}
}

func TestTime_ParseTimeFromString(t *testing.T) {
	testCases := []struct {
		name     string
		inputStr string
		expected Time
		scale    int32
		isErr    bool
	}{
		// ==================== Date format: yyyy-mm-dd hh:mm:ss(.msec) ====================
		{
			name: "TestParse-NoPrecision",
			// 11:22:33
			inputStr: "2022-12-12 11:22:33",
			expected: TimeFromClock(false, 11, 22, 33, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse-Precision01",
			// 11:22:33
			inputStr: "2022-12-12 11:22:33.1234",
			expected: TimeFromClock(false, 11, 22, 33, 123000),
			scale:    3,
			isErr:    false,
		},
		{
			name: "TestParse-Precision02",
			// 11:22:33
			inputStr: "2022-12-12 11:22:33.1235",
			expected: TimeFromClock(false, 11, 22, 33, 124000),
			scale:    3,
			isErr:    false,
		},
		{
			name: "TestParse-DateError",
			// invalid datetime
			inputStr: "2022-12-33 11:22:33",
			isErr:    true,
		},
		// ==================== Date format: yyyymmddhhmmss(.msec) ====================
		{
			name: "TestParse2-NoPrecision",
			// 11:22:33
			inputStr: "20221212112233",
			expected: TimeFromClock(false, 2022121211, 22, 33, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse2-Precision01",
			// 11:22:33
			inputStr: "20221212112233.1234",
			expected: TimeFromClock(false, 2022121211, 22, 33, 123000),
			scale:    3,
			isErr:    false,
		},
		{
			name: "TestParse2-Precision02",
			// 11:22:33
			inputStr: "20221212112233.1235",
			expected: TimeFromClock(false, 2022121211, 22, 33, 124000),
			scale:    3,
			isErr:    false,
		},
		// ==================== Time format: hh:mm:ss(.msec) ====================
		{
			name: "TestParse3-NoPrecision",
			// 11:22:33
			inputStr: "11:22:33",
			expected: TimeFromClock(false, 11, 22, 33, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse3-NoPrecision2",
			// 11:22:33
			inputStr: "555:22:33",
			expected: TimeFromClock(false, 555, 22, 33, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse3-NoPrecision2",
			// 11:22:33
			inputStr: "-555:22:33",
			expected: TimeFromClock(true, 555, 22, 33, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse3-Precision",
			// 11:22:33
			inputStr: "11:22:33.1234",
			expected: TimeFromClock(false, 11, 22, 33, 123000),
			scale:    3,
			isErr:    false,
		},
		{
			name: "TestParse3-Precision",
			// 11:22:33
			inputStr: "11:22:33.1235",
			expected: TimeFromClock(false, 11, 22, 33, 124000),
			scale:    3,
			isErr:    false,
		},
		// ==================== Time format: hhmmss(.msec) ====================
		{
			name: "TestParse4-NoPrecision01",
			// 11:22:33
			inputStr: "1",
			expected: TimeFromClock(false, 0, 0, 1, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse4-NoPrecision02",
			// 11:22:33
			inputStr: "112",
			expected: TimeFromClock(false, 0, 1, 12, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse4-NoPrecision03",
			// -00:01:12
			inputStr: "-112",
			expected: TimeFromClock(true, 0, 1, 12, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse4-NoPrecision04",
			// -01:12:32
			inputStr: "-11232",
			expected: TimeFromClock(true, 1, 12, 32, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse4-Precision01",
			// -01:12:32.123
			inputStr: "-11232.123",
			expected: TimeFromClock(true, 1, 12, 32, 123000),
			scale:    3,
			isErr:    false,
		},
		{
			name: "TestParse4-Precision02",
			// -01:12:32.124
			inputStr: "11232.1235",
			expected: TimeFromClock(false, 1, 12, 32, 124000),
			scale:    3,
			isErr:    false,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			// only 1 input
			parsed, err := ParseTime(c.inputStr, c.scale)
			if !c.isErr {
				require.NoError(t, err)
				require.Equal(t, parsed, c.expected)
			} else {
				require.Equal(t, err, moerr.NewInvalidInput(context.TODO(), "invalid time value %s", c.inputStr))
			}
		})

	}
}

func TestTime_CastBetweenTimeInt64(t *testing.T) {
	testCases := []struct {
		name     string
		input    int64
		expected Time
		scale    int32
		isErr    bool
	}{
		{
			name: "TestParse-validInt64-01",
			// 11:22:33
			input:    112233,
			expected: TimeFromClock(false, 11, 22, 33, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse-validInt64-02",
			// -11:22:33
			input:    -112233,
			expected: TimeFromClock(true, 11, 22, 33, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse-validInt64-03",
			// -00:01:12
			input:    -112,
			expected: TimeFromClock(true, 0, 1, 12, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse-validInt64-03",
			// 00:00:00
			input:    0,
			expected: TimeFromClock(false, 0, 0, 0, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse-validInt64-03",
			// -11:22:33
			input:    0,
			expected: TimeFromClock(false, 0, 0, 0, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse-validInt64-03",
			// 2562047787:59:59
			input:    20221212112233,
			expected: TimeFromClock(false, 2022121211, 22, 33, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse-validInt64-03",
			// 2562047787:59:59
			input:    25620477875959,
			expected: TimeFromClock(false, 2562047787, 59, 59, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name: "TestParse-validInt64-03",
			// 2562047787:59:59
			input:    -25620477875959,
			expected: TimeFromClock(true, 2562047787, 59, 59, 0),
			scale:    0,
			isErr:    false,
		},
		{
			name:     "TestParse-invalidInt64",
			input:    25620477880000,
			expected: TimeFromClock(false, 0, 0, 0, 0),
			scale:    0,
			isErr:    true,
		},
		{
			name:     "TestParse-invalidInt64",
			input:    -25620477880000,
			expected: TimeFromClock(true, 0, 0, 0, 0),
			scale:    0,
			isErr:    true,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			// Int64 to Time
			parsed, err := ParseInt64ToTime(c.input, c.scale)
			if !c.isErr {
				require.NoError(t, err)
				require.Equal(t, parsed, c.expected)

				// Time to Int64
				toInt := c.expected.ToInt64()
				require.Equal(t, toInt, c.input)
			} else {
				require.Equal(t, err, moerr.NewInvalidInput(context.TODO(), "invalid time value %d", c.input))
			}
		})
	}
}

func TestTime_ParseTimeFromDecimal128(t *testing.T) {
	testCases := []struct {
		name      string
		dcmStr    string
		expected  Time
		expected2 string
		scale     int32
		isCarry   bool
		isErr     bool
	}{
		{
			name:     "TestParse-ValidDecimal128",
			dcmStr:   "112233.444",
			expected: TimeFromClock(false, 11, 22, 33, 444000),
			scale:    3,
			isCarry:  false,
			isErr:    false,
		},
		{
			name:      "TestParse-ValidDecimal128",
			dcmStr:    "112233.44455",
			expected:  TimeFromClock(false, 11, 22, 33, 445000),
			expected2: "112233.445",
			scale:     3,
			isCarry:   true,
			isErr:     false,
		},
		{
			name:      "TestParse-ValidDecimal128",
			dcmStr:    "20201212112233.44455",
			expected:  TimeFromClock(false, 2020121211, 22, 33, 445000),
			expected2: "20201212112233.445",
			scale:     3,
			isCarry:   true,
			isErr:     false,
		},
		{
			name:      "TestParse-ValidDecimal128",
			dcmStr:    "-20201212112233.44455",
			expected:  TimeFromClock(true, 2020121211, 22, 33, 445000),
			expected2: "-20201212112233.445",
			scale:     3,
			isCarry:   true,
			isErr:     false,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			// decimal128 to Time
			dcm, err := Decimal128_FromString(c.dcmStr)
			println("the decimal is ", dcm.String())
			require.NoError(t, err)
			if !c.isErr {
				parsed, err := ParseDecimal128lToTime(dcm, c.scale)
				require.NoError(t, err)
				require.Equal(t, parsed, c.expected)

				// Time to Decimal
				toDcm, err := c.expected.ToDecimal128(context.TODO(), 34, c.scale)
				println("the toDecimal is ", toDcm.String())
				require.NoError(t, err)
				if c.isCarry {
					// if the scale cause carry
					// must compare it with decimal from c.expected2
					newdcm, err := Decimal128_FromString(c.expected2)
					require.NoError(t, err)
					require.Equal(t, toDcm, newdcm)
				} else {
					require.Equal(t, toDcm, dcm)
				}

			}
		})
	}
}
