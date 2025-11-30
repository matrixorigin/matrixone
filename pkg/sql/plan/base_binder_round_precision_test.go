// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestResetDateFunctionArgsDecimalIntervalRounding tests that resetDateFunctionArgs correctly
// uses math.Round to handle floating point precision issues when converting decimal intervals to microseconds.
// This is critical for cases like 1.000009 SECOND where 1.000009 * 1000000 = 1000008.9999999999,
// which should round to 1000009, not truncate to 1000008.
func TestResetDateFunctionArgsDecimalIntervalRounding(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name                 string
		intervalValue        float64
		intervalUnit         string
		expectedMicroseconds int64
		description          string
	}{
		{
			name:                 "1.000009 SECOND - critical precision case",
			intervalValue:        1.000009,
			intervalUnit:         "SECOND",
			expectedMicroseconds: 1000009, // math.Round(1.000009 * 1000000) = 1000009
			description:          "1.000009 * 1000000 = 1000008.9999999999, should round to 1000009",
		},
		{
			name:                 "1.1 SECOND",
			intervalValue:        1.1,
			intervalUnit:         "SECOND",
			expectedMicroseconds: 1100000, // math.Round(1.1 * 1000000) = 1100000
			description:          "1.1 * 1000000 = 1100000.0, should be exactly 1100000",
		},
		{
			name:                 "0.000001 SECOND - 1 microsecond",
			intervalValue:        0.000001,
			intervalUnit:         "SECOND",
			expectedMicroseconds: 1, // math.Round(0.000001 * 1000000) = 1
			description:          "0.000001 * 1000000 = 1.0, should be exactly 1",
		},
		{
			name:                 "1.999999 SECOND - near 2 seconds",
			intervalValue:        1.999999,
			intervalUnit:         "SECOND",
			expectedMicroseconds: 1999999, // math.Round(1.999999 * 1000000) = 1999999
			description:          "1.999999 * 1000000 = 1999999.0, should be exactly 1999999",
		},
		{
			name:                 "1.5 MINUTE",
			intervalValue:        1.5,
			intervalUnit:         "MINUTE",
			expectedMicroseconds: 90000000, // math.Round(1.5 * 60 * 1000000) = 90000000
			description:          "1.5 * 60 * 1000000 = 90000000.0, should be exactly 90000000",
		},
		{
			name:                 "0.5 HOUR",
			intervalValue:        0.5,
			intervalUnit:         "HOUR",
			expectedMicroseconds: 1800000000, // math.Round(0.5 * 3600 * 1000000) = 1800000000
			description:          "0.5 * 3600 * 1000000 = 1800000000.0, should be exactly 1800000000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Verify the expected calculation
			var expectedFloat float64
			switch tc.intervalUnit {
			case "SECOND":
				expectedFloat = tc.intervalValue * float64(types.MicroSecsPerSec)
			case "MINUTE":
				expectedFloat = tc.intervalValue * float64(types.MicroSecsPerSec*types.SecsPerMinute)
			case "HOUR":
				expectedFloat = tc.intervalValue * float64(types.MicroSecsPerSec*types.SecsPerHour)
			case "DAY":
				expectedFloat = tc.intervalValue * float64(types.MicroSecsPerSec*types.SecsPerDay)
			default:
				t.Fatalf("Unsupported interval unit: %s", tc.intervalUnit)
			}

			// Verify math.Round gives the expected result
			roundedValue := int64(math.Round(expectedFloat))
			require.Equal(t, tc.expectedMicroseconds, roundedValue,
				"math.Round should produce expected value: %s", tc.description)

			// Test the actual resetDateFunctionArgs function
			dateExpr := makeDatetimeConst("2000-01-01 01:00:00")
			intervalValueExpr := makeFloat64Const(tc.intervalValue)
			intervalExpr := makeIntervalExpr(intervalValueExpr, tc.intervalUnit)

			args, err := resetDateFunctionArgs(ctx, dateExpr, intervalExpr)
			require.NoError(t, err)
			require.Len(t, args, 3)

			// Verify the interval value matches expected (after rounding)
			intervalValue := extractInt64Value(args[1])
			require.Equal(t, tc.expectedMicroseconds, intervalValue,
				"resetDateFunctionArgs should use math.Round to convert %f %s to %d microseconds: %s",
				tc.intervalValue, tc.intervalUnit, tc.expectedMicroseconds, tc.description)

			// Verify the interval type is MicroSecond
			intervalType := extractInt64Value(args[2])
			require.Equal(t, int64(types.MicroSecond), intervalType,
				"Interval type should be MicroSecond after conversion")
		})
	}
}

// TestResetDateFunctionArgsDecimalIntervalRoundingEdgeCases tests edge cases for rounding
func TestResetDateFunctionArgsDecimalIntervalRoundingEdgeCases(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name                 string
		intervalValue        float64
		intervalUnit         string
		expectedMicroseconds int64
		description          string
	}{
		{
			name:                 "0.9999995 SECOND - exactly halfway",
			intervalValue:        0.9999995,
			intervalUnit:         "SECOND",
			expectedMicroseconds: 1000000, // math.Round(0.9999995 * 1000000) = 1000000 (rounds to even)
			description:          "0.9999995 * 1000000 = 999999.5, should round to 1000000",
		},
		{
			name:                 "0.9999994 SECOND - just below halfway",
			intervalValue:        0.9999994,
			intervalUnit:         "SECOND",
			expectedMicroseconds: 999999, // math.Round(0.9999994 * 1000000) = 999999
			description:          "0.9999994 * 1000000 = 999999.4, should round to 999999",
		},
		{
			name:                 "0.0000005 SECOND - very small value",
			intervalValue:        0.0000005,
			intervalUnit:         "SECOND",
			expectedMicroseconds: 1, // math.Round(0.0000005 * 1000000) = 1 (rounds up from 0.5)
			description:          "0.0000005 * 1000000 = 0.5, should round to 1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Verify the expected calculation
			var expectedFloat float64
			switch tc.intervalUnit {
			case "SECOND":
				expectedFloat = tc.intervalValue * float64(types.MicroSecsPerSec)
			default:
				t.Fatalf("Unsupported interval unit: %s", tc.intervalUnit)
			}

			// Verify math.Round gives the expected result
			roundedValue := int64(math.Round(expectedFloat))
			require.Equal(t, tc.expectedMicroseconds, roundedValue,
				"math.Round should produce expected value: %s", tc.description)

			// Test the actual resetDateFunctionArgs function
			dateExpr := makeDatetimeConst("2000-01-01 01:00:00")
			intervalValueExpr := makeFloat64Const(tc.intervalValue)
			intervalExpr := makeIntervalExpr(intervalValueExpr, tc.intervalUnit)

			args, err := resetDateFunctionArgs(ctx, dateExpr, intervalExpr)
			require.NoError(t, err)
			require.Len(t, args, 3)

			// Verify the interval value matches expected (after rounding)
			intervalValue := extractInt64Value(args[1])
			require.Equal(t, tc.expectedMicroseconds, intervalValue,
				"resetDateFunctionArgs should use math.Round to convert %f %s to %d microseconds: %s",
				tc.intervalValue, tc.intervalUnit, tc.expectedMicroseconds, tc.description)
		})
	}
}
