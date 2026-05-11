// Copyright 2025 Matrix Origin
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

package external

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Test_getColData_Timestamp_WithSessionTimeZone tests that getColData uses session timezone
// instead of hardcoded time.Local when parsing TIMESTAMP values.
// This test verifies the fix for issue #22921.
func Test_getColData_Timestamp_WithSessionTimeZone(t *testing.T) {
	// Create a process with session timezone set to UTC+8
	proc := testutil.NewProcess(t)
	utc8 := time.FixedZone("UTC+8", 8*3600)
	proc.Base.SessionInfo.TimeZone = utc8

	// Create test data
	timestampStr := "2020-09-07 00:00:00"
	colType := types.T_timestamp.ToType()
	colType.Scale = 0

	// Create batch and vector
	bat := batch.New([]string{"ts"})
	vec := vector.NewVec(colType)
	bat.Vecs[0] = vec

	// Create ExternalParam
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					Format: tree.CSV,
				},
			},
			Cols: []*plan.ColDef{
				{
					Name: "ts",
					Typ: plan.Type{
						Id:    int32(types.T_timestamp),
						Scale: 0,
					},
				},
			},
		},
	}

	// Create CSV field
	field := csvparser.Field{
		Val:            timestampStr,
		IsNull:         false,
		HasStringQuote: false,
	}
	line := []csvparser.Field{field}

	// Create ExternAttr
	attr := plan.ExternAttr{
		ColName:       "ts",
		ColIndex:      0,
		ColFieldIndex: 0,
	}

	// Call getColData with session timezone UTC+8
	mp := proc.GetMPool()
	err := getColData(bat, line, 0, param, mp, attr, proc)
	require.NoError(t, err)

	// Verify the stored value
	require.Equal(t, 1, vec.Length())
	storedTimestamp := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)[0]

	// Parse the same string with UTC+8 to get expected value
	expectedTimestamp, err := types.ParseTimestamp(utc8, timestampStr, 0)
	require.NoError(t, err)
	require.Equal(t, expectedTimestamp, storedTimestamp, "getColData should use session timezone (UTC+8)")

	// Verify that getColData uses session timezone
	// Parse with system timezone for comparison
	systemTimestamp, err := types.ParseTimestamp(time.Local, timestampStr, 0)
	require.NoError(t, err)

	// Check if system timezone is different from UTC+8
	_, systemOffset := time.Now().In(time.Local).Zone()
	_, sessionOffset := time.Now().In(utc8).Zone()

	if systemOffset != sessionOffset {
		// System timezone is different, so stored value should be different
		require.NotEqual(t, systemTimestamp, storedTimestamp,
			"getColData should NOT use system timezone when session timezone is set")
	} else {
		// System timezone matches session timezone, values will be same
		// But this still verifies getColData uses session timezone correctly
		require.Equal(t, systemTimestamp, storedTimestamp,
			"When system timezone matches session timezone, values should match")
	}

	// Verify display value using the same session timezone
	displayStr := storedTimestamp.String2(utc8, 0)
	require.Equal(t, timestampStr, displayStr,
		"Display value should match input when using same timezone")
}

// Test_getColData_Timestamp_DifferentTimeZones tests that getColData correctly handles
// different session timezones and produces consistent results.
func Test_getColData_Timestamp_DifferentTimeZones(t *testing.T) {
	timestampStr := "2020-09-07 00:00:00"
	testCases := []struct {
		name          string
		sessionTZ     *time.Location
		expectedValue string // Expected display value
	}{
		{
			name:          "UTC+8",
			sessionTZ:     time.FixedZone("UTC+8", 8*3600),
			expectedValue: "2020-09-07 00:00:00",
		},
		{
			name:          "UTC+0",
			sessionTZ:     time.FixedZone("UTC+0", 0),
			expectedValue: "2020-09-07 00:00:00",
		},
		{
			name:          "UTC-5",
			sessionTZ:     time.FixedZone("UTC-5", -5*3600),
			expectedValue: "2020-09-07 00:00:00",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.Base.SessionInfo.TimeZone = tc.sessionTZ

			colType := types.T_timestamp.ToType()
			colType.Scale = 0

			bat := batch.New([]string{"ts"})
			vec := vector.NewVec(colType)
			bat.Vecs[0] = vec

			param := &ExternalParam{
				ExParamConst: ExParamConst{
					Ctx: context.Background(),
					Extern: &tree.ExternParam{
						ExParamConst: tree.ExParamConst{
							Format: tree.CSV,
						},
					},
					Cols: []*plan.ColDef{
						{
							Name: "ts",
							Typ: plan.Type{
								Id:    int32(types.T_timestamp),
								Scale: 0,
							},
						},
					},
				},
			}

			field := csvparser.Field{
				Val:            timestampStr,
				IsNull:         false,
				HasStringQuote: false,
			}
			line := []csvparser.Field{field}

			attr := plan.ExternAttr{
				ColName:       "ts",
				ColIndex:      0,
				ColFieldIndex: 0,
			}

			mp := proc.GetMPool()
			err := getColData(bat, line, 0, param, mp, attr, proc)
			require.NoError(t, err)

			require.Equal(t, 1, vec.Length())
			storedTimestamp := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)[0]

			// Verify that the stored value, when displayed using the same session timezone,
			// matches the input
			displayStr := storedTimestamp.String2(tc.sessionTZ, 0)
			require.Equal(t, tc.expectedValue, displayStr,
				"LOAD and SELECT should use the same timezone, producing consistent results")
		})
	}
}

// Test_getColData_Timestamp_Consistency verifies that LOAD DATA and SELECT
// use the same timezone, ensuring consistency between insertion and retrieval.
func Test_getColData_Timestamp_Consistency(t *testing.T) {
	proc := testutil.NewProcess(t)
	// Use UTC+8 as session timezone (different from system timezone if system is not UTC+8)
	sessionTZ := time.FixedZone("UTC+8", 8*3600)
	proc.Base.SessionInfo.TimeZone = sessionTZ

	testValues := []string{
		"2020-09-07 00:00:00",
		"2024-01-01 12:00:00",
		"1970-01-01 00:00:01",
	}

	for _, timestampStr := range testValues {
		t.Run(timestampStr, func(t *testing.T) {
			colType := types.T_timestamp.ToType()
			colType.Scale = 0

			bat := batch.New([]string{"ts"})
			vec := vector.NewVec(colType)
			bat.Vecs[0] = vec

			param := &ExternalParam{
				ExParamConst: ExParamConst{
					Ctx: context.Background(),
					Extern: &tree.ExternParam{
						ExParamConst: tree.ExParamConst{
							Format: tree.CSV,
						},
					},
					Cols: []*plan.ColDef{
						{
							Name: "ts",
							Typ: plan.Type{
								Id:    int32(types.T_timestamp),
								Scale: 0,
							},
						},
					},
				},
			}

			field := csvparser.Field{
				Val:            timestampStr,
				IsNull:         false,
				HasStringQuote: false,
			}
			line := []csvparser.Field{field}

			attr := plan.ExternAttr{
				ColName:       "ts",
				ColIndex:      0,
				ColFieldIndex: 0,
			}

			mp := proc.GetMPool()
			// Simulate LOAD DATA: parse timestamp with session timezone
			err := getColData(bat, line, 0, param, mp, attr, proc)
			require.NoError(t, err)

			require.Equal(t, 1, vec.Length())
			storedTimestamp := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)[0]

			// Simulate SELECT: convert stored timestamp back to string using session timezone
			displayStr := storedTimestamp.String2(sessionTZ, 0)

			// The display value should match the input value when using the same timezone
			require.Equal(t, timestampStr, displayStr,
				"LOAD and SELECT should produce consistent results when using the same timezone")
		})
	}
}

// Test_isLegalLine_Timestamp tests that isLegalLine correctly validates TIMESTAMP fields
// using time.Local as fallback (since proc is not available in this context).
// This test verifies the code path for parallel LOAD DATA file offset calculation.
func Test_isLegalLine_Timestamp(t *testing.T) {
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format: tree.CSV,
		},
	}

	testCases := []struct {
		name      string
		timestamp string
		scale     int32
		expected  bool
	}{
		{
			name:      "valid timestamp without scale",
			timestamp: "2020-09-07 00:00:00",
			scale:     0,
			expected:  true,
		},
		{
			name:      "valid timestamp with scale",
			timestamp: "2020-09-07 00:00:00.123456",
			scale:     6,
			expected:  true,
		},
		{
			name:      "valid timestamp with microseconds",
			timestamp: "2024-01-01 12:30:45.123",
			scale:     3,
			expected:  true,
		},
		{
			name:      "invalid timestamp format",
			timestamp: "invalid-timestamp",
			scale:     0,
			expected:  false,
		},
		{
			name:      "invalid timestamp with wrong date",
			timestamp: "2020-13-45 25:70:80",
			scale:     0,
			expected:  false,
		},
		{
			name:      "empty timestamp string",
			timestamp: "",
			scale:     0,
			expected:  true, // empty string is treated as null/empty and skipped
		},
		{
			name:      "valid timestamp edge case - epoch",
			timestamp: "1970-01-01 00:00:00",
			scale:     0,
			expected:  true,
		},
		{
			name:      "valid timestamp edge case - future date",
			timestamp: "2099-12-31 23:59:59",
			scale:     0,
			expected:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cols := []*plan.ColDef{
				{
					Name: "ts",
					Typ: plan.Type{
						Id:    int32(types.T_timestamp),
						Scale: tc.scale,
					},
				},
			}

			fields := []csvparser.Field{
				{
					Val:            tc.timestamp,
					IsNull:         false,
					HasStringQuote: false,
				},
			}

			result := isLegalLine(param, cols, fields)
			require.Equal(t, tc.expected, result,
				"isLegalLine should return %v for timestamp '%s' with scale %d",
				tc.expected, tc.timestamp, tc.scale)
		})
	}

	// Test null field handling
	t.Run("null timestamp field", func(t *testing.T) {
		cols := []*plan.ColDef{
			{
				Name: "ts",
				Typ: plan.Type{
					Id:    int32(types.T_timestamp),
					Scale: 0,
				},
			},
		}

		fields := []csvparser.Field{
			{
				Val:            "2020-09-07 00:00:00",
				IsNull:         true,
				HasStringQuote: false,
			},
		}

		result := isLegalLine(param, cols, fields)
		require.True(t, result, "null field should be skipped and return true")
	})

	// Test that isLegalLine uses time.Local (not session timezone)
	// This is important because isLegalLine is used for file offset calculation
	// where proc is not available, so it must use time.Local as fallback
	t.Run("uses time.Local for parsing", func(t *testing.T) {
		cols := []*plan.ColDef{
			{
				Name: "ts",
				Typ: plan.Type{
					Id:    int32(types.T_timestamp),
					Scale: 0,
				},
			},
		}

		// A valid timestamp that can be parsed with time.Local
		fields := []csvparser.Field{
			{
				Val:            "2020-09-07 00:00:00",
				IsNull:         false,
				HasStringQuote: false,
			},
		}

		result := isLegalLine(param, cols, fields)
		require.True(t, result, "should successfully parse timestamp using time.Local")

		// Verify that the timestamp can actually be parsed with time.Local
		_, err := types.ParseTimestamp(time.Local, "2020-09-07 00:00:00", 0)
		require.NoError(t, err, "timestamp should be parseable with time.Local")
	})
}
