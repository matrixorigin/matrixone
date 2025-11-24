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

package util

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestHexToInt(t *testing.T) {
	var val uint64
	var err error

	val, err = HexToInt("0x1")
	require.NoError(t, err)
	require.Equal(t, uint64(1), val)

	val, err = HexToInt("0x123456789abcdef")
	require.NoError(t, err)
	require.Equal(t, uint64(81985529216486895), val)

	_, err = HexToInt("0xg")
	require.Error(t, err)
}

func TestBinaryToInt(t *testing.T) {
	var val uint64
	var err error

	val, err = BinaryToInt("0x1")
	require.NoError(t, err)
	require.Equal(t, uint64(1), val)

	val, err = BinaryToInt("0b0101010101001")
	require.NoError(t, err)
	require.Equal(t, uint64(2729), val)

	_, err = BinaryToInt("0x2")
	require.Error(t, err)
}

func TestScoreBinaryToInt(t *testing.T) {
	var val uint64
	var err error

	val, err = ScoreBinaryToInt("1")
	require.NoError(t, err)
	require.Equal(t, uint64(49), val)

	val, err = ScoreBinaryToInt("1234")
	require.NoError(t, err)
	require.Equal(t, uint64(825373492), val)

	_, err = ScoreBinaryToInt("123456789")
	require.Error(t, err)

	val, err = ScoreBinaryToInt("阿斯")
	require.NoError(t, err)
	require.Equal(t, uint64(256842263860911), val)
}

func TestSetInsertValueTimeStamp_MinValueValidation(t *testing.T) {
	// Test that timestamp minimum value validation works correctly across different timezones
	// MySQL behavior: TIMESTAMP valid range is always UTC [1970-01-01 00:00:01, 2038-01-19 03:14:07]
	// The input local time is converted to UTC using current time_zone, then checked against the range.
	// Timezones further west (smaller UTC offset) allow earlier local dates.

	typ := types.T_timestamp.ToType()
	typ.Scale = 6

	testCases := []struct {
		name        string
		timezone    *time.Location
		input       string
		shouldError bool
		description string
	}{
		{
			name:        "UTC+8_before_min",
			timezone:    time.FixedZone("UTC+8", 8*3600),
			input:       "1969-12-31 23:59:59",
			shouldError: true,
			description: "UTC+8: 1969-12-31 23:59:59 -> UTC 1969-12-31 15:59:59 (before min, should error)",
		},
		{
			name:        "UTC_before_min",
			timezone:    time.UTC,
			input:       "1969-12-31 23:59:59",
			shouldError: true,
			description: "UTC: 1969-12-31 23:59:59 -> UTC 1969-12-31 23:59:59 (before min, should error)",
		},
		{
			name:        "UTC-8_after_min",
			timezone:    time.FixedZone("UTC-8", -8*3600),
			input:       "1969-12-31 23:59:59",
			shouldError: false,
			description: "UTC-8: 1969-12-31 23:59:59 -> UTC 1970-01-01 07:59:59 (after min, should pass)",
		},
		{
			name:        "UTC+8_at_min",
			timezone:    time.FixedZone("UTC+8", 8*3600),
			input:       "1970-01-01 00:00:01",
			shouldError: true,
			description: "UTC+8: 1970-01-01 00:00:01 -> UTC 1969-12-31 16:00:01 (before min, should error)",
		},
		{
			name:        "UTC_at_min",
			timezone:    time.UTC,
			input:       "1970-01-01 00:00:01",
			shouldError: false,
			description: "UTC: 1970-01-01 00:00:01 -> UTC 1970-01-01 00:00:01 (at min, should pass)",
		},
		{
			name:        "UTC-8_at_min",
			timezone:    time.FixedZone("UTC-8", -8*3600),
			input:       "1969-12-31 16:00:01",
			shouldError: false,
			description: "UTC-8: 1969-12-31 16:00:01 -> UTC 1970-01-01 00:00:01 (at min, should pass)",
		},
		{
			name:        "UTC+8_valid",
			timezone:    time.FixedZone("UTC+8", 8*3600),
			input:       "2024-01-01 12:00:00",
			shouldError: false,
			description: "UTC+8: 2024-01-01 12:00:00 -> UTC 2024-01-01 04:00:00 (valid, should pass)",
		},
		{
			name:        "UTC_valid",
			timezone:    time.UTC,
			input:       "2024-01-01 12:00:00",
			shouldError: false,
			description: "UTC: 2024-01-01 12:00:00 -> UTC 2024-01-01 12:00:00 (valid, should pass)",
		},
		{
			name:        "UTC-8_valid",
			timezone:    time.FixedZone("UTC-8", -8*3600),
			input:       "2024-01-01 12:00:00",
			shouldError: false,
			description: "UTC-8: 2024-01-01 12:00:00 -> UTC 2024-01-01 20:00:00 (valid, should pass)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.Base.SessionInfo.TimeZone = tc.timezone

			numVal := tree.NewNumVal(tc.input, tc.input, false, tree.P_char)
			canInsert, isnull, res, err := SetInsertValueTimeStamp(proc, numVal, &typ)

			if tc.shouldError {
				require.False(t, canInsert, tc.description)
				require.Error(t, err, tc.description)
			} else {
				require.True(t, canInsert, tc.description)
				require.NoError(t, err, tc.description)
				require.False(t, isnull, tc.description)
				require.GreaterOrEqual(t, res, types.TimestampMinValue, tc.description)
			}
		})
	}
}
