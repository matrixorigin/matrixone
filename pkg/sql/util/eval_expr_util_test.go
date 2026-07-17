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
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

func TestSetInsertValueStringBinaryHexPadding(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name        string
		literal     string
		typ         types.Type
		expected    []byte
		expectError bool
	}{
		{
			name:     "binary pads to declared width",
			literal:  "0x4142",
			typ:      types.New(types.T_binary, 4, 0),
			expected: []byte{0x41, 0x42, 0x00, 0x00},
		},
		{
			name:     "varbinary keeps decoded width",
			literal:  "0x4142",
			typ:      types.New(types.T_varbinary, 4, 0),
			expected: []byte{0x41, 0x42},
		},
		{
			name:        "binary rejects decoded value over declared width",
			literal:     "0x4142",
			typ:         types.New(types.T_binary, 1, 0),
			expectError: true,
		},
		{
			name:        "varbinary rejects decoded value over declared width",
			literal:     "0x4142",
			typ:         types.New(types.T_varbinary, 1, 0),
			expectError: true,
		},
		{
			name:        "varbinary counts decoded bytes instead of runes",
			literal:     "0xC3A9",
			typ:         types.New(types.T_varbinary, 1, 0),
			expectError: true,
		},
		{
			name:     "binary accepts odd digit hex literal",
			literal:  "0x1",
			typ:      types.New(types.T_binary, 2, 0),
			expected: []byte{0x01, 0x00},
		},
		{
			name:        "invalid hex is rejected",
			literal:     "0xgg",
			typ:         types.New(types.T_binary, 4, 0),
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			numVal := tree.NewNumVal(tc.literal, tc.literal, false, tree.P_hexnum)
			canInsert, got, err := SetInsertValueString(proc, numVal, &tc.typ)
			if tc.expectError {
				require.Error(t, err)
				require.False(t, canInsert)
				return
			}
			require.NoError(t, err)
			require.True(t, canInsert)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestSetInsertValueStringBinaryStringKeepsLegacyWidthBehavior(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_binary, 2, 0)
	numVal := tree.NewNumVal("时", "时", false, tree.P_char)

	canInsert, got, err := SetInsertValueString(proc, numVal, &typ)
	require.NoError(t, err)
	require.True(t, canInsert)
	require.Equal(t, []byte("时"), got)
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
			name:        "zero_timestamp",
			timezone:    time.UTC,
			input:       "0000-00-00 00:00:00",
			shouldError: false,
			description: "the dedicated zero timestamp sentinel bypasses the normal minimum range check",
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
				if tc.input == "0000-00-00 00:00:00" {
					require.Equal(t, types.ZeroTimestamp, res, tc.description)
				} else {
					require.GreaterOrEqual(t, res, types.TimestampMinValue, tc.description)
				}
			}
		})
	}
}

func TestSetInsertValueRejectsZeroTemporalInStrictNoZeroDateMode(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		require.Equal(t, "sql_mode", name)
		require.True(t, isSystemVar)
		require.False(t, isGlobalVar)
		return "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE", nil
	})

	dateType := types.T_date.ToType()
	datetimeType := types.T_datetime.ToType()
	timestampType := types.T_timestamp.ToType()
	for _, tc := range []struct {
		name string
		call func() (bool, error)
	}{
		{
			name: "date",
			call: func() (bool, error) {
				canInsert, _, _, err := SetInsertValueDate(proc, tree.NewNumVal("0000-00-00", "0000-00-00", false, tree.P_char), &dateType)
				return canInsert, err
			},
		},
		{
			name: "datetime",
			call: func() (bool, error) {
				canInsert, _, _, err := SetInsertValueDateTime(proc, tree.NewNumVal("0000-00-00 00:00:00", "0000-00-00 00:00:00", false, tree.P_char), &datetimeType)
				return canInsert, err
			},
		},
		{
			name: "timestamp",
			call: func() (bool, error) {
				canInsert, _, _, err := SetInsertValueTimeStamp(proc, tree.NewNumVal("0000-00-00 00:00:00", "0000-00-00 00:00:00", false, tree.P_char), &timestampType)
				return canInsert, err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			canInsert, err := tc.call()
			require.False(t, canInsert)
			require.Error(t, err)
		})
	}
}

func TestRejectZeroTemporalWritePolicy(t *testing.T) {
	proc := testutil.NewProcess(t)
	cases := []struct {
		name    string
		resolve func(string, bool, bool) (any, error)
		ignore  bool
		want    bool
		wantErr string
	}{
		{
			name: "strict no zero date rejects",
			resolve: func(name string, isSystemVar, isGlobalVar bool) (any, error) {
				require.Equal(t, "sql_mode", name)
				require.True(t, isSystemVar)
				require.False(t, isGlobalVar)
				return "STRICT_TRANS_TABLES,NO_ZERO_DATE", nil
			},
			want: true,
		},
		{
			name: "ignore disables rejection",
			resolve: func(string, bool, bool) (any, error) {
				return "STRICT_ALL_TABLES,NO_ZERO_DATE", nil
			},
			ignore: true,
			want:   false,
		},
		{
			name: "non strict does not reject",
			resolve: func(string, bool, bool) (any, error) {
				return "NO_ZERO_DATE", nil
			},
			want: false,
		},
		{
			name:    "nil resolver is conservative false",
			resolve: nil,
			want:    false,
		},
		{
			name: "non string mode is conservative false",
			resolve: func(string, bool, bool) (any, error) {
				return 1, nil
			},
			want: false,
		},
		{
			name: "resolver error is returned",
			resolve: func(string, bool, bool) (any, error) {
				return nil, errors.New("boom")
			},
			wantErr: "boom",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proc.SetResolveVariableFunc(tc.resolve)
			proc.SetStmtProfile(&process.StmtProfile{})
			proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", tc.ignore)

			got, err := RejectZeroTemporalWritePolicy(proc)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
