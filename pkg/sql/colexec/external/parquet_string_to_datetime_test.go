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
	"bytes"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// TestParquet_StringToDate tests STRING → DATE conversion
func TestParquet_StringToDate(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name      string
		strValues []string
		expected  []types.Date
	}{
		{
			name:      "Standard date format",
			strValues: []string{"2024-01-01", "2024-12-31", "1970-01-01"},
			expected: []types.Date{
				mustParseDate(t, "2024-01-01"),
				mustParseDate(t, "2024-12-31"),
				mustParseDate(t, "1970-01-01"),
			},
		},
		{
			name:      "With whitespace",
			strValues: []string{" 2024-01-01 ", "2024-12-31", " 1970-01-01"},
			expected: []types.Date{
				mustParseDate(t, "2024-01-01"),
				mustParseDate(t, "2024-12-31"),
				mustParseDate(t, "1970-01-01"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)

			for _, s := range tc.strValues {
				row := parquet.Row{parquet.ByteArrayValue([]byte(s))}
				_, err := w.WriteRows([]parquet.Row{row})
				require.NoError(t, err)
			}
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(types.T_date, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{
				Id:          int32(types.T_date),
				NotNullable: true,
			})
			require.NotNil(t, mp, "STRING → DATE should be supported")
			require.NoError(t, mp.mapping(page, proc, vec))

			got := vector.MustFixedColWithTypeCheck[types.Date](vec)
			require.Equal(t, len(tc.expected), len(got))
			for i := range tc.expected {
				require.Equal(t, tc.expected[i], got[i], "Value at index %d", i)
			}
		})
	}
}

// TestParquet_StringToTime tests STRING → TIME conversion
func TestParquet_StringToTime(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name      string
		strValues []string
		scale     int32
		expected  []types.Time
	}{
		{
			name:      "Standard time format",
			strValues: []string{"12:30:45", "00:00:00", "23:59:59"},
			scale:     0,
			expected: []types.Time{
				mustParseTime(t, "12:30:45", 0),
				mustParseTime(t, "00:00:00", 0),
				mustParseTime(t, "23:59:59", 0),
			},
		},
		{
			name:      "With microseconds",
			strValues: []string{"12:30:45.123456", "00:00:00.000001"},
			scale:     6,
			expected: []types.Time{
				mustParseTime(t, "12:30:45.123456", 6),
				mustParseTime(t, "00:00:00.000001", 6),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)

			for _, s := range tc.strValues {
				row := parquet.Row{parquet.ByteArrayValue([]byte(s))}
				_, err := w.WriteRows([]parquet.Row{row})
				require.NoError(t, err)
			}
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(types.T_time, 0, tc.scale))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{
				Id:          int32(types.T_time),
				Scale:       tc.scale,
				NotNullable: true,
			})
			require.NotNil(t, mp, "STRING → TIME should be supported")
			require.NoError(t, mp.mapping(page, proc, vec))

			got := vector.MustFixedColWithTypeCheck[types.Time](vec)
			require.Equal(t, len(tc.expected), len(got))
			for i := range tc.expected {
				require.Equal(t, tc.expected[i], got[i], "Value at index %d", i)
			}
		})
	}
}

// TestParquet_StringToTimestamp tests STRING → TIMESTAMP conversion
func TestParquet_StringToTimestamp(t *testing.T) {
	proc := testutil.NewProc(t)
	// Set timezone to UTC for consistent testing
	proc.Base.SessionInfo.TimeZone = time.UTC

	tests := []struct {
		name      string
		strValues []string
		scale     int32
		expected  []types.Timestamp
	}{
		{
			name:      "Standard datetime format",
			strValues: []string{"2024-01-01 12:30:45", "2024-12-31 23:59:59", "1970-01-01 00:00:00"},
			scale:     0,
			expected: []types.Timestamp{
				mustParseTimestamp(t, time.UTC, "2024-01-01 12:30:45", 0),
				mustParseTimestamp(t, time.UTC, "2024-12-31 23:59:59", 0),
				mustParseTimestamp(t, time.UTC, "1970-01-01 00:00:00", 0),
			},
		},
		{
			name:      "With microseconds",
			strValues: []string{"2024-01-01 12:30:45.123456", "2024-12-31 23:59:59.999999"},
			scale:     6,
			expected: []types.Timestamp{
				mustParseTimestamp(t, time.UTC, "2024-01-01 12:30:45.123456", 6),
				mustParseTimestamp(t, time.UTC, "2024-12-31 23:59:59.999999", 6),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)

			for _, s := range tc.strValues {
				row := parquet.Row{parquet.ByteArrayValue([]byte(s))}
				_, err := w.WriteRows([]parquet.Row{row})
				require.NoError(t, err)
			}
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(types.T_timestamp, 0, tc.scale))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{
				Id:          int32(types.T_timestamp),
				Scale:       tc.scale,
				NotNullable: true,
			})
			require.NotNil(t, mp, "STRING → TIMESTAMP should be supported")
			require.NoError(t, mp.mapping(page, proc, vec))

			got := vector.MustFixedColWithTypeCheck[types.Timestamp](vec)
			require.Equal(t, len(tc.expected), len(got))
			for i := range tc.expected {
				require.Equal(t, tc.expected[i], got[i], "Value at index %d", i)
			}
		})
	}
}

// Helper functions
func mustParseDate(t *testing.T, s string) types.Date {
	result, err := types.ParseDateCast(s)
	require.NoError(t, err, "Failed to parse date: %s", s)
	return result
}

func mustParseTime(t *testing.T, s string, scale int32) types.Time {
	result, err := types.ParseTime(s, scale)
	require.NoError(t, err, "Failed to parse time: %s", s)
	return result
}

func mustParseTimestamp(t *testing.T, loc *time.Location, s string, scale int32) types.Timestamp {
	result, err := types.ParseTimestamp(loc, s, scale)
	require.NoError(t, err, "Failed to parse timestamp: %s", s)
	return result
}

// TestParquet_StringToDate_InvalidFormats tests error handling for DATE
func TestParquet_StringToDate_InvalidFormats(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name     string
		strValue string
	}{
		// DATE: Empty string and whitespace-only strings should fail
		{name: "Empty string", strValue: ""},
		{name: "Only spaces", strValue: "   "},
		// Invalid date values
		{name: "Invalid month", strValue: "2024-13-01"},
		{name: "Invalid day", strValue: "2024-02-30"},
		{name: "Letters", strValue: "abc"},
		{name: "Wrong format slash", strValue: "2024/01/01"},
		{name: "Invalid year", strValue: "99999-01-01"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)
			row := parquet.Row{parquet.ByteArrayValue([]byte(tc.strValue))}
			_, err := w.WriteRows([]parquet.Row{row})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(types.T_date, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{
				Id:          int32(types.T_date),
				NotNullable: true,
			})
			require.NotNil(t, mp)

			// Should return error
			err = mp.mapping(page, proc, vec)
			require.Error(t, err, "Expected error for invalid date: %s", tc.strValue)
		})
	}
}

// TestParquet_StringToTime_InvalidFormats tests error handling for TIME
func TestParquet_StringToTime_InvalidFormats(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name     string
		strValue string
	}{
		// TIME: Empty string and whitespace convert to 00:00:00 (MatrixOne design, consistent with MySQL)
		// So we don't test those scenarios
		// Only test truly invalid time values
		{name: "Invalid minute", strValue: "12:60:00"},
		{name: "Invalid second", strValue: "12:30:60"},
		{name: "Letters", strValue: "abc"},
		{name: "Completely invalid format", strValue: "not-a-time"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)
			row := parquet.Row{parquet.ByteArrayValue([]byte(tc.strValue))}
			_, err := w.WriteRows([]parquet.Row{row})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(types.T_time, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{
				Id:          int32(types.T_time),
				NotNullable: true,
			})
			require.NotNil(t, mp)

			// Should return error
			err = mp.mapping(page, proc, vec)
			require.Error(t, err, "Expected error for invalid time: %s", tc.strValue)
		})
	}
}

// TestParquet_StringToTimestamp_InvalidFormats tests error handling for TIMESTAMP
func TestParquet_StringToTimestamp_InvalidFormats(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.Base.SessionInfo.TimeZone = time.UTC

	tests := []struct {
		name     string
		strValue string
	}{
		// TIMESTAMP: Empty string and whitespace-only strings should fail
		{name: "Empty string", strValue: ""},
		{name: "Only spaces", strValue: "   "},
		// Date-only "2024-01-01" converts to "2024-01-01 00:00:00" (MatrixOne design, consistent with mainstream DBs)
		// So we don't test "Missing time" scenario
		// Only test truly invalid values
		{name: "Invalid date part", strValue: "2024-13-45 12:30:45"},
		{name: "Letters", strValue: "abc"},
		{name: "Completely invalid format", strValue: "not-a-timestamp"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)
			row := parquet.Row{parquet.ByteArrayValue([]byte(tc.strValue))}
			_, err := w.WriteRows([]parquet.Row{row})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{
				Id:          int32(types.T_timestamp),
				NotNullable: true,
			})
			require.NotNil(t, mp)

			// Should return error
			err = mp.mapping(page, proc, vec)
			require.Error(t, err, "Expected error for invalid timestamp: %s", tc.strValue)
		})
	}
}
