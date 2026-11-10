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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// The omitted-length CAST/CONVERT-to-CHAR target is the unbounded TEXT type
// (#32336, #32374). Every consumer function below already had a VARCHAR
// overload, and its overload list leads with a non-string overload: the TEXT
// type casts implicitly to VARCHAR/UINT64/DATE/DATETIME/TIMESTAMP at the same
// preferLevel, so the by-cast tie kept the FIRST (non-string) overload and
// changed the composition's semantics. These tests pin the restored contract:
// the TEXT results bind the string overloads DIRECTLY (no inserted implicit
// narrowing cast) and keep executing the string parsing paths.

// TestCeilFloorTextKeepStringNumericParsing covers the CEIL/FLOOR regression:
// with the TEXT target tied into CEIL's UINT64 overload, strconv.ParseUint
// rejected decimal and negative strings like '1.2' and '-1.2'.
func TestCeilFloorTextKeepStringNumericParsing(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		name   string
		inputs []string
		expect []float64
		op     fEvalFn
	}{
		{
			name:   "ceil",
			inputs: []string{"1.2", "-1.2", "2.5", "2"},
			expect: []float64{2, -1, 3, 2},
			op:     CeilStr,
		},
		{
			name:   "floor",
			inputs: []string{"1.2", "-1.2", "2.5", "2"},
			expect: []float64{1, -2, 2, 2},
			op:     FloorStr,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The TEXT overload must bind directly: a by-cast match would
			// restore the UINT64 (or other first-overload) selection.
			get, err := GetFunctionByName(proc.Ctx, tc.name, []types.Type{types.T_text.ToType()})
			require.NoError(t, err)
			require.Equal(t, types.T_float64, get.GetReturnType().Oid)
			_, shouldCast := get.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast,
				"%s(text) must bind the string overload directly, not by a narrowing cast", tc.name)

			nulls := make([]bool, len(tc.inputs))
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_text.ToType(), tc.inputs, nulls)},
				NewFunctionTestResult(types.T_float64.ToType(), false, tc.expect, nulls),
				tc.op)
			s, info := fcTC.Run()
			require.True(t, s, fmt.Sprintf("case is '%s text', err info is '%s'", tc.name, info))
		})
	}
}

// TestExtractFromTextKeepsStringParsingPath covers the EXTRACT regression:
// with the TEXT target tied into EXTRACT's first DATETIME overload, the
// short colon string '10:11:12' was read as the colon date 2010-11-12 00:00:00
// and every time-field extracted zero. The string path keeps trying TIME
// first for time-preferring units and still reads colon dates for date units.
func TestExtractFromTextKeepsStringParsingPath(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		unit   string
		input  string
		expect string
	}{
		{unit: "minute", input: "10:11:12", expect: "11"},
		{unit: "hour_minute", input: "10:11:12", expect: "1011"},
		{unit: "hour_second", input: "10:11:12", expect: "101112"},
		{unit: "minute_second", input: "10:11:12", expect: "1112"},
		{unit: "year", input: "10:11:12", expect: "2010"},
	} {
		t.Run(tc.unit+"/"+tc.input, func(t *testing.T) {
			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{tc.unit}, nil),
					NewFunctionTestInput(types.T_text.ToType(), []string{tc.input}, nil),
				},
				NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.expect}, nil),
				ExtractFromVarchar,
			)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}

	get, err := GetFunctionByName(proc.Ctx, "extract", []types.Type{types.T_varchar.ToType(), types.T_text.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_varchar, get.GetReturnType().Oid)
	_, shouldCast := get.ShouldDoImplicitTypeCast()
	require.False(t, shouldCast,
		"extract(varchar, text) must bind the string overload directly, not tie into the DATETIME overload")
}

// TestTimestampAddStringTextKeepsTimeOfDay covers the TIMESTAMPADD
// regression: with the TEXT target tied into TIMESTAMPADD's first DATE
// overload, the time-of-day was discarded before the interval was applied and
// TIMESTAMPADD(HOUR, 1, ...) '2024-01-01 12:34:56' produced 01:00:00 instead
// of 13:34:56.
func TestTimestampAddStringTextKeepsTimeOfDay(t *testing.T) {
	proc := testutil.NewProcess(t)

	get, err := GetFunctionByName(proc.Ctx, "timestampadd",
		[]types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_text.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_varchar, get.GetReturnType().Oid)
	_, shouldCast := get.ShouldDoImplicitTypeCast()
	require.False(t, shouldCast,
		"timestampadd(varchar, int64, text) must bind the string overload directly, not tie into the DATE overload")

	for _, tc := range []struct {
		unit   string
		input  string
		expect string
	}{
		{unit: "HOUR", input: "2024-01-01 12:34:56", expect: "2024-01-01 13:34:56"},
		{unit: "MINUTE", input: "2024-01-01 12:34:56", expect: "2024-01-01 12:35:56"},
		{unit: "DAY", input: "2024-01-01", expect: "2024-01-02"},
	} {
		t.Run(tc.unit+"/"+tc.input, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{tc.unit}, nil),
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{1}, nil),
					NewFunctionTestInput(types.T_text.ToType(), []string{tc.input}, nil),
				},
				NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.expect}, nil),
				TimestampAddString)
			s, info := fcTC.Run()
			require.True(t, s, fmt.Sprintf("case is 'timestampadd text %s', err info is '%s'", tc.unit, info))
		})
	}
}

// TestTimestampTextKeepsTimeOfDay covers the one-argument TIMESTAMP
// regression: with the TEXT target tied into TIMESTAMP's first DATE overload,
// TIMESTAMP('2024-01-01 12:34:56') returned midnight instead of preserving
// the datetime string's time-of-day.
func TestTimestampTextKeepsTimeOfDay(t *testing.T) {
	proc := testutil.NewProcess(t)

	get, err := GetFunctionByName(proc.Ctx, "timestamp", []types.Type{types.T_text.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_timestamp, get.GetReturnType().Oid)
	_, shouldCast := get.ShouldDoImplicitTypeCast()
	require.False(t, shouldCast,
		"timestamp(text) must bind the string overload directly, not tie into the DATE overload")

	input := "2024-01-01 12:34:56"
	expect, err := types.ParseTimestamp(proc.GetSessionInfo().TimeZone, input, 6)
	require.NoError(t, err)

	fcTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_text.ToType(), []string{input}, []bool{false})},
		NewFunctionTestResult(types.T_timestamp.ToType(), false, []types.Timestamp{expect}, []bool{false}),
		DateStringToTimestamp)
	s, info := fcTC.Run()
	require.True(t, s, fmt.Sprintf("case is 'timestamp text keeps time-of-day', err info is '%s'", info))
}

// TestTimestampDiffTextKeepsStringTemporalOverloads guards the mixed
// DATE/TEXT TIMESTAMPDIFF compositions raised by PR review: converting the
// omitted-length CAST/CONVERT-to-CHAR TEXT target (#32336, #32374) to DATE,
// DATETIME, TIMESTAMP or VARCHAR all costs the same, so fixedTypeMatch's
// by-cast tie kept the first DATE/DATETIME overloads and the inserted
// strToDate cast discarded the string operand's time-of-day - a 49-hour
// DATE/string pair silently returned 48 hours. Mirror overloads bind these
// T_text operands directly to the string parsing executors, keeping the
// values the pre-widening VARCHAR compositions produced.
func TestTimestampDiffTextKeepsStringTemporalOverloads(t *testing.T) {
	proc := testutil.NewProcess(t)

	// The three compositions carrying a widened TEXT operand must resolve to
	// an exact TEXT overload; a by-cast resolution means a narrowing cast was
	// inserted and the equal-cost tie regression is back.
	for _, tc := range []struct {
		name string
		args []types.T
	}{
		{"text_pair", []types.T{types.T_varchar, types.T_text, types.T_text}},
		{"date_then_text", []types.T{types.T_varchar, types.T_date, types.T_text}},
		{"text_then_date", []types.T{types.T_varchar, types.T_text, types.T_date}},
	} {
		t.Run("binds/"+tc.name, func(t *testing.T) {
			args := make([]types.Type, 0, len(tc.args))
			for _, a := range tc.args {
				args = append(args, a.ToType())
			}
			get, err := GetFunctionByName(proc.Ctx, "timestampdiff", args)
			require.NoError(t, err)
			require.Equal(t, types.T_int64, get.GetReturnType().Oid)
			_, shouldCast := get.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast,
				"timestampdiff %s must bind the string temporal overload directly, not tie into the DATE overload through an equal-cost cast", tc.name)
		})
	}

	// TIMESTAMP-peered mixes have no direct overload on either spelling, so
	// TEXT and VARCHAR must keep selecting the SAME overload through the
	// implicit-cast costs. A divergent cost table would silently narrow one
	// spelling away from the other.
	for _, tc := range []struct {
		name string
		text [2]types.T
		peer [2]types.T
	}{
		{"timestamp_then_text", [2]types.T{types.T_timestamp, types.T_text}, [2]types.T{types.T_timestamp, types.T_varchar}},
		{"text_then_timestamp", [2]types.T{types.T_text, types.T_timestamp}, [2]types.T{types.T_varchar, types.T_timestamp}},
	} {
		t.Run("peer_selection/"+tc.name, func(t *testing.T) {
			textSel, err := GetFunctionByName(proc.Ctx, "timestampdiff",
				[]types.Type{types.T_varchar.ToType(), tc.text[0].ToType(), tc.text[1].ToType()})
			require.NoError(t, err)
			peerSel, err := GetFunctionByName(proc.Ctx, "timestampdiff",
				[]types.Type{types.T_varchar.ToType(), tc.peer[0].ToType(), tc.peer[1].ToType()})
			require.NoError(t, err)
			require.Equal(t, peerSel.GetEncodedOverloadID(), textSel.GetEncodedOverloadID(),
				"the widened TEXT operand must resolve timestampdiff exactly like its VARCHAR peer")
		})
	}

	// Executor-level parity with T_text operands: the mirror executors keep
	// the hour counts their pre-widening VARCHAR peers produced, including
	// the ones that depend on the string's preserved time-of-day.
	t.Run("values", func(t *testing.T) {
		dateOdd, err := types.ParseDateCast("2024-01-01")
		require.NoError(t, err)
		dateEven, err := types.ParseDateCast("2024-01-02")
		require.NoError(t, err)
		dateFifth, err := types.ParseDateCast("2024-01-05")
		require.NoError(t, err)

		fcTC := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"hour"}, nil),
				NewFunctionTestInput(types.T_text.ToType(),
					[]string{"2024-01-01 00:00:00", "2024-01-01"}, nil),
				NewFunctionTestInput(types.T_text.ToType(),
					[]string{"2024-01-03 01:00:00", "2024-01-03"}, nil),
			},
			NewFunctionTestResult(types.T_int64.ToType(), false, []int64{49, 48}, nil),
			TimestampDiffString)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is 'timestampdiff text pair keeps time-of-day', err info is '%s'", info))

		fcTC = NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"hour"}, nil),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{dateOdd, dateEven}, nil),
				NewFunctionTestInput(types.T_text.ToType(),
					[]string{"2024-01-03 01:00:00", "2024-01-03 00:30:00"}, nil),
			},
			NewFunctionTestResult(types.T_int64.ToType(), false, []int64{49, 24}, nil),
			TimestampDiffDateString)
		s, info = fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is 'timestampdiff date/text keeps time-of-day', err info is '%s'", info))

		fcTC = NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"hour"}, nil),
				NewFunctionTestInput(types.T_text.ToType(),
					[]string{"2024-01-03 01:00:00", "2024-01-03 00:00:00"}, nil),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{dateFifth, dateFifth}, nil),
			},
			NewFunctionTestResult(types.T_int64.ToType(), false, []int64{47, 48}, nil),
			TimestampDiffStringDate)
		s, info = fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is 'timestampdiff text/date keeps time-of-day', err info is '%s'", info))
	})
}

// TestDateStringExtractorsBindTextDirectly covers the date-extraction
// regressions raised by PR review: DAYOFMONTH, QUARTER, DAYNAME, MONTHNAME
// and WEEKOFYEAR each keep a string parsing overload that preserves
// year-zero and incomplete-date components (0000-01-01 keeps January and
// day 1, 2024-00-15 keeps day 15), while their DATE overloads consume a
// strToDate cast that rejects those strings and NULLs them. The widened TEXT
// target (#32336, #32374) casts to DATE and VARCHAR at the same preferLevel,
// so the by-cast tie kept each first DATE overload; direct TEXT overloads
// restore the pre-widening string semantics.
func TestDateStringExtractorsBindTextDirectly(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		name string
		ret  types.T
	}{
		{"dayofmonth", types.T_uint8},
		{"quarter", types.T_uint8},
		{"dayname", types.T_varchar},
		{"monthname", types.T_varchar},
		{"weekofyear", types.T_int64},
	} {
		t.Run("binds/"+tc.name, func(t *testing.T) {
			get, err := GetFunctionByName(proc.Ctx, tc.name, []types.Type{types.T_text.ToType()})
			require.NoError(t, err)
			require.Equal(t, tc.ret, get.GetReturnType().Oid,
				"the TEXT overload must keep its VARCHAR peer's return type")
			_, shouldCast := get.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast,
				"%s(text) must bind the string overload directly, not tie into the DATE overload through an equal-cost cast", tc.name)
		})
	}

	// Executor-level parity with T_text operands: the values are the ones the
	// VARCHAR overloads already pin in TestDateStringExtractorsYearZeroAnd-
	// LegacyDelimiters (func_unary_test.go), including year-zero and legacy
	// delimiter strings. A tie into the DATE overload would NULL every row
	// that strToDate cannot parse; of the composed edge strings, only the
	// name/week extractors NULL the incomplete date 2024-00-15, while
	// DAYOFMONTH keeps its day component and QUARTER reports 0.
	inputs := []string{"0000-01-01", "2024/01/15", "2024:01:15T12:34:56", "0000-02-29", "2024-00-15"}
	for _, tc := range []struct {
		name   string
		result FunctionTestResult
		op     fEvalFn
	}{
		{
			name:   "dayofmonth",
			result: NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{1, 15, 15, 0, 15}, []bool{false, false, false, true, false}),
			op:     DateStringToDay,
		},
		{
			name:   "quarter",
			result: NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{1, 1, 1, 0, 0}, []bool{false, false, false, true, false}),
			op:     DateStringToQuarter,
		},
		{
			name:   "dayname",
			result: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"Sunday", "Monday", "Monday", "", ""}, []bool{false, false, false, true, true}),
			op:     DateStringToDayName,
		},
		{
			name:   "monthname",
			result: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"January", "January", "January", "", ""}, []bool{false, false, false, true, true}),
			op:     DateStringToMonthName,
		},
		{
			name:   "weekofyear",
			result: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{52, 3, 3, 0, 0}, []bool{false, false, false, true, true}),
			op:     DateStringToWeekOfYear,
		},
	} {
		t.Run("values/"+tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_text.ToType(), inputs, nil)},
				tc.result, tc.op)
			s, info := fcTC.Run()
			require.True(t, s, fmt.Sprintf("case is '%s keeps the string parsing values on TEXT', err info is '%s'", tc.name, info))
		})
	}
}
