// Copyright 2026 Matrix Origin
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

package function

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIncompleteCalendarClassification(t *testing.T) {
	for _, s := range []string{"2024-00-00", "2024-01-00", "2024-00-15", "20240000", "20240000123456.123456", " 2024/00/15 12:34:56 ", "2024-0-0T12:34", "0000-00-15", "2024/0/15", "24-00-15", "2024-00-00T12:34:56.123456"} {
		t.Run(s, func(t *testing.T) { _, ok := parseIncompleteCalendar(s); require.True(t, ok) })
	}
	for _, s := range []string{"0000-00-00", "0000-01-01", "2024-02-29", "2024-02-30", "2024-13-00", "2024-00-32", "2024-00-00junk", "2024-00-00 24:00:00", "2024-00-00 12:60:00", "2024-00-00 12:34:56.bad", "20240000000000junk", "", "garbage",
		"2024-02-29 12:34:56", " 2024-02-29T12:34:56.123456 ", "2024-13-01", "2024-01-32", "2024-01-01 24:00:00", "2024-01-01junk", "202x-01-01", "2024-aa-00", "2024-00-aa", "2024-01-", "2024-00-"} {
		t.Run(s, func(t *testing.T) { _, ok := parseIncompleteCalendar(s); require.False(t, ok) })
	}
}

func TestIncompleteDateFormatFields(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &numericWarningSession{}
	proc.Session = session
	for _, tc := range []struct {
		input, format, want string
		null                bool
	}{
		{"2024-00-00", "%Y-%m-%d", "2024-00-00", false},
		{"2024-01-00", "%Y/%m/%d %M %b", "2024/01/00 January Jan", false},
		{"2024-00-15", "%y|%c|%e|%D|%%Y|%q|末尾%", "24|0|15|15th|%Y|q|末尾%", false},
		{"20240000123456.123456", "%Y-%m-%d %T.%f %r", "2024-00-00 12:34:56.123456 12:34:56 PM", false},
		{"2024-00-00", "%M", "", true},
		{"2024-00-00", "%b", "", true},
		{"0000-00-00", "%Y-%m-%d", "0000-00-00", false},
		{"2024-02-29 01:02:03.123456", "%Y-%m-%d %T.%f", "2024-02-29 01:02:03.123456", false},
		{"", "%Y", "", true},
	} {
		t.Run(tc.input+tc.format, func(t *testing.T) {
			c := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.input}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{tc.format}, nil),
			}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, []bool{tc.null}), DateFormat)
			ok, info := c.RunAndFree()
			require.True(t, ok, info)
		})
	}
	require.Empty(t, session.warnings)
	proc.GetSessionInfo().LCTimeNames = "fr_FR"
	c := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"2024-02-00", "2024-00-00", "2024-00-00", "bad"}, []bool{false, false, true, false}),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"%M", "%W", "%Y", "%Y"}, nil),
	}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"février", "jeudi", "", ""}, []bool{false, false, true, true}), DateFormat).
		WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}})
	ok, info := c.RunAndFree()
	require.True(t, ok, info)
}

// Expected calendar-derived fields were obtained by compiling the unmodified
// calc_daynr/calc_week/calc_weekday functions from MySQL mysql-8.0.45 my_time.cc,
// then applying make_date_time's directive/padding rules. This is a source
// oracle, independent of MatrixOne's normalized DATE arithmetic.
func TestIncompleteDateFormatCalendarDirectives(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct{ input, want string }{
		{"2024-00-00", "-31|4|Thursday|Thu|613566752|613566753|613566752|01|2024|2025"},
		{"2024-01-00", "000|0|Sunday|Sun|00|52|53|52|2023|2023"},
		{"2024-00-15", "-16|5|Friday|Fri|613566754|613566755|613566754|01|2024|2025"},
		{"2024-00-30", "0-1|6|Saturday|Sat|613566756|613566757|613566756|01|2024|2025"},
		{"2024-02-00", "031|3|Wednesday|Wed|04|05|04|05|2024|2024"},
		{"0001-00-00", "-31|4|Thursday|Thu|613566752|613566753|613566752|01|0001|0002"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			c := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.input}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%j|%w|%W|%a|%U|%u|%V|%v|%X|%x"}, nil),
			}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, nil), DateFormat)
			ok, info := c.RunAndFree()
			require.True(t, ok, info)
		})
	}
}

func TestIncompleteCalendarFunctionWarnings(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fn    executeLogicOfOverload
		typ   types.T
		want  any
		extra []FunctionTestInput
	}{
		{"to_days", stringToDays, types.T_int64, []int64{0, 739310, 0, 0}, nil},
		{"to_seconds", stringToSeconds, types.T_int64, []int64{0, 63876384000, 0, 0}, nil},
		{"dayofweek", stringDayOfWeek, types.T_int64, []int64{0, 5, 0, 0}, nil},
		{"week", stringWeek, types.T_int32, []int32{0, 8, 0, 0}, []FunctionTestInput{NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, nil)}},
		{"last_day", LastDay, types.T_date, []types.Date{0, types.DateFromCalendar(2024, 2, 29), 0, 0}, nil},
		{"date_add", DateStringAdd, types.T_varchar, []string{"", "2024-03-01", "", ""}, []FunctionTestInput{NewFunctionTestConstInput(types.T_int64.ToType(), []int64{1}, nil), NewFunctionTestConstInput(types.T_int64.ToType(), []int64{int64(types.Day)}, nil)}},
		{"date_sub", DateStringSub, types.T_varchar, []string{"", "2024-02-28", "", ""}, []FunctionTestInput{NewFunctionTestConstInput(types.T_int64.ToType(), []int64{1}, nil), NewFunctionTestConstInput(types.T_int64.ToType(), []int64{int64(types.Day)}, nil)}},
		{"datediff", stringDateDiff, types.T_int64, []int64{0, 59, 0, 0}, []FunctionTestInput{NewFunctionTestConstInput(types.T_date.ToType(), []types.Date{types.DateFromCalendar(2024, 1, 1)}, nil)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			session := &numericWarningSession{}
			proc.Session = session
			inputs := append([]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"2024-00-00", "2024-02-29", "2024-00-00", "2024-00-00"}, []bool{false, false, true, false})}, tc.extra...)
			c := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(tc.typ.ToType(), false, tc.want, []bool{true, false, true, true}), tc.fn).
				WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}})
			ok, info := c.RunAndFree()
			require.True(t, ok, info)
			require.Equal(t, []numericWarning{{code: moerr.ER_TRUNCATED_WRONG_VALUE, msg: "Incorrect datetime value: '2024-00-00'"}}, session.warnings)
		})
	}
	appendIncompleteCalendarWarning(nil, "2024-00-00")
	appendIncompleteCalendarWarning(testutil.NewProcess(t), "2024-00-00")
}

func TestIncompleteDateDiffOperandDiagnostics(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &numericWarningSession{}
	proc.Session = session
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"2024-00-00", "2024-00-00", "2024-01-01", "2024-00-00"}, []bool{false, false, false, true}),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"2024-01-00", "2024-01-01", "2024-00-15", "2024-00-00"}, []bool{false, true, false, false}),
	}
	c := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 0, 0, 0}, []bool{true, true, true, true}), stringDateDiff)
	ok, info := c.RunAndFree()
	require.True(t, ok, info)
	require.Len(t, session.warnings, 3)
	for _, w := range session.warnings {
		require.Equal(t, moerr.ER_TRUNCATED_WRONG_VALUE, w.code)
	}
}

func TestIncompleteCalendarOverloadSelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, source := range []types.T{types.T_varchar, types.T_char, types.T_text, types.T_any} {
		for _, tc := range []struct {
			name  string
			fid   int
			id    int
			extra []types.Type
		}{
			{"date_format", DATE_FORMAT, 2, []types.Type{types.T_varchar.ToType()}},
			{"to_days", TO_DAYS, 1, nil}, {"to_seconds", TO_SECONDS, 1, nil},
			{"dayofweek", DAYOFWEEK, 3, nil}, {"week", WEEK, 4, nil},
			{"week", WEEK, 5, []types.Type{types.T_int64.ToType()}},
			{"datediff", DATEDIFF, 2, []types.Type{types.T_date.ToType()}},
		} {
			fn, err := GetFunctionByName(proc.Ctx, tc.name, append([]types.Type{source.ToType()}, tc.extra...))
			require.NoError(t, err)
			require.Equal(t, EncodeOverloadID(int32(tc.fid), int32(tc.id)), fn.GetEncodedOverloadID(), "%s %s", source, tc.name)
		}
	}
	for _, typ := range []types.T{types.T_date, types.T_datetime, types.T_timestamp} {
		fn, err := GetFunctionByName(proc.Ctx, "datediff", []types.Type{typ.ToType(), types.T_varchar.ToType()})
		require.NoError(t, err)
		require.Equal(t, EncodeOverloadID(DATEDIFF, 3), fn.GetEncodedOverloadID())
		cast, ok := fn.ShouldDoImplicitTypeCast()
		if typ != types.T_date {
			require.True(t, ok)
			require.Equal(t, types.T_date, cast[0].Oid)
		}
	}
	for _, tc := range []struct {
		fid  int32
		id   int32
		args []types.T
	}{
		{DATE_FORMAT, 0, []types.T{types.T_datetime, types.T_varchar}},
		{TO_DAYS, 0, []types.T{types.T_datetime}}, {TO_SECONDS, 0, []types.T{types.T_datetime}},
		{DAYOFWEEK, 0, []types.T{types.T_date}}, {WEEK, 0, []types.T{types.T_date}},
		{DATEDIFF, 0, []types.T{types.T_date, types.T_date}},
	} {
		fn, err := GetFunctionById(proc.Ctx, EncodeOverloadID(tc.fid, tc.id))
		require.NoError(t, err)
		require.Equal(t, tc.args, fn.args)
	}
}

// LAST_DAY's valid-month/zero-day value discrepancy predates this closure.
// Preserve its diagnostic policy: MySQL emits no warning for that input.
func TestIncompleteLastDayZeroDayDoesNotWarn(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &numericWarningSession{}
	proc.Session = session
	c := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"2024-02-00"}, nil)}, NewFunctionTestResult(types.T_date.ToType(), false, []types.Date{0}, []bool{true}), LastDay)
	ok, info := c.RunAndFree()
	require.True(t, ok, info)
	require.Empty(t, session.warnings)
}

func TestIncompleteDateFormatKeepsConversionPolicy(t *testing.T) {
	for _, text := range []string{"123456", "19981321000000", "19981033000000", "0000-02-28", "0000-01-01", "2024-02-30", "2024-13-00", "2024-00-32", "bad", "2024-00-00 24:00:00"} {
		t.Run(text, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			session := &numericWarningSession{}
			proc.Session = session
			c := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{text}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%Y-%m-%d"}, nil),
			}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}), DateFormat)
			ok, info := c.RunAndFree()
			require.True(t, ok, info)
			require.Empty(t, session.warnings)
		})
	}
	proc := testutil.NewProcess(t)
	session := &numericWarningSession{}
	proc.Session = session
	for _, dynamic := range []bool{false, true} {
		format := NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%Y-%m-%d"}, nil)
		if dynamic {
			format = NewFunctionTestInput(types.T_varchar.ToType(), []string{"%Y-%m-%d", "%Y-%m-%d", "%Y-%m-%d", "%Y-%m-%d", "%Y-%m-%d"}, nil)
		}
		c := NewFunctionTestCase(proc, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"2026-02-30", "2024-02-29", "2024-00-00", "", "invalid skipped row"}, []bool{false, false, false, true, false}), format,
		}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"", "2024-02-29", "2024-00-00", "", ""}, []bool{true, false, false, true, true}), DateFormat).
			WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, true, false}})
		ok, info := c.RunAndFree()
		require.True(t, ok, info)
		require.Empty(t, session.warnings)
	}

	for _, tc := range []struct {
		mode, text, want string
		null             bool
	}{
		{"STRICT_TRANS_TABLES,NO_ZERO_DATE", "0000-00-00", "", true},
		{"", "2024-02-29 01:02:03.1234567", "2024-02-29 01:02:03.123457", false},
		{"TIME_TRUNCATE_FRACTIONAL", "2024-02-29 01:02:03.1234567", "2024-02-29 01:02:03.123456", false},
	} {
		t.Run(tc.mode+tc.text, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
				if name == "sql_mode" {
					return tc.mode, nil
				}
				return nil, nil
			})
			c := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.text}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%Y-%m-%d %T.%f"}, nil),
			}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, []bool{tc.null}), DateFormat)
			ok, info := c.RunAndFree()
			require.True(t, ok, info)
		})
	}
}

func TestIncompleteTemporalLegacyBinding(t *testing.T) {
	require.False(t, LegacyIncompleteTemporalStrings(nil))
	text := types.T_varchar.ToType()
	for _, tc := range []struct {
		name     string
		inputs   []types.Type
		released int32
	}{
		{"date_format", []types.Type{text, text}, 2},
		{"datediff", []types.Type{text, text}, 1},
		{"to_days", []types.Type{text}, 1},
		{"to_seconds", []types.Type{text}, 1},
		{"dayofweek", []types.Type{text}, 3},
		{"week", []types.Type{text, types.T_int64.ToType()}, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current, err := GetFunctionByName(context.Background(), tc.name, tc.inputs)
			require.NoError(t, err)
			require.GreaterOrEqual(t, current.overloadId, tc.released)
			legacy, err := GetFunctionByName(WithLegacyIncompleteTemporalStrings(context.Background()), tc.name, tc.inputs)
			require.NoError(t, err)
			require.Less(t, legacy.overloadId, tc.released)
			require.True(t, legacy.needCast, "released registry must retain its typed input conversion")
		})
	}
}
