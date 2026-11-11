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

// TestCeilFloorTextKeepStringNumericParsing covers the one-argument CEIL and
// FLOOR TEXT overloads end to end: decimal, negative and exponent strings
// keep the pre-widening string-parser values the widening restored (review
// round: '1.2' and '-1.2' parse again instead of the UINT64 cast rejecting
// them), integer strings keep the exact INT64 and UINT64 values the
// pre-widening UINT64 tie produced (review round: the FLOAT64 parser
// silently rounded 9007199254740993 down to 9007199254740992 and constricted
// the accepted unsigned domain), every numeric token the pre-widening
// VARCHAR composition accepted through its FLOAT64 string parser keeps
// succeeding with the VARCHAR route's shortest-digit display (mo-bot P2
// review: CEIL(CAST('1e40' AS CHAR)) and FLOOR(CONVERT('18446744073709551616',
// CHAR)) raised DECIMAL128 conversion and UINT64 range errors), and NULL
// rows keep NULL results instead of failing the whole query (review round:
// FLOOR over a mixed nullable TEXT batch appended the NULL result and still
// parsed the NULL row's empty value through strconv.ParseFloat). The exact
// integer domains and every wide float fallback pack into the scale-0
// DECIMAL256(76,0) return — the widest integral numeric the type system
// provides — whose rendering equals the prior FLOAT64 displays digit for
// digit for every token within 76 digits.
func TestCeilFloorTextKeepStringNumericParsing(t *testing.T) {
	proc := testutil.NewProcess(t)

	parseDecs := func(vals []string) []types.Decimal256 {
		out := make([]types.Decimal256, len(vals))
		for i, v := range vals {
			d, err := types.ParseDecimal256(v, 76, 0)
			require.NoError(t, err, "test fixture %q must parse", v)
			out[i] = d
		}
		return out
	}

	// The one-argument TEXT overload must bind directly: a by-cast match
	// would restore the UINT64 (or other first-overload) selection, whose
	// parser rejects decimal and negative strings. The return type must be
	// the widest scale-0 numeric so the pre-widening composition domain
	// survives alongside exact integer handling.
	for _, name := range []string{"ceil", "floor"} {
		get, err := GetFunctionByName(proc.Ctx, name, []types.Type{types.T_text.ToType()})
		require.NoError(t, err)
		require.Equal(t, types.T_decimal256, get.GetReturnType().Oid,
			"%s(text) must keep the wide-domain exact return", name)
		_, shouldCast := get.ShouldDoImplicitTypeCast()
		require.False(t, shouldCast,
			"%s(text) must bind the direct TEXT overload, not by a narrowing cast", name)
	}

	// Row-level checks of the registered one-argument TEXT executors: each
	// row batch mixes decimal strings, the exact-integer controls that used
	// to round through float64, the pre-widening UINT64 tie's upper range,
	// base-prefixed tokens, whitespace and a NULL row, plus the wide
	// magnitudes the mo-bot P2 review restored: every expected row is the
	// output the prior production binary measured for the same token, so
	// integer rows keep the exact values and every other row keeps the
	// shortest-digit FLOAT64 display the VARCHAR route used.
	for _, tc := range []struct {
		name    string
		op      fEvalFn
		inputs  []string
		nullIn  []bool
		expects []string
	}{
		{
			name: "ceil",
			op:   CeilTextUnary,
			inputs: []string{
				"1.2", "-1.2", "2.5", "2", "9007199254740993",
				"9223372036854775807", "9223372036854775808", "18446744073709551615",
				"-9223372036854775808", " 42 ", "0x10", "1e2", "",
				"1e40", "99999999999999999999999", "18446744073709551617",
				"-1e40", "1.5e40", "1e75",
			},
			nullIn: []bool{false, false, false, false, false, false, false, false,
				false, false, false, false, true,
				false, false, false, false, false, false},
			expects: []string{
				"2", "-1", "3", "2", "9007199254740993",
				"9223372036854775807", "9223372036854775808", "18446744073709551615",
				"-9223372036854775808", "42", "16", "100", "0",
				"10000000000000000000000000000000000000000",
				"100000000000000000000000",
				"18446744073709552000",
				"-10000000000000000000000000000000000000000",
				"15000000000000000000000000000000000000000",
				"1000000000000000000000000000000000000000000000000000000000000000000000000000",
			},
		},
		{
			name: "floor",
			op:   FloorTextUnary,
			inputs: []string{
				"1.2", "-1.2", "2.5", "2", "9007199254740993",
				"9223372036854775807", "9223372036854775808", "18446744073709551615",
				"-9223372036854775808", " 42 ", "0x10", "1e2", "",
				"1e40", "99999999999999999999999", "18446744073709551616",
				"-1e40", "12345678901234567890123456789012345678901", "1e75", "-1e76",
			},
			nullIn: []bool{false, false, false, false, false, false, false, false,
				false, false, false, false, true,
				false, false, false, false, false, false, false},
			expects: []string{
				"1", "-2", "2", "2", "9007199254740993",
				"9223372036854775807", "9223372036854775808", "18446744073709551615",
				"-9223372036854775808", "42", "16", "100", "0",
				"10000000000000000000000000000000000000000",
				"100000000000000000000000",
				"18446744073709552000",
				"-10000000000000000000000000000000000000000",
				"12345678901234568000000000000000000000000",
				"1000000000000000000000000000000000000000000000000000000000000000000000000000",
				// The DECIMAL256FromFloat64 route formats the shortest float
				// digits ('g'), so -1e76 reaches the executor result through
				// the same '-1e+76' notation this fixture must use: the
				// types layer accepts the -10^76 boundary in this form while
				// the same magnitude as a digit literal does not parse.
				"-1e+76",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_text.ToType(), tc.inputs, tc.nullIn)},
				NewFunctionTestResult(types.T_decimal256.ToType(), false,
					parseDecs(tc.expects), tc.nullIn),
				tc.op)
			s, info := fcTC.Run()
			require.True(t, s, fmt.Sprintf("case is '%s text rows', err info is '%s'", tc.name, info))
		})
	}

	// The mixed nullable TEXT batch the review reproduced: FLOOR over
	// ('1'), (NULL) used to fail the whole query through the NULL parsing
	// defect. Every row must keep its own result. A second wide batch mixes
	// the P2 restorations with a NULL row: the NULL row must stay safe at
	// the widened domain too instead of failing the whole query.
	for _, tc := range []struct {
		name    string
		op      fEvalFn
		inputs  []string
		nullIn  []bool
		expects []string
	}{
		{
			name:    "ceil/mixed-null-batch",
			op:      CeilTextUnary,
			inputs:  []string{"1", "", "9007199254740993"},
			nullIn:  []bool{false, true, false},
			expects: []string{"1", "0", "9007199254740993"},
		},
		{
			name:    "floor/mixed-null-batch",
			op:      FloorTextUnary,
			inputs:  []string{"1", "", "9007199254740993"},
			nullIn:  []bool{false, true, false},
			expects: []string{"1", "0", "9007199254740993"},
		},
		{
			name:    "ceil/wide-mixed-null-batch",
			op:      CeilTextUnary,
			inputs:  []string{"1e40", "", "18446744073709551616"},
			nullIn:  []bool{false, true, false},
			expects: []string{"10000000000000000000000000000000000000000", "0", "18446744073709552000"},
		},
		{
			name:    "floor/wide-mixed-null-batch",
			op:      FloorTextUnary,
			inputs:  []string{"1e40", "", "12345678901234567890123456789012345678901"},
			nullIn:  []bool{false, true, false},
			expects: []string{"10000000000000000000000000000000000000000", "0", "12345678901234568000000000000000000000000"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_text.ToType(),
					tc.inputs, tc.nullIn)},
				NewFunctionTestResult(types.T_decimal256.ToType(), false,
					parseDecs(tc.expects), tc.nullIn),
				tc.op)
			s, info := fcTC.Run()
			require.True(t, s, fmt.Sprintf("case is '%s mixed nullable', err info is '%s'", tc.name, info))
		})
	}

	// Rows the numeric domain rejects keep the contract's errors: non-numeric
	// strings keep the string parser's strconv error, and float fallbacks
	// beyond the DECIMAL256(76,0) range keep the decimal conversion error.
	// The scale-0 width domain the types layer accepts is sign-asymmetric —
	// [-10^76, +10^76): positive 1e76 needs the 77th integral digit and
	// rejects, and so does every larger magnitude (1e300); -1.5e76 sits below
	// the negative bound while the IN-domain -1e76 is a success row in the
	// floor table above.
	for _, tc := range []struct {
		name   string
		op     fEvalFn
		inputs []string
	}{
		{"ceil-not-numeric", CeilTextUnary, []string{"中文"}},
		{"floor-not-numeric", FloorTextUnary, []string{"ab"}},
		{"ceil-beyond-decimal256", CeilTextUnary, []string{"1e300"}},
		{"floor-beyond-decimal256", FloorTextUnary, []string{"-1e300"}},
		{"ceil-float-needs-77-digits", CeilTextUnary, []string{"1e76"}},
		{"floor-float-needs-77-digits", FloorTextUnary, []string{"-1.5e76"}},
	} {
		t.Run("reject-"+tc.name, func(t *testing.T) {
			nulls := make([]bool, len(tc.inputs))
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_text.ToType(), tc.inputs, nulls)},
				NewFunctionTestResult(types.T_decimal256.ToType(), true,
					parseDecs([]string{"0"}), nulls),
				tc.op)
			s, info := fcTC.Run()
			require.True(t, s, fmt.Sprintf("case is 'reject %s', err info is '%s'", tc.name, info))
		})
	}

	// The VARCHAR one-argument spellings keep their own string parsers and
	// the FLOAT64 return, and their executors must skip the NULL rows of
	// mixed batches the same way: FloorStr's hand-rolled row loop used to
	// append the NULL result and still parse the NULL row's empty value,
	// failing the whole query.
	for _, tc := range []struct {
		name   string
		op     fEvalFn
		inputs []string
		nullIn []bool
		expect []float64
	}{
		{
			name:   "ceil-varchar",
			op:     CeilStr,
			inputs: []string{"1.2", "-1.2", "2.5", "2"},
			expect: []float64{2, -1, 3, 2},
		},
		{
			name:   "floor-varchar",
			op:     FloorStr,
			inputs: []string{"1.2", "-1.2", "2.5", "2"},
			expect: []float64{1, -2, 2, 2},
		},
		{
			name:   "ceil-varchar-mixed-nulls",
			op:     CeilStr,
			inputs: []string{"1", "", "2.5"},
			nullIn: []bool{false, true, false},
			expect: []float64{1, 0, 3},
		},
		{
			name:   "floor-varchar-mixed-nulls",
			op:     FloorStr,
			inputs: []string{"1", "", "2.5"},
			nullIn: []bool{false, true, false},
			expect: []float64{1, 0, 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nulls := tc.nullIn
			if nulls == nil {
				nulls = make([]bool, len(tc.inputs))
			}
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), tc.inputs, nulls)},
				NewFunctionTestResult(types.T_float64.ToType(), false, tc.expect, nulls),
				tc.op)
			s, info := fcTC.Run()
			require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.name, info))
		})
	}

	// The one-argument VARCHAR overload keeps binding its FLOAT64 string
	// parser directly: widening must not move it onto the TEXT overloads.
	for _, name := range []string{"ceil", "floor"} {
		get, err := GetFunctionByName(proc.Ctx, name,
			[]types.Type{types.T_varchar.ToType()})
		require.NoError(t, err)
		require.Equal(t, types.T_float64, get.GetReturnType().Oid,
			"%s(varchar) must keep the string parser overload", name)
		_, shouldCast := get.ShouldDoImplicitTypeCast()
		require.False(t, shouldCast, "%s(varchar) must bind its own overload directly", name)
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

// TestNumericTextCompositionsKeepSignedInt64Peer covers the regression
// raised by PR review: the omitted-length CAST/CONVERT-to-CHAR result widens
// to TEXT (#32336, #32374), and every TEXT-to-integer conversion then costs
// the same, so fixedTypeMatch's tie kept the FIRST listed overload of BIN,
// ROUND, TRUNCATE, SPACE and the two-argument CEIL/FLOOR, plus unary -/+. The
// inserted TEXT-to-UINT8/UINT64 casts rejected '256', '-1' and '-300' that
// the pre-widening VARCHAR-to-INT64 coercion accepted (SELECT BIN(CAST('256'
// AS CHAR)) failed with a uint8 range error; SQL evidence from the review
// and from the issue-32336 validation service). TEXT shares the CHAR/VARCHAR
// string-number domain again, so the compositions below resolve to exactly
// their VARCHAR peer's INT64 overload.
func TestNumericTextCompositionsKeepSignedInt64Peer(t *testing.T) {
	proc := testutil.NewProcess(t)

	// BIN, SPACE and unary +/- have no direct TEXT overload: their widened
	// TEXT operands must keep resolving their peer's signed INT64 overload
	// through the implicit TEXT-to-INT64 cast.
	for _, tc := range []struct {
		name string
		args []types.T
	}{
		{"bin", []types.T{types.T_text}},
		{"space", []types.T{types.T_text}},
		{"unary_minus", []types.T{types.T_text}},
		{"unary_plus", []types.T{types.T_text}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			textArgs := make([]types.Type, len(tc.args))
			peerArgs := make([]types.Type, len(tc.args))
			hasText := false
			for i, a := range tc.args {
				textArgs[i] = a.ToType()
				if a == types.T_text {
					hasText = true
					peerArgs[i] = types.T_varchar.ToType()
				} else {
					peerArgs[i] = a.ToType()
				}
			}
			require.True(t, hasText, "probe must feed a widened TEXT operand")

			textSel, err := GetFunctionByName(proc.Ctx, tc.name, textArgs)
			require.NoError(t, err)
			peerSel, err := GetFunctionByName(proc.Ctx, tc.name, peerArgs)
			require.NoError(t, err)
			require.Equal(t, peerSel.GetEncodedOverloadID(), textSel.GetEncodedOverloadID(),
				"%s: the widened TEXT operand must resolve its peer's signed INT64 overload, not the first listed unsigned one",
				tc.name)

			castTypes, shouldCast := textSel.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast,
				"%s: the numeric compositions consume TEXT through the pre-widening implicit string-to-INT64 cast", tc.name)
			for i := range textArgs {
				if textArgs[i].Oid != types.T_text {
					continue
				}
				require.Equal(t, types.T_int64, castTypes[i].Oid,
					"%s: the widened TEXT operand must insert the peer's INT64 cast target", tc.name)
			}
		})
	}

	// ROUND, TRUNCATE and the two-argument CEIL/FLOOR bind their direct
	// dual-domain TEXT overloads instead (round-five review): the signed
	// CHAR composition results stay, and the upper unsigned range that
	// existing TEXT inputs previously accepted through the UINT64 overloads
	// is preserved too.
	for _, tc := range []struct {
		name string
		args []types.T
	}{
		{"round", []types.T{types.T_text}},
		{"round", []types.T{types.T_text, types.T_int64}},
		{"truncate", []types.T{types.T_text}},
		{"truncate", []types.T{types.T_text, types.T_int64}},
		{"ceil", []types.T{types.T_text, types.T_int64}},
		{"floor", []types.T{types.T_text, types.T_int64}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			textArgs := make([]types.Type, len(tc.args))
			for i, a := range tc.args {
				textArgs[i] = a.ToType()
			}
			get, err := GetFunctionByName(proc.Ctx, tc.name, textArgs)
			require.NoError(t, err)
			require.Equal(t, types.T_decimal128, get.GetReturnType().Oid,
				"%s(text) must bind its direct dual-domain TEXT overload", tc.name)
			_, shouldCast := get.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast,
				"%s(text) must bind its direct TEXT overload, not an INT64-by-cast match", tc.name)
		})
	}

	// Executor-level sanity of the restored domain: the selected INT64
	// overload accepts the review's rejected values through the same
	// implicit cast the VARCHAR peer used (BIN renders two's complement,
	// so -1 is 64 ones).
	inputs := []int64{256, -1, -300}
	expects := make([]string, len(inputs))
	for i, v := range inputs {
		expects[i] = fmt.Sprintf("%b", uint64(v))
	}
	fcTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_int64.ToType(), inputs, nil)},
		NewFunctionTestResult(types.T_varchar.ToType(), false, expects, nil),
		Bin[int64])
	s, info := fcTC.Run()
	require.True(t, s, fmt.Sprintf("case is 'bin keeps the signed values through INT64', err info is '%s'", info))
}

// TestRoundTruncateCeilFloorTextKeepUnsignedUpperRange covers the round-five
// review regression: the global TEXT->INT64 preference made ROUND, TRUNCATE
// and the two-argument CEIL/FLOOR reject positive magnitudes at or above 2^63
// that existing TEXT inputs previously accepted through the first-listed
// UINT64 overloads (SELECT ROUND(CAST('9223372036854775808' AS TEXT)) and
// SELECT TRUNCATE(CAST('9223372036854775808' AS TEXT), 0) both returned
// 9223372036854775808 on the base). The direct dual-domain TEXT overloads
// parse per row: the signed core keeps the CHAR-composition results the
// earlier review rounds fixed, the unsigned core keeps the upper range
// [2^63, 2^64) with the UINT64 overload's exact math, and both domains pack
// exactly into the scale-0 DECIMAL128 result, whose rendering equals the
// UINT64 and INT64 displays (all expected values below are the outputs
// measured on the isolated base and PR validation services).
func TestRoundTruncateCeilFloorTextKeepUnsignedUpperRange(t *testing.T) {
	proc := testutil.NewProcess(t)

	parseDecs := func(vals []string) []types.Decimal128 {
		out := make([]types.Decimal128, len(vals))
		for i, v := range vals {
			d, err := types.ParseDecimal128(v, 38, 0)
			require.NoError(t, err, "test fixture %q must parse", v)
			out[i] = d
		}
		return out
	}

	// Row-level value checks per overload. digits applies to the
	// two-argument forms (the one-argument rows run with digits 0).
	for _, tc := range []struct {
		name    string
		op      fEvalFn
		digits  int64
		twoArg  bool
		inputs  []string
		expects []string
	}{
		{
			// Upper unsigned boundary plus the signed controls: the
			// unsigned rows match the base binary's UINT64-overload
			// outputs and the signed rows match the current (reviewed and
			// CI-green) INT64-route outputs.
			name: "round-one-arg",
			op:   RoundText,
			inputs: []string{
				"9223372036854775808",
				"18446744073709551615",
				"9223372036854775807",
				"-1",
				"-9223372036854775808",
				" 42 ",
				"0x10",
				"15",
			},
			expects: []string{
				"9223372036854775808",
				"18446744073709551615",
				"9223372036854775807",
				"-1",
				"-9223372036854775808",
				"42",
				"16",
				"15",
			},
		},
		{
			// '...807' is the largest int64 value whose -1 rounding
			// crosses int64's ceiling; it keeps the base binary's
			// UINT64-overload output instead of the INT64 core's
			// out-of-range panic. '...804' stays below half a unit
			// and never crosses.
			name:   "round-digits",
			op:     RoundText,
			twoArg: true,
			digits: -1,
			inputs: []string{
				"9223372036854775808",
				"9223372036854775807",
				"9223372036854775804",
				"-1",
				"-15",
				"25",
			},
			expects: []string{
				"9223372036854775810",
				"9223372036854775810",
				"9223372036854775800",
				"-10",
				"-20",
				"30",
			},
		},
		{
			name: "truncate-one-arg",
			op:   TruncateText,
			inputs: []string{
				"9223372036854775808",
				"18446744073709551615",
				"-1",
			},
			expects: []string{
				"9223372036854775808",
				"18446744073709551615",
				"-1",
			},
		},
		{
			// TRUNCATE only moves positive values toward zero, so the
			// largest int64 value truncates at -2 without crossing and
			// matches the base binary's UINT64-overload output.
			name:   "truncate-digits",
			op:     TruncateText,
			twoArg: true,
			digits: -2,
			inputs: []string{
				"9223372036854775808",
				"9223372036854775807",
				"18446744073709551615",
				"-15",
			},
			expects: []string{
				"9223372036854775800",
				"9223372036854775800",
				"18446744073709551600",
				"0",
			},
		},
		{
			name:   "ceil-two-arg",
			op:     CeilText,
			twoArg: true,
			digits: 0,
			inputs: []string{
				"9223372036854775808",
				"18446744073709551615",
				"-1",
				"15",
			},
			expects: []string{
				"9223372036854775808",
				"18446744073709551615",
				"-1",
				"15",
			},
		},
		{
			// ceilUint64's digit arithmetic overflows at the uint64
			// maximum; the shared core keeps the base's observed output.
			name:   "ceil-digits-overflow-quirk",
			op:     CeilText,
			twoArg: true,
			digits: -1,
			inputs: []string{
				"18446744073709551615",
				"9223372036854775808",
				"-15",
			},
			expects: []string{
				"0",
				"9223372036854775810",
				"-10",
			},
		},
		{
			name:   "floor-two-arg",
			op:     FloorText,
			twoArg: true,
			digits: 0,
			inputs: []string{
				"9223372036854775808",
				"18446744073709551615",
				"-1",
			},
			expects: []string{
				"9223372036854775808",
				"18446744073709551615",
				"-1",
			},
		},
		{
			// FLOOR only moves values down, so '...807' never crosses
			// int64's ceiling and matches the base binary's output.
			name:   "floor-digits",
			op:     FloorText,
			twoArg: true,
			digits: -1,
			inputs: []string{
				"18446744073709551615",
				"9223372036854775808",
				"9223372036854775807",
				"-15",
			},
			expects: []string{
				"18446744073709551610",
				"9223372036854775800",
				"9223372036854775800",
				"-20",
			},
		},
		{
			// The largest int64 value ceilings across int64's
			// ceiling, where ceilInt64 silently wraps; the corridor
			// keeps the base binary's UINT64-overload output instead.
			// '...800' is already a multiple of 10 and stays unchanged.
			name:   "ceil-digits-signed-corridor",
			op:     CeilText,
			twoArg: true,
			digits: -1,
			inputs: []string{
				"9223372036854775807",
				"9223372036854775800",
			},
			expects: []string{
				"9223372036854775810",
				"9223372036854775800",
			},
		},
		{
			// The same corridor one scale coarser.
			name:   "ceil-digits-signed-corridor-coarse",
			op:     CeilText,
			twoArg: true,
			digits: -2,
			inputs: []string{
				"9223372036854775807",
			},
			expects: []string{
				"9223372036854775900",
			},
		},
		{
			// The UINT64 kernels' scale table runs one digit further
			// than the INT64 kernels' (uint64 has 20 digits, int64 has
			// 19), so the base binary kept round's -19 digit row: the
			// values in the upper half of the last int64 unit route
			// through the UINT64 core while the rest keeps the shared
			// zero result (base outputs measured on 4.2-dev).
			name:   "round-digits-table-boundary",
			op:     RoundText,
			twoArg: true,
			digits: -19,
			inputs: []string{
				"9000000000000000000",
				"9223372036854775807",
				"4999999999999999999",
			},
			expects: []string{
				"10000000000000000000",
				"10000000000000000000",
				"0",
			},
		},
		{
			// The same digit-table row for CEIL: any positive value
			// below the 19-digit unit ceilings up to it.
			name:   "ceil-digits-table-boundary",
			op:     CeilText,
			twoArg: true,
			digits: -19,
			inputs: []string{
				"9000000000000000000",
				"4999999999999999999",
			},
			expects: []string{
				"10000000000000000000",
				"10000000000000000000",
			},
		},
		{
			// FLOOR and TRUNCATE collapse to zero at the boundary in
			// both kernels, so they need no routing and keep the base
			// outputs directly.
			name:   "floor-digits-table-boundary",
			op:     FloorText,
			twoArg: true,
			digits: -19,
			inputs: []string{
				"9000000000000000000",
			},
			expects: []string{
				"0",
			},
		},
		{
			name:   "truncate-digits-table-boundary",
			op:     TruncateText,
			twoArg: true,
			digits: -19,
			inputs: []string{
				"9000000000000000000",
			},
			expects: []string{
				"0",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nulls := make([]bool, len(tc.inputs))
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_text.ToType(), tc.inputs, nulls),
			}
			if tc.twoArg {
				digitRows := make([]int64, len(tc.inputs))
				for i := range digitRows {
					digitRows[i] = tc.digits
				}
				inputs = append(inputs,
					NewFunctionTestConstInput(types.T_int64.ToType(), digitRows[:1], nil))
			}
			fcTC := NewFunctionTestCase(proc, inputs,
				NewFunctionTestResult(types.T_decimal128.ToType(), false,
					parseDecs(tc.expects), nulls),
				tc.op)
			s, info := fcTC.Run()
			require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.name, info))
		})
	}

	// Rows both parsers reject keep the INT64 implicit-cast error, so the
	// current head's diagnostics and error expectations stay identical.
	for _, tc := range []struct {
		name   string
		op     fEvalFn
		inputs []string
	}{
		{"round-decimal", RoundText, []string{"2.5"}},
		{"round-not-numeric", RoundText, []string{"中文"}},
		{"round-above-uint64", RoundText, []string{"99999999999999999999999"}},
		{"truncate-decimal", TruncateText, []string{"2.5"}},
		{"ceil-two-arg-decimal", CeilText, []string{"-1.2"}},
		{"floor-two-arg-decimal", FloorText, []string{"-1.2"}},
	} {
		t.Run("reject-"+tc.name, func(t *testing.T) {
			nulls := make([]bool, len(tc.inputs))
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_text.ToType(), tc.inputs, nulls),
			}
			if _, twoArg := map[string]bool{"ceil-two-arg-decimal": true, "floor-two-arg-decimal": true}[tc.name]; twoArg {
				inputs = append(inputs,
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, nil))
			}
			fcTC := NewFunctionTestCase(proc, inputs,
				NewFunctionTestResult(types.T_decimal128.ToType(), true, parseDecs([]string{"0"}), nulls),
				tc.op)
			s, info := fcTC.Run()
			require.True(t, s, fmt.Sprintf("case is 'reject %s', err info is '%s'", tc.name, info))
		})
	}

	// NULL TEXT rows stay NULL, like the implicit-cast route.
	fcTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_text.ToType(),
				[]string{"9223372036854775808", ""}, []bool{false, true}),
			NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, nil),
		},
		NewFunctionTestResult(types.T_decimal128.ToType(), false,
			parseDecs([]string{"9223372036854775808", "0"}), []bool{false, true}),
		RoundText)
	s, info := fcTC.Run()
	require.True(t, s, fmt.Sprintf("case is 'round keeps NULL rows', err info is '%s'", info))
}

// TestRoundFamilyTemporalInputsKeepBindTimeRejection covers the CI follow-up
// (PROXY compose run 42899048729,
// test/distributed/cases/function/func_coalesce.test rows 133/144): the
// direct dual-domain TEXT overloads above accept any source through its
// implicit T_text cast, and DATE/DATETIME/TIME/TIMESTAMP/UUID have a
// preferLevel-2 T_text rule but no numeric rule, so those inputs stopped
// rejecting at bind time and started returning the TEXT executors' runtime
// cast errors: SELECT ROUND(COALESCE(a,a)) FROM t1 over a DATE column
// returned 'invalid argument cast to int, bad value 2000-01-01' instead of
// the pinned 'invalid argument function round, bad value [DATE]'. The
// T_text overload arguments now ignore non-TEXT inputs, so the temporal
// sources keep the base's bind-time rejection while every bare TEXT input
// keeps the direct dual-domain overload, and the string-typed sources keep
// their pre-widening routes (int64 for ROUND/TRUNCATE, the exact dual-domain
// one-argument executors for CEIL/FLOOR).
func TestRoundFamilyTemporalInputsKeepBindTimeRejection(t *testing.T) {
	proc := testutil.NewProcess(t)

	// The sources that reach T_text implicitly and have no numeric path:
	// binding must fail exactly as it did before the TEXT overloads joined
	// (the first two stages are func_coalesce.result's pinned error rows).
	bindRejected := func(name string, args []types.Type, wantStage string) {
		t.Helper()
		_, err := GetFunctionByName(proc.Ctx, name, args)
		require.Error(t, err, "%s(%v) must stay bind-rejected", name, args)
		require.ErrorContains(t, err, wantStage)
	}

	bindRejected("round", []types.Type{types.T_date.ToType()},
		"invalid argument function round, bad value [DATE]")
	bindRejected("round", []types.Type{types.T_datetime.ToType()},
		"invalid argument function round, bad value [DATETIME]")
	for _, tc := range []struct {
		name string
		arg  types.T
	}{
		{"round", types.T_time},
		{"round", types.T_timestamp},
		{"round", types.T_uuid},
		{"truncate", types.T_time},
		{"truncate", types.T_timestamp},
	} {
		bindRejected(tc.name, []types.Type{tc.arg.ToType()},
			fmt.Sprintf("invalid argument function %s, bad value [%s]", tc.name, tc.arg.String()))
	}

	// The digits forms: base had no (temporal, int64) route either, so with
	// the TEXT overloads gone these keep failing to bind as well.
	bindRejected("round", []types.Type{types.T_date.ToType(), types.T_int64.ToType()},
		"invalid argument function round, bad value [DATE")
	bindRejected("round", []types.Type{types.T_datetime.ToType(), types.T_int64.ToType()},
		"invalid argument function round, bad value [DATETIME")
	bindRejected("truncate", []types.Type{types.T_date.ToType(), types.T_int64.ToType()},
		"invalid argument function truncate, bad value [DATE")
	bindRejected("ceil", []types.Type{types.T_datetime.ToType(), types.T_int64.ToType()},
		"invalid argument function ceil, bad value [DATETIME")
	bindRejected("floor", []types.Type{types.T_date.ToType(), types.T_int64.ToType()},
		"invalid argument function floor, bad value [DATE")

	// Counterexamples: the fix must not narrow the intended reach.
	// Bare TEXT inputs keep binding the direct dual-domain TEXT overloads.
	for _, tc := range []struct {
		name string
		args []types.T
	}{
		{"round", []types.T{types.T_text}},
		{"round", []types.T{types.T_text, types.T_int64}},
		{"truncate", []types.T{types.T_text}},
		{"truncate", []types.T{types.T_text, types.T_int64}},
		{"ceil", []types.T{types.T_text, types.T_int64}},
		{"floor", []types.T{types.T_text, types.T_int64}},
	} {
		args := make([]types.Type, len(tc.args))
		for i, a := range tc.args {
			args[i] = a.ToType()
		}
		get, err := GetFunctionByName(proc.Ctx, tc.name, args)
		require.NoError(t, err)
		require.Equal(t, types.T_decimal128, get.GetReturnType().Oid,
			"%s(text...) must keep the direct dual-domain TEXT overload", tc.name)
		_, shouldCast := get.ShouldDoImplicitTypeCast()
		require.False(t, shouldCast, "%s(text...) must still bind directly", tc.name)
	}

	// The one-argument CEIL/FLOOR TEXT overloads keep the dual-domain
	// executors with the exact-integer, wide-domain DECIMAL256 return; the
	// VARCHAR spellings keep the string-parsing FLOAT64 return type.
	for _, name := range []string{"ceil", "floor"} {
		get, err := GetFunctionByName(proc.Ctx, name,
			[]types.Type{types.T_text.ToType()})
		require.NoError(t, err)
		require.Equal(t, types.T_decimal256, get.GetReturnType().Oid,
			"%s(text) must keep the wide-domain exact overload", name)

		get, err = GetFunctionByName(proc.Ctx, name,
			[]types.Type{types.T_varchar.ToType()})
		require.NoError(t, err)
		require.Equal(t, types.T_float64, get.GetReturnType().Oid,
			"%s(varchar) must keep the string parser overload", name)
		_, shouldCast := get.ShouldDoImplicitTypeCast()
		require.False(t, shouldCast, "%s(varchar) must bind its own overload directly", name)
	}

	// ROUND and TRUNCATE of VARCHAR/CHAR keep the pre-widening INT64 route
	// (their preferLevel-1 T_int64 rule): the fix must not move those onto
	// the TEXT overloads.
	for _, name := range []string{"round", "truncate"} {
		for _, s := range []types.T{types.T_varchar, types.T_char} {
			get, err := GetFunctionByName(proc.Ctx, name, []types.Type{s.ToType()})
			require.NoError(t, err)
			require.Equal(t, types.T_int64, get.GetReturnType().Oid,
				"%s(%s) must keep the INT64 route", name, s.String())
			_, shouldCast := get.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast,
				"%s(%s) keeps binding the INT64 overload by implicit cast", name, s.String())
		}
	}
}

// TestDateTextKeepsStringParser covers the regression raised by PR review:
// DATE(T#32336-widened TEXT) tied into DATE's first DATE overload, and the
// inserted strToDate cast rejected the compact yyyymmddhhmmss spelling
// ('20240102123456'), NULLing what the pre-widening VARCHAR composition
// parsed as 2024-01-02 (SQL evidence: SELECT DATE(CONVERT('20240102123456',
// CHAR)) returned NULL against the review's claim of 2024-01-02). The direct
// TEXT overload binds the string parser again, for both spellings.
func TestDateTextKeepsStringParser(t *testing.T) {
	proc := testutil.NewProcess(t)

	get, err := GetFunctionByName(proc.Ctx, "date", []types.Type{types.T_text.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_date, get.GetReturnType().Oid,
		"the TEXT overload must keep its VARCHAR peer's return type")
	_, shouldCast := get.ShouldDoImplicitTypeCast()
	require.False(t, shouldCast,
		"date(text) must bind the string overload directly, not tie into the DATE overload through an equal-cost cast")

	for _, input := range []string{"20240102123456", "20240102", "2024-01-02 12:34:56"} {
		expect, err := types.ParseDatetime(input, 6)
		require.NoError(t, err)
		fcTC := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_text.ToType(), []string{input}, nil)},
			NewFunctionTestResult(types.T_date.ToType(), false, []types.Date{expect.ToDate()}, nil),
			DateStringToDate)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is 'date keeps the string parser for %s', err info is '%s'", input, info))
	}
}

// TestHexTextKeepsStringEncoding covers the interaction of the restored
// TEXT-to-INT64 preference with a function that carries both string and
// integer overloads: without the direct TEXT overload the cheaper INT64
// route captures the widened TEXT operand and encodes it as a number,
// turning HEX(CAST('256' AS CHAR)) from the string encoding '323536' into
// the numeric '100'. The TEXT overload keeps the VARCHAR peer's string
// executor while integer inputs keep their own overloads.
func TestHexTextKeepsStringEncoding(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		arg   types.T
		casts bool
	}{
		{types.T_text, false},
		{types.T_varchar, false},
	} {
		get, err := GetFunctionByName(proc.Ctx, "hex", []types.Type{tc.arg.ToType()})
		require.NoError(t, err)
		require.Equal(t, types.T_varchar, get.GetReturnType().Oid,
			"hex(%s) must keep the VARCHAR peer's return type", tc.arg)
		_, shouldCast := get.ShouldDoImplicitTypeCast()
		require.Equal(t, tc.casts, shouldCast,
			"hex(%s) must bind the string overload directly, not the cheaper INT64 overload", tc.arg)
	}

	fcTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_text.ToType(), []string{"256"}, nil)},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"323536"}, nil),
		HexString)
	s, info := fcTC.Run()
	require.True(t, s, fmt.Sprintf("case is 'hex keeps the string encoding for TEXT', err info is '%s'", info))
}
