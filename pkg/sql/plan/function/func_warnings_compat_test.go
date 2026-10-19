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
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Issue #30217: MySQL emits warning 1649 (ER_UNKNOWN_LOCALE) when FORMAT
// receives an unknown or NULL locale, while still returning the en_US
// fallback value.
func TestFormatUnknownLocaleWarning(t *testing.T) {
	proc := testutil.NewProcess(t)
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings

	// String-domain path: unknown 'zz_ZZ' and NULL locale both fall back to
	// en_US with a warning; supported de_DE formats without one.
	caseData := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"1234567.89", "1234567.89", "1234567.89"}, []bool{false, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"2", "2", "2"}, []bool{false, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"zz_ZZ", "", "de_DE"}, []bool{false, true, false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false,
			[]string{"1,234,567.89", "1,234,567.89", "1.234.567,89"}, []bool{false, false, false}),
		FormatWith3Args,
	)
	succeed, info := caseData.RunAndFree()
	require.True(t, succeed, info)
	require.Len(t, warnings.warnings, 2)
	require.Equal(t, uint16(moerr.ER_UNKNOWN_LOCALE), warnings.warnings[0].code)
	require.Equal(t, "Unknown locale: 'zz_ZZ'", warnings.warnings[0].msg)
	require.Equal(t, uint16(moerr.ER_UNKNOWN_LOCALE), warnings.warnings[1].code)
	require.Equal(t, "Unknown locale: 'NULL'", warnings.warnings[1].msg)
}

// TestFormatUnknownLocaleWarningNumeric covers the exact DECIMAL/INTEGER
// path (formatWithNumericFirst) of the same contract.
func TestFormatUnknownLocaleWarningNumeric(t *testing.T) {
	proc := testutil.NewProcess(t)
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings

	caseData := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1234567, 1234567}, []bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"2", "2"}, []bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"zz_ZZ", ""}, []bool{false, true}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false,
			[]string{"1,234,567.00", "1,234,567.00"}, []bool{false, false}),
		FormatWith3Args,
	)
	succeed, info := caseData.RunAndFree()
	require.True(t, succeed, info)
	require.Len(t, warnings.warnings, 2)
	require.Equal(t, uint16(moerr.ER_UNKNOWN_LOCALE), warnings.warnings[0].code)
	require.Equal(t, "Unknown locale: 'zz_ZZ'", warnings.warnings[0].msg)
	require.Equal(t, uint16(moerr.ER_UNKNOWN_LOCALE), warnings.warnings[1].code)
	require.Equal(t, "Unknown locale: 'NULL'", warnings.warnings[1].msg)
}

// Issue #30214: MySQL emits warning 3020 (ER_INVALID_ARGUMENT_FOR_LOGARITHM)
// per evaluated invalid row for the LN/LOG family; the NULL value result is
// unchanged.
func TestLnInvalidArgWarning(t *testing.T) {
	proc := testutil.NewProcess(t)
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings

	caseData := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{0, -3, 1}, []bool{false, false, false}),
		},
		NewFunctionTestResult(types.T_float64.ToType(), false,
			[]float64{0, 0, 0}, []bool{true, true, false}),
		builtInLn,
	)
	succeed, info := caseData.RunAndFree()
	require.True(t, succeed, info)
	require.Len(t, warnings.warnings, 2)
	require.Equal(t, uint16(moerr.ER_INVALID_ARGUMENT_FOR_LOGARITHM), warnings.warnings[0].code)
	require.Equal(t, "Invalid argument for logarithm", warnings.warnings[0].msg)
	require.Equal(t, uint16(moerr.ER_INVALID_ARGUMENT_FOR_LOGARITHM), warnings.warnings[1].code)
	require.Equal(t, "Invalid argument for logarithm", warnings.warnings[1].msg)
}

func TestLogInvalidArgWarning(t *testing.T) {
	proc := testutil.NewProcess(t)
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings

	// LOG(base, x) is invalid for base 1, base <= 0, or x <= 0; each such
	// row gets exactly one warning and stays NULL.
	caseData := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{1, 0, 2, 2}, []bool{false, false, false, false}),
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{100, 100, 0, 8}, []bool{false, false, false, false}),
		},
		NewFunctionTestResult(types.T_float64.ToType(), false,
			[]float64{0, 0, 0, 3}, []bool{true, true, true, false}),
		builtInLog,
	)
	succeed, info := caseData.RunAndFree()
	require.True(t, succeed, info)
	require.Len(t, warnings.warnings, 3)
	for _, w := range warnings.warnings {
		require.Equal(t, uint16(moerr.ER_INVALID_ARGUMENT_FOR_LOGARITHM), w.code)
		require.Equal(t, "Invalid argument for logarithm", w.msg)
	}
}

func TestLog2Log10InvalidArgWarning(t *testing.T) {
	proc := testutil.NewProcess(t)
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings

	caseData := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{0, -3, 2}, []bool{false, false, false}),
		},
		NewFunctionTestResult(types.T_float64.ToType(), false,
			[]float64{0, 0, 1}, []bool{true, true, false}),
		builtInLog2,
	)
	succeed, info := caseData.RunAndFree()
	require.True(t, succeed, info)
	require.Len(t, warnings.warnings, 2)

	caseData2 := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{0, -3, 10}, []bool{false, false, false}),
		},
		NewFunctionTestResult(types.T_float64.ToType(), false,
			[]float64{0, 0, 1}, []bool{true, true, false}),
		builtInLog10,
	)
	succeed2, info2 := caseData2.RunAndFree()
	require.True(t, succeed2, info2)
	require.Len(t, warnings.warnings, 4)
	for _, w := range warnings.warnings {
		require.Equal(t, uint16(moerr.ER_INVALID_ARGUMENT_FOR_LOGARITHM), w.code)
		require.Equal(t, "Invalid argument for logarithm", w.msg)
	}
}

// Issue #30213: MySQL warns 1411 (ER_WRONG_VALUE_FOR_TYPE) on unparseable or
// calendar-invalid STR_TO_DATE input, and 1292 (ER_TRUNCATED_WRONG_VALUE) on
// trailing characters; both result values stay MySQL-identical.
func TestStrToDateWarnings(t *testing.T) {
	proc := testutil.NewProcess(t)
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings

	validDate := types.DateFromCalendar(2024, 1, 2)
	caseData := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"2024-02-30", "2024-01-02xyz", "zz", "2024-01-02"},
				[]bool{false, false, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"%Y-%m-%d", "%Y-%m-%d", "%Y-%m-%d", "%Y-%m-%d"},
				[]bool{false, false, false, false}),
		},
		NewFunctionTestResult(types.T_date.ToType(), false,
			[]types.Date{0, validDate, 0, validDate}, []bool{true, false, true, false}),
		builtInStrToDate,
	)
	succeed, info := caseData.RunAndFree()
	require.True(t, succeed, info)
	require.Len(t, warnings.warnings, 3)
	require.Equal(t, uint16(moerr.ER_WRONG_VALUE_FOR_TYPE), warnings.warnings[0].code)
	require.Equal(t, "Incorrect datetime value: '2024-02-30' for function str_to_date", warnings.warnings[0].msg)
	require.Equal(t, uint16(moerr.ER_TRUNCATED_WRONG_VALUE), warnings.warnings[1].code)
	require.Equal(t, "Truncated incorrect date value: '2024-01-02xyz'", warnings.warnings[1].msg)
	require.Equal(t, uint16(moerr.ER_WRONG_VALUE_FOR_TYPE), warnings.warnings[2].code)
	require.Equal(t, "Incorrect datetime value: 'zz' for function str_to_date", warnings.warnings[2].msg)
}

func TestStrToDatetimeWarnings(t *testing.T) {
	proc := testutil.NewProcess(t)
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings

	validDt := types.DatetimeFromClock(2024, 1, 2, 10, 30, 45, 0)
	caseData := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"2024-02-30 10:30:45", "2024-01-02 10:30:45xyz", "2024-01-02 10:30:45"},
				[]bool{false, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"%Y-%m-%d %H:%i:%s", "%Y-%m-%d %H:%i:%s", "%Y-%m-%d %H:%i:%s"},
				[]bool{false, false, false}),
		},
		NewFunctionTestResult(types.T_datetime.ToType(), false,
			[]types.Datetime{0, validDt, validDt}, []bool{true, false, false}),
		builtInStrToDatetime,
	)
	succeed, info := caseData.RunAndFree()
	require.True(t, succeed, info)
	require.Len(t, warnings.warnings, 2)
	require.Equal(t, uint16(moerr.ER_WRONG_VALUE_FOR_TYPE), warnings.warnings[0].code)
	require.Equal(t, "Incorrect datetime value: '2024-02-30 10:30:45' for function str_to_date", warnings.warnings[0].msg)
	require.Equal(t, uint16(moerr.ER_TRUNCATED_WRONG_VALUE), warnings.warnings[1].code)
	require.Equal(t, "Truncated incorrect datetime value: '2024-01-02 10:30:45xyz'", warnings.warnings[1].msg)
}

// Masked branches must not publish diagnostics for unevaluated rows.
func TestStrToDateWarningsRespectSelection(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  types.Type
		fn   executeLogicOfOverload
		zero any
	}{
		{"date", types.T_date.ToType(), builtInStrToDate, []types.Date{0, 0, 0}},
		{"datetime", types.T_datetime.ToType(), builtInStrToDatetime, []types.Datetime{0, 0, 0}},
		{"time", types.T_time.ToType(), builtInStrToTime, []types.Time{0, 0, 0}},
	} {
		for _, all := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/all=%t", tc.name, all), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				defer proc.Free()
				sink := &numericWarningSession{}
				proc.WarningSink = sink
				test := NewFunctionTestCase(proc, []FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{"bad", "bad", "bad"}, []bool{false, false, true}),
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%Y-%m-%d"}, nil),
				}, NewFunctionTestResult(tc.typ, false, tc.zero, []bool{true, true, true}), tc.fn).
					WithSelectList(&FunctionSelectList{AnyNull: true, AllNull: all, SelectList: []bool{false, true, true}})
				ok, info := test.RunAndFree()
				require.True(t, ok, info)
				want := 1
				if all {
					want = 0
				}
				require.Len(t, sink.warnings, want)
			})
		}
	}
}

func TestStrToDateWarningMessageBound(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	sink := &numericWarningSession{}
	proc.WarningSink = sink
	input := []byte(strings.Repeat("x", 1024))
	appendStrToDateWarning(proc, moerr.ER_WRONG_VALUE_FOR_TYPE, "Incorrect datetime value", " for function str_to_date", input)
	require.Equal(t, "Incorrect datetime value: '"+strings.Repeat("x", strToDateWarningInputMax)+"' for function str_to_date", sink.warnings[0].msg)
	probe := &process.WarningProbe{}
	proc.WarningSink = probe
	appendStrToDateWarning(proc, moerr.ER_WRONG_VALUE_FOR_TYPE, "Incorrect datetime value", " for function str_to_date", input)
	require.True(t, probe.Warned(), "count-only probes must still prevent unsafe constant folding")
}

// Keep diagnostic construction bounded even during optimizer probes that
// retain only warning presence. Inputs are allocated outside the timed loop.
func BenchmarkStrToDateWarningDiscard(b *testing.B) {
	proc := testutil.NewProcess(b)
	defer proc.Free()
	input := []byte(strings.Repeat("x", 2<<20))
	for _, probe := range []bool{false, true} {
		b.Run(fmt.Sprintf("probe=%t", probe), func(b *testing.B) {
			if probe {
				proc.WarningSink = &process.WarningProbe{}
			} else {
				proc.WarningSink = nil
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				appendStrToDateWarning(proc, moerr.ER_WRONG_VALUE_FOR_TYPE, "Incorrect datetime value", " for function str_to_date", input)
			}
		})
	}
}

// Exercise the canonical binder kernel as well as both legacy entry paths.
func TestFormatLocaleDiagnostics(t *testing.T) {
	for _, kernel := range []struct {
		name                      string
		run                       executeLogicOfOverload
		numeric, integerPrecision bool
	}{
		{"canonical", formatIntegerPrecision, true, true},
		{"legacy_numeric", FormatWith3Args, true, false},
		{"legacy_string", FormatWith3Args, false, false},
	} {
		t.Run(kernel.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			sink := &numericWarningSession{}
			proc.WarningSink = sink
			locales := []string{"", "", strings.Repeat("X", 129), "EN_us"}
			number := NewFunctionTestInput(types.T_varchar.ToType(), []string{"1234", "1234", "1234", "1234"}, nil)
			if kernel.numeric {
				number = NewFunctionTestInput(types.T_int64.ToType(), []int64{1234, 1234, 1234, 1234}, nil)
			}
			digits := NewFunctionTestInput(types.T_varchar.ToType(), []string{"2", "2", "2", "2"}, nil)
			if kernel.integerPrecision {
				digits = NewFunctionTestInput(types.T_int64.ToType(), []int64{2, 2, 2, 2}, nil)
			}
			fc := NewFunctionTestCase(proc, []FunctionTestInput{number, digits,
				NewFunctionTestInput(types.T_varchar.ToType(), locales, []bool{false, true, false, false})},
				NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"1,234.00", "1,234.00", "1,234.00", "1,234.00"}, nil), kernel.run)
			defer fc.Free()
			ok, info := fc.Run()
			require.True(t, ok, info)
			require.Equal(t, []numericWarning{
				{code: moerr.ER_UNKNOWN_LOCALE, msg: "Unknown locale: ''"},
				{code: moerr.ER_UNKNOWN_LOCALE, msg: "Unknown locale: 'NULL'"},
				{code: moerr.ER_UNKNOWN_LOCALE, msg: "Unknown locale: '" + strings.Repeat("X", 128) + "'"},
			}, sink.warnings)
			probe := &process.WarningProbe{}
			proc.WarningSink = probe
			ok, info = fc.Run()
			require.True(t, ok, info)
			require.True(t, probe.Warned())
		})
	}
}

// Allocate input vectors outside the measured loop; measure the actual SQL
// kernel and the warning builder separately, with no retained output growth.
func BenchmarkFormatLocaleDiagnostics(b *testing.B) {
	for _, size := range []int{5, 2 << 20} {
		locale := strings.Repeat("X", size)
		for _, mode := range []string{"none", "probe", "retain"} {
			b.Run(fmt.Sprintf("bytes=%d/%s", size, mode), func(b *testing.B) {
				proc := testutil.NewProcess(b)
				defer proc.Free()
				sink := &numericWarningSession{}
				switch mode {
				case "probe":
					proc.WarningSink = &process.WarningProbe{}
				case "retain":
					proc.WarningSink = sink
				}
				for _, kernel := range []bool{false, true} {
					b.Run(fmt.Sprintf("kernel=%t", kernel), func(b *testing.B) {
						fc := NewFunctionTestCase(proc, []FunctionTestInput{
							NewFunctionTestInput(types.T_int64.ToType(), []int64{1234}, nil),
							NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, nil),
							NewFunctionTestInput(types.T_varchar.ToType(), []string{locale}, nil)},
							NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil), formatIntegerPrecision)
						defer fc.Free()
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							sink.warnings = sink.warnings[:0]
							if kernel {
								if err := fc.result.PreExtendAndReset(1); err != nil {
									b.Fatal(err)
								}
								if err := formatIntegerPrecision(fc.parameters, fc.result, proc, 1, nil); err != nil {
									b.Fatal(err)
								}
							} else {
								appendLocaleUnknownWarning(proc, locale)
							}
						}
					})
				}
			})
		}
	}
}

func TestFormatDirectDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		locale   string
		null     bool
		warnings int
	}{
		{"bad_locale", false, 3}, {"", false, 3}, {"", true, 3}, {"en_US", false, 0},
	} {
		t.Run(fmt.Sprintf("%s/null=%t", tc.locale, tc.null), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			sink := &numericWarningSession{}
			proc.WarningSink = sink
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1234}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.locale}, []bool{tc.null}),
			}, NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil), formatIntegerPrecision)
			defer fc.Free()
			for _, input := range fc.parameters {
				input.SetClass(vector.CONSTANT)
				input.SetLength(3)
			}
			for _, length := range []int{3, 0} {
				sink.warnings = nil
				func() {
					out, err := RunFunctionDirectly(proc, EncodeOverloadID(FORMAT, FormatIntegerPrecisionLocaleOverload), fc.parameters, length)
					require.NoError(t, err)
					defer out.Free(proc.Mp())
					require.Equal(t, length, out.Length())
					want := 0
					if length > 0 {
						want = tc.warnings
						require.True(t, out.IsConst())
						require.Equal(t, "1,234.00", out.GetStringAt(0))
					}
					require.Len(t, sink.warnings, want)
				}()
			}
		})
	}
}

func TestScalarWarningUTF8Boundary(t *testing.T) {
	for _, tc := range []struct{ name, input, prefix string }{
		{"ascii_at", strings.Repeat("a", 128), strings.Repeat("a", 128)},
		{"ascii_over", strings.Repeat("a", 129), strings.Repeat("a", 128)},
		{"two_byte_at", strings.Repeat("é", 64), strings.Repeat("é", 64)},
		{"two_byte_split", "a" + strings.Repeat("é", 64), "a" + strings.Repeat("é", 63)},
		{"three_byte_under", strings.Repeat("界", 42), strings.Repeat("界", 42)},
		{"three_byte_split", strings.Repeat("界", 43), strings.Repeat("界", 42)},
		{"four_byte_at", strings.Repeat("😀", 32), strings.Repeat("😀", 32)},
		{"four_byte_split", "a" + strings.Repeat("😀", 32), "a" + strings.Repeat("😀", 31)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			sink := &numericWarningSession{}
			proc.WarningSink = sink
			input := NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.input}, nil)
			formatCase := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1234}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, nil), input},
				NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"1,234.00"}, nil), formatIntegerPrecision)
			ok, info := formatCase.RunAndFree()
			require.True(t, ok, info)
			dateCase := NewFunctionTestCase(proc, []FunctionTestInput{input,
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"%Y-%m-%d"}, nil)},
				NewFunctionTestResult(types.T_date.ToType(), false, []types.Date{0}, []bool{true}), builtInStrToDate)
			ok, info = dateCase.RunAndFree()
			require.True(t, ok, info)
			trailing := "2024-01-02" + tc.input
			// The prefix is counted from the entire input, including the valid date.
			var trailingPrefix string
			for _, r := range trailing {
				if len(trailingPrefix)+utf8.RuneLen(r) > 128 {
					break
				}
				trailingPrefix += string(r)
			}
			trailingCase := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{trailing}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"%Y-%m-%d"}, nil)},
				NewFunctionTestResult(types.T_date.ToType(), false, []types.Date{types.DateFromCalendar(2024, 1, 2)}, nil), builtInStrToDate)
			ok, info = trailingCase.RunAndFree()
			require.True(t, ok, info)
			require.Equal(t, []numericWarning{
				{code: moerr.ER_UNKNOWN_LOCALE, msg: "Unknown locale: '" + tc.prefix + "'"},
				{code: moerr.ER_WRONG_VALUE_FOR_TYPE, msg: "Incorrect datetime value: '" + tc.prefix + "' for function str_to_date"},
				{code: moerr.ER_TRUNCATED_WRONG_VALUE, msg: "Truncated incorrect date value: '" + trailingPrefix + "'"},
			}, sink.warnings)
			for _, w := range sink.warnings {
				require.True(t, utf8.ValidString(w.msg), "%x", w.msg)
			}
		})
	}
}
