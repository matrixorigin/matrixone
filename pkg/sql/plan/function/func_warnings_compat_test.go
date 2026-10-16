// Copyright 2023 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
	require.Equal(t, "Unknown locale: ''", warnings.warnings[1].msg)
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
	require.Equal(t, "Unknown locale: ''", warnings.warnings[1].msg)
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
