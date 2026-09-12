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
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func runAssignmentIgnoreStringCast(t *testing.T, sourceType, targetType types.Type,
	values []string, nulls []bool, binary bool) (*vector.Vector, *numericWarningSession, error) {
	return runAssignmentIgnoreStringCastWithSelection(t, sourceType, targetType, values, nulls, binary, nil)
}

func runAssignmentIgnoreStringCastWithSelection(t *testing.T, sourceType, targetType types.Type,
	values []string, nulls []bool, binary bool, selectList *FunctionSelectList) (*vector.Vector, *numericWarningSession, error) {
	t.Helper()
	session := &numericWarningSession{}
	proc := testutil.NewProcess(t)
	proc.Session = session
	tc := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(sourceType, values, nulls),
			NewFunctionTestInput(targetType, emptyCastTargetValues(targetType), nil),
		},
		NewFunctionTestResult(targetType, false, nil, nil),
		NewAssignIgnoreCast,
	)
	tc.parameters[0].SetIsBin(binary)
	if err := tc.result.PreExtendAndReset(tc.fnLength); err != nil {
		return nil, session, err
	}
	if selectList == nil {
		result, err := tc.DebugRun()
		return result, session, err
	}
	if err := tc.fn(tc.parameters, tc.result, proc, tc.fnLength, selectList); err != nil {
		return nil, session, err
	}
	return tc.GetResultVectorDirectly(), session, nil
}

func emptyCastTargetValues(typ types.Type) any {
	switch typ.Oid {
	case types.T_int8:
		return []int8{}
	case types.T_int16:
		return []int16{}
	case types.T_int32:
		return []int32{}
	case types.T_int64:
		return []int64{}
	case types.T_uint8:
		return []uint8{}
	case types.T_uint16:
		return []uint16{}
	case types.T_uint32:
		return []uint32{}
	case types.T_uint64:
		return []uint64{}
	case types.T_decimal64:
		return []types.Decimal64{}
	case types.T_decimal128:
		return []types.Decimal128{}
	case types.T_decimal256:
		return []types.Decimal256{}
	case types.T_date:
		return []types.Date{}
	case types.T_datetime:
		return []types.Datetime{}
	case types.T_timestamp:
		return []types.Timestamp{}
	default:
		panic("unsupported assignment-ignore target type")
	}
}

func TestAssignmentIgnoreAdjustsLexicalNumericAndTemporalValues(t *testing.T) {
	t.Run("signed integer", func(t *testing.T) {
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_int32.ToType(), []string{"abc"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []int32{0}, vector.MustFixedColWithTypeCheck[int32](result))
		require.False(t, result.GetNulls().Contains(0))
		require.Equal(t, []numericWarning{{code: moerr.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD}},
			stripWarningMessages(session.warnings))
	})

	t.Run("signed numeric prefix is retained", func(t *testing.T) {
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_int32.ToType(), []string{"12tail"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []int32{12}, vector.MustFixedColWithTypeCheck[int32](result))
		require.Equal(t, []numericWarning{{code: moerr.WARN_DATA_TRUNCATED}},
			stripWarningMessages(session.warnings))
	})

	t.Run("unsigned numeric prefix is retained", func(t *testing.T) {
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_uint32.ToType(), []string{"12.9tail"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []uint32{13}, vector.MustFixedColWithTypeCheck[uint32](result))
		require.Equal(t, []numericWarning{{code: moerr.WARN_DATA_TRUNCATED}},
			stripWarningMessages(session.warnings))
	})

	t.Run("complete unsigned decimal and exponent do not warn", func(t *testing.T) {
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_uint32.ToType(),
			[]string{"12.9", "1e2", "12.9tail", "1e2tail"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []uint32{13, 100, 13, 100}, vector.MustFixedColWithTypeCheck[uint32](result))
		require.Equal(t, []numericWarning{
			{code: moerr.WARN_DATA_TRUNCATED},
			{code: moerr.WARN_DATA_TRUNCATED},
		}, stripWarningMessages(session.warnings))
	})

	t.Run("fraction and exponent numeric prefixes round", func(t *testing.T) {
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_int64.ToType(),
			[]string{"12.9tail", "12.9", "1e2tail", "1e2", "-12.5tail", ".5tail", "1e+tail"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []int64{13, 13, 100, 100, -13, 1, 1}, vector.MustFixedColWithTypeCheck[int64](result))
		require.Equal(t, []numericWarning{
			{code: moerr.WARN_DATA_TRUNCATED},
			{code: moerr.WARN_DATA_TRUNCATED},
			{code: moerr.WARN_DATA_TRUNCATED},
			{code: moerr.WARN_DATA_TRUNCATED},
			{code: moerr.WARN_DATA_TRUNCATED},
		}, stripWarningMessages(session.warnings))
	})

	t.Run("unsigned negative remains a range error", func(t *testing.T) {
		_, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_uint32.ToType(), []string{"-1"}, nil, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "bad value -1")
		require.Empty(t, session.warnings)
	})

	t.Run("integer range remains an error", func(t *testing.T) {
		_, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_int8.ToType(), []string{"999"}, nil, false)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
		require.Empty(t, session.warnings)
	})

	t.Run("extension numeric syntax is not treated as a decimal prefix", func(t *testing.T) {
		_, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_int32.ToType(), []string{"0x12tail"}, nil, false)
		require.Error(t, err)
		require.Empty(t, session.warnings)

		_, session, err = runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.New(types.T_decimal64, 10, 2), []string{"0x12tail"}, nil, false)
		require.Error(t, err)
		require.Empty(t, session.warnings)
	})

	t.Run("ordinary words sharing float prefixes are adjusted", func(t *testing.T) {
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_int32.ToType(),
			[]string{"information", "nanny"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []int32{0, 0}, vector.MustFixedColWithTypeCheck[int32](result))
		require.Equal(t, []numericWarning{
			{code: moerr.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD},
			{code: moerr.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD},
		}, stripWarningMessages(session.warnings))
	})

	t.Run("date", func(t *testing.T) {
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_date.ToType(), []string{"2024-02-30"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []types.Date{types.ZeroDate}, vector.MustFixedColWithTypeCheck[types.Date](result))
		require.False(t, result.GetNulls().Contains(0))
		require.Equal(t, []numericWarning{{code: moerr.ER_WARN_DATA_OUT_OF_RANGE}},
			stripWarningMessages(session.warnings))
	})

	t.Run("empty temporal strings are not NULL", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			target types.Type
			zero   any
		}{
			{name: "date", target: types.T_date.ToType(), zero: []types.Date{types.ZeroDate}},
			{name: "datetime", target: types.T_datetime.ToTypeWithScale(6), zero: []types.Datetime{types.ZeroDatetime}},
			{name: "timestamp", target: types.T_timestamp.ToTypeWithScale(6), zero: []types.Timestamp{types.ZeroTimestamp}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				result, session, err := runAssignmentIgnoreStringCast(
					t, types.T_varchar.ToType(), tc.target, []string{""}, nil, false)
				require.NoError(t, err)
				require.Equal(t, tc.zero, temporalVectorValues(result, tc.target.Oid))
				require.False(t, result.GetNulls().Contains(0))
				require.Equal(t, []numericWarning{{code: moerr.ER_WARN_DATA_OUT_OF_RANGE}},
					stripWarningMessages(session.warnings))
			})
		}
	})

	t.Run("datetime", func(t *testing.T) {
		target := types.T_datetime.ToTypeWithScale(6)
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), target, []string{"2024-02-30 25:00:00"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []types.Datetime{types.ZeroDatetime}, vector.MustFixedColWithTypeCheck[types.Datetime](result))
		require.False(t, result.GetNulls().Contains(0))
		require.Equal(t, []numericWarning{{code: moerr.ER_WARN_DATA_OUT_OF_RANGE}},
			stripWarningMessages(session.warnings))
	})

	t.Run("timestamp", func(t *testing.T) {
		target := types.T_timestamp.ToTypeWithScale(6)
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), target, []string{"2024-02-30 25:00:00"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []types.Timestamp{types.ZeroTimestamp}, vector.MustFixedColWithTypeCheck[types.Timestamp](result))
		require.False(t, result.GetNulls().Contains(0))
		require.Equal(t, []numericWarning{{code: moerr.ER_WARN_DATA_OUT_OF_RANGE}},
			stripWarningMessages(session.warnings))
	})
}

func TestAssignmentIgnoreDecimalClassification(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target types.Type
		zero   any
	}{
		{name: "decimal64", target: types.New(types.T_decimal64, 10, 2), zero: []types.Decimal64{0}},
		{name: "decimal128", target: types.New(types.T_decimal128, 20, 2), zero: []types.Decimal128{{}}},
		{name: "decimal256", target: types.New(types.T_decimal256, 40, 2), zero: []types.Decimal256{{}}},
	} {
		t.Run(tc.name+" lexical", func(t *testing.T) {
			result, session, err := runAssignmentIgnoreStringCast(
				t, types.T_varchar.ToType(), tc.target, []string{"abc"}, nil, false)
			require.NoError(t, err)
			require.Equal(t, tc.zero, vectorValues(result, tc.target.Oid))
			require.False(t, result.GetNulls().Contains(0))
			require.Equal(t, []numericWarning{{code: moerr.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD}},
				stripWarningMessages(session.warnings))
		})

		t.Run(tc.name+" range is not lexical", func(t *testing.T) {
			_, session, err := runAssignmentIgnoreStringCast(
				t, types.T_varchar.ToType(), tc.target, []string{"999999999999999999999999999999999999999"}, nil, false)
			require.Error(t, err)
			require.Empty(t, session.warnings)
		})

		t.Run(tc.name+" prefix is retained", func(t *testing.T) {
			result, session, err := runAssignmentIgnoreStringCast(
				t, types.T_varchar.ToType(), tc.target, []string{"12.34tail"}, nil, false)
			require.NoError(t, err)
			require.Equal(t, []numericWarning{{code: moerr.WARN_DATA_TRUNCATED}},
				stripWarningMessages(session.warnings))
			require.Equal(t, "12.34", decimalVectorValueString(result, tc.target))
		})

		t.Run(tc.name+" prefixed range remains an error", func(t *testing.T) {
			_, session, err := runAssignmentIgnoreStringCast(
				t, types.T_varchar.ToType(), tc.target,
				[]string{"999999999999999999999999999999999999999tail"}, nil, false)
			require.Error(t, err)
			require.Empty(t, session.warnings)
		})
	}
}

func TestAssignmentIgnorePreservesNullsAndWarnsPerRow(t *testing.T) {
	result, session, err := runAssignmentIgnoreStringCast(
		t, types.T_varchar.ToType(), types.T_int64.ToType(),
		[]string{"abc", "abc", "ignored"}, []bool{false, false, true}, false)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 0, 0}, vector.MustFixedColWithTypeCheck[int64](result))
	require.False(t, result.GetNulls().Contains(0))
	require.False(t, result.GetNulls().Contains(1))
	require.True(t, result.GetNulls().Contains(2))
	require.Len(t, session.warnings, 2)
	for _, warning := range session.warnings {
		require.Contains(t, warning.msg, "Incorrect INTEGER value")
		require.NotContains(t, warning.msg, "row ")
		require.NotContains(t, warning.msg, "column ")
	}
}

func TestAssignmentIgnoreIntegerPrefixRoundingBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target types.Type
		value  string
		want   any
	}{
		{name: "signed int8 positive", target: types.T_int8.ToType(), value: "127.49tail", want: []int8{127}},
		{name: "signed int8 negative", target: types.T_int8.ToType(), value: "-128.49tail", want: []int8{-128}},
		{name: "signed int16", target: types.T_int16.ToType(), value: "32767.4tail", want: []int16{32767}},
		{name: "signed int32", target: types.T_int32.ToType(), value: "2147483647.4tail", want: []int32{2147483647}},
		{name: "signed int64 minimum", target: types.T_int64.ToType(), value: "-9223372036854775808.4tail", want: []int64{math.MinInt64}},
		{name: "signed int64 maximum", target: types.T_int64.ToType(), value: "9223372036854775807.4tail", want: []int64{math.MaxInt64}},
		{name: "unsigned uint8", target: types.T_uint8.ToType(), value: "255.4tail", want: []uint8{math.MaxUint8}},
		{name: "unsigned uint16", target: types.T_uint16.ToType(), value: "65535.4tail", want: []uint16{math.MaxUint16}},
		{name: "unsigned uint32", target: types.T_uint32.ToType(), value: "4294967295.4tail", want: []uint32{math.MaxUint32}},
		{name: "unsigned uint64 maximum", target: types.T_uint64.ToType(), value: "18446744073709551615.4tail", want: []uint64{math.MaxUint64}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, session, err := runAssignmentIgnoreStringCast(
				t, types.T_varchar.ToType(), tc.target, []string{tc.value}, nil, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, assignmentIntegerVectorValues(result, tc.target.Oid))
			require.Equal(t, []numericWarning{{code: moerr.WARN_DATA_TRUNCATED}},
				stripWarningMessages(session.warnings))
		})
	}

	for _, tc := range []struct {
		name   string
		target types.Type
		value  string
	}{
		{name: "signed int8 positive carry", target: types.T_int8.ToType(), value: "127.5tail"},
		{name: "signed int8 negative carry", target: types.T_int8.ToType(), value: "-128.5tail"},
		{name: "signed int16 carry", target: types.T_int16.ToType(), value: "32767.5tail"},
		{name: "signed int32 carry", target: types.T_int32.ToType(), value: "2147483647.5tail"},
		{name: "signed int64 minimum carry", target: types.T_int64.ToType(), value: "-9223372036854775808.5tail"},
		{name: "signed int64 maximum carry", target: types.T_int64.ToType(), value: "9223372036854775807.5tail"},
		{name: "unsigned uint8 carry", target: types.T_uint8.ToType(), value: "255.5tail"},
		{name: "unsigned uint16 carry", target: types.T_uint16.ToType(), value: "65535.5tail"},
		{name: "unsigned uint32 carry", target: types.T_uint32.ToType(), value: "4294967295.5tail"},
		{name: "unsigned uint64 carry", target: types.T_uint64.ToType(), value: "18446744073709551615.5tail"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, session, err := runAssignmentIgnoreStringCast(
				t, types.T_varchar.ToType(), tc.target, []string{tc.value}, nil, false)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
			require.Empty(t, session.warnings)
		})
	}

	t.Run("rounds exact values above float64 integer precision", func(t *testing.T) {
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_int64.ToType(),
			[]string{"9007199254740992.6tail", "9223372036854775806.6tail"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []int64{9007199254740993, math.MaxInt64},
			vector.MustFixedColWithTypeCheck[int64](result))
		require.Equal(t, []numericWarning{
			{code: moerr.WARN_DATA_TRUNCATED},
			{code: moerr.WARN_DATA_TRUNCATED},
		}, stripWarningMessages(session.warnings))
	})

	t.Run("unsigned negative values are accepted only when rounded to zero", func(t *testing.T) {
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_uint32.ToType(), []string{"-0.4tail"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []uint32{0}, vector.MustFixedColWithTypeCheck[uint32](result))
		require.Equal(t, []numericWarning{{code: moerr.WARN_DATA_TRUNCATED}},
			stripWarningMessages(session.warnings))

		_, session, err = runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_uint32.ToType(), []string{"-0.5tail"}, nil, false)
		require.Error(t, err)
		require.Empty(t, session.warnings)
	})
}

func TestAssignmentIgnoreIntegerPrefixExponentIsBounded(t *testing.T) {
	t.Run("long mantissa cancels a negative exponent", func(t *testing.T) {
		value := strings.Repeat("9", 1000) + "e-999tail"
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_int64.ToType(), []string{value}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []int64{10}, vector.MustFixedColWithTypeCheck[int64](result))
		require.Len(t, session.warnings, 1)
	})

	t.Run("huge negative exponent rounds to zero", func(t *testing.T) {
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_int64.ToType(),
			[]string{"1e-999999999999999999999999tail"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []int64{0}, vector.MustFixedColWithTypeCheck[int64](result))
		require.Len(t, session.warnings, 1)
	})

	t.Run("zero mantissa with huge positive exponent remains zero", func(t *testing.T) {
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_uint64.ToType(),
			[]string{strings.Repeat("0", 1000) + "e999999999999999999999tail"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []uint64{0}, vector.MustFixedColWithTypeCheck[uint64](result))
		require.Len(t, session.warnings, 1)
	})

	t.Run("exponent with many leading zeros is parsed exactly", func(t *testing.T) {
		result, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_int64.ToType(),
			[]string{"1e" + strings.Repeat("0", 1000) + "2tail"}, nil, false)
		require.NoError(t, err)
		require.Equal(t, []int64{100}, vector.MustFixedColWithTypeCheck[int64](result))
		require.Len(t, session.warnings, 1)
	})

	t.Run("huge positive exponent remains a range error", func(t *testing.T) {
		_, session, err := runAssignmentIgnoreStringCast(
			t, types.T_varchar.ToType(), types.T_uint64.ToType(),
			[]string{"1e999999999999999999999999tail"}, nil, false)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
		require.Empty(t, session.warnings)
	})
}

func TestAssignmentIgnoreSkipsInactiveNumericRows(t *testing.T) {
	selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
	for _, target := range []types.Type{
		types.T_int32.ToType(),
		types.T_uint32.ToType(),
		types.New(types.T_decimal64, 10, 2),
		types.New(types.T_decimal128, 20, 2),
		types.New(types.T_decimal256, 40, 2),
	} {
		t.Run(target.Oid.String(), func(t *testing.T) {
			result, session, err := runAssignmentIgnoreStringCastWithSelection(
				t, types.T_varchar.ToType(), target, []string{"abc", "invalid"}, nil, false, selectList)
			require.NoError(t, err)
			require.False(t, result.IsNull(0))
			require.True(t, result.IsNull(1))
			require.Len(t, session.warnings, 1)
		})
	}

	t.Run("inactive fractional overflow does not fail or warn", func(t *testing.T) {
		selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}}
		result, session, err := runAssignmentIgnoreStringCastWithSelection(
			t, types.T_varchar.ToType(), types.T_int8.ToType(),
			[]string{"127.5tail", "12.9tail"}, nil, false, selectList)
		require.NoError(t, err)
		require.True(t, result.IsNull(0))
		require.False(t, result.IsNull(1))
		require.Equal(t, int8(13), vector.MustFixedColWithTypeCheck[int8](result)[1])
		require.Equal(t, []numericWarning{{code: moerr.WARN_DATA_TRUNCATED}},
			stripWarningMessages(session.warnings))
	})
}

func TestAssignmentIgnoreDoesNotAdjustBinaryString(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target types.Type
		value  string
	}{
		{name: "integer", target: types.T_int64.ToType(), value: "123456789"},
		{name: "decimal", target: types.New(types.T_decimal64, 10, 2), value: "123456789012345"},
		{name: "date", target: types.T_date.ToType(), value: "2024-02-30"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, session, err := runAssignmentIgnoreStringCast(
				t, types.T_binary.ToType(), tc.target, []string{tc.value}, nil, true)
			require.Error(t, err)
			require.Empty(t, session.warnings)
		})
	}
}

func TestAssignmentPrefixClassification(t *testing.T) {
	for _, tc := range []struct {
		value            string
		integerPrefix    string
		hasIntegerPrefix bool
		truncated        bool
		decimalPrefix    string
		hasDecimalPrefix bool
	}{
		{value: "12tail", integerPrefix: "12", hasIntegerPrefix: true, truncated: true, decimalPrefix: "12", hasDecimalPrefix: true},
		{value: "12.34tail", integerPrefix: "12", hasIntegerPrefix: true, truncated: true, decimalPrefix: "12.34", hasDecimalPrefix: true},
		{value: "12.9tail", integerPrefix: "13", hasIntegerPrefix: true, truncated: true, decimalPrefix: "12.9", hasDecimalPrefix: true},
		{value: "1e2tail", integerPrefix: "100", hasIntegerPrefix: true, truncated: true, decimalPrefix: "1e2", hasDecimalPrefix: true},
		{value: "-12.5tail", integerPrefix: "-13", hasIntegerPrefix: true, truncated: true, decimalPrefix: "-12.5", hasDecimalPrefix: true},
		{value: ".5tail", integerPrefix: "1", hasIntegerPrefix: true, truncated: true, decimalPrefix: ".5", hasDecimalPrefix: true},
		{value: "1e+tail", integerPrefix: "1", hasIntegerPrefix: true, truncated: true, decimalPrefix: "1", hasDecimalPrefix: true},
		{value: " 12.9 \t", integerPrefix: "13", hasIntegerPrefix: true, decimalPrefix: "12.9", hasDecimalPrefix: true},
		{value: " 12.9 tail", integerPrefix: "13", hasIntegerPrefix: true, truncated: true, decimalPrefix: "12.9", hasDecimalPrefix: true},
		{value: "abc", hasIntegerPrefix: false, hasDecimalPrefix: false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			integerPrefix, hasIntegerPrefix, truncated, err := assignmentIntegerPrefix(tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.hasIntegerPrefix, hasIntegerPrefix)
			require.Equal(t, tc.truncated, truncated)
			require.Equal(t, tc.integerPrefix, integerPrefix)
			prefix, has := assignmentDecimalPrefix(tc.value)
			require.Equal(t, tc.hasDecimalPrefix, has)
			if tc.hasDecimalPrefix {
				require.Equal(t, tc.decimalPrefix, prefix)
			}
		})
	}
}

func stripWarningMessages(warnings []numericWarning) []numericWarning {
	result := make([]numericWarning, len(warnings))
	for i, warning := range warnings {
		result[i] = numericWarning{code: warning.code}
	}
	return result
}

func vectorValues(result *vector.Vector, oid types.T) any {
	switch oid {
	case types.T_decimal64:
		return vector.MustFixedColWithTypeCheck[types.Decimal64](result)
	case types.T_decimal128:
		return vector.MustFixedColWithTypeCheck[types.Decimal128](result)
	case types.T_decimal256:
		return vector.MustFixedColWithTypeCheck[types.Decimal256](result)
	default:
		panic("unsupported decimal test type")
	}
}

func assignmentIntegerVectorValues(result *vector.Vector, oid types.T) any {
	switch oid {
	case types.T_int8:
		return vector.MustFixedColWithTypeCheck[int8](result)
	case types.T_int16:
		return vector.MustFixedColWithTypeCheck[int16](result)
	case types.T_int32:
		return vector.MustFixedColWithTypeCheck[int32](result)
	case types.T_int64:
		return vector.MustFixedColWithTypeCheck[int64](result)
	case types.T_uint8:
		return vector.MustFixedColWithTypeCheck[uint8](result)
	case types.T_uint16:
		return vector.MustFixedColWithTypeCheck[uint16](result)
	case types.T_uint32:
		return vector.MustFixedColWithTypeCheck[uint32](result)
	case types.T_uint64:
		return vector.MustFixedColWithTypeCheck[uint64](result)
	default:
		panic("unsupported integer test type")
	}
}

func temporalVectorValues(result *vector.Vector, oid types.T) any {
	switch oid {
	case types.T_date:
		return vector.MustFixedColWithTypeCheck[types.Date](result)
	case types.T_datetime:
		return vector.MustFixedColWithTypeCheck[types.Datetime](result)
	case types.T_timestamp:
		return vector.MustFixedColWithTypeCheck[types.Timestamp](result)
	default:
		panic("unsupported temporal test type")
	}
}

func decimalVectorValueString(result *vector.Vector, target types.Type) string {
	switch target.Oid {
	case types.T_decimal64:
		return vector.MustFixedColWithTypeCheck[types.Decimal64](result)[0].Format(target.Scale)
	case types.T_decimal128:
		return vector.MustFixedColWithTypeCheck[types.Decimal128](result)[0].Format(target.Scale)
	case types.T_decimal256:
		return vector.MustFixedColWithTypeCheck[types.Decimal256](result)[0].Format(target.Scale)
	default:
		panic("unsupported decimal test type")
	}
}
