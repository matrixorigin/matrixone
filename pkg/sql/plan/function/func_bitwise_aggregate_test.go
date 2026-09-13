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
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestBitwiseAggregateBinaryOperandWidth(t *testing.T) {
	ctx := context.Background()
	for _, functionName := range []string{"bit_and", "bit_or", "bit_xor"} {
		for _, oid := range []types.T{types.T_binary, types.T_varbinary} {
			for _, width := range []int32{510, 511} {
				resolved, err := GetFunctionByName(ctx, functionName,
					[]types.Type{types.New(oid, width, 0)})
				require.NoError(t, err, "%s(%s(%d))", functionName, oid, width)
				require.Equal(t, oid, resolved.GetReturnType().Oid)
				require.Equal(t, width, resolved.GetReturnType().Width)
			}

			for _, width := range []int32{512, 600} {
				_, err := GetFunctionByName(ctx, functionName,
					[]types.Type{types.New(oid, width, 0)})
				require.Error(t, err, "%s(%s(%d)) must be rejected", functionName, oid, width)
				moErr := moerr.DowncastError(err)
				require.Equal(t, moerr.ErrInvalidBitwiseAggregateOperandsSize, moErr.ErrorCode())
				require.Equal(t, uint16(moerr.ER_INVALID_BITWISE_AGGREGATE_OPERANDS_SIZE), moErr.MySQLCode())
				require.Equal(t,
					"Aggregate bitwise functions cannot accept arguments longer than 511 bytes; consider using the SUBSTRING() function",
					moErr.Error())
			}
		}
	}
}

func TestBitwiseAggregateAcceptsBoundedBinaryExpressions(t *testing.T) {
	ctx := context.Background()
	textInput := types.New(types.T_varchar, 64, 0)

	for _, producer := range []struct {
		name string
		args []types.Type
	}{
		{name: "uuid_to_bin", args: []types.Type{textInput}},
		{name: "inet6_aton", args: []types.Type{textInput}},
	} {
		t.Run(producer.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(ctx, producer.name, producer.args)
			require.NoError(t, err)
			resultType := resolved.GetReturnType()
			require.Equal(t, types.T_varbinary, resultType.Oid)
			require.Equal(t, int32(16), resultType.Width)

			for _, aggregateName := range []string{"bit_and", "bit_or", "bit_xor"} {
				aggregate, err := GetFunctionByName(ctx, aggregateName, []types.Type{resultType})
				require.NoError(t, err, "%s(%s(...))", aggregateName, producer.name)
				require.Equal(t, resultType, aggregate.GetReturnType())
			}
		})
	}

	operand := types.NewWithCharset(types.T_varbinary, 16, 0, types.CharsetBinary)
	for _, operatorName := range []string{"&", "|", "^"} {
		t.Run("binary operator "+operatorName, func(t *testing.T) {
			resolved, err := GetFunctionByName(ctx, operatorName, []types.Type{operand, operand})
			require.NoError(t, err)
			require.Equal(t, types.T_varbinary, resolved.GetReturnType().Oid)
			require.Equal(t, int32(16), resolved.GetReturnType().Width)
		})
	}

	for _, test := range []struct {
		name      string
		operator  string
		left      types.Type
		right     types.Type
		wantWidth int32
		tooWide   bool
	}{
		{
			name:      "bounded result",
			operator:  "|",
			left:      types.NewWithCharset(types.T_varbinary, 16, 0, types.CharsetBinary),
			right:     types.NewWithCharset(types.T_varbinary, 24, 0, types.CharsetBinary),
			wantWidth: 24,
		},
		{
			name:      "oversized result",
			operator:  "^",
			left:      types.NewWithCharset(types.T_varbinary, 16, 0, types.CharsetBinary),
			right:     types.NewWithCharset(types.T_varbinary, 512, 0, types.CharsetBinary),
			wantWidth: 512,
			tooWide:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(ctx, test.operator, []types.Type{test.left, test.right})
			require.NoError(t, err)
			require.Equal(t, types.T_varbinary, resolved.GetReturnType().Oid)
			require.Equal(t, test.wantWidth, resolved.GetReturnType().Width)

			_, err = GetFunctionByName(ctx, "bit_or", []types.Type{resolved.GetReturnType()})
			if test.tooWide {
				require.Error(t, err)
				moErr := moerr.DowncastError(err)
				require.Equal(t, moerr.ErrInvalidBitwiseAggregateOperandsSize, moErr.ErrorCode())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBitwiseBinaryReturnTypeRejectsUnknownOperandDomain(t *testing.T) {
	result := bitwiseBinaryReturnType([]types.Type{types.T_blob.ToType(), types.T_blob.ToType()})
	require.Equal(t, types.T_blob, result.Oid)
	require.Zero(t, result.Width)
}

func TestBitwiseAggregateNumericCastPreservesDecimalRounding(t *testing.T) {
	proc := testutil.NewProcess(t)

	d64Type := types.New(types.T_decimal64, 18, 1)
	d64Values := make([]types.Decimal64, 0, 4)
	for _, value := range []string{"7.4", "3.5", "-2.5", "-2.4"} {
		parsed, err := types.ParseDecimal64(value, d64Type.Width, d64Type.Scale)
		require.NoError(t, err)
		d64Values = append(d64Values, parsed)
	}
	assertBitwiseAggregateCast(t, proc, d64Type, d64Values, nil,
		[]int64{7, 4, -3, -2}, nil, nil)

	d128Type := types.New(types.T_decimal128, 38, 1)
	d128Value, err := types.ParseDecimal128("9007199254740993.5", d128Type.Width, d128Type.Scale)
	require.NoError(t, err)
	assertBitwiseAggregateCast(t, proc, d128Type, []types.Decimal128{d128Value}, nil,
		[]int64{9007199254740994}, nil, nil)

	d256Type := types.New(types.T_decimal256, 65, 1)
	d256Value, err := types.ParseDecimal256("9007199254740993.5", d256Type.Width, d256Type.Scale)
	require.NoError(t, err)
	assertBitwiseAggregateCast(t, proc, d256Type, []types.Decimal256{d256Value}, nil,
		[]int64{9007199254740994}, nil, nil)
}

func TestBitwiseAggregateNumericCastFloatAndNulls(t *testing.T) {
	proc := testutil.NewProcess(t)
	assertBitwiseAggregateCast(t, proc, types.T_float32.ToType(),
		[]float32{2.5, 3.5, -2.5, 0}, []bool{false, false, false, true},
		[]int64{2, 4, -2, 0}, []bool{false, false, false, true}, nil)
	assertBitwiseAggregateCast(t, proc, types.T_float64.ToType(),
		[]float64{2.5, 3.5, -2.5}, nil, []int64{2, 4, -2}, nil, nil)

	warnings := &numericWarningSession{}
	proc.WarningSink = warnings
	assertBitwiseAggregateCast(t, proc, types.T_float64.ToType(),
		[]float64{math.Exp2(63), -math.Exp2(63) - math.Exp2(11)}, nil,
		[]int64{math.MaxInt64, math.MinInt64}, nil, nil)
	require.Len(t, warnings.warnings, 2)
	for _, warning := range warnings.warnings {
		require.Equal(t, moerr.ER_TRUNCATED_WRONG_VALUE, warning.code)
		require.Contains(t, warning.msg, "Truncated incorrect INTEGER value")
	}
	warnings.warnings = nil
	assertBitwiseAggregateCast(t, proc, types.T_float64.ToType(),
		[]float64{-math.Exp2(63)}, nil, []int64{math.MinInt64}, nil, nil)
	require.Empty(t, warnings.warnings, "the inclusive lower INT64 bound is not overflow")
}

func TestBitwiseAggregateNumericCastStringPrefixAndSelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings
	textType := types.New(types.T_varchar, 32, 0)
	assertBitwiseAggregateCast(t, proc, textType,
		[]string{"7tail", "3.5", "not-a-number", ""}, nil,
		[]int64{7, 3, 0, 0}, nil, nil)
	require.Len(t, warnings.warnings, 3)
	for _, warning := range warnings.warnings {
		require.Equal(t, moerr.ER_TRUNCATED_WRONG_VALUE, warning.code)
		require.Contains(t, warning.msg, "Truncated incorrect INTEGER value")
	}
	assertBitwiseAggregateCast(t, proc, textType,
		[]string{"9223372036854775808"}, nil, []int64{math.MinInt64}, nil, nil)
	require.Len(t, warnings.warnings, 3, "2^63 is a valid unsigned 64-bit bit pattern, not a warning")

	// The invalid string is deliberately masked. A short-circuited CASE/IF arm
	// must not parse it or surface a conversion error.
	assertBitwiseAggregateCast(t, proc, textType,
		[]string{"6", "not-a-number"}, nil, []int64{6, 0}, []bool{false, true},
		&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}})
	require.Len(t, warnings.warnings, 3)
	assertBitwiseAggregateCast(t, proc, textType, []string{"not-a-number"}, nil,
		[]int64{0}, []bool{true},
		&FunctionSelectList{AnyNull: true, AllNull: true, SelectList: []bool{false}})
	require.Len(t, warnings.warnings, 3)
}

func TestBitwiseAggregateDecimalCastUnsignedBoundaries(t *testing.T) {
	maxUint64 := ^uint64(0)
	for _, test := range []struct {
		value   string
		scale   int32
		want    uint64
		wantErr bool
	}{
		{value: "9223372036854775807", want: uint64(math.MaxInt64)},
		{value: "9223372036854775808", want: uint64(1) << 63},
		{value: "-9223372036854775809", want: uint64(math.MaxInt64)},
		{value: "18446744073709551615", want: maxUint64},
		{value: "-18446744073709551615", want: 1},
		{value: "18446744073709551616", wantErr: true},
		{value: "-18446744073709551616", wantErr: true},
		{value: "9223372036854775807.5", scale: 1, want: uint64(1) << 63},
		{value: "-9223372036854775808.5", scale: 1, want: uint64(math.MaxInt64)},
		{value: "18446744073709551615.4", scale: 1, want: maxUint64},
		{value: "-18446744073709551615.4", scale: 1, want: 1},
		{value: "18446744073709551615.5", scale: 1, wantErr: true},
		{value: "-18446744073709551615.5", scale: 1, wantErr: true},
	} {
		t.Run("decimal128/"+test.value, func(t *testing.T) {
			assertBitwiseAggregateDecimal128Value(t, test.value, test.scale, test.want, test.wantErr)
		})
		t.Run("decimal256/"+test.value, func(t *testing.T) {
			assertBitwiseAggregateDecimal256Value(t, test.value, test.scale, test.want, test.wantErr)
		})
	}
}

func assertBitwiseAggregateDecimal128Value(t *testing.T, text string, scale int32, want uint64, wantErr bool) {
	t.Helper()
	typ := types.New(types.T_decimal128, 38, scale)
	value, err := types.ParseDecimal128(text, typ.Width, typ.Scale)
	require.NoError(t, err)
	divisor, roundsToZero, err := decimal128RoundingDivisor(scale)
	require.NoError(t, err)
	got, err := bitwiseAggregateDecimal128ToInt64Value(value, divisor, roundsToZero)
	if wantErr {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	require.Equal(t, int64(want), got)
}

func assertBitwiseAggregateDecimal256Value(t *testing.T, text string, scale int32, want uint64, wantErr bool) {
	t.Helper()
	typ := types.New(types.T_decimal256, 65, scale)
	value, err := types.ParseDecimal256(text, typ.Width, typ.Scale)
	require.NoError(t, err)
	divisor, roundsToZero, err := decimal256RoundingDivisor(scale)
	require.NoError(t, err)
	got, err := bitwiseAggregateDecimal256ToInt64Value(value, divisor, roundsToZero)
	if wantErr {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	require.Equal(t, int64(want), got)
}

func TestBitwiseAggregateDecimalCastVectorMasksAndNulls(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}

	d128Type := types.New(types.T_decimal128, 38, 0)
	d128Boundary, err := types.ParseDecimal128("9223372036854775808", d128Type.Width, d128Type.Scale)
	require.NoError(t, err)
	d128Overflow, err := types.ParseDecimal128("18446744073709551616", d128Type.Width, d128Type.Scale)
	require.NoError(t, err)
	assertBitwiseAggregateCast(t, proc, d128Type,
		[]types.Decimal128{d128Boundary, d128Overflow, types.Decimal128{}},
		[]bool{false, false, true},
		[]int64{math.MinInt64, 0, 0}, []bool{false, true, true}, selectList)

	d256Type := types.New(types.T_decimal256, 65, 0)
	d256Boundary, err := types.ParseDecimal256("-9223372036854775809", d256Type.Width, d256Type.Scale)
	require.NoError(t, err)
	d256Overflow, err := types.ParseDecimal256("18446744073709551616", d256Type.Width, d256Type.Scale)
	require.NoError(t, err)
	assertBitwiseAggregateCast(t, proc, d256Type,
		[]types.Decimal256{d256Boundary, d256Overflow, types.Decimal256{}},
		[]bool{false, false, true},
		[]int64{math.MaxInt64, 0, 0}, []bool{false, true, true}, selectList)
}

func TestBitwiseAggregateCastRegistration(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimalType := types.New(types.T_decimal128, 38, 0)
	resolved, err := GetFunctionByNameWithOverload(proc.Ctx, "cast",
		[]types.Type{decimalType, types.T_int64.ToType()}, 4)
	require.NoError(t, err)
	fid, overloadID := DecodeOverloadID(resolved.GetEncodedOverloadID())
	require.Equal(t, int32(CAST), fid)
	require.Equal(t, int32(4), overloadID)

	registered, err := GetFunctionById(proc.Ctx, resolved.GetEncodedOverloadID())
	require.NoError(t, err)
	eval, _, _, _ := registered.GetExecuteMethod()
	require.NotNil(t, eval)

	value, err := types.ParseDecimal128("9223372036854775808", decimalType.Width, decimalType.Scale)
	require.NoError(t, err)
	testCase := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(decimalType, []types.Decimal128{value}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, nil),
	}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{math.MinInt64}, nil), fEvalFn(eval))
	success, info := testCase.Run()
	require.True(t, success, info)
}

func TestBitwiseAggregateCastDispatchAndErrors(t *testing.T) {
	proc := testutil.NewProcess(t)
	assertBitwiseAggregateCastFails(t, proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_int32.ToType(), []int32{1}, nil),
	}, nil)
	assertBitwiseAggregateCastFails(t, proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_int32.ToType(), []int32{1}, nil),
		NewFunctionTestInput(types.T_int32.ToType(), []int32{0}, nil),
	}, nil)
	binaryTextCase := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.NewWithCharset(types.T_varchar, 8, 0, types.CharsetBinary), []string{"1"}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, nil),
	}, NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil), NewBitwiseAggregateCast)
	binaryTextCase.parameters[0].SetIsBin(true)
	success, info := binaryTextCase.Run()
	require.True(t, success, info)
	assertBitwiseAggregateCastFails(t, proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_uuid.ToType(), []types.Uuid{{}}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, nil),
	}, nil)
}

func TestBitwiseAggregateCastYearAndNullableRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}
	assertBitwiseAggregateCast(t, proc, types.T_year.ToType(),
		[]types.MoYear{2024, 2025, 0}, []bool{false, false, true},
		[]int64{2024, 0, 0}, []bool{false, true, true}, selectList)

	date1, err := types.ParseDateCast("2024-01-02")
	require.NoError(t, err)
	date2, err := types.ParseDateCast("2024-01-03")
	require.NoError(t, err)
	assertBitwiseAggregateCast(t, proc, types.T_date.ToType(),
		[]types.Date{date1, date2, 0}, []bool{false, false, true},
		[]int64{20240102, 0, 0}, []bool{false, true, true}, selectList)

	d64Type := types.New(types.T_decimal64, 18, 1)
	d64Value, err := types.ParseDecimal64("1.5", d64Type.Width, d64Type.Scale)
	require.NoError(t, err)
	d64Masked, err := types.ParseDecimal64("2.5", d64Type.Width, d64Type.Scale)
	require.NoError(t, err)
	assertBitwiseAggregateCast(t, proc, d64Type,
		[]types.Decimal64{d64Value, d64Masked, 0}, []bool{false, false, true},
		[]int64{2, 0, 0}, []bool{false, true, true}, selectList)

	datetimeType := types.New(types.T_datetime, 0, 6)
	datetime, err := types.ParseDatetime("2024-12-31 23:59:59.500000", datetimeType.Scale)
	require.NoError(t, err)
	maxDatetime, err := types.ParseDatetime("9999-12-31 23:59:59.500000", datetimeType.Scale)
	require.NoError(t, err)
	assertBitwiseAggregateCast(t, proc, datetimeType,
		[]types.Datetime{datetime, maxDatetime, 0}, []bool{false, false, true},
		[]int64{20250101000000, 0, 0}, []bool{false, true, true}, selectList)

	timestampType := types.New(types.T_timestamp, 0, 6)
	proc.GetSessionInfo().TimeZone = time.UTC
	timestamp, err := types.ParseTimestamp(time.UTC, "2024-12-31 23:59:59.500000", 6)
	require.NoError(t, err)
	maxTimestamp, err := types.ParseTimestamp(time.UTC, "9999-12-31 23:59:59.500000", 6)
	require.NoError(t, err)
	assertBitwiseAggregateCast(t, proc, timestampType,
		[]types.Timestamp{timestamp, maxTimestamp, 0}, []bool{false, false, true},
		[]int64{20250101000000, 0, 0}, []bool{false, true, true}, selectList)

	timeType := types.New(types.T_time, 0, 6)
	timeValue, err := types.ParseTime("-02:03:04.500000", timeType.Scale)
	require.NoError(t, err)
	timeMasked, err := types.ParseTime("02:03:04.500000", timeType.Scale)
	require.NoError(t, err)
	assertBitwiseAggregateCast(t, proc, timeType,
		[]types.Time{timeValue, timeMasked, 0}, []bool{false, false, true},
		[]int64{-20305, 0, 0}, []bool{false, true, true}, selectList)
}

func TestBitwiseAggregateCastFloatErrorsAndOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings
	assertBitwiseAggregateCast(t, proc, types.T_float32.ToType(),
		[]float32{float32(math.Inf(1)), float32(math.Inf(-1))}, nil,
		[]int64{math.MaxInt64, math.MinInt64}, nil, nil)
	assertBitwiseAggregateCast(t, proc, types.T_float64.ToType(),
		[]float64{math.Inf(1), math.Inf(-1)}, nil,
		[]int64{math.MaxInt64, math.MinInt64}, nil, nil)
	require.Len(t, warnings.warnings, 4)

	assertBitwiseAggregateCastFails(t, proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_float32.ToType(), []float32{float32(math.NaN())}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, nil),
	}, nil)
	assertBitwiseAggregateCastFails(t, proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_float64.ToType(), []float64{math.NaN()}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, nil),
	}, nil)
}

func TestBitwiseAggregateCastTemporalRoundingOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)
	datetimeType := types.New(types.T_datetime, 0, 6)
	maxDatetime, err := types.ParseDatetime("9999-12-31 23:59:59.500000", datetimeType.Scale)
	require.NoError(t, err)
	assertBitwiseAggregateCastFails(t, proc, []FunctionTestInput{
		NewFunctionTestInput(datetimeType, []types.Datetime{maxDatetime}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, nil),
	}, nil)

	timestampType := types.New(types.T_timestamp, 0, 6)
	maxTimestamp, err := types.ParseTimestamp(time.UTC, "9999-12-31 23:59:59.500000", 6)
	require.NoError(t, err)
	proc.GetSessionInfo().TimeZone = time.UTC
	assertBitwiseAggregateCastFails(t, proc, []FunctionTestInput{
		NewFunctionTestInput(timestampType, []types.Timestamp{maxTimestamp}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, nil),
	}, nil)
}

func TestBitwiseAggregateDecimalConversionScaleAndSignedNarrowing(t *testing.T) {
	if _, roundsToZero := decimal64RoundingDivisor(20); !roundsToZero {
		t.Fatal("DECIMAL64 scale above its supported precision must round to zero")
	}
	if _, err := decimal64ToInt64(1, -1, 1, false); err == nil {
		t.Fatal("negative DECIMAL64 scale must be rejected")
	}
	if got, err := decimal64ToInt64(123, 20, 0, true); err != nil || got != 0 {
		t.Fatalf("DECIMAL64 values below the supported scale should round to zero: got %d, err %v", got, err)
	}
	if _, _, err := decimal128RoundingDivisor(-1); err == nil {
		t.Fatal("negative DECIMAL128 scale must be rejected")
	}
	if _, roundsToZero, err := decimal128RoundingDivisor(39); err != nil || !roundsToZero {
		t.Fatalf("DECIMAL128 scale above precision should round to zero: roundsToZero=%t, err=%v", roundsToZero, err)
	}
	if _, _, err := decimal256RoundingDivisor(-1); err == nil {
		t.Fatal("negative DECIMAL256 scale must be rejected")
	}
	if _, roundsToZero, err := decimal256RoundingDivisor(66); err != nil || !roundsToZero {
		t.Fatalf("DECIMAL256 scale above precision should round to zero: roundsToZero=%t, err=%v", roundsToZero, err)
	}
	if got, err := bitwiseAggregateDecimal128ToInt64Value(types.Decimal128{B0_63: 123}, types.Decimal128{}, true); err != nil || got != 0 {
		t.Fatalf("DECIMAL128 value at an unsupported scale should round to zero: got %d, err %v", got, err)
	}
	if got, err := bitwiseAggregateDecimal256ToInt64Value(types.Decimal256{B0_63: 123}, types.Decimal256{}, true); err != nil || got != 0 {
		t.Fatalf("DECIMAL256 value at an unsupported scale should round to zero: got %d, err %v", got, err)
	}
	if _, err := int64FromSignedMagnitude(uint64(math.MaxInt64)+1, false); err == nil {
		t.Fatal("signed narrowing must reject positive values beyond INT64")
	}
	if _, err := int64FromSignedMagnitude(uint64(1)<<63+1, true); err == nil {
		t.Fatal("signed narrowing must reject negative magnitudes beyond INT64")
	}
}

func assertBitwiseAggregateCastFails(
	t *testing.T,
	proc *process.Process,
	inputs []FunctionTestInput,
	selectList *FunctionSelectList,
) {
	t.Helper()
	testCase := NewFunctionTestCase(proc, inputs,
		NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil), NewBitwiseAggregateCast)
	if selectList != nil {
		testCase = testCase.WithSelectList(selectList)
	}
	success, info := testCase.Run()
	require.True(t, success, info)
}

func TestBitwiseAggregateNumericCastDateAndTemporalBoundaries(t *testing.T) {
	proc := testutil.NewProcess(t)
	dateType := types.T_date.ToType()
	date1, err := types.ParseDateCast("2024-01-02")
	require.NoError(t, err)
	date2, err := types.ParseDateCast("2024-01-03")
	require.NoError(t, err)
	assertBitwiseAggregateCast(t, proc, dateType, []types.Date{date1, date2}, nil,
		[]int64{20240102, 20240103}, nil, nil)

	datetimeType := types.New(types.T_datetime, 0, 6)
	datetime, err := types.ParseDatetime("2024-12-31 23:59:59.500000", datetimeType.Scale)
	require.NoError(t, err)
	assertBitwiseAggregateCast(t, proc, datetimeType, []types.Datetime{datetime}, nil,
		[]int64{20250101000000}, nil, nil)

	timeType := types.New(types.T_time, 0, 6)
	timeValue, err := types.ParseTime("-02:03:04.500000", timeType.Scale)
	require.NoError(t, err)
	assertBitwiseAggregateCast(t, proc, timeType, []types.Time{timeValue}, nil,
		[]int64{-20305}, nil, nil)
	assertBitwiseAggregateCast(t, proc, types.New(types.T_time, 0, 0),
		[]types.Time{types.TimeFromClock(false, 2, 3, 4, 500000)}, nil,
		[]int64{20304}, nil, nil)

	timestampType := types.New(types.T_timestamp, 0, 6)
	timestamp, err := types.ParseTimestamp(time.UTC, "2024-12-31 23:59:59.500000", 6)
	require.NoError(t, err)
	proc.GetSessionInfo().TimeZone = time.UTC
	assertBitwiseAggregateCast(t, proc, timestampType, []types.Timestamp{timestamp}, nil,
		[]int64{20250101000000}, nil, nil)
	proc.GetSessionInfo().TimeZone = time.FixedZone("UTC+8", 8*60*60)
	assertBitwiseAggregateCast(t, proc, timestampType, []types.Timestamp{timestamp}, nil,
		[]int64{20250101080000}, nil, nil)
}

func assertBitwiseAggregateCast(
	t *testing.T,
	proc *process.Process,
	inputType types.Type,
	inputValues any,
	inputNulls []bool,
	want []int64,
	wantNulls []bool,
	selectList *FunctionSelectList,
) {
	t.Helper()
	resultType := types.T_int64.ToType()
	inputs := []FunctionTestInput{
		NewFunctionTestInput(inputType, inputValues, inputNulls),
		NewFunctionTestInput(resultType, make([]int64, len(want)), nil),
	}
	expected := NewFunctionTestResult(resultType, false, want, wantNulls)
	testCase := NewFunctionTestCase(proc, inputs, expected, NewBitwiseAggregateCast)
	if selectList != nil {
		testCase = testCase.WithSelectList(selectList)
	}
	success, info := testCase.Run()
	require.True(t, success, info)
}
