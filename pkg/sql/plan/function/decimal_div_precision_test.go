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
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimalDivisionTypeUsesPrecisionIncrement(t *testing.T) {
	decimal10Scale2 := types.New(types.T_decimal64, 10, 2)
	decimal50Scale10 := types.New(types.T_decimal256, 50, 10)
	integer := types.T_int64.ToType()

	tests := []struct {
		name      string
		increment int32
		inputs    []types.Type
		want      types.Type
		wantCasts []types.Type
	}{
		{
			name:      "zero increment",
			increment: 0,
			inputs:    []types.Type{decimal10Scale2, decimal10Scale2},
			want:      types.New(types.T_decimal128, 12, 2),
		},
		{
			name:      "default increment",
			increment: 4,
			inputs:    []types.Type{decimal10Scale2, decimal10Scale2},
			want:      types.New(types.T_decimal128, 16, 6),
		},
		{
			name:      "larger increment",
			increment: 10,
			inputs:    []types.Type{decimal10Scale2, decimal10Scale2},
			want:      types.New(types.T_decimal128, 22, 12),
		},
		{
			name:      "precision promotes physical decimal",
			increment: 30,
			inputs:    []types.Type{decimal10Scale2, decimal10Scale2},
			want:      types.New(types.T_decimal256, 42, 30),
			wantCasts: []types.Type{
				types.New(types.T_decimal256, 10, 2),
				types.New(types.T_decimal256, 10, 2),
			},
		},
		{
			name:      "decimal256 precision and scale caps",
			increment: 30,
			inputs:    []types.Type{decimal50Scale10, integer},
			want:      types.New(types.T_decimal256, 65, 30),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := WithDivPrecisionIncrement(context.Background(), test.increment)
			resolved, err := GetFunctionByName(ctx, "/", test.inputs)
			require.NoError(t, err)
			require.Equal(t, test.want, resolved.GetReturnType())
			casts, shouldCast := resolved.ShouldDoImplicitTypeCast()
			if test.wantCasts != nil {
				require.True(t, shouldCast)
				require.Equal(t, test.wantCasts, casts)
			}
		})
	}

	resolved, err := GetFunctionByName(
		WithDivPrecisionIncrement(context.Background(), 30),
		"/",
		[]types.Type{types.T_float64.ToType(), types.T_float64.ToType()},
	)
	require.NoError(t, err)
	require.Equal(t, types.T_float64.ToType(), resolved.GetReturnType())
}

func TestTemporalDivisionUsesPackedPrecision(t *testing.T) {
	integer := types.T_int64.ToType()
	for _, test := range []struct {
		name   string
		left   types.Type
		right  types.Type
		widths [4]int32 // Increments 0, 4, 10, 30.
		scales [4]int32
	}{
		{"date", types.T_date.ToType(), types.New(types.T_decimal64, 10, 0), [4]int32{8, 12, 18, 38}, [4]int32{0, 4, 10, 30}},
		{"year", types.T_year.ToType(), types.New(types.T_decimal64, 10, 0), [4]int32{4, 8, 14, 34}, [4]int32{0, 4, 10, 30}},
		{"datetime(0)", types.New(types.T_datetime, 0, 0), integer, [4]int32{14, 18, 24, 44}, [4]int32{0, 4, 10, 30}},
		{"datetime(6)", types.New(types.T_datetime, 0, 6), integer, [4]int32{20, 24, 30, 50}, [4]int32{6, 10, 16, 30}},
		{"timestamp(6)", types.New(types.T_timestamp, 0, 6), integer, [4]int32{20, 24, 30, 50}, [4]int32{6, 10, 16, 30}},
		{"time(3)", types.New(types.T_time, 0, 3), integer, [4]int32{17, 21, 27, 47}, [4]int32{3, 7, 13, 30}},
		{"time(6)", types.New(types.T_time, 0, 6), integer, [4]int32{20, 24, 30, 50}, [4]int32{6, 10, 16, 30}},
		{"reversed time(3)", integer, types.New(types.T_time, 0, 3), [4]int32{22, 26, 32, 52}, [4]int32{0, 4, 10, 30}},
	} {
		for i, increment := range []int32{0, 4, 10, 30} {
			t.Run(test.name+"/increment="+strconv.Itoa(int(increment)), func(t *testing.T) {
				resolved, err := GetFunctionByName(
					WithDivPrecisionIncrement(context.Background(), increment),
					"/", []types.Type{test.left, test.right})
				require.NoError(t, err)
				oid := types.T_decimal128
				if test.widths[i] > 38 {
					oid = types.T_decimal256
				}
				require.Equal(t, types.New(oid, test.widths[i], test.scales[i]), resolved.GetReturnType())
			})
		}
	}

	// TIME(6) and BIGINT can each exceed Decimal64's 18-digit capacity.
	resolved, err := GetFunctionByName(context.Background(), "/",
		[]types.Type{types.New(types.T_time, 0, 6), integer})
	require.NoError(t, err)
	require.Equal(t, types.New(types.T_decimal128, 24, 10), resolved.GetReturnType())
	casts, needed := resolved.ShouldDoImplicitTypeCast()
	require.True(t, needed)
	require.Equal(t, []types.Type{
		types.New(types.T_decimal128, 20, 6),
		types.New(types.T_decimal128, 19, 0),
	}, casts)
}

func TestDecimalDivisionExecutionUsesBoundResultScale(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.New(types.T_decimal64, 10, 2)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(inputType, []types.Decimal64{100}, nil),
		NewFunctionTestInput(inputType, []types.Decimal64{300}, nil),
	}

	for _, test := range []struct {
		name string
		typ  types.Type
		want types.Decimal128
	}{
		{
			name: "scale 2",
			typ:  types.New(types.T_decimal128, 12, 2),
			want: types.Decimal128{B0_63: 33},
		},
		{
			name: "scale 6",
			typ:  types.New(types.T_decimal128, 16, 6),
			want: types.Decimal128{B0_63: 333333},
		},
		{
			name: "scale 12",
			typ:  types.New(types.T_decimal128, 22, 12),
			want: types.Decimal128{B0_63: 333333333333},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			caseUnderTest := NewFunctionTestCase(
				proc,
				inputs,
				NewFunctionTestResult(test.typ, false, []types.Decimal128{test.want}, nil),
				divFn,
			)
			defer caseUnderTest.Free()
			passed, info := caseUnderTest.Run()
			require.True(t, passed, info)
			require.Equal(t, test.typ, *caseUnderTest.GetResultVectorDirectly().GetType())
		})
	}
}

func TestDecimal256DivisionExecutionUsesBoundResultScale(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.New(types.T_decimal256, 10, 2)
	one, err := types.ParseDecimal256("1.00", inputType.Width, inputType.Scale)
	require.NoError(t, err)
	three, err := types.ParseDecimal256("3.00", inputType.Width, inputType.Scale)
	require.NoError(t, err)
	resultType := types.New(types.T_decimal256, 42, 30)
	want, err := types.ParseDecimal256("0.333333333333333333333333333333", resultType.Width, resultType.Scale)
	require.NoError(t, err)

	caseUnderTest := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(inputType, []types.Decimal256{one}, nil),
			NewFunctionTestInput(inputType, []types.Decimal256{three}, nil),
		},
		NewFunctionTestResult(resultType, false, []types.Decimal256{want}, nil),
		divFn,
	)
	defer caseUnderTest.Free()
	passed, info := caseUnderTest.Run()
	require.True(t, passed, info)
	require.Equal(t, resultType, *caseUnderTest.GetResultVectorDirectly().GetType())
}

func TestDecimal256DivisionAvoidsScaledNumeratorOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.New(types.T_decimal256, 65, 0)
	value, err := types.ParseDecimal256(
		"99999999999999999999999999999999999999999999999999999999999999999",
		inputType.Width,
		inputType.Scale,
	)
	require.NoError(t, err)
	third, err := types.ParseDecimal256(
		"33333333333333333333333333333333333333333333333333333333333333333",
		inputType.Width,
		inputType.Scale,
	)
	require.NoError(t, err)
	resultType := types.New(types.T_decimal256, 65, 30)
	wantOne, err := types.ParseDecimal256("1.000000000000000000000000000000", resultType.Width, resultType.Scale)
	require.NoError(t, err)
	wantThird, err := types.ParseDecimal256("0.333333333333333333333333333333", resultType.Width, resultType.Scale)
	require.NoError(t, err)

	caseUnderTest := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(inputType, []types.Decimal256{value, third}, nil),
			NewFunctionTestInput(inputType, []types.Decimal256{value, value}, nil),
		},
		NewFunctionTestResult(resultType, false, []types.Decimal256{wantOne, wantThird}, nil),
		divFn,
	)
	passed, info := caseUnderTest.RunAndFree()
	require.True(t, passed, info)
}

func TestDecimalDivisionScaleCapBelowInputScale(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("decimal128", func(t *testing.T) {
		leftType := types.New(types.T_decimal128, 38, 37)
		rightType := types.New(types.T_decimal128, 1, 0)
		resultType := types.New(types.T_decimal128, 38, 30)
		left, err := types.ParseDecimal128("0.1000000000000000000000000000000000000", leftType.Width, leftType.Scale)
		require.NoError(t, err)
		right, err := types.ParseDecimal128("2", rightType.Width, rightType.Scale)
		require.NoError(t, err)
		want, err := types.ParseDecimal128("0.050000000000000000000000000000", resultType.Width, resultType.Scale)
		require.NoError(t, err)
		caseUnderTest := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(leftType, []types.Decimal128{left}, nil),
				NewFunctionTestInput(rightType, []types.Decimal128{right}, nil),
			},
			NewFunctionTestResult(resultType, false, []types.Decimal128{want}, nil),
			divFn,
		)
		defer caseUnderTest.Free()
		passed, info := caseUnderTest.Run()
		require.True(t, passed, info)
		require.Equal(t, resultType, *caseUnderTest.GetResultVectorDirectly().GetType())
	})

	t.Run("decimal256", func(t *testing.T) {
		leftType := types.New(types.T_decimal256, 65, 40)
		rightType := types.New(types.T_decimal256, 1, 0)
		resultType := types.New(types.T_decimal256, 65, 30)
		left, err := types.ParseDecimal256("0.1000000000000000000000000000000000000000", leftType.Width, leftType.Scale)
		require.NoError(t, err)
		right, err := types.ParseDecimal256("2", rightType.Width, rightType.Scale)
		require.NoError(t, err)
		want, err := types.ParseDecimal256("0.050000000000000000000000000000", resultType.Width, resultType.Scale)
		require.NoError(t, err)
		caseUnderTest := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(leftType, []types.Decimal256{left}, nil),
				NewFunctionTestInput(rightType, []types.Decimal256{right}, nil),
			},
			NewFunctionTestResult(resultType, false, []types.Decimal256{want}, nil),
			divFn,
		)
		defer caseUnderTest.Free()
		passed, info := caseUnderTest.Run()
		require.True(t, passed, info)
		require.Equal(t, resultType, *caseUnderTest.GetResultVectorDirectly().GetType())
	})
}

func TestDecimal256DivisionShiftBoundaries(t *testing.T) {
	denominator, err := types.ParseDecimal256("1"+strings.Repeat("0", 46), 47, 0)
	require.NoError(t, err)
	for _, test := range []struct {
		name, numerator, want string
	}{
		{"ordinary", "1" + strings.Repeat("0", 46), "1"},
		{"shifted numerator changes sign", "3" + strings.Repeat("0", 46), "3"},
		{"divisor alignment wraps", "28" + strings.Repeat("0", 45), "2.8"},
		{"scale multiplication overflows", "6" + strings.Repeat("0", 46), "6"},
	} {
		t.Run(test.name, func(t *testing.T) {
			numerator, err := types.ParseDecimal256(test.numerator, 47, 0)
			require.NoError(t, err)
			for _, signs := range []struct {
				negativeNumerator, negativeDenominator bool
			}{
				{false, false}, {true, false}, {false, true}, {true, true},
			} {
				x, y := numerator, denominator
				if signs.negativeNumerator {
					x = x.Minus()
				}
				if signs.negativeDenominator {
					y = y.Minus()
				}
				got := make([]types.Decimal256, 1)
				require.NoError(t, d256DivAtScale([]types.Decimal256{x}, []types.Decimal256{y},
					got, 0, 0, 30, mustNullsOK(nulls.NewWithSize(1, bitmapTestMP)), true))
				want := test.want
				if !strings.Contains(want, ".") {
					want += "."
				}
				want += strings.Repeat("0", 30-len(strings.SplitN(want, ".", 2)[1]))
				if signs.negativeNumerator != signs.negativeDenominator {
					want = "-" + want
				}
				require.Equal(t, want, got[0].Format(30), "signs=%+v", signs)
			}
		})
	}

	// A NULL row with a zero divisor must never enter the arithmetic loop.
	got := make([]types.Decimal256, 2)
	resultNulls := mustNullsOK(nulls.NewWithSize(2, bitmapTestMP))
	resultNulls.Add(1)
	x, err := types.ParseDecimal256("28"+strings.Repeat("0", 45), 47, 0)
	require.NoError(t, err)
	require.NoError(t, d256DivAtScale([]types.Decimal256{x, x},
		[]types.Decimal256{denominator, {}}, got, 0, 0, 30, resultNulls, true))
	require.Equal(t, "2.8"+strings.Repeat("0", 29), got[0].Format(30))
	require.True(t, resultNulls.Contains(1))
}

func TestDecimal256DivisionInternalHeadroom(t *testing.T) {
	// These physical coefficients straddle the conservative 2^253 boundary.
	// The reference computes the quotient and remainder with independent big.Int
	// operations rather than calling the production fallback.
	maxLow := ^uint64(0)
	denominator := types.Decimal256{B0_63: 3, B128_191: 1}
	denominatorBig, ok := new(big.Int).SetString(denominator.Format(0), 10)
	require.True(t, ok)
	for _, numerator := range []types.Decimal256{
		{B0_63: maxLow, B64_127: maxLow, B128_191: maxLow, B192_255: (1 << 61) - 1},
		{B192_255: 1 << 61},
		{B0_63: 1, B192_255: 1 << 61},
		{B192_255: 1 << 62},
	} {
		numeratorBig, ok := new(big.Int).SetString(numerator.Format(0), 10)
		require.True(t, ok)
		want, remainder := new(big.Int), new(big.Int)
		want.QuoRem(numeratorBig, denominatorBig, remainder)
		if new(big.Int).Lsh(remainder, 1).Cmp(denominatorBig) >= 0 {
			want.Add(want, big.NewInt(1))
		}
		var got types.Decimal256
		require.NoError(t, d256DivAdjusted(numerator, denominator, 0, false, &got))
		require.Equal(t, want.String(), got.Format(0), "numerator=%s", numerator.Format(0))
	}
}

func TestDecimalDivisionNegativeScaleRoundsOnce(t *testing.T) {
	for _, test := range []struct {
		coefficient int64
		want        string
	}{
		{9, "0"}, {10, "1"}, {11, "1"}, {-9, "0"}, {-10, "-1"}, {-11, "-1"},
	} {
		coefficient := test.coefficient
		if coefficient < 0 {
			coefficient = -coefficient
		}
		x := types.Decimal128{B0_63: uint64(coefficient)}
		if test.coefficient < 0 {
			x = x.Minus()
		}
		y := types.Decimal128{B0_63: 2}
		got128 := make([]types.Decimal128, 1)
		require.NoError(t, d128DivAtScale([]types.Decimal128{x}, []types.Decimal128{y},
			got128, 1, 0, 0, mustNullsOK(nulls.NewWithSize(1, bitmapTestMP)), true))
		require.Equal(t, test.want, got128[0].Format(0))
		x256 := types.Decimal256{B0_63: uint64(coefficient)}
		if test.coefficient < 0 {
			x256 = x256.Minus()
		}
		got256 := make([]types.Decimal256, 1)
		require.NoError(t, d256DivAtScale([]types.Decimal256{x256},
			[]types.Decimal256{{B0_63: 2}}, got256, 1, 0, 0, mustNullsOK(nulls.NewWithSize(1, bitmapTestMP)), true))
		require.Equal(t, test.want, got256[0].Format(0))
	}
}

func TestDecimalDivisionNegativeScaleWideFallback(t *testing.T) {
	// Multiplying this divisor by ten exceeds signed D128, while the rounded
	// quotient remains representable. The D256 fixed-width path must take over.
	x, err := types.ParseDecimal128("9"+strings.Repeat("0", 36), 38, 1)
	require.NoError(t, err)
	y, err := types.ParseDecimal128("18"+strings.Repeat("0", 36), 38, 0)
	require.NoError(t, err)
	got128 := make([]types.Decimal128, 1)
	require.NoError(t, d128DivAtScale([]types.Decimal128{x}, []types.Decimal128{y},
		got128, 1, 0, 0, mustNullsOK(nulls.NewWithSize(1, bitmapTestMP)), true))
	require.Equal(t, "1", got128[0].Format(0))

	// A genuine D256 numerator takes the generic branch, and a divisor
	// multiplication that exceeds D256 takes the bounded wide fallback.
	wideX, err := types.ParseDecimal256("3000000000", 47, 37)
	require.NoError(t, err)
	wideY, err := types.ParseDecimal256("1"+strings.Repeat("0", 20), 21, 0)
	require.NoError(t, err)
	got256 := make([]types.Decimal256, 1)
	require.NoError(t, d256DivAtScale([]types.Decimal256{wideX}, []types.Decimal256{wideY},
		got256, 37, 0, 30, mustNullsOK(nulls.NewWithSize(1, bitmapTestMP)), true))
	require.Equal(t, "0.00000000003"+strings.Repeat("0", 19), got256[0].Format(30))

	maxCoefficient := "1" + strings.Repeat("0", 64)
	hugeX, err := types.ParseDecimal256("1"+strings.Repeat("0", 14), 65, 50)
	require.NoError(t, err)
	hugeY, err := types.ParseDecimal256(maxCoefficient, 65, 0)
	require.NoError(t, err)
	require.NoError(t, d256DivAtScale([]types.Decimal256{hugeX}, []types.Decimal256{hugeY},
		got256, 50, 0, 30, mustNullsOK(nulls.NewWithSize(1, bitmapTestMP)), true))
	require.Equal(t, "0."+strings.Repeat("0", 30), got256[0].Format(30))
}

func TestDecimal256DivisionDeclaredPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)
	resultType := types.New(types.T_decimal256, 65, 0)
	physicalType := types.New(types.T_decimal256, 76, 0)
	for _, tc := range []struct {
		name                string
		leftType, rightType types.Type
		left, right, want   string
	}{
		{"legal operands overflow", types.New(types.T_decimal256, 38, 0), types.New(types.T_decimal256, 38, 30), "1" + strings.Repeat("0", 37), "0." + strings.Repeat("0", 29) + "1", ""},
		{"below bound", resultType, resultType, strings.Repeat("9", 65), "1", strings.Repeat("9", 65)},
		// These physical inputs isolate the precision guard and rounding carry;
		// their numerators exceed the precision of a legal SQL decimal type.
		{"at bound", physicalType, physicalType, "1" + strings.Repeat("0", 65), "1", ""},
		{"rounding crosses bound", physicalType, physicalType, "1" + strings.Repeat("9", 65), "2", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left, err := types.ParseDecimal256(tc.left, tc.leftType.Width, tc.leftType.Scale)
			require.NoError(t, err)
			right, err := types.ParseDecimal256(tc.right, tc.rightType.Width, tc.rightType.Scale)
			require.NoError(t, err)
			var want []types.Decimal256
			if tc.want != "" {
				value, err := types.ParseDecimal256(tc.want, resultType.Width, resultType.Scale)
				require.NoError(t, err)
				want = []types.Decimal256{value}
			}
			test := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(tc.leftType, []types.Decimal256{left}, nil),
				NewFunctionTestInput(tc.rightType, []types.Decimal256{right}, nil),
			}, NewFunctionTestResult(resultType, tc.want == "", want, nil), divFn)
			defer test.Free()
			if tc.want == "" {
				_, err = test.DebugRun()
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "error: %v", err)
			} else {
				passed, info := test.Run()
				require.True(t, passed, info)
				require.Equal(t, resultType, *test.GetResultVectorDirectly().GetType())
			}
		})
	}
}

func BenchmarkDecimalDivisionScaleCap(b *testing.B) {
	const rows = 1024
	x, err := types.ParseDecimal128("0.1", 38, 37)
	if err != nil {
		b.Fatal(err)
	}
	xs, ys, results := make([]types.Decimal128, rows), make([]types.Decimal128, rows), make([]types.Decimal128, rows)
	for i := range xs {
		xs[i], ys[i] = x, types.Decimal128{B0_63: 2}
	}
	resultNulls := mustNullsOK(nulls.NewWithSize(rows, bitmapTestMP))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := d128DivAtScale(xs, ys, results, 37, 0, 30, resultNulls, true); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecimal256DivisionScaleCap(b *testing.B) {
	const rows = 1024
	x, err := types.ParseDecimal256("0.1", 65, 40)
	if err != nil {
		b.Fatal(err)
	}
	xs, ys, results := make([]types.Decimal256, rows), make([]types.Decimal256, rows), make([]types.Decimal256, rows)
	for i := range xs {
		xs[i], ys[i] = x, types.Decimal256{B0_63: 2}
	}
	resultNulls := mustNullsOK(nulls.NewWithSize(rows, bitmapTestMP))
	for _, test := range []struct {
		name     string
		divisors []types.Decimal256
	}{
		{"vector", ys},
		{"constant", ys[:1]},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := d256DivAtScale(xs, test.divisors, results, 40, 0, 30, resultNulls, true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestExactDivisionFullTypeAndIncrementMatrix(t *testing.T) {
	exactInputs := []struct {
		name      string
		typ       types.Type
		precision int32
		scale     int32
	}{
		{"int8", types.T_int8.ToType(), 3, 0},
		{"int16", types.T_int16.ToType(), 5, 0},
		{"int32", types.T_int32.ToType(), 10, 0},
		{"int64", types.T_int64.ToType(), 19, 0},
		{"uint8", types.T_uint8.ToType(), 3, 0},
		{"uint16", types.T_uint16.ToType(), 5, 0},
		{"uint32", types.T_uint32.ToType(), 10, 0},
		{"uint64", types.T_uint64.ToType(), 20, 0},
		{"bool", types.T_bool.ToType(), 1, 0},
		{"bit", types.New(types.T_bit, 64, 0), 20, 0},
		{"date", types.T_date.ToType(), 8, 0},
		{"year", types.T_year.ToType(), 4, 0},
		{"time(0)", types.New(types.T_time, 0, 0), 14, 0},
		{"time(6)", types.New(types.T_time, 0, 6), 20, 6},
		{"datetime(0)", types.New(types.T_datetime, 0, 0), 14, 0},
		{"datetime(6)", types.New(types.T_datetime, 0, 6), 20, 6},
		{"timestamp(0)", types.New(types.T_timestamp, 0, 0), 14, 0},
		{"timestamp(6)", types.New(types.T_timestamp, 0, 6), 20, 6},
	}

	for _, left := range exactInputs {
		for _, right := range exactInputs {
			for _, inc := range []int32{0, 4, 10, 30} {
				wantScale := min32(left.scale+inc, maxDecimalDivisionScale)
				wantPrec := max32(min32(left.precision+right.scale+inc, maxDecimalDivisionPrecision), wantScale)
				wantOid := types.T_decimal128
				if wantPrec > 38 {
					wantOid = types.T_decimal256
				}
				ctx := WithDivPrecisionIncrement(context.Background(), inc)
				resolved, err := GetFunctionByName(ctx, "/", []types.Type{left.typ, right.typ})
				require.NoError(t, err, "%s / %s (inc=%d)", left.name, right.name, inc)
				require.Equal(t, types.New(wantOid, wantPrec, wantScale), resolved.GetReturnType(), "%s / %s (inc=%d)", left.name, right.name, inc)
				if targets, needCast := resolved.ShouldDoImplicitTypeCast(); needCast {
					require.True(t, IfTypeCastSupported(left.typ.Oid, targets[0].Oid), "cast %s -> %s", left.name, targets[0].Oid)
					require.True(t, IfTypeCastSupported(right.typ.Oid, targets[1].Oid), "cast %s -> %s", right.name, targets[1].Oid)
				}
			}
		}
	}

	// Exact / NULL and NULL / Exact promote to DECIMAL, while NULL / NULL stays DOUBLE.
	for _, exact := range []types.Type{types.T_int32.ToType(), types.T_int64.ToType(), types.T_bit.ToType(), types.T_bool.ToType(), types.T_year.ToType()} {
		for _, pair := range [][]types.Type{{exact, types.T_any.ToType()}, {types.T_any.ToType(), exact}} {
			resolved, err := GetFunctionByName(context.Background(), "/", pair)
			require.NoError(t, err)
			require.True(t, resolved.GetReturnType().Oid.IsDecimal(), "%v", pair)
		}
	}
	nullPair, err := GetFunctionByName(context.Background(), "/", []types.Type{types.T_any.ToType(), types.T_any.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_float64, nullPair.GetReturnType().Oid)

	// Approximate / string / JSON / ENUM / low-precision float / vector / BIT+float controls stay non-decimal.
	for _, pair := range [][]types.Type{
		{types.T_int64.ToType(), types.T_float32.ToType()},
		{types.T_int64.ToType(), types.T_float64.ToType()},
		{types.T_int64.ToType(), types.T_bf16.ToType()},
		{types.T_int64.ToType(), types.T_float16.ToType()},
		{types.T_int64.ToType(), types.T_float8.ToType()},
		{types.T_int64.ToType(), types.T_float4.ToType()},
		{types.T_int64.ToType(), types.T_varchar.ToType()},
		{types.T_int64.ToType(), types.T_json.ToType()},
		{types.New(types.T_bit, 64, 0), types.T_float64.ToType()},
	} {
		resolved, err := GetFunctionByName(context.Background(), "/", pair)
		require.NoError(t, err)
		require.Equal(t, types.T_float64, resolved.GetReturnType().Oid, "%v", pair)
	}
	// ENUM has no direct binary cast rule in fixedBinaryCastRule2; binder casts ENUM before `/`
	// (unchanged from fixedTypeCastRule2).
	_, enumErr := GetFunctionByName(context.Background(), "/", []types.Type{types.T_int64.ToType(), types.T_enum.ToType()})
	require.Error(t, enumErr)
	intDivPair, err := GetFunctionByName(context.Background(), "div", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_int64, intDivPair.GetReturnType().Oid)
	f32Pair, err := GetFunctionByName(context.Background(), "/", []types.Type{types.T_float32.ToType(), types.T_float32.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_float32, f32Pair.GetReturnType().Oid)

	vecPair, err := GetFunctionByName(context.Background(), "/", []types.Type{types.T_array_float32.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_array_float32, vecPair.GetReturnType().Oid)

	// Legacy float `/` era restores DOUBLE for int/int and year/year.
	legacyCtx := WithLegacyIntegerFloatDiv(context.Background())
	for _, pair := range [][]types.Type{
		{types.T_int64.ToType(), types.T_int64.ToType()},
		{types.T_year.ToType(), types.T_year.ToType()},
	} {
		legacyRes, err := GetFunctionByName(legacyCtx, "/", pair)
		require.NoError(t, err)
		require.Equal(t, types.T_float64, legacyRes.GetReturnType().Oid)
	}
}

func TestExactDivisionExtremeValuesAndDivByZero(t *testing.T) {
	proc := testutil.NewProcess(t)
	d128 := types.New(types.T_decimal128, 20, 0)
	res128 := types.New(types.T_decimal128, 24, 4)

	minInt64Dec, err := types.ParseDecimal128("-9223372036854775808", 20, 0)
	require.NoError(t, err)
	negOneDec, err := types.ParseDecimal128("-1", 20, 0)
	require.NoError(t, err)
	wantMinInt64DivNegOne, err := types.ParseDecimal128("9223372036854775808.0000", 24, 4)
	require.NoError(t, err)

	maxUint64Dec, err := types.ParseDecimal128("18446744073709551615", 20, 0)
	require.NoError(t, err)
	oneDec, err := types.ParseDecimal128("1", 20, 0)
	require.NoError(t, err)
	wantMaxUint64DivOne, err := types.ParseDecimal128("18446744073709551615.0000", 24, 4)
	require.NoError(t, err)

	zeroDec := types.Decimal128{}

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(d128, []types.Decimal128{minInt64Dec, maxUint64Dec}, nil),
			NewFunctionTestInput(d128, []types.Decimal128{negOneDec, oneDec}, nil),
		},
		NewFunctionTestResult(res128, false, []types.Decimal128{wantMinInt64DivNegOne, wantMaxUint64DivOne}, nil),
		divFn,
	)
	defer tc.Free()
	succeed, info := tc.Run()
	require.True(t, succeed, info)

	// Division by zero in SELECT mode returns NULL + warning (no error); NULL / 0 returns NULL without warning.
	selectProc := testutil.NewProcess(t)
	selectProc.Base.DivByZeroErrorMode = 0
	divZeroCase := NewFunctionTestCase(selectProc,
		[]FunctionTestInput{
			NewFunctionTestInput(d128, []types.Decimal128{oneDec, oneDec}, []bool{false, true}),
			NewFunctionTestInput(d128, []types.Decimal128{zeroDec, zeroDec}, nil),
		},
		NewFunctionTestResult(res128, false, []types.Decimal128{{}, {}}, []bool{true, true}),
		divFn,
	)
	defer divZeroCase.Free()
	succeed, info = divZeroCase.Run()
	require.True(t, succeed, info)

	// Verify NULL / 0 alone does not error and 1 / 0 in strict DML mode returns ErrDivByZero.
	strictProc := testutil.NewProcess(t)
	strictProc.Base.DivByZeroErrorMode = 1
	strictCase := NewFunctionTestCase(strictProc,
		[]FunctionTestInput{
			NewFunctionTestInput(d128, []types.Decimal128{oneDec}, nil),
			NewFunctionTestInput(d128, []types.Decimal128{zeroDec}, nil),
		},
		NewFunctionTestResult(res128, true, []types.Decimal128{{}}, nil),
		divFn,
	)
	defer strictCase.Free()
	_, execErr := strictCase.DebugRun()
	require.Error(t, execErr)
	require.True(t, moerr.IsMoErrCode(execErr, moerr.ErrDivByZero), "got: %v", execErr)

	nullDivZeroStrict := NewFunctionTestCase(strictProc,
		[]FunctionTestInput{
			NewFunctionTestInput(d128, []types.Decimal128{oneDec}, []bool{true}),
			NewFunctionTestInput(d128, []types.Decimal128{zeroDec}, nil),
		},
		NewFunctionTestResult(res128, false, []types.Decimal128{{}}, []bool{true}),
		divFn,
	)
	defer nullDivZeroStrict.Free()
	succeed, info = nullDivZeroStrict.Run()
	require.True(t, succeed, info)
}

func TestTemporalCastToDecimal256Execution(t *testing.T) {
	proc := testutil.NewProcess(t)
	d256Scale0 := types.New(types.T_decimal256, 38, 0)
	d256Scale6 := types.New(types.T_decimal256, 38, 6)

	dateVal := types.DateFromCalendar(2024, 1, 2)
	wantDate, err := types.ParseDecimal256("20240102", 38, 0)
	require.NoError(t, err)
	dateCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_date.ToType(), []types.Date{dateVal}, nil),
			NewFunctionTestInput(d256Scale0, []types.Decimal256{}, nil),
		},
		NewFunctionTestResult(d256Scale0, false, []types.Decimal256{wantDate}, nil),
		NewCast,
	)
	defer dateCase.Free()
	succeed, info := dateCase.Run()
	require.True(t, succeed, info)

	yearVal := types.MoYear(2024)
	wantYear, err := types.ParseDecimal256("2024", 38, 0)
	require.NoError(t, err)
	yearCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{yearVal}, nil),
			NewFunctionTestInput(d256Scale0, []types.Decimal256{}, nil),
		},
		NewFunctionTestResult(d256Scale0, false, []types.Decimal256{wantYear}, nil),
		NewCast,
	)
	defer yearCase.Free()
	succeed, info = yearCase.Run()
	require.True(t, succeed, info)

	timeVal := types.TimeFromClock(false, 12, 34, 56, 789000)
	wantTime, err := types.ParseDecimal256("123456.789000", 38, 6)
	require.NoError(t, err)
	timeCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_time, 0, 6), []types.Time{timeVal}, nil),
			NewFunctionTestInput(d256Scale6, []types.Decimal256{}, nil),
		},
		NewFunctionTestResult(d256Scale6, false, []types.Decimal256{wantTime}, nil),
		NewCast,
	)
	defer timeCase.Free()
	succeed, info = timeCase.Run()
	require.True(t, succeed, info)

	dtVal := types.DatetimeFromClock(2024, 1, 2, 12, 34, 56, 789000)
	wantDT, err := types.ParseDecimal256("20240102123456.789000", 38, 6)
	require.NoError(t, err)
	dtCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_datetime, 0, 6), []types.Datetime{dtVal}, nil),
			NewFunctionTestInput(d256Scale6, []types.Decimal256{}, nil),
		},
		NewFunctionTestResult(d256Scale6, false, []types.Decimal256{wantDT}, nil),
		NewCast,
	)
	defer dtCase.Free()
	succeed, info = dtCase.Run()
	require.True(t, succeed, info)

	tsVal := dtVal.ToTimestamp(proc.GetSessionInfo().TimeZone)
	tsCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_timestamp, 0, 6), []types.Timestamp{tsVal}, nil),
			NewFunctionTestInput(d256Scale6, []types.Decimal256{}, nil),
		},
		NewFunctionTestResult(d256Scale6, false, []types.Decimal256{wantDT}, nil),
		NewCast,
	)
	defer tsCase.Free()
	succeed, info = tsCase.Run()
	require.True(t, succeed, info)

	// High-scale DECIMAL(65, 30) must succeed without overflowing Decimal128(38, 30).
	d256Wide := types.New(types.T_decimal256, 65, 30)
	wantDateWide, err := types.ParseDecimal256("20240102", 65, 30)
	require.NoError(t, err)
	wantYearWide, err := types.ParseDecimal256("2024", 65, 30)
	require.NoError(t, err)
	wantTimeWide, err := types.ParseDecimal256("123456.789000", 65, 30)
	require.NoError(t, err)
	wantDTWide, err := types.ParseDecimal256("20240102123456.789000", 65, 30)
	require.NoError(t, err)

	for _, tc := range []struct {
		name  string
		input FunctionTestInput
		want  types.Decimal256
	}{
		{"date", NewFunctionTestInput(types.T_date.ToType(), []types.Date{dateVal}, nil), wantDateWide},
		{"year", NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{yearVal}, nil), wantYearWide},
		{"time", NewFunctionTestInput(types.New(types.T_time, 0, 6), []types.Time{timeVal}, nil), wantTimeWide},
		{"datetime", NewFunctionTestInput(types.New(types.T_datetime, 0, 6), []types.Datetime{dtVal}, nil), wantDTWide},
		{"timestamp", NewFunctionTestInput(types.New(types.T_timestamp, 0, 6), []types.Timestamp{tsVal}, nil), wantDTWide},
	} {
		t.Run("DECIMAL(65,30)/"+tc.name, func(t *testing.T) {
			c := NewFunctionTestCase(proc,
				[]FunctionTestInput{tc.input, NewFunctionTestInput(d256Wide, []types.Decimal256{}, nil)},
				NewFunctionTestResult(d256Wide, false, []types.Decimal256{tc.want}, nil),
				NewCast,
			)
			defer c.Free()
			ok, msg := c.Run()
			require.True(t, ok, msg)
		})
	}

	// Width overflow must error on unmasked rows, and selectList must skip masked rows.
	narrowD256 := types.New(types.T_decimal256, 5, 0)
	overflowTime := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_time, 0, 0), []types.Time{types.TimeFromClock(false, 12, 34, 56, 0)}, nil),
			NewFunctionTestInput(narrowD256, []types.Decimal256{}, nil),
		},
		NewFunctionTestResult(narrowD256, true, []types.Decimal256{{}}, nil),
		NewCast,
	)
	defer overflowTime.Free()
	_, timeErr := overflowTime.DebugRun()
	require.Error(t, timeErr)

	overflowDT := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_datetime, 0, 0), []types.Datetime{dtVal}, nil),
			NewFunctionTestInput(narrowD256, []types.Decimal256{}, nil),
		},
		NewFunctionTestResult(narrowD256, true, []types.Decimal256{{}}, nil),
		NewCast,
	)
	defer overflowDT.Free()
	_, dtErr := overflowDT.DebugRun()
	require.Error(t, dtErr)

	maskedDT := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_datetime, 0, 0), []types.Datetime{dtVal, dtVal}, []bool{true, false}),
			NewFunctionTestInput(narrowD256, []types.Decimal256{}, nil),
		},
		NewFunctionTestResult(narrowD256, false, []types.Decimal256{{}, {}}, []bool{true, true}),
		NewCast,
	).WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}})
	defer maskedDT.Free()
	succeed, info = maskedDT.Run()
	require.True(t, succeed, info)
}

func TestTemporalExplicitCastToDecimal256ClampsWithWarning(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC
	target39_30 := types.New(types.T_decimal256, 39, 30)
	clamped39_30, err := clampDecimal256Value(false, 39, 30)
	require.NoError(t, err)

	dtVal := types.DatetimeFromClock(2024, 1, 2, 3, 4, 5, 0)
	dateVal := types.DateFromCalendar(2024, 1, 2)
	tsVal := dtVal.ToTimestamp(time.UTC)
	timeVal := types.TimeFromClock(false, 12, 34, 56, 0)
	yearVal := types.MoYear(2024)

	// DATETIME(20240102030405 = 14 integer digits) and TIMESTAMP exceed DECIMAL(39,30) (9 integer digits).
	for _, tc := range []struct {
		name  string
		input FunctionTestInput
	}{
		{"datetime", NewFunctionTestInput(types.New(types.T_datetime, 0, 0), []types.Datetime{dtVal}, nil)},
		{"timestamp", NewFunctionTestInput(types.New(types.T_timestamp, 0, 0), []types.Timestamp{tsVal}, nil)},
	} {
		t.Run("explicit/"+tc.name, func(t *testing.T) {
			session := &numericWarningSession{}
			proc.Session = session
			c := NewFunctionTestCase(proc,
				[]FunctionTestInput{tc.input, NewFunctionTestInput(target39_30, []types.Decimal256{}, nil)},
				NewFunctionTestResult(target39_30, false, []types.Decimal256{clamped39_30}, nil),
				NewExplicitCast,
			)
			defer c.Free()
			ok, msg := c.Run()
			require.True(t, ok, msg)
			require.Len(t, session.warnings, 1)
			require.Equal(t, moerr.ER_WARN_DATA_OUT_OF_RANGE, session.warnings[0].code)
		})
		t.Run("implicit/"+tc.name, func(t *testing.T) {
			c := NewFunctionTestCase(proc,
				[]FunctionTestInput{tc.input, NewFunctionTestInput(target39_30, []types.Decimal256{}, nil)},
				NewFunctionTestResult(target39_30, true, []types.Decimal256{{}}, nil),
				NewCast,
			)
			defer c.Free()
			_, castErr := c.DebugRun()
			require.Error(t, castErr)
		})
	}

	// DATE(8 digits), TIME(6 digits), YEAR(4 digits) overflow DECIMAL(39,36) (3 integer digits).
	target39_36 := types.New(types.T_decimal256, 39, 36)
	clamped39_36, err := clampDecimal256Value(false, 39, 36)
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		input FunctionTestInput
	}{
		{"date", NewFunctionTestInput(types.T_date.ToType(), []types.Date{dateVal}, nil)},
		{"time", NewFunctionTestInput(types.New(types.T_time, 0, 0), []types.Time{timeVal}, nil)},
		{"year", NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{yearVal}, nil)},
	} {
		t.Run("explicit/"+tc.name, func(t *testing.T) {
			session := &numericWarningSession{}
			proc.Session = session
			c := NewFunctionTestCase(proc,
				[]FunctionTestInput{tc.input, NewFunctionTestInput(target39_36, []types.Decimal256{}, nil)},
				NewFunctionTestResult(target39_36, false, []types.Decimal256{clamped39_36}, nil),
				NewExplicitCast,
			)
			defer c.Free()
			ok, msg := c.Run()
			require.True(t, ok, msg)
			require.Len(t, session.warnings, 1)
			require.Equal(t, moerr.ER_WARN_DATA_OUT_OF_RANGE, session.warnings[0].code)
		})
	}
}

func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}
