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

package types

import (
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestDecimalPrecisionCheckedOwner(t *testing.T) {
	for _, tc := range []struct {
		x, ceil, floor, round int64
	}{
		{105, 110, 100, 110}, {-105, -100, -110, -110},
		{100, 100, 100, 100}, {-100, -100, -100, -100}, {0, 0, 0, 0},
	} {
		for _, constantScale := range []bool{false, true} {
			divisor := int64(1)
			if constantScale {
				divisor = 10
			}
			x64 := Decimal64(uint64(tc.x))
			x128 := Decimal128FromInt64(tc.x)
			require.Equal(t, Decimal64(uint64(tc.ceil/divisor)), x64.Ceil(2, 1, constantScale))
			require.Equal(t, Decimal64(uint64(tc.floor/divisor)), x64.Floor(2, 1, constantScale))
			require.Equal(t, Decimal64(uint64(tc.round/divisor)), x64.Round(2, 1, constantScale))
			require.Equal(t, Decimal128FromInt64(tc.ceil/divisor), x128.Ceil(2, 1, constantScale))
			require.Equal(t, Decimal128FromInt64(tc.floor/divisor), x128.Floor(2, 1, constantScale))
			require.Equal(t, Decimal128FromInt64(tc.round/divisor), x128.Round(2, 1, constantScale))
		}
	}
	for _, constantScale := range []bool{false, true} {
		for _, x := range []Decimal64{Decimal64Min, Decimal64Max} {
			require.Equal(t, x, x.Ceil(0, 0, constantScale))
			require.Equal(t, x, x.Floor(0, 0, constantScale))
			require.Equal(t, x, x.Round(0, 0, constantScale))
		}
		for _, x := range []Decimal128{Decimal128Min, Decimal128Max} {
			require.Equal(t, x, x.Ceil(0, 0, constantScale))
			require.Equal(t, x, x.Floor(0, 0, constantScale))
			require.Equal(t, x, x.Round(0, 0, constantScale))
		}
		for _, operation := range []func(){
			func() { Decimal64Min.Ceil(0, -1, constantScale) },
			func() { Decimal64Min.Floor(0, -1, constantScale) },
			func() { Decimal128Min.Ceil(0, -1, constantScale) },
			func() { Decimal128Min.Floor(0, -1, constantScale) },
			func() { Decimal64Max.Round(0, -1, constantScale) },
			func() { Decimal64Max.Minus().Round(0, -1, constantScale) },
			func() { Decimal128Max.Round(0, -1, constantScale) },
			func() { Decimal128Max.Minus().Round(0, -1, constantScale) },
			func() { Decimal64Max.Ceil(0, -1, constantScale) },
			func() { Decimal64Max.Minus().Floor(0, -1, constantScale) },
			func() { Decimal128Max.Ceil(0, -1, constantScale) },
			func() { Decimal128Max.Minus().Floor(0, -1, constantScale) },
		} {
			var failure any
			func() { defer func() { failure = recover() }(); operation() }()
			err, ok := failure.(error)
			require.True(t, ok, "quantizer must reject a nonrepresentable result")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange))
		}
	}
}

func TestParseDecimalRejectsBothPrecisionEndpoints(t *testing.T) {
	for _, test := range []struct {
		name     string
		parse    func(string) error
		boundary string
		overflow string
	}{
		{
			name: "decimal64",
			parse: func(value string) error {
				_, err := ParseDecimal64(value, 5, 2)
				return err
			},
			boundary: "999.99",
			overflow: "1000.00",
		},
		{
			name: "decimal128",
			parse: func(value string) error {
				_, err := ParseDecimal128(value, 19, 2)
				return err
			},
			boundary: "99999999999999999.99",
			overflow: "100000000000000000.00",
		},
		{
			name: "decimal256",
			parse: func(value string) error {
				_, err := ParseDecimal256(value, 40, 2)
				return err
			},
			boundary: "99999999999999999999999999999999999999.99",
			overflow: "100000000000000000000000000000000000000.00",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, test.parse(test.boundary))
			require.NoError(t, test.parse("-"+test.boundary))
			require.Error(t, test.parse(test.overflow))
			require.Error(t, test.parse("-"+test.overflow))
		})
	}
}

func TestParse64(t *testing.T) {
	x, y := ParseDecimal64("99999.99999999999999999999999999999999", 12, 6)
	if y != nil || x != 100000000000 {
		panic("Decimal64Parse wrong")
	}
}

// TestParse64ScientificNotation tests parsing decimal with scientific notation including + sign
func TestParse64ScientificNotation(t *testing.T) {
	// Test case 1: e+06 format (the bug case from issue #22396)
	x1, err1 := ParseDecimal64("1.23456789e+06", 10, 2)
	if err1 != nil {
		t.Errorf("Failed to parse '1.23456789e+06': %v", err1)
	}
	expected1 := Decimal64(123456789) // 1234567.89 in scale 2
	if x1 != expected1 {
		t.Errorf("ParseDecimal64('1.23456789e+06', 10, 2) = %v, expected %v", x1, expected1)
	}

	// Test case 2: e6 format (should also work)
	x2, err2 := ParseDecimal64("1.23456789e6", 10, 2)
	if err2 != nil {
		t.Errorf("Failed to parse '1.23456789e6': %v", err2)
	}
	if x2 != expected1 {
		t.Errorf("ParseDecimal64('1.23456789e6', 10, 2) = %v, expected %v", x2, expected1)
	}

	// Test case 3: e-06 format (negative exponent)
	x3, err3 := ParseDecimal64("1.23456789e-06", 10, 8)
	if err3 != nil {
		t.Errorf("Failed to parse '1.23456789e-06': %v", err3)
	}
	expected3 := Decimal64(123) // 0.00000123 in scale 8
	if x3 != expected3 {
		t.Errorf("ParseDecimal64('1.23456789e-06', 10, 8) = %v, expected %v", x3, expected3)
	}

	// Test case 4: e+2 format (small positive exponent)
	x4, err4 := ParseDecimal64("12.34e+2", 10, 2)
	if err4 != nil {
		t.Errorf("Failed to parse '12.34e+2': %v", err4)
	}
	expected4 := Decimal64(123400) // 1234.00 in scale 2
	if x4 != expected4 {
		t.Errorf("ParseDecimal64('12.34e+2', 10, 2) = %v, expected %v", x4, expected4)
	}
}

func TestParse128(t *testing.T) {
	x, y := ParseDecimal128("99999.999999999999999999999999999999999", 12, 6)
	if y != nil || x.B0_63 != 100000000000 {
		panic("Decimal128Parse wrong")
	}
}

func TestParse256(t *testing.T) {
	x, err := ParseDecimal256("12345678901234567890123456789012345.123456789012345678901234567890", 65, 30)
	if err != nil {
		t.Fatalf("ParseDecimal256 failed: %v", err)
	}
	if got := x.Format(30); got != "12345678901234567890123456789012345.123456789012345678901234567890" {
		t.Fatalf("unexpected decimal256 format: %s", got)
	}
}

func TestDecimalModScaleAlignmentOverflow(t *testing.T) {
	max64 := Decimal64(^uint64(0) >> 1)
	max128 := Decimal128{B0_63: ^uint64(0), B64_127: 0x7FFFFFFFFFFFFFFF}
	max256 := Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 0x7FFFFFFFFFFFFFFF}
	// Each signed maximum is 2^odd-1: multiplying by 10^18 leaves remainder 1 modulo 3.
	for _, tc := range []struct{ divisor, want uint64 }{{1, 0}, {3, 1}} {
		t.Run(fmt.Sprintf("wide_coefficient_divisor_%d", tc.divisor), func(t *testing.T) {
			got64, scale, err := max64.Mod(Decimal64(tc.divisor), 0, 18)
			require.NoError(t, err)
			require.Equal(t, int32(18), scale)
			require.Equal(t, Decimal64(tc.want), got64)
			got128, scale, err := max128.Mod(Decimal128{B0_63: tc.divisor}, 0, 18)
			require.NoError(t, err)
			require.Equal(t, int32(18), scale)
			require.Equal(t, Decimal128{B0_63: tc.want}, got128)
			got256, scale, err := max256.Mod(Decimal256{B0_63: tc.divisor}, 0, 18)
			require.NoError(t, err)
			require.Equal(t, int32(18), scale)
			require.Equal(t, Decimal256{B0_63: tc.want}, got256)
		})
	}

	maxCoefficient := new(big.Int).Sub(
		new(big.Int).Exp(big.NewInt(10), big.NewInt(65), nil), big.NewInt(1))
	maximum, err := ParseDecimal256(maxCoefficient.String(), 65, 0)
	require.NoError(t, err)
	seven := Decimal256FromInt64(7)

	for _, scaleDiff := range []int32{11, 12, 30, 65} {
		t.Run(fmt.Sprintf("scale_diff_%d", scaleDiff), func(t *testing.T) {
			factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
			wantMagnitude := new(big.Int).Mul(maxCoefficient, factor)
			wantMagnitude.Mod(wantMagnitude, big.NewInt(7))

			for _, signDividend := range []bool{false, true} {
				for _, signDivisor := range []bool{false, true} {
					dividend, divisor := maximum, seven
					if signDividend {
						dividend = dividend.Minus()
					}
					if signDivisor {
						divisor = divisor.Minus()
					}
					got, resultScale, err := dividend.Mod(divisor, 0, scaleDiff)
					require.NoError(t, err)
					require.Equal(t, scaleDiff, resultScale)

					want := new(big.Int).Set(wantMagnitude)
					if signDividend {
						want.Neg(want)
					}
					wantDecimal := Decimal256FromInt64(want.Int64())
					require.Equal(t, wantDecimal, got)
				}
			}
		})
	}

	// Scaling the divisor overflows, but it is mathematically larger than the
	// small dividend, so the remainder remains the dividend at the common scale.
	for _, signDividend := range []bool{false, true} {
		for _, signDivisor := range []bool{false, true} {
			dividend, divisor := seven, maximum
			if signDividend {
				dividend = dividend.Minus()
			}
			if signDivisor {
				divisor = divisor.Minus()
			}
			got, resultScale, err := dividend.Mod(divisor, 30, 0)
			require.NoError(t, err)
			require.Equal(t, int32(30), resultScale)
			want := int64(7)
			if signDividend {
				want = -want
			}
			require.Equal(t, Decimal256FromInt64(want), got)
		}
	}

	minimum := Decimal256{B192_255: uint64(1) << 63}
	got, _, err := minimum.Mod(seven, 77, 0)
	require.NoError(t, err)
	require.Equal(t, minimum, got)
}

func TestParse256LargePositiveExponentReturnsErrorWithoutPanic(t *testing.T) {
	var err error
	require.NotPanics(t, func() {
		_, _, err = Parse256("1e100")
	})
	require.Error(t, err)
}

func TestDecimal256ExtremeScaleDoesNotPanic(t *testing.T) {
	value := Decimal256FromInt64(1)
	require.NotPanics(t, func() {
		_, _ = value.Scale(math.MinInt32)
		_, _ = value.Scale(math.MaxInt32)
	})
}

func TestDecimal256ScaleNegativeSeventySevenPreservesRounding(t *testing.T) {
	maximum := Decimal256{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxInt64}
	positive, err := maximum.Scale(-77)
	require.NoError(t, err)
	require.Equal(t, Decimal256FromInt64(1), positive)

	negative, err := maximum.Minus().Scale(-77)
	require.NoError(t, err)
	require.Equal(t, Decimal256FromInt64(-1), negative)
}

func TestDecimal128ScaleMultiStepRoundsOnlyFinalChunk(t *testing.T) {
	for _, tc := range []struct {
		input         string
		want          string
		wantTruncated string
	}{
		{"1.499999999999999999999999999999", "1", "1"},
		{"-1.499999999999999999999999999999", "-1", "-1"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			x, err := ParseDecimal128(tc.input, 38, 30)
			require.NoError(t, err)

			got, err := x.Scale(-30)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Format(0))

			got, err = x.ScaleTruncate(-30)
			require.NoError(t, err)
			require.Equal(t, tc.wantTruncated, got.Format(0))

			inplace := x
			require.NoError(t, inplace.ScaleInplace(-30))
			require.Equal(t, tc.want, inplace.Format(0))
		})
	}
}

func TestDecimal128ScaleMinimumAndExtremeNegativeScale(t *testing.T) {
	got, err := Decimal128Min.Scale(-38)
	require.NoError(t, err)
	require.Equal(t, "-2", got.Format(0))

	got, err = Decimal128Min.ScaleTruncate(-38)
	require.NoError(t, err)
	require.Equal(t, "-1", got.Format(0))

	got, err = Decimal128Min.Scale(math.MinInt32)
	require.NoError(t, err)
	require.Equal(t, "0", got.Format(0))
	got, err = Decimal128Min.ScaleTruncate(math.MinInt32)
	require.NoError(t, err)
	require.Equal(t, "0", got.Format(0))

	inplace := Decimal128Min
	require.NoError(t, inplace.ScaleInplace(math.MinInt32))
	require.Equal(t, Decimal128{}, inplace)
}

func TestDecimal256ScaleMultiStepRoundingAndMinimum(t *testing.T) {
	for _, tc := range []struct {
		input         string
		want          string
		wantTruncated string
	}{
		{"1.499999999999999999999999999999", "1", "1"},
		{"-1.499999999999999999999999999999", "-1", "-1"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			x, err := ParseDecimal256(tc.input, 65, 30)
			require.NoError(t, err)

			got, err := x.Scale(-30)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Format(0))

			got, err = x.ScaleTruncate(-30)
			require.NoError(t, err)
			require.Equal(t, tc.wantTruncated, got.Format(0))
		})
	}

	minimum := Decimal256{B192_255: uint64(1) << 63}
	got, err := minimum.Scale(-77)
	require.NoError(t, err)
	require.Equal(t, "-1", got.Format(0))
	got, err = minimum.ScaleTruncate(-77)
	require.NoError(t, err)
	require.Equal(t, "0", got.Format(0))

	// The signed minimum is 2^255 in magnitude. Verify a nonzero multi-chunk
	// truncation against an independent arbitrary-precision quotient.
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	want := new(big.Int).Quo(new(big.Int).Lsh(big.NewInt(1), 255), divisor)
	want.Neg(want)
	got, err = minimum.ScaleTruncate(-38)
	require.NoError(t, err)
	require.Equal(t, want.String(), got.Format(0))

	got, err = minimum.ScaleTruncate(math.MinInt32)
	require.NoError(t, err)
	require.Equal(t, "0", got.Format(0))
}

func TestParse256ExponentOverflowReturnsErrorWithoutPanic(t *testing.T) {
	for _, input := range []string{"1e2147483648", "1e-2147483648"} {
		t.Run(input, func(t *testing.T) {
			var err error
			require.NotPanics(t, func() {
				_, _, err = Parse256(input)
			})
			require.Error(t, err)
		})
	}
}

func TestDecimal256Format(t *testing.T) {
	cases := []struct {
		name  string
		input string
		width int32
		scale int32
		want  string
	}{
		{
			name:  "zero integer",
			input: "0",
			width: 65,
			scale: 0,
			want:  "0",
		},
		{
			name:  "zero fraction",
			input: "0",
			width: 65,
			scale: 4,
			want:  "0.0000",
		},
		{
			name:  "leading fractional zeros",
			input: "0.0012",
			width: 65,
			scale: 4,
			want:  "0.0012",
		},
		{
			name:  "negative fraction",
			input: "-123.4500",
			width: 65,
			scale: 4,
			want:  "-123.4500",
		},
		{
			name:  "large integer",
			input: "12345678901234567890123456789012345678901234567890123456789012345",
			width: 65,
			scale: 0,
			want:  "12345678901234567890123456789012345678901234567890123456789012345",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			x, err := ParseDecimal256(tc.input, tc.width, tc.scale)
			if err != nil {
				t.Fatalf("ParseDecimal256(%q) failed: %v", tc.input, err)
			}
			if got := x.Format(tc.scale); got != tc.want {
				t.Fatalf("Format(%q, scale=%d) = %q, want %q", tc.input, tc.scale, got, tc.want)
			}
		})
	}
}

func TestDecimal256ToFloat64NegativeScale(t *testing.T) {
	x := Decimal256FromInt64(123)
	if got := Decimal256ToFloat64(x, -2); got != 12300 {
		t.Fatalf("unexpected Decimal256ToFloat64 result: %v", got)
	}
}

func TestDecimal256ToFloat64PreservesLow128SignBit(t *testing.T) {
	tests := []struct {
		name  string
		value Decimal256
		scale int32
		want  float64
	}{
		{
			name:  "2^127-1",
			value: Decimal256{B0_63: ^uint64(0), B64_127: 1<<63 - 1},
			want:  math.Ldexp(1, 127) - 1,
		},
		{
			name:  "2^127",
			value: Decimal256{B64_127: 1 << 63},
			want:  math.Ldexp(1, 127),
		},
		{
			name:  "3*2^126",
			value: Decimal256{B64_127: 3 << 62},
			want:  3 * math.Ldexp(1, 126),
		},
		{
			name:  "2^127 scaled",
			value: Decimal256{B64_127: 1 << 63},
			scale: 30,
			want:  math.Ldexp(1, 127) / 1e30,
		},
		{
			name:  "2^128",
			value: Decimal256{B128_191: 1},
			want:  math.Ldexp(1, 128),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Decimal256ToFloat64(tc.value, tc.scale)
			require.InEpsilon(t, tc.want, got, 1e-15)

			negative := tc.value.Minus()
			gotNegative := Decimal256ToFloat64(negative, tc.scale)
			require.InEpsilon(t, -tc.want, gotNegative, 1e-15)
		})
	}
}

func TestCompare64(t *testing.T) {
	x := Decimal64(0)
	y := ^x
	if CompareDecimal64(x, y) != 1 {
		panic("CompareDecimal64 wrong")
	}
}

func TestCompareDecimal64WithScaleFallsBackOnSignedOverflow(t *testing.T) {
	positive, err := ParseDecimal64("9.99999999999999998", 18, 17)
	require.NoError(t, err)
	negative, err := ParseDecimal64("-9.99999999999999998", 18, 17)
	require.NoError(t, err)
	positiveBound, err := ParseDecimal64("100", 18, 0)
	require.NoError(t, err)
	negativeBound, err := ParseDecimal64("-100", 18, 0)
	require.NoError(t, err)

	require.Less(t, CompareDecimal64WithScale(positive, positiveBound, 17, 0), 0)
	require.Greater(t, CompareDecimal64WithScale(negative, negativeBound, 17, 0), 0)
	require.Greater(t, CompareDecimal64WithScale(positiveBound, positive, 0, 17), 0)
	require.Less(t, CompareDecimal64WithScale(negativeBound, negative, 0, 17), 0)
}

func TestCompareDecimal64WithScaleFastPaths(t *testing.T) {
	negative := Decimal64(1).Minus()

	require.Less(t, CompareDecimal64WithScale(Decimal64(1), Decimal64(2), 2, 2), 0)
	require.Less(t, CompareDecimal64WithScale(negative, Decimal64(1), 1, 2), 0)
	require.Greater(t, CompareDecimal64WithScale(Decimal64(1), negative, 2, 1), 0)
	require.Zero(t, CompareDecimal64WithScale(Decimal64(12), Decimal64(120), 1, 2))
	require.Zero(t, CompareDecimal64WithScale(Decimal64(120), Decimal64(12), 2, 1))
}

func TestCompare128(t *testing.T) {
	x := Decimal128{0, 0}
	y := Decimal128{^x.B0_63, ^x.B64_127}
	if CompareDecimal128(x, y) != 1 {
		panic("CompareDecimal128 wrong")
	}
}

func TestCompare256(t *testing.T) {
	x := Decimal256{0, 0, 0, 0}
	y := Decimal256{^x.B0_63, ^x.B64_127, ^x.B128_191, ^x.B192_255}
	if CompareDecimal256(x, y) != 1 {
		panic("CompareDecimal256 wrong")
	}
}

func TestDecimalToFloat64(t *testing.T) {
	// Binary floating point is lossy; forward conversion is not a reversible quantizer.
	for _, tc := range []struct {
		name      string
		value     Decimal128
		scale     int32
		want      uint64
		decimal64 bool
	}{
		{"zero", Decimal128{}, 15, 0, true},
		{"positive fraction", Decimal128{125000000000000, 0}, 15, 0x3fc0000000000000, true},
		{"negative fraction", Decimal128{18446619073709551616, 0xffffffffffffffff}, 15, 0xbfc0000000000000, true},
		{"negative scale", Decimal128{125, 0}, -1, 0x4093880000000000, true},
		{"integer tie", Decimal128{9007199254740993, 0}, 0, 0x4340000000000000, true},
		{"high word", Decimal128{0, 1}, 0, 0x43f0000000000000, false},
		{"scale chunk boundary", Decimal128{10000000000000000000, 0}, 19, 0x3ff0000000000000, false},
		{"scale chunk continuation", Decimal128{7766279631452241920, 5}, 20, 0x3ff0000000000000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, math.Float64bits(Decimal128ToFloat64(tc.value, tc.scale)))
			if tc.decimal64 {
				require.Equal(t, tc.want, math.Float64bits(Decimal64ToFloat64(Decimal64(tc.value.B0_63), tc.scale)))
			}
		})
	}
	converted, err := Decimal64FromFloat64(0.125, 18, 5)
	require.NoError(t, err)
	require.Equal(t, Decimal64(12500), converted)
}

func TestDecimal128FromFloat64PreservesScaledIntegerPrecision(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		scale int32
		want  string
	}{
		{name: "positive scale 0", value: 1000000000001, scale: 0, want: "1000000000001"},
		{name: "positive scale 3", value: 1000000000001, scale: 3, want: "1000000000001.000"},
		{name: "positive scale 5", value: 1000000000001, scale: 5, want: "1000000000001.00000"},
		{name: "positive scale 6", value: 1000000000001, scale: 6, want: "1000000000001.000000"},
		{name: "positive scale 7", value: 1000000000001, scale: 7, want: "1000000000001.0000000"},
		{name: "negative scale 6", value: -1000000000001, scale: 6, want: "-1000000000001.000000"},
		{name: "adjacent lower integer", value: 1000000000000, scale: 6, want: "1000000000000.000000"},
		{name: "adjacent higher integer", value: 1000000000002, scale: 6, want: "1000000000002.000000"},
		{name: "2^53 minus 1", value: math.Exp2(53) - 1, scale: 6, want: "9007199254740991.000000"},
		{name: "2^53", value: math.Exp2(53), scale: 6, want: "9007199254740992.000000"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := Decimal128FromFloat64(test.value, 38, test.scale)
			require.NoError(t, err)
			require.Equal(t, test.want, result.Format(test.scale))
		})
	}
}

func TestDecimal128FromFloat64IntegralBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		want  string
	}{
		{name: "positive 2^63", value: math.Ldexp(1, 63), want: "9223372036854775808"},
		{name: "negative 2^63", value: -math.Ldexp(1, 63), want: "-9223372036854775808"},
		{name: "positive next below 2^64", value: math.Nextafter(math.Ldexp(1, 64), 0), want: "18446744073709549568"},
		{name: "negative next below 2^64", value: -math.Nextafter(math.Ldexp(1, 64), 0), want: "-18446744073709549568"},
		{name: "positive 2^64", value: math.Ldexp(1, 64), want: "18446744073709551616"},
		{name: "negative 2^64", value: -math.Ldexp(1, 64), want: "-18446744073709551616"},
		{name: "positive 2^126", value: math.Ldexp(1, 126), want: "85070591730234615865843651857942052864"},
		{name: "negative 2^126", value: -math.Ldexp(1, 126), want: "-85070591730234615865843651857942052864"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := Decimal128FromFloat64(test.value, 38, 0)
			require.NoError(t, err)
			require.Equal(t, test.want, result.Format(0))
		})
	}

	result, err := Decimal128FromFloat64(math.Ldexp(1, 64), 38, 6)
	require.NoError(t, err)
	require.Equal(t, "18446744073709551616.000000", result.Format(6))

	for _, value := range []float64{
		math.Ldexp(1, 63),
		-math.Ldexp(1, 63),
		math.Nextafter(math.Ldexp(1, 64), 0),
		-math.Nextafter(math.Ldexp(1, 64), 0),
		math.Ldexp(1, 64),
		-math.Ldexp(1, 64),
	} {
		_, err := Decimal128FromFloat64(value, 38, 38)
		require.Error(t, err)
	}
	for _, value := range []float64{math.Ldexp(1, 127), -math.Ldexp(1, 127)} {
		_, err := Decimal128FromFloat64(value, 38, 0)
		require.Error(t, err)
	}
}

func TestDecimal128FromFloat64KeepsDecimalRoundingAndRangeChecks(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		want  string
	}{
		{name: "positive half", value: 1.125, want: "1.13"},
		{name: "negative half", value: -1.125, want: "-1.13"},
		{name: "binary fraction", value: 2.675, want: "2.68"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := Decimal128FromFloat64(test.value, 6, 2)
			require.NoError(t, err)
			require.Equal(t, test.want, result.Format(2))
		})
	}

	result, err := Decimal128FromFloat64(1000000000001, 19, 6)
	require.NoError(t, err)
	require.Equal(t, "1000000000001.000000", result.Format(6))

	for _, width := range []int32{18, 19} {
		result, err = Decimal128FromFloat64(1.005, width, 2)
		require.NoError(t, err)
		require.Equal(t, "1.00", result.Format(2))
	}

	result, err = Decimal128FromFloat64(1.0000000000000002, 38, 18)
	require.NoError(t, err)
	require.Equal(t, "1.000000000000000256", result.Format(18))

	_, err = Decimal128FromFloat64(1000000000001, 18, 6)
	require.Error(t, err)
	_, err = Decimal128FromFloat64(999.995, 5, 2)
	require.Error(t, err)
	_, err = Decimal128FromFloat64(-999.995, 5, 2)
	require.Error(t, err)

	_, err = Decimal128FromFloat64(1e20, 19, 6)
	require.EqualError(t, err, "invalid input: Can't convert Float64 To Decimal128: 100000000000000000000.000000(19,6)")
}

var decimal128FromFloat64Sink Decimal128

func TestDecimal128FromFloat64DoesNotAllocate(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		width int32
		scale int32
	}{
		{name: "integral", value: 1000000000001, width: 38, scale: 6},
		{name: "integral above uint64", value: math.Ldexp(1, 64), width: 38, scale: 0},
		{name: "fractional", value: 12345.6789, width: 38, scale: 6},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := Decimal128FromFloat64(test.value, test.width, test.scale)
			require.NoError(t, err)
			decimal128FromFloat64Sink = result

			allocs := testing.AllocsPerRun(100, func() {
				decimal128FromFloat64Sink, _ = Decimal128FromFloat64(test.value, test.width, test.scale)
			})
			require.Zero(t, allocs)
		})
	}
}

func BenchmarkDecimal128FromFloat64(b *testing.B) {
	tests := []struct {
		name  string
		value float64
		width int32
		scale int32
	}{
		{name: "integral", value: 1000000000001, width: 38, scale: 6},
		{name: "integral above uint64", value: math.Ldexp(1, 64), width: 38, scale: 0},
		{name: "fractional", value: 12345.6789, width: 38, scale: 6},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, err := Decimal128FromFloat64(test.value, test.width, test.scale)
				if err != nil {
					b.Fatal(err)
				}
				decimal128FromFloat64Sink = result
			}
		})
	}
}

func TestDecimal128FromFloat64RejectsSpecialValuesAndInvalidTypes(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := Decimal128FromFloat64(value, 38, 6)
		require.Error(t, err)
	}

	for _, target := range [][2]int32{{0, 0}, {39, 0}, {6, -1}, {5, 6}} {
		_, err := Decimal128FromFloat64(1, target[0], target[1])
		require.Error(t, err)
	}

	result, err := Decimal128FromFloat64(math.SmallestNonzeroFloat64, 38, 6)
	require.NoError(t, err)
	require.Equal(t, "0.000000", result.Format(6))
	_, err = Decimal128FromFloat64(math.MaxFloat64, 38, 6)
	require.Error(t, err)
}

func TestDecimal64AddSub(t *testing.T) {
	result, scale, err := Decimal64(2305843009213693952).Add(1152921504606846976, 0, 0)
	require.NoError(t, err)
	require.Equal(t, Decimal64(3458764513820540928), result)
	require.Equal(t, int32(0), scale)
	result, scale, err = Decimal64(3458764513820540928).Sub(1152921504606846976, 0, 0)
	require.NoError(t, err)
	require.Equal(t, Decimal64(2305843009213693952), result)
	require.Equal(t, int32(0), scale)

	_, _, err = Decimal64(9223372036854775807).Add(1, 0, 0)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.EqualError(t, err, "invalid input: Decimal64 Add overflow: 9223372036854775807+1")
	result, scale, err = Decimal64(0).Sub(9223372036854775807, 0, 10)
	require.NoError(t, err)
	require.Equal(t, Decimal64(9223372036854775809), result) // -(2^63-1) in two's complement.
	require.Equal(t, int32(10), scale)
}
func TestDecimal128AddSub(t *testing.T) {
	left := Decimal128{B0_63: 0x4000000000000000, B64_127: 0x2000000000000000}
	right := Decimal128{B0_63: 0x2000000000000000, B64_127: 0x1000000000000000}
	sum := Decimal128{B0_63: 0x6000000000000000, B64_127: 0x3000000000000000}
	for _, test := range []struct {
		name                  string
		fn                    func(Decimal128, Decimal128, int32, int32) (Decimal128, int32, error)
		left, right, want     Decimal128
		leftScale, rightScale int32
		wantScale             int32
	}{
		{"wide add", Decimal128.Add, left, right, sum, 0, 0, 0},
		{"wide subtract", Decimal128.Sub, sum, right, left, 0, 0, 0},
		{"carry", Decimal128.Add, Decimal128{B0_63: ^uint64(0), B64_127: 1}, Decimal128{B0_63: 1}, Decimal128{B64_127: 2}, 0, 0, 0},
		{"borrow", Decimal128.Sub, Decimal128{B64_127: 2}, Decimal128{B0_63: 1}, Decimal128{B0_63: ^uint64(0), B64_127: 1}, 0, 0, 0},
		{"add zero right", Decimal128.Add, Decimal128{B0_63: 1}, Decimal128{}, Decimal128{B0_63: 1}, 2, 2, 2},
		{"add zero left", Decimal128.Add, Decimal128{}, Decimal128{B0_63: 1}, Decimal128{B0_63: 1}, 2, 2, 2},
		{"align fractional zero", Decimal128.Add, Decimal128{B0_63: 1}, Decimal128{}, Decimal128{B0_63: 10}, 1, 2, 2},
		{"subtract zero", Decimal128.Sub, Decimal128{B0_63: 1}, Decimal128{}, Decimal128{B0_63: 1}, 2, 2, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, scale, err := test.fn(test.left, test.right, test.leftScale, test.rightScale)
			require.NoError(t, err)
			require.Equal(t, test.want, result)
			require.Equal(t, test.wantScale, scale)
		})
	}
}

func TestDecimal64MulDiv(t *testing.T) {
	result, scale, err := Decimal64(1073741823).Mul(1073741821, 0, 0)
	require.NoError(t, err)
	require.Equal(t, Decimal64(1152921500311879683), result)
	require.Equal(t, int32(0), scale)
	result, scale, err = Decimal64(1152921500311879683).Div(1073741821, 12, 0)
	require.NoError(t, err)
	require.Equal(t, Decimal64(1073741823), result)
	require.Equal(t, int32(12), scale)
}
func TestDecimal128MulDiv(t *testing.T) {
	left := Decimal128{B0_63: 18014398509481983}
	right := Decimal128{B0_63: 1, B64_127: 64}
	product := Decimal128{B0_63: 18014398509481983, B64_127: 1152921504606846912}
	result, scale, err := left.Mul(right, 0, 0)
	require.NoError(t, err)
	require.Equal(t, product, result)
	require.Equal(t, int32(0), scale)
	result, scale, err = product.Div(right, 12, 0)
	require.NoError(t, err)
	require.Equal(t, left, result)
	require.Equal(t, int32(12), scale)
}

func TestDecimal128OverDiv(t *testing.T) {
	x, _, _ := Parse128("99999999999999999999999999999999999999")
	y, _, _ := Parse128("10000000000")
	z, _, err := x.Div(y, 0, 0)
	if err != nil || z.Format(0) != "10000000000000000000000000000000000" {
		panic("wrong")
	}
}

func TestDecimal128Div128HalfUpLargeDivisor(t *testing.T) {
	fromBig := func(value *big.Int) Decimal128 {
		t.Helper()
		if value.Sign() < 0 || value.BitLen() > 127 {
			t.Fatalf("value does not fit a positive Decimal128: %s", value)
		}
		hi := new(big.Int).Rsh(new(big.Int).Set(value), 64).Uint64()
		return Decimal128{B0_63: value.Uint64(), B64_127: hi}
	}
	toBig := func(value Decimal128) *big.Int {
		result := new(big.Int).SetUint64(value.B64_127)
		result.Lsh(result, 64)
		return result.Or(result, new(big.Int).SetUint64(value.B0_63))
	}

	for _, divisorCase := range []struct {
		name  string
		value string
	}{
		{name: "even", value: "100000000000000000000"},
		{name: "odd", value: "100000000000000000001"},
	} {
		divisor, ok := new(big.Int).SetString(divisorCase.value, 10)
		require.True(t, ok)
		half := new(big.Int).Rsh(new(big.Int).Set(divisor), 1)
		for _, quotient := range []int64{0, 1, 123456, 850705917302346158} {
			for _, delta := range []int64{-1, 0, 1} {
				t.Run(fmt.Sprintf("%s_q_%d_delta_%d", divisorCase.name, quotient, delta), func(t *testing.T) {
					x := new(big.Int).Mul(big.NewInt(quotient), divisor)
					x.Add(x, half)
					x.Add(x, big.NewInt(delta))
					input := fromBig(x)
					divisor128 := fromBig(divisor)

					got, err := input.Div128(divisor128)
					require.NoError(t, err)

					want, remainder := new(big.Int), new(big.Int)
					want.QuoRem(x, divisor, remainder)
					if new(big.Int).Lsh(remainder, 1).Cmp(divisor) >= 0 {
						want.Add(want, big.NewInt(1))
					}
					require.Equal(t, want, toBig(got))
				})
			}
		}
	}

	t.Run("odd half threshold carries into high limb", func(t *testing.T) {
		x := new(big.Int).Lsh(big.NewInt(1), 64)
		y := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 65), big.NewInt(1))
		got, err := fromBig(x).Div128(fromBig(y))
		require.NoError(t, err)
		require.Equal(t, big.NewInt(1), toBig(got))
	})

	t.Run("corrects high quotient estimate at signed limit", func(t *testing.T) {
		x := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
		y := new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(3))
		input, divisor := fromBig(x), fromBig(y)

		truncated, err := input.Div128Trunc(divisor)
		require.NoError(t, err)
		wantTruncated := new(big.Int).Quo(new(big.Int).Set(x), y)
		require.Equal(t, wantTruncated, toBig(truncated))

		got, err := input.Div128(divisor)
		require.NoError(t, err)

		want, remainder := new(big.Int), new(big.Int)
		want.QuoRem(x, y, remainder)
		if new(big.Int).Lsh(remainder, 1).Cmp(y) >= 0 {
			want.Add(want, big.NewInt(1))
		}
		require.Equal(t, big.NewInt(9223372036854775807), want)
		require.Equal(t, want, toBig(got))
	})

	t.Run("rounded quotient reaches the high bit of the low limb", func(t *testing.T) {
		x := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
		y := new(big.Int).Lsh(big.NewInt(1), 64)
		got, err := fromBig(x).Div128(fromBig(y))
		require.NoError(t, err)
		want := new(big.Int).Lsh(big.NewInt(1), 63)
		require.Equal(t, want, toBig(got))
	})
}

func TestDecimal128Div128TruncByZero(t *testing.T) {
	_, err := (Decimal128{B0_63: 1}).Div128Trunc(Decimal128{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Decimal128 Div by Zero")
}

func TestParseFormat(t *testing.T) {
	x := Decimal128{0, 1}
	c := x.Format(5)
	y, err := ParseDecimal128(c, 30, 5)
	if err != nil {
		panic("error")
	}
	if x != y {
		fmt.Println(x.B64_127, x.B0_63)
		fmt.Println(y.B64_127, y.B0_63)
		panic("wrong")
	}
}

func decimalFormat[T DecimalWithFormat](x T, scale int32) string {
	return x.Format(scale)
}

var decimalFormatSink string

func TestDecimalFormat(t *testing.T) {
	d64 := Decimal64(0)
	d128 := Decimal128{0, 0}

	d64str := decimalFormat(d64, 5)
	if d64str != "0.00000" {
		t.Error("Decimal64 format failed")
	}

	d128str := decimalFormat(d128, 5)
	if d128str != "0.00000" {
		t.Error("Decimal128 format failed")
	}
}

func BenchmarkFor(b *testing.B) {
	for i := 0; i < b.N; i++ {
	}
}
func BenchmarkFloatAdd(b *testing.B) {
	x := float64(rand.Int())
	y := float64(rand.Int())
	for i := 0; i < b.N; i++ {
		x += y
	}
	z := Decimal128{uint64(x), 0}
	z.Add128(z)
}

func BenchmarkAdd(b *testing.B) {
	x := Decimal128{uint64(rand.Int()), uint64(rand.Int()) >> 1}
	y := Decimal128{uint64(rand.Int()), uint64(rand.Int()) >> 1}
	for i := 0; i < b.N; i++ {
		x.Add128(y)
	}
}

func BenchmarkFloatSub(b *testing.B) {
	x := float64(rand.Int())
	y := float64(rand.Int())
	for i := 0; i < b.N; i++ {
		x -= y
	}
	z := Decimal128{uint64(x), 0}
	z.Add128(z)
}

func BenchmarkSub(b *testing.B) {
	x := Decimal128{uint64(rand.Int()), uint64(rand.Int())}
	y := Decimal128{uint64(rand.Int()), uint64(rand.Int())}
	for i := 0; i < b.N; i++ {
		x.Sub128(y)
	}
}

func BenchmarkFloatMul(b *testing.B) {
	x := float64(rand.Int())
	y := float64(1.0001)
	for i := 0; i < b.N; i++ {
		x *= y
	}
	z := Decimal128{uint64(x), 0}
	z.Add128(z)
}

func BenchmarkMul64(b *testing.B) {
	x := Decimal64(rand.Int() >> 32)
	y := Decimal64(rand.Int() >> 32)
	for i := 0; i < b.N; i++ {
		_, _ = x.Mul64(y)
	}
}
func BenchmarkMul(b *testing.B) {
	x := Decimal128{uint64(rand.Int()) >> 8, 0}
	y := Decimal128{uint64(rand.Int()), uint64(rand.Int()) & 255}
	for i := 0; i < b.N; i++ {
		x.Mul128(y)
	}
}

func BenchmarkFloatDiv(b *testing.B) {
	x := float64(rand.Int())
	y := float64(1.0000001)
	for i := 0; i < b.N; i++ {
		x /= y
	}
	z := Decimal128{uint64(x), 0}
	z.Add128(z)
}

func BenchmarkDiv(b *testing.B) {
	x := Decimal128{uint64(rand.Int()), uint64(rand.Int())}
	y := Decimal128{uint64(rand.Int()), uint64(rand.Int()) >> 4}
	for i := 0; i < b.N; i++ {
		x.Div128(y)
	}
}

func BenchmarkIntMod(b *testing.B) {
	x := rand.Int()
	y := rand.Int()
	z := int(0)
	for i := 0; i < b.N; i++ {
		z += x % y
	}
	w := Decimal128{uint64(z), 0}
	w.Add128(w)

}

func BenchmarkMod(b *testing.B) {
	x := Decimal128{uint64(rand.Int()), uint64(rand.Int())}
	y := Decimal128{uint64(rand.Int()), uint64(rand.Int())}
	for i := 0; i < b.N; i++ {
		x.Mod128(y)
	}
}

func BenchmarkDecimal256Format(b *testing.B) {
	x, err := ParseDecimal256("12345678901234567890123456789012345.123456789012345678901234567890", 65, 30)
	if err != nil {
		b.Fatalf("ParseDecimal256 failed: %v", err)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		decimalFormatSink = x.Format(30)
	}
}

// TestAdd128Unchecked tests the branchless Add128Unchecked for SUM aggregation.
func TestAdd128Unchecked(t *testing.T) {
	cases := []struct {
		name string
		x, y Decimal128
	}{
		{"both_pos", Decimal128{100, 0}, Decimal128{200, 0}},
		{"pos_neg", Decimal128{100, 0}, Decimal128{B0_63: ^uint64(99), B64_127: ^uint64(0)}}, // -100
		{"neg_neg", Decimal128{B0_63: ^uint64(99), B64_127: ^uint64(0)}, Decimal128{B0_63: ^uint64(49), B64_127: ^uint64(0)}},
		{"zero_add", Decimal128{}, Decimal128{42, 0}},
		{"large", Decimal128{^uint64(0), 0x3FFFFFFFFFFFFFFF}, Decimal128{1, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.x.Add128Unchecked(tc.y)
			want, _ := tc.x.Add128(tc.y)
			if got != want {
				t.Fatalf("Add128Unchecked(%v, %v) = %v, want %v", tc.x, tc.y, got, want)
			}
		})
	}
}

// TestDecimal128DivFallbackSign exercises the 256-bit fallback in Decimal128.Div
// where the sign correction was added (signx != signy → negate result).
func TestDecimal128DivFallbackSign(t *testing.T) {
	// We need x.Scale(scaleAdj) to fail so we enter the D256 fallback path.
	// Large x with high scaleAdj will trigger this.
	// x * 10^6 must overflow D128 but x * 10^6 / y must fit D128.
	// 3e32 * 10^6 = 3e38 > D128max ≈ 1.7e38, but 3e38/3 = 1e38 < D128max.
	largePos, _ := ParseDecimal128("300000000000000000000000000000000", 38, 0)
	largeNeg, _ := ParseDecimal128("-300000000000000000000000000000000", 38, 0)
	smallPos, _ := ParseDecimal128("3", 38, 0)
	smallNeg, _ := ParseDecimal128("-3", 38, 0)

	t.Run("pos_div_neg", func(t *testing.T) {
		z, _, err := largePos.Div(smallNeg, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !z.Sign() {
			t.Fatalf("expected negative result, got %s", z.Format(12))
		}
	})
	t.Run("neg_div_pos", func(t *testing.T) {
		z, _, err := largeNeg.Div(smallPos, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !z.Sign() {
			t.Fatalf("expected negative result, got %s", z.Format(12))
		}
	})
	t.Run("neg_div_neg", func(t *testing.T) {
		z, _, err := largeNeg.Div(smallNeg, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if z.Sign() {
			t.Fatalf("expected positive result, got %s", z.Format(12))
		}
	})
}

// TestDecimal256AddErrorFormat exercises the Decimal256.Add error message path
// where origX/origY are preserved for formatting.
func TestDecimal256AddErrorFormat(t *testing.T) {
	// Two very large Decimal256 whose sum overflows.
	maxVal := Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 0x7FFFFFFFFFFFFFFF}
	one := Decimal256{B0_63: 1}
	_, _, err := maxVal.Add(one, 0, 0)
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

// TestDecimal128AddSubErrorFormat exercises Decimal128.Add/Sub with origX/origY.
func TestDecimal128AddSubErrorFormat(t *testing.T) {
	maxD128 := Decimal128{B0_63: ^uint64(0), B64_127: 0x7FFFFFFFFFFFFFFF}
	one := Decimal128{B0_63: 1}

	result, scale, err := maxD128.Add(Decimal128{}, 0, 0)
	require.NoError(t, err)
	require.Equal(t, maxD128, result)
	require.Equal(t, int32(0), scale)
	_, _, err = maxD128.Add(one, 0, 0)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.EqualError(t, err, "invalid input: Decimal128 Add overflow: 170141183460469231731687303715884105727+1")

	result, scale, err = maxD128.Sub(Decimal128{}, 0, 0)
	require.NoError(t, err)
	require.Equal(t, maxD128, result)
	require.Equal(t, int32(0), scale)
	_, _, err = maxD128.Sub(Decimal128{B0_63: ^uint64(0), B64_127: ^uint64(0)}, 0, 0)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.EqualError(t, err, "invalid input: Decimal128 Sub overflow: 170141183460469231731687303715884105727--1")
}

func TestDecimalScaleOverflowErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		x        int64
		n        int32
		want     int64
		overflow bool
	}{
		{"largest scale", 1, 18, 1000000000000000000, false},
		{"signed positive overflow", 1, 19, 0, true},
		{"signed negative overflow", -1, 19, 0, true},
		{"positive boundary", 922337203685477580, 1, 9223372036854775800, false},
		{"positive beyond boundary", 922337203685477581, 1, 0, true},
		{"negative boundary", -922337203685477580, 1, -9223372036854775800, false},
		{"negative beyond boundary", -922337203685477581, 1, 0, true},
		{"minimum unchanged", math.MinInt64, 0, math.MinInt64, false},
		{"minimum downscale", math.MinInt64, -1, -922337203685477581, false},
		{"minimum coarse rounding", math.MinInt64, -19, -1, false},
		{"maximum coarse rounding", math.MaxInt64, -19, 1, false},
		{"maximum scale18 rounding", math.MaxInt64, -18, 9, false},
		{"exact chunk downscale", 125000000000000, -10, 12500, false},
		{"unsigned overflow", math.MaxInt64, 18, 0, true},
		{"unsigned coarse overflow", math.MaxInt64, 38, 0, true},
		{"extreme positive", 1, math.MaxInt32, 0, true},
		{"extreme negative", math.MinInt64, math.MinInt32, 0, false},
		{"zero", 0, math.MaxInt32, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := Decimal64(uint64(tc.x))
			got, err := x.Scale(tc.n)
			if tc.overflow {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "result %d, error %v", int64(got), err)
				require.Equal(t, x, got, "failed alignment must preserve input for widening consumers")
				require.EqualError(t, err, fmt.Sprintf("invalid input: Decimal64 scale overflow: coefficient %d, target scale=%d", tc.x, tc.n))
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, int64(got))
			}
		})
	}

	maxD128 := Decimal128{B0_63: ^uint64(0), B64_127: 0x7FFFFFFFFFFFFFFF}
	maxD256 := Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 0x7FFFFFFFFFFFFFFF}
	const d128Error = "Decimal128 scale overflow: coefficient 170141183460469231731687303715884105727, target scale="
	const d256Error = "Decimal256 scale overflow: coefficient 57896044618658097711785492504343953926634992332820282019728792003956564819967, target scale="
	for _, tc := range []struct {
		name, want string
		scale      func(int32) error
	}{
		{"d128_inplace", d128Error, func(n int32) error { x := maxD128; return x.ScaleInplace(n) }},
		{"d128", d128Error, func(n int32) error { _, err := maxD128.Scale(n); return err }},
		{"d128_truncate", d128Error, func(n int32) error { _, err := maxD128.ScaleTruncate(n); return err }},
		{"d256", "Decimal256 scale overflow: target scale=", func(n int32) error { _, err := maxD256.Scale(n); return err }},
		{"d256_truncate", d256Error, func(n int32) error { _, err := maxD256.ScaleTruncate(n); return err }},
	} {
		// 18 exercises the final multiply; 38 overflows in the first 19-digit chunk.
		for _, n := range []int32{18, 38} {
			t.Run(fmt.Sprintf("%s/%d", tc.name, n), func(t *testing.T) {
				err := tc.scale(n)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				require.EqualError(t, err, fmt.Sprintf("invalid input: %s%d", tc.want, n))
			})
		}
	}
	t.Run("d128_scaleinplace_down", func(t *testing.T) {
		x := maxD128
		require.NoError(t, x.ScaleInplace(-38))
		require.Equal(t, Decimal128{B0_63: 2}, x)
	})
}

func TestDecimalArithOverflowErrors(t *testing.T) {
	max64 := Decimal64(^uint64(0) >> 1)
	max128 := Decimal128{B0_63: ^uint64(0), B64_127: 0x7FFFFFFFFFFFFFFF}
	max256 := Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 0x7FFFFFFFFFFFFFFF}
	const coefficient64 = "9223372036854775807"
	const coefficient128 = "170141183460469231731687303715884105727"
	const coefficient256 = "57896044618658097711785492504343953926634992332820282019728792003956564819967"
	for _, tc := range []struct {
		name, want string
		run        func() error
	}{
		{"d64_mul", "Decimal64 Mul overflow: " + coefficient64 + "*" + coefficient64, func() error { _, _, err := max64.Mul(max64, 0, 0); return err }},
		{"d128_mul", "Decimal128 Mul overflow: " + coefficient128 + "*" + coefficient128, func() error { _, _, err := max128.Mul(max128, 0, 0); return err }},
		{"d64_div", "Decimal64 Div overflow: " + coefficient64 + "/1", func() error { _, _, err := max64.Div(1, 0, 0); return err }},
		{"d128_div", "Decimal128 Div overflow: " + coefficient128 + "/1", func() error { _, _, err := max128.Div(Decimal128{B0_63: 1}, 0, 0); return err }},
		{"d256_div", "Decimal256 Div overflow: " + coefficient256 + "/1", func() error { _, _, err := max256.Div(Decimal256{B0_63: 1}, 0, 0); return err }},
		{"d64_sub", "Decimal64 Sub overflow: " + coefficient64 + "-0.000000000000000001", func() error { _, _, err := max64.Sub(1, 0, 18); return err }},
		{"d128_sub", "Decimal128 Sub overflow: " + coefficient128 + "-0.000000000000000001", func() error { _, _, err := max128.Sub(Decimal128{B0_63: 1}, 0, 18); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
			require.EqualError(t, err, "invalid input: "+tc.want)
		})
	}
}

func TestDecimal64MulCappedScale(t *testing.T) {
	for _, tc := range []struct{ input, want Decimal64 }{
		{100000000, 10000},
		{Decimal64(100000000).Minus(), Decimal64(10000).Minus()},
		{Decimal64(1) << 63, Decimal64(922337203685478).Minus()},
	} {
		got, scale, err := tc.input.Mul(1, 8, 8)
		require.NoError(t, err)
		require.Equal(t, int32(12), scale)
		require.Equal(t, tc.want, got)
	}
	minimum := Decimal64(1) << 63
	got, _, err := minimum.Mul(1, 0, 0)
	require.NoError(t, err)
	require.Equal(t, minimum, got)
	_, _, err = minimum.Mul(Decimal64(1).Minus(), 0, 0)
	require.Error(t, err)
}

func TestDecimalScientificExponentBoundaries(t *testing.T) {
	for _, tc := range []struct {
		spelling    string
		coefficient uint64
	}{
		{"1E-2", 1}, {"1e-2", 1}, {"1E+2", 10000},
		{"0E2147483647", 0}, {"1E-2147483647", 0},
	} {
		d64, e64 := ParseDecimal64(tc.spelling, 18, 2)
		d128, e128 := ParseDecimal128(tc.spelling, 38, 2)
		d256, e256 := ParseDecimal256(tc.spelling, 65, 2)
		require.NoError(t, e64, tc.spelling)
		require.NoError(t, e128, tc.spelling)
		require.NoError(t, e256, tc.spelling)
		require.Equal(t, Decimal64(tc.coefficient), d64, tc.spelling)
		require.Equal(t, Decimal128{B0_63: tc.coefficient}, d128, tc.spelling)
		require.Equal(t, Decimal256{B0_63: tc.coefficient}, d256, tc.spelling)
	}
	for _, spelling := range []string{"1E4294967296", "1E-4294967296", "1E2147483647"} {
		_, e64 := ParseDecimal64(spelling, 18, 2)
		_, e128 := ParseDecimal128(spelling, 38, 2)
		_, e256 := ParseDecimal256(spelling, 65, 2)
		require.True(t, moerr.IsMoErrCode(e64, moerr.ErrInvalidInput), spelling)
		require.True(t, moerr.IsMoErrCode(e128, moerr.ErrInvalidInput), spelling)
		require.True(t, moerr.IsMoErrCode(e256, moerr.ErrInvalidInput), spelling)
	}
}

func TestDecimalFromCoefficient(t *testing.T) {
	for _, tc := range []struct {
		name  string
		width int
		parse func([]byte) (string, error)
	}{
		{"64", 18, func(d []byte) (string, error) { v, e := Decimal64FromCoefficient(d); return v.Format(0), e }},
		{"128", 38, func(d []byte) (string, error) { v, e := Decimal128FromCoefficient(d); return v.Format(0), e }},
		{"256", 76, func(d []byte) (string, error) { v, e := Decimal256FromCoefficient(d); return v.Format(0), e }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, digits := range []string{"", "0", "1234567", strings.Repeat("9", tc.width)} {
				got, err := tc.parse([]byte(digits))
				require.NoError(t, err)
				if digits == "" {
					digits = "0"
				}
				require.Equal(t, digits, got)
			}
			for _, digits := range []string{"-1", "1.2", "1e2", "a", strings.Repeat("9", tc.width+1)} {
				_, err := tc.parse([]byte(digits))
				require.Error(t, err, digits)
			}
		})
	}
}

func TestDecimal64DivWidenedScale(t *testing.T) {
	for _, x := range []int64{1, -1} {
		for _, y := range []int64{10000000000000, -10000000000000} {
			got, scale, err := Decimal64(uint64(x)).Div(Decimal64(uint64(y)), 0, 13)
			require.NoError(t, err)
			require.Equal(t, int32(6), scale)
			want := int64(1000000)
			if (x < 0) != (y < 0) {
				want = -want
			}
			require.Equal(t, want, int64(got))
		}
	}
	negative, _, err := Decimal64Min.Div(1, 12, 0)
	require.NoError(t, err)
	require.Equal(t, Decimal64Min, negative)
	_, _, err = Decimal64Min.Div(Decimal64(1).Minus(), 12, 0)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))

	got, _, err := Decimal64Min.Div(1000000, 0, 0)
	require.NoError(t, err)
	require.Equal(t, Decimal64Min, got)
	_, _, err = Decimal64Min.Div(Decimal64(1000000).Minus(), 0, 0)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
}

func TestDecimal64Div64MagnitudeRounding(t *testing.T) {
	// Div64 is an unsigned magnitude primitive, including the negative signed
	// endpoint and widened intermediates. Use unbounded arithmetic as oracle.
	for _, x := range []uint64{0, 1, 4, 5, 9, 1<<63 - 1, 1 << 63, math.MaxUint64} {
		for _, y := range []uint64{1, 2, 3, 10, 1 << 63, 10000000000000000000, math.MaxUint64} {
			quotient, remainder := new(big.Int), new(big.Int)
			divisor := new(big.Int).SetUint64(y)
			quotient.QuoRem(new(big.Int).SetUint64(x), divisor, remainder)
			if remainder.Lsh(remainder, 1).Cmp(divisor) >= 0 {
				quotient.Add(quotient, big.NewInt(1))
			}
			got, err := Decimal64(x).Div64(Decimal64(y))
			require.NoError(t, err)
			require.Equal(t, quotient.Uint64(), uint64(got), "%d/%d", x, y)
		}
	}
	_, err := Decimal64(1).Div64(0)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
}
