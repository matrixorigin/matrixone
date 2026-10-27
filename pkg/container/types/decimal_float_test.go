// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"math"
	"math/big"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

// The oracle uses exact rational arithmetic, independently of the production
// parser, scaling, rounding and fast-path guard. Decimal128 deliberately preserves
// exact binary integers, including those whose shortest text is approximate.
func floatDecimalCoefficient(t *testing.T, value float64, scale int32, integral128 bool) *big.Int {
	t.Helper()
	a := math.Abs(value)
	r := new(big.Rat).SetFloat64(a)
	if !integral128 || !r.IsInt() {
		var ok bool
		r, ok = new(big.Rat).SetString(strconv.FormatFloat(a, 'e', -1, 64))
		require.True(t, ok)
	}
	power := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	numerator := new(big.Int).Mul(r.Num(), power)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, r.Denom(), remainder)
	if remainder.Lsh(remainder, 1).Cmp(r.Denom()) >= 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient
}

func TestFloatDecimalRationalOracle(t *testing.T) {
	checks := 0
	check := func(value float64, width, scale int32) {
		t.Helper()
		limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(width)), nil)
		want := floatDecimalCoefficient(t, value, scale, true)
		got, err := Decimal128FromFloat64(value, width, scale)
		if want.Cmp(limit) >= 0 {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "value=%g (%d,%d): %v", value, width, scale, err)
		} else {
			require.NoError(t, err, "value=%g (%d,%d)", value, width, scale)
			bits := new(big.Int).Set(want)
			if value < 0 && bits.Sign() != 0 {
				bits.Sub(new(big.Int).Lsh(big.NewInt(1), 128), bits)
			}
			require.Equal(t, Decimal128{B0_63: bits.Uint64(), B64_127: new(big.Int).Rsh(bits, 64).Uint64()}, got, "value=%g (%d,%d)", value, width, scale)
		}
		if width <= 18 {
			want = floatDecimalCoefficient(t, value, scale, false)
			got, err := Decimal64FromFloat64(value, width, scale)
			if value+1 == value || want.Cmp(limit) >= 0 {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "value=%g (%d,%d): %v", value, width, scale, err)
			} else {
				require.NoError(t, err, "value=%g (%d,%d)", value, width, scale)
				if value < 0 && want.Sign() != 0 {
					want.Sub(new(big.Int).Lsh(big.NewInt(1), 64), want)
				}
				require.Equal(t, Decimal64(want.Uint64()), got, "value=%g (%d,%d)", value, width, scale)
			}
		}
		checks++
	}
	checkBoth := func(value float64, scale int32) {
		check(value, 38, scale)
		if scale <= 18 {
			check(value, 18, scale)
		}
	}
	// Include both sides of the normal/subnormal transition and signed tiny
	// values at the fast-path scale limit and the first unconditional fallback scale.
	for _, value := range []float64{math.SmallestNonzeroFloat64, math.Float64frombits((1 << 52) - 1), math.Float64frombits(1 << 52)} {
		for _, scale := range []int32{0, 19, 20} {
			checkBoth(value, scale)
			checkBoth(-value, scale)
		}
	}
	scales := []int32{0, 1, 2, 6, 18, 19, 20, 38}
	// Stratified finite bit patterns include subnormals, binade transitions,
	// 17-digit significands, exact large integers and overflow at both signs.
	exponents := []uint64{0, 1, 2, 900, 960, 970, 1000, 1022, 1023, 1024, 1050, 1074, 1075, 1076, 1086, 1087, 1149, 1150, 2045, 2046}
	for exponent := uint64(127); exponent < 2046; exponent += 127 {
		exponents = append(exponents, exponent)
	}
	for _, exponent := range exponents {
		for _, mantissa := range []uint64{0, 1, 1 << 51, (1 << 52) - 2, (1 << 52) - 1} {
			for _, sign := range []uint64{0, 1 << 63} {
				value := math.Float64frombits(sign | exponent<<52 | mantissa)
				for _, scale := range scales {
					checkBoth(value, scale)
				}
			}
		}
	}
	for _, scale := range scales {
		factor := math.Pow10(int(scale))
		// Confirm that the oracle actually challenges both sides of the
		// rounded-product cutoff; expected results remain rational-derived.
		if scale <= 19 {
			cutoff := 0.25 / factor
			require.Less(t, math.Nextafter(cutoff, 0)*factor, 0.25)
			require.GreaterOrEqual(t, math.Nextafter(cutoff, math.Inf(1))*factor, 0.25)
		}
		// Decimal half-integer grid and the closest binary values on either side.
		for _, odd := range []float64{1, 3, 17, 201, 203, 535, 199999} {
			value := (odd / 2) / factor
			for _, nearby := range []float64{math.Nextafter(value, 0), value, math.Nextafter(value, math.Inf(1))} {
				checkBoth(nearby, scale)
				checkBoth(-nearby, scale)
			}
		}
		// Range guards and the transition where the conservative allowance
		// rejects every fractional part. No production guard is used as oracle.
		for _, scaled := range []float64{0.25, 0x1p49, 0x1p52} {
			value := scaled / factor
			for _, nearby := range []float64{math.Nextafter(value, 0), value, math.Nextafter(value, math.Inf(1))} {
				checkBoth(nearby, scale)
				checkBoth(-nearby, scale)
			}
		}
	}
	// A few ULPs around the acceptance/rejection boundary near 1.5. Results
	// come from the rational oracle, rather than from duplicating the guard.
	for offset := -16; offset <= 16; offset++ {
		value := math.Float64frombits(uint64(int64(math.Float64bits(1.5)) + int64(offset)))
		checkBoth(value, 0)
		checkBoth(-value, 0)
	}
	for _, width := range []int32{1, 5, 18, 19, 38} {
		for _, value := range []float64{0.125, -0.125, 999.994, 999.995, -999.995, math.Nextafter(1e18, 0), 1e18} {
			check(value, width, 0)
			if width >= 2 {
				check(value, width, 2)
			}
		}
	}
	// Real binary64 inputs whose normalized shortest powers plus scale cross
	// the downscale/drop cutoff and checked upscale boundary.
	for _, tc := range []struct {
		value float64
		scale int32
	}{
		{1e-18, 0}, {5e-18, 0}, {5e-17, 0}, {0.5, 0},
		{0.125, 3}, {0.125, 4},
		// These q=0/1 coefficients exceed the binary fast-path range.
		{0.12345678901234568, 17}, {0.12345678901234568, 18},
		{0.12345678901234568, 19},
		{0.12345678901234568, 20}, {0.12345678901234568, 38},
		{9.999999999999998, 38},
	} {
		checkBoth(tc.value, tc.scale)
		checkBoth(-tc.value, tc.scale)
	}
	t.Logf("checked %d finite value/target combinations", checks)
}

var decimal64FromFloat64Sink Decimal64

func TestFloatDecimalConversionsDoNotAllocate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value float64
		scale int32
	}{
		{"ordinary", 12345.6789, 6},
		{"tie", 1.005, 2},
		{"near tie", math.Nextafter(1.005, 0), 2},
		{"tiny", 1e-20, 6},
		{"large", 123456789012.34567, 6},
		{"high scale", 0.12345678901234568, 18},
		{"subnormal", math.SmallestNonzeroFloat64, 18},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decimal64FromFloat64(tc.value, 18, tc.scale)
			require.NoError(t, err)
			_, err = Decimal128FromFloat64(tc.value, 38, tc.scale)
			require.NoError(t, err)
			require.Zero(t, testing.AllocsPerRun(100, func() {
				decimal64FromFloat64Sink, _ = Decimal64FromFloat64(tc.value, 18, tc.scale)
				decimal128FromFloat64Sink, _ = Decimal128FromFloat64(tc.value, 38, tc.scale)
			}))
		})
	}
	for _, scale := range []int32{20, 38} {
		_, err := Decimal128FromFloat64(0.12345678901234568, 38, scale)
		require.NoError(t, err)
		require.Zero(t, testing.AllocsPerRun(100, func() {
			decimal128FromFloat64Sink, _ = Decimal128FromFloat64(0.12345678901234568, 38, scale)
		}))
	}
}

func BenchmarkFloatDecimalConversions(b *testing.B) {
	for _, tc := range []struct {
		name   string
		values []float64
		scale  int32
	}{
		{"ordinary", []float64{12345.6789}, 6},
		{"integral", []float64{12345, -12346}, 6},
		{"tie", []float64{1.005}, 2},
		{"near_tie", []float64{math.Nextafter(1.005, 0)}, 2},
		{"small", []float64{1e-20}, 6},
		{"large", []float64{123456789012.34567}, 6},
		{"high_scale", []float64{0.12345678901234568}, 18},
		{"scale19", []float64{1e-20, 2e-20}, 19},
		{"scale20", []float64{1e-20, 2e-20}, 20},
		{"scale38", []float64{1e-20, 2e-20}, 38},
		// Fixed before measurements: eight ordinary values, two tie/neighbor
		// values, one tiny value, one large value. Inputs vary on every call.
		{"mixed", []float64{12345.6789, -12345.6789, 12.3456789, -98.7654321, 0.1234567, 456.1234567, 77.1234567, 9876.5432101, 1.0000005, math.Nextafter(1.0000005, 0), 1e-20, 123456789012.34567}, 6},
	} {
		if tc.scale <= 18 {
			b.Run("decimal64/"+tc.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					result, err := Decimal64FromFloat64(tc.values[i%len(tc.values)], 18, tc.scale)
					if err != nil {
						b.Fatal(err)
					}
					decimal64FromFloat64Sink = result
				}
			})
		}
		b.Run("decimal128/"+tc.name, func(b *testing.B) {
			scale := tc.scale
			if tc.name == "high_scale" {
				scale = 20 // Also exercise the scale > 19 fallback.
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, err := Decimal128FromFloat64(tc.values[i%len(tc.values)], 38, scale)
				if err != nil {
					b.Fatal(err)
				}
				decimal128FromFloat64Sink = result
			}
		})
	}
}
