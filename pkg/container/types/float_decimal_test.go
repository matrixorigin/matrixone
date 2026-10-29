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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFloatDecimalShortestPair(t *testing.T) {
	checks := 0
	check := func(value float64) {
		t.Helper()
		text := strconv.FormatFloat(math.Abs(value), 'e', -1, 64)
		mantissa, exponent, ok := strings.Cut(text, "e")
		require.True(t, ok)
		e, err := strconv.Atoi(exponent)
		require.NoError(t, err)
		digits := strings.ReplaceAll(mantissa, ".", "")
		want, err := strconv.ParseUint(digits, 10, 64)
		require.NoError(t, err)
		power := int32(e - len(digits) + 1)
		if want == 0 {
			power = 0
		} else {
			for want%10 == 0 {
				want /= 10
				power++
			}
		}
		got, p := float64ShortestDecimal(value)
		if got != want || p != power || got >= 1e17 || got != 0 && got%10 == 0 {
			t.Fatalf("bits=%016x text=%s: got (%d,%d), want (%d,%d)", math.Float64bits(value), text, got, p, want, power)
		}
		// Exact value equality complements the pair comparison. The shortest
		// selection oracle shares its algorithm with the adapted primitive;
		// this test proves adaptation fidelity, not an independent shortest proof.
		r, ok := new(big.Rat).SetString(text)
		require.True(t, ok)
		actual := new(big.Rat).SetInt(new(big.Int).SetUint64(got))
		magnitude := int64(p)
		if magnitude < 0 {
			magnitude = -magnitude
		}
		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(magnitude), nil)
		if p < 0 {
			actual.Quo(actual, new(big.Rat).SetInt(factor))
		} else {
			actual.Mul(actual, new(big.Rat).SetInt(factor))
		}
		if r.Cmp(actual) != 0 {
			t.Fatalf("bits=%016x: decimal value differs", math.Float64bits(value))
		}
		checks++
	}
	for _, tc := range []struct {
		value float64
		c     uint64
		p     int32
	}{
		{0, 0, 0}, {math.Copysign(0, -1), 0, 0},
		{math.SmallestNonzeroFloat64, 5, -324}, {1.005, 1005, -3},
		{1000000000001, 1000000000001, 0}, {math.Ldexp(1, 64), 18446744073709552, 3},
	} {
		c, p := float64ShortestDecimal(tc.value)
		require.Equal(t, tc.c, c)
		require.Equal(t, tc.p, p)
		check(tc.value)
	}
	for exponent := uint64(0); exponent < 2047; exponent++ {
		for _, mantissa := range []uint64{0, 1, 1 << 51, (1 << 52) - 2, (1 << 52) - 1} {
			for _, sign := range []uint64{0, 1 << 63} {
				check(math.Float64frombits(sign | exponent<<52 | mantissa))
			}
		}
	}
	neighbors := func(value float64) {
		for _, nearby := range []float64{math.Nextafter(value, 0), value, math.Nextafter(value, math.Inf(1))} {
			if !math.IsInf(nearby, 0) {
				check(nearby)
				check(-nearby)
			}
		}
	}
	for bit := 0; bit < 52; bit++ {
		neighbors(math.Float64frombits(uint64(1) << bit))
	}
	for exponent := -1074; exponent <= 1023; exponent++ {
		neighbors(math.Ldexp(1, exponent))
	}
	for exponent := -323; exponent <= 308; exponent++ {
		neighbors(math.Pow10(exponent))
	}
	// xorshift64, fixed seed/algorithm: no environment-dependent random source.
	state := uint64(0x9e3779b97f4a7c15)
	for samples := 0; samples < 100000; {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		if state>>52&0x7ff != 0x7ff {
			check(math.Float64frombits(state))
			samples++
		}
	}
	t.Logf("checked %d finite shortest pairs", checks)
}

func TestFloatDecimalScalingBounds(t *testing.T) {
	// Enumerate exponent classes independently with real logarithms, rather
	// than allowing the production shift mask to hide an out-of-range shift.
	minE, maxE, minP, maxP, minS, maxS := 0, 0, 0, 0, 64, 0
	classes := 0
	check := func(e, z int, asymmetric bool) {
		footprint := float64(e+z) * math.Log10(2)
		if asymmetric {
			footprint += math.Log10(0.75)
			require.Equal(t, int(math.Floor(footprint)), decimalFloatSkewed(e+z))
		} else {
			require.Equal(t, int(math.Floor(footprint)), decimalFloatLog10Pow2(e+z))
		}
		p := -int(math.Floor(footprint))
		lp := int(math.Floor(float64(p) * math.Log2(10)))
		require.Equal(t, lp, decimalFloatLog2Pow10(p))
		var pre decimalFloatScaler
		decimalFloatPrescale(&pre, e, p, lp)
		require.GreaterOrEqual(t, p, decimalFloatPow10Min)
		require.LessOrEqual(t, p, decimalFloatPow10Max)
		require.GreaterOrEqual(t, pre.s, 0)
		require.Less(t, pre.s, 64)
		minE, maxE = min(minE, e), max(maxE, e)
		minP, maxP = min(minP, p), max(maxP, p)
		minS, maxS = min(minS, pre.s), max(maxS, pre.s)
		classes++
	}
	for exponentBits := 1; exponentBits < 2047; exponentBits++ {
		e := exponentBits - 1086 // 53-bit normal mantissa left-aligned by 11.
		check(e, 11, false)
		if exponentBits > 1 { // Smallest normal has symmetric subnormal spacing.
			check(e, 11, true)
		}
	}
	for highestBit := 0; highestBit < 52; highestBit++ {
		s := 63 - highestBit
		check(-1074-s, s, false)
	}
	require.Equal(t, 4143, classes)
	require.Equal(t, []int{-1137, 960, -292, 324, 5, 58}, []int{minE, maxE, minP, maxP, minS, maxS})
	require.Len(t, decimalFloatPow10, 696)
}
