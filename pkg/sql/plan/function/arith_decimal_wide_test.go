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
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func parseD256WideTest(t *testing.T, value string) (types.Decimal256, int32) {
	t.Helper()
	d, scale, err := types.Parse256(value)
	require.NoError(t, err)
	return d, scale
}

func bigD256WideTest(d types.Decimal256) *big.Int {
	abs := d
	neg := d.Sign()
	if neg {
		abs = abs.Minus()
	}
	z := new(big.Int).SetUint64(abs.B192_255)
	z.Lsh(z, 64)
	z.Or(z, new(big.Int).SetUint64(abs.B128_191))
	z.Lsh(z, 64)
	z.Or(z, new(big.Int).SetUint64(abs.B64_127))
	z.Lsh(z, 64)
	z.Or(z, new(big.Int).SetUint64(abs.B0_63))
	if neg {
		z.Neg(z)
	}
	return z
}

func decimal256FromBigWideTest(z *big.Int) types.Decimal256 {
	modulus := new(big.Int).Lsh(big.NewInt(1), 256)
	value := new(big.Int).Mod(new(big.Int).Set(z), modulus)
	return types.Decimal256{
		B0_63:    value.Uint64(),
		B64_127:  new(big.Int).Rsh(new(big.Int).Set(value), 64).Uint64(),
		B128_191: new(big.Int).Rsh(new(big.Int).Set(value), 128).Uint64(),
		B192_255: new(big.Int).Rsh(new(big.Int).Set(value), 192).Uint64(),
	}
}

func expectedD256MulWideTest(x, y types.Decimal256, scale1, scale2 int32) (types.Decimal256, bool) {
	desiredScale := int32(12)
	if scale1 > desiredScale {
		desiredScale = scale1
	}
	if scale2 > desiredScale {
		desiredScale = scale2
	}
	if scale1+scale2 < desiredScale {
		desiredScale = scale1 + scale2
	}
	return expectedD256ScaleDownWideTest(x, y, scale1+scale2-desiredScale)
}

func expectedD256ScaleDownWideTest(x, y types.Decimal256, scaleDown int32) (types.Decimal256, bool) {
	product := new(big.Int).Mul(bigD256WideTest(x), bigD256WideTest(y))
	if scaleDown > 0 {
		negative := product.Sign() < 0
		magnitude := new(big.Int).Abs(product)
		divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scaleDown)), nil)
		quotient, remainder := new(big.Int).QuoRem(magnitude, divisor, new(big.Int))
		if new(big.Int).Lsh(new(big.Int).Set(remainder), 1).Cmp(divisor) >= 0 {
			quotient.Add(quotient, big.NewInt(1))
		}
		if negative {
			quotient.Neg(quotient)
		}
		product = quotient
	}
	absolute := new(big.Int).Abs(new(big.Int).Set(product))
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(65), nil)
	if absolute.Cmp(limit) > 0 || (absolute.Cmp(limit) == 0 && product.Sign() >= 0) {
		return types.Decimal256{}, false
	}
	return decimal256FromBigWideTest(product), true
}

func TestD256MulScaledFallbackEnforcesDeclaredWidth(t *testing.T) {
	coefficient := "1" + strings.Repeat("0", 39)
	x, _ := parseD256WideTest(t, coefficient)
	y, _ := parseD256WideTest(t, coefficient)

	for _, tc := range []struct {
		name string
		left types.Decimal256
	}{
		{name: "positive", left: x},
		{name: "negative", left: x.Minus()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := []types.Decimal256{{B0_63: 42}}
			require.Error(t, d256Mul(
				[]types.Decimal256{tc.left}, []types.Decimal256{y}, result, 11, 11, nulls.NewWithSize(1)))
			require.Equal(t, types.Decimal256{B0_63: 42}, result[0])
		})
	}
}

func TestD256MulScaledFallbackHonorsNegativeWidthBoundary(t *testing.T) {
	coefficient := "1" + strings.Repeat("0", 66)
	positive, _ := parseD256WideTest(t, coefficient)
	one, _ := parseD256WideTest(t, "1")
	negative := positive.Minus()
	want, _ := parseD256WideTest(t, "1"+strings.Repeat("0", 65))
	want = want.Minus()

	var got types.Decimal256
	require.False(t, d256MulScaledFallback(&positive, &one, 1, &got))
	require.True(t, d256MulScaledFallback(&negative, &one, 1, &got))
	require.Equal(t, want, got)
}

func TestD256MulScaledRawOverflow(t *testing.T) {
	x, sx := parseD256WideTest(t, "0.32846287164921643232142372817438921749321")
	y, sy := parseD256WideTest(t, "0.9999999999999999999999999999999999999")
	want, ok := expectedD256MulWideTest(x, y, sx, sy)
	require.True(t, ok)
	negativeWant, ok := expectedD256MulWideTest(x.Minus(), y, sx, sy)
	require.True(t, ok)

	for _, tc := range []struct {
		name        string
		left, right []types.Decimal256
		nulls       *nulls.Nulls
		want        []types.Decimal256
	}{
		{name: "vec_vec", left: []types.Decimal256{x, x}, right: []types.Decimal256{y, y}, want: []types.Decimal256{want, want}},
		{name: "const_vec", left: []types.Decimal256{x}, right: []types.Decimal256{y, y}, want: []types.Decimal256{want, want}},
		{name: "vec_const", left: []types.Decimal256{x, x}, right: []types.Decimal256{y}, want: []types.Decimal256{want, want}},
		{name: "negative", left: []types.Decimal256{x.Minus()}, right: []types.Decimal256{y}, want: []types.Decimal256{negativeWant}},
		{name: "null", left: []types.Decimal256{x, x}, right: []types.Decimal256{y, y}, nulls: func() *nulls.Nulls { n := nulls.NewWithSize(2); n.Add(1); return n }(), want: []types.Decimal256{want, {}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nul := tc.nulls
			if nul == nil {
				nul = nulls.NewWithSize(len(tc.want))
			}
			got := make([]types.Decimal256, len(tc.want))
			require.NoError(t, d256Mul(tc.left, tc.right, got, sx, sy, nul))
			require.Equal(t, tc.want, got)
		})
	}
}

func TestD256MulScaledRawOverflowPreservesOverflow(t *testing.T) {
	// Both operands have 76 significant digits at scale 75. Their raw product
	// exceeds 256 bits and the rounded target-scale result exceeds the signed
	// Decimal256 range as well, so the original overflow must remain visible.
	value := "9." + strings.Repeat("9", 75)
	x, sx := parseD256WideTest(t, value)
	y, sy := parseD256WideTest(t, value)
	_, ok := expectedD256MulWideTest(x, y, sx, sy)
	require.False(t, ok)
	nul := nulls.NewWithSize(1)
	got := make([]types.Decimal256, 1)
	require.Error(t, d256Mul([]types.Decimal256{x}, []types.Decimal256{y}, got, sx, sy, nul))
}

func TestD256MulScaledFallbackRoundingBoundary(t *testing.T) {
	const scaleDown = int32(1)
	for _, tc := range []struct {
		name        string
		left, right types.Decimal256
	}{
		// Remainders below, exactly at, and above one half of 10.
		{name: "below_half", left: types.Decimal256{B0_63: 1}, right: types.Decimal256{B0_63: 4}},
		{name: "exact_half", left: types.Decimal256{B0_63: 1}, right: types.Decimal256{B0_63: 5}},
		{name: "exact_half_negative", left: types.Decimal256{B0_63: 1}.Minus(), right: types.Decimal256{B0_63: 5}},
		{name: "above_half", left: types.Decimal256{B0_63: 1}, right: types.Decimal256{B0_63: 6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, ok := expectedD256ScaleDownWideTest(tc.left, tc.right, scaleDown)
			require.True(t, ok)
			var got types.Decimal256
			require.True(t, d256MulScaledFallback(&tc.left, &tc.right, scaleDown, &got))
			require.Equal(t, want, got)
		})
	}

	// The rounded quotient is exactly 2^255 and must be rejected. Before
	// rounding, it is max signed D256 with a half-unit discarded remainder.
	const carryScale1, carryScale2 = int32(6), int32(7)
	carryLeft := types.Decimal256{B0_63: 1, B128_191: 1}
	carryRight := types.Decimal256{B0_63: ^uint64(0) - 4, B64_127: ^uint64(0), B128_191: 4}
	_, ok := expectedD256MulWideTest(carryLeft, carryRight, carryScale1, carryScale2)
	require.False(t, ok)
	result := []types.Decimal256{{B0_63: 42}}
	require.Error(t, d256Mul([]types.Decimal256{carryLeft}, []types.Decimal256{carryRight}, result, carryScale1, carryScale2, nulls.NewWithSize(1)))
	require.Equal(t, types.Decimal256{B0_63: 42}, result[0], "failed fallback must not overwrite the destination")
}

func TestD256MulScaledFallbackChunkBoundaries(t *testing.T) {
	// 2^128 * 2^128 is a raw 256-bit overflow, while every target scale below
	// is small enough for the rounded quotient to fit. Its remainders exercise
	// both sides of half across the 19-digit chunk boundaries.
	wideLeft := types.Decimal256{B128_191: 1}
	wideRight := types.Decimal256{B128_191: 1}
	for _, scaleDown := range []int32{38, 39, 57, 76} {
		t.Run("deterministic_"+strconv.FormatInt(int64(scaleDown), 10), func(t *testing.T) {
			want, wantOK := expectedD256ScaleDownWideTest(wideLeft, wideRight, scaleDown)
			require.True(t, wantOK)
			var got types.Decimal256
			require.True(t, d256MulScaledFallback(&wideLeft, &wideRight, scaleDown, &got))
			require.Equal(t, want, got)
			product := new(big.Int).Lsh(big.NewInt(1), 256)
			divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scaleDown)), nil)
			truncated, _ := new(big.Int).QuoRem(product, divisor, new(big.Int))
			if bigD256WideTest(want).Cmp(truncated) == 0 {
				// The n=39 case is below half; all other selected powers
				// have a discarded remainder above half and must increment.
				require.Equal(t, int32(39), scaleDown)
			} else {
				require.NotEqual(t, int32(39), scaleDown)
			}

			// Keep the sign restoration on the same path, including a remainder
			// that rounds away from zero.
			negativeLeft := wideLeft.Minus()
			negativeWant, negativeOK := expectedD256ScaleDownWideTest(negativeLeft, wideRight, scaleDown)
			require.True(t, negativeOK)
			got = types.Decimal256{}
			require.True(t, d256MulScaledFallback(&negativeLeft, &wideRight, scaleDown, &got))
			require.Equal(t, negativeWant, got)

			// Exercise the same case through the vector kernel, including its
			// >38-scale fallback dispatch.
			gotVector := make([]types.Decimal256, 1)
			require.NoError(t, d256Mul([]types.Decimal256{wideLeft}, []types.Decimal256{wideRight}, gotVector, scaleDown, scaleDown, nulls.NewWithSize(1)))
			require.Equal(t, want, gotVector[0])
		})
	}

	rng := rand.New(rand.NewSource(2887501))
	for _, scaleDown := range []int32{38, 39, 57, 76} {
		for i := 0; i < 32; i++ {
			left := types.Decimal256{B0_63: rng.Uint64(), B64_127: rng.Uint64(), B128_191: rng.Uint64(), B192_255: rng.Uint64() &^ (uint64(1) << 63)}
			right := types.Decimal256{B0_63: rng.Uint64(), B64_127: rng.Uint64(), B128_191: rng.Uint64(), B192_255: rng.Uint64() &^ (uint64(1) << 63)}
			want, wantOK := expectedD256ScaleDownWideTest(left, right, scaleDown)
			var got types.Decimal256
			gotOK := d256MulScaledFallback(&left, &right, scaleDown, &got)
			require.Equal(t, wantOK, gotOK, "scale=%d case=%d", scaleDown, i)
			if wantOK {
				require.Equal(t, want, got, "scale=%d case=%d", scaleDown, i)
			}
		}
	}
}

func TestD256MulHighScaleSmallValues(t *testing.T) {
	// Small coefficients must not re-enter the int32/int64 tiers for a scale
	// reduction that needs more than two 19-digit power-of-ten limbs.
	left := types.Decimal256{B0_63: 1}
	right := types.Decimal256{B0_63: 1}
	for _, scale := range []int32{39, 57, 76} {
		t.Run(strconv.FormatInt(int64(scale), 10), func(t *testing.T) {
			want, ok := expectedD256ScaleDownWideTest(left, right, scale)
			require.True(t, ok)

			vecVec := make([]types.Decimal256, 2)
			require.NoError(t, d256Mul([]types.Decimal256{left, left}, []types.Decimal256{right, right}, vecVec, scale, scale, nulls.NewWithSize(2)))
			require.Equal(t, []types.Decimal256{want, want}, vecVec)

			vecConst := make([]types.Decimal256, 2)
			require.NoError(t, d256Mul([]types.Decimal256{left, left}, []types.Decimal256{right}, vecConst, scale, scale, nulls.NewWithSize(2)))
			require.Equal(t, []types.Decimal256{want, want}, vecConst)

			constVec := make([]types.Decimal256, 2)
			partialNull := nulls.NewWithSize(2)
			partialNull.Add(1)
			require.NoError(t, d256Mul([]types.Decimal256{left}, []types.Decimal256{right, right}, constVec, scale, scale, partialNull))
			require.Equal(t, []types.Decimal256{want, {}}, constVec)

			allNull := []types.Decimal256{{B0_63: 42}, {B0_63: 43}}
			nul := nulls.NewWithSize(2)
			nul.Add(0)
			nul.Add(1)
			require.NoError(t, d256Mul([]types.Decimal256{left, left}, []types.Decimal256{right, right}, allNull, scale, scale, nul))
			require.Equal(t, []types.Decimal256{{B0_63: 42}, {B0_63: 43}}, allNull)
		})
	}
}

func TestD256MulWideMatchesBigInt(t *testing.T) {
	cases := []struct {
		left, right types.Decimal256
	}{
		{left: types.Decimal256{B0_63: 1}, right: types.Decimal256{B192_255: 1}},
		{left: types.Decimal256{B64_127: ^uint64(0), B128_191: 7}, right: types.Decimal256{B0_63: 31, B192_255: 2}},
		{left: types.Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0)}, right: types.Decimal256{B0_63: 3}},
	}
	for i, tc := range cases {
		assertD256WideProduct(t, i, tc.left, tc.right)
	}

	rng := rand.New(rand.NewSource(28875))
	for i := 0; i < 256; i++ {
		left := types.Decimal256{B0_63: rng.Uint64(), B64_127: rng.Uint64(), B128_191: rng.Uint64(), B192_255: rng.Uint64()}
		right := types.Decimal256{B0_63: rng.Uint64(), B64_127: rng.Uint64(), B128_191: rng.Uint64(), B192_255: rng.Uint64()}
		assertD256WideProduct(t, len(cases)+i, left, right)
	}
}

func assertD256WideProduct(t *testing.T, index int, left, right types.Decimal256) {
	t.Helper()
	product, sign := d256MulWide(&left, &right)
	var wantSign uint64
	if left.Sign() != right.Sign() {
		wantSign = 1
	}
	require.Equal(t, wantSign, sign, "sign[%d]", index)
	want := new(big.Int).Mul(new(big.Int).Abs(bigD256WideTest(left)), new(big.Int).Abs(bigD256WideTest(right)))
	got := new(big.Int).SetUint64(product[7])
	for limb := 6; limb >= 0; limb-- {
		got.Lsh(got, 64)
		got.Or(got, new(big.Int).SetUint64(product[limb]))
	}
	require.Equal(t, want, got, "product[%d]", index)
}
