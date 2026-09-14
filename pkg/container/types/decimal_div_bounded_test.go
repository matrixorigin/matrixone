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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecimal256DivIntermediateOverflow(t *testing.T) {
	x, err := ParseDecimal256("9223372036854775807", 65, 24)
	require.NoError(t, err)
	y, err := ParseDecimal256("1", 38, 37)
	require.NoError(t, err)
	for _, negativeX := range []bool{false, true} {
		for _, negativeY := range []bool{false, true} {
			a, b := x, y
			if negativeX {
				a = a.Minus()
			}
			if negativeY {
				b = b.Minus()
			}
			got, scale, err := a.Div(b, 24, 37)
			require.NoError(t, err)
			require.Equal(t, int32(24), scale)
			want := x
			if negativeX != negativeY {
				want = want.Minus()
			}
			require.Equal(t, want, got)
		}
	}
}

func TestDecimal256DivBoundedFallback(t *testing.T) {
	for _, tc := range []struct {
		x, y int64
		want int64
	}{
		{1, 2, 1}, {-1, 2, -1}, {1, -2, -1}, {-1, -2, 1}, {1, 3, 0}, {2, 3, 1},
	} {
		got, err := decimal256DivScaleUp(Decimal256FromInt64(tc.x), Decimal256FromInt64(tc.y), 0)
		require.NoError(t, err)
		require.Equal(t, Decimal256FromInt64(tc.want), got)
	}
	minimum := Decimal256{B192_255: uint64(1) << 63}
	got, err := decimal256DivScaleUp(minimum, Decimal256FromInt64(1), 0)
	require.NoError(t, err)
	require.Equal(t, minimum, got)
	_, err = decimal256DivScaleUp(minimum, Decimal256FromInt64(-1), 0)
	require.Error(t, err)
	_, err = decimal256DivScaleUp(Decimal256FromInt64(1), Decimal256{}, 0)
	require.Error(t, err)
	for _, scale := range []int64{-1, 154, math.MaxInt32} {
		_, err = decimal256DivScaleUp(Decimal256FromInt64(1), minimum, scale)
		require.Error(t, err)
	}
	got, err = decimal256DivScaleUp(Decimal256{}, minimum, math.MaxInt32)
	require.NoError(t, err)
	require.Equal(t, Decimal256{}, got)
	// Largest allowed power: 10^153 / 10^76 = 10^77 is a true result
	// overflow, while 10^152 / 10^76 = 10^76 remains representable.
	divisor := decimal256FromMagnitudeBigInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(76), nil))
	_, err = decimal256DivScaleUp(Decimal256FromInt64(1), divisor, 153)
	require.Error(t, err)
	got, err = decimal256DivScaleUp(Decimal256FromInt64(1), divisor, 152)
	require.NoError(t, err)
	require.Equal(t, divisor, got)
}

func BenchmarkDecimal256DivIntermediateOverflow(b *testing.B) {
	x, _ := ParseDecimal256("9223372036854775807", 65, 24)
	y, _ := ParseDecimal256("1", 38, 37)
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := x.Div(y, 24, 37); err != nil {
			b.Fatal(err)
		}
	}
}
