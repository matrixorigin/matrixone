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

package types

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// nearestEvenOf rounds v to the nearest of the sorted non-negative values vals (each with
// its code), ties to the even code: the reference for a single rounding of a float64.
func nearestEvenOf(v float64, vals []float64, codes []int) int {
	i := sort.SearchFloat64s(vals, v)
	if i == 0 {
		return codes[0]
	}
	if i == len(vals) {
		return codes[len(vals)-1]
	}
	lo, hi := vals[i-1], vals[i]
	switch {
	case v-lo < hi-v:
		return codes[i-1]
	case v-lo > hi-v:
		return codes[i]
	case codes[i-1]%2 == 0:
		return codes[i-1]
	default:
		return codes[i]
	}
}

// TestFloat32RoundToOddRoundsOnce checks that Float32RoundToOdd then the float32 constructor
// rounds a float64 once, as the nearest value of each format, ties to even.
func TestFloat32RoundToOddRoundsOnce(t *testing.T) {
	type format struct {
		name  string
		codes int
		value func(c int) float32
		round func(f float32) int
		max   float64
	}
	formats := []format{
		{"bf16", 0x7f80, func(c int) float32 { return BF16(c).ToFloat32() }, func(f float32) int { return int(BF16FromFloat32(f)) }, float64(BF16(0x7f7f).ToFloat32())},
		{"float16", 0x7c00, func(c int) float32 { return Float16(c).ToFloat32() }, func(f float32) int { return int(Float16FromFloat32(f)) }, 65504},
		{"float8", 0x7f, func(c int) float32 { return Float8(c).ToFloat32() }, func(f float32) int { return int(Float8FromFloat32(f)) }, 448},
		{"float4", 8, func(c int) float32 { return Float4(c).ToFloat32() }, func(f float32) int { return int(Float4FromFloat32(f)) }, 6},
	}
	r := rand.New(rand.NewSource(1))
	for _, f := range formats {
		vals := make([]float64, f.codes)
		codes := make([]int, f.codes)
		for c := 0; c < f.codes; c++ {
			vals[c], codes[c] = float64(f.value(c)), c
		}
		var inputs []float64
		for c := 0; c+1 < f.codes; c++ {
			lo, hi := vals[c], vals[c+1]
			mid := lo + (hi-lo)/2
			inputs = append(inputs, mid, math.Nextafter(mid, 0), math.Nextafter(mid, math.Inf(1)),
				mid+(hi-lo)*1e-9, mid-(hi-lo)*1e-9, lo+(hi-lo)*r.Float64())
		}
		for _, v := range inputs {
			if v > f.max {
				continue
			}
			want := nearestEvenOf(v, vals, codes)
			require.Equal(t, want, f.round(Float32RoundToOdd(v)), "%s %v", f.name, v)
			require.Equal(t, want|signBitOf(f.name), f.round(Float32RoundToOdd(-v))|signBitOf(f.name), "%s %v", f.name, -v)
		}
	}
	// exact values and non-finite inputs pass through
	for _, v := range []float64{0, 1, -2.5, math.MaxFloat32, math.Inf(1), math.Inf(-1)} {
		require.Equal(t, float32(v), Float32RoundToOdd(v))
	}
	require.True(t, math.IsNaN(float64(Float32RoundToOdd(math.NaN()))))
}

func signBitOf(name string) int {
	switch name {
	case "bf16", "float16":
		return 0x8000
	case "float8":
		return 0x80
	}
	return 0x08
}

// TestFloat32RoundToOddExactSources checks sources wider than float64: decimal text with
// more digits than float64 holds and integers above 2^53 round once from their exact value.
func TestFloat32RoundToOddExactSources(t *testing.T) {
	bf16 := func(f float32) float32 { return BF16FromFloat32(f).ToFloat32() }
	for _, c := range []struct {
		text string
		want float32
	}{
		{"1.00390625000000000001", 1.0078125}, // above the bf16 tie 1 + 2^-8; float64 is the tie
		{"1.00390624999999999999", 1},         // below it
		{"-1.00390625000000000001", -1.0078125},
		{"1.00390625", 1}, // the tie itself, to even
		{" 1.5 ", 1.5},
		{"0", 0},
	} {
		f, _, err := Float32RoundToOddString(c.text)
		require.NoError(t, err, c.text)
		require.Equal(t, c.want, bf16(f), c.text)
	}
	// text below the float64 range is not zero: the smallest float32 of its sign
	f, d, err := Float32RoundToOddString("1e-400")
	require.NoError(t, err)
	require.Equal(t, 0.0, d)
	require.Equal(t, float32(math.SmallestNonzeroFloat32), f)
	f, _, err = Float32RoundToOddString("-1e-400")
	require.NoError(t, err)
	require.Equal(t, float32(-math.SmallestNonzeroFloat32), f)
	_, _, err = Float32RoundToOddString("abc")
	require.Error(t, err)

	// 2^60 + 2^52 + 1 is just above the bf16 tie 2^60 + 2^52; float64(v) is the tie
	v := int64(1)<<60 + int64(1)<<52 + 1
	require.Equal(t, float32(math.Ldexp(1, 60)+math.Ldexp(1, 53)), bf16(Float32RoundToOddInt(v)))
	require.Equal(t, -float32(math.Ldexp(1, 60)+math.Ldexp(1, 53)), bf16(Float32RoundToOddInt(-v)))
	require.Equal(t, float32(math.Ldexp(1, 60)+math.Ldexp(1, 53)), bf16(Float32RoundToOddUint(uint64(v))))
	require.Equal(t, float32(1<<24-1), Float32RoundToOddUint(1<<24-1))
	// an integer exact in float64 agrees with Float32RoundToOdd of its float64
	for _, u := range []uint64{1 << 30, 1<<30 + 1, 1<<53 - 1, 12345678901234567 &^ 3} {
		require.Equal(t, Float32RoundToOdd(float64(u)), Float32RoundToOddUint(u), u)
	}
	// an out-of-range error names the input
	err = RejectNarrowFloatInput(Float32RoundToOdd(1e39), T_bf16, 1e39, "1e39")
	require.ErrorContains(t, err, "1e39")
	err = RejectNarrowFloatInput(Float32RoundToOdd(448.0000001), T_float8, 448.0000001, "448.0000001")
	require.ErrorContains(t, err, "448.0000001")
}
