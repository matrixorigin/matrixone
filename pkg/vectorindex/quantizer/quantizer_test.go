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

package quantizer

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestToVectorType(t *testing.T) {
	cases := []struct {
		in   string
		want types.T
		ok   bool
	}{
		{"float32", types.T_array_float32, true},
		{"float16", types.T_array_float16, true},
		{"bf16", types.T_array_bf16, true},
		{"int8", types.T_array_int8, true},
		{"uint8", types.T_array_uint8, true},
		// case-insensitive + surrounding space
		{"FLOAT16", types.T_array_float16, true},
		{"BF16", types.T_array_bf16, true},
		{"  Int8  ", types.T_array_int8, true},
		{"UINT8", types.T_array_uint8, true},
		// not quantization targets
		{"float64", 0, false},
		{"f16", 0, false}, // only canonical names
		{"bfloat16", 0, false},
		{"", 0, false},
		{"garbage", 0, false},
	}
	for _, c := range cases {
		got, ok := ToVectorType(c.in)
		require.Equalf(t, c.ok, ok, "ok for %q", c.in)
		if c.ok {
			require.Equalf(t, c.want, got, "type for %q", c.in)
		}
	}
}

func TestSQLTypeName(t *testing.T) {
	require.Equal(t, "vecf32", SQLTypeName(types.T_array_float32))
	require.Equal(t, "vecf64", SQLTypeName(types.T_array_float64))
	require.Equal(t, "vecbf16", SQLTypeName(types.T_array_bf16))
	require.Equal(t, "vecf16", SQLTypeName(types.T_array_float16))
	require.Equal(t, "vecint8", SQLTypeName(types.T_array_int8))
	require.Equal(t, "", SQLTypeName(types.T_int32))
}

func TestInt8Params(t *testing.T) {
	// q(x) = round(x*mul + add) must map min -> -128 and max -> +127.
	min, max := -2.0, 6.0
	mul, add := Int8Params(min, max)
	qmin := min*mul + add
	qmax := max*mul + add
	require.InDelta(t, -128.0, qmin, 1e-6)
	require.InDelta(t, 127.0, qmax, 1e-6)
	// midpoint maps near 0 (the [-128,127] center is -0.5)
	mid := (min+max)/2*mul + add
	require.InDelta(t, -0.5, mid, 1e-6)

	// asymmetric (all-positive) range still spans the full grid.
	mul, add = Int8Params(0.07, 0.83)
	require.InDelta(t, -128.0, 0.07*mul+add, 1e-6)
	require.InDelta(t, 127.0, 0.83*mul+add, 1e-6)

	// degenerate range -> identity (no panic / no inf).
	mul, add = Int8Params(1.0, 1.0)
	require.Equal(t, 1.0, mul)
	require.Equal(t, 0.0, add)
	mul, add = Int8Params(5.0, 1.0)
	require.Equal(t, 1.0, mul)
	require.Equal(t, 0.0, add)
	require.False(t, math.IsInf(mul, 0))
}

func TestInt8ParamsEdgeCases(t *testing.T) {
	// Across a variety of ranges, q(min) must hit -128 and q(max) must hit +127.
	ranges := [][2]float64{
		{-10, -2},      // all-negative
		{-5, 5},        // symmetric about 0
		{0.999, 1.001}, // tiny range near 1
		{-1e6, 1e6},    // huge range
		{0, 255},       // exactly the int8-span width
	}
	for _, r := range ranges {
		mul, add := Int8Params(r[0], r[1])
		require.InDeltaf(t, -128.0, r[0]*mul+add, 1e-6, "min %v", r)
		require.InDeltaf(t, 127.0, r[1]*mul+add, 1e-6, "max %v", r)
		// a value inside the range stays inside [-128,127].
		mid := (r[0] + r[1]) / 2
		q := mid*mul + add
		require.GreaterOrEqualf(t, q, -128.0-1e-6, "mid in range %v", r)
		require.LessOrEqualf(t, q, 127.0+1e-6, "mid in range %v", r)
	}

	// dequant round-trip: x ~= (q - add) / mul within one quantization step.
	min, max := -3.0, 7.0
	mul, add := Int8Params(min, max)
	step := (max - min) / 255.0
	for _, x := range []float64{-3, -1.5, 0, 2.2, 6.99} {
		q := math.Round(x*mul + add)
		deq := (q - add) / mul
		require.InDeltaf(t, x, deq, step, "round-trip x=%v", x)
	}
}

func TestTrainInt8(t *testing.T) {
	// empty -> (-1, 1)
	lo, hi := TrainInt8([][]float32{})
	require.Equal(t, -1.0, lo)
	require.Equal(t, 1.0, hi)

	// uniform 0..999: P0.1 near 0, P99.9 near 999
	d := make([][]float32, 1)
	d[0] = make([]float32, 1000)
	for i := range d[0] {
		d[0][i] = float32(i)
	}
	lo, hi = TrainInt8(d)
	require.InDelta(t, 0.0, lo, 2)
	require.InDelta(t, 999.0, hi, 2)

	// degenerate (all equal) -> (v, v+1) so the range is never zero.
	lo, hi = TrainInt8([][]float32{{5, 5, 5, 5}})
	require.Equal(t, 5.0, lo)
	require.Equal(t, 6.0, hi)

	// a single extreme outlier is clipped by the P99.9 percentile.
	o := make([][]float32, 1)
	o[0] = make([]float32, 1000)
	for i := 0; i < 999; i++ {
		o[0][i] = 1.0
	}
	o[0][999] = 1e6
	_, hi = TrainInt8(o)
	require.Less(t, hi, 1e6)

	// works on float64 too (f64-base quantization): bounds are sane and ordered
	// (exact percentiles of a 4-element array are not the raw min/max).
	lo64, hi64 := TrainInt8([][]float64{{-3, -1, 1, 3}})
	require.GreaterOrEqual(t, lo64, -3.0)
	require.LessOrEqual(t, hi64, 3.0)
	require.Less(t, lo64, hi64)
}

func TestTrainInt8Edge(t *testing.T) {
	// single value -> degenerate (v, v+1)
	lo, hi := TrainInt8([][]float32{{5}})
	require.Equal(t, 5.0, lo)
	require.Equal(t, 6.0, hi)

	// all-negative data: bounds stay inside the data range and ordered.
	lo, hi = TrainInt8([][]float32{{-10, -8, -5, -3, -2}})
	require.GreaterOrEqual(t, lo, -10.0)
	require.LessOrEqual(t, hi, -2.0)
	require.Less(t, lo, hi)

	// subsampling path: > 2M values (stride > 1) must not panic and stays in range.
	big := make([][]float32, 2500)
	for i := range big {
		v := make([]float32, 1000) // 2.5M values total
		for j := range v {
			v[j] = float32((i*1000 + j) % 1000) // cycles 0..999
		}
		big[i] = v
	}
	lo, hi = TrainInt8(big)
	require.GreaterOrEqual(t, lo, 0.0)
	require.LessOrEqual(t, hi, 999.0)
	require.Less(t, lo, hi)

	// NaN/Inf are skipped: a poisoned sample still trains finite, ordered bounds.
	lo, hi = TrainInt8([][]float64{{math.NaN(), math.Inf(1), math.Inf(-1), 1, 2, 3, 4}})
	require.False(t, math.IsNaN(lo) || math.IsInf(lo, 0))
	require.False(t, math.IsNaN(hi) || math.IsInf(hi, 0))
	require.Less(t, lo, hi)
}

func TestApplyInt8(t *testing.T) {
	// identity (mul,add)=(1,0): plain round+clamp narrowing, input unchanged.
	in := []float32{-130, -1.4, 0.6, 5, 200}
	got := ApplyInt8(in, 1.0, 0.0)
	require.Equal(t, []int8{-128, -1, 1, 5, 127}, got)
	require.Equal(t, []float32{-130, -1.4, 0.6, 5, 200}, in, "input must not be mutated")

	// trained transform matches q(x)=round(x*mul+add): map [0.1,0.99] -> full range.
	mul, add := Int8Params(0.10, 0.99)
	q := ApplyInt8([]float32{0.10, 0.99, 0.50}, mul, add)
	require.Equal(t, int8(-128), q[0]) // min -> -128
	require.Equal(t, int8(127), q[1])  // max -> +127
	// 0.50 matches the float64 multiply-add, rounded.
	want := int8(math.Round(0.50*mul + add))
	require.Equal(t, want, q[2])

	// empty input -> empty output, no panic.
	require.Empty(t, ApplyInt8([]float32{}, mul, add))
}

func TestEntrySQLBuilders(t *testing.T) {
	// literal-bounds (build) projection.
	require.Equal(t,
		"cast(`v` * 286.516854 + (-156.651685) as vecint8(4))",
		Int8EntrySQL("`v`", 286.516854, -156.651685, 4))

	// metadata-subquery (CDC) projection with COALESCE identity fallback.
	min := "(SELECT m FROM meta WHERE k='quantize_min')"
	max := "(SELECT m FROM meta WHERE k='quantize_max')"
	got := Int8EntrySQLFromBounds("src1", min, max, 4)
	require.Equal(t,
		"cast(src1 * COALESCE(255.0 / ("+max+" - "+min+"), 1.0) + "+
			"COALESCE(0.0 - "+min+" * 255.0 / ("+max+" - "+min+") - 128.0, 0.0) as vecint8(4))",
		got)

	// plain narrowing cast (float formats / untrained int8).
	require.Equal(t, "cast(`v` as vecf16(8))", CastSQL("`v`", types.T_array_float16, 8))
	require.Equal(t, "cast(`v` as vecint8(8))", CastSQL("`v`", types.T_array_int8, 8))
	require.Equal(t, "cast(`v` as vecuint8(8))", CastSQL("`v`", types.T_array_uint8, 8))
}

func TestUint8Params(t *testing.T) {
	// q(x)=round(x*mul+add) must map min -> 0 and max -> 255 (unsigned range).
	min, max := -2.0, 6.0
	mul, add := Uint8Params(min, max)
	require.InDelta(t, 0.0, min*mul+add, 1e-6)
	require.InDelta(t, 255.0, max*mul+add, 1e-6)
	// midpoint maps near the center 127.5.
	require.InDelta(t, 127.5, (min+max)/2*mul+add, 1e-6)

	// all-positive range still spans the full grid.
	mul, add = Uint8Params(0.07, 0.83)
	require.InDelta(t, 0.0, 0.07*mul+add, 1e-6)
	require.InDelta(t, 255.0, 0.83*mul+add, 1e-6)

	// degenerate range -> identity.
	mul, add = Uint8Params(1.0, 1.0)
	require.Equal(t, 1.0, mul)
	require.Equal(t, 0.0, add)
}

func TestApplyUint8(t *testing.T) {
	// identity: round+clamp to [0,255], input unchanged.
	in := []float32{-5, 0.6, 5, 254.5, 300}
	got := ApplyUint8(in, 1.0, 0.0)
	require.Equal(t, []uint8{0, 1, 5, 255, 255}, got)
	require.Equal(t, []float32{-5, 0.6, 5, 254.5, 300}, in, "input must not be mutated")

	// trained transform maps [0.1,0.99] -> [0,255].
	mul, add := Uint8Params(0.10, 0.99)
	q := ApplyUint8([]float32{0.10, 0.99, 0.50}, mul, add)
	require.Equal(t, uint8(0), q[0])
	require.Equal(t, uint8(255), q[1])
	require.Equal(t, uint8(math.Round(0.50*mul+add)), q[2])

	require.Empty(t, ApplyUint8([]float32{}, mul, add))
}

func TestUint8EntrySQLBuilders(t *testing.T) {
	// literal-bounds (build) projection -> vecuint8.
	require.Equal(t,
		"cast(`v` * 286.516854 + (28.6516854) as vecuint8(4))",
		Uint8EntrySQL("`v`", 286.516854, 28.6516854, 4))

	// metadata-subquery (CDC) projection: no -128 offset, identity COALESCE fallback.
	min := "(SELECT m FROM meta WHERE k='quantize_min')"
	max := "(SELECT m FROM meta WHERE k='quantize_max')"
	require.Equal(t,
		"cast(src1 * COALESCE(255.0 / ("+max+" - "+min+"), 1.0) + "+
			"COALESCE(0.0 - "+min+" * 255.0 / ("+max+" - "+min+"), 0.0) as vecuint8(4))",
		Uint8EntrySQLFromBounds("src1", min, max, 4))
}
