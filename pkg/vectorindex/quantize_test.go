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

package vectorindex

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestQuantizationToVectorType(t *testing.T) {
	cases := []struct {
		in   string
		want types.T
		ok   bool
	}{
		{"float32", types.T_array_float32, true},
		{"float16", types.T_array_float16, true},
		{"bf16", types.T_array_bf16, true},
		{"int8", types.T_array_int8, true},
		// case-insensitive + surrounding space
		{"FLOAT16", types.T_array_float16, true},
		{"BF16", types.T_array_bf16, true},
		{"  Int8  ", types.T_array_int8, true},
		// not quantization targets
		{"uint8", 0, false},
		{"float64", 0, false},
		{"f16", 0, false}, // only canonical names
		{"bfloat16", 0, false},
		{"", 0, false},
		{"garbage", 0, false},
	}
	for _, c := range cases {
		got, ok := QuantizationToVectorType(c.in)
		require.Equalf(t, c.ok, ok, "ok for %q", c.in)
		if c.ok {
			require.Equalf(t, c.want, got, "type for %q", c.in)
		}
	}
}

func TestQuantizationSQLTypeName(t *testing.T) {
	require.Equal(t, "vecf32", QuantizationSQLTypeName(types.T_array_float32))
	require.Equal(t, "vecf64", QuantizationSQLTypeName(types.T_array_float64))
	require.Equal(t, "vecbf16", QuantizationSQLTypeName(types.T_array_bf16))
	require.Equal(t, "vecf16", QuantizationSQLTypeName(types.T_array_float16))
	require.Equal(t, "vecint8", QuantizationSQLTypeName(types.T_array_int8))
	require.Equal(t, "", QuantizationSQLTypeName(types.T_int32))
}

func TestInt8QuantizeParams(t *testing.T) {
	// q(x) = round(x*mul + add) must map min -> -128 and max -> +127.
	min, max := -2.0, 6.0
	mul, add := Int8QuantizeParams(min, max)
	qmin := min*mul + add
	qmax := max*mul + add
	require.InDelta(t, -128.0, qmin, 1e-6)
	require.InDelta(t, 127.0, qmax, 1e-6)
	// midpoint maps near 0 (the [-128,127] center is -0.5)
	mid := (min + max) / 2 * mul + add
	require.InDelta(t, -0.5, mid, 1e-6)

	// asymmetric (all-positive) range still spans the full grid.
	mul, add = Int8QuantizeParams(0.07, 0.83)
	require.InDelta(t, -128.0, 0.07*mul+add, 1e-6)
	require.InDelta(t, 127.0, 0.83*mul+add, 1e-6)

	// degenerate range -> identity (no panic / no inf).
	mul, add = Int8QuantizeParams(1.0, 1.0)
	require.Equal(t, 1.0, mul)
	require.Equal(t, 0.0, add)
	mul, add = Int8QuantizeParams(5.0, 1.0)
	require.Equal(t, 1.0, mul)
	require.Equal(t, 0.0, add)
	require.False(t, math.IsInf(mul, 0))
}

func TestInt8QuantizeParamsEdgeCases(t *testing.T) {
	// Across a variety of ranges, q(min) must hit -128 and q(max) must hit +127.
	ranges := [][2]float64{
		{-10, -2},     // all-negative
		{-5, 5},       // symmetric about 0
		{0.999, 1.001}, // tiny range near 1
		{-1e6, 1e6},   // huge range
		{0, 255},      // exactly the int8-span width
	}
	for _, r := range ranges {
		mul, add := Int8QuantizeParams(r[0], r[1])
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
	mul, add := Int8QuantizeParams(min, max)
	step := (max - min) / 255.0
	for _, x := range []float64{-3, -1.5, 0, 2.2, 6.99} {
		q := math.Round(x*mul + add)
		deq := (q - add) / mul
		require.InDeltaf(t, x, deq, step, "round-trip x=%v", x)
	}
}
