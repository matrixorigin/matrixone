// Copyright 2021 - 2024 Matrix Origin
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
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

var blockScaledFormats = []BlockScaledFormat{BlockScaledMXFP8, BlockScaledNVFP4}

func mustBlockScaled(t *testing.T, f BlockScaledFormat, v []float32) []byte {
	t.Helper()
	cell, err := AppendBlockScaled(nil, f, v)
	require.NoError(t, err)
	return cell
}

func TestBlockScaledCellLayout(t *testing.T) {
	for _, tc := range []struct {
		f         BlockScaledFormat
		dim       int
		scales    int
		elemBytes int
	}{
		{BlockScaledMXFP8, 1, 1, 1},
		{BlockScaledMXFP8, 32, 1, 32},
		{BlockScaledMXFP8, 33, 2, 33},
		{BlockScaledMXFP8, 1024, 32, 1024},
		{BlockScaledNVFP4, 1, 1, 1},
		{BlockScaledNVFP4, 16, 1, 8},
		{BlockScaledNVFP4, 17, 2, 9},
		{BlockScaledNVFP4, 1024, 64, 512},
	} {
		v := make([]float32, tc.dim)
		for i := range v {
			v[i] = float32(i%7) - 3
		}
		cell := mustBlockScaled(t, tc.f, v)
		require.Equal(t, BlockScaledHeaderSize+tc.scales+tc.elemBytes, len(cell), "%s(%d)", tc.f, tc.dim)
		require.Equal(t, len(cell), BlockScaledCellSize(tc.f, tc.dim))
		require.Equal(t, byte(blockScaledVersion), cell[0])
		require.Equal(t, byte(tc.f), cell[1])
		require.Equal(t, []byte{0, 0}, cell[2:4])
		require.Equal(t, uint32(tc.dim), binary.LittleEndian.Uint32(cell[4:8]))

		c, err := ParseBlockScaledCell(cell)
		require.NoError(t, err)
		require.Equal(t, tc.f, c.Format)
		require.Equal(t, tc.dim, c.Dim)
		require.Len(t, c.Scales, tc.scales)
		require.Len(t, c.Elems, tc.elemBytes)
	}
}

func TestBlockScaledAppendKeepsPrefix(t *testing.T) {
	prefix := []byte{0xaa, 0xbb}
	out, err := AppendBlockScaled(prefix, BlockScaledNVFP4, []float32{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, prefix, out[:2])
	_, err = ParseBlockScaledCell(out[2:])
	require.NoError(t, err)
}

// Values that are exactly representable round-trip exactly.
func TestBlockScaledExactValues(t *testing.T) {
	// vecf8: e4m3 values times one power of two per block.
	v8 := []float32{448, -448, 1.5, -0.125, 0, 3.5, 0.015625, -240}
	got, err := BlockScaledToFloat32(mustBlockScaled(t, BlockScaledMXFP8, v8))
	require.NoError(t, err)
	require.Equal(t, v8, got)

	// vecf4: amax 6 gives g = 6/2688 and scale 448, so g*scale = 1 and e2m1 values are exact.
	v4 := []float32{6, -6, 0.5, -1.5, 0, 3, 4, -2, 1}
	got, err = BlockScaledToFloat32(mustBlockScaled(t, BlockScaledNVFP4, v4))
	require.NoError(t, err)
	require.Equal(t, v4, got)
}

func TestBlockScaledZeroVector(t *testing.T) {
	for _, f := range blockScaledFormats {
		cell := mustBlockScaled(t, f, make([]float32, 40))
		c, err := ParseBlockScaledCell(cell)
		require.NoError(t, err)
		for _, s := range c.Scales {
			require.Zero(t, s)
		}
		for _, e := range c.Elems {
			require.Zero(t, e)
		}
		got, err := BlockScaledToFloat32(cell)
		require.NoError(t, err)
		require.Equal(t, make([]float32, 40), got)
	}
}

func TestBlockScaledGlobalScale(t *testing.T) {
	v := []float32{0.25, -1000, 7, 0}
	c8, err := ParseBlockScaledCell(mustBlockScaled(t, BlockScaledMXFP8, v))
	require.NoError(t, err)
	require.Equal(t, float32(1), c8.Global)

	c4, err := ParseBlockScaledCell(mustBlockScaled(t, BlockScaledNVFP4, v))
	require.NoError(t, err)
	require.Equal(t, float32(1000.0/2688.0), c4.Global)
	// The block holding the vector maximum uses the largest E4M3 scale.
	require.Equal(t, uint8(f8e4m3MaxBits), c4.Scales[0])
}

// Element 2i is the low nibble, matching CUDA's __nv_cvt_float2_to_fp4x2.
func TestBlockScaledNVFP4NibbleOrderMatchesCUDA(t *testing.T) {
	c, err := ParseBlockScaledCell(mustBlockScaled(t, BlockScaledNVFP4, []float32{1, 6}))
	require.NoError(t, err)
	require.Equal(t, uint8(f8e4m3MaxBits), c.Scales[0])
	require.Equal(t, uint8(cudaFloat4PairOneSix), c.Elems[0])
}

func TestBlockScaledOddDimPadding(t *testing.T) {
	cell := mustBlockScaled(t, BlockScaledNVFP4, []float32{1, 2, -3})
	c, err := ParseBlockScaledCell(cell)
	require.NoError(t, err)
	require.Len(t, c.Elems, 2)
	require.Zero(t, c.Elems[1]>>4)
}

// Dequantization error is bounded by the format's rounding step at every magnitude.
func TestBlockScaledRoundTripError(t *testing.T) {
	rng := rand.New(rand.NewSource(20567))
	for _, magnitude := range []float64{1e-30, 1e-6, 1, 3000, 1e20, 1e30} {
		for _, dim := range []int{1, 15, 16, 31, 32, 33, 768, 1000} {
			v := make([]float32, dim)
			for i := range v {
				v[i] = float32(rng.NormFloat64() * magnitude)
			}
			for _, f := range blockScaledFormats {
				got, err := BlockScaledToFloat32(mustBlockScaled(t, f, v))
				require.NoError(t, err)
				bs := f.BlockSize()
				for b := 0; b < (dim+bs-1)/bs; b++ {
					lo, hi := b*bs, min((b+1)*bs, dim)
					var amax float64
					for _, x := range v[lo:hi] {
						amax = math.Max(amax, math.Abs(float64(x)))
					}
					for i := lo; i < hi; i++ {
						diff := math.Abs(float64(got[i]) - float64(v[i]))
						var bound float64
						if f == BlockScaledMXFP8 {
							// e4m3 half-ulp (1/16 relative) plus the subnormal half-step of a
							// power-of-two scale below 2*amax/448.
							bound = math.Abs(float64(v[i]))/16 + amax/448*2*math.Exp2(-10)
						} else {
							// e2m1 largest half-gap (1 of 6) with the E4M3 scale rounded up by
							// at most 1/8.
							bound = amax * 1.125 / 6
						}
						require.LessOrEqual(t, diff, bound*(1+1e-6)+1e-45,
							"%s dim=%d mag=%g i=%d v=%g got=%g", f, dim, magnitude, i, v[i], got[i])
					}
				}
			}
		}
	}
}

func TestBlockScaledStringRoundTrip(t *testing.T) {
	// Exact in both formats: amax 6 makes the vecf4 unit g*scale = 1.
	for _, f := range blockScaledFormats {
		cell, err := StringToBlockScaled(f, "[1, -3, 0, 6]")
		require.NoError(t, err)
		s, err := BlockScaledToString(cell)
		require.NoError(t, err)
		require.Equal(t, "[1, -3, 0, 6]", s)
	}
	// Lossy in vecf4: amax 4 makes the unit 4/6, and -2.5 = -3.75 units rounds to -4 units.
	for f, want := range map[BlockScaledFormat]string{
		BlockScaledMXFP8: "[1, -2.5, 0, 4]",
		BlockScaledNVFP4: "[1, -2.6666667, 0, 4]",
	} {
		cell, err := StringToBlockScaled(f, "[1, -2.5, 0, 4]")
		require.NoError(t, err)
		s, err := BlockScaledToString(cell)
		require.NoError(t, err)
		require.Equal(t, want, s, f.String())
	}
	_, err := StringToBlockScaled(BlockScaledMXFP8, "[1, 2")
	require.Error(t, err)
	_, err = StringToBlockScaled(BlockScaledNVFP4, "[]")
	require.Error(t, err)
}

func TestBlockScaledAppendRejects(t *testing.T) {
	for _, f := range blockScaledFormats {
		for _, bad := range [][]float32{
			nil,
			{},
			{1, float32(math.NaN())},
			{float32(math.Inf(1))},
			{float32(math.Inf(-1)), 2},
			make([]float32, MaxArrayDimension+1),
		} {
			_, err := AppendBlockScaled(nil, f, bad)
			require.Error(t, err, "%s len=%d", f, len(bad))
		}
	}
	_, err := AppendBlockScaled(nil, BlockScaledFormat(9), []float32{1})
	require.Error(t, err)
}

func TestParseBlockScaledCellRejects(t *testing.T) {
	good8 := mustBlockScaled(t, BlockScaledMXFP8, []float32{1, 2, 3})
	good4 := mustBlockScaled(t, BlockScaledNVFP4, []float32{1, 2, 3})
	mutate := func(src []byte, fn func([]byte)) []byte {
		out := append([]byte(nil), src...)
		fn(out)
		return out
	}
	setGlobal := func(g float32) func([]byte) {
		return func(b []byte) { binary.LittleEndian.PutUint32(b[8:12], math.Float32bits(g)) }
	}
	scaleAt, elemAt := BlockScaledHeaderSize, BlockScaledHeaderSize+1
	for name, cell := range map[string][]byte{
		"empty":              nil,
		"short header":       good8[:BlockScaledHeaderSize-1],
		"truncated":          good8[:len(good8)-1],
		"trailing byte":      append(append([]byte(nil), good8...), 0),
		"version":            mutate(good8, func(b []byte) { b[0] = 2 }),
		"format":             mutate(good8, func(b []byte) { b[1] = 3 }),
		"reserved":           mutate(good8, func(b []byte) { b[3] = 1 }),
		"zero dim":           mutate(good8, func(b []byte) { binary.LittleEndian.PutUint32(b[4:8], 0) }),
		"dim over max":       mutate(good8, func(b []byte) { binary.LittleEndian.PutUint32(b[4:8], MaxArrayDimension+1) }),
		"vecf8 global not 1": mutate(good8, setGlobal(2)),
		"vecf4 global NaN":   mutate(good4, setGlobal(float32(math.NaN()))),
		"vecf4 global Inf":   mutate(good4, setGlobal(float32(math.Inf(1)))),
		"vecf4 global neg":   mutate(good4, setGlobal(-1)),
		"vecf4 global -0":    mutate(good4, setGlobal(float32(math.Copysign(0, -1)))),
		"vecf8 NaN scale":    mutate(good8, func(b []byte) { b[scaleAt] = 0xff }),
		"vecf4 signed scale": mutate(good4, func(b []byte) { b[scaleAt] |= 0x80 }),
		"vecf4 NaN scale":    mutate(good4, func(b []byte) { b[scaleAt] = 0x7f }),
		"vecf8 NaN element":  mutate(good8, func(b []byte) { b[elemAt] = 0x7f }),
		"vecf8 -NaN element": mutate(good8, func(b []byte) { b[elemAt] = 0xff }),
		"vecf4 padding":      mutate(good4, func(b []byte) { b[len(b)-1] |= 0x10 }),
	} {
		_, err := ParseBlockScaledCell(cell)
		require.Error(t, err, name)
		_, err = BlockScaledToFloat32(cell)
		require.Error(t, err, name)
		_, err = BlockScaledToString(cell)
		require.Error(t, err, name)
	}
}

func TestBlockScaledFormatString(t *testing.T) {
	require.Equal(t, "vecf8", BlockScaledMXFP8.String())
	require.Equal(t, "vecf4", BlockScaledNVFP4.String())
	require.Equal(t, "unknown(7)", BlockScaledFormat(7).String())
}

func TestE8M0ToFloat32(t *testing.T) {
	require.Equal(t, float32(1), E8M0ToFloat32(127))
	require.Equal(t, float32(0.5), E8M0ToFloat32(126))
	require.Equal(t, float32(math.Ldexp(1, 127)), E8M0ToFloat32(254))
	require.True(t, math.IsNaN(float64(E8M0ToFloat32(0xff))))
}

// e8m0ScaleFor returns the smallest E8M0 exponent with 448*2^(b-127) >= amax.
func TestE8M0ScaleForIsMinimal(t *testing.T) {
	var inputs []float32
	for k := -140; k <= 128; k++ {
		edge := float32(math.Ldexp(f8e4m3MaxNorm, k))
		if math.IsInf(float64(edge), 0) || edge == 0 {
			continue
		}
		inputs = append(inputs, edge, math.Nextafter32(edge, 0), math.Nextafter32(edge, float32(math.Inf(1))))
	}
	inputs = append(inputs, math.SmallestNonzeroFloat32, math.MaxFloat32, 1, 0.3, 1e-40)
	for _, amax := range inputs {
		if amax <= 0 || math.IsInf(float64(amax), 0) {
			continue
		}
		b := e8m0ScaleFor(amax)
		require.NotEqual(t, uint8(0xff), b, "amax=%g", amax)
		covers := func(b uint8) bool { return f8e4m3MaxNorm*float64(E8M0ToFloat32(b)) >= float64(amax) }
		require.True(t, covers(b), "amax=%g b=%d does not cover", amax, b)
		if b > 0 {
			require.False(t, covers(b-1), "amax=%g b=%d is not minimal", amax, b)
		}
	}
	require.Equal(t, uint8(0), e8m0ScaleFor(0))
}

// ue4m3ScaleFor returns the smallest non-negative E4M3 code covering target, capped at 448.
func TestUE4M3ScaleForIsMinimal(t *testing.T) {
	require.Equal(t, uint8(0), ue4m3ScaleFor(0))
	require.Equal(t, uint8(0), ue4m3ScaleFor(-1))
	require.Equal(t, uint8(f8e4m3MaxBits), ue4m3ScaleFor(448))
	require.Equal(t, uint8(f8e4m3MaxBits), ue4m3ScaleFor(448.0001))
	for c := 1; c <= f8e4m3MaxBits; c++ {
		v := float64(Float8(c).ToFloat32())
		prev := float64(Float8(c - 1).ToFloat32())
		for _, target := range []float64{v, math.Nextafter(v, 0), (v + prev) / 2, math.Nextafter(prev, math.Inf(1))} {
			if target <= 0 {
				continue
			}
			b := ue4m3ScaleFor(target)
			require.GreaterOrEqual(t, float64(Float8(b).ToFloat32()), target, "target=%g code=0x%02x", target, b)
			if b > 0 {
				require.Less(t, float64(Float8(b-1).ToFloat32()), target, "target=%g code=0x%02x not minimal", target, b)
			}
		}
	}
}

func TestBlockScaledVectorTypes(t *testing.T) {
	for _, tc := range []struct {
		oid     T
		sqlName string
		upper   string
		oidName string
		format  BlockScaledFormat
	}{
		{T_array_float8, "vecf8", "VECF8", "T_array_float8", BlockScaledMXFP8},
		{T_array_float4, "vecf4", "VECF4", "T_array_float4", BlockScaledNVFP4},
	} {
		require.Equal(t, tc.sqlName, tc.oid.ArraySQLName())
		require.Equal(t, tc.upper, tc.oid.String())
		require.Equal(t, tc.oidName, tc.oid.OidString())
		require.Equal(t, VarlenaSize, tc.oid.TypeLen())
		require.Equal(t, -24, tc.oid.FixedLength())
		require.False(t, tc.oid.IsFixedLen())

		require.True(t, tc.oid.IsBlockScaledVector())
		require.True(t, tc.oid.IsVectorType())
		require.False(t, tc.oid.IsArrayRelate())
		f, ok := tc.oid.BlockScaledFormat()
		require.True(t, ok)
		require.Equal(t, tc.format, f)

		typ := tc.oid.ToType()
		require.Equal(t, int32(VarlenaSize), typ.Size)
		require.Equal(t, int32(MaxArrayDimension), typ.Width)

		typ.Width = 1024
		require.Equal(t, tc.upper+"(1024)", typ.DescString())
		require.Equal(t, BlockScaledCellSize(tc.format, 1024), typ.ArrayCellBytes())
		require.Panics(t, func() { typ.GetArrayElementSize() })

		require.Equal(t, []byte{1, 2}, DecodeValue([]byte{1, 2}, tc.oid))
		require.Equal(t, []byte{1, 2}, EncodeValue([]byte{1, 2}, tc.oid))
	}
	require.Equal(t, T_array_float8, Types["array float8"])
	require.Equal(t, T_array_float4, Types["array float4"])
	require.Equal(t, 1068, New(T_array_float8, 1024, 0).ArrayCellBytes())
	require.Equal(t, 588, New(T_array_float4, 1024, 0).ArrayCellBytes())
}

func TestVectorTypePredicates(t *testing.T) {
	fixed := []T{T_array_float32, T_array_float64, T_array_bf16, T_array_float16, T_array_int8, T_array_uint8}
	for _, oid := range fixed {
		require.True(t, oid.IsArrayRelate(), oid.String())
		require.True(t, oid.IsVectorType(), oid.String())
		require.False(t, oid.IsBlockScaledVector(), oid.String())
		_, ok := oid.BlockScaledFormat()
		require.False(t, ok)
		typ := New(oid, 10, 0)
		require.Equal(t, 10*typ.GetArrayElementSize(), typ.ArrayCellBytes())
	}
	for _, oid := range []T{T_float32, T_float8, T_float4, T_varchar, T_json, T_int64} {
		require.False(t, oid.IsArrayRelate(), oid.String())
		require.False(t, oid.IsVectorType(), oid.String())
		require.False(t, oid.IsBlockScaledVector(), oid.String())
	}
}
