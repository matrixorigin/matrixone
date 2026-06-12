// Copyright 2023 Matrix Origin
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

package metric

import (
	"math"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// resolveNarrowBytes mirrors the (removed) byte-keyed narrow resolver for these
// tests: an oid-keyed, raw-bytes-in, float64-out distance fn built on the merged
// ResolveDistanceFn. R=float64 keeps int8's int64 sum exact (the exact-match
// oracle), and errors for non-narrow oids / invalid metrics exactly as before.
func resolveNarrowBytes(oid types.T, m MetricType) (func(a, b []byte) (float64, error), error) {
	switch oid {
	case types.T_array_bf16:
		fn, err := ResolveDistanceFn[types.BF16, float64](m)
		if err != nil {
			return nil, err
		}
		return func(a, b []byte) (float64, error) {
			return fn(types.BytesToArray[types.BF16](a), types.BytesToArray[types.BF16](b))
		}, nil
	case types.T_array_float16:
		fn, err := ResolveDistanceFn[types.Float16, float64](m)
		if err != nil {
			return nil, err
		}
		return func(a, b []byte) (float64, error) {
			return fn(types.BytesToArray[types.Float16](a), types.BytesToArray[types.Float16](b))
		}, nil
	case types.T_array_int8:
		fn, err := ResolveDistanceFn[int8, float64](m)
		if err != nil {
			return nil, err
		}
		return func(a, b []byte) (float64, error) {
			return fn(types.BytesToArray[int8](a), types.BytesToArray[int8](b))
		}, nil
	case types.T_array_uint8:
		fn, err := ResolveDistanceFn[uint8, float64](m)
		if err != nil {
			return nil, err
		}
		return func(a, b []byte) (float64, error) {
			return fn(types.BytesToArray[uint8](a), types.BytesToArray[uint8](b))
		}, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("resolveNarrowBytes: not a narrow vector type")
	}
}

// reference distance over float64, mirroring ResolveDistanceFn semantics.
func refDist(metric MetricType, a, b []float64) float64 {
	switch metric {
	case Metric_L2Distance, Metric_L2sqDistance:
		var s float64
		for i := range a {
			d := a[i] - b[i]
			s += d * d
		}
		return s
	case Metric_InnerProduct:
		var s float64
		for i := range a {
			s += a[i] * b[i]
		}
		return -s
	case Metric_L1Distance:
		var s float64
		for i := range a {
			s += math.Abs(a[i] - b[i])
		}
		return s
	case Metric_CosineDistance:
		var dot, na2, nb2 float64
		for i := range a {
			dot += a[i] * b[i]
			na2 += a[i] * a[i]
			nb2 += b[i] * b[i]
		}
		den := math.Sqrt(na2) * math.Sqrt(nb2)
		if den == 0 {
			return 1.0
		}
		sim := dot / den
		if sim > 1 {
			sim = 1
		} else if sim < -1 {
			sim = -1
		}
		return 1.0 - sim
	}
	return 0
}

var narrowMetrics = []MetricType{Metric_L2Distance, Metric_L2sqDistance, Metric_InnerProduct, Metric_CosineDistance, Metric_L1Distance}

func TestNarrowInt8KernelsExact(t *testing.T) {
	// int8 values -> exact integer arithmetic, must match float64 reference exactly.
	a := []int8{1, -2, 3, -4, 5, -6, 7, -8, 9, -10, 11}
	b := []int8{-1, 2, -3, 4, 0, 6, -7, 8, -9, 1, 2}
	af := make([]float64, len(a))
	bf := make([]float64, len(b))
	for i := range a {
		af[i] = float64(a[i])
		bf[i] = float64(b[i])
	}
	ab := types.ArrayToBytes(a)
	bb := types.ArrayToBytes(b)
	for _, m := range narrowMetrics {
		fn, err := resolveNarrowBytes(types.T_array_int8, m)
		if err != nil {
			t.Fatalf("resolve int8 m=%d: %v", m, err)
		}
		got, err := fn(ab, bb)
		if err != nil {
			t.Fatalf("int8 dist m=%d: %v", m, err)
		}
		want := refDist(m, af, bf)
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("int8 m=%d: got %v want %v", m, got, want)
		}
	}
}

func TestNarrowUint8KernelsExact(t *testing.T) {
	// uint8 values (0..255) -> exact integer arithmetic, must match float64 reference.
	a := []uint8{1, 2, 3, 4, 5, 250, 7, 8, 9, 10, 255}
	b := []uint8{255, 2, 0, 4, 100, 6, 7, 200, 9, 1, 2}
	af := make([]float64, len(a))
	bf := make([]float64, len(b))
	for i := range a {
		af[i] = float64(a[i])
		bf[i] = float64(b[i])
	}
	ab := types.ArrayToBytes(a)
	bb := types.ArrayToBytes(b)
	for _, m := range narrowMetrics {
		fn, err := resolveNarrowBytes(types.T_array_uint8, m)
		if err != nil {
			t.Fatalf("resolve uint8 m=%d: %v", m, err)
		}
		got, err := fn(ab, bb)
		if err != nil {
			t.Fatalf("uint8 dist m=%d: %v", m, err)
		}
		want := refDist(m, af, bf)
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("uint8 m=%d: got %v want %v", m, got, want)
		}
	}
}

func TestNarrowBF16F16Kernels(t *testing.T) {
	src1 := []float32{1, 2, 3, 0.5, -4, 6, 7.5, -8, 9, 10, 11}
	src2 := []float32{-1, 2, 0.25, 4, 5, 6, -7, 8, -9, 1, 2}
	// bf16
	bf1 := types.Float32ToBF16Slice(src1)
	bf2 := types.Float32ToBF16Slice(src2)
	af := types.BF16ToFloat32Slice(bf1)
	bf := types.BF16ToFloat32Slice(bf2)
	af64 := f32to64(af)
	bf64 := f32to64(bf)
	for _, m := range narrowMetrics {
		fn, _ := resolveNarrowBytes(types.T_array_bf16, m)
		got, err := fn(types.ArrayToBytes(bf1), types.ArrayToBytes(bf2))
		if err != nil {
			t.Fatalf("bf16 m=%d: %v", m, err)
		}
		want := refDist(m, af64, bf64)
		if math.Abs(got-want) > 1e-4 {
			t.Errorf("bf16 m=%d: got %v want %v", m, got, want)
		}
	}
	// f16
	h1 := types.Float32ToFloat16Slice(src1)
	h2 := types.Float32ToFloat16Slice(src2)
	haf := f32to64(types.Float16ToFloat32Slice(h1))
	hbf := f32to64(types.Float16ToFloat32Slice(h2))
	for _, m := range narrowMetrics {
		fn, _ := resolveNarrowBytes(types.T_array_float16, m)
		got, err := fn(types.ArrayToBytes(h1), types.ArrayToBytes(h2))
		if err != nil {
			t.Fatalf("f16 m=%d: %v", m, err)
		}
		want := refDist(m, haf, hbf)
		if math.Abs(got-want) > 1e-4 {
			t.Errorf("f16 m=%d: got %v want %v", m, got, want)
		}
	}
}

func TestNarrowResolveErrors(t *testing.T) {
	if _, err := resolveNarrowBytes(types.T_array_float32, Metric_L2Distance); err == nil {
		t.Errorf("expected error for non-narrow oid")
	}
	if _, err := resolveNarrowBytes(types.T_array_int8, MetricType(999)); err == nil {
		t.Errorf("expected error for invalid metric")
	}
}

func f32to64(s []float32) []float64 {
	out := make([]float64, len(s))
	for i, v := range s {
		out[i] = float64(v)
	}
	return out
}

func TestF16FastExhaustive(t *testing.T) {
	for u := 0; u < 65536; u++ {
		h := types.Float16(uint16(u))
		want := h.ToFloat32()
		got := f16fast(h)
		if math.IsNaN(float64(want)) {
			if !math.IsNaN(float64(got)) {
				t.Fatalf("h=0x%04x: want NaN, got %v", u, got)
			}
			continue
		}
		if math.Float32bits(got) != math.Float32bits(want) {
			t.Fatalf("h=0x%04x: f16fast=%v (0x%08x) ToFloat32=%v (0x%08x)",
				u, got, math.Float32bits(got), want, math.Float32bits(want))
		}
	}
}

func TestNarrowKernelEdgeCases(t *testing.T) {
	narrowOids := []types.T{types.T_array_bf16, types.T_array_float16, types.T_array_int8}

	// dimension mismatch -> error on every metric/type.
	for _, oid := range narrowOids {
		for _, m := range narrowMetrics {
			fn, err := resolveNarrowBytes(oid, m)
			require.NoError(t, err)
			var a, b []byte
			switch oid {
			case types.T_array_int8:
				a = types.ArrayToBytes([]int8{1, 2, 3})
				b = types.ArrayToBytes([]int8{1, 2})
			case types.T_array_bf16:
				a = types.ArrayToBytes(types.Float32ToBF16Slice([]float32{1, 2, 3}))
				b = types.ArrayToBytes(types.Float32ToBF16Slice([]float32{1, 2}))
			default:
				a = types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{1, 2, 3}))
				b = types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{1, 2}))
			}
			_, err = fn(a, b)
			require.Errorf(t, err, "oid=%d metric=%d dim mismatch", oid, m)
		}
	}

	// empty vectors: distance 0 for all metrics/types (cosine has an explicit
	// empty guard; the rest sum nothing).
	for _, oid := range narrowOids {
		for _, m := range narrowMetrics {
			fn, _ := resolveNarrowBytes(oid, m)
			got, err := fn(nil, nil)
			require.NoError(t, err)
			require.InDeltaf(t, 0.0, got, 1e-9, "oid=%d metric=%d empty", oid, m)
		}
	}

	// cosine of a zero vector -> 1.0 (denominator 0).
	for _, oid := range narrowOids {
		fn, _ := resolveNarrowBytes(oid, Metric_CosineDistance)
		var z []byte
		switch oid {
		case types.T_array_int8:
			z = types.ArrayToBytes([]int8{0, 0, 0, 0})
		case types.T_array_bf16:
			z = types.ArrayToBytes(types.Float32ToBF16Slice([]float32{0, 0, 0, 0}))
		default:
			z = types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{0, 0, 0, 0}))
		}
		got, err := fn(z, z)
		require.NoError(t, err)
		require.InDeltaf(t, 1.0, got, 1e-9, "oid=%d zero cosine", oid)
	}

	// int8 extremes over a large dimension: integer accumulation must not overflow.
	// L2: dim * (127-(-128))^2 = 1000 * 255^2 = 65025000, exact in int64.
	dim := 1000
	amax := make([]int8, dim)
	amin := make([]int8, dim)
	for i := range amax {
		amax[i] = 127
		amin[i] = -128
	}
	fn, _ := resolveNarrowBytes(types.T_array_int8, Metric_L2sqDistance)
	got, err := fn(types.ArrayToBytes(amax), types.ArrayToBytes(amin))
	require.NoError(t, err)
	require.InDelta(t, float64(dim)*255.0*255.0, got, 1e-6)

	// single-element vectors work (loop-remainder path).
	for _, oid := range narrowOids {
		fn, _ := resolveNarrowBytes(oid, Metric_L2sqDistance)
		var a, b []byte
		switch oid {
		case types.T_array_int8:
			a, b = types.ArrayToBytes([]int8{3}), types.ArrayToBytes([]int8{1})
		case types.T_array_bf16:
			a, b = types.ArrayToBytes(types.Float32ToBF16Slice([]float32{3})), types.ArrayToBytes(types.Float32ToBF16Slice([]float32{1}))
		default:
			a, b = types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{3})), types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{1}))
		}
		got, err := fn(a, b)
		require.NoError(t, err)
		require.InDeltaf(t, 4.0, got, 1e-3, "oid=%d single elem", oid) // (3-1)^2
	}
}

// TestCosineDistanceClampNonNegative guards the [-1,1] similarity clamp. For two
// identical vectors the true cosine similarity is exactly 1, but sqrt(n)*sqrt(n)
// rounds a hair below n for many inputs, so dot/denom lands slightly above 1 and
// an unclamped kernel returns a tiny NEGATIVE distance (outside cosine's [0,2]
// domain). This exercises whichever kernel is active in the build (scalar in the
// default build, AVX512/AVX2 SIMD under GOEXPERIMENT=simd) — every one must clamp
// to >= 0. Random non-parallel vectors never reach sim>1, so the existing
// equivalence tests miss this; here we sample many identical vectors (fixed seed,
// several dims) so the unclamped path reliably produces negatives and fails.
func TestCosineDistanceClampNonNegative(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	const samplesPerDim = 400
	dims := []int{4, 7, 16, 17, 31, 64} // cover SIMD-block (>=16) and tail paths

	genF32 := func(dim int) []float32 {
		v := make([]float32, dim)
		for i := range v {
			v[i] = float32(r.Float64()*16 - 8)
		}
		return v
	}

	bf16, err := ResolveDistanceFn[types.BF16, float32](Metric_CosineDistance)
	require.NoError(t, err)
	f16, err := ResolveDistanceFn[types.Float16, float32](Metric_CosineDistance)
	require.NoError(t, err)
	i8, err := ResolveDistanceFn[int8, float32](Metric_CosineDistance)
	require.NoError(t, err)
	u8, err := ResolveDistanceFn[uint8, float32](Metric_CosineDistance)
	require.NoError(t, err)

	for _, dim := range dims {
		for s := 0; s < samplesPerDim; s++ {
			f := genF32(dim)

			// f32 / f64 native
			d32, err := CosineDistance(f, f)
			require.NoError(t, err)
			require.GreaterOrEqualf(t, d32, float32(0), "f32 cosine of identical vector must be >= 0 (dim=%d)", dim)

			f64v := make([]float64, dim)
			i8v := make([]int8, dim)
			u8v := make([]uint8, dim)
			for i, x := range f {
				f64v[i] = float64(x)
				i8v[i] = int8(x * 8)         // [-64,64)
				u8v[i] = uint8(x*8 + 128)    // [0,256)
			}
			d64, err := CosineDistance(f64v, f64v)
			require.NoError(t, err)
			require.GreaterOrEqualf(t, d64, float64(0), "f64 cosine of identical vector must be >= 0 (dim=%d)", dim)

			// narrow types
			bv := types.Float32ToBF16Slice(f)
			db, err := bf16(bv, bv)
			require.NoError(t, err)
			require.GreaterOrEqualf(t, db, float32(0), "bf16 cosine of identical vector must be >= 0 (dim=%d)", dim)

			hv := types.Float32ToFloat16Slice(f)
			dh, err := f16(hv, hv)
			require.NoError(t, err)
			require.GreaterOrEqualf(t, dh, float32(0), "f16 cosine of identical vector must be >= 0 (dim=%d)", dim)

			di, err := i8(i8v, i8v)
			require.NoError(t, err)
			require.GreaterOrEqualf(t, di, float32(0), "int8 cosine of identical vector must be >= 0 (dim=%d)", dim)

			du, err := u8(u8v, u8v)
			require.NoError(t, err)
			require.GreaterOrEqualf(t, du, float32(0), "uint8 cosine of identical vector must be >= 0 (dim=%d)", dim)
		}
	}
}
