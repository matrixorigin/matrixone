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
	"math/bits"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/stretchr/testify/require"
)

// ---- helpers ----

const benchN = 8192
const testBatchSize = 256

func randD64(rng *rand.Rand) types.Decimal64 {
	return types.Decimal64(rng.Int63n(2_000_000_000) - 1_000_000_000)
}

func randD128(rng *rand.Rand) types.Decimal128 {
	lo := rng.Uint64()
	hi := uint64(rng.Int63n(1000))
	if rng.Intn(2) == 0 {
		hi = ^uint64(0) - hi
	}
	return types.Decimal128{B0_63: lo, B64_127: hi}
}

func randD128Small(rng *rand.Rand) types.Decimal128 {
	v := types.Decimal128{B0_63: uint64(rng.Int63n(1_000_000_000)), B64_127: 0}
	if rng.Intn(2) == 0 {
		v = v.Minus()
	}
	return v
}

func randD256(rng *rand.Rand) types.Decimal256 {
	return types.Decimal256{
		B0_63:    rng.Uint64(),
		B64_127:  uint64(rng.Int63n(1000)),
		B128_191: 0,
		B192_255: 0,
	}
}

func randD256Small(rng *rand.Rand) types.Decimal256 {
	return types.Decimal256{B0_63: uint64(rng.Int63n(1_000_000_000))}
}

var sinkD128 types.Decimal128

// ---- Decimal64 add/sub ----

func TestD64AddSubBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	v1 := make([]types.Decimal64, testBatchSize)
	v2 := make([]types.Decimal64, testBatchSize)
	rsAdd := make([]types.Decimal64, testBatchSize)
	rsSub := make([]types.Decimal64, testBatchSize)
	for i := range v1 {
		v1[i] = randD64(rng)
		v2[i] = randD64(rng)
	}

	nul := nulls.NewWithSize(testBatchSize)
	require.NoError(t, d64Add(v1, v2, rsAdd, 2, 2, nul))
	require.NoError(t, d64Sub(v1, v2, rsSub, 2, 2, nul))

	for i := range v1 {
		wantAdd, _, err := v1[i].Add(v2[i], 2, 2)
		require.NoError(t, err)
		require.Equal(t, wantAdd, rsAdd[i], "d64Add[%d]", i)

		wantSub, _, err := v1[i].Sub(v2[i], 2, 2)
		require.NoError(t, err)
		require.Equal(t, wantSub, rsSub[i], "d64Sub[%d]", i)
	}
}

func TestD64AddSub_SameScale(t *testing.T) {
	tests := []struct {
		name  string
		x, y  types.Decimal64
		isSub bool
		want  types.Decimal64
	}{
		{"simple add", types.Decimal64(100), types.Decimal64(200), false, types.Decimal64(300)},
		{"simple sub", types.Decimal64(300), types.Decimal64(100), true, types.Decimal64(200)},
		{"add negative", types.Decimal64(100), types.Decimal64(^uint64(100) + 1), false, types.Decimal64(0)},
		{"sub negative (add)", types.Decimal64(100), types.Decimal64(^uint64(100) + 1), true, types.Decimal64(200)},
		{"zero + zero", types.Decimal64(0), types.Decimal64(0), false, types.Decimal64(0)},
		{"large values", types.Decimal64(999999999999999), types.Decimal64(1), false, types.Decimal64(1000000000000000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got types.Decimal64
			var err error
			if tt.isSub {
				got, err = tt.x.Sub64(tt.y)
			} else {
				got, err = tt.x.Add64(tt.y)
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got, "d64 op(%d, %d, sub=%v)", tt.x, tt.y, tt.isSub)
		})
	}
}

func BenchmarkD64Add_Fast(b *testing.B) {
	x := types.Decimal64(123456789)
	y := types.Decimal64(987654321)
	var r types.Decimal64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = x.Add64(y)
	}
	_ = r
}

func BenchmarkD64Add_Generic(b *testing.B) {
	x := types.Decimal64(123456789)
	y := types.Decimal64(987654321)
	var r types.Decimal64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _, _ = x.Add(y, 2, 2)
	}
	_ = r
}

func BenchmarkD64Sub_Fast(b *testing.B) {
	x := types.Decimal64(987654321)
	y := types.Decimal64(123456789)
	var r types.Decimal64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = x.Sub64(y)
	}
	_ = r
}

func BenchmarkD64Sub_Generic(b *testing.B) {
	x := types.Decimal64(987654321)
	y := types.Decimal64(123456789)
	var r types.Decimal64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _, _ = x.Sub(y, 2, 2)
	}
	_ = r
}

// ---- Decimal64 multiply ----

func TestD64MulBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	v1 := make([]types.Decimal64, testBatchSize)
	v2 := make([]types.Decimal64, testBatchSize)
	rs := make([]types.Decimal128, testBatchSize)
	for i := range v1 {
		v1[i] = randD64(rng)
		v2[i] = randD64(rng)
	}

	nul := nulls.NewWithSize(testBatchSize)
	err := d64Mul(v1, v2, rs, 2, 3, nul)
	require.NoError(t, err)

	for i := range v1 {
		x := functionUtil.ConvertD64ToD128(v1[i])
		y := functionUtil.ConvertD64ToD128(v2[i])
		want, _, err := x.Mul(y, 2, 3)
		require.NoError(t, err)
		require.Equal(t, want, rs[i], "d64Mul[%d]", i)
	}
}

func BenchmarkD64Mul_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal64, benchN)
	ys := make([]types.Decimal64, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		xs[i] = types.Decimal64(rng.Int63n(1_000_000_000) - 500_000_000)
		ys[i] = types.Decimal64(rng.Int63n(100) + 1)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			rs[i] = d64MulInline(xs[i], ys[i])
		}
	}
	_ = rs
}

func BenchmarkD64Mul_Generic(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal64, benchN)
	ys := make([]types.Decimal64, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		xs[i] = types.Decimal64(rng.Int63n(1_000_000_000) - 500_000_000)
		ys[i] = types.Decimal64(rng.Int63n(100) + 1)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			x128 := functionUtil.ConvertD64ToD128(xs[i])
			y128 := functionUtil.ConvertD64ToD128(ys[i])
			rs[i], _, _ = x128.Mul(y128, 2, 2)
		}
	}
	_ = rs
}

func BenchmarkD64Mul_Inline(b *testing.B) {
	x := types.Decimal64(123456789)
	y := types.Decimal64(98)
	var r types.Decimal128
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = d64MulInline(x, y)
	}
	_ = r
}

func BenchmarkBitsMul64(b *testing.B) {
	x := uint64(123456789)
	y := uint64(98)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hi, lo := bits.Mul64(x, y)
		sinkD128 = types.Decimal128{B0_63: lo, B64_127: hi}
	}
}

// ---- Decimal64 division ----

func TestD64DivBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	v1 := make([]types.Decimal64, testBatchSize)
	v2 := make([]types.Decimal64, testBatchSize)
	rs := make([]types.Decimal128, testBatchSize)
	for i := range v1 {
		v1[i] = randD64(rng)
		v2[i] = types.Decimal64(rng.Int63n(999) + 1)
	}

	nul := nulls.NewWithSize(testBatchSize)
	err := d64Div(v1, v2, rs, 2, 2, nul, true)
	require.NoError(t, err)

	for i := range v1 {
		x := functionUtil.ConvertD64ToD128(v1[i])
		y := functionUtil.ConvertD64ToD128(v2[i])
		want, _, err := x.Div(y, 2, 2)
		require.NoError(t, err)
		require.Equal(t, want, rs[i], "d64Div[%d]", i)
	}
}

func BenchmarkD64Div_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal64, benchN)
	ys := make([]types.Decimal64, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		xs[i] = randD64(rng)
		ys[i] = types.Decimal64(rng.Int63n(999) + 1)
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d64Div(xs, ys, rs, 2, 2, nul, true)
	}
}

func BenchmarkD64Div_Generic(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal64, benchN)
	ys := make([]types.Decimal64, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		xs[i] = randD64(rng)
		ys[i] = types.Decimal64(rng.Int63n(999) + 1)
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			x128 := functionUtil.ConvertD64ToD128(xs[i])
			y128 := functionUtil.ConvertD64ToD128(ys[i])
			rs[i], _, _ = x128.Div(y128, 2, 2)
		}
	}
}

// ---- Decimal128 add/sub ----

func TestD128AddSubBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	v1 := make([]types.Decimal128, testBatchSize)
	v2 := make([]types.Decimal128, testBatchSize)
	rsAdd := make([]types.Decimal128, testBatchSize)
	rsSub := make([]types.Decimal128, testBatchSize)
	for i := range v1 {
		v1[i] = randD128(rng)
		v2[i] = randD128(rng)
	}

	nul := nulls.NewWithSize(testBatchSize)
	require.NoError(t, d128Add(v1, v2, rsAdd, 2, 2, nul))
	require.NoError(t, d128Sub(v1, v2, rsSub, 2, 2, nul))

	for i := range v1 {
		wantAdd, _, err := v1[i].Add(v2[i], 2, 2)
		require.NoError(t, err)
		require.Equal(t, wantAdd, rsAdd[i], "d128Add[%d]", i)

		wantSub, _, err := v1[i].Sub(v2[i], 2, 2)
		require.NoError(t, err)
		require.Equal(t, wantSub, rsSub[i], "d128Sub[%d]", i)
	}
}

func BenchmarkD128Add_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal128, benchN)
	ys := make([]types.Decimal128, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		xs[i] = randD128(rng)
		ys[i] = randD128(rng)
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d128Add(xs, ys, rs, 2, 2, nul)
	}
}

func BenchmarkD128Add_Generic(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal128, benchN)
	ys := make([]types.Decimal128, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		xs[i] = randD128(rng)
		ys[i] = randD128(rng)
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			rs[i], _, _ = xs[i].Add(ys[i], 2, 2)
		}
	}
}

// ---- Decimal128 multiply ----

func TestD128MulBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	v1 := make([]types.Decimal128, testBatchSize)
	v2 := make([]types.Decimal128, testBatchSize)
	rs := make([]types.Decimal128, testBatchSize)
	for i := range v1 {
		v1[i] = randD128Small(rng)
		v2[i] = randD128Small(rng)
	}

	nul := nulls.NewWithSize(testBatchSize)
	err := d128Mul(v1, v2, rs, 2, 3, nul)
	require.NoError(t, err)

	for i := range v1 {
		want, _, err := v1[i].Mul(v2[i], 2, 3)
		require.NoError(t, err)
		require.Equal(t, want, rs[i], "d128Mul[%d]", i)
	}
}

func BenchmarkD128Mul_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal128, benchN)
	ys := make([]types.Decimal128, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		xs[i] = randD128Small(rng)
		ys[i] = randD128Small(rng)
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d128Mul(xs, ys, rs, 2, 3, nul)
	}
}

func BenchmarkD128Mul_Generic(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal128, benchN)
	ys := make([]types.Decimal128, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		xs[i] = randD128Small(rng)
		ys[i] = randD128Small(rng)
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			rs[i], _, _ = xs[i].Mul(ys[i], 2, 3)
		}
	}
}

// ---- Decimal128 division ----

func TestD128DivBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(6))
	v1 := make([]types.Decimal128, testBatchSize)
	v2 := make([]types.Decimal128, testBatchSize)
	rs := make([]types.Decimal128, testBatchSize)
	for i := range v1 {
		v1[i] = randD128(rng)
		v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1), B64_127: 0}
	}

	nul := nulls.NewWithSize(testBatchSize)
	err := d128Div(v1, v2, rs, 2, 2, nul, true)
	require.NoError(t, err)

	for i := range v1 {
		want, _, err := v1[i].Div(v2[i], 2, 2)
		require.NoError(t, err)
		require.Equal(t, want, rs[i], "d128Div[%d]", i)
	}
}

func BenchmarkD128Div_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal128, benchN)
	ys := make([]types.Decimal128, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		xs[i] = randD128(rng)
		ys[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1), B64_127: 0}
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d128Div(xs, ys, rs, 2, 2, nul, true)
	}
}

func BenchmarkD128Div_Generic(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal128, benchN)
	ys := make([]types.Decimal128, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		xs[i] = randD128(rng)
		ys[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1), B64_127: 0}
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			rs[i], _, _ = xs[i].Div(ys[i], 2, 2)
		}
	}
}

// ---- Decimal256 add/sub ----

func TestD256AddSubBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	v1 := make([]types.Decimal256, testBatchSize)
	v2 := make([]types.Decimal256, testBatchSize)
	rsAdd := make([]types.Decimal256, testBatchSize)
	rsSub := make([]types.Decimal256, testBatchSize)
	for i := range v1 {
		v1[i] = randD256(rng)
		v2[i] = randD256(rng)
	}

	nul := nulls.NewWithSize(testBatchSize)
	require.NoError(t, d256Add(v1, v2, rsAdd, 2, 2, nul))
	require.NoError(t, d256Sub(v1, v2, rsSub, 2, 2, nul))

	for i := range v1 {
		wantAdd, _, err := v1[i].Add(v2[i], 2, 2)
		require.NoError(t, err)
		require.Equal(t, wantAdd, rsAdd[i], "d256Add[%d]", i)

		wantSub, _, err := v1[i].Sub(v2[i], 2, 2)
		require.NoError(t, err)
		require.Equal(t, wantSub, rsSub[i], "d256Sub[%d]", i)
	}
}

func BenchmarkD256Add_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal256, benchN)
	ys := make([]types.Decimal256, benchN)
	rs := make([]types.Decimal256, benchN)
	for i := range xs {
		xs[i] = randD256(rng)
		ys[i] = randD256(rng)
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d256Add(xs, ys, rs, 2, 2, nul)
	}
}

func BenchmarkD256Add_Generic(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal256, benchN)
	ys := make([]types.Decimal256, benchN)
	rs := make([]types.Decimal256, benchN)
	for i := range xs {
		xs[i] = randD256(rng)
		ys[i] = randD256(rng)
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			rs[i], _, _ = xs[i].Add(ys[i], 2, 2)
		}
	}
}

// ---- Decimal256 multiply ----

func TestD256MulBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(8))
	v1 := make([]types.Decimal256, testBatchSize)
	v2 := make([]types.Decimal256, testBatchSize)
	rs := make([]types.Decimal256, testBatchSize)
	for i := range v1 {
		v1[i] = randD256Small(rng)
		v2[i] = randD256Small(rng)
	}

	nul := nulls.NewWithSize(testBatchSize)
	err := d256Mul(v1, v2, rs, 2, 3, nul)
	require.NoError(t, err)

	for i := range v1 {
		want, _, err := v1[i].Mul(v2[i], 2, 3)
		require.NoError(t, err)
		require.Equal(t, want, rs[i], "d256Mul[%d]", i)
	}
}

func BenchmarkD256Mul_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal256, benchN)
	ys := make([]types.Decimal256, benchN)
	rs := make([]types.Decimal256, benchN)
	for i := range xs {
		xs[i] = randD256Small(rng)
		ys[i] = randD256Small(rng)
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d256Mul(xs, ys, rs, 2, 3, nul)
	}
}

func BenchmarkD256Mul_Generic(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal256, benchN)
	ys := make([]types.Decimal256, benchN)
	rs := make([]types.Decimal256, benchN)
	for i := range xs {
		xs[i] = randD256Small(rng)
		ys[i] = randD256Small(rng)
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			rs[i], _, _ = xs[i].Mul(ys[i], 2, 3)
		}
	}
}

// ---- Decimal256 division ----

func TestD256DivBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	v1 := make([]types.Decimal256, testBatchSize)
	v2 := make([]types.Decimal256, testBatchSize)
	rs := make([]types.Decimal256, testBatchSize)
	for i := range v1 {
		v1[i] = randD256(rng)
		v2[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
	}

	nul := nulls.NewWithSize(testBatchSize)
	err := d256Div(v1, v2, rs, 2, 2, nul, true)
	require.NoError(t, err)

	for i := range v1 {
		want, _, err := v1[i].Div(v2[i], 2, 2)
		require.NoError(t, err)
		require.Equal(t, want, rs[i], "d256Div[%d]", i)
	}
}

func BenchmarkD256Div_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal256, benchN)
	ys := make([]types.Decimal256, benchN)
	rs := make([]types.Decimal256, benchN)
	for i := range xs {
		xs[i] = randD256(rng)
		ys[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d256Div(xs, ys, rs, 2, 2, nul, true)
	}
}

func BenchmarkD256Div_Generic(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal256, benchN)
	ys := make([]types.Decimal256, benchN)
	rs := make([]types.Decimal256, benchN)
	for i := range xs {
		xs[i] = randD256(rng)
		ys[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			rs[i], _, _ = xs[i].Div(ys[i], 2, 2)
		}
	}
}

// ---- Edge cases ----

func TestDivByZero_NullBehavior(t *testing.T) {
	v1 := []types.Decimal128{{B0_63: 100, B64_127: 0}}
	v2 := []types.Decimal128{{B0_63: 0, B64_127: 0}}
	rs := make([]types.Decimal128, 1)
	nul := nulls.NewWithSize(1)

	err := d128Div(v1, v2, rs, 2, 2, nul, false)
	require.NoError(t, err)
	require.True(t, nul.Contains(0), "div by zero should set null")
}
