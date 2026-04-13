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

func makeNulls(n int) *nulls.Nulls {
	nul := nulls.NewWithSize(n)
	for i := 0; i < n; i += 4 {
		nul.Add(uint64(i))
	}
	return nul
}

// d256SubRef is a test-only reference implementation for Decimal256 subtraction
// with scale alignment (mirrors the deleted Decimal256.Sub method).
func TestDivByZero_NullBehavior(t *testing.T) {
	v1 := []types.Decimal128{{B0_63: 100, B64_127: 0}}
	v2 := []types.Decimal128{{B0_63: 0, B64_127: 0}}
	rs := make([]types.Decimal128, 1)
	nul := nulls.NewWithSize(1)

	err := d128Div(v1, v2, rs, 2, 2, nul, false)
	require.NoError(t, err)
	require.True(t, nul.Contains(0), "div by zero should set null")
}

func TestModByZero_NullBehavior(t *testing.T) {
	// d64Mod: shouldError=false should set null
	v1d64 := []types.Decimal64{types.Decimal64(100)}
	v2d64 := []types.Decimal64{types.Decimal64(0)}
	rsd64 := make([]types.Decimal64, 1)
	nul := nulls.NewWithSize(1)
	err := d64Mod(v1d64, v2d64, rsd64, 2, 2, nul, false)
	require.NoError(t, err)
	require.True(t, nul.Contains(0), "d64Mod: mod by zero should set null")

	// d128Mod: shouldError=false should set null
	v1d128 := []types.Decimal128{{B0_63: 100}}
	v2d128 := []types.Decimal128{{B0_63: 0}}
	rsd128 := make([]types.Decimal128, 1)
	nul2 := nulls.NewWithSize(1)
	err = d128Mod(v1d128, v2d128, rsd128, 2, 2, nul2, false)
	require.NoError(t, err)
	require.True(t, nul2.Contains(0), "d128Mod: mod by zero should set null")
}

// TestD256Mul_HighScale exercises the batch-level d256ScaleDown/d256DivPow10 path.
func TestNullHandling(t *testing.T) {
	t.Run("D64Add_WithNulls", func(t *testing.T) {
		v1 := make([]types.Decimal64, 8)
		v2 := make([]types.Decimal64, 8)
		rs := make([]types.Decimal64, 8)
		for i := range v1 {
			v1[i] = types.Decimal64(int64(i*100 + 1))
			v2[i] = types.Decimal64(int64(i*10 + 1))
		}
		nul := nulls.NewWithSize(8)
		nul.Add(1)
		nul.Add(3)
		nul.Add(5)
		require.NoError(t, d64Add(v1, v2, rs, 2, 2, nul))
		for i := range v1 {
			if nul.Contains(uint64(i)) {
				continue
			}
			want, err := v1[i].Add64(v2[i])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64Add null[%d]", i)
		}
	})

	t.Run("D128Mod_WithNulls", func(t *testing.T) {
		v1 := make([]types.Decimal128, 8)
		v2 := make([]types.Decimal128, 8)
		rs := make([]types.Decimal128, 8)
		for i := range v1 {
			v1[i] = types.Decimal128{B0_63: uint64(i*100 + 1)}
			v2[i] = types.Decimal128{B0_63: uint64(i*10 + 1)}
		}
		nul := nulls.NewWithSize(8)
		nul.Add(0)
		nul.Add(4)
		nul.Add(7)
		require.NoError(t, d128Mod(v1, v2, rs, 2, 5, nul, true))
		for i := range v1 {
			if nul.Contains(uint64(i)) {
				continue
			}
			want, _, err := v1[i].Mod(v2[i], 2, 5)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128Mod null[%d]", i)
		}
	})

	t.Run("D256Div_WithNulls", func(t *testing.T) {
		v1 := make([]types.Decimal256, 8)
		v2 := make([]types.Decimal256, 8)
		rs := make([]types.Decimal256, 8)
		for i := range v1 {
			v1[i] = randD256Small(rand.New(rand.NewSource(int64(i))))
			v2[i] = types.Decimal256{B0_63: uint64(i + 1)}
		}
		nul := nulls.NewWithSize(8)
		nul.Add(2)
		nul.Add(6)
		require.NoError(t, d256Div(v1, v2, rs, 2, 2, nul, true))
		for i := range v1 {
			if nul.Contains(uint64(i)) {
				continue
			}
			want, _, err := v1[i].Div(v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256Div null[%d]", i)
		}
	})

	t.Run("D64Sub_DiffScale_WithNulls", func(t *testing.T) {
		v1 := make([]types.Decimal64, 8)
		v2 := make([]types.Decimal64, 8)
		rs := make([]types.Decimal64, 8)
		for i := range v1 {
			v1[i] = types.Decimal64(int64(i*100 + 1))
			v2[i] = types.Decimal64(int64(i*10 + 1))
		}
		nul := nulls.NewWithSize(8)
		nul.Add(0)
		nul.Add(3)
		require.NoError(t, d64Sub(v1, v2, rs, 2, 5, nul))
		for i := range v1 {
			if nul.Contains(uint64(i)) {
				continue
			}
			want, _, err := v1[i].Sub(v2[i], 2, 5)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64Sub diffscale null[%d]", i)
		}
	})
}

// TestD128Div_DiffScale tests D128 division with different scales (ScalarVec and VecScalar).
func BenchmarkBitsMul64(b *testing.B) {
	x := uint64(123456789)
	y := uint64(98)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hi, lo := bits.Mul64(x, y)
		sinkD128 = types.Decimal128{B0_63: lo, B64_127: hi}
	}
}

func randD64(rng *rand.Rand) types.Decimal64 {
	return types.Decimal64(rng.Int63n(2_000_000_000) - 1_000_000_000)
}

func TestD64Add(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(3))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		rs := make([]types.Decimal64, testBatchSize)
		for i := range v1 {
			v1[i] = randD64(rng)
			v2[i] = randD64(rng)
		}
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Add(v1, v2, rs, 2, 2, nul))
		for i := range v1 {
			wantAdd, _, err := v1[i].Add(v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, wantAdd, rs[i], "d64Add[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(30))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = randD64(rng)
		}
		scalar := []types.Decimal64{randD64(rng)}

		rs := make([]types.Decimal64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Add(scalar, vec, rs, 2, 2, nul))
		for i := range vec {
			want, err := scalar[0].Add64(vec[i])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "const-vec[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(30))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = randD64(rng)
		}
		scalar := []types.Decimal64{randD64(rng)}

		rs := make([]types.Decimal64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Add(vec, scalar, rs, 2, 2, nul))
		for i := range vec {
			want, err := vec[i].Add64(scalar[0])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "vec-const[%d]", i)
		}
	})

	t.Run("Nulls", func(t *testing.T) {
		// vec-vec with nulls
		rng := rand.New(rand.NewSource(35))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		for i := range v1 {
			v1[i] = randD64(rng)
			v2[i] = randD64(rng)
		}
		rs := make([]types.Decimal64, testBatchSize)
		nul := makeNulls(testBatchSize)
		require.NoError(t, d64Add(v1, v2, rs, 2, 2, nul))
		for i := range v1 {
			if i%4 == 0 {
				continue
			}
			want, err := v1[i].Add64(v2[i])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "add-null[%d]", i)
		}

		// const-vec with nulls
		rng2 := rand.New(rand.NewSource(30))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = randD64(rng2)
		}
		scalar := []types.Decimal64{randD64(rng2)}
		rs2 := make([]types.Decimal64, testBatchSize)
		nul2 := makeNulls(testBatchSize)
		require.NoError(t, d64Add(scalar, vec, rs2, 2, 2, nul2))
		for i := range vec {
			if i%4 == 0 {
				continue
			}
			want, err := scalar[0].Add64(vec[i])
			require.NoError(t, err)
			require.Equal(t, want, rs2[i], "const-vec-null[%d]", i)
		}
	})

	t.Run("DiffScale", func(t *testing.T) {
		rng := rand.New(rand.NewSource(10))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		rs := make([]types.Decimal64, testBatchSize)
		for i := range v1 {
			v1[i] = types.Decimal64(rng.Int63n(100_000) - 50_000)
			v2[i] = types.Decimal64(rng.Int63n(100_000) - 50_000)
		}
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Add(v1, v2, rs, 1, 3, nul))
		for i := range v1 {
			wantAdd, _, err := v1[i].Add(v2[i], 1, 3)
			require.NoError(t, err)
			require.Equal(t, wantAdd, rs[i], "d64Add DiffScale[%d]", i)
		}
	})

	t.Run("DiffScaleScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(36))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(10000) - 5000)
		}
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(10000) - 5000)}

		// scalar(scale1) + vec(scale3)
		rs := make([]types.Decimal64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Add(scalar, vec, rs, 1, 3, nul))
		for i := range vec {
			a := functionUtil.ConvertD64ToD128(scalar[0])
			b := functionUtil.ConvertD64ToD128(vec[i])
			want, _, err := a.Add(b, 1, 3)
			require.NoError(t, err)
			require.Equal(t, types.Decimal64(want.B0_63), rs[i], "diffscale const-vec[%d]", i)
		}

		// vec(scale3) + scalar(scale1)
		rs2 := make([]types.Decimal64, testBatchSize)
		nul2 := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Add(vec, scalar, rs2, 3, 1, nul2))
		for i := range vec {
			a := functionUtil.ConvertD64ToD128(vec[i])
			b := functionUtil.ConvertD64ToD128(scalar[0])
			want, _, err := a.Add(b, 3, 1)
			require.NoError(t, err)
			require.Equal(t, types.Decimal64(want.B0_63), rs2[i], "diffscale vec-const[%d]", i)
		}
	})
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

func TestD64Sub(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(3))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		rs := make([]types.Decimal64, testBatchSize)
		for i := range v1 {
			v1[i] = randD64(rng)
			v2[i] = randD64(rng)
		}
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Sub(v1, v2, rs, 2, 2, nul))
		for i := range v1 {
			wantSub, _, err := v1[i].Sub(v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, wantSub, rs[i], "d64Sub[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(31))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = randD64(rng)
		}
		scalar := []types.Decimal64{randD64(rng)}

		rs := make([]types.Decimal64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Sub(scalar, vec, rs, 2, 2, nul))
		for i := range vec {
			want, err := scalar[0].Sub64(vec[i])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "const-vec sub[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(31))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = randD64(rng)
		}
		scalar := []types.Decimal64{randD64(rng)}

		rs := make([]types.Decimal64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Sub(vec, scalar, rs, 2, 2, nul))
		for i := range vec {
			want, err := vec[i].Sub64(scalar[0])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "vec-const sub[%d]", i)
		}
	})

	t.Run("DiffScale", func(t *testing.T) {
		rng := rand.New(rand.NewSource(10))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		rs := make([]types.Decimal64, testBatchSize)
		for i := range v1 {
			v1[i] = types.Decimal64(rng.Int63n(100_000) - 50_000)
			v2[i] = types.Decimal64(rng.Int63n(100_000) - 50_000)
		}
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Sub(v1, v2, rs, 1, 3, nul))
		for i := range v1 {
			wantSub, _, err := v1[i].Sub(v2[i], 1, 3)
			require.NoError(t, err)
			require.Equal(t, wantSub, rs[i], "d64Sub DiffScale[%d]", i)
		}
	})

	t.Run("DiffScaleScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(90))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(10000) - 5000)
		}
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(10000) - 5000)}

		rs := make([]types.Decimal64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Sub(scalar, vec, rs, 1, 3, nul))
		for i := range vec {
			a := functionUtil.ConvertD64ToD128(scalar[0])
			b := functionUtil.ConvertD64ToD128(vec[i])
			want, _, err := a.Sub(b, 1, 3)
			require.NoError(t, err)
			require.Equal(t, types.Decimal64(want.B0_63), rs[i], "diffscale const-vec sub[%d]", i)
		}

		rs2 := make([]types.Decimal64, testBatchSize)
		nul2 := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Sub(vec, scalar, rs2, 3, 1, nul2))
		for i := range vec {
			a := functionUtil.ConvertD64ToD128(vec[i])
			b := functionUtil.ConvertD64ToD128(scalar[0])
			want, _, err := a.Sub(b, 3, 1)
			require.NoError(t, err)
			require.Equal(t, types.Decimal64(want.B0_63), rs2[i], "diffscale vec-const sub[%d]", i)
		}
	})
}

func TestD64Mul(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
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
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(32))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = randD64(rng)
		}
		scalar := []types.Decimal64{randD64(rng)}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Mul(scalar, vec, rs, 2, 2, nul))
		for i := range vec {
			x := functionUtil.ConvertD64ToD128(scalar[0])
			y := functionUtil.ConvertD64ToD128(vec[i])
			want, _, err := x.Mul(y, 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "const-vec mul[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(32))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = randD64(rng)
		}
		scalar := []types.Decimal64{randD64(rng)}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Mul(vec, scalar, rs, 2, 2, nul))
		for i := range vec {
			x := functionUtil.ConvertD64ToD128(vec[i])
			y := functionUtil.ConvertD64ToD128(scalar[0])
			want, _, err := x.Mul(y, 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "vec-const mul[%d]", i)
		}
	})

	t.Run("Scaled", func(t *testing.T) {
		rng := rand.New(rand.NewSource(20))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = types.Decimal64(rng.Int63n(10000) - 5000)
			v2[i] = types.Decimal64(rng.Int63n(10000) - 5000)
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d64Mul(v1, v2, rs, 8, 8, nul)
		require.NoError(t, err)
		for i := range v1 {
			x := functionUtil.ConvertD64ToD128(v1[i])
			y := functionUtil.ConvertD64ToD128(v2[i])
			want, _, err := x.Mul(y, 8, 8)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64MulScaled[%d]", i)
		}
	})
}

func TestD64Div(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
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
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(33))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(998) + 1)
		}
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(998) + 1)}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Div(scalar, vec, rs, 2, 2, nul, true))
		for i := range vec {
			x := functionUtil.ConvertD64ToD128(scalar[0])
			y := functionUtil.ConvertD64ToD128(vec[i])
			want, _, err := x.Div(y, 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "const-vec div[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(33))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(998) + 1)
		}
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(998) + 1)}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Div(vec, scalar, rs, 2, 2, nul, true))
		for i := range vec {
			x := functionUtil.ConvertD64ToD128(vec[i])
			y := functionUtil.ConvertD64ToD128(scalar[0])
			want, _, err := x.Div(y, 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "vec-const div[%d]", i)
		}
	})

	t.Run("DiffScale", func(t *testing.T) {
		rng := rand.New(rand.NewSource(16))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = types.Decimal64(rng.Int63n(100_000) - 50_000)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d64Div(v1, v2, rs, 1, 3, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			x := functionUtil.ConvertD64ToD128(v1[i])
			y := functionUtil.ConvertD64ToD128(v2[i])
			want, _, err := x.Div(y, 1, 3)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64Div DiffScale[%d]", i)
		}
	})

	t.Run("Kernel", func(t *testing.T) {
		v1 := []types.Decimal64{types.Decimal64(100)}
		v2 := []types.Decimal64{types.Decimal64(3)}
		rs := make([]types.Decimal128, 1)
		nul := nulls.NewWithSize(1)

		kernel := d64DivKernel(true)
		err := kernel(v1, v2, rs, 2, 2, nul)
		require.NoError(t, err)

		x := functionUtil.ConvertD64ToD128(v1[0])
		y := functionUtil.ConvertD64ToD128(v2[0])
		want, _, _ := x.Div(y, 2, 2)
		require.Equal(t, want, rs[0])
	})
}

func TestD64Mod(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(13))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		rs := make([]types.Decimal64, testBatchSize)
		for i := range v1 {
			v1[i] = randD64(rng)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d64Mod(v1, v2, rs, 2, 2, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Mod(v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64Mod[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(34))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(998) + 1)
		}
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(998) + 1)}

		rs := make([]types.Decimal64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Mod(scalar, vec, rs, 2, 2, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Mod(vec[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "const-vec mod[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(34))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(998) + 1)
		}
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(998) + 1)}

		rs := make([]types.Decimal64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Mod(vec, scalar, rs, 2, 2, nul, true))
		for i := range vec {
			want, _, err := vec[i].Mod(scalar[0], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "vec-const mod[%d]", i)
		}
	})

	t.Run("Kernel", func(t *testing.T) {
		v1 := []types.Decimal64{types.Decimal64(100)}
		v2 := []types.Decimal64{types.Decimal64(3)}
		rs := make([]types.Decimal64, 1)
		nul := nulls.NewWithSize(1)

		kernel := d64ModKernel(true)
		err := kernel(v1, v2, rs, 2, 2, nul)
		require.NoError(t, err)

		want, _, _ := v1[0].Mod(v2[0], 2, 2)
		require.Equal(t, want, rs[0])
	})

	t.Run("DiffScale_VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(77))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		rs := make([]types.Decimal64, testBatchSize)
		for i := range v1 {
			v1[i] = randD64(rng)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d64Mod(v1, v2, rs, 2, 5, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Mod(v2[i], 2, 5)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64Mod diffscale[%d]", i)
		}
	})

	t.Run("DiffScale_ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(78))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(998) + 1)
		}
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(998) + 1)}
		rs := make([]types.Decimal64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Mod(scalar, vec, rs, 3, 6, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Mod(vec[i], 3, 6)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64 diffscale const-vec[%d]", i)
		}
	})

	t.Run("DiffScale_VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(79))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(998) + 1)
		}
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(998) + 1)}
		rs := make([]types.Decimal64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Mod(vec, scalar, rs, 6, 3, nul, true))
		for i := range vec {
			want, _, err := vec[i].Mod(scalar[0], 6, 3)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64 diffscale vec-const[%d]", i)
		}
	})
}

// TestD128MulPow10Carry verifies that d128MulPow10 correctly detects overflow
// when the cross-product carry overflows uint64 (hi + crossLo > 2^64).
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

func BenchmarkD64AddDiffScale_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal64, benchN)
	ys := make([]types.Decimal64, benchN)
	rs := make([]types.Decimal64, benchN)
	for i := range xs {
		xs[i] = types.Decimal64(rng.Int63n(1_000_000_000) - 500_000_000)
		ys[i] = types.Decimal64(rng.Int63n(1_000_000_000) - 500_000_000)
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d64Add(xs, ys, rs, 2, 5, nul)
	}
}

func BenchmarkD64SubDiffScale_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal64, benchN)
	ys := make([]types.Decimal64, benchN)
	rs := make([]types.Decimal64, benchN)
	for i := range xs {
		xs[i] = types.Decimal64(rng.Int63n(1_000_000_000) - 500_000_000)
		ys[i] = types.Decimal64(rng.Int63n(1_000_000_000) - 500_000_000)
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d64Sub(xs, ys, rs, 2, 5, nul)
	}
}

func BenchmarkD64AddDiffScale_FastLarge(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal64, benchN)
	ys := make([]types.Decimal64, benchN)
	rs := make([]types.Decimal64, benchN)
	for i := range xs {
		// Values near D64 max (~9.2e18) — will NOT pass prescan for scaleDiff=3
		v := int64(rng.Int63n(4_000_000_000_000_000_000) + 5_000_000_000_000_000_000)
		if rng.Intn(2) == 0 {
			v = -v
		}
		xs[i] = types.Decimal64(v)
		ys[i] = types.Decimal64(rng.Int63n(1_000_000) - 500_000) // small so add doesn't overflow
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d64Add(xs, ys, rs, 2, 5, nul)
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

func BenchmarkD64MulScaled_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal64, benchN)
	ys := make([]types.Decimal64, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		xs[i] = types.Decimal64(rng.Int63n(1_000_000_000) - 500_000_000)
		ys[i] = types.Decimal64(rng.Int63n(100) + 1)
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d64Mul(xs, ys, rs, 10, 10, nul)
	}
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

func BenchmarkD64Mod_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal64, benchN)
	ys := make([]types.Decimal64, benchN)
	rs := make([]types.Decimal64, benchN)
	for i := range xs {
		xs[i] = types.Decimal64(rng.Int63())
		ys[i] = types.Decimal64(rng.Int63n(999) + 1)
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d64Mod(xs, ys, rs, 2, 2, nul, true)
	}
}

func BenchmarkD64ModDiffScale_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal64, benchN)
	ys := make([]types.Decimal64, benchN)
	rs := make([]types.Decimal64, benchN)
	for i := range xs {
		xs[i] = types.Decimal64(rng.Int63())
		ys[i] = types.Decimal64(rng.Int63n(999) + 1)
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d64Mod(xs, ys, rs, 2, 4, nul, true)
	}
}

func BenchmarkD64IntDiv_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal64, benchN)
	ys := make([]types.Decimal64, benchN)
	rs := make([]int64, benchN)
	for i := range xs {
		xs[i] = types.Decimal64(rng.Int63())
		ys[i] = types.Decimal64(rng.Int63n(999) + 1)
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d64IntDiv(xs, ys, rs, 2, 2, nul, true)
	}
}

func BenchmarkD64IntDiv_Generic(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal64, benchN)
	ys := make([]types.Decimal64, benchN)
	rs := make([]int64, benchN)
	for i := range xs {
		xs[i] = types.Decimal64(rng.Int63())
		ys[i] = types.Decimal64(rng.Int63n(999) + 1)
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			d1 := types.Decimal128{B0_63: uint64(xs[i])}
			if xs[i]>>63 != 0 {
				d1.B64_127 = ^uint64(0)
			}
			d2 := types.Decimal128{B0_63: uint64(ys[i])}
			r, rScale, _ := d1.Div(d2, 2, 2)
			if rScale > 0 {
				r, _ = r.Scale(-rScale)
			}
			rs[i], _ = decimal128ToInt64(r)
		}
	}
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

func TestD128Add(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(4))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = randD128(rng)
			v2[i] = randD128(rng)
		}
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Add(v1, v2, rs, 2, 2, nul))
		for i := range v1 {
			wantAdd, _, err := v1[i].Add(v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, wantAdd, rs[i], "d128Add[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(40))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		scalar := []types.Decimal128{randD128Small(rng)}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Add(scalar, vec, rs, 2, 2, nul))
		for i := range vec {
			want, err := scalar[0].Add128(vec[i])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 const-vec add[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(40))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		scalar := []types.Decimal128{randD128Small(rng)}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Add(vec, scalar, rs, 2, 2, nul))
		for i := range vec {
			want, err := vec[i].Add128(scalar[0])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 vec-const add[%d]", i)
		}
	})

	t.Run("Nulls", func(t *testing.T) {
		rng := rand.New(rand.NewSource(40))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		scalar := []types.Decimal128{randD128Small(rng)}

		rs := make([]types.Decimal128, testBatchSize)
		nul := makeNulls(testBatchSize)
		require.NoError(t, d128Add(vec, scalar, rs, 2, 2, nul))
		for i := range vec {
			if i%4 == 0 {
				continue
			}
			want, err := vec[i].Add128(scalar[0])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 vec-const add null[%d]", i)
		}
	})

	t.Run("DiffScale", func(t *testing.T) {
		rng := rand.New(rand.NewSource(11))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = randD128Small(rng)
		}
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Add(v1, v2, rs, 2, 5, nul))
		for i := range v1 {
			wantAdd, _, err := v1[i].Add(v2[i], 2, 5)
			require.NoError(t, err)
			require.Equal(t, wantAdd, rs[i], "d128Add DiffScale[%d]", i)
		}
	})

	t.Run("DiffScaleScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(55))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		scalar := []types.Decimal128{randD128Small(rng)}

		// scalar(scale1) + vec(scale3)
		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Add(scalar, vec, rs, 1, 3, nul))
		for i := range vec {
			want, _, err := scalar[0].Add(vec[i], 1, 3)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 diffscale const-vec add[%d]", i)
		}

		// vec(scale3) + scalar(scale1)
		rs2 := make([]types.Decimal128, testBatchSize)
		nul2 := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Add(vec, scalar, rs2, 3, 1, nul2))
		for i := range vec {
			want, _, err := vec[i].Add(scalar[0], 3, 1)
			require.NoError(t, err)
			require.Equal(t, want, rs2[i], "d128 diffscale vec-const add[%d]", i)
		}
	})
}

func TestD128Sub(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(4))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = randD128(rng)
			v2[i] = randD128(rng)
		}
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Sub(v1, v2, rs, 2, 2, nul))
		for i := range v1 {
			wantSub, _, err := v1[i].Sub(v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, wantSub, rs[i], "d128Sub[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(41))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		scalar := []types.Decimal128{randD128Small(rng)}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Sub(scalar, vec, rs, 2, 2, nul))
		for i := range vec {
			want, err := scalar[0].Sub128(vec[i])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 const-vec sub[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(41))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		scalar := []types.Decimal128{randD128Small(rng)}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Sub(vec, scalar, rs, 2, 2, nul))
		for i := range vec {
			want, err := vec[i].Sub128(scalar[0])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 vec-const sub[%d]", i)
		}
	})

	t.Run("DiffScale", func(t *testing.T) {
		rng := rand.New(rand.NewSource(11))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = randD128Small(rng)
		}
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Sub(v1, v2, rs, 2, 5, nul))
		for i := range v1 {
			wantSub, _, err := v1[i].Sub(v2[i], 2, 5)
			require.NoError(t, err)
			require.Equal(t, wantSub, rs[i], "d128Sub DiffScale[%d]", i)
		}
	})

	t.Run("DiffScaleScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(55))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		scalar := []types.Decimal128{randD128Small(rng)}

		// scalar(scale1) - vec(scale3)
		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Sub(scalar, vec, rs, 1, 3, nul))
		for i := range vec {
			want, _, err := scalar[0].Sub(vec[i], 1, 3)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 diffscale const-vec sub[%d]", i)
		}

		// vec(scale3) - scalar(scale1)
		rs2 := make([]types.Decimal128, testBatchSize)
		nul2 := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Sub(vec, scalar, rs2, 3, 1, nul2))
		for i := range vec {
			want, _, err := vec[i].Sub(scalar[0], 3, 1)
			require.NoError(t, err)
			require.Equal(t, want, rs2[i], "d128 diffscale vec-const sub[%d]", i)
		}
	})
}

func TestD128MulPow10Carry(t *testing.T) {
	// x = {B0_63: MaxUint64, B64_127: 1} = 2^65 - 1 ≈ 3.69e19.
	// x * 10^19 ≈ 3.69e38 > 2^127 ≈ 1.70e38 → must overflow.
	x := types.Decimal128{B0_63: ^uint64(0), B64_127: 1}
	require.False(t, d128MulPow10(&x, 19), "d128MulPow10 should detect carry overflow")

	// x = {B0_63: MaxUint64, B64_127: 1}, n=1: x * 10 = 10*(2^65-1) ≈ 3.69e20.
	// Fits in 128-bit unsigned (< 2^127), should succeed.
	x = types.Decimal128{B0_63: ^uint64(0), B64_127: 1}
	require.True(t, d128MulPow10(&x, 1), "d128MulPow10 should succeed for small factor")
	// Verify: 10 * (2^65-1) = 10*2^65 - 10 = 20*2^64 - 10.
	// B0_63 = lo64(MaxUint64 * 10), B64_127 = hi64(MaxUint64 * 10) + 10.
	hi, lo := bits.Mul64(^uint64(0), 10)
	hi += 10 // cross product: 1 * 10
	require.Equal(t, lo, x.B0_63)
	require.Equal(t, hi, x.B64_127)
}

func TestD128Mul(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
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
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(42))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		scalar := []types.Decimal128{randD128Small(rng)}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Mul(scalar, vec, rs, 2, 2, nul))
		for i := range vec {
			want, _, err := scalar[0].Mul(vec[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 const-vec mul[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(42))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		scalar := []types.Decimal128{randD128Small(rng)}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Mul(vec, scalar, rs, 2, 2, nul))
		for i := range vec {
			want, _, err := vec[i].Mul(scalar[0], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 vec-const mul[%d]", i)
		}
	})

	// Regression: values in [2^63, 2^64-1] must fall through to slow path,
	// not be mis-handled by the int64 fast path.
	t.Run("NearInt64Boundary", func(t *testing.T) {
		boundary := []types.Decimal128{
			{B0_63: 0x8000000000000001, B64_127: 0}, // 2^63+1, positive
			{B0_63: 0xFFFFFFFFFFFFFFFF, B64_127: 0}, // 2^64-1, positive
			{B0_63: 0x8000000000000000, B64_127: 0}, // 2^63, positive
			{B0_63: 0x7FFFFFFFFFFFFFFF, B64_127: 0}, // 2^63-1, max int64
		}
		one := []types.Decimal128{{B0_63: 1, B64_127: 0}}
		for _, x := range boundary {
			v1 := []types.Decimal128{x}
			rs := make([]types.Decimal128, 1)
			nul := nulls.NewWithSize(1)
			require.NoError(t, d128Mul(v1, one, rs, 0, 0, nul))
			want, _, err := x.Mul(one[0], 0, 0)
			require.NoError(t, err)
			require.Equal(t, want, rs[0], "boundary %v × 1", x)
		}
	})

	// Exercises the inlined MulInplace slow path with large-value batches.
	t.Run("LargeValues", func(t *testing.T) {
		rng := rand.New(rand.NewSource(99))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			// Large positive: B0_63 bit 63 set → not representable as int64.
			v1[i] = types.Decimal128{B0_63: rng.Uint64() | (1 << 63), B64_127: 0}
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(1_000_000) + 1), B64_127: 0}
			if rng.Intn(2) == 0 {
				v2[i] = v2[i].Minus()
			}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d128Mul(v1, v2, rs, 2, 3, nul)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Mul(v2[i], 2, 3)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128Mul large[%d] %v × %v", i, v1[i], v2[i])
		}
	})
}

func TestD128Div(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
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
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(43))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Div(scalar, vec, rs, 2, 2, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Div(vec[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 const-vec div[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(43))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Div(vec, scalar, rs, 2, 2, nul, true))
		for i := range vec {
			want, _, err := vec[i].Div(scalar[0], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 vec-const div[%d]", i)
		}
	})

	t.Run("DiffScale", func(t *testing.T) {
		rng := rand.New(rand.NewSource(17))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d128Div(v1, v2, rs, 2, 5, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Div(v2[i], 2, 5)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128Div DiffScale[%d]", i)
		}
	})

	t.Run("DiffScaleScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(56))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}

		// const-vec div with diff scale
		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Div(scalar, vec, rs, 2, 5, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Div(vec[i], 2, 5)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 diffscale const-vec div[%d]", i)
		}

		// vec-const div with diff scale
		rs2 := make([]types.Decimal128, testBatchSize)
		nul2 := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Div(vec, scalar, rs2, 5, 2, nul2, true))
		for i := range vec {
			want, _, err := vec[i].Div(scalar[0], 5, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs2[i], "d128 diffscale vec-const div[%d]", i)
		}
	})

	t.Run("LargeDivisor", func(t *testing.T) {
		rng := rand.New(rand.NewSource(21))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = types.Decimal128{B0_63: uint64(rng.Int63n(1e15)), B64_127: uint64(rng.Int63n(100) + 1)}
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(1e12) + 1), B64_127: uint64(rng.Int63n(5) + 1)}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d128Div(v1, v2, rs, 2, 2, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Div(v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128DivOne large[%d]", i)
		}
	})

	t.Run("Kernel", func(t *testing.T) {
		v1 := []types.Decimal128{{B0_63: 100}}
		v2 := []types.Decimal128{{B0_63: 3}}
		rs := make([]types.Decimal128, 1)
		nul := nulls.NewWithSize(1)

		kernel := d128DivKernel(true)
		err := kernel(v1, v2, rs, 2, 2, nul)
		require.NoError(t, err)

		want, _, _ := v1[0].Div(v2[0], 2, 2)
		require.Equal(t, want, rs[0])
	})
}

func TestD128Div_DiffScale(t *testing.T) {
	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(95))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal128{randD128Small(rng)}
		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Div(scalar, vec, rs, 3, 6, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Div(vec[i], 3, 6)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128Div diffscale const-vec[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(96))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}
		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Div(vec, scalar, rs, 6, 3, nul, true))
		for i := range vec {
			want, _, err := vec[i].Div(scalar[0], 6, 3)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128Div diffscale vec-const[%d]", i)
		}
	})

	t.Run("HighScaleAdj", func(t *testing.T) {
		// scale1=0, scale2=37 → scaleAdj=43, previously panicked with Pow10 index out of bounds.
		// The result overflows D128 but should return an error, not panic.
		v1 := []types.Decimal128{{B0_63: 12345678}}
		v2 := []types.Decimal128{{B0_63: 1000000}}
		rs := make([]types.Decimal128, 1)
		nul := nulls.NewWithSize(1)
		err := d128Div(v1, v2, rs, 0, 37, nul, true)
		require.Error(t, err, "expected overflow error for extreme scaleAdj")
	})
}

func TestD128Mod(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(14))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d128Mod(v1, v2, rs, 2, 2, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Mod(v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128Mod[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(44))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Mod(scalar, vec, rs, 2, 2, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Mod(vec[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 const-vec mod[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(44))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Mod(vec, scalar, rs, 2, 2, nul, true))
		for i := range vec {
			want, _, err := vec[i].Mod(scalar[0], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 vec-const mod[%d]", i)
		}
	})

	t.Run("Kernel", func(t *testing.T) {
		v1 := []types.Decimal128{{B0_63: 100}}
		v2 := []types.Decimal128{{B0_63: 3}}
		rs := make([]types.Decimal128, 1)
		nul := nulls.NewWithSize(1)

		kernel := d128ModKernel(true)
		err := kernel(v1, v2, rs, 2, 2, nul)
		require.NoError(t, err)

		want, _, _ := v1[0].Mod(v2[0], 2, 2)
		require.Equal(t, want, rs[0])
	})

	t.Run("DiffScale_VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(80))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d128Mod(v1, v2, rs, 2, 5, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Mod(v2[i], 2, 5)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128Mod diffscale[%d]", i)
		}
	})

	t.Run("DiffScale_ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(81))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}
		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Mod(scalar, vec, rs, 3, 6, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Mod(vec[i], 3, 6)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 diffscale const-vec[%d]", i)
		}
	})

	t.Run("DiffScale_VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(82))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}
		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Mod(vec, scalar, rs, 6, 3, nul, true))
		for i := range vec {
			want, _, err := vec[i].Mod(scalar[0], 6, 3)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 diffscale vec-const[%d]", i)
		}
	})
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

func BenchmarkD128AddDiffScale_Fast(b *testing.B) {
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
		_ = d128Add(xs, ys, rs, 2, 5, nul)
	}
}

func BenchmarkD128SubDiffScale_Fast(b *testing.B) {
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
		_ = d128Sub(xs, ys, rs, 2, 5, nul)
	}
}

func BenchmarkD128AddDiffScale_FastLarge(b *testing.B) {
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
		_ = d128Add(xs, ys, rs, 2, 5, nul)
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

func BenchmarkD128Mul_FastLarge(b *testing.B) {
	// Values that don't fit in int64 — exercises the inlined MulInplace slow path.
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal128, benchN)
	ys := make([]types.Decimal128, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		// Large positive value: B0_63 has bit 63 set, B64_127=0 → not int64-representable.
		xs[i] = types.Decimal128{B0_63: rng.Uint64() | (1 << 63), B64_127: 0}
		ys[i] = types.Decimal128{B0_63: uint64(rng.Int63n(1_000_000) + 1), B64_127: 0}
		if rng.Intn(2) == 0 {
			ys[i] = ys[i].Minus()
		}
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d128Mul(xs, ys, rs, 2, 3, nul)
	}
}

func BenchmarkD128Mul_GenericLarge(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal128, benchN)
	ys := make([]types.Decimal128, benchN)
	rs := make([]types.Decimal128, benchN)
	for i := range xs {
		xs[i] = types.Decimal128{B0_63: rng.Uint64() | (1 << 63), B64_127: 0}
		ys[i] = types.Decimal128{B0_63: uint64(rng.Int63n(1_000_000) + 1), B64_127: 0}
		if rng.Intn(2) == 0 {
			ys[i] = ys[i].Minus()
		}
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			rs[i], _, _ = xs[i].Mul(ys[i], 2, 3)
		}
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

func BenchmarkD128Mod_Fast(b *testing.B) {
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
		_ = d128Mod(xs, ys, rs, 2, 2, nul, true)
	}
}

func BenchmarkD128ModDiffScale_Fast(b *testing.B) {
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
		_ = d128Mod(xs, ys, rs, 2, 4, nul, true)
	}
}

func BenchmarkD128IntDiv_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal128, benchN)
	ys := make([]types.Decimal128, benchN)
	rs := make([]int64, benchN)
	for i := range xs {
		// Values representable as int64 after truncation.
		xs[i] = types.Decimal128{B0_63: uint64(rng.Int63()), B64_127: 0}
		ys[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1), B64_127: 0}
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d128IntDiv(xs, ys, rs, 2, 2, nul, true)
	}
}

func BenchmarkD128IntDiv_Generic(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal128, benchN)
	ys := make([]types.Decimal128, benchN)
	rs := make([]int64, benchN)
	for i := range xs {
		xs[i] = types.Decimal128{B0_63: uint64(rng.Int63()), B64_127: 0}
		ys[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1), B64_127: 0}
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			r, rScale, _ := xs[i].Div(ys[i], 2, 2)
			if rScale > 0 {
				r, _ = r.Scale(-rScale)
			}
			rs[i], _ = decimal128ToInt64(r)
		}
	}
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

func TestD256Add(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(7))
		v1 := make([]types.Decimal256, testBatchSize)
		v2 := make([]types.Decimal256, testBatchSize)
		rs := make([]types.Decimal256, testBatchSize)
		for i := range v1 {
			v1[i] = randD256(rng)
			v2[i] = randD256(rng)
		}
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Add(v1, v2, rs, 2, 2, nul))
		for i := range v1 {
			wantAdd, _, err := v1[i].Add(v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, wantAdd, rs[i], "d256Add[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(53))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = randD256Small(rng)
		}
		scalar := []types.Decimal256{randD256Small(rng)}

		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Add(scalar, vec, rs, 2, 2, nul))
		for i := range vec {
			want, err := scalar[0].Add256(vec[i])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 const-vec add[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(53))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = randD256Small(rng)
		}
		scalar := []types.Decimal256{randD256Small(rng)}

		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Add(vec, scalar, rs, 2, 2, nul))
		for i := range vec {
			want, err := vec[i].Add256(scalar[0])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 vec-const add[%d]", i)
		}
	})

	t.Run("Nulls", func(t *testing.T) {
		rng := rand.New(rand.NewSource(53))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = randD256Small(rng)
		}
		scalar := []types.Decimal256{randD256Small(rng)}

		rs := make([]types.Decimal256, testBatchSize)
		nul := makeNulls(testBatchSize)
		require.NoError(t, d256Add(vec, scalar, rs, 2, 2, nul))
		for i := range vec {
			if i%4 == 0 {
				continue
			}
			want, err := vec[i].Add256(scalar[0])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 vec-const add null[%d]", i)
		}
	})

	t.Run("DiffScale", func(t *testing.T) {
		rng := rand.New(rand.NewSource(12))
		v1 := make([]types.Decimal256, testBatchSize)
		v2 := make([]types.Decimal256, testBatchSize)
		rs := make([]types.Decimal256, testBatchSize)
		for i := range v1 {
			v1[i] = randD256Small(rng)
			v2[i] = randD256Small(rng)
		}
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Add(v1, v2, rs, 1, 4, nul))
		for i := range v1 {
			wantAdd, _, err := v1[i].Add(v2[i], 1, 4)
			require.NoError(t, err)
			require.Equal(t, wantAdd, rs[i], "d256Add DiffScale[%d]", i)
		}
	})

	t.Run("DiffScaleScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(57))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = randD256Small(rng)
		}
		scalar := []types.Decimal256{randD256Small(rng)}

		// scalar(scale1) + vec(scale3)
		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Add(scalar, vec, rs, 1, 3, nul))
		for i := range vec {
			want, _, err := scalar[0].Add(vec[i], 1, 3)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 diffscale const-vec add[%d]", i)
		}

		// vec(scale3) + scalar(scale1)
		rs2 := make([]types.Decimal256, testBatchSize)
		nul2 := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Add(vec, scalar, rs2, 3, 1, nul2))
		for i := range vec {
			want, _, err := vec[i].Add(scalar[0], 3, 1)
			require.NoError(t, err)
			require.Equal(t, want, rs2[i], "d256 diffscale vec-const add[%d]", i)
		}
	})
}

func d256SubRef(x, y types.Decimal256, scale1, scale2 int32) (types.Decimal256, int32, error) {
	var err error
	var scale int32
	if scale1 > scale2 {
		scale = scale1
		y, err = y.Scale(scale - scale2)
	} else if scale1 < scale2 {
		scale = scale2
		x, err = x.Scale(scale - scale1)
	} else {
		scale = scale1
	}
	if err != nil {
		return types.Decimal256{}, scale, err
	}
	z, err := x.Sub256(y)
	return z, scale, err
}

// d256MulRef is a test-only reference implementation for Decimal256 multiplication
// with scale (mirrors the deleted Decimal256.Mul method).
func TestD256Sub(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(7))
		v1 := make([]types.Decimal256, testBatchSize)
		v2 := make([]types.Decimal256, testBatchSize)
		rs := make([]types.Decimal256, testBatchSize)
		for i := range v1 {
			v1[i] = randD256(rng)
			v2[i] = randD256(rng)
		}
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Sub(v1, v2, rs, 2, 2, nul))
		for i := range v1 {
			wantSub, _, err := d256SubRef(v1[i], v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, wantSub, rs[i], "d256Sub[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(54))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = randD256Small(rng)
		}
		scalar := []types.Decimal256{randD256Small(rng)}

		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Sub(scalar, vec, rs, 2, 2, nul))
		for i := range vec {
			want, err := scalar[0].Sub256(vec[i])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 const-vec sub[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(54))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = randD256Small(rng)
		}
		scalar := []types.Decimal256{randD256Small(rng)}

		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Sub(vec, scalar, rs, 2, 2, nul))
		for i := range vec {
			want, err := vec[i].Sub256(scalar[0])
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 vec-const sub[%d]", i)
		}
	})

	t.Run("DiffScale", func(t *testing.T) {
		rng := rand.New(rand.NewSource(12))
		v1 := make([]types.Decimal256, testBatchSize)
		v2 := make([]types.Decimal256, testBatchSize)
		rs := make([]types.Decimal256, testBatchSize)
		for i := range v1 {
			v1[i] = randD256Small(rng)
			v2[i] = randD256Small(rng)
		}
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Sub(v1, v2, rs, 1, 4, nul))
		for i := range v1 {
			wantSub, _, err := d256SubRef(v1[i], v2[i], 1, 4)
			require.NoError(t, err)
			require.Equal(t, wantSub, rs[i], "d256Sub DiffScale[%d]", i)
		}

		// scalar-vec diff scale sub
		rng2 := rand.New(rand.NewSource(57))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = randD256Small(rng2)
		}
		scalar := []types.Decimal256{randD256Small(rng2)}
		rs2 := make([]types.Decimal256, testBatchSize)
		nul2 := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Sub(vec, scalar, rs2, 1, 3, nul2))
		for i := range vec {
			want, _, err := d256SubRef(vec[i], scalar[0], 1, 3)
			require.NoError(t, err)
			require.Equal(t, want, rs2[i], "d256 diffscale vec-const sub[%d]", i)
		}
	})
}

func d256MulRef(x, y types.Decimal256, scale1, scale2 int32) (types.Decimal256, int32, error) {
	scale := int32(12)
	if scale1 > scale {
		scale = scale1
	}
	if scale2 > scale {
		scale = scale2
	}
	if scale1+scale2 < scale {
		scale = scale1 + scale2
	}
	signx := x.Sign()
	x1 := x
	signy := y.Sign()
	y1 := y
	if signx {
		x1 = x1.Minus()
	}
	if signy {
		y1 = y1.Minus()
	}
	z, err := x1.Mul256(y1)
	if err != nil {
		return z, scale, err
	}
	if scale-scale1-scale2 != 0 {
		z, err = z.Scale(scale - scale1 - scale2)
		if err != nil {
			return z, scale, err
		}
	}
	if signx != signy {
		z = z.Minus()
	}
	return z, scale, nil
}

// ---- D64 ----

func TestD256Mul(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
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
			want, _, err := d256MulRef(v1[i], v2[i], 2, 3)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256Mul[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(50))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = randD256Small(rng)
		}
		scalar := []types.Decimal256{randD256Small(rng)}

		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Mul(scalar, vec, rs, 2, 2, nul))
		for i := range vec {
			want, _, err := d256MulRef(scalar[0], vec[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 const-vec mul[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(50))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = randD256Small(rng)
		}
		scalar := []types.Decimal256{randD256Small(rng)}

		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Mul(vec, scalar, rs, 2, 2, nul))
		for i := range vec {
			want, _, err := d256MulRef(vec[i], scalar[0], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 vec-const mul[%d]", i)
		}
	})
}

func TestD256Mul_HighScale(t *testing.T) {
	rng := rand.New(rand.NewSource(94))
	v1 := make([]types.Decimal256, testBatchSize)
	v2 := make([]types.Decimal256, testBatchSize)
	rs := make([]types.Decimal256, testBatchSize)
	for i := range v1 {
		v1[i] = randD256Small(rng)
		v2[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
	}
	nul := nulls.NewWithSize(testBatchSize)
	// scale1=10, scale2=10 → desiredScale=12, scaleAdj=-8, triggers d256ScaleDown
	err := d256Mul(v1, v2, rs, 10, 10, nul)
	require.NoError(t, err)
	for i := range v1 {
		// Reference: raw Mul256 then scale down by 8 (= 10+10 - 12).
		raw, err2 := v1[i].Mul256(v2[i])
		require.NoError(t, err2)
		d256ScaleDown(&raw, 8)
		require.Equal(t, raw, rs[i], "d256Mul HighScale[%d]", i)
	}
}

// TestD256Mod_LargeValues tests D256 Mod with values outside D128 range (generic slow path).
func TestD256Div(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
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
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(51))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}

		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Div(scalar, vec, rs, 2, 2, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Div(vec[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 const-vec div[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(51))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}

		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Div(vec, scalar, rs, 2, 2, nul, true))
		for i := range vec {
			want, _, err := vec[i].Div(scalar[0], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 vec-const div[%d]", i)
		}
	})

	t.Run("DiffScale", func(t *testing.T) {
		rng := rand.New(rand.NewSource(58))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}

		// const-vec div
		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Div(scalar, vec, rs, 2, 5, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Div(vec[i], 2, 5)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 diffscale const-vec div[%d]", i)
		}

		// vec-const div
		rs2 := make([]types.Decimal256, testBatchSize)
		nul2 := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Div(vec, scalar, rs2, 5, 2, nul2, true))
		for i := range vec {
			want, _, err := vec[i].Div(scalar[0], 5, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs2[i], "d256 diffscale vec-const div[%d]", i)
		}
	})

	t.Run("DiffScaleScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(58))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}

		// const-vec div
		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Div(scalar, vec, rs, 2, 5, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Div(vec[i], 2, 5)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 diffscale const-vec div[%d]", i)
		}

		// vec-const div
		rs2 := make([]types.Decimal256, testBatchSize)
		nul2 := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Div(vec, scalar, rs2, 5, 2, nul2, true))
		for i := range vec {
			want, _, err := vec[i].Div(scalar[0], 5, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs2[i], "d256 diffscale vec-const div[%d]", i)
		}
	})

	t.Run("Kernel", func(t *testing.T) {
		rng := rand.New(rand.NewSource(22))
		v1 := make([]types.Decimal256, testBatchSize)
		v2 := make([]types.Decimal256, testBatchSize)
		rs := make([]types.Decimal256, testBatchSize)
		for i := range v1 {
			v1[i] = randD256(rng)
			v2[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(testBatchSize)
		kernel := d256DivKernel(true)
		err := kernel(v1, v2, rs, 2, 2, nul)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Div(v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256DivKernel[%d]", i)
		}
	})
}

func TestD256Div_LargeValues(t *testing.T) {
	v1 := make([]types.Decimal256, 4)
	v2 := make([]types.Decimal256, 4)
	rs := make([]types.Decimal256, 4)
	v1[0] = types.Decimal256{B0_63: 0xabcdef1234567890, B64_127: 0x1234, B128_191: 1}
	v1[1] = types.Decimal256{B0_63: 0x9876543210fedcba, B64_127: 0x5678, B128_191: 2}
	v1[2] = types.Decimal256{B0_63: 0x1111111111111111, B64_127: 0x2222, B128_191: 3}
	v1[3] = types.Decimal256{B0_63: 0xffffffffffffffff, B64_127: 0x3333, B128_191: 0}
	v2[0] = types.Decimal256{B0_63: 17}
	v2[1] = types.Decimal256{B0_63: 31}
	v2[2] = types.Decimal256{B0_63: 97}
	v2[3] = types.Decimal256{B0_63: 1000003}

	nul := nulls.NewWithSize(4)
	err := d256Div(v1, v2, rs, 2, 2, nul, true)
	require.NoError(t, err)
	for i := range v1 {
		want, _, err := v1[i].Div(v2[i], 2, 2)
		require.NoError(t, err)
		require.Equal(t, want, rs[i], "d256Div large[%d]", i)
	}
}

// TestNullHandling tests that null entries are properly skipped in batch operations.
func TestD256Mod(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(15))
		v1 := make([]types.Decimal256, testBatchSize)
		v2 := make([]types.Decimal256, testBatchSize)
		rs := make([]types.Decimal256, testBatchSize)
		for i := range v1 {
			v1[i] = randD256Small(rng)
			v2[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d256Mod(v1, v2, rs, 2, 2, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Mod(v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256Mod[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(52))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}

		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Mod(scalar, vec, rs, 2, 2, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Mod(vec[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 const-vec mod[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(52))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}

		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Mod(vec, scalar, rs, 2, 2, nul, true))
		for i := range vec {
			want, _, err := vec[i].Mod(scalar[0], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 vec-const mod[%d]", i)
		}
	})

	t.Run("Kernel", func(t *testing.T) {
		v1 := []types.Decimal256{{B0_63: 100}}
		v2 := []types.Decimal256{{B0_63: 3}}
		rs := make([]types.Decimal256, 1)
		nul := nulls.NewWithSize(1)

		kernel := d256ModKernel(true)
		err := kernel(v1, v2, rs, 2, 2, nul)
		require.NoError(t, err)

		want, _, _ := v1[0].Mod(v2[0], 2, 2)
		require.Equal(t, want, rs[0])
	})

	t.Run("DiffScale_VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(83))
		v1 := make([]types.Decimal256, testBatchSize)
		v2 := make([]types.Decimal256, testBatchSize)
		rs := make([]types.Decimal256, testBatchSize)
		for i := range v1 {
			v1[i] = randD256Small(rng)
			v2[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d256Mod(v1, v2, rs, 2, 5, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Mod(v2[i], 2, 5)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256Mod diffscale[%d]", i)
		}
	})

	t.Run("DiffScale_ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(84))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}
		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Mod(scalar, vec, rs, 3, 6, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Mod(vec[i], 3, 6)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 diffscale const-vec[%d]", i)
		}
	})

	t.Run("DiffScale_VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(85))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}
		rs := make([]types.Decimal256, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256Mod(vec, scalar, rs, 6, 3, nul, true))
		for i := range vec {
			want, _, err := vec[i].Mod(scalar[0], 6, 3)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 diffscale vec-const[%d]", i)
		}
	})
}

func TestD256Mod_LargeValues(t *testing.T) {
	// Values with B128_191 != 0 so d256AllFitD128 returns false.
	v1 := make([]types.Decimal256, 4)
	v2 := make([]types.Decimal256, 4)
	rs := make([]types.Decimal256, 4)
	v1[0] = types.Decimal256{B0_63: 0xabcdef1234567890, B64_127: 0x1234, B128_191: 1}
	v1[1] = types.Decimal256{B0_63: 0x9876543210fedcba, B64_127: 0x5678, B128_191: 2}
	v1[2] = types.Decimal256{B0_63: 0x1111111111111111, B64_127: 0x2222, B128_191: 3}
	v1[3] = types.Decimal256{B0_63: 0xffffffffffffffff, B64_127: 0x3333, B128_191: 0}
	v2[0] = types.Decimal256{B0_63: 17}
	v2[1] = types.Decimal256{B0_63: 31}
	v2[2] = types.Decimal256{B0_63: 97}
	v2[3] = types.Decimal256{B0_63: 1000003}

	nul := nulls.NewWithSize(4)
	err := d256Mod(v1, v2, rs, 2, 2, nul, true)
	require.NoError(t, err)
	for i := range v1 {
		want, _, err := v1[i].Mod(v2[i], 2, 2)
		require.NoError(t, err)
		require.Equal(t, want, rs[i], "d256Mod large[%d]", i)
	}
}

// TestD256Div_LargeValues tests D256 Div with values outside D128 range (generic slow path).
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

func BenchmarkD256AddDiffScale_Fast(b *testing.B) {
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
		_ = d256Add(xs, ys, rs, 2, 5, nul)
	}
}

func BenchmarkD256AddDiffScale_Generic(b *testing.B) {
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
			rs[i], _, _ = xs[i].Add(ys[i], 2, 5)
		}
	}
}

func BenchmarkD256SubDiffScale_Fast(b *testing.B) {
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
		_ = d256Sub(xs, ys, rs, 2, 5, nul)
	}
}

func BenchmarkD256SubDiffScale_Generic(b *testing.B) {
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
			neg := ys[i].Minus()
			rs[i], _, _ = xs[i].Add(neg, 2, 5)
		}
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

func BenchmarkD256Mul_FastMixed(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal256, benchN)
	ys := make([]types.Decimal256, benchN)
	rs := make([]types.Decimal256, benchN)
	for i := range xs {
		xs[i] = randD256Small(rng)
		ys[i] = randD256Small(rng)
		if rng.Intn(2) == 0 {
			xs[i] = xs[i].Minus()
		}
		if rng.Intn(2) == 0 {
			ys[i] = ys[i].Minus()
		}
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d256Mul(xs, ys, rs, 2, 3, nul)
	}
}

func BenchmarkD256MulScaled_Fast(b *testing.B) {
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
		_ = d256Mul(xs, ys, rs, 10, 10, nul)
	}
}

func BenchmarkD256Mul_FastLarge(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal256, benchN)
	ys := make([]types.Decimal256, benchN)
	rs := make([]types.Decimal256, benchN)
	for i := range xs {
		// Values that don't fit in int64 — use 2 limbs.
		xs[i] = types.Decimal256{B0_63: uint64(rng.Int63n(1_000_000_000)), B64_127: uint64(rng.Int63n(100))}
		ys[i] = types.Decimal256{B0_63: uint64(rng.Int63n(1_000_000_000))}
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
			rs[i], _, _ = d256MulRef(xs[i], ys[i], 2, 3)
		}
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

// ---- IntDiv (DIV operator) benchmarks ----

func BenchmarkD256Mod_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal256, benchN)
	ys := make([]types.Decimal256, benchN)
	rs := make([]types.Decimal256, benchN)
	for i := range xs {
		xs[i] = randD256Small(rng)
		ys[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d256Mod(xs, ys, rs, 2, 2, nul, true)
	}
}

func BenchmarkD256ModDiffScale_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal256, benchN)
	ys := make([]types.Decimal256, benchN)
	rs := make([]types.Decimal256, benchN)
	for i := range xs {
		xs[i] = randD256Small(rng)
		ys[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d256Mod(xs, ys, rs, 2, 5, nul, true)
	}
}

func BenchmarkD256IntDiv_Fast(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal256, benchN)
	ys := make([]types.Decimal256, benchN)
	rs := make([]int64, benchN)
	for i := range xs {
		// Values that fit in D128 and produce int64 results.
		xs[i] = types.Decimal256{B0_63: uint64(rng.Int63())}
		ys[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
	}
	nul := nulls.NewWithSize(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		_ = d256IntDiv(xs, ys, rs, 2, 2, nul, true)
	}
}

func BenchmarkD256IntDiv_Generic(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	xs := make([]types.Decimal256, benchN)
	ys := make([]types.Decimal256, benchN)
	rs := make([]int64, benchN)
	for i := range xs {
		xs[i] = types.Decimal256{B0_63: uint64(rng.Int63())}
		ys[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < benchN; i++ {
			r, rScale, _ := xs[i].Div(ys[i], 2, 2)
			if rScale > 0 {
				r, _ = r.Scale(-rScale)
			}
			rs[i], _ = decimal256ToInt64(r)
		}
	}
}
