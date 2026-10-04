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
	"math/bits"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

// TestNullHandling tests that null entries are properly skipped in batch operations.
func TestNullHandling(t *testing.T) {

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

func randD64(rng *rand.Rand) types.Decimal64 {
	return types.Decimal64(rng.Int63n(2_000_000_000) - 1_000_000_000)
}

func TestD64Add(t *testing.T) {
	// Coefficients are independently derived; no decimal producer computes the oracle.
	for _, tc := range []struct {
		name                  string
		left, right           []types.Decimal64
		leftScale, rightScale int32
		nullRows              []uint64
		want                  []types.Decimal64
		wantError             uint16
	}{
		{
			"same_vv/no_null",
			[]types.Decimal64{2305843009213693951, 16140901064495857665, 9223372036854775807, 9223372036854775808},
			[]types.Decimal64{1, 18446744073709551615, 0, 0},
			2, 2, nil,
			[]types.Decimal64{2305843009213693952, 16140901064495857664, 9223372036854775807, 9223372036854775808}, 0,
		},
		{
			"same_vv/null_middle",
			[]types.Decimal64{16140901064495857665, 9223372036854775807, 0},
			[]types.Decimal64{18446744073709551615, 1, 0},
			2, 2, []uint64{1},
			[]types.Decimal64{16140901064495857664, 0, 0}, 0,
		},
		{
			"same_sv/no_null",
			[]types.Decimal64{2305843009213693951},
			[]types.Decimal64{1, 18446744073709551615, 16140901064495857665},
			2, 2, nil,
			[]types.Decimal64{2305843009213693952, 2305843009213693950, 0}, 0,
		},
		{
			"same_sv/null_middle",
			[]types.Decimal64{2305843009213693951},
			[]types.Decimal64{18446744073709551615, 9223372036854775807, 16140901064495857665},
			2, 2, []uint64{1},
			[]types.Decimal64{2305843009213693950, 0, 0}, 0,
		},
		{
			"same_vs/no_null",
			[]types.Decimal64{1, 18446744073709551615, 16140901064495857665},
			[]types.Decimal64{2305843009213693951},
			2, 2, nil,
			[]types.Decimal64{2305843009213693952, 2305843009213693950, 0}, 0,
		},
		{
			"same_vs/null_middle",
			[]types.Decimal64{18446744073709551615, 9223372036854775807, 16140901064495857665},
			[]types.Decimal64{2305843009213693951},
			2, 2, []uint64{1},
			[]types.Decimal64{2305843009213693950, 0, 0}, 0,
		},
		{
			"diff_vv_left_lower/no_null",
			[]types.Decimal64{17, 18446744073709551593, 0},
			[]types.Decimal64{3, 18446744073709551609, 0},
			1, 3, nil,
			[]types.Decimal64{1703, 18446744073709549309, 0}, 0,
		},
		{
			"diff_vv_left_lower/null_middle",
			[]types.Decimal64{18446744073709551593, 17, 0},
			[]types.Decimal64{18446744073709551609, 9223372036854775807, 0},
			1, 3, []uint64{1},
			[]types.Decimal64{18446744073709549309, 0, 0}, 0,
		},
		{
			"diff_sv_left_lower/no_null",
			[]types.Decimal64{17},
			[]types.Decimal64{3, 18446744073709551609, 18446744073709549916},
			1, 3, nil,
			[]types.Decimal64{1703, 1693, 0}, 0,
		},
		{
			"diff_sv_left_lower/null_middle",
			[]types.Decimal64{17},
			[]types.Decimal64{18446744073709551609, 9223372036854775807, 18446744073709549916},
			1, 3, []uint64{1},
			[]types.Decimal64{1693, 0, 0}, 0,
		},
		{
			"diff_vs_right_lower/no_null",
			[]types.Decimal64{3, 18446744073709551609, 18446744073709549916},
			[]types.Decimal64{17},
			3, 1, nil,
			[]types.Decimal64{1703, 1693, 0}, 0,
		},
		{
			"diff_vs_right_lower/null_middle",
			[]types.Decimal64{18446744073709551609, 9223372036854775807, 18446744073709549916},
			[]types.Decimal64{17},
			3, 1, []uint64{1},
			[]types.Decimal64{1693, 0, 0}, 0,
		},
		// checked scaled NULL poison, with later live row; D128 wide signed scaling
		{
			"diff_sv_right_lower/null_middle",
			[]types.Decimal64{5},
			[]types.Decimal64{2, 9223372036854775807, 18446744073709551614},
			2, 0, []uint64{1},
			[]types.Decimal64{205, 0, 18446744073709551421}, 0,
		},
		// opposite vector scaling direction
		{
			"diff_vs_left_lower/no_null",
			[]types.Decimal64{2, 18446744073709551614},
			[]types.Decimal64{5},
			0, 2, nil,
			[]types.Decimal64{205, 18446744073709551421}, 0,
		},
		// opposite vector scaling direction
		{
			"diff_vv_right_lower/no_null",
			[]types.Decimal64{5, 18446744073709551609},
			[]types.Decimal64{2, 18446744073709551614},
			2, 0, nil,
			[]types.Decimal64{205, 18446744073709551409}, 0,
		},
		{
			"overflow_positive",
			[]types.Decimal64{9223372036854775807},
			[]types.Decimal64{1},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"overflow_negative",
			[]types.Decimal64{9223372036854775808},
			[]types.Decimal64{18446744073709551615},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"scale_error_scalar",
			[]types.Decimal64{9223372036854775807},
			[]types.Decimal64{1, 2},
			0, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			length := max(len(tc.left), len(tc.right))
			n := nulls.NewWithSize(length)
			for _, row := range tc.nullRows {
				n.Add(row)
			}
			result := make([]types.Decimal64, length)
			err := d64Add(tc.left, tc.right, result, tc.leftScale, tc.rightScale, n)
			if tc.wantError != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.wantError), "error: %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.nullRows), n.Count())
			for i := range result {
				masked := false
				for _, row := range tc.nullRows {
					masked = masked || row == uint64(i)
				}
				require.Equal(t, masked, n.Contains(uint64(i)), "NULL row %d", i)
				if !masked && tc.wantError == 0 {
					require.Equal(t, tc.want[i], result[i], "row %d", i)
				}
			}
		})
	}
}

func TestD64Sub(t *testing.T) {
	// Coefficients are independently derived; no decimal producer computes the oracle.
	for _, tc := range []struct {
		name                  string
		left, right           []types.Decimal64
		leftScale, rightScale int32
		nullRows              []uint64
		want                  []types.Decimal64
		wantError             uint16
	}{
		{
			"same_vv/no_null",
			[]types.Decimal64{2305843009213693951, 16140901064495857665, 9223372036854775807, 9223372036854775808},
			[]types.Decimal64{1, 18446744073709551615, 0, 0},
			2, 2, nil,
			[]types.Decimal64{2305843009213693950, 16140901064495857666, 9223372036854775807, 9223372036854775808}, 0,
		},
		{
			"same_vv/null_middle",
			[]types.Decimal64{16140901064495857665, 9223372036854775808, 0},
			[]types.Decimal64{18446744073709551615, 1, 0},
			2, 2, []uint64{1},
			[]types.Decimal64{16140901064495857666, 0, 0}, 0,
		},
		{
			"same_sv/no_null",
			[]types.Decimal64{2305843009213693951},
			[]types.Decimal64{1, 18446744073709551615, 16140901064495857665},
			2, 2, nil,
			[]types.Decimal64{2305843009213693950, 2305843009213693952, 4611686018427387902}, 0,
		},
		{
			"same_sv/null_middle",
			[]types.Decimal64{2305843009213693951},
			[]types.Decimal64{18446744073709551615, 9223372036854775808, 16140901064495857665},
			2, 2, []uint64{1},
			[]types.Decimal64{2305843009213693952, 0, 4611686018427387902}, 0,
		},
		{
			"same_vs/no_null",
			[]types.Decimal64{1, 18446744073709551615, 16140901064495857665},
			[]types.Decimal64{2305843009213693951},
			2, 2, nil,
			[]types.Decimal64{16140901064495857666, 16140901064495857664, 13835058055282163714}, 0,
		},
		{
			"same_vs/null_middle",
			[]types.Decimal64{18446744073709551615, 9223372036854775808, 16140901064495857665},
			[]types.Decimal64{2305843009213693951},
			2, 2, []uint64{1},
			[]types.Decimal64{16140901064495857664, 0, 13835058055282163714}, 0,
		},
		{
			"diff_vv_left_lower/no_null",
			[]types.Decimal64{17, 18446744073709551593, 0},
			[]types.Decimal64{3, 18446744073709551609, 0},
			1, 3, nil,
			[]types.Decimal64{1697, 18446744073709549323, 0}, 0,
		},
		{
			"diff_vv_left_lower/null_middle",
			[]types.Decimal64{18446744073709551593, 17, 0},
			[]types.Decimal64{18446744073709551609, 9223372036854775808, 0},
			1, 3, []uint64{1},
			[]types.Decimal64{18446744073709549323, 0, 0}, 0,
		},
		{
			"diff_sv_left_lower/no_null",
			[]types.Decimal64{17},
			[]types.Decimal64{3, 18446744073709551609, 18446744073709549916},
			1, 3, nil,
			[]types.Decimal64{1697, 1707, 3400}, 0,
		},
		{
			"diff_sv_left_lower/null_middle",
			[]types.Decimal64{17},
			[]types.Decimal64{18446744073709551609, 9223372036854775808, 18446744073709549916},
			1, 3, []uint64{1},
			[]types.Decimal64{1707, 0, 3400}, 0,
		},
		{
			"diff_vs_right_lower/no_null",
			[]types.Decimal64{3, 18446744073709551609, 18446744073709549916},
			[]types.Decimal64{17},
			3, 1, nil,
			[]types.Decimal64{18446744073709549919, 18446744073709549909, 18446744073709548216}, 0,
		},
		{
			"diff_vs_right_lower/null_middle",
			[]types.Decimal64{18446744073709551609, 9223372036854775808, 18446744073709549916},
			[]types.Decimal64{17},
			3, 1, []uint64{1},
			[]types.Decimal64{18446744073709549909, 0, 18446744073709548216}, 0,
		},
		// checked scaled NULL poison, with later live row; D128 wide signed scaling
		{
			"diff_sv_right_lower/null_middle",
			[]types.Decimal64{5},
			[]types.Decimal64{2, 9223372036854775807, 18446744073709551614},
			2, 0, []uint64{1},
			[]types.Decimal64{18446744073709551421, 0, 205}, 0,
		},
		// opposite vector scaling direction
		{
			"diff_vs_left_lower/no_null",
			[]types.Decimal64{2, 18446744073709551614},
			[]types.Decimal64{5},
			0, 2, nil,
			[]types.Decimal64{195, 18446744073709551411}, 0,
		},
		// opposite vector scaling direction
		{
			"diff_vv_right_lower/no_null",
			[]types.Decimal64{5, 18446744073709551609},
			[]types.Decimal64{2, 18446744073709551614},
			2, 0, nil,
			[]types.Decimal64{18446744073709551421, 193}, 0,
		},
		{
			"overflow_positive",
			[]types.Decimal64{9223372036854775807},
			[]types.Decimal64{18446744073709551615},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"overflow_negative",
			[]types.Decimal64{9223372036854775808},
			[]types.Decimal64{1},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"scale_error_vector",
			[]types.Decimal64{1, 9223372036854775807},
			[]types.Decimal64{1},
			0, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			length := max(len(tc.left), len(tc.right))
			n := nulls.NewWithSize(length)
			for _, row := range tc.nullRows {
				n.Add(row)
			}
			result := make([]types.Decimal64, length)
			err := d64Sub(tc.left, tc.right, result, tc.leftScale, tc.rightScale, n)
			if tc.wantError != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.wantError), "error: %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.nullRows), n.Count())
			for i := range result {
				masked := false
				for _, row := range tc.nullRows {
					masked = masked || row == uint64(i)
				}
				require.Equal(t, masked, n.Contains(uint64(i)), "NULL row %d", i)
				if !masked && tc.wantError == 0 {
					require.Equal(t, tc.want[i], result[i], "row %d", i)
				}
			}
		})
	}
}

func TestD64Mul(t *testing.T) {
	// Literal coefficients use exact signed multiplication and half-away-from-zero rounding.
	const allBits = ^uint64(0)
	const maxCoefficient = types.Decimal64(1<<63 - 1)
	const minCoefficient = types.Decimal64(1 << 63)
	for _, tc := range []struct {
		name                  string
		left, right           []types.Decimal64
		leftScale, rightScale int32
		nullRows              []uint64
		want                  []types.Decimal128
	}{
		{
			"unscaled_vv_values",
			[]types.Decimal64{maxCoefficient, minCoefficient, minCoefficient, 0}, []types.Decimal64{maxCoefficient, minCoefficient, 0xffffffffffffffff, minCoefficient},
			2, 3, nil,
			[]types.Decimal128{{B0_63: 1, B64_127: 0x3fffffffffffffff}, {B64_127: 0x4000000000000000}, {B0_63: 0x8000000000000000}, {}},
		},
		{
			"unscaled_sv_values",
			[]types.Decimal64{minCoefficient}, []types.Decimal64{0xffffffffffffffff, 1, 0},
			2, 2, nil,
			[]types.Decimal128{{B0_63: 0x8000000000000000}, {B0_63: 0x8000000000000000, B64_127: allBits}, {}},
		},
		{
			"unscaled_vs_values",
			[]types.Decimal64{0xffffffffffffffff, 1, 0}, []types.Decimal64{minCoefficient},
			2, 2, nil,
			[]types.Decimal128{{B0_63: 0x8000000000000000}, {B0_63: 0x8000000000000000, B64_127: allBits}, {}},
		},
		{
			"unscaled_vv_null_prefix",
			[]types.Decimal64{maxCoefficient, minCoefficient, 0xfffffffffffffff9}, []types.Decimal64{maxCoefficient, 0xffffffffffffffff, 3},
			2, 3, []uint64{0},
			[]types.Decimal128{{}, {B0_63: 0x8000000000000000}, {B0_63: 0xffffffffffffffeb, B64_127: allBits}},
		},
		{
			"unscaled_sv_null_prefix",
			[]types.Decimal64{0xfffffffffffffff9}, []types.Decimal64{13, 3, 0xfffffffffffffffd},
			2, 2, []uint64{0},
			[]types.Decimal128{{}, {B0_63: 0xffffffffffffffeb, B64_127: allBits}, {B0_63: 21}},
		},
		{
			"unscaled_vs_null_prefix",
			[]types.Decimal64{13, 3, 0xfffffffffffffffd}, []types.Decimal64{0xfffffffffffffff9},
			2, 2, []uint64{0},
			[]types.Decimal128{{}, {B0_63: 0xffffffffffffffeb, B64_127: allBits}, {B0_63: 21}},
		},
		{
			"scaled_int32_vv_values",
			[]types.Decimal64{4999, 5000, 5001, 0xffffffffffffec78, 2147483647, 0xffffffff80000000}, []types.Decimal64{1, 1, 1, 1, 2147483647, 2147483647},
			8, 8, nil,
			[]types.Decimal128{{}, {B0_63: 1}, {B0_63: 1}, {B0_63: 0xffffffffffffffff, B64_127: allBits}, {B0_63: 0x1a36e2eab367a}, {B0_63: 0xfffe5c91d15182aa, B64_127: allBits}},
		},
		{
			"scaled_int32_sv_values",
			[]types.Decimal64{1000000001}, []types.Decimal64{49999999, 50000000, 50000001, 0xfffffffffd050f80, 2000000000, 0xffffffff88ca6c00},
			10, 10, nil,
			[]types.Decimal128{{B0_63: 0x1dcd64f6}, {B0_63: 0x1dcd6501}, {B0_63: 0x1dcd650b}, {B0_63: 0xffffffffe2329aff, B64_127: allBits}, {B0_63: 0x4a817c814}, {B0_63: 0xfffffffb57e837ec, B64_127: allBits}},
		},
		{
			"scaled_int32_vs_values",
			[]types.Decimal64{2147483647, 0xffffffff80000000, 0}, []types.Decimal64{3000},
			7, 8, nil,
			[]types.Decimal128{{B0_63: 0x17ffffffd}, {B0_63: 0xfffffffe80000000, B64_127: allBits}, {}},
		},
		{
			"scaled_int32_vv_null_prefix",
			[]types.Decimal64{5000, 0xffffffffffffec78, 5001}, []types.Decimal64{1, 1, 1},
			8, 8, []uint64{0},
			[]types.Decimal128{{}, {B0_63: 0xffffffffffffffff, B64_127: allBits}, {B0_63: 1}},
		},
		{
			"scaled_int32_sv_null_prefix",
			[]types.Decimal64{5000}, []types.Decimal64{1, 0xffffffffffffffff, 3},
			8, 8, []uint64{0},
			[]types.Decimal128{{}, {B0_63: 0xffffffffffffffff, B64_127: allBits}, {B0_63: 2}},
		},
		{
			"scaled_int32_vs_null_prefix",
			[]types.Decimal64{1, 0xffffffffffffffff, 3}, []types.Decimal64{5000},
			8, 8, []uint64{0},
			[]types.Decimal128{{}, {B0_63: 0xffffffffffffffff, B64_127: allBits}, {B0_63: 2}},
		},
		{
			"scaled_wide_vv_values",
			[]types.Decimal64{1, 1099511627776, 0xffffff0000000000}, []types.Decimal64{1, 1099511627776, 1099511627776},
			10, 10, nil,
			[]types.Decimal128{{}, {B0_63: 0x2af31dc4611874}, {B0_63: 0xffd50ce23b9ee78c, B64_127: allBits}},
		},
		{
			"scaled_wide_sv_values",
			[]types.Decimal64{2147483647}, []types.Decimal64{1, minCoefficient, maxCoefficient},
			7, 8, nil,
			[]types.Decimal128{{B0_63: 0x20c49c}, {B0_63: 0x2d2f1a9fbe76c8b4, B64_127: 0xffffffffffef9db2}, {B0_63: 0xd2d0e560416872b0, B64_127: 0x10624d}},
		},
		{
			"scaled_wide_vs_values",
			[]types.Decimal64{minCoefficient, 1, 0xffffffffffffffff}, []types.Decimal64{minCoefficient},
			8, 8, nil,
			[]types.Decimal128{{B0_63: 0xca57a786c226809d, B64_127: 0x1a36e2eb1c432}, {B0_63: 0xfffcb923a29c779a, B64_127: allBits}, {B0_63: 0x346dc5d638866}},
		},
		{
			"scaled_wide_vv_null_prefix",
			[]types.Decimal64{maxCoefficient, minCoefficient, 1}, []types.Decimal64{maxCoefficient, minCoefficient, 1},
			10, 10, []uint64{0},
			[]types.Decimal128{{}, {B0_63: 0x461cefcfdc20d2b3, B64_127: 0xabcc77118}, {}},
		},
		{
			"scaled_wide_sv_null_prefix",
			[]types.Decimal64{maxCoefficient}, []types.Decimal64{maxCoefficient, minCoefficient, 0xffffffffffffffff},
			7, 8, []uint64{0},
			[]types.Decimal128{{}, {B0_63: 0x18b4395810624dd3, B64_127: 0xffef9db22d0e5604}, {B0_63: 0xffdf3b645a1cac08, B64_127: allBits}},
		},
		{
			"scaled_wide_vs_null_prefix",
			[]types.Decimal64{maxCoefficient, minCoefficient, 0xffffffffffffffff}, []types.Decimal64{maxCoefficient},
			8, 8, []uint64{0},
			[]types.Decimal128{{}, {B0_63: 0x35ab9f559b3d07c8, B64_127: 0xfffe5c91d14e3bcd}, {B0_63: 0xfffcb923a29c779a, B64_127: allBits}},
		},
		{
			"const_broadcast_unscaled",
			[]types.Decimal64{minCoefficient}, []types.Decimal64{0xffffffffffffffff},
			2, 2, []uint64{0},
			[]types.Decimal128{{}, {B0_63: 0x8000000000000000}},
		},
		{
			"const_broadcast_scaled",
			[]types.Decimal64{1500}, []types.Decimal64{1},
			7, 8, []uint64{0},
			[]types.Decimal128{{}, {B0_63: 2}},
		},
		{
			"left_scale_dominates",
			[]types.Decimal64{149, 150, 151, 0xffffffffffffff6a}, []types.Decimal64{1, 1, 1, 1},
			18, 2, nil,
			[]types.Decimal128{{B0_63: 1}, {B0_63: 2}, {B0_63: 2}, {B0_63: 0xfffffffffffffffe, B64_127: allBits}},
		},
		{
			"right_scale_dominates",
			[]types.Decimal64{149, 150, 151, 0xffffffffffffff6a}, []types.Decimal64{1, 1, 1, 1},
			2, 18, nil,
			[]types.Decimal128{{B0_63: 1}, {B0_63: 2}, {B0_63: 2}, {B0_63: 0xfffffffffffffffe, B64_127: allBits}},
		},
		{
			"maximum_reduction18",
			[]types.Decimal64{0xffffffff80000000, 0xffffffff80000000}, []types.Decimal64{0xffffffff80000000, 1},
			18, 18, nil,
			[]types.Decimal128{{B0_63: 5}, {}},
		},
		{
			"wide_admission_boundary",
			[]types.Decimal64{2147483647, 2147483648, 0xffffffff7fffffff}, []types.Decimal64{1, 1, 1},
			7, 8, nil,
			[]types.Decimal128{{B0_63: 0x20c49c}, {B0_63: 0x20c49c}, {B0_63: 0xffffffffffdf3b64, B64_127: allBits}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := make([]types.Decimal128, len(tc.want))
			n := nulls.NewWithSize(len(result))
			for _, row := range tc.nullRows {
				n.Add(row)
			}
			require.NoError(t, d64Mul(tc.left, tc.right, result, tc.leftScale, tc.rightScale, n))
			require.Equal(t, len(tc.nullRows), n.Count())
			for i := range result {
				masked := false
				for _, row := range tc.nullRows {
					masked = masked || row == uint64(i)
				}
				require.Equal(t, masked, n.Contains(uint64(i)), "NULL row %d", i)
				if !masked {
					require.Equal(t, tc.want[i], result[i], "row %d", i)
				}
			}
		})
	}
}

func TestD64Div(t *testing.T) {
	for _, tc := range []struct {
		name              string
		x, y              []types.Decimal64
		want              []types.Decimal128
		scales            [3]int32
		initial, wantNull []uint64
		strict, adapter   bool
		errorCode         uint16
	}{
		{
			name:   "ordinary_VV",
			x:      []types.Decimal64{18446744072709551616, 1000000000, 1},
			y:      []types.Decimal64{3, 18446744073709551613, 1},
			want:   []types.Decimal128{{B0_63: 18413410740376218283, B64_127: 18446744073709551615}, {B0_63: 18413410740376218283, B64_127: 18446744073709551615}, {B0_63: 100000000}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:     "masked_VV",
			x:        []types.Decimal64{1, 18446744073709550617, 999},
			y:        []types.Decimal64{1, 18446744073709551613, 3},
			want:     []types.Decimal128{{}, {B0_63: 33300000000}, {B0_63: 33300000000}},
			scales:   [3]int32{6, 2, 12},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "noninline24_VV_plain",
			x:      []types.Decimal64{1, 18446744073709550617, 999},
			y:      []types.Decimal64{1, 3, 3},
			want:   []types.Decimal128{{B0_63: 2003764205206896640, B64_127: 54210}, {B0_63: 15276050393356828672, B64_127: 18446744073691499649}, {B0_63: 3170693680352722944, B64_127: 18051966}},
			scales: [3]int32{0, 18, 6},
		},
		{
			name:     "noninline24_VV_null",
			x:        []types.Decimal64{1, 18446744073709550617, 999},
			y:        []types.Decimal64{1, 3, 3},
			want:     []types.Decimal128{{}, {B0_63: 15276050393356828672, B64_127: 18446744073691499649}, {B0_63: 3170693680352722944, B64_127: 18051966}},
			scales:   [3]int32{0, 18, 6},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "wide4_VV_plain",
			x:      []types.Decimal64{1, 17446744073709551617, 999999999999999999},
			y:      []types.Decimal64{1, 3, 3},
			want:   []types.Decimal128{{B0_63: 10000}, {B0_63: 5527344008095512496, B64_127: 18446744073709551435}, {B0_63: 12919400065614039120, B64_127: 180}},
			scales: [3]int32{10, 2, 12},
		},
		{
			name:     "wide4_VV_null",
			x:        []types.Decimal64{1, 17446744073709551617, 999999999999999999},
			y:        []types.Decimal64{1, 3, 3},
			want:     []types.Decimal128{{}, {B0_63: 5527344008095512496, B64_127: 18446744073709551435}, {B0_63: 12919400065614039120, B64_127: 180}},
			scales:   [3]int32{10, 2, 12},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{name: "permissive_VV_plain", x: []types.Decimal64{1, 999, 1000}, y: []types.Decimal64{1, 0, 3}, want: []types.Decimal128{{B0_63: 10000000000}, {}, {B0_63: 3333333333333}}, scales: [3]int32{4, 4, 10}, wantNull: []uint64{1}},
		{name: "permissive_VV_null", x: []types.Decimal64{1, 999, 1000}, y: []types.Decimal64{1, 0, 3}, want: []types.Decimal128{{}, {}, {B0_63: 3333333333333}}, scales: [3]int32{4, 4, 10}, initial: []uint64{0}, wantNull: []uint64{0, 1}},
		{name: "strict_VV", x: []types.Decimal64{1, 999, 1000}, y: []types.Decimal64{1, 0, 3}, scales: [3]int32{4, 4, 10}, strict: true, errorCode: moerr.ErrDivByZero},
		{
			name:   "ordinary_SV",
			x:      []types.Decimal64{999},
			y:      []types.Decimal64{3, 18446744073709551613, 1},
			want:   []types.Decimal128{{B0_63: 33300000000}, {B0_63: 18446744040409551616, B64_127: 18446744073709551615}, {B0_63: 99900000000}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:     "masked_SV",
			x:        []types.Decimal64{999},
			y:        []types.Decimal64{1, 18446744073709551613, 3},
			want:     []types.Decimal128{{}, {B0_63: 18446744040409551616, B64_127: 18446744073709551615}, {B0_63: 33300000000}},
			scales:   [3]int32{6, 2, 12},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "noninline24_SV_plain",
			x:      []types.Decimal64{999},
			y:      []types.Decimal64{1, 18446744073709551613, 3},
			want:   []types.Decimal128{{B0_63: 9512081041058168832, B64_127: 54155898}, {B0_63: 15276050393356828672, B64_127: 18446744073691499649}, {B0_63: 3170693680352722944, B64_127: 18051966}},
			scales: [3]int32{0, 18, 6},
		},
		{
			name:     "noninline24_SV_null",
			x:        []types.Decimal64{999},
			y:        []types.Decimal64{1, 18446744073709551613, 3},
			want:     []types.Decimal128{{}, {B0_63: 15276050393356828672, B64_127: 18446744073691499649}, {B0_63: 3170693680352722944, B64_127: 18051966}},
			scales:   [3]int32{0, 18, 6},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "wide4_SV_plain",
			x:      []types.Decimal64{999999999999999999},
			y:      []types.Decimal64{1, 18446744073709551613, 3},
			want:   []types.Decimal128{{B0_63: 1864712049423014128, B64_127: 542}, {B0_63: 5527344008095512496, B64_127: 18446744073709551435}, {B0_63: 12919400065614039120, B64_127: 180}},
			scales: [3]int32{10, 2, 12},
		},
		{name: "permissive_SV_plain", x: []types.Decimal64{999}, y: []types.Decimal64{1, 0, 3}, want: []types.Decimal128{{B0_63: 9990000000000}, {}, {B0_63: 3330000000000}}, scales: [3]int32{4, 4, 10}, wantNull: []uint64{1}},
		{name: "permissive_SV_null", x: []types.Decimal64{999}, y: []types.Decimal64{1, 0, 3}, want: []types.Decimal128{{}, {}, {B0_63: 3330000000000}}, scales: [3]int32{4, 4, 10}, initial: []uint64{0}, wantNull: []uint64{0, 1}},
		{name: "strict_SV", x: []types.Decimal64{999}, y: []types.Decimal64{1, 0, 3}, scales: [3]int32{4, 4, 10}, strict: true, errorCode: moerr.ErrDivByZero},
		{
			name:   "ordinary_VS",
			x:      []types.Decimal64{18446744072709551616, 1000000000, 1},
			y:      []types.Decimal64{3},
			want:   []types.Decimal128{{B0_63: 18413410740376218283, B64_127: 18446744073709551615}, {B0_63: 33333333333333333}, {B0_63: 33333333}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:     "masked_VS",
			x:        []types.Decimal64{1, 18446744073709550617, 999},
			y:        []types.Decimal64{3},
			want:     []types.Decimal128{{}, {B0_63: 18446744040409551616, B64_127: 18446744073709551615}, {B0_63: 33300000000}},
			scales:   [3]int32{6, 2, 12},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "noninline24_VS_plain",
			x:      []types.Decimal64{1, 18446744073709550617, 999},
			y:      []types.Decimal64{3},
			want:   []types.Decimal128{{B0_63: 667921401735632213, B64_127: 18070}, {B0_63: 15276050393356828672, B64_127: 18446744073691499649}, {B0_63: 3170693680352722944, B64_127: 18051966}},
			scales: [3]int32{0, 18, 6},
		},
		{
			name:     "noninline24_VS_null",
			x:        []types.Decimal64{1, 18446744073709550617, 999},
			y:        []types.Decimal64{3},
			want:     []types.Decimal128{{}, {B0_63: 15276050393356828672, B64_127: 18446744073691499649}, {B0_63: 3170693680352722944, B64_127: 18051966}},
			scales:   [3]int32{0, 18, 6},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "wide4_VS_plain",
			x:      []types.Decimal64{1, 17446744073709551617, 999999999999999999},
			y:      []types.Decimal64{3},
			want:   []types.Decimal128{{B0_63: 3333}, {B0_63: 5527344008095512496, B64_127: 18446744073709551435}, {B0_63: 12919400065614039120, B64_127: 180}},
			scales: [3]int32{10, 2, 12},
		},
		{name: "permissive_VS_plain", x: []types.Decimal64{1, 999, 1000}, y: []types.Decimal64{0}, want: []types.Decimal128{{}, {}, {}}, scales: [3]int32{4, 4, 10}, wantNull: []uint64{0, 1, 2}},
		{name: "strict_VS", x: []types.Decimal64{1, 999, 1000}, y: []types.Decimal64{0}, scales: [3]int32{4, 4, 10}, strict: true, errorCode: moerr.ErrDivByZero},
		{
			name:   "unequal_VV",
			x:      []types.Decimal64{18446744073709501617, 49999, 1},
			y:      []types.Decimal64{3, 18446744073709551613, 20},
			want:   []types.Decimal128{{B0_63: 18446727407376218283, B64_127: 18446744073709551615}, {B0_63: 18446727407376218283, B64_127: 18446744073709551615}, {B0_63: 50000000}},
			scales: [3]int32{1, 3, 7},
			strict: true,
		},
		{name: "Kernel", x: []types.Decimal64{100}, y: []types.Decimal64{3}, want: []types.Decimal128{{B0_63: 3333333333}}, scales: [3]int32{2, 2, 8}, strict: true, adapter: true},
		{
			name:   "highscale_VV",
			x:      []types.Decimal64{18446744073709550617, 999, 1},
			y:      []types.Decimal64{3, 18446744073709551613, 18446744073709551615},
			want:   []types.Decimal128{{B0_63: 18446744073709518316, B64_127: 18446744073709551615}, {B0_63: 18446744073709518316, B64_127: 18446744073709551615}, {B0_63: 18446744073709551516, B64_127: 18446744073709551615}},
			scales: [3]int32{18, 2, 18},
		},
		{
			name:   "admission19_VV_plain",
			x:      []types.Decimal64{1, 9223372036854775808, 9223372036854775808},
			y:      []types.Decimal64{1, 9223372036854775808, 18446744073709551615},
			want:   []types.Decimal128{{B0_63: 10000000000000000000}, {B0_63: 10000000000000000000}, {B64_127: 5000000000000000000}},
			scales: [3]int32{0, 0, 19},
			strict: true,
		},
		{
			name:     "admission19_VV_null",
			x:        []types.Decimal64{1, 9223372036854775808, 9223372036854775808},
			y:        []types.Decimal64{1, 9223372036854775808, 18446744073709551615},
			want:     []types.Decimal128{{}, {B0_63: 10000000000000000000}, {B64_127: 5000000000000000000}},
			scales:   [3]int32{0, 0, 19},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
		{
			name:   "noninline20_VV",
			x:      []types.Decimal64{1, 18446744073709550617, 999},
			y:      []types.Decimal64{1, 18446744073709551613, 3},
			want:   []types.Decimal128{{B0_63: 7766279631452241920, B64_127: 5}, {B0_63: 3626946954259333120, B64_127: 1805}, {B0_63: 3626946954259333120, B64_127: 1805}},
			scales: [3]int32{0, 0, 20},
			strict: true,
		},
		{
			name:   "admission19_SV_plain",
			x:      []types.Decimal64{9223372036854775808},
			y:      []types.Decimal64{1, 9223372036854775808, 18446744073709551615},
			want:   []types.Decimal128{{B64_127: 13446744073709551616}, {B0_63: 10000000000000000000}, {B64_127: 5000000000000000000}},
			scales: [3]int32{0, 0, 19},
			strict: true,
		},
		{
			name:     "admission19_SV_null",
			x:        []types.Decimal64{9223372036854775808},
			y:        []types.Decimal64{1, 9223372036854775808, 18446744073709551615},
			want:     []types.Decimal128{{}, {B0_63: 10000000000000000000}, {B64_127: 5000000000000000000}},
			scales:   [3]int32{0, 0, 19},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
		{
			name:   "noninline20_SV",
			x:      []types.Decimal64{999},
			y:      []types.Decimal64{1, 18446744073709551613, 3},
			want:   []types.Decimal128{{B0_63: 10880840862777999360, B64_127: 5415}, {B0_63: 14819797119450218496, B64_127: 18446744073709549810}, {B0_63: 3626946954259333120, B64_127: 1805}},
			scales: [3]int32{0, 0, 20},
			strict: true,
		},
		{
			name:   "admission19_VS_plain",
			x:      []types.Decimal64{1, 9223372036854775808, 9223372036854775808},
			y:      []types.Decimal64{9223372036854775808},
			want:   []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 18446744073709551615}, {B0_63: 10000000000000000000}, {B0_63: 10000000000000000000}},
			scales: [3]int32{0, 0, 19},
			strict: true,
		},
		{
			name:     "admission19_VS_null",
			x:        []types.Decimal64{1, 9223372036854775808, 9223372036854775808},
			y:        []types.Decimal64{9223372036854775808},
			want:     []types.Decimal128{{}, {B0_63: 10000000000000000000}, {B0_63: 10000000000000000000}},
			scales:   [3]int32{0, 0, 19},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
		{
			name:   "noninline20_VS",
			x:      []types.Decimal64{1, 18446744073709550617, 999},
			y:      []types.Decimal64{3},
			want:   []types.Decimal128{{B0_63: 14886589259623781717, B64_127: 1}, {B0_63: 14819797119450218496, B64_127: 18446744073709549810}, {B0_63: 3626946954259333120, B64_127: 1805}},
			scales: [3]int32{0, 0, 20},
			strict: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := max(len(tc.x), len(tc.y))
			mask := nulls.NewWithSize(n)
			for _, i := range tc.initial {
				mask.Add(i)
			}
			out := make([]types.Decimal128, n)
			var err error
			if tc.adapter {
				err = d64DivKernelAtScale(tc.strict, tc.scales[2])(tc.x, tc.y, out, tc.scales[0], tc.scales[1], mask)
			} else {
				err = d64DivAtScale(tc.x, tc.y, out, tc.scales[0], tc.scales[1], tc.scales[2], mask, tc.strict)
			}
			if tc.errorCode != 0 {
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, tc.errorCode), "error: %v", err)
				return
			}
			require.NoError(t, err)
			require.Len(t, tc.want, n)
			require.Equal(t, len(tc.wantNull), mask.Count())
			for i := range out {
				expectedNull := false
				for _, j := range tc.wantNull {
					if uint64(i) == j {
						expectedNull = true
					}
				}
				require.Equal(t, expectedNull, mask.Contains(uint64(i)), "row %d null", i)
				if !expectedNull {
					require.Equal(t, tc.want[i], out[i], "row %d", i)
				}
			}
		})
	}
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
		_ = d64DivAtScale(xs, ys, rs, 2, 2, 8, nul, true)
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
	// Coefficients are independently derived; no decimal producer computes the oracle.
	const allBits = ^uint64(0)
	maxCoefficient := types.Decimal128{B0_63: allBits, B64_127: 0x7fffffffffffffff}
	minCoefficient := types.Decimal128{B64_127: 0x8000000000000000}
	negativeOne := types.Decimal128{B0_63: allBits, B64_127: allBits}
	carryInput := types.Decimal128{B0_63: allBits}
	negativeCarryInput := types.Decimal128{B0_63: 0x1, B64_127: allBits}
	carryBoundary := types.Decimal128{B64_127: 0x1}
	scaledWide := types.Decimal128{B0_63: 0x1, B64_127: 0x1}
	negativeScaledWide := types.Decimal128{B0_63: allBits, B64_127: 0xfffffffffffffffe}
	for _, tc := range []struct {
		name                  string
		left, right           []types.Decimal128
		leftScale, rightScale int32
		nullRows              []uint64
		want                  []types.Decimal128
		wantError             uint16
	}{
		{
			"same_vv/no_null",
			[]types.Decimal128{carryInput, negativeCarryInput, maxCoefficient, minCoefficient},
			[]types.Decimal128{{B0_63: 0x1}, negativeOne, {}, {}},
			2, 2, nil,
			[]types.Decimal128{carryBoundary, {B64_127: allBits}, maxCoefficient, minCoefficient}, 0,
		},
		{
			"same_vv/null_middle",
			[]types.Decimal128{negativeCarryInput, maxCoefficient, {}},
			[]types.Decimal128{negativeOne, {B0_63: 0x1}, {}},
			2, 2, []uint64{1},
			[]types.Decimal128{{B64_127: allBits}, {}, {}}, 0,
		},
		{
			"same_sv/no_null",
			[]types.Decimal128{carryInput},
			[]types.Decimal128{{B0_63: 0x1}, negativeOne, negativeCarryInput},
			2, 2, nil,
			[]types.Decimal128{carryBoundary, {B0_63: 0xfffffffffffffffe}, {}}, 0,
		},
		{
			"same_sv/null_middle",
			[]types.Decimal128{carryInput},
			[]types.Decimal128{negativeOne, maxCoefficient, negativeCarryInput},
			2, 2, []uint64{1},
			[]types.Decimal128{{B0_63: 0xfffffffffffffffe}, {}, {}}, 0,
		},
		{
			"same_vs/no_null",
			[]types.Decimal128{{B0_63: 0x1}, negativeOne, negativeCarryInput},
			[]types.Decimal128{carryInput},
			2, 2, nil,
			[]types.Decimal128{carryBoundary, {B0_63: 0xfffffffffffffffe}, {}}, 0,
		},
		{
			"same_vs/null_middle",
			[]types.Decimal128{negativeOne, maxCoefficient, negativeCarryInput},
			[]types.Decimal128{carryInput},
			2, 2, []uint64{1},
			[]types.Decimal128{{B0_63: 0xfffffffffffffffe}, {}, {}}, 0,
		},
		{
			"diff_vv_left_lower/no_null",
			[]types.Decimal128{{B0_63: 0x11}, {B0_63: 0xffffffffffffffe9, B64_127: allBits}, {}},
			[]types.Decimal128{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: allBits}, {}},
			1, 3, nil,
			[]types.Decimal128{{B0_63: 0x6a7}, {B0_63: 0xfffffffffffff6fd, B64_127: allBits}, {}}, 0,
		},
		{
			"diff_vv_left_lower/null_middle",
			[]types.Decimal128{{B0_63: 0xffffffffffffffe9, B64_127: allBits}, {B0_63: 0x11}, {}},
			[]types.Decimal128{{B0_63: 0xfffffffffffffff9, B64_127: allBits}, maxCoefficient, {}},
			1, 3, []uint64{1},
			[]types.Decimal128{{B0_63: 0xfffffffffffff6fd, B64_127: allBits}, {}, {}}, 0,
		},
		{
			"diff_sv_left_lower/no_null",
			[]types.Decimal128{{B0_63: 0x11}},
			[]types.Decimal128{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: allBits}, {B0_63: 0xfffffffffffff95c, B64_127: allBits}},
			1, 3, nil,
			[]types.Decimal128{{B0_63: 0x6a7}, {B0_63: 0x69d}, {}}, 0,
		},
		{
			"diff_sv_left_lower/null_middle",
			[]types.Decimal128{{B0_63: 0x11}},
			[]types.Decimal128{{B0_63: 0xfffffffffffffff9, B64_127: allBits}, maxCoefficient, {B0_63: 0xfffffffffffff95c, B64_127: allBits}},
			1, 3, []uint64{1},
			[]types.Decimal128{{B0_63: 0x69d}, {}, {}}, 0,
		},
		{
			"diff_vs_right_lower/no_null",
			[]types.Decimal128{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: allBits}, {B0_63: 0xfffffffffffff95c, B64_127: allBits}},
			[]types.Decimal128{{B0_63: 0x11}},
			3, 1, nil,
			[]types.Decimal128{{B0_63: 0x6a7}, {B0_63: 0x69d}, {}}, 0,
		},
		{
			"diff_vs_right_lower/null_middle",
			[]types.Decimal128{{B0_63: 0xfffffffffffffff9, B64_127: allBits}, maxCoefficient, {B0_63: 0xfffffffffffff95c, B64_127: allBits}},
			[]types.Decimal128{{B0_63: 0x11}},
			3, 1, []uint64{1},
			[]types.Decimal128{{B0_63: 0x69d}, {}, {}}, 0,
		},
		// checked scaled NULL poison, with later live row; D128 wide signed scaling
		{
			"diff_sv_right_lower/null_middle",
			[]types.Decimal128{{B0_63: 0x5}},
			[]types.Decimal128{scaledWide, maxCoefficient, negativeScaledWide},
			2, 0, []uint64{1},
			[]types.Decimal128{{B0_63: 0x69, B64_127: 0x64}, {}, {B0_63: 0xffffffffffffffa1, B64_127: 0xffffffffffffff9b}}, 0,
		},
		// opposite vector scaling direction
		{
			"diff_vs_left_lower/no_null",
			[]types.Decimal128{scaledWide, negativeScaledWide},
			[]types.Decimal128{{B0_63: 0x5}},
			0, 2, nil,
			[]types.Decimal128{{B0_63: 0x69, B64_127: 0x64}, {B0_63: 0xffffffffffffffa1, B64_127: 0xffffffffffffff9b}}, 0,
		},
		// opposite vector scaling direction
		{
			"diff_vv_right_lower/no_null",
			[]types.Decimal128{{B0_63: 0x5}, {B0_63: 0xfffffffffffffff9, B64_127: allBits}},
			[]types.Decimal128{scaledWide, negativeScaledWide},
			2, 0, nil,
			[]types.Decimal128{{B0_63: 0x69, B64_127: 0x64}, {B0_63: 0xffffffffffffff95, B64_127: 0xffffffffffffff9b}}, 0,
		},
		{
			"overflow_negative",
			[]types.Decimal128{minCoefficient},
			[]types.Decimal128{negativeOne},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"scale_error_scalar",
			[]types.Decimal128{maxCoefficient},
			[]types.Decimal128{{B0_63: 0x1}, {B0_63: 0x2}},
			0, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"overflow_vv/no_null",
			[]types.Decimal128{maxCoefficient, maxCoefficient},
			[]types.Decimal128{{B0_63: 0x1}, {B0_63: 0x1}},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"overflow_vv/null_prefix",
			[]types.Decimal128{maxCoefficient, maxCoefficient},
			[]types.Decimal128{{B0_63: 0x1}, {B0_63: 0x1}},
			2, 2, []uint64{0},
			nil, moerr.ErrInvalidInput,
		},
		{
			"overflow_sv/no_null",
			[]types.Decimal128{maxCoefficient},
			[]types.Decimal128{{B0_63: 0x1}, {B0_63: 0x1}},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"overflow_sv/null_prefix",
			[]types.Decimal128{maxCoefficient},
			[]types.Decimal128{{B0_63: 0x1}, {B0_63: 0x1}},
			2, 2, []uint64{0},
			nil, moerr.ErrInvalidInput,
		},
		{
			"overflow_vs/no_null",
			[]types.Decimal128{maxCoefficient, maxCoefficient},
			[]types.Decimal128{{B0_63: 0x1}},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"overflow_vs/null_prefix",
			[]types.Decimal128{maxCoefficient, maxCoefficient},
			[]types.Decimal128{{B0_63: 0x1}},
			2, 2, []uint64{0},
			nil, moerr.ErrInvalidInput,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			length := max(len(tc.left), len(tc.right))
			n := nulls.NewWithSize(length)
			for _, row := range tc.nullRows {
				n.Add(row)
			}
			result := make([]types.Decimal128, length)
			err := d128Add(tc.left, tc.right, result, tc.leftScale, tc.rightScale, n)
			if tc.wantError != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.wantError), "error: %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.nullRows), n.Count())
			for i := range result {
				masked := false
				for _, row := range tc.nullRows {
					masked = masked || row == uint64(i)
				}
				require.Equal(t, masked, n.Contains(uint64(i)), "NULL row %d", i)
				if !masked && tc.wantError == 0 {
					require.Equal(t, tc.want[i], result[i], "row %d", i)
				}
			}
		})
	}
}

func TestD128Sub(t *testing.T) {
	// Coefficients are independently derived; no decimal producer computes the oracle.
	const allBits = ^uint64(0)
	maxCoefficient := types.Decimal128{B0_63: allBits, B64_127: 0x7fffffffffffffff}
	minCoefficient := types.Decimal128{B64_127: 0x8000000000000000}
	negativeOne := types.Decimal128{B0_63: allBits, B64_127: allBits}
	carryInput := types.Decimal128{B0_63: allBits}
	negativeCarryInput := types.Decimal128{B0_63: 0x1, B64_127: allBits}
	carryBoundary := types.Decimal128{B64_127: 0x1}
	scaledWide := types.Decimal128{B0_63: 0x1, B64_127: 0x1}
	negativeScaledWide := types.Decimal128{B0_63: allBits, B64_127: 0xfffffffffffffffe}
	for _, tc := range []struct {
		name                  string
		left, right           []types.Decimal128
		leftScale, rightScale int32
		nullRows              []uint64
		want                  []types.Decimal128
		wantError             uint16
	}{
		{
			"same_vv/no_null",
			[]types.Decimal128{carryBoundary, negativeCarryInput, maxCoefficient, minCoefficient},
			[]types.Decimal128{{B0_63: 0x1}, negativeOne, {}, {}},
			2, 2, nil,
			[]types.Decimal128{carryInput, {B0_63: 0x2, B64_127: allBits}, maxCoefficient, minCoefficient}, 0,
		},
		{
			"same_vv/null_middle",
			[]types.Decimal128{carryBoundary, minCoefficient, {}},
			[]types.Decimal128{negativeOne, {B0_63: 0x1}, {}},
			2, 2, []uint64{1},
			[]types.Decimal128{scaledWide, {}, {}}, 0,
		},
		{
			"same_sv/no_null",
			[]types.Decimal128{carryInput},
			[]types.Decimal128{{B0_63: 0x1}, negativeOne, negativeCarryInput},
			2, 2, nil,
			[]types.Decimal128{{B0_63: 0xfffffffffffffffe}, carryBoundary, {B0_63: 0xfffffffffffffffe, B64_127: 0x1}}, 0,
		},
		{
			"same_sv/null_middle",
			[]types.Decimal128{carryInput},
			[]types.Decimal128{negativeOne, minCoefficient, negativeCarryInput},
			2, 2, []uint64{1},
			[]types.Decimal128{carryBoundary, {}, {B0_63: 0xfffffffffffffffe, B64_127: 0x1}}, 0,
		},
		{
			"same_vs/no_null",
			[]types.Decimal128{{B0_63: 0x1}, negativeOne, negativeCarryInput},
			[]types.Decimal128{carryInput},
			2, 2, nil,
			[]types.Decimal128{{B0_63: 0x2, B64_127: allBits}, {B64_127: allBits}, {B0_63: 0x2, B64_127: 0xfffffffffffffffe}}, 0,
		},
		{
			"same_vs/null_middle",
			[]types.Decimal128{negativeOne, minCoefficient, negativeCarryInput},
			[]types.Decimal128{carryInput},
			2, 2, []uint64{1},
			[]types.Decimal128{{B64_127: allBits}, {}, {B0_63: 0x2, B64_127: 0xfffffffffffffffe}}, 0,
		},
		{
			"diff_vv_left_lower/no_null",
			[]types.Decimal128{{B0_63: 0x11}, {B0_63: 0xffffffffffffffe9, B64_127: allBits}, {}},
			[]types.Decimal128{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: allBits}, {}},
			1, 3, nil,
			[]types.Decimal128{{B0_63: 0x6a1}, {B0_63: 0xfffffffffffff70b, B64_127: allBits}, {}}, 0,
		},
		{
			"diff_vv_left_lower/null_middle",
			[]types.Decimal128{{B0_63: 0xffffffffffffffe9, B64_127: allBits}, {B0_63: 0x11}, {}},
			[]types.Decimal128{{B0_63: 0xfffffffffffffff9, B64_127: allBits}, minCoefficient, {}},
			1, 3, []uint64{1},
			[]types.Decimal128{{B0_63: 0xfffffffffffff70b, B64_127: allBits}, {}, {}}, 0,
		},
		{
			"diff_sv_left_lower/no_null",
			[]types.Decimal128{{B0_63: 0x11}},
			[]types.Decimal128{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: allBits}, {B0_63: 0xfffffffffffff95c, B64_127: allBits}},
			1, 3, nil,
			[]types.Decimal128{{B0_63: 0x6a1}, {B0_63: 0x6ab}, {B0_63: 0xd48}}, 0,
		},
		{
			"diff_sv_left_lower/null_middle",
			[]types.Decimal128{{B0_63: 0x11}},
			[]types.Decimal128{{B0_63: 0xfffffffffffffff9, B64_127: allBits}, minCoefficient, {B0_63: 0xfffffffffffff95c, B64_127: allBits}},
			1, 3, []uint64{1},
			[]types.Decimal128{{B0_63: 0x6ab}, {}, {B0_63: 0xd48}}, 0,
		},
		{
			"diff_vs_right_lower/no_null",
			[]types.Decimal128{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: allBits}, {B0_63: 0xfffffffffffff95c, B64_127: allBits}},
			[]types.Decimal128{{B0_63: 0x11}},
			3, 1, nil,
			[]types.Decimal128{{B0_63: 0xfffffffffffff95f, B64_127: allBits}, {B0_63: 0xfffffffffffff955, B64_127: allBits}, {B0_63: 0xfffffffffffff2b8, B64_127: allBits}}, 0,
		},
		{
			"diff_vs_right_lower/null_middle",
			[]types.Decimal128{{B0_63: 0xfffffffffffffff9, B64_127: allBits}, minCoefficient, {B0_63: 0xfffffffffffff95c, B64_127: allBits}},
			[]types.Decimal128{{B0_63: 0x11}},
			3, 1, []uint64{1},
			[]types.Decimal128{{B0_63: 0xfffffffffffff955, B64_127: allBits}, {}, {B0_63: 0xfffffffffffff2b8, B64_127: allBits}}, 0,
		},
		// checked scaled NULL poison, with later live row; D128 wide signed scaling
		{
			"diff_sv_right_lower/null_middle",
			[]types.Decimal128{{B0_63: 0x5}},
			[]types.Decimal128{scaledWide, maxCoefficient, negativeScaledWide},
			2, 0, []uint64{1},
			[]types.Decimal128{{B0_63: 0xffffffffffffffa1, B64_127: 0xffffffffffffff9b}, {}, {B0_63: 0x69, B64_127: 0x64}}, 0,
		},
		// opposite vector scaling direction
		{
			"diff_vs_left_lower/no_null",
			[]types.Decimal128{scaledWide, negativeScaledWide},
			[]types.Decimal128{{B0_63: 0x5}},
			0, 2, nil,
			[]types.Decimal128{{B0_63: 0x5f, B64_127: 0x64}, {B0_63: 0xffffffffffffff97, B64_127: 0xffffffffffffff9b}}, 0,
		},
		// opposite vector scaling direction
		{
			"diff_vv_right_lower/no_null",
			[]types.Decimal128{{B0_63: 0x5}, {B0_63: 0xfffffffffffffff9, B64_127: allBits}},
			[]types.Decimal128{scaledWide, negativeScaledWide},
			2, 0, nil,
			[]types.Decimal128{{B0_63: 0xffffffffffffffa1, B64_127: 0xffffffffffffff9b}, {B0_63: 0x5d, B64_127: 0x64}}, 0,
		},
		{
			"overflow_positive",
			[]types.Decimal128{maxCoefficient},
			[]types.Decimal128{negativeOne},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"overflow_negative",
			[]types.Decimal128{minCoefficient},
			[]types.Decimal128{{B0_63: 0x1}},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"scale_error_vector",
			[]types.Decimal128{{B0_63: 0x1}, maxCoefficient},
			[]types.Decimal128{{B0_63: 0x1}},
			0, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			length := max(len(tc.left), len(tc.right))
			n := nulls.NewWithSize(length)
			for _, row := range tc.nullRows {
				n.Add(row)
			}
			result := make([]types.Decimal128, length)
			err := d128Sub(tc.left, tc.right, result, tc.leftScale, tc.rightScale, n)
			if tc.wantError != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.wantError), "error: %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.nullRows), n.Count())
			for i := range result {
				masked := false
				for _, row := range tc.nullRows {
					masked = masked || row == uint64(i)
				}
				require.Equal(t, masked, n.Contains(uint64(i)), "NULL row %d", i)
				if !masked && tc.wantError == 0 {
					require.Equal(t, tc.want[i], result[i], "row %d", i)
				}
			}
		})
	}
}

func TestD128SubDiffScaleDecimalMinusIntLiteralCases(t *testing.T) {
	for _, tc := range []struct {
		name     string
		left     int64
		right    int64
		want     int64
		wantText string
	}{
		{name: "positive result", left: 800000, right: 1299, want: 670100, wantText: "6701.00"},
		{name: "zero result", left: 30000, right: 300, want: 0, wantText: "0.00"},
		{name: "negative by one", left: 30000, right: 301, want: -100, wantText: "-1.00"},
		{name: "negative by hundred", left: 10000, right: 200, want: -10000, wantText: "-100.00"},
		{name: "large negative", left: 30000, right: 3999, want: -369900, wantText: "-3699.00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v1 := []types.Decimal128{types.Decimal128FromInt64(tc.left)}
			v2 := []types.Decimal128{types.Decimal128FromInt64(tc.right)}
			rs := make([]types.Decimal128, 1)
			nul := nulls.NewWithSize(1)

			idx, err := d128SubDiffScale(v1, v2, rs, 2, 0, nul)
			require.NoError(t, err)
			require.Equal(t, -1, idx)
			require.Equal(t, types.Decimal128FromInt64(tc.want), rs[0])
			require.Equal(t, tc.wantText, rs[0].Format(2))
		})
	}

	t.Run("left scalar right vector with null", func(t *testing.T) {
		v1 := []types.Decimal128{types.Decimal128FromInt64(30000)}
		v2 := []types.Decimal128{
			types.Decimal128FromInt64(300),
			types.Decimal128FromInt64(301),
			types.Decimal128FromInt64(777),
			types.Decimal128FromInt64(3999),
		}
		rs := make([]types.Decimal128, len(v2))
		nul := nulls.NewWithSize(len(v2))
		nul.Add(2)

		idx, err := d128SubDiffScale(v1, v2, rs, 2, 0, nul)
		require.NoError(t, err)
		require.Equal(t, -1, idx)
		require.True(t, nul.Contains(2))
		require.Equal(t, types.Decimal128FromInt64(0), rs[0])
		require.Equal(t, types.Decimal128FromInt64(-100), rs[1])
		require.Equal(t, types.Decimal128FromInt64(-369900), rs[3])
		require.Equal(t, "0.00", rs[0].Format(2))
		require.Equal(t, "-1.00", rs[1].Format(2))
		require.Equal(t, "-3699.00", rs[3].Format(2))
	})
}

func TestD128SameScaleAliasOverflowChecks(t *testing.T) {
	max := types.Decimal128{B0_63: ^uint64(0), B64_127: ^uint64(0) >> 1}

	t.Run("add same length no null", func(t *testing.T) {
		rs := []types.Decimal128{types.Decimal128FromInt64(1)}
		nul := nulls.NewWithSize(1)

		idx := d128AddSameScale([]types.Decimal128{max}, rs, rs, nul)
		require.Equal(t, 0, idx)
	})

	t.Run("add same length with null", func(t *testing.T) {
		rs := []types.Decimal128{types.Decimal128FromInt64(1), types.Decimal128FromInt64(1)}
		nul := nulls.NewWithSize(2)
		nul.Add(0)

		idx := d128AddSameScale([]types.Decimal128{max, max}, rs, rs, nul)
		require.Equal(t, 1, idx)
	})

	t.Run("add left scalar no null", func(t *testing.T) {
		rs := []types.Decimal128{types.Decimal128FromInt64(1), types.Decimal128FromInt64(1)}
		nul := nulls.NewWithSize(2)

		idx := d128AddSameScale([]types.Decimal128{max}, rs, rs, nul)
		require.Equal(t, 0, idx)
	})

	t.Run("add left scalar with null", func(t *testing.T) {
		rs := []types.Decimal128{types.Decimal128FromInt64(1), types.Decimal128FromInt64(1)}
		nul := nulls.NewWithSize(2)
		nul.Add(0)

		idx := d128AddSameScale([]types.Decimal128{max}, rs, rs, nul)
		require.Equal(t, 1, idx)
	})

	t.Run("add right scalar no null", func(t *testing.T) {
		rs := []types.Decimal128{max, max}
		nul := nulls.NewWithSize(2)

		idx := d128AddSameScale(rs, []types.Decimal128{types.Decimal128FromInt64(1)}, rs, nul)
		require.Equal(t, 0, idx)
	})

	t.Run("add right scalar with null", func(t *testing.T) {
		rs := []types.Decimal128{max, max}
		nul := nulls.NewWithSize(2)
		nul.Add(0)

		idx := d128AddSameScale(rs, []types.Decimal128{types.Decimal128FromInt64(1)}, rs, nul)
		require.Equal(t, 1, idx)
	})

	t.Run("sub same length no null", func(t *testing.T) {
		rs := []types.Decimal128{types.Decimal128FromInt64(30100)}
		nul := nulls.NewWithSize(1)

		idx := d128SubSameScale([]types.Decimal128{types.Decimal128FromInt64(30000)}, rs, rs, nul)
		require.Equal(t, -1, idx)
		require.Equal(t, types.Decimal128FromInt64(-100), rs[0])
	})

	t.Run("sub same length with null", func(t *testing.T) {
		rs := []types.Decimal128{types.Decimal128FromInt64(30100), types.Decimal128FromInt64(30100)}
		nul := nulls.NewWithSize(2)
		nul.Add(0)

		idx := d128SubSameScale(
			[]types.Decimal128{types.Decimal128FromInt64(30000), types.Decimal128FromInt64(30000)},
			rs,
			rs,
			nul,
		)
		require.Equal(t, -1, idx)
		require.Equal(t, types.Decimal128FromInt64(-100), rs[1])
	})

	t.Run("sub left scalar no null", func(t *testing.T) {
		rs := []types.Decimal128{types.Decimal128FromInt64(30100), types.Decimal128FromInt64(30100)}
		nul := nulls.NewWithSize(2)

		idx := d128SubSameScale([]types.Decimal128{types.Decimal128FromInt64(30000)}, rs, rs, nul)
		require.Equal(t, -1, idx)
		require.Equal(t, types.Decimal128FromInt64(-100), rs[0])
	})

	t.Run("sub left scalar with null", func(t *testing.T) {
		rs := []types.Decimal128{types.Decimal128FromInt64(30100), types.Decimal128FromInt64(30100)}
		nul := nulls.NewWithSize(2)
		nul.Add(0)

		idx := d128SubSameScale([]types.Decimal128{types.Decimal128FromInt64(30000)}, rs, rs, nul)
		require.Equal(t, -1, idx)
		require.Equal(t, types.Decimal128FromInt64(-100), rs[1])
	})

	t.Run("sub right scalar no null", func(t *testing.T) {
		rs := []types.Decimal128{max, max}
		nul := nulls.NewWithSize(2)

		idx := d128SubSameScale(rs, []types.Decimal128{types.Decimal128FromInt64(-1)}, rs, nul)
		require.Equal(t, 0, idx)
	})

	t.Run("sub right scalar with null", func(t *testing.T) {
		rs := []types.Decimal128{max, max}
		nul := nulls.NewWithSize(2)
		nul.Add(0)

		idx := d128SubSameScale(rs, []types.Decimal128{types.Decimal128FromInt64(-1)}, rs, nul)
		require.Equal(t, 1, idx)
	})
}

func TestD64SameScaleAliasOverflowChecks(t *testing.T) {
	max := types.Decimal64(1<<63 - 1)
	one := types.Decimal64(1)
	negHundred := int64(-100)
	negOne := int64(-1)
	wantNegHundred := types.Decimal64(uint64(negHundred))

	t.Run("add same length no null", func(t *testing.T) {
		rs := []types.Decimal64{one}
		nul := nulls.NewWithSize(1)

		idx := d64AddSameScale([]types.Decimal64{max}, rs, rs, nul)
		require.Equal(t, 0, idx)
	})

	t.Run("add same length with null", func(t *testing.T) {
		rs := []types.Decimal64{one, one}
		nul := nulls.NewWithSize(2)
		nul.Add(0)

		idx := d64AddSameScale([]types.Decimal64{max, max}, rs, rs, nul)
		require.Equal(t, 1, idx)
	})

	t.Run("add left scalar no null", func(t *testing.T) {
		rs := []types.Decimal64{one, one}
		nul := nulls.NewWithSize(2)

		idx := d64AddSameScale([]types.Decimal64{max}, rs, rs, nul)
		require.Equal(t, 0, idx)
	})

	t.Run("add left scalar with null", func(t *testing.T) {
		rs := []types.Decimal64{one, one}
		nul := nulls.NewWithSize(2)
		nul.Add(0)

		idx := d64AddSameScale([]types.Decimal64{max}, rs, rs, nul)
		require.Equal(t, 1, idx)
	})

	t.Run("add right scalar no null", func(t *testing.T) {
		rs := []types.Decimal64{max, max}
		nul := nulls.NewWithSize(2)

		idx := d64AddSameScale(rs, []types.Decimal64{one}, rs, nul)
		require.Equal(t, 0, idx)
	})

	t.Run("add right scalar with null", func(t *testing.T) {
		rs := []types.Decimal64{max, max}
		nul := nulls.NewWithSize(2)
		nul.Add(0)

		idx := d64AddSameScale(rs, []types.Decimal64{one}, rs, nul)
		require.Equal(t, 1, idx)
	})

	t.Run("sub same length no null", func(t *testing.T) {
		rs := []types.Decimal64{30100}
		nul := nulls.NewWithSize(1)

		idx := d64SubSameScale([]types.Decimal64{30000}, rs, rs, nul)
		require.Equal(t, -1, idx)
		require.Equal(t, wantNegHundred, rs[0])
	})

	t.Run("sub same length with null", func(t *testing.T) {
		rs := []types.Decimal64{30100, 30100}
		nul := nulls.NewWithSize(2)
		nul.Add(0)

		idx := d64SubSameScale([]types.Decimal64{30000, 30000}, rs, rs, nul)
		require.Equal(t, -1, idx)
		require.Equal(t, wantNegHundred, rs[1])
	})

	t.Run("sub left scalar no null", func(t *testing.T) {
		rs := []types.Decimal64{30100, 30100}
		nul := nulls.NewWithSize(2)

		idx := d64SubSameScale([]types.Decimal64{30000}, rs, rs, nul)
		require.Equal(t, -1, idx)
		require.Equal(t, wantNegHundred, rs[0])
	})

	t.Run("sub left scalar with null", func(t *testing.T) {
		rs := []types.Decimal64{30100, 30100}
		nul := nulls.NewWithSize(2)
		nul.Add(0)

		idx := d64SubSameScale([]types.Decimal64{30000}, rs, rs, nul)
		require.Equal(t, -1, idx)
		require.Equal(t, wantNegHundred, rs[1])
	})

	t.Run("sub right scalar no null", func(t *testing.T) {
		rs := []types.Decimal64{max, max}
		nul := nulls.NewWithSize(2)

		idx := d64SubSameScale(rs, []types.Decimal64{types.Decimal64(uint64(negOne))}, rs, nul)
		require.Equal(t, 0, idx)
	})

	t.Run("sub right scalar with null", func(t *testing.T) {
		rs := []types.Decimal64{max, max}
		nul := nulls.NewWithSize(2)
		nul.Add(0)

		idx := d64SubSameScale(rs, []types.Decimal64{types.Decimal64(uint64(negOne))}, rs, nul)
		require.Equal(t, 1, idx)
	})
}

// TestD128MulPow10Carry verifies that d128MulPow10 correctly detects overflow
// when the cross-product carry overflows uint64 (hi + crossLo > 2^64).
func TestD128MulPow10Carry(t *testing.T) {
	// (2^65-1)*10^19 exceeds the positive D128 range.
	x := types.Decimal128{B0_63: ^uint64(0), B64_127: 1}
	require.False(t, d128MulPow10(&x, 19))

	// (2^65-1)*10 = 20*2^64-10: low limb 2^64-10, high limb 19.
	x = types.Decimal128{B0_63: ^uint64(0), B64_127: 1}
	require.True(t, d128MulPow10(&x, 1))
	require.Equal(t, types.Decimal128{B0_63: 18446744073709551606, B64_127: 19}, x)
}

func TestD128Mul(t *testing.T) {
	type decimal = types.Decimal128
	for _, tc := range []struct {
		name       string
		x, y, want []decimal
		s1, s2     int32
		masked     []uint64
		errorCode  uint16
	}{
		{
			name: "int64_unscaled_VV",
			x:    []decimal{{B0_63: 123456}, {B0_63: 18446744073709546616, B64_127: ^uint64(0)}, {B0_63: 9223372036854775808, B64_127: ^uint64(0)}},
			y:    []decimal{{B0_63: 789012}, {B0_63: 1}, {B0_63: 1}},
			want: []decimal{{B0_63: 97408265472}, {B0_63: 18446744073709546616, B64_127: ^uint64(0)}, {B0_63: 9223372036854775808, B64_127: ^uint64(0)}},
			s1:   0,
			s2:   0,
		},
		{
			name:   "int64_unscaled_SV",
			x:      []decimal{{B0_63: 123456}},
			y:      []decimal{{B0_63: 42}, {B0_63: 789012}},
			want:   []decimal{{B0_63: 5185152}, {B0_63: 97408265472}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:   "int64_unscaled_VS",
			x:      []decimal{{B0_63: 42}, {B0_63: 123456}},
			y:      []decimal{{B0_63: 789012}},
			want:   []decimal{{B0_63: 33138504}, {B0_63: 97408265472}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},

		{
			name:   "inline_unscaled_VV",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: 9223372036854775807}, {B0_63: 1000}, {B0_63: ^uint64(0)}},
			y:      []decimal{{B0_63: ^uint64(0), B64_127: 9223372036854775807}, {B0_63: 2000}, {B0_63: 3}},
			want:   []decimal{{}, {B0_63: 2000000}, {B0_63: 18446744073709551613, B64_127: 2}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:   "inline_unscaled_SV",
			x:      []decimal{{B0_63: 4294967295, B64_127: 1}},
			y:      []decimal{{B0_63: ^uint64(0), B64_127: 9223372036854775807}, {B0_63: 3}},
			want:   []decimal{{B0_63: 18446744069414584321, B64_127: 9223372036854775806}, {B0_63: 12884901885, B64_127: 3}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:   "inline_unscaled_VS",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: 9223372036854775807}, {B0_63: 3}},
			y:      []decimal{{B0_63: 4294967295, B64_127: 1}},
			want:   []decimal{{B0_63: 18446744069414584321, B64_127: 9223372036854775806}, {B0_63: 12884901885, B64_127: 3}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:   "int64_scaled_VV",
			x:      []decimal{{B0_63: 42}, {B0_63: 18446744073709546616, B64_127: ^uint64(0)}, {B0_63: 4999}},
			y:      []decimal{{B0_63: 789012}, {B0_63: 1}, {B0_63: 1}},
			want:   []decimal{{B0_63: 3314}, {B0_63: ^uint64(0), B64_127: ^uint64(0)}, {}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},
		{
			name:   "int64_scaled_SV",
			x:      []decimal{{B0_63: 123456}},
			y:      []decimal{{B0_63: 42}, {B0_63: 789012}},
			want:   []decimal{{B0_63: 519}, {B0_63: 9740827}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},
		{
			name:   "int64_scaled_VS",
			x:      []decimal{{B0_63: 42}, {B0_63: 123456}},
			y:      []decimal{{B0_63: 789012}},
			want:   []decimal{{B0_63: 3314}, {B0_63: 9740827}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},
		{
			name: "inline_scaled_VV",
			x:    []decimal{{B0_63: 1000000}, {B0_63: ^uint64(0), B64_127: 32767}, {B0_63: 18446744073708551616, B64_127: ^uint64(0)}},
			y:    []decimal{{B0_63: 500000}, {B0_63: ^uint64(0), B64_127: 1}, {B0_63: 500000}},
			want: []decimal{{B0_63: 1}, {B0_63: 11606224177980273129, B64_127: 1208925819614}, {B0_63: ^uint64(0), B64_127: ^uint64(0)}},
			s1:   12,
			s2:   12,
		},
		{
			name:   "inline_scaled_SV",
			x:      []decimal{{B0_63: 4294967295, B64_127: 1}},
			y:      []decimal{{B0_63: ^uint64(0), B64_127: 9223372036854775807}, {B0_63: 3}},
			want:   []decimal{{B0_63: 9383858710295619410, B64_127: 17442318858896066530}, {B0_63: 5534023223401356}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},
		{
			name:   "inline_scaled_VS",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: 9223372036854775807}, {B0_63: 3}},
			y:      []decimal{{B0_63: 4294967295, B64_127: 1}},
			want:   []decimal{{B0_63: 9383858710295619410, B64_127: 17442318858896066530}, {B0_63: 5534023223401356}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},
		{
			name: "boundary_0",
			x:    []decimal{{B0_63: 9223372036854775809}},
			y:    []decimal{{B0_63: 1}},
			want: []decimal{{B0_63: 9223372036854775809}},
			s1:   0,
			s2:   0,
		},
		{
			name: "boundary_1",
			x:    []decimal{{B0_63: ^uint64(0)}},
			y:    []decimal{{B0_63: 1}},
			want: []decimal{{B0_63: ^uint64(0)}},
			s1:   0,
			s2:   0,
		},
		{
			name: "boundary_2",
			x:    []decimal{{B0_63: 9223372036854775808}},
			y:    []decimal{{B0_63: 1}},
			want: []decimal{{B0_63: 9223372036854775808}},
			s1:   0,
			s2:   0,
		},
		{
			name: "boundary_3",
			x:    []decimal{{B0_63: 9223372036854775807}},
			y:    []decimal{{B0_63: 1}},
			want: []decimal{{B0_63: 9223372036854775807}},
			s1:   0,
			s2:   0,
		},
		{
			name: "scale_12_2",
			x:    []decimal{{B0_63: 100}},
			y:    []decimal{{B0_63: 3}},
			want: []decimal{{B0_63: 3}},
			s1:   12,
			s2:   2,
		},
		{
			name: "scale_2_12",
			x:    []decimal{{B0_63: 100}},
			y:    []decimal{{B0_63: 3}},
			want: []decimal{{B0_63: 3}},
			s1:   2,
			s2:   12,
		},
		{
			name: "scale_14_2",
			x:    []decimal{{B0_63: 100}},
			y:    []decimal{{B0_63: 3}},
			want: []decimal{{B0_63: 3}},
			s1:   14,
			s2:   2,
		},
		{
			name: "scale_2_14",
			x:    []decimal{{B0_63: 100}},
			y:    []decimal{{B0_63: 3}},
			want: []decimal{{B0_63: 3}},
			s1:   2,
			s2:   14,
		},
		{
			name:      "overflow",
			x:         []decimal{{B0_63: ^uint64(0), B64_127: 32767}},
			y:         []decimal{{B0_63: ^uint64(0), B64_127: 1}},
			want:      []decimal{{B0_63: 1, B64_127: 18446744073709518846}},
			s1:        0,
			s2:        0,
			errorCode: moerr.ErrInvalidInput,
		},
		{
			name:   "all_NULL",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: 9223372036854775807}, {B0_63: ^uint64(0), B64_127: 9223372036854775807}},
			y:      []decimal{{B0_63: ^uint64(0), B64_127: 9223372036854775807}, {B0_63: ^uint64(0), B64_127: 9223372036854775807}},
			want:   []decimal{{B0_63: 1}, {B0_63: 1}},
			s1:     0,
			s2:     0,
			masked: []uint64{0, 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nul := nulls.NewWithSize(len(tc.want))
			for _, i := range tc.masked {
				nul.Add(i)
			}
			got := make([]decimal, len(tc.want))
			err := d128Mul(tc.x, tc.y, got, tc.s1, tc.s2, nul)
			if tc.errorCode != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.errorCode), "unexpected error: %v", err)
				return
			}
			require.NoError(t, err)
			expectedNull := make(map[uint64]bool, len(tc.masked))
			for _, i := range tc.masked {
				expectedNull[i] = true
			}
			require.Equal(t, len(expectedNull), nul.Count())
			for i := range got {
				require.Equal(t, expectedNull[uint64(i)], nul.Contains(uint64(i)), "NULL row %d", i)
				if !expectedNull[uint64(i)] {
					require.Equal(t, tc.want[i], got[i], "coefficient row %d", i)
				}
			}
		})
	}
}

func TestD128Div(t *testing.T) {
	for _, tc := range []struct {
		name              string
		x, y              []types.Decimal128
		want              []types.Decimal128
		scales            [3]int32
		initial, wantNull []uint64
		strict, adapter   bool
		errorCode         uint16
	}{
		{name: "small_singleton_s2_2_r8_permissive_zero", x: []types.Decimal128{{B0_63: 999999999}}, y: []types.Decimal128{{}}, want: []types.Decimal128{{}}, scales: [3]int32{2, 2, 8}, wantNull: []uint64{0}},
		{
			name:   "high_VV_s2_2_r8_strict",
			x:      []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 3}, {B0_63: 18446744073709551609, B64_127: 18446744073709551615}},
			want:   []types.Decimal128{{B0_63: 6148914691203183872, B64_127: 546133333333}, {B0_63: 12297829382506367744, B64_127: 18446743527576218282}, {B0_63: 15811494920351044242, B64_127: 18446743839652408758}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "small_SV_s2_2_r8_strict",
			x:      []types.Decimal128{{B0_63: 999999999}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:   []types.Decimal128{{B0_63: 33333333300000000}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 14285714271428571}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "small_VS_s2_2_r8_strict",
			x:      []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:      []types.Decimal128{{B0_63: 3}},
			want:   []types.Decimal128{{B0_63: 33333333300000000}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 33333333266666667}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "small_VV_s2_5_r8_strict",
			x:      []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:   []types.Decimal128{{B0_63: 14886589226290448384, B64_127: 1}, {B0_63: 14886589226290448384, B64_127: 1}, {B0_63: 14285714257142857143}},
			scales: [3]int32{2, 5, 8},
			strict: true,
		},
		{
			name:   "small_SV_s2_5_r8_strict",
			x:      []types.Decimal128{{B0_63: 999999999}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:   []types.Decimal128{{B0_63: 14886589226290448384, B64_127: 1}, {B0_63: 3560154847419103232, B64_127: 18446744073709551614}, {B0_63: 14285714271428571429}},
			scales: [3]int32{2, 5, 8},
			strict: true,
		},
		{
			name:   "small_VS_s5_2_r11_strict",
			x:      []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:      []types.Decimal128{{B0_63: 3}},
			want:   []types.Decimal128{{B0_63: 33333333300000000}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 33333333266666667}},
			scales: [3]int32{5, 2, 11},
			strict: true,
		},
		{
			name:   "bothwide_VV_s2_2_r8_strict",
			x:      []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 17, B64_127: 5}, {B0_63: 18446744073709551599, B64_127: 18446744073709551610}},
			want:   []types.Decimal128{{B0_63: 6148914691203183872, B64_127: 546133333333}, {B0_63: 18446743746029551616, B64_127: 18446744073709551615}, {B0_63: 18446743746029551616, B64_127: 18446744073709551615}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{name: "adapter_singleton_s2_2_r8_strict", x: []types.Decimal128{{B0_63: 100}}, y: []types.Decimal128{{B0_63: 3}}, want: []types.Decimal128{{B0_63: 3333333333}}, scales: [3]int32{2, 2, 8}, strict: true, adapter: true},
		{
			name:   "small_SV_s3_6_r9_strict",
			x:      []types.Decimal128{{B0_63: 999999999}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:   []types.Decimal128{{B0_63: 1291939673228070912, B64_127: 18}, {B0_63: 17154804400481480704, B64_127: 18446744073709551597}, {B0_63: 13729934198318852974, B64_127: 7}},
			scales: [3]int32{3, 6, 9},
			strict: true,
		},
		{
			name:   "small_VS_s6_3_r12_strict",
			x:      []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:      []types.Decimal128{{B0_63: 3}},
			want:   []types.Decimal128{{B0_63: 333333333000000000}, {B0_63: 18113410740709551616, B64_127: 18446744073709551615}, {B0_63: 333333332666666667}},
			scales: [3]int32{6, 3, 12},
			strict: true,
		},
		{name: "adjustment43_overflow_singleton_s0_37_r6_strict", x: []types.Decimal128{{B0_63: 12345678}}, y: []types.Decimal128{{B0_63: 1000000}}, scales: [3]int32{0, 37, 6}, strict: true, errorCode: moerr.ErrInvalidInput},
		{
			name:   "small_SV_s6_2_r12_strict",
			x:      []types.Decimal128{{B0_63: 999999999}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:   []types.Decimal128{{B0_63: 33333333300000000}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 14285714271428571}},
			scales: [3]int32{6, 2, 12},
			strict: true,
		},
		{
			name:   "small_VS_s6_2_r12_strict",
			x:      []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:      []types.Decimal128{{B0_63: 3}},
			want:   []types.Decimal128{{B0_63: 33333333300000000}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 33333333266666667}},
			scales: [3]int32{6, 2, 12},
			strict: true,
		},
		{
			name:   "small_SV_s2_2_r8_permissive",
			x:      []types.Decimal128{{B0_63: 999999999}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:   []types.Decimal128{{B0_63: 33333333300000000}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 14285714271428571}},
			scales: [3]int32{2, 2, 8},
		},
		{
			name:   "small_VS_s2_2_r8_permissive",
			x:      []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:      []types.Decimal128{{B0_63: 3}},
			want:   []types.Decimal128{{B0_63: 33333333300000000}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 33333333266666667}},
			scales: [3]int32{2, 2, 8},
		},
		{
			name:     "small_VV_s2_2_r8_masked_permissive",
			x:        []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:        []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:     []types.Decimal128{{}, {B0_63: 33333333300000000}, {B0_63: 14285714257142857}},
			scales:   [3]int32{2, 2, 8},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "small_VS_s6_2_r12_permissive",
			x:      []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:      []types.Decimal128{{B0_63: 3}},
			want:   []types.Decimal128{{B0_63: 33333333300000000}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 33333333266666667}},
			scales: [3]int32{6, 2, 12},
		},
		{name: "small_singleton_s2_2_r8_strict_zero", x: []types.Decimal128{{B0_63: 999999999}}, y: []types.Decimal128{{}}, scales: [3]int32{2, 2, 8}, strict: true, errorCode: moerr.ErrDivByZero},
		{
			name:      "small_VS_s2_2_r8_strict_zero",
			x:         []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:         []types.Decimal128{{}},
			scales:    [3]int32{2, 2, 8},
			strict:    true,
			errorCode: moerr.ErrDivByZero,
		},
		{
			name:   "high_VV_s2_2_r8_permissive",
			x:      []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 3}, {B0_63: 18446744073709551609, B64_127: 18446744073709551615}},
			want:   []types.Decimal128{{B0_63: 6148914691203183872, B64_127: 546133333333}, {B0_63: 12297829382506367744, B64_127: 18446743527576218282}, {B0_63: 15811494920351044242, B64_127: 18446743839652408758}},
			scales: [3]int32{2, 2, 8},
		},
		{
			name:     "small_SV_s2_2_r8_masked_permissive",
			x:        []types.Decimal128{{B0_63: 999999999}},
			y:        []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:     []types.Decimal128{{}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 14285714271428571}},
			scales:   [3]int32{2, 2, 8},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:     "small_VS_s2_2_r8_masked_permissive",
			x:        []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:        []types.Decimal128{{B0_63: 3}},
			want:     []types.Decimal128{{}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 33333333266666667}},
			scales:   [3]int32{2, 2, 8},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:     "small_VS_s6_2_r12_masked_permissive",
			x:        []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:        []types.Decimal128{{B0_63: 3}},
			want:     []types.Decimal128{{}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 33333333266666667}},
			scales:   [3]int32{6, 2, 12},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:     "small_VV_s6_2_r12_masked_permissive",
			x:        []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:        []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:     []types.Decimal128{{}, {B0_63: 33333333300000000}, {B0_63: 14285714257142857}},
			scales:   [3]int32{6, 2, 12},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "rightwide_VV_s4_4_r10_permissive",
			x:      []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 17, B64_127: 5}, {B0_63: 18446744073709551599, B64_127: 18446744073709551610}},
			want:   []types.Decimal128{{B0_63: 3333333330000000000}, {}, {}},
			scales: [3]int32{4, 4, 10},
		},
		{
			name:     "rightwide_VV_s4_4_r10_masked_permissive",
			x:        []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:        []types.Decimal128{{B0_63: 3}, {B0_63: 17, B64_127: 5}, {B0_63: 18446744073709551599, B64_127: 18446744073709551610}},
			want:     []types.Decimal128{{}, {}, {}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "rightwide_SV_s4_4_r10_permissive",
			x:      []types.Decimal128{{B0_63: 999999999}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 17, B64_127: 5}, {B0_63: 18446744073709551599, B64_127: 18446744073709551610}},
			want:   []types.Decimal128{{B0_63: 3333333330000000000}, {}, {}},
			scales: [3]int32{4, 4, 10},
		},
		{
			name:     "rightwide_SV_s4_4_r10_masked_permissive",
			x:        []types.Decimal128{{B0_63: 999999999}},
			y:        []types.Decimal128{{B0_63: 3}, {B0_63: 17, B64_127: 5}, {B0_63: 18446744073709551599, B64_127: 18446744073709551610}},
			want:     []types.Decimal128{{}, {}, {}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "rightwide_VS_s4_4_r10_permissive",
			x:      []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:      []types.Decimal128{{B0_63: 17, B64_127: 5}},
			want:   []types.Decimal128{{}, {}, {}},
			scales: [3]int32{4, 4, 10},
		},
		{
			name:     "rightwide_VS_s4_4_r10_masked_permissive",
			x:        []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:        []types.Decimal128{{B0_63: 17, B64_127: 5}},
			want:     []types.Decimal128{{}, {}, {}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:     "bothwide_VV_s4_4_r10_permissive_zero",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:        []types.Decimal128{{B0_63: 17, B64_127: 5}, {}, {B0_63: 7}},
			want:     []types.Decimal128{{B0_63: 32768000000000}, {}, {B0_63: 5270498303917014747, B64_127: 23405714285714}},
			scales:   [3]int32{4, 4, 10},
			wantNull: []uint64{1},
		},
		{
			name:     "bothwide_VV_s4_4_r10_masked_permissive_zero",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:        []types.Decimal128{{B0_63: 17, B64_127: 5}, {}, {B0_63: 7}},
			want:     []types.Decimal128{{}, {}, {B0_63: 5270498303917014747, B64_127: 23405714285714}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0, 1},
		},
		{
			name:     "bothwide_SV_s4_4_r10_permissive_zero",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}},
			y:        []types.Decimal128{{B0_63: 17, B64_127: 5}, {}, {B0_63: 7}},
			want:     []types.Decimal128{{B0_63: 32768000000000}, {}, {B0_63: 5270498305345586176, B64_127: 23405714285714}},
			scales:   [3]int32{4, 4, 10},
			wantNull: []uint64{1},
		},
		{
			name:     "bothwide_SV_s4_4_r10_masked_permissive_zero",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}},
			y:        []types.Decimal128{{B0_63: 17, B64_127: 5}, {}, {B0_63: 7}},
			want:     []types.Decimal128{{}, {}, {B0_63: 5270498305345586176, B64_127: 23405714285714}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0, 1},
		},
		{
			name:     "bothwide_VS_s4_4_r10_permissive_zero",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:        []types.Decimal128{{}},
			want:     []types.Decimal128{{}, {}, {}},
			scales:   [3]int32{4, 4, 10},
			wantNull: []uint64{0, 1, 2},
		},
		{
			name:   "bothwide_VV_s18_2_r18_permissive",
			x:      []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 17, B64_127: 5}, {B0_63: 18446744073709551599, B64_127: 18446744073709551610}},
			want:   []types.Decimal128{{B0_63: 6148914691236517172, B64_127: 546133}, {B0_63: 18446744073709223936, B64_127: 18446744073709551615}, {B0_63: 18446744073709223936, B64_127: 18446744073709551615}},
			scales: [3]int32{18, 2, 18},
		},
		{
			name:   "high_VV_s4_4_r10_permissive",
			x:      []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 3}, {B0_63: 18446744073709551609, B64_127: 18446744073709551615}},
			want:   []types.Decimal128{{B0_63: 6148914687903183872, B64_127: 54613333333333}, {B0_63: 12297829385806367744, B64_127: 18446689460376218282}, {B0_63: 13176245769792536869, B64_127: 18446720667995265901}},
			scales: [3]int32{4, 4, 10},
		},
		{
			name:     "high_VV_s4_4_r10_masked_permissive",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:        []types.Decimal128{{B0_63: 3}, {B0_63: 3}, {B0_63: 18446744073709551609, B64_127: 18446744073709551615}},
			want:     []types.Decimal128{{}, {B0_63: 12297829385806367744, B64_127: 18446689460376218282}, {B0_63: 13176245769792536869, B64_127: 18446720667995265901}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "high_SV_s4_4_r10_permissive",
			x:      []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:   []types.Decimal128{{B0_63: 6148914687903183872, B64_127: 54613333333333}, {B0_63: 12297829385806367744, B64_127: 18446689460376218282}, {B0_63: 5270498305345586176, B64_127: 23405714285714}},
			scales: [3]int32{4, 4, 10},
		},
		{
			name:     "high_SV_s4_4_r10_masked_permissive",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}},
			y:        []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:     []types.Decimal128{{}, {B0_63: 12297829385806367744, B64_127: 18446689460376218282}, {B0_63: 5270498305345586176, B64_127: 23405714285714}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "high_VS_s4_4_r10_permissive",
			x:      []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:      []types.Decimal128{{B0_63: 3}},
			want:   []types.Decimal128{{B0_63: 6148914687903183872, B64_127: 54613333333333}, {B0_63: 12297829385806367744, B64_127: 18446689460376218282}, {B0_63: 6148914684569850539, B64_127: 54613333333333}},
			scales: [3]int32{4, 4, 10},
		},
		{
			name:     "high_VS_s4_4_r10_masked_permissive",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:        []types.Decimal128{{B0_63: 3}},
			want:     []types.Decimal128{{}, {B0_63: 12297829385806367744, B64_127: 18446689460376218282}, {B0_63: 6148914684569850539, B64_127: 54613333333333}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "bothwide_VS_s4_4_r10_permissive",
			x:      []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:      []types.Decimal128{{B0_63: 17, B64_127: 5}},
			want:   []types.Decimal128{{B0_63: 32768000000000}, {B0_63: 18446711305709551616, B64_127: 18446744073709551615}, {B0_63: 32768000000000}},
			scales: [3]int32{4, 4, 10},
		},
		{
			name:     "bothwide_VS_s4_4_r10_masked_permissive",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:        []types.Decimal128{{B0_63: 17, B64_127: 5}},
			want:     []types.Decimal128{{}, {B0_63: 18446711305709551616, B64_127: 18446744073709551615}, {B0_63: 32768000000000}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:      "small_VV_s4_4_r10_strict_zero",
			x:         []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:         []types.Decimal128{{B0_63: 3}, {}, {B0_63: 7}},
			scales:    [3]int32{4, 4, 10},
			strict:    true,
			errorCode: moerr.ErrDivByZero,
		},
		{name: "small_SV_s4_4_r10_strict_zero", x: []types.Decimal128{{B0_63: 999999999}}, y: []types.Decimal128{{B0_63: 3}, {}, {B0_63: 7}}, scales: [3]int32{4, 4, 10}, strict: true, errorCode: moerr.ErrDivByZero},
		{
			name:      "small_VS_s4_4_r10_strict_zero",
			x:         []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:         []types.Decimal128{{}},
			scales:    [3]int32{4, 4, 10},
			strict:    true,
			errorCode: moerr.ErrDivByZero,
		},
		{name: "small_SV_s6_2_r12_strict_zero", x: []types.Decimal128{{B0_63: 999999999}}, y: []types.Decimal128{{B0_63: 3}, {}, {B0_63: 7}}, scales: [3]int32{6, 2, 12}, strict: true, errorCode: moerr.ErrDivByZero},
		{
			name:      "small_VS_s6_2_r12_strict_zero",
			x:         []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:         []types.Decimal128{{}},
			scales:    [3]int32{6, 2, 12},
			strict:    true,
			errorCode: moerr.ErrDivByZero,
		},
		{
			name:   "small_SV_s6_2_r12_permissive",
			x:      []types.Decimal128{{B0_63: 999999999}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:   []types.Decimal128{{B0_63: 33333333300000000}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 14285714271428571}},
			scales: [3]int32{6, 2, 12},
		},
		{
			name:     "small_SV_s6_2_r12_masked_permissive",
			x:        []types.Decimal128{{B0_63: 999999999}},
			y:        []types.Decimal128{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}},
			want:     []types.Decimal128{{}, {B0_63: 18413410740409551616, B64_127: 18446744073709551615}, {B0_63: 14285714271428571}},
			scales:   [3]int32{6, 2, 12},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:     "small_VS_s6_2_r12_permissive_zero",
			x:        []types.Decimal128{{B0_63: 999999999}, {B0_63: 18446744072709551617, B64_127: 18446744073709551615}, {B0_63: 999999998}},
			y:        []types.Decimal128{{}},
			want:     []types.Decimal128{{}, {}, {}},
			scales:   [3]int32{6, 2, 12},
			wantNull: []uint64{0, 1, 2},
		},
		{
			name:     "small_SV_s6_2_r12_permissive_zero",
			x:        []types.Decimal128{{B0_63: 999999999}},
			y:        []types.Decimal128{{B0_63: 3}, {}, {B0_63: 7}},
			want:     []types.Decimal128{{B0_63: 33333333300000000}, {}, {B0_63: 14285714271428571}},
			scales:   [3]int32{6, 2, 12},
			wantNull: []uint64{1},
		},
		{
			name:     "high_VV_s4_4_r10_masked_permissive_zero",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:        []types.Decimal128{{B0_63: 3}, {}, {B0_63: 7}},
			want:     []types.Decimal128{{}, {}, {B0_63: 5270498303917014747, B64_127: 23405714285714}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0, 1},
		},
		{
			name:     "high_VV_s4_4_r10_masked_strict_zero_suppressed",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:        []types.Decimal128{{}, {B0_63: 3}, {B0_63: 7}},
			want:     []types.Decimal128{{}, {B0_63: 12297829385806367744, B64_127: 18446689460376218282}, {B0_63: 5270498303917014747, B64_127: 23405714285714}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
		{
			name:   "bothwide_VV_s4_4_r10_permissive",
			x:      []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 17, B64_127: 5}, {B0_63: 18446744073709551599, B64_127: 18446744073709551610}},
			want:   []types.Decimal128{{B0_63: 6148914687903183872, B64_127: 54613333333333}, {B0_63: 18446711305709551616, B64_127: 18446744073709551615}, {B0_63: 18446711305709551616, B64_127: 18446744073709551615}},
			scales: [3]int32{4, 4, 10},
		},
		{
			name:     "bothwide_VV_s4_4_r10_masked_permissive",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}, {B0_63: 1, B64_127: 18446744073709535232}, {B0_63: 18446744073709551614, B64_127: 16383}},
			y:        []types.Decimal128{{B0_63: 3}, {B0_63: 17, B64_127: 5}, {B0_63: 18446744073709551599, B64_127: 18446744073709551610}},
			want:     []types.Decimal128{{}, {B0_63: 18446711305709551616, B64_127: 18446744073709551615}, {B0_63: 18446711305709551616, B64_127: 18446744073709551615}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:     "high_SV_s4_4_r10_masked_permissive_zero",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}},
			y:        []types.Decimal128{{B0_63: 3}, {}, {B0_63: 7}},
			want:     []types.Decimal128{{}, {}, {B0_63: 5270498305345586176, B64_127: 23405714285714}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0, 1},
		},
		{name: "high_SV_s4_4_r10_strict_zero", x: []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}}, y: []types.Decimal128{{B0_63: 3}, {}, {B0_63: 7}}, scales: [3]int32{4, 4, 10}, strict: true, errorCode: moerr.ErrDivByZero},
		{
			name:   "bothwide_SV_s4_4_r10_permissive",
			x:      []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}},
			y:      []types.Decimal128{{B0_63: 3}, {B0_63: 17, B64_127: 5}, {B0_63: 18446744073709551599, B64_127: 18446744073709551610}},
			want:   []types.Decimal128{{B0_63: 6148914687903183872, B64_127: 54613333333333}, {B0_63: 32768000000000}, {B0_63: 18446711305709551616, B64_127: 18446744073709551615}},
			scales: [3]int32{4, 4, 10},
		},
		{
			name:     "bothwide_SV_s4_4_r10_masked_permissive",
			x:        []types.Decimal128{{B0_63: 18446744073709551615, B64_127: 16383}},
			y:        []types.Decimal128{{B0_63: 3}, {B0_63: 17, B64_127: 5}, {B0_63: 18446744073709551599, B64_127: 18446744073709551610}},
			want:     []types.Decimal128{{}, {B0_63: 32768000000000}, {B0_63: 18446711305709551616, B64_127: 18446744073709551615}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		// K = floor((2^127-1)/10^10). K scales inline; K+1 needs fallback.
		// Dividing K+1 by 10^10 restores K+1; dividing by 1 overflows D128.
		{
			name:   "inline_boundary_VV_plain_nearest_inline",
			x:      []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160323, B64_127: 922337203}},
			y:      []types.Decimal128{{B0_63: 1}, {B0_63: 10000000000}},
			want:   []types.Decimal128{{B0_63: 10000000000}, {B0_63: 12644829501283160323, B64_127: 922337203}},
			scales: [3]int32{4, 4, 10},
			strict: true,
		},
		{
			name:   "inline_boundary_VV_plain_fallback_control",
			x:      []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			y:      []types.Decimal128{{B0_63: 1}, {B0_63: 10000000000}},
			want:   []types.Decimal128{{B0_63: 10000000000}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			scales: [3]int32{4, 4, 10},
			strict: true,
		},
		{
			name:      "inline_boundary_VV_plain_final_overflow",
			x:         []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			y:         []types.Decimal128{{B0_63: 1}, {B0_63: 1}},
			scales:    [3]int32{4, 4, 10},
			strict:    true,
			errorCode: moerr.ErrInvalidInput,
		},
		{
			name:     "inline_boundary_VV_masked_nearest_inline",
			x:        []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160323, B64_127: 922337203}},
			y:        []types.Decimal128{{B0_63: 1}, {B0_63: 10000000000}},
			want:     []types.Decimal128{{}, {B0_63: 12644829501283160323, B64_127: 922337203}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
		{
			name:     "inline_boundary_VV_masked_fallback_control",
			x:        []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			y:        []types.Decimal128{{B0_63: 1}, {B0_63: 10000000000}},
			want:     []types.Decimal128{{}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
		{
			name:      "inline_boundary_VV_masked_final_overflow",
			x:         []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			y:         []types.Decimal128{{B0_63: 1}, {B0_63: 1}},
			scales:    [3]int32{4, 4, 10},
			initial:   []uint64{0},
			strict:    true,
			errorCode: moerr.ErrInvalidInput,
		},
		{
			name:   "inline_boundary_SV_plain_nearest_inline",
			x:      []types.Decimal128{{B0_63: 12644829501283160323, B64_127: 922337203}},
			y:      []types.Decimal128{{B0_63: 10000000000}, {B0_63: 10000000000}},
			want:   []types.Decimal128{{B0_63: 12644829501283160323, B64_127: 922337203}, {B0_63: 12644829501283160323, B64_127: 922337203}},
			scales: [3]int32{4, 4, 10},
			strict: true,
		},
		{
			name:   "inline_boundary_SV_plain_fallback_control",
			x:      []types.Decimal128{{B0_63: 12644829501283160324, B64_127: 922337203}},
			y:      []types.Decimal128{{B0_63: 10000000000}, {B0_63: 10000000000}},
			want:   []types.Decimal128{{B0_63: 12644829501283160324, B64_127: 922337203}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			scales: [3]int32{4, 4, 10},
			strict: true,
		},
		{
			name:      "inline_boundary_SV_plain_final_overflow",
			x:         []types.Decimal128{{B0_63: 12644829501283160324, B64_127: 922337203}},
			y:         []types.Decimal128{{B0_63: 10000000000}, {B0_63: 1}},
			scales:    [3]int32{4, 4, 10},
			strict:    true,
			errorCode: moerr.ErrInvalidInput,
		},
		{
			name:     "inline_boundary_SV_masked_nearest_inline",
			x:        []types.Decimal128{{B0_63: 12644829501283160323, B64_127: 922337203}},
			y:        []types.Decimal128{{B0_63: 10000000000}, {B0_63: 10000000000}},
			want:     []types.Decimal128{{}, {B0_63: 12644829501283160323, B64_127: 922337203}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
		{
			name:     "inline_boundary_SV_masked_fallback_control",
			x:        []types.Decimal128{{B0_63: 12644829501283160324, B64_127: 922337203}},
			y:        []types.Decimal128{{B0_63: 10000000000}, {B0_63: 10000000000}},
			want:     []types.Decimal128{{}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
		{
			name:      "inline_boundary_SV_masked_final_overflow",
			x:         []types.Decimal128{{B0_63: 12644829501283160324, B64_127: 922337203}},
			y:         []types.Decimal128{{B0_63: 10000000000}, {B0_63: 1}},
			scales:    [3]int32{4, 4, 10},
			initial:   []uint64{0},
			strict:    true,
			errorCode: moerr.ErrInvalidInput,
		},
		{
			name:   "inline_boundary_VS_plain_nearest_inline",
			x:      []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160323, B64_127: 922337203}},
			y:      []types.Decimal128{{B0_63: 10000000000}},
			want:   []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160323, B64_127: 922337203}},
			scales: [3]int32{4, 4, 10},
			strict: true,
		},
		{
			name:   "inline_boundary_VS_plain_fallback_control",
			x:      []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			y:      []types.Decimal128{{B0_63: 10000000000}},
			want:   []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			scales: [3]int32{4, 4, 10},
			strict: true,
		},
		{
			name:      "inline_boundary_VS_plain_final_overflow",
			x:         []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			y:         []types.Decimal128{{B0_63: 1}},
			scales:    [3]int32{4, 4, 10},
			strict:    true,
			errorCode: moerr.ErrInvalidInput,
		},
		{
			name:     "inline_boundary_VS_masked_nearest_inline",
			x:        []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160323, B64_127: 922337203}},
			y:        []types.Decimal128{{B0_63: 10000000000}},
			want:     []types.Decimal128{{}, {B0_63: 12644829501283160323, B64_127: 922337203}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
		{
			name:     "inline_boundary_VS_masked_fallback_control",
			x:        []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			y:        []types.Decimal128{{B0_63: 10000000000}},
			want:     []types.Decimal128{{}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			scales:   [3]int32{4, 4, 10},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
		{
			name:      "inline_boundary_VS_masked_final_overflow",
			x:         []types.Decimal128{{B0_63: 1}, {B0_63: 12644829501283160324, B64_127: 922337203}},
			y:         []types.Decimal128{{B0_63: 1}},
			scales:    [3]int32{4, 4, 10},
			initial:   []uint64{0},
			strict:    true,
			errorCode: moerr.ErrInvalidInput,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := max(len(tc.x), len(tc.y))
			mask := nulls.NewWithSize(n)
			for _, i := range tc.initial {
				mask.Add(i)
			}
			out := make([]types.Decimal128, n)
			var err error
			if tc.adapter {
				err = d128DivKernelAtScale(tc.strict, tc.scales[2])(tc.x, tc.y, out, tc.scales[0], tc.scales[1], mask)
			} else {
				err = d128DivAtScale(tc.x, tc.y, out, tc.scales[0], tc.scales[1], tc.scales[2], mask, tc.strict)
			}
			if tc.errorCode != 0 {
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, tc.errorCode), "error: %v", err)
				return
			}
			require.NoError(t, err)
			require.Len(t, tc.want, n)
			require.Equal(t, len(tc.wantNull), mask.Count())
			for i := range out {
				expectedNull := false
				for _, j := range tc.wantNull {
					if uint64(i) == j {
						expectedNull = true
					}
				}
				require.Equal(t, expectedNull, mask.Contains(uint64(i)), "row %d null", i)
				if !expectedNull {
					require.Equal(t, tc.want[i], out[i], "row %d", i)
				}
			}
		})
	}
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
		_ = d128DivAtScale(xs, ys, rs, 2, 2, 8, nul, true)
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
	// Coefficients are independently derived; no decimal producer computes the oracle.
	const allBits = ^uint64(0)
	maxCoefficient := types.Decimal256{B0_63: allBits, B64_127: allBits, B128_191: allBits, B192_255: 0x7fffffffffffffff}
	minCoefficient := types.Decimal256{B192_255: 0x8000000000000000}
	negativeOne := types.Decimal256{B0_63: allBits, B64_127: allBits, B128_191: allBits, B192_255: allBits}
	carryInput := types.Decimal256{B0_63: allBits, B64_127: allBits, B128_191: allBits}
	negativeCarryInput := types.Decimal256{B0_63: 0x1, B192_255: allBits}
	carryBoundary := types.Decimal256{B192_255: 0x1}
	wideLeft := types.Decimal256{B0_63: 0x3, B64_127: 0x1}
	negativeWideLeft := types.Decimal256{B0_63: 0xfffffffffffffffd, B64_127: 0xfffffffffffffffe, B128_191: allBits, B192_255: allBits}
	wideRight := types.Decimal256{B0_63: 0x7, B64_127: 0x1}
	negativeWideRight := types.Decimal256{B0_63: 0xfffffffffffffff9, B64_127: 0xfffffffffffffffe, B128_191: allBits, B192_255: allBits}
	for _, tc := range []struct {
		name                  string
		left, right           []types.Decimal256
		leftScale, rightScale int32
		nullRows              []uint64
		want                  []types.Decimal256
		wantError             uint16
	}{
		{
			"same_vv/no_null",
			[]types.Decimal256{carryInput, negativeCarryInput, maxCoefficient, minCoefficient},
			[]types.Decimal256{{B0_63: 0x1}, negativeOne, {}, {}},
			2, 2, nil,
			[]types.Decimal256{carryBoundary, {B192_255: allBits}, maxCoefficient, minCoefficient}, 0,
		},
		{
			"same_vv/null_middle",
			[]types.Decimal256{negativeCarryInput, maxCoefficient, {}},
			[]types.Decimal256{negativeOne, {B0_63: 0x1}, {}},
			2, 2, []uint64{1},
			[]types.Decimal256{{B192_255: allBits}, {}, {}}, 0,
		},
		{
			"same_sv/no_null",
			[]types.Decimal256{carryInput},
			[]types.Decimal256{{B0_63: 0x1}, negativeOne, negativeCarryInput},
			2, 2, nil,
			[]types.Decimal256{carryBoundary, {B0_63: 0xfffffffffffffffe, B64_127: allBits, B128_191: allBits}, {}}, 0,
		},
		{
			"same_sv/null_middle",
			[]types.Decimal256{carryInput},
			[]types.Decimal256{negativeOne, maxCoefficient, negativeCarryInput},
			2, 2, []uint64{1},
			[]types.Decimal256{{B0_63: 0xfffffffffffffffe, B64_127: allBits, B128_191: allBits}, {}, {}}, 0,
		},
		{
			"same_vs/no_null",
			[]types.Decimal256{{B0_63: 0x1}, negativeOne, negativeCarryInput},
			[]types.Decimal256{carryInput},
			2, 2, nil,
			[]types.Decimal256{carryBoundary, {B0_63: 0xfffffffffffffffe, B64_127: allBits, B128_191: allBits}, {}}, 0,
		},
		{
			"same_vs/null_middle",
			[]types.Decimal256{negativeOne, maxCoefficient, negativeCarryInput},
			[]types.Decimal256{carryInput},
			2, 2, []uint64{1},
			[]types.Decimal256{{B0_63: 0xfffffffffffffffe, B64_127: allBits, B128_191: allBits}, {}, {}}, 0,
		},
		{
			"diff_vv_left_lower/no_null",
			[]types.Decimal256{{B0_63: 0x11}, {B0_63: 0xffffffffffffffe9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}},
			[]types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}},
			1, 3, nil,
			[]types.Decimal256{{B0_63: 0x6a7}, {B0_63: 0xfffffffffffff6fd, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}}, 0,
		},
		{
			"diff_vv_left_lower/null_middle",
			[]types.Decimal256{{B0_63: 0xffffffffffffffe9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0x11}, {}},
			[]types.Decimal256{{B0_63: 0xfffffffffffffff9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, maxCoefficient, {}},
			1, 3, []uint64{1},
			[]types.Decimal256{{B0_63: 0xfffffffffffff6fd, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}, {}}, 0,
		},
		{
			"diff_vv_right_lower/no_null",
			[]types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0xfffffffffffff95c, B64_127: allBits, B128_191: allBits, B192_255: allBits}},
			[]types.Decimal256{{B0_63: 0x11}, {B0_63: 0xffffffffffffffe9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}},
			3, 1, nil,
			[]types.Decimal256{{B0_63: 0x6a7}, {B0_63: 0xfffffffffffff6fd, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0xfffffffffffff95c, B64_127: allBits, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"diff_vv_right_lower/null_middle",
			[]types.Decimal256{{B0_63: 0xfffffffffffffff9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0x3}, {B0_63: 0xfffffffffffff95c, B64_127: allBits, B128_191: allBits, B192_255: allBits}},
			[]types.Decimal256{{B0_63: 0xffffffffffffffe9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0x11}, {}},
			3, 1, []uint64{1},
			[]types.Decimal256{{B0_63: 0xfffffffffffff6fd, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}, {B0_63: 0xfffffffffffff95c, B64_127: allBits, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"diff_sv_right_lower/no_null",
			[]types.Decimal256{{B0_63: 0x3}},
			[]types.Decimal256{{B0_63: 0x11}, {B0_63: 0xffffffffffffffe9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}},
			3, 1, nil,
			[]types.Decimal256{{B0_63: 0x6a7}, {B0_63: 0xfffffffffffff707, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0x3}}, 0,
		},
		{
			"diff_vs_left_lower/no_null",
			[]types.Decimal256{{B0_63: 0x11}, {B0_63: 0xffffffffffffffe9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}},
			[]types.Decimal256{{B0_63: 0x3}},
			1, 3, nil,
			[]types.Decimal256{{B0_63: 0x6a7}, {B0_63: 0xfffffffffffff707, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0x3}}, 0,
		},
		{
			"wide_sv_left_lower/no_null",
			[]types.Decimal256{wideLeft},
			[]types.Decimal256{{B0_63: 0x3}, wideRight, negativeWideRight},
			2, 4, nil,
			[]types.Decimal256{{B0_63: 0x12f, B64_127: 0x64}, {B0_63: 0x133, B64_127: 0x65}, {B0_63: 0x125, B64_127: 0x63}}, 0,
		},
		{
			"wide_sv_left_lower/null_middle",
			[]types.Decimal256{wideLeft},
			[]types.Decimal256{wideRight, {B0_63: 0x3}, negativeWideRight},
			2, 4, []uint64{1},
			[]types.Decimal256{{B0_63: 0x133, B64_127: 0x65}, {}, {B0_63: 0x125, B64_127: 0x63}}, 0,
		},
		{
			"wide_sv_right_lower/no_null",
			[]types.Decimal256{wideLeft},
			[]types.Decimal256{{B0_63: 0x3}, wideRight, negativeWideRight},
			4, 2, nil,
			[]types.Decimal256{{B0_63: 0x12f, B64_127: 0x1}, {B0_63: 0x2bf, B64_127: 0x65}, {B0_63: 0xfffffffffffffd47, B64_127: 0xffffffffffffff9c, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"wide_sv_right_lower/null_middle",
			[]types.Decimal256{wideLeft},
			[]types.Decimal256{wideRight, {B0_63: 0x3}, negativeWideRight},
			4, 2, []uint64{1},
			[]types.Decimal256{{B0_63: 0x2bf, B64_127: 0x65}, {}, {B0_63: 0xfffffffffffffd47, B64_127: 0xffffffffffffff9c, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"wide_vs_left_lower/no_null",
			[]types.Decimal256{{B0_63: 0x1}, wideLeft, negativeWideLeft},
			[]types.Decimal256{wideRight},
			2, 4, nil,
			[]types.Decimal256{{B0_63: 0x6b, B64_127: 0x1}, {B0_63: 0x133, B64_127: 0x65}, {B0_63: 0xfffffffffffffedb, B64_127: 0xffffffffffffff9c, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"wide_vs_left_lower/null_middle",
			[]types.Decimal256{wideLeft, {B0_63: 0x1}, negativeWideLeft},
			[]types.Decimal256{wideRight},
			2, 4, []uint64{1},
			[]types.Decimal256{{B0_63: 0x133, B64_127: 0x65}, {}, {B0_63: 0xfffffffffffffedb, B64_127: 0xffffffffffffff9c, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"wide_vs_right_lower/no_null",
			[]types.Decimal256{{B0_63: 0x1}, wideLeft, negativeWideLeft},
			[]types.Decimal256{wideRight},
			4, 2, nil,
			[]types.Decimal256{{B0_63: 0x2bd, B64_127: 0x64}, {B0_63: 0x2bf, B64_127: 0x65}, {B0_63: 0x2b9, B64_127: 0x63}}, 0,
		},
		{
			"wide_vv_left_lower/no_null",
			[]types.Decimal256{{B0_63: 0x1}, wideLeft, negativeWideLeft},
			[]types.Decimal256{{B0_63: 0x3}, wideRight, negativeWideRight},
			2, 4, nil,
			[]types.Decimal256{{B0_63: 0x67}, {B0_63: 0x133, B64_127: 0x65}, {B0_63: 0xfffffffffffffecd, B64_127: 0xffffffffffffff9a, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"wide_vv_left_lower/null_middle",
			[]types.Decimal256{wideLeft, {B0_63: 0x1}, negativeWideLeft},
			[]types.Decimal256{wideRight, {B0_63: 0x3}, negativeWideRight},
			2, 4, []uint64{1},
			[]types.Decimal256{{B0_63: 0x133, B64_127: 0x65}, {}, {B0_63: 0xfffffffffffffecd, B64_127: 0xffffffffffffff9a, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"wide_vs_right_lower/null_middle",
			[]types.Decimal256{{B0_63: 0x1, B64_127: 0x1}, {B0_63: 0x3}, {B0_63: allBits, B64_127: 0xfffffffffffffffe, B128_191: allBits, B192_255: allBits}},
			[]types.Decimal256{{B0_63: 0x5, B64_127: 0x1}},
			4, 2, []uint64{1},
			[]types.Decimal256{{B0_63: 0x1f5, B64_127: 0x65}, {}, {B0_63: 0x1f3, B64_127: 0x63}}, 0,
		},
		// wide scaling on right, mask otherwise-overflowing coefficient
		{
			"wide_vv_right_lower/null_middle",
			[]types.Decimal256{{B128_191: 0x1}, {B128_191: 0x1}, {B128_191: allBits, B192_255: allBits}},
			[]types.Decimal256{{B0_63: 0x1, B64_127: 0x1}, maxCoefficient, {B0_63: allBits, B64_127: 0xfffffffffffffffe, B128_191: allBits, B192_255: allBits}},
			2, 0, []uint64{1},
			[]types.Decimal256{{B0_63: 0x64, B64_127: 0x64, B128_191: 0x1}, {}, {B0_63: 0xffffffffffffff9c, B64_127: 0xffffffffffffff9b, B128_191: 0xfffffffffffffffe, B192_255: allBits}}, 0,
		},
		{
			"overflow_positive",
			[]types.Decimal256{maxCoefficient},
			[]types.Decimal256{{B0_63: 0x1}},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"overflow_negative",
			[]types.Decimal256{minCoefficient},
			[]types.Decimal256{negativeOne},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"scale_error_scalar",
			[]types.Decimal256{maxCoefficient},
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			0, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		// known vector-upscale defect at39
		{
			"scale_boundary_38_left_vector",
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			[]types.Decimal256{{B0_63: 0x3}},
			0, 38, nil,
			[]types.Decimal256{{B0_63: 0x98a224000000003, B64_127: 0x4b3b4ca85a86c47a}, {B0_63: 0x1314448000000003, B64_127: 0x96769950b50d88f4}}, 0,
		},
		// known vector-upscale defect at39
		{
			"scale_boundary_38_right_vector",
			[]types.Decimal256{{B0_63: 0x3}},
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			38, 0, nil,
			[]types.Decimal256{{B0_63: 0x98a224000000003, B64_127: 0x4b3b4ca85a86c47a}, {B0_63: 0x1314448000000003, B64_127: 0x96769950b50d88f4}}, 0,
		},
		// scalar-upscale nearest control
		{
			"scale_boundary_38_scalar",
			[]types.Decimal256{{B0_63: 0x1}},
			[]types.Decimal256{{B0_63: 0x3}, {B0_63: 0x7}},
			0, 38, nil,
			[]types.Decimal256{{B0_63: 0x98a224000000003, B64_127: 0x4b3b4ca85a86c47a}, {B0_63: 0x98a224000000007, B64_127: 0x4b3b4ca85a86c47a}}, 0,
		},
		// known vector-upscale defect at39
		{
			"scale_boundary_39_left_vector",
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			[]types.Decimal256{{B0_63: 0x3}},
			0, 39, nil,
			[]types.Decimal256{{B0_63: 0x5f65568000000003, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0xbecaad0000000003, B64_127: 0xe0a1fd2712875988, B128_191: 0x5}}, 0,
		},
		// known vector-upscale defect at39
		{
			"scale_boundary_39_right_vector",
			[]types.Decimal256{{B0_63: 0x3}},
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			39, 0, nil,
			[]types.Decimal256{{B0_63: 0x5f65568000000003, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0xbecaad0000000003, B64_127: 0xe0a1fd2712875988, B128_191: 0x5}}, 0,
		},
		// scalar-upscale nearest control
		{
			"scale_boundary_39_scalar",
			[]types.Decimal256{{B0_63: 0x1}},
			[]types.Decimal256{{B0_63: 0x3}, {B0_63: 0x7}},
			0, 39, nil,
			[]types.Decimal256{{B0_63: 0x5f65568000000003, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0x5f65568000000007, B64_127: 0xf050fe938943acc4, B128_191: 0x2}}, 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			length := max(len(tc.left), len(tc.right))
			n := nulls.NewWithSize(length)
			for _, row := range tc.nullRows {
				n.Add(row)
			}
			result := make([]types.Decimal256, length)
			err := d256Add(tc.left, tc.right, result, tc.leftScale, tc.rightScale, n)
			if tc.wantError != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.wantError), "error: %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.nullRows), n.Count())
			for i := range result {
				masked := false
				for _, row := range tc.nullRows {
					masked = masked || row == uint64(i)
				}
				require.Equal(t, masked, n.Contains(uint64(i)), "NULL row %d", i)
				if !masked && tc.wantError == 0 {
					require.Equal(t, tc.want[i], result[i], "row %d", i)
				}
			}
		})
	}
}

func TestD256Sub(t *testing.T) {
	// Coefficients are independently derived; no decimal producer computes the oracle.
	const allBits = ^uint64(0)
	maxCoefficient := types.Decimal256{B0_63: allBits, B64_127: allBits, B128_191: allBits, B192_255: 0x7fffffffffffffff}
	minCoefficient := types.Decimal256{B192_255: 0x8000000000000000}
	negativeOne := types.Decimal256{B0_63: allBits, B64_127: allBits, B128_191: allBits, B192_255: allBits}
	carryInput := types.Decimal256{B0_63: allBits, B64_127: allBits, B128_191: allBits}
	negativeCarryInput := types.Decimal256{B0_63: 0x1, B192_255: allBits}
	carryBoundary := types.Decimal256{B192_255: 0x1}
	wideLeft := types.Decimal256{B0_63: 0x3, B64_127: 0x1}
	negativeWideLeft := types.Decimal256{B0_63: 0xfffffffffffffffd, B64_127: 0xfffffffffffffffe, B128_191: allBits, B192_255: allBits}
	wideRight := types.Decimal256{B0_63: 0x7, B64_127: 0x1}
	negativeWideRight := types.Decimal256{B0_63: 0xfffffffffffffff9, B64_127: 0xfffffffffffffffe, B128_191: allBits, B192_255: allBits}
	for _, tc := range []struct {
		name                  string
		left, right           []types.Decimal256
		leftScale, rightScale int32
		nullRows              []uint64
		want                  []types.Decimal256
		wantError             uint16
	}{
		{
			"same_vv/no_null",
			[]types.Decimal256{carryBoundary, negativeCarryInput, maxCoefficient, minCoefficient},
			[]types.Decimal256{{B0_63: 0x1}, negativeOne, {}, {}},
			2, 2, nil,
			[]types.Decimal256{carryInput, {B0_63: 0x2, B192_255: allBits}, maxCoefficient, minCoefficient}, 0,
		},
		{
			"same_vv/null_middle",
			[]types.Decimal256{carryBoundary, minCoefficient, {}},
			[]types.Decimal256{negativeOne, {B0_63: 0x1}, {}},
			2, 2, []uint64{1},
			[]types.Decimal256{{B0_63: 0x1, B192_255: 0x1}, {}, {}}, 0,
		},
		{
			"same_sv/no_null",
			[]types.Decimal256{carryInput},
			[]types.Decimal256{{B0_63: 0x1}, negativeOne, negativeCarryInput},
			2, 2, nil,
			[]types.Decimal256{{B0_63: 0xfffffffffffffffe, B64_127: allBits, B128_191: allBits}, carryBoundary, {B0_63: 0xfffffffffffffffe, B64_127: allBits, B128_191: allBits, B192_255: 0x1}}, 0,
		},
		{
			"same_sv/null_middle",
			[]types.Decimal256{carryInput},
			[]types.Decimal256{negativeOne, minCoefficient, negativeCarryInput},
			2, 2, []uint64{1},
			[]types.Decimal256{carryBoundary, {}, {B0_63: 0xfffffffffffffffe, B64_127: allBits, B128_191: allBits, B192_255: 0x1}}, 0,
		},
		{
			"same_vs/no_null",
			[]types.Decimal256{{B0_63: 0x1}, negativeOne, negativeCarryInput},
			[]types.Decimal256{carryInput},
			2, 2, nil,
			[]types.Decimal256{{B0_63: 0x2, B192_255: allBits}, {B192_255: allBits}, {B0_63: 0x2, B192_255: 0xfffffffffffffffe}}, 0,
		},
		{
			"same_vs/null_middle",
			[]types.Decimal256{negativeOne, minCoefficient, negativeCarryInput},
			[]types.Decimal256{carryInput},
			2, 2, []uint64{1},
			[]types.Decimal256{{B192_255: allBits}, {}, {B0_63: 0x2, B192_255: 0xfffffffffffffffe}}, 0,
		},
		{
			"diff_vv_left_lower/no_null",
			[]types.Decimal256{{B0_63: 0x11}, {B0_63: 0xffffffffffffffe9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}},
			[]types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}},
			1, 3, nil,
			[]types.Decimal256{{B0_63: 0x6a1}, {B0_63: 0xfffffffffffff70b, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}}, 0,
		},
		{
			"diff_vv_left_lower/null_middle",
			[]types.Decimal256{{B0_63: 0xffffffffffffffe9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0x11}, {}},
			[]types.Decimal256{{B0_63: 0xfffffffffffffff9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, minCoefficient, {}},
			1, 3, []uint64{1},
			[]types.Decimal256{{B0_63: 0xfffffffffffff70b, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}, {}}, 0,
		},
		{
			"diff_vv_right_lower/no_null",
			[]types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0xfffffffffffff95c, B64_127: allBits, B128_191: allBits, B192_255: allBits}},
			[]types.Decimal256{{B0_63: 0x11}, {B0_63: 0xffffffffffffffe9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}},
			3, 1, nil,
			[]types.Decimal256{{B0_63: 0xfffffffffffff95f, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0x8f5}, {B0_63: 0xfffffffffffff95c, B64_127: allBits, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"diff_sv_right_lower/no_null",
			[]types.Decimal256{{B0_63: 0x3}},
			[]types.Decimal256{{B0_63: 0x11}, {B0_63: 0xffffffffffffffe9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}},
			3, 1, nil,
			[]types.Decimal256{{B0_63: 0xfffffffffffff95f, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0x8ff}, {B0_63: 0x3}}, 0,
		},
		{
			"diff_vs_left_lower/no_null",
			[]types.Decimal256{{B0_63: 0x11}, {B0_63: 0xffffffffffffffe9, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}},
			[]types.Decimal256{{B0_63: 0x3}},
			1, 3, nil,
			[]types.Decimal256{{B0_63: 0x6a1}, {B0_63: 0xfffffffffffff701, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0xfffffffffffffffd, B64_127: allBits, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"wide_sv_left_lower/no_null",
			[]types.Decimal256{wideLeft},
			[]types.Decimal256{{B0_63: 0x3}, wideRight, negativeWideRight},
			2, 4, nil,
			[]types.Decimal256{{B0_63: 0x129, B64_127: 0x64}, {B0_63: 0x125, B64_127: 0x63}, {B0_63: 0x133, B64_127: 0x65}}, 0,
		},
		{
			"wide_sv_left_lower/null_middle",
			[]types.Decimal256{wideLeft},
			[]types.Decimal256{wideRight, {B0_63: 0x3}, negativeWideRight},
			2, 4, []uint64{1},
			[]types.Decimal256{{B0_63: 0x125, B64_127: 0x63}, {}, {B0_63: 0x133, B64_127: 0x65}}, 0,
		},
		{
			"wide_sv_right_lower/no_null",
			[]types.Decimal256{wideLeft},
			[]types.Decimal256{{B0_63: 0x3}, wideRight, negativeWideRight},
			4, 2, nil,
			[]types.Decimal256{{B0_63: 0xfffffffffffffed7}, {B0_63: 0xfffffffffffffd47, B64_127: 0xffffffffffffff9c, B128_191: allBits, B192_255: allBits}, {B0_63: 0x2bf, B64_127: 0x65}}, 0,
		},
		{
			"wide_sv_right_lower/null_middle",
			[]types.Decimal256{wideLeft},
			[]types.Decimal256{wideRight, {B0_63: 0x3}, negativeWideRight},
			4, 2, []uint64{1},
			[]types.Decimal256{{B0_63: 0xfffffffffffffd47, B64_127: 0xffffffffffffff9c, B128_191: allBits, B192_255: allBits}, {}, {B0_63: 0x2bf, B64_127: 0x65}}, 0,
		},
		{
			"wide_vs_left_lower/no_null",
			[]types.Decimal256{{B0_63: 0x1}, wideLeft, negativeWideLeft},
			[]types.Decimal256{wideRight},
			2, 4, nil,
			[]types.Decimal256{{B0_63: 0x5d, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {B0_63: 0x125, B64_127: 0x63}, {B0_63: 0xfffffffffffffecd, B64_127: 0xffffffffffffff9a, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"wide_vs_left_lower/null_middle",
			[]types.Decimal256{wideLeft, {B0_63: 0x1}, negativeWideLeft},
			[]types.Decimal256{wideRight},
			2, 4, []uint64{1},
			[]types.Decimal256{{B0_63: 0x125, B64_127: 0x63}, {}, {B0_63: 0xfffffffffffffecd, B64_127: 0xffffffffffffff9a, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"wide_vs_right_lower/no_null",
			[]types.Decimal256{{B0_63: 0x1}, wideLeft, negativeWideLeft},
			[]types.Decimal256{wideRight},
			4, 2, nil,
			[]types.Decimal256{{B0_63: 0xfffffffffffffd45, B64_127: 0xffffffffffffff9b, B128_191: allBits, B192_255: allBits}, {B0_63: 0xfffffffffffffd47, B64_127: 0xffffffffffffff9c, B128_191: allBits, B192_255: allBits}, {B0_63: 0xfffffffffffffd41, B64_127: 0xffffffffffffff9a, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"wide_vv_left_lower/no_null",
			[]types.Decimal256{{B0_63: 0x1}, wideLeft, negativeWideLeft},
			[]types.Decimal256{{B0_63: 0x3}, wideRight, negativeWideRight},
			2, 4, nil,
			[]types.Decimal256{{B0_63: 0x61}, {B0_63: 0x125, B64_127: 0x63}, {B0_63: 0xfffffffffffffedb, B64_127: 0xffffffffffffff9c, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"wide_vv_left_lower/null_middle",
			[]types.Decimal256{wideLeft, {B0_63: 0x1}, negativeWideLeft},
			[]types.Decimal256{wideRight, {B0_63: 0x3}, negativeWideRight},
			2, 4, []uint64{1},
			[]types.Decimal256{{B0_63: 0x125, B64_127: 0x63}, {}, {B0_63: 0xfffffffffffffedb, B64_127: 0xffffffffffffff9c, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"high_limb_vv_scaled_right_int64/no_null",
			[]types.Decimal256{{B0_63: 0x5, B192_255: 0x1}, {B0_63: 0x6, B192_255: 0x1}, {B0_63: 0xfffffffffffffffb, B64_127: allBits, B128_191: allBits, B192_255: 0xfffffffffffffffe}},
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x3}, {B0_63: 0xfffffffffffffffd, B64_127: allBits, B128_191: allBits, B192_255: allBits}},
			6, 2, nil,
			[]types.Decimal256{{B0_63: 0xffffffffffffd8f5, B64_127: allBits, B128_191: allBits}, {B0_63: 0xffffffffffff8ad6, B64_127: allBits, B128_191: allBits}, {B0_63: 0x752b, B192_255: allBits}}, 0,
		},
		{
			"high_limb_sv_scaled_right_int64/null_middle",
			[]types.Decimal256{{B0_63: 0x5, B192_255: 0x1}},
			[]types.Decimal256{{B0_63: 0x3}, {B0_63: 0x1}, {B0_63: 0xfffffffffffffffd, B64_127: allBits, B128_191: allBits, B192_255: allBits}},
			6, 2, []uint64{1},
			[]types.Decimal256{{B0_63: 0xffffffffffff8ad5, B64_127: allBits, B128_191: allBits}, {}, {B0_63: 0x7535, B192_255: 0x1}}, 0,
		},
		{
			"wide_vs_right_lower/null_middle",
			[]types.Decimal256{{B0_63: 0x1, B64_127: 0x1}, {B0_63: 0x3}, {B0_63: allBits, B64_127: 0xfffffffffffffffe, B128_191: allBits, B192_255: allBits}},
			[]types.Decimal256{{B0_63: 0x5, B64_127: 0x1}},
			4, 2, []uint64{1},
			[]types.Decimal256{{B0_63: 0xfffffffffffffe0d, B64_127: 0xffffffffffffff9c, B128_191: allBits, B192_255: allBits}, {}, {B0_63: 0xfffffffffffffe0b, B64_127: 0xffffffffffffff9a, B128_191: allBits, B192_255: allBits}}, 0,
		},
		// wide scaling on right, mask otherwise-overflowing coefficient
		{
			"wide_vv_right_lower/null_middle",
			[]types.Decimal256{{B128_191: 0x1}, {B128_191: 0x1}, {B128_191: allBits, B192_255: allBits}},
			[]types.Decimal256{{B0_63: 0x1, B64_127: 0x1}, maxCoefficient, {B0_63: allBits, B64_127: 0xfffffffffffffffe, B128_191: allBits, B192_255: allBits}},
			2, 0, []uint64{1},
			[]types.Decimal256{{B0_63: 0xffffffffffffff9c, B64_127: 0xffffffffffffff9b}, {}, {B0_63: 0x64, B64_127: 0x64, B128_191: allBits, B192_255: allBits}}, 0,
		},
		{
			"diff_vv_right_lower/null_middle",
			[]types.Decimal256{{B0_63: 0x5}, {B0_63: 0x7}, {B0_63: 0xfffffffffffffff9, B64_127: allBits, B128_191: allBits, B192_255: allBits}},
			[]types.Decimal256{{B0_63: 0x2}, {B0_63: 0x3}, {B0_63: 0xfffffffffffffffd, B64_127: allBits, B128_191: allBits, B192_255: allBits}},
			2, 0, []uint64{1},
			[]types.Decimal256{{B0_63: 0xffffffffffffff3d, B64_127: allBits, B128_191: allBits, B192_255: allBits}, {}, {B0_63: 0x125}}, 0,
		},
		{
			"overflow_positive",
			[]types.Decimal256{maxCoefficient},
			[]types.Decimal256{negativeOne},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"overflow_negative",
			[]types.Decimal256{minCoefficient},
			[]types.Decimal256{{B0_63: 0x1}},
			2, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		{
			"scale_error_vector",
			[]types.Decimal256{{B0_63: 0x1}, maxCoefficient},
			[]types.Decimal256{{B0_63: 0x1}},
			0, 2, nil,
			nil, moerr.ErrInvalidInput,
		},
		// known vector-upscale defect at39
		{
			"scale_boundary_38_left_vector",
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			[]types.Decimal256{{B0_63: 0x3}},
			0, 38, nil,
			[]types.Decimal256{{B0_63: 0x98a223ffffffffd, B64_127: 0x4b3b4ca85a86c47a}, {B0_63: 0x1314447ffffffffd, B64_127: 0x96769950b50d88f4}}, 0,
		},
		// known vector-upscale defect at39
		{
			"scale_boundary_38_right_vector",
			[]types.Decimal256{{B0_63: 0x3}},
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			38, 0, nil,
			[]types.Decimal256{{B0_63: 0xf675ddc000000003, B64_127: 0xb4c4b357a5793b85, B128_191: allBits, B192_255: allBits}, {B0_63: 0xecebbb8000000003, B64_127: 0x698966af4af2770b, B128_191: allBits, B192_255: allBits}}, 0,
		},
		// scalar-upscale nearest control
		{
			"scale_boundary_38_scalar",
			[]types.Decimal256{{B0_63: 0x1}},
			[]types.Decimal256{{B0_63: 0x3}, {B0_63: 0x7}},
			0, 38, nil,
			[]types.Decimal256{{B0_63: 0x98a223ffffffffd, B64_127: 0x4b3b4ca85a86c47a}, {B0_63: 0x98a223ffffffff9, B64_127: 0x4b3b4ca85a86c47a}}, 0,
		},
		// known vector-upscale defect at39
		{
			"scale_boundary_39_left_vector",
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			[]types.Decimal256{{B0_63: 0x3}},
			0, 39, nil,
			[]types.Decimal256{{B0_63: 0x5f65567ffffffffd, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0xbecaacfffffffffd, B64_127: 0xe0a1fd2712875988, B128_191: 0x5}}, 0,
		},
		// known vector-upscale defect at39
		{
			"scale_boundary_39_right_vector",
			[]types.Decimal256{{B0_63: 0x3}},
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			39, 0, nil,
			[]types.Decimal256{{B0_63: 0xa09aa98000000003, B64_127: 0xfaf016c76bc533b, B128_191: 0xfffffffffffffffd, B192_255: allBits}, {B0_63: 0x4135530000000003, B64_127: 0x1f5e02d8ed78a677, B128_191: 0xfffffffffffffffa, B192_255: allBits}}, 0,
		},
		// scalar-upscale nearest control
		{
			"scale_boundary_39_scalar",
			[]types.Decimal256{{B0_63: 0x1}},
			[]types.Decimal256{{B0_63: 0x3}, {B0_63: 0x7}},
			0, 39, nil,
			[]types.Decimal256{{B0_63: 0x5f65567ffffffffd, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0x5f65567ffffffff9, B64_127: 0xf050fe938943acc4, B128_191: 0x2}}, 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			length := max(len(tc.left), len(tc.right))
			n := nulls.NewWithSize(length)
			for _, row := range tc.nullRows {
				n.Add(row)
			}
			result := make([]types.Decimal256, length)
			err := d256Sub(tc.left, tc.right, result, tc.leftScale, tc.rightScale, n)
			if tc.wantError != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.wantError), "error: %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.nullRows), n.Count())
			for i := range result {
				masked := false
				for _, row := range tc.nullRows {
					masked = masked || row == uint64(i)
				}
				require.Equal(t, masked, n.Contains(uint64(i)), "NULL row %d", i)
				if !masked && tc.wantError == 0 {
					require.Equal(t, tc.want[i], result[i], "row %d", i)
				}
			}
		})
	}
}

// d256MulRef supplies the generic multiplication benchmark baseline.
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

// ---- Decimal256 multiplication ----

func TestD256Mul(t *testing.T) {
	type decimal = types.Decimal256
	for _, tc := range []struct {
		name       string
		x, y, want []decimal
		s1, s2     int32
		masked     []uint64
	}{

		{
			name:   "int32_unscaled_VV",
			x:      []decimal{{B0_63: 2147483647}, {B0_63: 2147483647}, {B0_63: 18446744071562067968, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:      []decimal{{B0_63: 1}, {B0_63: 1}, {B0_63: 1}},
			want:   []decimal{{}, {B0_63: 2147483647}, {B0_63: 18446744071562067968, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:   "int32_unscaled_SV",
			x:      []decimal{{B0_63: 123456}},
			y:      []decimal{{B0_63: 42}, {B0_63: 789012}},
			want:   []decimal{{B0_63: 5185152}, {B0_63: 97408265472}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:   "int32_unscaled_VS",
			x:      []decimal{{B0_63: 42}, {B0_63: 789012}},
			y:      []decimal{{B0_63: 123456}},
			want:   []decimal{{B0_63: 5185152}, {B0_63: 97408265472}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},

		{
			name:   "int32_scaled_VV",
			x:      []decimal{{B0_63: 2147483647}, {B0_63: 2147483647}, {B0_63: 18446744071562067968, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:      []decimal{{B0_63: 1}, {B0_63: 1}, {B0_63: 1}},
			want:   []decimal{{}, {B0_63: 214748}, {B0_63: 18446744073709336868, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},
		{
			name:   "int32_scaled_SV",
			x:      []decimal{{B0_63: 123456}},
			y:      []decimal{{B0_63: 42}, {B0_63: 789012}},
			want:   []decimal{{B0_63: 519}, {B0_63: 9740827}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},
		{
			name:   "int32_scaled_VS",
			x:      []decimal{{B0_63: 42}, {B0_63: 789012}},
			y:      []decimal{{B0_63: 123456}},
			want:   []decimal{{B0_63: 519}, {B0_63: 9740827}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},

		{
			name:   "int64_unscaled_VV",
			x:      []decimal{{}, {B0_63: 1000}, {B0_63: 9223372036854775807}},
			y:      []decimal{{B0_63: 1}, {B0_63: 2000}, {B0_63: 9223372036854775807}},
			want:   []decimal{{}, {B0_63: 2000000}, {B0_63: 1, B64_127: 4611686018427387903}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:   "int64_unscaled_SV",
			x:      []decimal{{B0_63: 4611686018427387904}},
			y:      []decimal{{B0_63: 42}, {B0_63: 8}},
			want:   []decimal{{B0_63: 9223372036854775808, B64_127: 10}, {B0_63: 0, B64_127: 2}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:   "int64_unscaled_VS",
			x:      []decimal{{B0_63: 42}, {B0_63: 8}},
			y:      []decimal{{B0_63: 4611686018427387904}},
			want:   []decimal{{B0_63: 9223372036854775808, B64_127: 10}, {B0_63: 0, B64_127: 2}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name: "int64_scaled_VV",
			x:    []decimal{{B0_63: 1000}, {B0_63: 9223372036854775807}},
			y:    []decimal{{B0_63: 2000}, {B0_63: 9223372036854775807}},
			want: []decimal{{B0_63: 200}, {B0_63: 14578461841452658642, B64_127: 461168601842738}},
			s1:   8,
			s2:   8,
		},
		{
			name:   "int64_scaled_SV",
			x:      []decimal{{B0_63: 4611686018427387904}},
			y:      []decimal{{B0_63: 42}, {B0_63: 8}},
			want:   []decimal{{B0_63: 19369081277395029}, {B0_63: 3689348814741910}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},
		{
			name:   "int64_scaled_VS",
			x:      []decimal{{B0_63: 42}, {B0_63: 8}},
			y:      []decimal{{B0_63: 4611686018427387904}},
			want:   []decimal{{B0_63: 19369081277395029}, {B0_63: 3689348814741910}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},

		{
			name:   "generic_unscaled_VV",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 9223372036854775807}, {B0_63: 1000}, {B0_63: 4294967295, B64_127: 1}, {B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			y:      []decimal{{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 9223372036854775807}, {B0_63: 2000}, {B0_63: 3}, {B0_63: 3}},
			want:   []decimal{{}, {B0_63: 2000000}, {B0_63: 12884901885, B64_127: 3}, {B0_63: 18446744073709551613, B64_127: 5, B128_191: 3, B192_255: 3}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:   "generic_unscaled_SV",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			y:      []decimal{{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 9223372036854775807}, {B0_63: 3}},
			want:   []decimal{{B0_63: 1, B64_127: 18446744073709551614, B128_191: 18446744073709551614, B192_255: 9223372036854775806}, {B0_63: 18446744073709551613, B64_127: 5, B128_191: 3, B192_255: 3}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:   "generic_unscaled_VS",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 9223372036854775807}, {B0_63: 3}},
			y:      []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			want:   []decimal{{B0_63: 1, B64_127: 18446744073709551614, B128_191: 18446744073709551614, B192_255: 9223372036854775806}, {B0_63: 18446744073709551613, B64_127: 5, B128_191: 3, B192_255: 3}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},

		{
			name:   "generic_scaled_VV",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 9223372036854775807}, {B0_63: 1000}, {B0_63: 4294967295, B64_127: 1}, {B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			y:      []decimal{{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 9223372036854775807}, {B0_63: 2000}, {B0_63: 3}, {B0_63: 3}},
			want:   []decimal{{}, {B0_63: 200}, {B0_63: 5534023223401356}, {B0_63: 17011587384774948500, B64_127: 8948515550156503488, B128_191: 5534023222112865}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},
		{
			name:   "generic_scaled_SV",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			y:      []decimal{{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 9223372036854775807}, {B0_63: 3}},
			want:   []decimal{{B0_63: 3892262999552715391, B64_127: 7022675468861226300, B128_191: 3805563302406280498, B192_255: 6994083015546976495}, {B0_63: 17011587384774948500, B64_127: 8948515550156503488, B128_191: 5534023222112865}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},
		{
			name:   "generic_scaled_VS",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 9223372036854775807}, {B0_63: 3}},
			y:      []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			want:   []decimal{{B0_63: 3892262999552715391, B64_127: 7022675468861226300, B128_191: 3805563302406280498, B192_255: 6994083015546976495}, {B0_63: 17011587384774948500, B64_127: 8948515550156503488, B128_191: 5534023222112865}},
			s1:     8,
			s2:     8,
			masked: []uint64{0},
		},
		{
			name: "scale_14_2",
			x:    []decimal{{B0_63: 100}},
			y:    []decimal{{B0_63: 3}},
			want: []decimal{{B0_63: 3}},
			s1:   14,
			s2:   2,
		},
		{
			name: "scale_2_14",
			x:    []decimal{{B0_63: 100}},
			y:    []decimal{{B0_63: 3}},
			want: []decimal{{B0_63: 3}},
			s1:   2,
			s2:   14,
		},
		{
			name:   "all_NULL",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 9223372036854775807}, {B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 9223372036854775807}},
			y:      []decimal{{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 9223372036854775807}, {B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 9223372036854775807}},
			want:   []decimal{{B0_63: 1}, {B0_63: 1}},
			s1:     0,
			s2:     0,
			masked: []uint64{0, 1},
		},
		{
			name: "int32_reduction20",
			x:    []decimal{{B0_63: 2147483647}, {B0_63: 18446744071562067968, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:    []decimal{{B0_63: 1}, {B0_63: 1}},
			want: []decimal{{}, {}},
			s1:   20,
			s2:   20,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nul := nulls.NewWithSize(len(tc.want))
			for _, i := range tc.masked {
				nul.Add(i)
			}
			got := make([]decimal, len(tc.want))
			err := d256Mul(tc.x, tc.y, got, tc.s1, tc.s2, nul)
			require.NoError(t, err)
			expectedNull := make(map[uint64]bool, len(tc.masked))
			for _, i := range tc.masked {
				expectedNull[i] = true
			}
			require.Equal(t, len(expectedNull), nul.Count())
			for i := range got {
				require.Equal(t, expectedNull[uint64(i)], nul.Contains(uint64(i)), "NULL row %d", i)
				if !expectedNull[uint64(i)] {
					require.Equal(t, tc.want[i], got[i], "coefficient row %d", i)
				}
			}
		})
	}
}

func TestD256Mul_Int32ScaleDown(t *testing.T) {
	x := []types.Decimal256{{B0_63: 49999999}, {B0_63: 50000000}, {B0_63: 18446744073659551616, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}}
	y := []types.Decimal256{{B0_63: 1}}
	want := []types.Decimal256{{}, {B0_63: 1}, {B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}}
	got := make([]types.Decimal256, len(want))
	nul := nulls.NewWithSize(len(want))
	require.NoError(t, d256Mul(x, y, got, 10, 10, nul))
	require.Equal(t, 0, nul.Count())
	require.Equal(t, want, got)
}

func TestD256Div(t *testing.T) {
	for _, tc := range []struct {
		name              string
		x, y              []types.Decimal256
		want              []types.Decimal256
		scales            [3]int32
		initial, wantNull []uint64
		strict, adapter   bool
	}{
		{
			name:   "via128_scale6_SV_plain",
			x:      []types.Decimal256{{B0_63: 999999999}},
			y:      []types.Decimal256{{B0_63: 3}, {B0_63: 7}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			want:   []types.Decimal256{{B0_63: 333333333000000}, {B0_63: 142857142714286}, {B0_63: 18446410740376551616, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			scales: [3]int32{4, 4, 6},
		},
		{
			name:     "via128_scale6_SV_null",
			x:        []types.Decimal256{{B0_63: 999999999}},
			y:        []types.Decimal256{{B0_63: 3}, {B0_63: 7}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			want:     []types.Decimal256{{}, {B0_63: 142857142714286}, {B0_63: 18446410740376551616, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			scales:   [3]int32{4, 4, 6},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "via128_scale6_VS_plain",
			x:      []types.Decimal256{{B0_63: 1}, {B0_63: 999999999}, {B0_63: 999999998}},
			y:      []types.Decimal256{{B0_63: 3}},
			want:   []types.Decimal256{{B0_63: 333333}, {B0_63: 333333333000000}, {B0_63: 333333332666667}},
			scales: [3]int32{4, 4, 6},
		},
		{
			name:     "via128_scale6_VS_null",
			x:        []types.Decimal256{{B0_63: 1}, {B0_63: 999999999}, {B0_63: 999999998}},
			y:        []types.Decimal256{{B0_63: 3}},
			want:     []types.Decimal256{{}, {B0_63: 333333333000000}, {B0_63: 333333332666667}},
			scales:   [3]int32{4, 4, 6},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:     "via128_scale6_zero_VV_plain",
			x:        []types.Decimal256{{B0_63: 1}, {B0_63: 999999999}, {B0_63: 999999998}},
			y:        []types.Decimal256{{B0_63: 3}, {}, {B0_63: 7}},
			want:     []types.Decimal256{{B0_63: 333333}, {}, {B0_63: 142857142571429}},
			scales:   [3]int32{4, 4, 6},
			wantNull: []uint64{1},
		},
		{
			name:     "via128_scale6_zero_SV_plain",
			x:        []types.Decimal256{{B0_63: 999999999}},
			y:        []types.Decimal256{{B0_63: 3}, {}, {B0_63: 7}},
			want:     []types.Decimal256{{B0_63: 333333333000000}, {}, {B0_63: 142857142714286}},
			scales:   [3]int32{4, 4, 6},
			wantNull: []uint64{1},
		},
		{
			name:     "via128_scale6_zero_SV_null",
			x:        []types.Decimal256{{B0_63: 999999999}},
			y:        []types.Decimal256{{B0_63: 3}, {}, {B0_63: 7}},
			want:     []types.Decimal256{{}, {}, {B0_63: 142857142714286}},
			scales:   [3]int32{4, 4, 6},
			initial:  []uint64{0},
			wantNull: []uint64{0, 1},
		},
		{name: "via128_scale6_zero_VS_plain", x: []types.Decimal256{{B0_63: 1}, {B0_63: 999999999}, {B0_63: 999999998}}, y: []types.Decimal256{{}}, want: []types.Decimal256{{}, {}, {}}, scales: [3]int32{4, 4, 6}, wantNull: []uint64{0, 1, 2}},
		{
			name:   "generic_bothwide_VV_plain",
			x:      []types.Decimal256{{B0_63: 17, B64_127: 1, B128_191: 100}, {B0_63: 18446744073709551599, B64_127: 18446744073709551614, B128_191: 18446744073709551515, B192_255: 18446744073709551615}, {B0_63: 18, B64_127: 1, B128_191: 100}},
			y:      []types.Decimal256{{B0_63: 31, B128_191: 1}, {B0_63: 18446744073709551585, B64_127: 18446744073709551615, B128_191: 18446744073709551614, B192_255: 18446744073709551615}, {B0_63: 32, B128_191: 1}},
			want:   []types.Decimal256{{B0_63: 10000000000}, {B0_63: 10000000000}, {B0_63: 10000000000}},
			scales: [3]int32{2, 2, 8},
		},
		{
			name:   "generic_bothwide_SV_plain",
			x:      []types.Decimal256{{B0_63: 17, B64_127: 1, B128_191: 100}},
			y:      []types.Decimal256{{B0_63: 31, B128_191: 1}, {B0_63: 18446744073709551585, B64_127: 18446744073709551615, B128_191: 18446744073709551614, B192_255: 18446744073709551615}, {B0_63: 32, B128_191: 1}},
			want:   []types.Decimal256{{B0_63: 10000000000}, {B0_63: 18446744063709551616, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}, {B0_63: 10000000000}},
			scales: [3]int32{2, 2, 8},
		},
		{
			name:   "generic_bothwide_VS_plain",
			x:      []types.Decimal256{{B0_63: 17, B64_127: 1, B128_191: 100}, {B0_63: 18446744073709551599, B64_127: 18446744073709551614, B128_191: 18446744073709551515, B192_255: 18446744073709551615}, {B0_63: 18, B64_127: 1, B128_191: 100}},
			y:      []types.Decimal256{{B0_63: 31, B128_191: 1}},
			want:   []types.Decimal256{{B0_63: 10000000000}, {B0_63: 18446744063709551616, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}, {B0_63: 10000000000}},
			scales: [3]int32{2, 2, 8},
		},
		{
			name:     "generic_bothwide_VV_null",
			x:        []types.Decimal256{{B0_63: 17, B64_127: 1, B128_191: 100}, {B0_63: 18446744073709551599, B64_127: 18446744073709551614, B128_191: 18446744073709551515, B192_255: 18446744073709551615}, {B0_63: 18, B64_127: 1, B128_191: 100}},
			y:        []types.Decimal256{{B0_63: 31, B128_191: 1}, {B0_63: 18446744073709551585, B64_127: 18446744073709551615, B128_191: 18446744073709551614, B192_255: 18446744073709551615}, {B0_63: 32, B128_191: 1}},
			want:     []types.Decimal256{{}, {B0_63: 10000000000}, {B0_63: 10000000000}},
			scales:   [3]int32{2, 2, 8},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "narrow_highlimb_VV",
			x:      []types.Decimal256{{B0_63: 18446744073709551615, B64_127: 999}, {B0_63: 1, B64_127: 18446744073709550616, B128_191: 18446744073709551615, B192_255: 18446744073709551615}, {B0_63: 1}},
			y:      []types.Decimal256{{B0_63: 3}, {B0_63: 3}, {B0_63: 7}},
			want:   []types.Decimal256{{B0_63: 6148914691203183872, B64_127: 33333333333}, {B0_63: 12297829382506367744, B64_127: 18446744040376218282, B128_191: 18446744073709551615, B192_255: 18446744073709551615}, {B0_63: 14285714}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "narrow_SV",
			x:      []types.Decimal256{{B0_63: 999999999}},
			y:      []types.Decimal256{{B0_63: 3}, {B0_63: 18446744073709551609, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}, {B0_63: 1}},
			want:   []types.Decimal256{{B0_63: 33333333300000000}, {B0_63: 18432458359438123045, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}, {B0_63: 99999999900000000}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "narrow_VS",
			x:      []types.Decimal256{{B0_63: 999999999}, {B0_63: 999}, {B0_63: 1}},
			y:      []types.Decimal256{{B0_63: 3}},
			want:   []types.Decimal256{{B0_63: 33333333300000000}, {B0_63: 33300000000}, {B0_63: 33333333}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "unequal_SV",
			x:      []types.Decimal256{{B0_63: 999}},
			y:      []types.Decimal256{{B0_63: 3}, {B0_63: 18446744073709551609, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}, {B0_63: 1}},
			want:   []types.Decimal256{{B0_63: 33300000000000}, {B0_63: 18446729802280980187, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}, {B0_63: 99900000000000}},
			scales: [3]int32{2, 5, 8},
			strict: true,
		},
		{
			name:   "unequal_VS",
			x:      []types.Decimal256{{B0_63: 999}, {B0_63: 18446744073709550618, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}, {B0_63: 1}},
			y:      []types.Decimal256{{B0_63: 3}},
			want:   []types.Decimal256{{B0_63: 33300000000}, {B0_63: 18446744040442884949, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}, {B0_63: 33333333}},
			scales: [3]int32{5, 2, 11},
			strict: true,
		},
		{
			name:    "Kernel",
			x:       []types.Decimal256{{B0_63: 18446744073709551615, B64_127: 999}, {B0_63: 18446744073709551614, B64_127: 999}},
			y:       []types.Decimal256{{B0_63: 3}, {B0_63: 7}},
			want:    []types.Decimal256{{B0_63: 6148914691203183872, B64_127: 33333333333}, {B0_63: 13176245766906822583, B64_127: 14285714285}},
			scales:  [3]int32{2, 2, 8},
			strict:  true,
			adapter: true,
		},
		{
			name:     "narrow_masked_VV",
			x:        []types.Decimal256{{B0_63: 1}, {B0_63: 999999999}, {B0_63: 999999998}},
			y:        []types.Decimal256{{B0_63: 1}, {B0_63: 3}, {B0_63: 7}},
			want:     []types.Decimal256{{}, {B0_63: 33333333300000000}, {B0_63: 14285714257142857}},
			scales:   [3]int32{2, 2, 8},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:     "narrow_masked_SV",
			x:        []types.Decimal256{{B0_63: 999999999}},
			y:        []types.Decimal256{{B0_63: 1}, {B0_63: 3}, {B0_63: 7}},
			want:     []types.Decimal256{{}, {B0_63: 33333333300000000}, {B0_63: 14285714271428571}},
			scales:   [3]int32{2, 2, 8},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:     "narrow_masked_VS",
			x:        []types.Decimal256{{B0_63: 1}, {B0_63: 999999999}, {B0_63: 999999998}},
			y:        []types.Decimal256{{B0_63: 3}},
			want:     []types.Decimal256{{}, {B0_63: 33333333300000000}, {B0_63: 33333333266666667}},
			scales:   [3]int32{2, 2, 8},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "noninline24_VV_plain",
			x:      []types.Decimal256{{B0_63: 1}, {B0_63: 999}, {B0_63: 18446744073709550618, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			y:      []types.Decimal256{{B0_63: 1}, {B0_63: 3}, {B0_63: 7}},
			want:   []types.Decimal256{{B0_63: 2003764205206896640, B64_127: 54210}, {B0_63: 3170693680352722944, B64_127: 18051966}, {B0_63: 6833130769325340379, B64_127: 18446744073701822803, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			scales: [3]int32{0, 18, 6},
		},
		{
			name:     "noninline24_VV_null",
			x:        []types.Decimal256{{B0_63: 1}, {B0_63: 999}, {B0_63: 18446744073709550618, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			y:        []types.Decimal256{{B0_63: 1}, {B0_63: 3}, {B0_63: 7}},
			want:     []types.Decimal256{{}, {B0_63: 3170693680352722944, B64_127: 18051966}, {B0_63: 6833130769325340379, B64_127: 18446744073701822803, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			scales:   [3]int32{0, 18, 6},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "noninline24_SV_plain",
			x:      []types.Decimal256{{B0_63: 999}},
			y:      []types.Decimal256{{B0_63: 1}, {B0_63: 3}, {B0_63: 7}},
			want:   []types.Decimal256{{B0_63: 9512081041058168832, B64_127: 54155898}, {B0_63: 3170693680352722944, B64_127: 18051966}, {B0_63: 17170363640473639790, B64_127: 7736556}},
			scales: [3]int32{0, 18, 6},
		},
		{
			name:     "noninline24_SV_null",
			x:        []types.Decimal256{{B0_63: 999}},
			y:        []types.Decimal256{{B0_63: 1}, {B0_63: 3}, {B0_63: 7}},
			want:     []types.Decimal256{{}, {B0_63: 3170693680352722944, B64_127: 18051966}, {B0_63: 17170363640473639790, B64_127: 7736556}},
			scales:   [3]int32{0, 18, 6},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{
			name:   "noninline24_VS_plain",
			x:      []types.Decimal256{{B0_63: 1}, {B0_63: 999}, {B0_63: 18446744073709550618, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			y:      []types.Decimal256{{B0_63: 3}},
			want:   []types.Decimal256{{B0_63: 667921401735632213, B64_127: 18070}, {B0_63: 3170693680352722944, B64_127: 18051966}, {B0_63: 15943971795092460885, B64_127: 18446744073691517719, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			scales: [3]int32{0, 18, 6},
		},
		{
			name:     "noninline24_VS_null",
			x:        []types.Decimal256{{B0_63: 1}, {B0_63: 999}, {B0_63: 18446744073709550618, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			y:        []types.Decimal256{{B0_63: 3}},
			want:     []types.Decimal256{{}, {B0_63: 3170693680352722944, B64_127: 18051966}, {B0_63: 15943971795092460885, B64_127: 18446744073691517719, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			scales:   [3]int32{0, 18, 6},
			initial:  []uint64{0},
			wantNull: []uint64{0},
		},
		{name: "generic_zero_VS", x: []types.Decimal256{{B0_63: 1, B128_191: 1}, {B0_63: 3, B128_191: 2}}, y: []types.Decimal256{{}}, want: []types.Decimal256{{}, {}}, scales: [3]int32{4, 4, 10}, wantNull: []uint64{0, 1}},
		{
			name:   "generic_wide_numerator",
			x:      []types.Decimal256{{B0_63: 12379813812177893520, B64_127: 4660, B128_191: 1}, {B0_63: 10986060915027139770, B64_127: 22136, B128_191: 2}, {B0_63: 1229782938247303441, B64_127: 8738, B128_191: 3}, {B0_63: 18446744073709551615, B64_127: 13107}},
			y:      []types.Decimal256{{B0_63: 17}, {B0_63: 31}, {B0_63: 97}, {B0_63: 1000003}},
			want:   []types.Decimal256{{B0_63: 16440005941155234033, B64_127: 17361641508554113938, B128_191: 5882352}, {B0_63: 5686150883390127021, B64_127: 16661575363791193574, B128_191: 6451612}, {B0_63: 316954365527586798, B64_127: 9318458355521388617, B128_191: 3092783}, {B0_63: 1247217518659094127, B64_127: 1310796}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "admission_left_VV",
			x:      []types.Decimal256{{B0_63: 1}, {B0_63: 18446744073709551599, B64_127: 18446744073709551615, B128_191: 18446744073709551614, B192_255: 18446744073709551615}},
			y:      []types.Decimal256{{B0_63: 3}, {B0_63: 31}},
			want:   []types.Decimal256{{B0_63: 33333333}, {B0_63: 14281350250559007703, B64_127: 10115956427518141208, B128_191: 18446744073706325809, B192_255: 18446744073709551615}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "admission_left_SV",
			x:      []types.Decimal256{{B0_63: 17, B128_191: 1}},
			y:      []types.Decimal256{{B0_63: 3}, {B0_63: 31}},
			want:   []types.Decimal256{{B0_63: 6148914691803183872, B64_127: 6148914691236517205, B128_191: 33333333}, {B0_63: 4165393823150543913, B64_127: 8330787646191410407, B128_191: 3225806}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "admission_left_VS",
			x:      []types.Decimal256{{B0_63: 1}, {B0_63: 18446744073709551599, B64_127: 18446744073709551615, B128_191: 18446744073709551614, B192_255: 18446744073709551615}},
			y:      []types.Decimal256{{B0_63: 31}},
			want:   []types.Decimal256{{B0_63: 3225806}, {B0_63: 14281350250559007703, B64_127: 10115956427518141208, B128_191: 18446744073706325809, B192_255: 18446744073709551615}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "admission_right_VV",
			x:      []types.Decimal256{{B0_63: 1}, {B0_63: 18446744073709551609, B64_127: 13835058055282163711, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			y:      []types.Decimal256{{B0_63: 3}, {B0_63: 31, B64_127: 9223372036854775808}},
			want:   []types.Decimal256{{B0_63: 33333333}, {B0_63: 18446744073659551616, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "admission_right_SV",
			x:      []types.Decimal256{{B0_63: 7, B64_127: 4611686018427387904}},
			y:      []types.Decimal256{{B0_63: 3}, {B0_63: 31, B64_127: 9223372036854775808}},
			want:   []types.Decimal256{{B0_63: 6148914691469850539, B64_127: 6148914691236517205, B128_191: 8333333}, {B0_63: 50000000}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "admission_right_VS",
			x:      []types.Decimal256{{B0_63: 1}, {B0_63: 18446744073709551609, B64_127: 13835058055282163711, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			y:      []types.Decimal256{{B0_63: 31, B64_127: 9223372036854775808}},
			want:   []types.Decimal256{{}, {B0_63: 18446744073659551616, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			scales: [3]int32{2, 2, 8},
			strict: true,
		},
		{
			name:   "wide_quotient_VV_plain",
			x:      []types.Decimal256{{B0_63: 1}, {B0_63: 13369799803404288000, B64_127: 18446744019499442991, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			y:      []types.Decimal256{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			want:   []types.Decimal256{{B0_63: 333333333333333333}, {B0_63: 9205451463337530709, B64_127: 12640439345423072014, B128_191: 979578625}},
			scales: [3]int32{30, 18, 30},
			strict: true,
		},
		{
			name:     "wide_quotient_VV_null",
			x:        []types.Decimal256{{B0_63: 1}, {B0_63: 13369799803404288000, B64_127: 18446744019499442991, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			y:        []types.Decimal256{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			want:     []types.Decimal256{{}, {B0_63: 9205451463337530709, B64_127: 12640439345423072014, B128_191: 979578625}},
			scales:   [3]int32{30, 18, 30},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
		{
			name:   "wide_quotient_SV_plain",
			x:      []types.Decimal256{{B0_63: 5076944270305263616, B64_127: 54210108624}},
			y:      []types.Decimal256{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			want:   []types.Decimal256{{B0_63: 9205451463337530709, B64_127: 12640439345423072014, B128_191: 979578625}, {B0_63: 9241292610372020907, B64_127: 5806304728286479601, B128_191: 18446744072729972990, B192_255: 18446744073709551615}},
			scales: [3]int32{30, 18, 30},
			strict: true,
		},
		{
			name:     "wide_quotient_SV_null",
			x:        []types.Decimal256{{B0_63: 5076944270305263616, B64_127: 54210108624}},
			y:        []types.Decimal256{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			want:     []types.Decimal256{{}, {B0_63: 9241292610372020907, B64_127: 5806304728286479601, B128_191: 18446744072729972990, B192_255: 18446744073709551615}},
			scales:   [3]int32{30, 18, 30},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
		{
			name:   "wide_quotient_VS_plain",
			x:      []types.Decimal256{{B0_63: 1}, {B0_63: 13369799803404288000, B64_127: 18446744019499442991, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			y:      []types.Decimal256{{B0_63: 3}},
			want:   []types.Decimal256{{B0_63: 333333333333333333}, {B0_63: 9241292610372020907, B64_127: 5806304728286479601, B128_191: 18446744072729972990, B192_255: 18446744073709551615}},
			scales: [3]int32{30, 18, 30},
			strict: true,
		},
		{
			name:     "wide_quotient_VS_null",
			x:        []types.Decimal256{{B0_63: 1}, {B0_63: 13369799803404288000, B64_127: 18446744019499442991, B128_191: 18446744073709551615, B192_255: 18446744073709551615}},
			y:        []types.Decimal256{{B0_63: 3}},
			want:     []types.Decimal256{{}, {B0_63: 9241292610372020907, B64_127: 5806304728286479601, B128_191: 18446744072729972990, B192_255: 18446744073709551615}},
			scales:   [3]int32{30, 18, 30},
			initial:  []uint64{0},
			wantNull: []uint64{0},
			strict:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := max(len(tc.x), len(tc.y))
			mask := nulls.NewWithSize(n)
			for _, i := range tc.initial {
				mask.Add(i)
			}
			out := make([]types.Decimal256, n)
			var err error
			if tc.adapter {
				err = d256DivKernelAtScale(tc.strict, tc.scales[2])(tc.x, tc.y, out, tc.scales[0], tc.scales[1], mask)
			} else {
				err = d256DivAtScale(tc.x, tc.y, out, tc.scales[0], tc.scales[1], tc.scales[2], mask, tc.strict)
			}
			require.NoError(t, err)
			require.Len(t, tc.want, n)
			require.Equal(t, len(tc.wantNull), mask.Count())
			for i := range out {
				expectedNull := false
				for _, j := range tc.wantNull {
					if uint64(i) == j {
						expectedNull = true
					}
				}
				require.Equal(t, expectedNull, mask.Contains(uint64(i)), "row %d null", i)
				if !expectedNull {
					require.Equal(t, tc.want[i], out[i], "row %d", i)
				}
			}
		})
	}
}

func TestD256DivViaD128PreservesWideQuotient(t *testing.T) {
	x, err := types.ParseDecimal256("1", 65, 30)
	require.NoError(t, err)
	y, err := types.ParseDecimal256("0.000000000000000003", 65, 18)
	require.NoError(t, err)

	got := make([]types.Decimal256, 1)
	resultNulls := nulls.NewWithSize(1)
	err = d256DivAtScale([]types.Decimal256{x}, []types.Decimal256{y}, got, 30, 18, 30, resultNulls, true)
	require.NoError(t, err)
	require.Equal(t, "333333333333333333.333333333333333333333333333333", got[0].Format(30))

	// The same quotient cannot fit a Decimal128 result and must remain an
	// overflow for callers whose declared result domain is Decimal128.
	x128, err := types.ParseDecimal128("1", 38, 30)
	require.NoError(t, err)
	y128, err := types.ParseDecimal128("0.000000000000000003", 38, 18)
	require.NoError(t, err)
	got128 := make([]types.Decimal128, 1)
	err = d128DivAtScale([]types.Decimal128{x128}, []types.Decimal128{y128}, got128, 30, 18, 30, nulls.NewWithSize(1), true)
	require.Error(t, err)
}

func TestD256DivViaD128NegativePowerOfTwoDivisor(t *testing.T) {
	const numerator = "18446744073709551616"
	for _, tc := range []struct {
		divisor     string
		quotient    string
		intQuotient int64
	}{
		{"-18446744073709551615", "-1.0000", -1},
		{"-18446744073709551616", "-1.0000", -1},
		{"-18446744073709551617", "-1.0000", 0},
	} {
		x, err := types.ParseDecimal256(numerator, 65, 0)
		require.NoError(t, err)
		y, err := types.ParseDecimal256(tc.divisor, 65, 0)
		require.NoError(t, err)
		for _, shape := range []struct {
			name        string
			left, right []types.Decimal256
		}{
			{"vector/vector", []types.Decimal256{x, x}, []types.Decimal256{y, y}},
			{"constant/vector", []types.Decimal256{x}, []types.Decimal256{y, y}},
			{"vector/constant", []types.Decimal256{x, x}, []types.Decimal256{y}},
		} {
			t.Run(tc.divisor+"/"+shape.name, func(t *testing.T) {
				result := make([]types.Decimal256, 2)
				require.NoError(t, d256DivAtScale(shape.left, shape.right, result, 0, 0, 4, nulls.NewWithSize(2), true))
				for _, got := range result {
					require.Equal(t, tc.quotient, got.Format(4))
				}
				integers := make([]int64, 2)
				require.NoError(t, d256IntDiv(shape.left, shape.right, integers, 0, 0, nulls.NewWithSize(2), true))
				require.Equal(t, []int64{tc.intQuotient, tc.intQuotient}, integers)
			})
		}
	}
}

func TestDecimalDivisionMaxUint64Rounding(t *testing.T) {
	const divisor = "18446744073709551615"
	for _, tc := range []struct {
		numerator string
		want      string
	}{
		{"0", "0"},
		{"1", "0"},
		{"9223372036854775807", "0"},
		{"9223372036854775808", "1"},
		{divisor, "1"},
		{"18446744073709551616", "1"},
	} {
		for _, negative := range []bool{false, true} {
			xText, yText, want := tc.numerator, divisor, tc.want
			if negative {
				yText = "-" + yText
				if want != "0" {
					want = "-" + want
				}
			}
			x, err := types.ParseDecimal128(xText, 38, 0)
			require.NoError(t, err)
			y, err := types.ParseDecimal128(yText, 38, 0)
			require.NoError(t, err)
			got := make([]types.Decimal128, 1)
			require.NoError(t, d128DivAtScale([]types.Decimal128{x}, []types.Decimal128{y}, got, 0, 0, 0, nulls.NewWithSize(1), true))
			require.Equal(t, want, got[0].Format(0), "numerator=%s divisor=%s", xText, yText)
		}
	}
}

func TestD256DivViaD128AvoidsIntermediateScaleOverflow(t *testing.T) {
	left, err := types.ParseDecimal256("100000000000000000000", 38, 0)
	require.NoError(t, err)
	right, err := types.ParseDecimal256("1", 38, 30)
	require.NoError(t, err)

	for _, negative := range []bool{false, true} {
		numerator := left
		want := "100000000000000000000.000000000000000000000000000000"
		if negative {
			numerator = numerator.Minus()
			want = "-" + want
		}

		got := make([]types.Decimal256, 1)
		err = d256DivAtScale(
			[]types.Decimal256{numerator},
			[]types.Decimal256{right},
			got,
			0,
			30,
			30,
			nulls.NewWithSize(1),
			true,
		)
		require.NoError(t, err, "negative=%t", negative)
		require.Equal(t, want, got[0].Format(30), "negative=%t", negative)
	}

	overflowingLeft, err := types.ParseDecimal256("99999999999999999999999999999999999999", 38, 0)
	require.NoError(t, err)
	tinyRight, err := types.ParseDecimal256("0.000000000000000000000000000001", 38, 30)
	require.NoError(t, err)
	require.Error(t, d256DivAtScale(
		[]types.Decimal256{overflowingLeft},
		[]types.Decimal256{tinyRight},
		make([]types.Decimal256, 1),
		0,
		30,
		30,
		nulls.NewWithSize(1),
		true,
	))
}

func TestD256DivViaD128MinInt128Divisor(t *testing.T) {
	const numerator = "85070591730234615865843651857943"
	const divisor = "-170141183460469231731687303715884105728"

	for _, negativeNumerator := range []bool{false, true} {
		leftText := numerator
		want := "-0.000001"
		if negativeNumerator {
			leftText = "-" + leftText
			want = "0.000001"
		}

		left, err := types.ParseDecimal256(leftText, 65, 0)
		require.NoError(t, err)
		right, err := types.ParseDecimal256(divisor, 65, 0)
		require.NoError(t, err)

		got := make([]types.Decimal256, 1)
		err = d256DivAtScale([]types.Decimal256{left}, []types.Decimal256{right}, got, 0, 0, 6, nulls.NewWithSize(1), true)
		require.NoError(t, err, "negativeNumerator=%t", negativeNumerator)
		require.Equal(t, want, got[0].Format(6), "negativeNumerator=%t", negativeNumerator)
	}
}

func TestD128DivLargeDivisorHalfUp(t *testing.T) {
	x, err := types.ParseDecimal128("123456789012345", 38, 0)
	require.NoError(t, err)
	y, err := types.ParseDecimal128("999999999999999.999999999999999999", 38, 18)
	require.NoError(t, err)

	for _, negativeX := range []bool{false, true} {
		for _, negativeY := range []bool{false, true} {
			left, right := x, y
			if negativeX {
				left = left.Minus()
			}
			if negativeY {
				right = right.Minus()
			}

			got := make([]types.Decimal128, 1)
			err := d128DivAtScale([]types.Decimal128{left}, []types.Decimal128{right}, got, 0, 18, 6, nulls.NewWithSize(1), true)
			require.NoError(t, err)
			want := "0.123457"
			if negativeX != negativeY {
				want = "-" + want
			}
			require.Equal(t, want, got[0].Format(6), "negativeX=%t negativeY=%t", negativeX, negativeY)
		}
	}
}

func TestD128DivOneToD256FailurePaths(t *testing.T) {
	x := types.Decimal128{B0_63: 1}
	zero := types.Decimal128{}

	t.Run("zero divisor returns error", func(t *testing.T) {
		nul := nulls.NewWithSize(1)
		var dst types.Decimal256
		err := d128DivOneToD256(x, zero, &dst, 0, nul, 0, true, 0, 0)
		require.Error(t, err)
	})

	t.Run("zero divisor sets null", func(t *testing.T) {
		nul := nulls.NewWithSize(1)
		var dst types.Decimal256
		err := d128DivOneToD256(x, zero, &dst, 0, nul, 0, false, 0, 0)
		require.NoError(t, err)
		require.True(t, nul.Contains(0))
	})

	t.Run("D256 scale-up overflow", func(t *testing.T) {
		numerator := []types.Decimal256{{B0_63: 20}}
		divisor := []types.Decimal256{{B0_63: 1}}
		result := make([]types.Decimal256, 1)
		err := d256DivAtScale(numerator, divisor, result, 0, 76, 6, nulls.NewWithSize(1), true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Decimal256 Div overflow")
	})
}

func TestD256Mod(t *testing.T) {
	type decimal = types.Decimal256
	for _, tc := range []struct {
		name       string
		x, y, want []decimal
		s1, s2     int32
		masked     []uint64
	}{
		{
			name: "narrow_VV",
			x:    []decimal{{B0_63: 100}, {B0_63: 18446744073709551516, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:    []decimal{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			want: []decimal{{B0_63: 1}, {B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   0,
			s2:   0,
		},
		{
			name:   "narrow_SV",
			x:      []decimal{{B0_63: 4294967295, B64_127: 1}},
			y:      []decimal{{B0_63: 7}, {B0_63: 7}},
			want:   []decimal{{B0_63: 5}, {B0_63: 5}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},

		{
			name:   "narrow_VS",
			x:      []decimal{{}, {B0_63: 100}, {B0_63: 101}},
			y:      []decimal{{B0_63: 3}},
			want:   []decimal{{}, {B0_63: 1}, {B0_63: 2}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name: "scaleX_VV",
			x:    []decimal{{B0_63: 100}, {B0_63: 18446744073709551516, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:    []decimal{{B0_63: 7}, {B0_63: 7}},
			want: []decimal{{B0_63: 5}, {B0_63: 18446744073709551611, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   2,
			s2:   5,
		},
		{
			name:   "scaleX_SV",
			x:      []decimal{{B0_63: 100}},
			y:      []decimal{{B0_63: 7}, {B0_63: 7}},
			want:   []decimal{{B0_63: 5}, {B0_63: 5}},
			s1:     3,
			s2:     6,
			masked: []uint64{0},
		},
		{
			name: "scaleX_VS",
			x:    []decimal{{B0_63: 100}, {B0_63: 18446744073709551516, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:    []decimal{{B0_63: 7}},
			want: []decimal{{B0_63: 4}, {B0_63: 18446744073709551612, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   4,
			s2:   6,
		},
		{
			name: "scaleX_two_chunks",
			x:    []decimal{{B0_63: 1}, {B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:    []decimal{{B0_63: 7}, {B0_63: 7}},
			want: []decimal{{B0_63: 2}, {B0_63: 18446744073709551614, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   0,
			s2:   20,
		},
		{
			name: "wide_VV",
			x:    []decimal{{B0_63: 100, B64_127: 2}, {B0_63: 18446744073709551516, B64_127: 18446744073709551613, B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:    []decimal{{B0_63: 7, B64_127: 1}, {B0_63: 7, B64_127: 1}},
			want: []decimal{{B0_63: 86}, {B0_63: 18446744073709551530, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   0,
			s2:   0,
		},
		{
			name:   "wide_SV",
			x:      []decimal{{B0_63: 100, B64_127: 2}},
			y:      []decimal{{B0_63: 7, B64_127: 1}, {B0_63: 7, B64_127: 1}},
			want:   []decimal{{B0_63: 86}, {B0_63: 86}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:   "wide_VS",
			x:      []decimal{{B0_63: 100, B64_127: 2}, {B0_63: 18446744073709551516, B64_127: 18446744073709551613, B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:      []decimal{{B0_63: 7, B64_127: 1}},
			want:   []decimal{{B0_63: 86}, {B0_63: 18446744073709551530, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name: "wide_scaleX",
			x:    []decimal{{B0_63: 1}, {B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:    []decimal{{B0_63: 7, B64_127: 1}, {B0_63: 7, B64_127: 1}},
			want: []decimal{{B0_63: 100}, {B0_63: 18446744073709551516, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   0,
			s2:   2,
		},
		{
			name: "scaleY_VV",
			x:    []decimal{{B0_63: 100}, {B0_63: 18446744073709551516, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:    []decimal{{B0_63: 7}, {B0_63: 7}},
			want: []decimal{{B0_63: 100}, {B0_63: 18446744073709551516, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   6,
			s2:   2,
		},
		{
			name:   "scaleY_SV",
			x:      []decimal{{B0_63: 100}},
			y:      []decimal{{B0_63: 7}, {B0_63: 7}},
			want:   []decimal{{B0_63: 100}, {B0_63: 100}},
			s1:     6,
			s2:     2,
			masked: []uint64{0},
		},
		{
			name:   "scaleY_VS",
			x:      []decimal{{B0_63: 100}, {B0_63: 18446744073709551516, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:      []decimal{{B0_63: 7}},
			want:   []decimal{{B0_63: 100}, {B0_63: 18446744073709551516, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:     6,
			s2:     3,
			masked: []uint64{0},
		},
		{
			name: "scaleY_two_chunks",
			x:    []decimal{{B0_63: 7}, {B0_63: 18446744073709551609, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:    []decimal{{B0_63: 1}, {B0_63: 1}},
			want: []decimal{{B0_63: 7}, {B0_63: 18446744073709551609, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   20,
			s2:   0,
		},
		{
			name: "fallback_inline_X",
			x:    []decimal{{B0_63: ^uint64(0), B64_127: 9223372036854775807}, {B0_63: 1, B64_127: 9223372036854775808, B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:    []decimal{{B0_63: 7}, {B0_63: 7}},
			want: []decimal{{B0_63: 2}, {B0_63: 18446744073709551614, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   0,
			s2:   2,
		},
		{
			name: "fallback_wide_X",
			x:    []decimal{{B0_63: ^uint64(0), B64_127: 9223372036854775807}, {B0_63: 1, B64_127: 9223372036854775808, B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:    []decimal{{B0_63: 7, B64_127: 1}, {B0_63: 7, B64_127: 1}},
			want: []decimal{{B0_63: 2350}, {B0_63: 18446744073709549266, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   0,
			s2:   2,
		},
		{
			name: "fallback_Y",
			x:    []decimal{{B0_63: 7}, {B0_63: 18446744073709551609, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			y:    []decimal{{B0_63: ^uint64(0), B64_127: 9223372036854775807}},
			want: []decimal{{B0_63: 7}, {B0_63: 18446744073709551609, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   2,
			s2:   0,
		},
		{
			name:   "generic_SV",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			y:      []decimal{{B0_63: 17}, {B0_63: 17}},
			want:   []decimal{{B0_63: 3}, {B0_63: 3}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name: "generic_VS",
			x:    []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}, {B0_63: 1, B64_127: 18446744073709551614, B128_191: 18446744073709551614, B192_255: 18446744073709551614}},
			y:    []decimal{{B0_63: 7, B128_191: 1}},
			want: []decimal{{B0_63: ^uint64(0), B64_127: 18446744073709551610}, {B0_63: 1, B64_127: 5, B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   0,
			s2:   0,
		},
		{
			name: "generic_scaleY",
			x:    []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}, {B0_63: 1, B64_127: 18446744073709551614, B128_191: 18446744073709551614, B192_255: 18446744073709551614}},
			y:    []decimal{{B0_63: 7}, {B0_63: 7}},
			want: []decimal{{B0_63: 57583}, {B0_63: 18446744073709494033, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   6,
			s2:   2,
		},
		{
			name: "generic_scaleX",
			x:    []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}, {B0_63: 1, B64_127: 18446744073709551614, B128_191: 18446744073709551614, B192_255: 18446744073709551614}},
			y:    []decimal{{B0_63: 7}, {B0_63: 7}},
			want: []decimal{{B0_63: 4}, {B0_63: 18446744073709551612, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			s1:   2,
			s2:   6,
		},
		{
			name:   "large_right_scalar",
			x:      []decimal{{B0_63: 100}, {B0_63: 100}},
			y:      []decimal{{B0_63: 3, B128_191: 1}},
			want:   []decimal{{B0_63: 100}, {B0_63: 100}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nul := nulls.NewWithSize(len(tc.want))
			for _, i := range tc.masked {
				nul.Add(i)
			}
			got := make([]decimal, len(tc.want))
			var err error
			if tc.name == "narrow_VV" {
				err = d256ModKernel(true)(tc.x, tc.y, got, tc.s1, tc.s2, nul)
			} else {
				err = d256Mod(tc.x, tc.y, got, tc.s1, tc.s2, nul, true)
			}
			require.NoError(t, err)
			expectedNull := make(map[uint64]bool, len(tc.masked))
			for _, i := range tc.masked {
				expectedNull[i] = true
			}
			require.Equal(t, len(expectedNull), nul.Count())
			for i := range got {
				require.Equal(t, expectedNull[uint64(i)], nul.Contains(uint64(i)), "NULL row %d", i)
				if !expectedNull[uint64(i)] {
					require.Equal(t, tc.want[i], got[i], "coefficient row %d", i)
				}
			}
		})
	}
}

// TestD256Mod_LargeValues tests D256 Mod with values outside D128 range (generic slow path).
func TestD256Mod_LargeValues(t *testing.T) {
	type decimal = types.Decimal256
	x := []decimal{{B0_63: 12379813812177893520, B64_127: 4660, B128_191: 1}, {B0_63: 10986060915027139770, B64_127: 22136, B128_191: 2}, {B0_63: 1229782938247303441, B64_127: 8738, B128_191: 3}, {B0_63: ^uint64(0), B64_127: 13107}}
	y := []decimal{{B0_63: 17}, {B0_63: 31}, {B0_63: 97}, {B0_63: 1000003}}
	want := []decimal{{B0_63: 1}, {B0_63: 12}, {B0_63: 15}, {B0_63: 791407}}
	got := make([]decimal, len(want))
	nul := nulls.NewWithSize(len(want))
	require.NoError(t, d256Mod(x, y, got, 0, 0, nul, true))
	require.Equal(t, 0, nul.Count())
	require.Equal(t, want, got)
}

func TestD256ModScaleAlignmentOverflow(t *testing.T) {
	maxCoefficient := new(big.Int).Sub(
		new(big.Int).Exp(big.NewInt(10), big.NewInt(65), nil), big.NewInt(1))
	maximum, err := types.ParseDecimal256(maxCoefficient.String(), 65, 0)
	require.NoError(t, err)
	seven := types.Decimal256FromInt64(7)
	negativeSeven := seven.Minus()
	values := []types.Decimal256{maximum, maximum.Minus(), maximum, maximum.Minus()}
	divisors := []types.Decimal256{seven, seven, negativeSeven, negativeSeven}
	results := make([]types.Decimal256, len(values))
	require.NoError(t, d256Mod(values, divisors, results, 0, 30, nulls.NewWithSize(len(values)), true))
	for i, want := range []int64{4, -4, 4, -4} {
		require.Equal(t, types.Decimal256FromInt64(want), results[i], "scale-up remainder[%d]", i)
	}

	// The reverse alignment overflows while scaling the divisor; the divisor
	// is nevertheless larger than the dividend at the common scale.
	values = []types.Decimal256{seven, negativeSeven, seven, negativeSeven}
	divisors = []types.Decimal256{maximum, maximum, maximum.Minus(), maximum.Minus()}
	results = make([]types.Decimal256, len(values))
	require.NoError(t, d256Mod(values, divisors, results, 30, 0, nulls.NewWithSize(len(values)), true))
	for i, want := range []int64{7, -7, 7, -7} {
		require.Equal(t, types.Decimal256FromInt64(want), results[i], "scale-down remainder[%d]", i)
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
		_ = d256DivAtScale(xs, ys, rs, 2, 2, 8, nul, true)
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

// ---- IntDiv correctness tests ----

// refD128IntDiv computes the reference result for D128 integer division.
func refD128IntDiv(x, y types.Decimal128, scale1, scale2 int32) (int64, error) {
	signx := x.Sign()
	signy := y.Sign()
	if signx {
		x = x.Minus()
	}
	if signy {
		y = y.Minus()
	}
	x256 := types.Decimal256{B0_63: x.B0_63, B64_127: x.B64_127}
	y256 := types.Decimal256{B0_63: y.B0_63, B64_127: y.B64_127}
	return refD256IntDivUnsigned(x256, y256, signx != signy, scale1, scale2)
}

func TestD64IntDiv(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(101))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		rs := make([]int64, testBatchSize)
		for i := range v1 {
			v1[i] = randD64(rng)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d64IntDiv(v1, v2, rs, 2, 2, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			x := functionUtil.ConvertD64ToD128(v1[i])
			y := functionUtil.ConvertD64ToD128(v2[i])
			want, err := refD128IntDiv(x, y, 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64IntDiv[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(102))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(998) + 1)
		}
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(998) + 1)}

		rs := make([]int64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64IntDiv(scalar, vec, rs, 2, 2, nul, true))
		for i := range vec {
			x := functionUtil.ConvertD64ToD128(scalar[0])
			y := functionUtil.ConvertD64ToD128(vec[i])
			want, err := refD128IntDiv(x, y, 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64 const-vec intdiv[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(103))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = randD64(rng)
		}
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(998) + 1)}

		rs := make([]int64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64IntDiv(vec, scalar, rs, 2, 2, nul, true))
		for i := range vec {
			x := functionUtil.ConvertD64ToD128(vec[i])
			y := functionUtil.ConvertD64ToD128(scalar[0])
			want, err := refD128IntDiv(x, y, 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64 vec-const intdiv[%d]", i)
		}
	})

	t.Run("DivByZero_Null", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{0}
		rs := make([]int64, 1)
		nul := nulls.NewWithSize(1)
		err := d64IntDiv(v1, v2, rs, 2, 2, nul, false)
		require.NoError(t, err)
		require.True(t, nul.Contains(0))
	})

	t.Run("DiffScale", func(t *testing.T) {
		rng := rand.New(rand.NewSource(104))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		rs := make([]int64, testBatchSize)
		for i := range v1 {
			v1[i] = randD64(rng)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d64IntDiv(v1, v2, rs, 4, 2, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			x := functionUtil.ConvertD64ToD128(v1[i])
			y := functionUtil.ConvertD64ToD128(v2[i])
			want, err := refD128IntDiv(x, y, 4, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64IntDiv DiffScale[%d]", i)
		}
	})
}

func TestD128IntDiv(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(201))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]int64, testBatchSize)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d128IntDiv(v1, v2, rs, 2, 2, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, err := refD128IntDiv(v1[i], v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128IntDiv[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(202))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal128{randD128Small(rng)}

		rs := make([]int64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128IntDiv(scalar, vec, rs, 2, 2, nul, true))
		for i := range vec {
			want, err := refD128IntDiv(scalar[0], vec[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 const-vec intdiv[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(203))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}

		rs := make([]int64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128IntDiv(vec, scalar, rs, 2, 2, nul, true))
		for i := range vec {
			want, err := refD128IntDiv(vec[i], scalar[0], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128 vec-const intdiv[%d]", i)
		}
	})

	t.Run("DivByZero_Null", func(t *testing.T) {
		v1 := []types.Decimal128{{B0_63: 100}}
		v2 := []types.Decimal128{{B0_63: 0, B64_127: 0}}
		rs := make([]int64, 1)
		nul := nulls.NewWithSize(1)
		err := d128IntDiv(v1, v2, rs, 2, 2, nul, false)
		require.NoError(t, err)
		require.True(t, nul.Contains(0))
	})

	t.Run("DiffScale", func(t *testing.T) {
		rng := rand.New(rand.NewSource(204))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]int64, testBatchSize)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d128IntDiv(v1, v2, rs, 4, 2, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, err := refD128IntDiv(v1[i], v2[i], 4, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128IntDiv DiffScale[%d]", i)
		}
	})

	t.Run("LargeValues_Fallback", func(t *testing.T) {
		rng := rand.New(rand.NewSource(205))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]int64, testBatchSize)
		for i := range v1 {
			v1[i] = randD128(rng)
			v2[i] = randD128(rng)
			if d128IsZero(v2[i]) {
				v2[i].B0_63 = 1
			}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d128IntDiv(v1, v2, rs, 2, 2, nul, false)
		require.NoError(t, err)
		for i := range v1 {
			if nul.Contains(uint64(i)) {
				continue
			}
			want, err := refD128IntDiv(v1[i], v2[i], 2, 2)
			if err != nil {
				continue
			}
			require.Equal(t, want, rs[i], "d128IntDiv large[%d]", i)
		}
	})
}

// refD256IntDiv computes the reference result for D256 integer division.
func refD256IntDiv(x, y types.Decimal256, scale1, scale2 int32) (int64, error) {
	signx := x.Sign()
	signy := y.Sign()
	if signx {
		x = x.Minus()
	}
	if signy {
		y = y.Minus()
	}
	return refD256IntDivUnsigned(x, y, signx != signy, scale1, scale2)
}

func refD256IntDivUnsigned(x, y types.Decimal256, neg bool, scale1, scale2 int32) (int64, error) {
	scaleAdj := scale2 - scale1
	var err error
	if scaleAdj > 0 {
		x, err = x.Scale(scaleAdj)
	} else if scaleAdj < 0 {
		y, err = y.Scale(-scaleAdj)
	}
	if err != nil {
		return 0, err
	}
	r, err := x.Div256Trunc(y)
	if err != nil {
		return 0, err
	}
	if neg {
		r = r.Minus()
	}
	return decimal256ToInt64(r)
}

func TestD256IntDiv(t *testing.T) {
	t.Run("VecVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(301))
		v1 := make([]types.Decimal256, testBatchSize)
		v2 := make([]types.Decimal256, testBatchSize)
		rs := make([]int64, testBatchSize)
		for i := range v1 {
			v1[i] = randD256Small(rng)
			v2[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d256IntDiv(v1, v2, rs, 2, 2, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, err := refD256IntDiv(v1[i], v2[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256IntDiv[%d]", i)
		}
	})

	t.Run("GenericPathTruncates", func(t *testing.T) {
		x := types.Decimal256{B128_191: 5}
		y := types.Decimal256{B128_191: 2}
		v1 := []types.Decimal256{x, x.Minus()}
		v2 := []types.Decimal256{y, y}
		rs := make([]int64, len(v1))
		nul := nulls.NewWithSize(len(v1))

		require.NoError(t, d256IntDiv(v1, v2, rs, 0, 0, nul, true))
		require.Equal(t, []int64{2, -2}, rs)
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(302))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal256{randD256Small(rng)}

		rs := make([]int64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256IntDiv(scalar, vec, rs, 2, 2, nul, true))
		for i := range vec {
			want, err := refD256IntDiv(scalar[0], vec[i], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 const-vec intdiv[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(303))
		vec := make([]types.Decimal256, testBatchSize)
		for i := range vec {
			vec[i] = randD256Small(rng)
		}
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}

		rs := make([]int64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d256IntDiv(vec, scalar, rs, 2, 2, nul, true))
		for i := range vec {
			want, err := refD256IntDiv(vec[i], scalar[0], 2, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d256 vec-const intdiv[%d]", i)
		}
	})

	t.Run("DivByZero_Null", func(t *testing.T) {
		v1 := []types.Decimal256{{B0_63: 100}}
		v2 := []types.Decimal256{{B0_63: 0}}
		rs := make([]int64, 1)
		nul := nulls.NewWithSize(1)
		err := d256IntDiv(v1, v2, rs, 2, 2, nul, false)
		require.NoError(t, err)
		require.True(t, nul.Contains(0))
	})

	t.Run("LargeValues", func(t *testing.T) {
		rng := rand.New(rand.NewSource(304))
		v1 := make([]types.Decimal256, testBatchSize)
		v2 := make([]types.Decimal256, testBatchSize)
		rs := make([]int64, testBatchSize)
		for i := range v1 {
			v1[i] = randD256(rng)
			v2[i] = randD256(rng)
			if v2[i].B0_63 == 0 && v2[i].B64_127 == 0 {
				v2[i].B0_63 = 1
			}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d256IntDiv(v1, v2, rs, 2, 2, nul, false)
		require.NoError(t, err)
		for i := range v1 {
			if nul.Contains(uint64(i)) {
				continue
			}
			want, err := refD256IntDiv(v1[i], v2[i], 2, 2)
			if err != nil {
				continue
			}
			require.Equal(t, want, rs[i], "d256IntDiv large[%d]", i)
		}
	})
}

func TestD256IntDivScaleAlignmentOverflow(t *testing.T) {
	maxCoefficient := new(big.Int).Sub(
		new(big.Int).Exp(big.NewInt(10), big.NewInt(65), nil), big.NewInt(1))
	maximum, err := types.ParseDecimal256(maxCoefficient.String(), 65, 0)
	require.NoError(t, err)
	seven := types.Decimal256FromInt64(7)

	// 7e-30 divided by a 65-digit integer is a representable zero even though
	// expanding the divisor by 10^30 cannot fit in Decimal256.
	zeroResults := make([]int64, 4)
	zeroInputs := []types.Decimal256{seven, seven.Minus(), seven, seven.Minus()}
	zeroDivisors := []types.Decimal256{maximum, maximum, maximum.Minus(), maximum.Minus()}
	require.NoError(t, d256IntDiv(
		zeroInputs, zeroDivisors, zeroResults, 30, 0, nulls.NewWithSize(4), true))
	require.Equal(t, []int64{0, 0, 0, 0}, zeroResults)

	// This distinguishes truncation from the rounded Scale operation:
	// floor((10^60-1)/10^30)/(5*10^29) is 1, whereas rounding first yields 2.
	truncateNumeratorCoefficient := new(big.Int).Sub(
		new(big.Int).Exp(big.NewInt(10), big.NewInt(60), nil), big.NewInt(1))
	truncateNumerator, err := types.ParseDecimal256(truncateNumeratorCoefficient.String(), 65, 0)
	require.NoError(t, err)
	truncateDivisorCoefficient := new(big.Int).Mul(big.NewInt(5), new(big.Int).Exp(big.NewInt(10), big.NewInt(29), nil))
	truncateDivisor, err := types.ParseDecimal256(truncateDivisorCoefficient.String(), 65, 0)
	require.NoError(t, err)
	truncateResults := make([]int64, 2)
	require.NoError(t, d256IntDiv(
		[]types.Decimal256{truncateNumerator, truncateNumerator.Minus()},
		[]types.Decimal256{truncateDivisor, truncateDivisor},
		truncateResults, 30, 0, nulls.NewWithSize(2), true))
	require.Equal(t, []int64{1, -1}, truncateResults)

	// The positive scale adjustment can also overflow the numerator while the
	// quotient remains representable; preserve the exact quotient and sign.
	positiveResults := make([]int64, 4)
	positiveNumerators := []types.Decimal256{maximum, maximum.Minus(), maximum, maximum.Minus()}
	positiveDivisors := []types.Decimal256{maximum, maximum, maximum.Minus(), maximum.Minus()}
	require.NoError(t, d256IntDiv(
		positiveNumerators, positiveDivisors, positiveResults, 0, 12, nulls.NewWithSize(4), true))
	require.Equal(t, []int64{1_000_000_000_000, -1_000_000_000_000, -1_000_000_000_000, 1_000_000_000_000}, positiveResults)

	// Preserve the null-row short circuit before division-by-zero handling.
	nullResults := make([]int64, 2)
	nullsWithZero := nulls.NewWithSize(2)
	nullsWithZero.Add(1)
	require.NoError(t, d256IntDiv(
		[]types.Decimal256{maximum, seven},
		[]types.Decimal256{maximum, {}},
		nullResults, 0, 12, nullsWithZero, true))
	require.Equal(t, int64(1_000_000_000_000), nullResults[0])
	require.True(t, nullsWithZero.Contains(1))

	// A genuinely out-of-range BIGINT quotient remains an error.
	err = d256IntDiv(
		[]types.Decimal256{maximum},
		[]types.Decimal256{seven},
		make([]int64, 1), 0, 12, nulls.NewWithSize(1), true)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange))
}

// ---- D128/D256 Mod with diff-scale (additional coverage) ----

func TestD128Mod_DiffScale(t *testing.T) {
	t.Run("VecVec_Scale1GT", func(t *testing.T) {
		rng := rand.New(rand.NewSource(701))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d128Mod(v1, v2, rs, 6, 2, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Mod(v2[i], 6, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128Mod DiffScale s1>s2[%d]", i)
		}
	})

	t.Run("VecVec_Scale1LT", func(t *testing.T) {
		rng := rand.New(rand.NewSource(702))
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d128Mod(v1, v2, rs, 2, 6, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Mod(v2[i], 2, 6)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128Mod DiffScale s1<s2[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(703))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		scalar := []types.Decimal128{randD128Small(rng)}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Mod(scalar, vec, rs, 6, 2, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Mod(vec[i], 6, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128Mod const-vec diffscale[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(704))
		vec := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}

		rs := make([]types.Decimal128, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d128Mod(vec, scalar, rs, 6, 2, nul, true))
		for i := range vec {
			want, _, err := vec[i].Mod(scalar[0], 6, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d128Mod vec-const diffscale[%d]", i)
		}
	})
}

func TestD64Mod_DiffScale(t *testing.T) {
	t.Run("VecVec_Scale1GT", func(t *testing.T) {
		rng := rand.New(rand.NewSource(801))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		rs := make([]types.Decimal64, testBatchSize)
		for i := range v1 {
			v1[i] = randD64(rng)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d64Mod(v1, v2, rs, 6, 2, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Mod(v2[i], 6, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64Mod DiffScale s1>s2[%d]", i)
		}
	})

	t.Run("VecVec_Scale1LT", func(t *testing.T) {
		rng := rand.New(rand.NewSource(802))
		v1 := make([]types.Decimal64, testBatchSize)
		v2 := make([]types.Decimal64, testBatchSize)
		rs := make([]types.Decimal64, testBatchSize)
		for i := range v1 {
			v1[i] = randD64(rng)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(testBatchSize)
		err := d64Mod(v1, v2, rs, 2, 6, nul, true)
		require.NoError(t, err)
		for i := range v1 {
			want, _, err := v1[i].Mod(v2[i], 2, 6)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64Mod DiffScale s1<s2[%d]", i)
		}
	})

	t.Run("ScalarVec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(803))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		scalar := []types.Decimal64{randD64(rng)}

		rs := make([]types.Decimal64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Mod(scalar, vec, rs, 6, 2, nul, true))
		for i := range vec {
			want, _, err := scalar[0].Mod(vec[i], 6, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64Mod const-vec diffscale[%d]", i)
		}
	})

	t.Run("VecScalar", func(t *testing.T) {
		rng := rand.New(rand.NewSource(804))
		vec := make([]types.Decimal64, testBatchSize)
		for i := range vec {
			vec[i] = randD64(rng)
		}
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}

		rs := make([]types.Decimal64, testBatchSize)
		nul := nulls.NewWithSize(testBatchSize)
		require.NoError(t, d64Mod(vec, scalar, rs, 6, 2, nul, true))
		for i := range vec {
			want, _, err := vec[i].Mod(scalar[0], 6, 2)
			require.NoError(t, err)
			require.Equal(t, want, rs[i], "d64Mod vec-const diffscale[%d]", i)
		}
	})
}

// ---- D256 Mul additional scalar paths ----

// ---- D256 Mod scalar paths ----

// ---- Extended coverage tests for scalar/vector dispatch paths ----

func TestD128MulInline_Coverage(t *testing.T) {
	for _, tc := range []struct {
		name        string
		x, y, want  types.Decimal128
		adj, s1, s2 int32
	}{
		{"BothFitInt64", types.Decimal128{B0_63: 1000}, types.Decimal128{B0_63: 2000}, types.Decimal128{B0_63: 2000000}, 0, 2, 2},
		{"BothFitInt64_Negative", types.Decimal128{B0_63: 18446744073709550616, B64_127: 18446744073709551615}, types.Decimal128{B0_63: 2000}, types.Decimal128{B0_63: 18446744073707551616, B64_127: 18446744073709551615}, 0, 2, 2},
		{"BothFitInt64_WithScaleAdj", types.Decimal128{B0_63: 123456}, types.Decimal128{B0_63: 789012}, types.Decimal128{B0_63: 9740827}, -4, 8, 8},
		{"OneHiNonZero_Fits128", types.Decimal128{B0_63: 4294967295, B64_127: 1}, types.Decimal128{B0_63: 3}, types.Decimal128{B0_63: 12884901885, B64_127: 3}, 0, 2, 2},
		{"D256Fallback", types.Decimal128{B0_63: 18446744073709551615, B64_127: 32767}, types.Decimal128{B0_63: 18446744073709551615, B64_127: 1}, types.Decimal128{B0_63: 11606224177980273129, B64_127: 1208925819614}, -12, 12, 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got types.Decimal128
			require.NoError(t, d128MulInline(&tc.x, &tc.y, &got, tc.adj, tc.s1, tc.s2))
			require.Equal(t, tc.want, got)
		})
	}
}

func TestD128ScaleIntoRs_Coverage(t *testing.T) {
	for _, tc := range []struct {
		name      string
		input     []types.Decimal128
		scale     int32
		nullRows  []uint64
		want      []types.Decimal128
		wantError uint16
	}{
		{"AllFitInt64_NoNull", []types.Decimal128{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff}}, 5, nil, []types.Decimal128{{B0_63: 0x186a0}, {B0_63: 0xfffffffffffe7960, B64_127: 0xffffffffffffffff}}, 0},
		{"AllFitInt64_WithNull", []types.Decimal128{{B0_63: 0x1}, {}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff}}, 5, []uint64{1}, []types.Decimal128{{B0_63: 0x186a0}, {}, {B0_63: 0xfffffffffffe7960, B64_127: 0xffffffffffffffff}}, 0},
		{"LargeValues_NoNull", []types.Decimal128{{B0_63: 0x1, B64_127: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xfffffffffffffffe}}, 5, nil, []types.Decimal128{{B0_63: 0x186a0, B64_127: 0x186a0}, {B0_63: 0xfffffffffffe7960, B64_127: 0xfffffffffffe795f}}, 0},
		{"LargeValues_WithNull", []types.Decimal128{{B0_63: 0x1, B64_127: 0x1}, {}, {B0_63: 0xffffffffffffffff, B64_127: 0xfffffffffffffffe}}, 5, []uint64{1}, []types.Decimal128{{B0_63: 0x186a0, B64_127: 0x186a0}, {}, {B0_63: 0xfffffffffffe7960, B64_127: 0xfffffffffffe795f}}, 0},
		{"NineteenDigitFactor", []types.Decimal128{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff}}, 19, nil, []types.Decimal128{{B0_63: 0x8ac7230489e80000}, {B0_63: 0x7538dcfb76180000, B64_127: 0xffffffffffffffff}}, 0},
		{"TwoStepThirtyEight", []types.Decimal128{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff}}, 38, nil, []types.Decimal128{{B0_63: 0x98a224000000000, B64_127: 0x4b3b4ca85a86c47a}, {B0_63: 0xf675ddc000000000, B64_127: 0xb4c4b357a5793b85}}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := nulls.NewWithSize(len(tc.input))
			for _, row := range tc.nullRows {
				n.Add(row)
			}
			got := make([]types.Decimal128, len(tc.input))
			err := d128ScaleIntoRs(tc.input, got, len(got), tc.scale, n)
			if tc.wantError != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.wantError), "error: %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.nullRows), n.Count())
			for i := range got {
				masked := false
				for _, row := range tc.nullRows {
					masked = masked || row == uint64(i)
				}
				require.Equal(t, masked, n.Contains(uint64(i)))
				if !masked && tc.wantError == 0 {
					require.Equal(t, tc.want[i], got[i], "row %d", i)
				}
			}
		})
	}
}

func TestD128DivPow10_Coverage(t *testing.T) {
	for _, tc := range []struct {
		name        string
		input, want types.Decimal128
		scale       int32
	}{
		{"single_chunk_rounds", types.Decimal128{B0_63: 123456789}, types.Decimal128{B0_63: 123457}, 3},
		{"last_single_chunk", types.Decimal128{B64_127: 1}, types.Decimal128{B0_63: 2}, 19},
		{"first_two_chunk", types.Decimal128{B64_127: 1}, types.Decimal128{}, 20},
		{"largest_two_chunk", types.Decimal128{B0_63: ^uint64(0), B64_127: 1<<63 - 1}, types.Decimal128{B0_63: 2}, 38},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.input
			d128DivPow10(&got, tc.scale)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestDecimalScaleDownMultiStepRounding(t *testing.T) {
	pow10a, twoStep, pow10b := scalePow10Factors(30)
	require.True(t, twoStep)

	for _, tc := range []struct {
		input string
		want  string
	}{
		{"0.499999999999999999999999999999", "0"},
		{"-0.499999999999999999999999999999", "0"},
		{"0.500000000000000000000000000001", "1"},
		{"-0.500000000000000000000000000001", "-1"},
	} {
		t.Run("D128/"+tc.input, func(t *testing.T) {
			x, err := types.ParseDecimal128(tc.input, 38, 30)
			require.NoError(t, err)

			general := x
			d128ScaleDown(&general, 30)
			require.Equal(t, tc.want, general.Format(0))

			optimized := x
			d128ScaleDownPow10(&optimized, pow10a, twoStep, pow10b)
			require.Equal(t, tc.want, optimized.Format(0))
		})

		t.Run("D256/"+tc.input, func(t *testing.T) {
			x, err := types.ParseDecimal256(tc.input, 65, 30)
			require.NoError(t, err)

			general := x
			d256ScaleDown(&general, 30)
			require.Equal(t, tc.want, general.Format(0))

			optimized := x
			d256ScaleDownPow10(&optimized, pow10a, twoStep, pow10b)
			require.Equal(t, tc.want, optimized.Format(0))
		})
	}

	t.Run("CallerD128_generic", func(t *testing.T) {
		type decimal = types.Decimal128
		x := []decimal{{B0_63: 2538472135152631807, B64_127: 27105054312}, {B0_63: 15908271938556919809, B64_127: 18446744046604497303}, {B0_63: 2538472135152631809, B64_127: 27105054312}, {B0_63: 15908271938556919807, B64_127: 18446744046604497303}}
		y := []decimal{{B0_63: 1, B64_127: 0}}
		want := []decimal{{B0_63: 0, B64_127: 0}, {B0_63: 0, B64_127: 0}, {B0_63: 1, B64_127: 0}, {B0_63: 18446744073709551615, B64_127: 18446744073709551615}}
		got := make([]decimal, len(want))
		nul := nulls.NewWithSize(len(want))
		require.NoError(t, d128Mul(x, y, got, 30, 30, nul))
		require.Equal(t, 0, nul.Count())
		require.Equal(t, want, got)
	})

	t.Run("CallerD128_int64", func(t *testing.T) {
		type decimal = types.Decimal128
		x := []decimal{{B0_63: 499999999999999, B64_127: 0}, {B0_63: 18446244073709551616, B64_127: 18446744073709551615}}
		y := []decimal{{B0_63: 1000000000000000, B64_127: 0}}
		want := []decimal{{B0_63: 0, B64_127: 0}, {B0_63: 18446744073709551615, B64_127: 18446744073709551615}}
		got := make([]decimal, len(want))
		nul := nulls.NewWithSize(len(want))
		require.NoError(t, d128Mul(x, y, got, 30, 30, nul))
		require.Equal(t, 0, nul.Count())
		require.Equal(t, want, got)
	})

	t.Run("CallerD256_generic", func(t *testing.T) {
		type decimal = types.Decimal256
		x := []decimal{{B0_63: 2538472135152631807, B64_127: 27105054312, B128_191: 0, B192_255: 0}, {B0_63: 15908271938556919809, B64_127: 18446744046604497303, B128_191: 18446744073709551615, B192_255: 18446744073709551615}, {B0_63: 2538472135152631809, B64_127: 27105054312, B128_191: 0, B192_255: 0}, {B0_63: 15908271938556919807, B64_127: 18446744046604497303, B128_191: 18446744073709551615, B192_255: 18446744073709551615}}
		y := []decimal{{B0_63: 1, B64_127: 0, B128_191: 0, B192_255: 0}}
		want := []decimal{{B0_63: 0, B64_127: 0, B128_191: 0, B192_255: 0}, {B0_63: 0, B64_127: 0, B128_191: 0, B192_255: 0}, {B0_63: 1, B64_127: 0, B128_191: 0, B192_255: 0}, {B0_63: 18446744073709551615, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}}
		got := make([]decimal, len(want))
		nul := nulls.NewWithSize(len(want))
		require.NoError(t, d256Mul(x, y, got, 30, 30, nul))
		require.Equal(t, 0, nul.Count())
		require.Equal(t, want, got)
	})

	t.Run("CallerD256_int64", func(t *testing.T) {
		type decimal = types.Decimal256
		x := []decimal{{B0_63: 499999999999999, B64_127: 0, B128_191: 0, B192_255: 0}, {B0_63: 18446244073709551616, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}}
		y := []decimal{{B0_63: 1000000000000000, B64_127: 0, B128_191: 0, B192_255: 0}}
		want := []decimal{{B0_63: 0, B64_127: 0, B128_191: 0, B192_255: 0}, {B0_63: 18446744073709551615, B64_127: 18446744073709551615, B128_191: 18446744073709551615, B192_255: 18446744073709551615}}
		got := make([]decimal, len(want))
		nul := nulls.NewWithSize(len(want))
		require.NoError(t, d256Mul(x, y, got, 30, 30, nul))
		require.Equal(t, 0, nul.Count())
		require.Equal(t, want, got)
	})
}

func TestD128ScaleDown_Coverage(t *testing.T) {
	t.Run("Positive", func(t *testing.T) {
		x := types.Decimal128{B0_63: 123456789}
		d128ScaleDown(&x, 3)
		require.Equal(t, uint64(123457), x.B0_63)
		require.Equal(t, uint64(0), x.B64_127)
	})

	t.Run("Negative", func(t *testing.T) {
		// -123456789 in two's complement
		x := types.Decimal128{B0_63: ^uint64(123456789) + 1, B64_127: ^uint64(0)}
		d128ScaleDown(&x, 3)
		// Should be -123457
		want := types.Decimal128{B0_63: ^uint64(123457) + 1, B64_127: ^uint64(0)}
		require.Equal(t, want, x)
	})
}

func TestD256ScalePow10_Coverage(t *testing.T) {
	t.Run("ScaleUpPow10_OneStep", func(t *testing.T) {
		x := types.Decimal256{B0_63: 42}
		ok := d256ScaleUpPow10(&x, types.Pow10[3], false, 0)
		require.True(t, ok)
		require.Equal(t, types.Decimal256{B0_63: 0xa410}, x)
	})

	t.Run("ScaleUpPow10_TwoStep", func(t *testing.T) {
		x := types.Decimal256{B0_63: 1}
		ok := d256ScaleUpPow10(&x, types.Pow10[19], true, types.Pow10[5])
		require.True(t, ok)
		require.Equal(t, types.Decimal256{B0_63: 0x1bcecceda1000000, B64_127: 0xd3c2}, x)
	})

	t.Run("ScaleUpPow10_Negative", func(t *testing.T) {
		x := types.Decimal256{B0_63: ^uint64(42) + 1, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}
		ok := d256ScaleUpPow10(&x, types.Pow10[3], false, 0)
		require.True(t, ok)
		require.Equal(t, types.Decimal256{B0_63: 0xffffffffffff5bf0, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}, x)
	})

	t.Run("ScaleDownPow10_OneStep", func(t *testing.T) {
		x := types.Decimal256{B0_63: 42000}
		d256ScaleDownPow10(&x, types.Pow10[3], false, 0)
		require.Equal(t, uint64(42), x.B0_63)
	})

	t.Run("ScaleDownPow10_TwoStep", func(t *testing.T) {
		x := types.Decimal256{B0_63: 0, B64_127: 1}
		d256ScaleDownPow10(&x, types.Pow10[10], true, types.Pow10[5])
	})

	t.Run("ScaleDownPow10_Negative", func(t *testing.T) {
		x := types.Decimal256{B0_63: ^uint64(42000) + 1, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}
		d256ScaleDownPow10(&x, types.Pow10[3], false, 0)
	})
}

func TestD128ModOne_Coverage(t *testing.T) {
	t.Run("SmallValues", func(t *testing.T) {
		x := types.Decimal128{B0_63: 17}
		y := types.Decimal128{B0_63: 5}
		r := d128ModOne(x, y)
		require.Equal(t, uint64(2), r.B0_63)
	})

	t.Run("NegativeDividend", func(t *testing.T) {
		x := types.Decimal128{B0_63: ^uint64(17) + 1, B64_127: ^uint64(0)} // -17
		y := types.Decimal128{B0_63: 5}
		r := d128ModOne(x, y)
		// -17 % 5 = -2
		want := types.Decimal128{B0_63: ^uint64(2) + 1, B64_127: ^uint64(0)}
		require.Equal(t, want, r)
	})

	t.Run("LargeDivisor", func(t *testing.T) {
		x := types.Decimal128{B0_63: 100, B64_127: 1}
		y := types.Decimal128{B0_63: 7, B64_127: 1}
		r := d128ModOne(x, y)
		_ = r // just exercise the large-divisor Mod128 path
	})
}

func TestD128ModDiffScaleXPow10_Coverage(t *testing.T) {
	t.Run("OneStep", func(t *testing.T) {
		x := types.Decimal128{B0_63: 17}
		y := types.Decimal128{B0_63: 50}
		r, ok := d128ModDiffScaleXPow10(x, y, types.Pow10[1], false, 0) // scale x up by 10
		require.True(t, ok)
		// 170 % 50 = 20
		require.Equal(t, uint64(20), r.B0_63)
	})

	t.Run("TwoStep", func(t *testing.T) {
		x := types.Decimal128{B0_63: 1}
		y := types.Decimal128{B0_63: 7}
		r, ok := d128ModDiffScaleXPow10(x, y, types.Pow10[10], true, types.Pow10[5])
		require.True(t, ok)
		_ = r
	})

	t.Run("Overflow", func(t *testing.T) {
		// Very large x that overflows when scaled
		x := types.Decimal128{B0_63: ^uint64(0), B64_127: 0x7FFFFFFFFFFFFFFF}
		y := types.Decimal128{B0_63: 3}
		_, ok := d128ModDiffScaleXPow10(x, y, types.Pow10[18], false, 0)
		require.False(t, ok)
	})
}

func TestD128Mod_NullPaths(t *testing.T) {
	rng := rand.New(rand.NewSource(9104))

	t.Run("SameScale_VecVec_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := makeNulls(testBatchSize)
		require.NoError(t, d128Mod(v1, v2, rs, 2, 2, nul, false))
	})

	t.Run("SameScale_ConstDiv_Nulls", func(t *testing.T) {
		vec := make([]types.Decimal128, testBatchSize)
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(9999) + 1)}}
		rs := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		nul := makeNulls(testBatchSize)
		require.NoError(t, d128Mod(vec, scalar, rs, 2, 2, nul, false))
	})

	t.Run("DiffScale_VecVec_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal128, testBatchSize)
		v2 := make([]types.Decimal128, testBatchSize)
		rs := make([]types.Decimal128, testBatchSize)
		for i := range v1 {
			v1[i] = types.Decimal128{B0_63: uint64(rng.Int63n(9999) + 1)}
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := makeNulls(testBatchSize)
		require.NoError(t, d128Mod(v1, v2, rs, 6, 2, nul, false))
	})

	t.Run("DiffScale_ConstDiv_Nulls", func(t *testing.T) {
		vec := make([]types.Decimal128, testBatchSize)
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}
		rs := make([]types.Decimal128, testBatchSize)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(9999) + 1)}
		}
		nul := makeNulls(testBatchSize)
		require.NoError(t, d128Mod(vec, scalar, rs, 6, 2, nul, false))
	})

	t.Run("LargeDivisor_SameScale_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal128, 8)
		v2 := make([]types.Decimal128, 8)
		rs := make([]types.Decimal128, 8)
		for i := range v1 {
			v1[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999999) + 1), B64_127: uint64(rng.Int63n(100))}
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1), B64_127: uint64(rng.Int63n(10) + 1)}
		}
		nul := makeNulls(8)
		require.NoError(t, d128Mod(v1, v2, rs, 2, 2, nul, false))
	})
}

// largeD128 creates a Decimal128 value that does NOT fit in int64 (B64_127 set beyond sign).
func largeD128(rng *rand.Rand) types.Decimal128 {
	return types.Decimal128{
		B0_63:   uint64(rng.Int63()),
		B64_127: uint64(rng.Int63n(100)) + 1,
	}
}

func TestD128IntDiv_LargeValues(t *testing.T) {
	rng := rand.New(rand.NewSource(9206))

	t.Run("VecVec_Large", func(t *testing.T) {
		v1 := make([]types.Decimal128, 32)
		v2 := make([]types.Decimal128, 32)
		rs := make([]int64, 32)
		for i := range v1 {
			v1[i] = types.Decimal128{B0_63: uint64(rng.Int63n(9999) + 1)}
			v2[i] = largeD128(rng)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128IntDiv(v1, v2, rs, 2, 2, nul, false))
	})

	t.Run("ConstDiv_Large", func(t *testing.T) {
		vec := make([]types.Decimal128, 32)
		scalar := []types.Decimal128{largeD128(rng)}
		rs := make([]int64, 32)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(9999) + 1)}
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128IntDiv(vec, scalar, rs, 2, 2, nul, false))
	})

	t.Run("ConstDividend_Large", func(t *testing.T) {
		scalar := []types.Decimal128{largeD128(rng)}
		vec := make([]types.Decimal128, 32)
		rs := make([]int64, 32)
		for i := range vec {
			vec[i] = largeD128(rng)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128IntDiv(scalar, vec, rs, 2, 2, nul, false))
	})

	t.Run("DiffScale_Large", func(t *testing.T) {
		v1 := make([]types.Decimal128, 32)
		v2 := make([]types.Decimal128, 32)
		rs := make([]int64, 32)
		for i := range v1 {
			v1[i] = types.Decimal128{B0_63: uint64(rng.Int63n(9999) + 1)}
			v2[i] = largeD128(rng)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128IntDiv(v1, v2, rs, 6, 2, nul, false))
	})
}

// ---- Coverage for scaleX=true paths in d64Mod (scale1 < scale2) ----

func TestD64Mod_ScaleXPath(t *testing.T) {
	rng := rand.New(rand.NewSource(9301))
	// scale1 < scale2 → scaleX = true
	s1, s2 := int32(2), int32(8)

	t.Run("VecVec_NoNull", func(t *testing.T) {
		v1 := make([]types.Decimal64, 32)
		v2 := make([]types.Decimal64, 32)
		rs := make([]types.Decimal64, 32)
		for i := range v1 {
			v1[i] = types.Decimal64(rng.Int63n(999) + 1)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d64Mod(v1, v2, rs, s1, s2, nul, false))
	})

	t.Run("VecVec_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal64, 32)
		v2 := make([]types.Decimal64, 32)
		rs := make([]types.Decimal64, 32)
		for i := range v1 {
			v1[i] = types.Decimal64(rng.Int63n(999) + 1)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64Mod(v1, v2, rs, s1, s2, nul, false))
	})

	t.Run("ConstLeft_NoNull", func(t *testing.T) {
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		vec := make([]types.Decimal64, 32)
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d64Mod(scalar, vec, rs, s1, s2, nul, false))
	})

	t.Run("ConstLeft_Nulls", func(t *testing.T) {
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		vec := make([]types.Decimal64, 32)
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64Mod(scalar, vec, rs, s1, s2, nul, false))
	})

	t.Run("ConstRight_NoNull", func(t *testing.T) {
		vec := make([]types.Decimal64, 32)
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d64Mod(vec, scalar, rs, s1, s2, nul, false))
	})

	t.Run("ConstRight_Nulls", func(t *testing.T) {
		vec := make([]types.Decimal64, 32)
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64Mod(vec, scalar, rs, s1, s2, nul, false))
	})
}

// ---- Coverage for !scaleX paths (scale1 > scale2) in d64Mod with all dispatch types ----

func TestD64IntDiv_NotCanInline(t *testing.T) {
	rng := rand.New(rand.NewSource(9303))
	// Use scale1=0, scale2=14 → scale=6 → scaleAdj=6+14=20 → !canInline
	s1, s2 := int32(0), int32(14)

	t.Run("VecVec_NoNull", func(t *testing.T) {
		v1 := make([]types.Decimal64, 32)
		v2 := make([]types.Decimal64, 32)
		rs := make([]int64, 32)
		for i := range v1 {
			v1[i] = types.Decimal64(rng.Int63n(999) + 1)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d64IntDiv(v1, v2, rs, s1, s2, nul, false))
	})

	t.Run("VecVec_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal64, 32)
		v2 := make([]types.Decimal64, 32)
		rs := make([]int64, 32)
		for i := range v1 {
			v1[i] = types.Decimal64(rng.Int63n(999) + 1)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64IntDiv(v1, v2, rs, s1, s2, nul, false))
	})

	t.Run("ConstLeft_NoNull", func(t *testing.T) {
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		vec := make([]types.Decimal64, 32)
		rs := make([]int64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d64IntDiv(scalar, vec, rs, s1, s2, nul, false))
	})

	t.Run("ConstLeft_Nulls", func(t *testing.T) {
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		vec := make([]types.Decimal64, 32)
		rs := make([]int64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64IntDiv(scalar, vec, rs, s1, s2, nul, false))
	})

	t.Run("ConstRight_NoNull", func(t *testing.T) {
		vec := make([]types.Decimal64, 32)
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		rs := make([]int64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d64IntDiv(vec, scalar, rs, s1, s2, nul, false))
	})

	t.Run("ConstRight_Nulls", func(t *testing.T) {
		vec := make([]types.Decimal64, 32)
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		rs := make([]int64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64IntDiv(vec, scalar, rs, s1, s2, nul, false))
	})
}

// ---- Coverage for d128Mod SameScale with large divisors (d128ModSameScale) ----

func TestD128Mod_SameScale_LargeDivisors(t *testing.T) {
	rng := rand.New(rand.NewSource(9304))

	t.Run("VecVec_LargeBoth", func(t *testing.T) {
		v1 := make([]types.Decimal128, 32)
		v2 := make([]types.Decimal128, 32)
		rs := make([]types.Decimal128, 32)
		for i := range v1 {
			v1[i] = largeD128(rng)
			v2[i] = largeD128(rng)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128Mod(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("ConstDiv_Large", func(t *testing.T) {
		vec := make([]types.Decimal128, 32)
		scalar := []types.Decimal128{largeD128(rng)}
		rs := make([]types.Decimal128, 32)
		for i := range vec {
			vec[i] = largeD128(rng)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128Mod(vec, scalar, rs, 4, 4, nul, false))
	})

	t.Run("ConstDividend_Large", func(t *testing.T) {
		scalar := []types.Decimal128{largeD128(rng)}
		vec := make([]types.Decimal128, 32)
		rs := make([]types.Decimal128, 32)
		for i := range vec {
			vec[i] = largeD128(rng)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128Mod(scalar, vec, rs, 4, 4, nul, false))
	})

	t.Run("VecVec_LargeBoth_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal128, 32)
		v2 := make([]types.Decimal128, 32)
		rs := make([]types.Decimal128, 32)
		for i := range v1 {
			v1[i] = largeD128(rng)
			v2[i] = largeD128(rng)
		}
		nul := makeNulls(32)
		require.NoError(t, d128Mod(v1, v2, rs, 4, 4, nul, false))
	})
}

// ---- Coverage for d128Mod diff-scale with large values ----

func TestD128Mod_DiffScale_AllDispatches(t *testing.T) {
	rng := rand.New(rand.NewSource(9305))

	t.Run("ConstDividend_DiffScale_Large", func(t *testing.T) {
		scalar := []types.Decimal128{largeD128(rng)}
		vec := make([]types.Decimal128, 32)
		rs := make([]types.Decimal128, 32)
		for i := range vec {
			vec[i] = largeD128(rng)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128Mod(scalar, vec, rs, 6, 2, nul, false))
	})

	t.Run("ConstDivisor_DiffScale_Large", func(t *testing.T) {
		vec := make([]types.Decimal128, 32)
		scalar := []types.Decimal128{largeD128(rng)}
		rs := make([]types.Decimal128, 32)
		for i := range vec {
			vec[i] = largeD128(rng)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128Mod(vec, scalar, rs, 6, 2, nul, false))
	})

	t.Run("VecVec_DiffScale_Large_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal128, 32)
		v2 := make([]types.Decimal128, 32)
		rs := make([]types.Decimal128, 32)
		for i := range v1 {
			v1[i] = largeD128(rng)
			v2[i] = largeD128(rng)
		}
		nul := makeNulls(32)
		require.NoError(t, d128Mod(v1, v2, rs, 6, 2, nul, false))
	})
}

func TestD128IntDiv_SameScale_AllDispatches(t *testing.T) {
	rng := rand.New(rand.NewSource(9307))

	t.Run("ConstDividend_NoNull", func(t *testing.T) {
		scalar := []types.Decimal128{randD128Small(rng)}
		vec := make([]types.Decimal128, 32)
		rs := make([]int64, 32)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128IntDiv(scalar, vec, rs, 4, 4, nul, false))
	})

	t.Run("ConstDividend_Large", func(t *testing.T) {
		scalar := []types.Decimal128{largeD128(rng)}
		vec := make([]types.Decimal128, 32)
		rs := make([]int64, 32)
		for i := range vec {
			vec[i] = largeD128(rng)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128IntDiv(scalar, vec, rs, 4, 4, nul, false))
	})

	t.Run("VecVec_NoNull", func(t *testing.T) {
		v1 := make([]types.Decimal128, 32)
		v2 := make([]types.Decimal128, 32)
		rs := make([]int64, 32)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecVec_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal128, 32)
		v2 := make([]types.Decimal128, 32)
		rs := make([]int64, 32)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := makeNulls(32)
		require.NoError(t, d128IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecVec_Large_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal128, 32)
		v2 := make([]types.Decimal128, 32)
		rs := make([]int64, 32)
		for i := range v1 {
			v1[i] = largeD128(rng)
			v2[i] = largeD128(rng)
		}
		nul := makeNulls(32)
		require.NoError(t, d128IntDiv(v1, v2, rs, 4, 4, nul, false))
	})
}

// ---- Coverage for d256Mul generic (non-allFitInt64) with scale adjustment ----

func hugeD256(rng *rand.Rand) types.Decimal256 {
	return types.Decimal256{
		B0_63:    uint64(rng.Int63()),
		B64_127:  uint64(rng.Int63()),
		B128_191: uint64(rng.Int63n(100)) + 1,
		B192_255: 0,
	}
}

func TestD256IntDiv_GenericSlowPath(t *testing.T) {
	rng := rand.New(rand.NewSource(9402))

	t.Run("VecVec_Huge", func(t *testing.T) {
		v1 := make([]types.Decimal256, 8)
		v2 := make([]types.Decimal256, 8)
		rs := make([]int64, 8)
		for i := range v1 {
			v1[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
			v2[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1), B64_127: 0, B128_191: uint64(rng.Int63n(10) + 1)}
		}
		nul := nulls.NewWithSize(8)
		_ = d256IntDiv(v1, v2, rs, 2, 2, nul, false)
	})

	t.Run("ConstRight_Huge", func(t *testing.T) {
		vec := make([]types.Decimal256, 8)
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1), B64_127: 0, B128_191: uint64(rng.Int63n(10) + 1)}}
		rs := make([]int64, 8)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(8)
		_ = d256IntDiv(vec, scalar, rs, 2, 2, nul, false)
	})

	t.Run("ConstLeft_Huge", func(t *testing.T) {
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}
		vec := make([]types.Decimal256, 8)
		rs := make([]int64, 8)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1), B64_127: 0, B128_191: uint64(rng.Int63n(10) + 1)}
		}
		nul := nulls.NewWithSize(8)
		_ = d256IntDiv(scalar, vec, rs, 2, 2, nul, false)
	})
}

// ---- ViaD128 with !canInline (scaleAdj > 19) ----

func TestD256IntDivViaD128_AllPaths(t *testing.T) {
	rng := rand.New(rand.NewSource(9405))

	t.Run("VecVec_NoNull", func(t *testing.T) {
		v1 := make([]types.Decimal256, 16)
		v2 := make([]types.Decimal256, 16)
		rs := make([]int64, 16)
		for i := range v1 {
			v1[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
			v2[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(16)
		require.NoError(t, d256IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecVec_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal256, 16)
		v2 := make([]types.Decimal256, 16)
		rs := make([]int64, 16)
		for i := range v1 {
			v1[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
			v2[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := makeNulls(16)
		require.NoError(t, d256IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("ConstLeft_NoNull", func(t *testing.T) {
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}
		vec := make([]types.Decimal256, 16)
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(16)
		require.NoError(t, d256IntDiv(scalar, vec, rs, 4, 4, nul, false))
	})

	t.Run("ConstLeft_Nulls", func(t *testing.T) {
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}
		vec := make([]types.Decimal256, 16)
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := makeNulls(16)
		require.NoError(t, d256IntDiv(scalar, vec, rs, 4, 4, nul, false))
	})

	t.Run("ConstRight_NoNull", func(t *testing.T) {
		vec := make([]types.Decimal256, 16)
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(16)
		require.NoError(t, d256IntDiv(vec, scalar, rs, 4, 4, nul, false))
	})

	t.Run("ConstRight_Nulls", func(t *testing.T) {
		vec := make([]types.Decimal256, 16)
		scalar := []types.Decimal256{{B0_63: uint64(rng.Int63n(999) + 1)}}
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := makeNulls(16)
		require.NoError(t, d256IntDiv(vec, scalar, rs, 4, 4, nul, false))
	})

	t.Run("DiffScale_VecVec_NoNull", func(t *testing.T) {
		v1 := make([]types.Decimal256, 16)
		v2 := make([]types.Decimal256, 16)
		rs := make([]int64, 16)
		for i := range v1 {
			v1[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
			v2[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(16)
		require.NoError(t, d256IntDiv(v1, v2, rs, 6, 2, nul, false))
	})

	t.Run("DiffScale_VecVec_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal256, 16)
		v2 := make([]types.Decimal256, 16)
		rs := make([]int64, 16)
		for i := range v1 {
			v1[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
			v2[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := makeNulls(16)
		require.NoError(t, d256IntDiv(v1, v2, rs, 6, 2, nul, false))
	})
}

// ---- d128Mod remaining dispatch paths ----

func TestD128Mod_AllDispatches_Extra(t *testing.T) {
	rng := rand.New(rand.NewSource(9406))

	t.Run("SameScale_ConstDividend_NoNull", func(t *testing.T) {
		scalar := []types.Decimal128{randD128Small(rng)}
		vec := make([]types.Decimal128, 32)
		rs := make([]types.Decimal128, 32)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128Mod(scalar, vec, rs, 4, 4, nul, false))
	})

	t.Run("SameScale_ConstDividend_Nulls", func(t *testing.T) {
		scalar := []types.Decimal128{randD128Small(rng)}
		vec := make([]types.Decimal128, 32)
		rs := make([]types.Decimal128, 32)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := makeNulls(32)
		require.NoError(t, d128Mod(scalar, vec, rs, 4, 4, nul, false))
	})

	t.Run("SameScale_ConstDivisor_NoNull", func(t *testing.T) {
		vec := make([]types.Decimal128, 32)
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}
		rs := make([]types.Decimal128, 32)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128Mod(vec, scalar, rs, 4, 4, nul, false))
	})

	t.Run("SameScale_VecVec_NoNull", func(t *testing.T) {
		v1 := make([]types.Decimal128, 32)
		v2 := make([]types.Decimal128, 32)
		rs := make([]types.Decimal128, 32)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128Mod(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("DiffScale_ConstDividend_NoNull", func(t *testing.T) {
		scalar := []types.Decimal128{randD128Small(rng)}
		vec := make([]types.Decimal128, 32)
		rs := make([]types.Decimal128, 32)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128Mod(scalar, vec, rs, 6, 2, nul, false))
	})

	t.Run("DiffScale_ConstDividend_Nulls", func(t *testing.T) {
		scalar := []types.Decimal128{randD128Small(rng)}
		vec := make([]types.Decimal128, 32)
		rs := make([]types.Decimal128, 32)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := makeNulls(32)
		require.NoError(t, d128Mod(scalar, vec, rs, 6, 2, nul, false))
	})

	t.Run("DiffScale_ConstDivisor_NoNull", func(t *testing.T) {
		vec := make([]types.Decimal128, 32)
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}
		rs := make([]types.Decimal128, 32)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d128Mod(vec, scalar, rs, 6, 2, nul, false))
	})
}

// ---- d64Mod same-scale all dispatches ----

func TestD64Mod_SameScale_AllDispatches(t *testing.T) {
	rng := rand.New(rand.NewSource(9407))

	t.Run("ConstLeft_NoNull", func(t *testing.T) {
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(99999) + 1)}
		vec := make([]types.Decimal64, 32)
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d64Mod(scalar, vec, rs, 4, 4, nul, false))
	})

	t.Run("ConstLeft_Nulls", func(t *testing.T) {
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(99999) + 1)}
		vec := make([]types.Decimal64, 32)
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64Mod(scalar, vec, rs, 4, 4, nul, false))
	})

	t.Run("ConstRight_NoNull", func(t *testing.T) {
		vec := make([]types.Decimal64, 32)
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(99999) + 1)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d64Mod(vec, scalar, rs, 4, 4, nul, false))
	})

	t.Run("ConstRight_Nulls", func(t *testing.T) {
		vec := make([]types.Decimal64, 32)
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(99999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64Mod(vec, scalar, rs, 4, 4, nul, false))
	})
}

// ---- d128IntDiv: remaining dispatch paths ----

func TestD128IntDiv_AllDispatches_Extra(t *testing.T) {
	rng := rand.New(rand.NewSource(9505))

	t.Run("DiffScale_ConstLeft_NoNull", func(t *testing.T) {
		scalar := []types.Decimal128{randD128Small(rng)}
		vec := make([]types.Decimal128, 16)
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(16)
		require.NoError(t, d128IntDiv(scalar, vec, rs, 6, 2, nul, false))
	})

	t.Run("DiffScale_ConstLeft_Nulls", func(t *testing.T) {
		scalar := []types.Decimal128{randD128Small(rng)}
		vec := make([]types.Decimal128, 16)
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := makeNulls(16)
		require.NoError(t, d128IntDiv(scalar, vec, rs, 6, 2, nul, false))
	})

	t.Run("DiffScale_ConstRight_NoNull", func(t *testing.T) {
		vec := make([]types.Decimal128, 16)
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		nul := nulls.NewWithSize(16)
		require.NoError(t, d128IntDiv(vec, scalar, rs, 6, 2, nul, false))
	})

	t.Run("DiffScale_ConstRight_Nulls", func(t *testing.T) {
		vec := make([]types.Decimal128, 16)
		scalar := []types.Decimal128{{B0_63: uint64(rng.Int63n(999) + 1)}}
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		nul := makeNulls(16)
		require.NoError(t, d128IntDiv(vec, scalar, rs, 6, 2, nul, false))
	})

	t.Run("DiffScale_VecVec_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal128, 16)
		v2 := make([]types.Decimal128, 16)
		rs := make([]int64, 16)
		for i := range v1 {
			v1[i] = randD128Small(rng)
			v2[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := makeNulls(16)
		require.NoError(t, d128IntDiv(v1, v2, rs, 6, 2, nul, false))
	})

	t.Run("SameScale_ConstLeft_NoNull", func(t *testing.T) {
		scalar := []types.Decimal128{randD128Small(rng)}
		vec := make([]types.Decimal128, 16)
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := nulls.NewWithSize(16)
		require.NoError(t, d128IntDiv(scalar, vec, rs, 4, 4, nul, false))
	})

	t.Run("SameScale_ConstLeft_Nulls", func(t *testing.T) {
		scalar := []types.Decimal128{randD128Small(rng)}
		vec := make([]types.Decimal128, 16)
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = types.Decimal128{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		nul := makeNulls(16)
		require.NoError(t, d128IntDiv(scalar, vec, rs, 4, 4, nul, false))
	})
}

// ---- d64IntDiv: remaining dispatch paths ----

func TestD64IntDiv_ConstPaths(t *testing.T) {
	rng := rand.New(rand.NewSource(9506))

	t.Run("ConstLeft_NoNull", func(t *testing.T) {
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		vec := make([]types.Decimal64, 16)
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(16)
		require.NoError(t, d64IntDiv(scalar, vec, rs, 4, 4, nul, false))
	})

	t.Run("ConstLeft_Nulls", func(t *testing.T) {
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		vec := make([]types.Decimal64, 16)
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(16)
		require.NoError(t, d64IntDiv(scalar, vec, rs, 4, 4, nul, false))
	})

	t.Run("ConstRight_Nulls", func(t *testing.T) {
		vec := make([]types.Decimal64, 16)
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		rs := make([]int64, 16)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(16)
		require.NoError(t, d64IntDiv(vec, scalar, rs, 4, 4, nul, false))
	})

	t.Run("VecVec_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		v2 := make([]types.Decimal64, 16)
		rs := make([]int64, 16)
		for i := range v1 {
			v1[i] = types.Decimal64(rng.Int63n(999) + 1)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(16)
		require.NoError(t, d64IntDiv(v1, v2, rs, 4, 4, nul, false))
	})
}

// ---- d64Mod: scaleX const-left/right, !scaleX const-left ----

func TestD64Mod_ConstPaths_Extra(t *testing.T) {
	rng := rand.New(rand.NewSource(9507))

	// scaleX path (scale1 < scale2): const-left with nulls
	t.Run("ScaleX_ConstLeft_Nulls", func(t *testing.T) {
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(99999) + 1)}
		vec := make([]types.Decimal64, 32)
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64Mod(scalar, vec, rs, 2, 8, nul, false))
	})

	// scaleX path: const-right with nulls
	t.Run("ScaleX_ConstRight_Nulls", func(t *testing.T) {
		vec := make([]types.Decimal64, 32)
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(99999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64Mod(vec, scalar, rs, 2, 8, nul, false))
	})

	// !scaleX path: const-left no-null
	t.Run("NotScaleX_ConstLeft_NoNull", func(t *testing.T) {
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(99999) + 1)}
		vec := make([]types.Decimal64, 32)
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := nulls.NewWithSize(32)
		require.NoError(t, d64Mod(scalar, vec, rs, 8, 2, nul, false))
	})

	// !scaleX path: const-left with nulls
	t.Run("NotScaleX_ConstLeft_Nulls", func(t *testing.T) {
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(99999) + 1)}
		vec := make([]types.Decimal64, 32)
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64Mod(scalar, vec, rs, 8, 2, nul, false))
	})

	// !scaleX path: const-right with nulls
	t.Run("NotScaleX_ConstRight_Nulls", func(t *testing.T) {
		vec := make([]types.Decimal64, 32)
		scalar := []types.Decimal64{types.Decimal64(rng.Int63n(999) + 1)}
		rs := make([]types.Decimal64, 32)
		for i := range vec {
			vec[i] = types.Decimal64(rng.Int63n(99999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64Mod(vec, scalar, rs, 8, 2, nul, false))
	})

	// !scaleX path: vec-vec with nulls
	t.Run("NotScaleX_VecVec_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal64, 32)
		v2 := make([]types.Decimal64, 32)
		rs := make([]types.Decimal64, 32)
		for i := range v1 {
			v1[i] = types.Decimal64(rng.Int63n(99999) + 1)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(32)
		require.NoError(t, d64Mod(v1, v2, rs, 8, 2, nul, false))
	})
}

// TestMiscEdgePaths covers the handful of statements only reachable via
// specific edge-case dispatch combos (zero-const-divisor with shouldError,
// and d64Mod same-scale vec×vec with nulls).
func TestMiscEdgePaths(t *testing.T) {
	rng := rand.New(rand.NewSource(9600))
	zero128 := types.Decimal128{}

	t.Run("D128IntDiv_ZeroConst_ShouldError", func(t *testing.T) {
		vec := make([]types.Decimal128, 4)
		for i := range vec {
			vec[i] = randD128Small(rng)
		}
		rs := make([]int64, 4)
		nul := nulls.NewWithSize(4)
		require.Error(t, d128IntDiv(vec, []types.Decimal128{zero128}, rs, 4, 4, nul, true))
	})

	t.Run("D256IntDiv_ZeroConst_ShouldError", func(t *testing.T) {
		vec := make([]types.Decimal256, 4)
		for i := range vec {
			vec[i] = types.Decimal256{B0_63: uint64(rng.Int63n(999) + 1)}
		}
		rs := make([]int64, 4)
		nul := nulls.NewWithSize(4)
		require.Error(t, d256IntDiv(vec, []types.Decimal256{{}}, rs, 4, 4, nul, true))
	})

	t.Run("D64Mod_SameScale_VecVec_Nulls", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		v2 := make([]types.Decimal64, 16)
		rs := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = types.Decimal64(rng.Int63n(99999) + 1)
			v2[i] = types.Decimal64(rng.Int63n(999) + 1)
		}
		nul := makeNulls(16)
		require.NoError(t, d64Mod(v1, v2, rs, 4, 4, nul, false))
	})
}

// =============================================================================
// Block-coverage tests: target uncovered dispatch branches
// =============================================================================

// TestD128IntDiv_DivByZeroPaths covers div-by-zero across dispatch variants.
func TestD128IntDiv_DivByZeroPaths(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	makeVecWithZeros := func(n int) []types.Decimal128 {
		v := make([]types.Decimal128, n)
		for i := range v {
			if i%3 == 1 {
				v[i] = types.Decimal128{} // zero
			} else {
				v[i] = randD128(rng)
				if d128IsZero(v[i]) {
					v[i].B0_63 = 1
				}
			}
		}
		return v
	}

	t.Run("VecVec_DivByZero", func(t *testing.T) {
		v1 := make([]types.Decimal128, 16)
		for i := range v1 {
			v1[i] = randD128(rng)
		}
		v2 := makeVecWithZeros(16)
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("ConstVec_DivByZero", func(t *testing.T) {
		v1 := []types.Decimal128{randD128(rng)}
		v2 := makeVecWithZeros(16)
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("ConstVec_DivByZero_WithNull", func(t *testing.T) {
		v1 := []types.Decimal128{randD128(rng)}
		v2 := makeVecWithZeros(16)
		rs := make([]int64, 16)
		nul := makeNulls(16)
		require.NoError(t, d128IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecConst_ZeroDivisor", func(t *testing.T) {
		v1 := make([]types.Decimal128, 16)
		for i := range v1 {
			v1[i] = randD128(rng)
		}
		v2 := []types.Decimal128{{}}
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("HighScale_ScaleLtScale1", func(t *testing.T) {
		v1 := make([]types.Decimal128, 8)
		v2 := make([]types.Decimal128, 8)
		for i := range v1 {
			v1[i] = randD128(rng)
			v2[i] = randD128(rng)
			if d128IsZero(v2[i]) {
				v2[i].B0_63 = 1
			}
		}
		rs := make([]int64, 8)
		nul := &nulls.Nulls{}
		require.NoError(t, d128IntDiv(v1, v2, rs, 18, 2, nul, false))
	})
}

// TestD128Mod_DivByZeroPaths covers modulo div-by-zero in various dispatch paths.
func TestD128Mod_DivByZeroPaths(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	makeVecWithZeros := func(n int) []types.Decimal128 {
		v := make([]types.Decimal128, n)
		for i := range v {
			if i%3 == 1 {
				v[i] = types.Decimal128{} // zero
			} else {
				v[i] = randD128(rng)
				if d128IsZero(v[i]) {
					v[i].B0_63 = 1
				}
			}
		}
		return v
	}

	t.Run("VecVec_DiffScale_DivByZero", func(t *testing.T) {
		v1 := make([]types.Decimal128, 16)
		for i := range v1 {
			v1[i] = randD128(rng)
		}
		v2 := makeVecWithZeros(16)
		rs := make([]types.Decimal128, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 4, 6, nul, false))
	})

	t.Run("VecVec_DiffScale_DivByZero_WithNull", func(t *testing.T) {
		v1 := make([]types.Decimal128, 16)
		for i := range v1 {
			v1[i] = randD128(rng)
		}
		v2 := makeVecWithZeros(16)
		rs := make([]types.Decimal128, 16)
		nul := makeNulls(16)
		require.NoError(t, d128Mod(v1, v2, rs, 4, 6, nul, false))
	})

	t.Run("ConstVec_DiffScale_DivByZero", func(t *testing.T) {
		v1 := []types.Decimal128{randD128(rng)}
		v2 := makeVecWithZeros(16)
		rs := make([]types.Decimal128, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 4, 6, nul, false))
	})

	t.Run("VecConst_ZeroDivisor", func(t *testing.T) {
		v1 := make([]types.Decimal128, 16)
		for i := range v1 {
			v1[i] = randD128(rng)
		}
		v2 := []types.Decimal128{{}}
		rs := make([]types.Decimal128, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 4, 6, nul, false))
	})

	t.Run("VecConst_SameScale_ZeroDivisor", func(t *testing.T) {
		v1 := make([]types.Decimal128, 16)
		for i := range v1 {
			v1[i] = randD128(rng)
		}
		v2 := []types.Decimal128{{}}
		rs := make([]types.Decimal128, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecVec_SameScale_DivByZero", func(t *testing.T) {
		v1 := make([]types.Decimal128, 16)
		for i := range v1 {
			v1[i] = randD128(rng)
		}
		v2 := makeVecWithZeros(16)
		rs := make([]types.Decimal128, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 4, 4, nul, false))
	})
}

// TestD64Mod_DivByZeroPaths covers d64 modulo div-by-zero paths.
func TestD64Mod_DivByZeroPaths(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	makeVecWithZeros := func(n int) []types.Decimal64 {
		v := make([]types.Decimal64, n)
		for i := range v {
			if i%3 == 1 {
				v[i] = 0
			} else {
				v[i] = randD64(rng)
				if v[i] == 0 {
					v[i] = 1
				}
			}
		}
		return v
	}

	t.Run("VecVec_SameScale_DivByZero", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = randD64(rng)
		}
		v2 := makeVecWithZeros(16)
		rs := make([]types.Decimal64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecVec_DiffScale_DivByZero", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = randD64(rng)
		}
		v2 := makeVecWithZeros(16)
		rs := make([]types.Decimal64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 4, 6, nul, false))
	})

	t.Run("VecVec_DiffScale_DivByZero_WithNull", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = randD64(rng)
		}
		v2 := makeVecWithZeros(16)
		rs := make([]types.Decimal64, 16)
		nul := makeNulls(16)
		require.NoError(t, d64Mod(v1, v2, rs, 4, 6, nul, false))
	})

	t.Run("ConstVec_DivByZero", func(t *testing.T) {
		v1 := []types.Decimal64{randD64(rng)}
		v2 := makeVecWithZeros(16)
		rs := make([]types.Decimal64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecConst_ZeroDivisor", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = randD64(rng)
		}
		v2 := []types.Decimal64{0}
		rs := make([]types.Decimal64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("ScaleX_DivByZero", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = randD64(rng)
		}
		v2 := makeVecWithZeros(16)
		rs := make([]types.Decimal64, 16)
		nul := &nulls.Nulls{}
		// scale1 < scale2 → scaleX path
		require.NoError(t, d64Mod(v1, v2, rs, 2, 6, nul, false))
	})

	t.Run("ScaleX_DivByZero_WithNull", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = randD64(rng)
		}
		v2 := makeVecWithZeros(16)
		rs := make([]types.Decimal64, 16)
		nul := makeNulls(16)
		require.NoError(t, d64Mod(v1, v2, rs, 2, 6, nul, false))
	})
}

// TestD64IntDiv_DivByZeroPaths covers d64 integer division div-by-zero paths.
func TestD64IntDiv_DivByZeroPaths(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	makeVecWithZeros := func(n int) []types.Decimal64 {
		v := make([]types.Decimal64, n)
		for i := range v {
			if i%3 == 1 {
				v[i] = 0
			} else {
				v[i] = randD64(rng)
				if v[i] == 0 {
					v[i] = 1
				}
			}
		}
		return v
	}

	t.Run("VecVec_DivByZero", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = randD64(rng)
		}
		v2 := makeVecWithZeros(16)
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecVec_DivByZero_WithNull", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = randD64(rng)
		}
		v2 := makeVecWithZeros(16)
		rs := make([]int64, 16)
		nul := makeNulls(16)
		require.NoError(t, d64IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("ConstVec_DivByZero", func(t *testing.T) {
		v1 := []types.Decimal64{randD64(rng)}
		v2 := makeVecWithZeros(16)
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("ConstVec_DivByZero_WithNull", func(t *testing.T) {
		v1 := []types.Decimal64{randD64(rng)}
		v2 := makeVecWithZeros(16)
		rs := make([]int64, 16)
		nul := makeNulls(16)
		require.NoError(t, d64IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecConst_ZeroDivisor", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = randD64(rng)
		}
		v2 := []types.Decimal64{0}
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("HighScale_ScaleLtScale1", func(t *testing.T) {
		v1 := make([]types.Decimal64, 8)
		v2 := make([]types.Decimal64, 8)
		for i := range v1 {
			v1[i] = randD64(rng)
			v2[i] = randD64(rng)
			if v2[i] == 0 {
				v2[i] = 1
			}
		}
		rs := make([]int64, 8)
		nul := &nulls.Nulls{}
		require.NoError(t, d64IntDiv(v1, v2, rs, 18, 2, nul, false))
	})
}

// TestD256Mod_DivByZeroPaths covers D256 mod div-by-zero in various dispatch paths.
func TestD256Mod_DivByZeroPaths(t *testing.T) {
	type decimal = types.Decimal256
	for _, tc := range []struct {
		name       string
		x, y, want []decimal
		s1, s2     int32
		masked     []uint64
		errorCode  uint16
		permissive bool
	}{
		{
			name:      "narrow_inline_VV_strict",
			x:         []decimal{{B0_63: 100}, {B0_63: 101}},
			y:         []decimal{{B0_63: 3}, {}},
			want:      []decimal{{B0_63: 1}, {}},
			s1:        0,
			s2:        0,
			errorCode: moerr.ErrDivByZero,
		},

		{
			name:       "narrow_inline_VV_permissive",
			x:          []decimal{{B0_63: 101}, {B0_63: 100}},
			y:          []decimal{{}, {B0_63: 3}},
			want:       []decimal{{}, {B0_63: 1}},
			s1:         0,
			s2:         0,
			permissive: true,
		},

		{
			name:   "narrow_inline_VV_masked_zero",
			x:      []decimal{{B0_63: 101}, {B0_63: 100}},
			y:      []decimal{{}, {B0_63: 3}},
			want:   []decimal{{}, {B0_63: 1}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:      "narrow_inline_SV_strict",
			x:         []decimal{{B0_63: 100}},
			y:         []decimal{{B0_63: 3}, {}},
			want:      []decimal{{B0_63: 1}, {}},
			s1:        0,
			s2:        0,
			errorCode: moerr.ErrDivByZero,
		},

		{
			name:       "narrow_inline_SV_permissive",
			x:          []decimal{{B0_63: 100}},
			y:          []decimal{{}, {B0_63: 3}},
			want:       []decimal{{}, {B0_63: 1}},
			s1:         0,
			s2:         0,
			permissive: true,
		},

		{
			name:   "narrow_inline_SV_masked_zero",
			x:      []decimal{{B0_63: 100}},
			y:      []decimal{{}, {B0_63: 3}},
			want:   []decimal{{}, {B0_63: 1}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:      "narrow_general_VV_strict",
			x:         []decimal{{B0_63: 100, B64_127: 2}, {B0_63: 11}},
			y:         []decimal{{B0_63: 7, B64_127: 1}, {}},
			want:      []decimal{{B0_63: 86}, {}},
			s1:        0,
			s2:        0,
			errorCode: moerr.ErrDivByZero,
		},

		{
			name:       "narrow_general_VV_permissive",
			x:          []decimal{{B0_63: 11}, {B0_63: 100, B64_127: 2}},
			y:          []decimal{{}, {B0_63: 7, B64_127: 1}},
			want:       []decimal{{}, {B0_63: 86}},
			s1:         0,
			s2:         0,
			permissive: true,
		},

		{
			name:   "narrow_general_VV_masked_zero",
			x:      []decimal{{B0_63: 11}, {B0_63: 100, B64_127: 2}},
			y:      []decimal{{}, {B0_63: 7, B64_127: 1}},
			want:   []decimal{{}, {B0_63: 86}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:      "narrow_general_SV_strict",
			x:         []decimal{{B0_63: 100, B64_127: 2}},
			y:         []decimal{{B0_63: 7, B64_127: 1}, {}},
			want:      []decimal{{B0_63: 86}, {}},
			s1:        0,
			s2:        0,
			errorCode: moerr.ErrDivByZero,
		},

		{
			name:       "narrow_general_SV_permissive",
			x:          []decimal{{B0_63: 100, B64_127: 2}},
			y:          []decimal{{}, {B0_63: 7, B64_127: 1}},
			want:       []decimal{{}, {B0_63: 86}},
			s1:         0,
			s2:         0,
			permissive: true,
		},

		{
			name:   "narrow_general_SV_masked_zero",
			x:      []decimal{{B0_63: 100, B64_127: 2}},
			y:      []decimal{{}, {B0_63: 7, B64_127: 1}},
			want:   []decimal{{}, {B0_63: 86}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:      "narrow_scalar_strict",
			x:         []decimal{{B0_63: 100}, {B0_63: 101}},
			y:         []decimal{{}},
			want:      []decimal{{}, {}},
			s1:        0,
			s2:        0,
			errorCode: moerr.ErrDivByZero,
		},
		{
			name:       "narrow_scalar_permissive",
			x:          []decimal{{B0_63: 100}, {B0_63: 101}},
			y:          []decimal{{}},
			want:       []decimal{{}, {}},
			s1:         0,
			s2:         0,
			permissive: true,
		},
		{
			name:      "generic_VV_strict",
			x:         []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}, {B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			y:         []decimal{{B0_63: 17}, {}},
			want:      []decimal{{B0_63: 3}, {}},
			s1:        0,
			s2:        0,
			errorCode: moerr.ErrDivByZero,
		},

		{
			name:       "generic_VV_permissive",
			x:          []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}, {B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			y:          []decimal{{}, {B0_63: 17}},
			want:       []decimal{{}, {B0_63: 3}},
			s1:         0,
			s2:         0,
			permissive: true,
		},

		{
			name:   "generic_VV_masked_zero",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}, {B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			y:      []decimal{{}, {B0_63: 17}},
			want:   []decimal{{}, {B0_63: 3}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:      "generic_SV_strict",
			x:         []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			y:         []decimal{{B0_63: 17}, {}},
			want:      []decimal{{B0_63: 3}, {}},
			s1:        0,
			s2:        0,
			errorCode: moerr.ErrDivByZero,
		},

		{
			name:       "generic_SV_permissive",
			x:          []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			y:          []decimal{{}, {B0_63: 17}},
			want:       []decimal{{}, {B0_63: 3}},
			s1:         0,
			s2:         0,
			permissive: true,
		},

		{
			name:   "generic_SV_masked_zero",
			x:      []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}},
			y:      []decimal{{}, {B0_63: 17}},
			want:   []decimal{{}, {B0_63: 3}},
			s1:     0,
			s2:     0,
			masked: []uint64{0},
		},
		{
			name:      "generic_scalar_strict",
			x:         []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}, {B0_63: 0, B64_127: 2, B128_191: 1, B192_255: 1}},
			y:         []decimal{{}},
			want:      []decimal{{}, {}},
			s1:        0,
			s2:        0,
			errorCode: moerr.ErrDivByZero,
		},
		{
			name:       "generic_scalar_permissive",
			x:          []decimal{{B0_63: ^uint64(0), B64_127: 1, B128_191: 1, B192_255: 1}, {B0_63: 0, B64_127: 2, B128_191: 1, B192_255: 1}},
			y:          []decimal{{}},
			want:       []decimal{{}, {}},
			s1:         0,
			s2:         0,
			permissive: true,
		},
		{
			name:   "all_NULL_vector_zero",
			x:      []decimal{{B0_63: 100}, {B0_63: 101}},
			y:      []decimal{{}, {}},
			want:   []decimal{{}, {}},
			s1:     0,
			s2:     0,
			masked: []uint64{0, 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nul := nulls.NewWithSize(len(tc.want))
			for _, i := range tc.masked {
				nul.Add(i)
			}
			got := make([]decimal, len(tc.want))
			err := d256Mod(tc.x, tc.y, got, tc.s1, tc.s2, nul, !tc.permissive)
			if tc.errorCode != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.errorCode), "unexpected error: %v", err)
				return
			}
			require.NoError(t, err)
			expectedNull := make(map[uint64]bool, len(tc.masked))
			for _, i := range tc.masked {
				expectedNull[i] = true
			}
			if tc.permissive {
				for i := range got {
					if tc.y[0] == (decimal{}) && len(tc.y) == 1 || len(tc.y) > 1 && tc.y[i] == (decimal{}) {
						expectedNull[uint64(i)] = true
					}
				}
			}
			require.Equal(t, len(expectedNull), nul.Count())
			for i := range got {
				require.Equal(t, expectedNull[uint64(i)], nul.Contains(uint64(i)), "NULL row %d", i)
				if !expectedNull[uint64(i)] {
					require.Equal(t, tc.want[i], got[i], "coefficient row %d", i)
				}
			}
		})
	}
}

// TestD256IntDiv_DivByZeroPaths covers D256 integer division div-by-zero paths.
func TestD256IntDiv_DivByZeroPaths(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	makeVecWithZeros := func(n int) []types.Decimal256 {
		v := make([]types.Decimal256, n)
		for i := range v {
			if i%3 == 1 {
				v[i] = types.Decimal256{}
			} else {
				v[i] = randD256Small(rng)
			}
		}
		return v
	}

	t.Run("ViaD128_VecVec_DivByZero", func(t *testing.T) {
		v1 := make([]types.Decimal256, 16)
		for i := range v1 {
			v1[i] = randD256Small(rng)
		}
		v2 := makeVecWithZeros(16)
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, false, 4, 4, !nul.IsEmpty(), nul.GetBitmap()))
	})

	t.Run("ViaD128_ConstVec_DivByZero", func(t *testing.T) {
		v1 := []types.Decimal256{randD256Small(rng)}
		v2 := makeVecWithZeros(16)
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, false, 4, 4, !nul.IsEmpty(), nul.GetBitmap()))
	})

	t.Run("ViaD128_ConstVec_DivByZero_WithNull", func(t *testing.T) {
		v1 := []types.Decimal256{randD256Small(rng)}
		v2 := makeVecWithZeros(16)
		rs := make([]int64, 16)
		nul := makeNulls(16)
		require.NoError(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, false, 4, 4, !nul.IsEmpty(), nul.GetBitmap()))
	})

	t.Run("ViaD128_VecConst_ZeroDivisor", func(t *testing.T) {
		v1 := make([]types.Decimal256, 16)
		for i := range v1 {
			v1[i] = randD256Small(rng)
		}
		v2 := []types.Decimal256{{}}
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, false, 4, 4, !nul.IsEmpty(), nul.GetBitmap()))
	})

	t.Run("Generic_VecConst_ZeroDivisor", func(t *testing.T) {
		v1 := make([]types.Decimal256, 16)
		for i := range v1 {
			v1[i] = hugeD256(rng)
		}
		v2 := []types.Decimal256{{}}
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d256IntDiv(v1, v2, rs, 4, 4, nul, false))
	})
}

// TestD64Mod_ConstAndScalePaths covers d64 mod const and various scale dispatch paths.
func TestD64Mod_ConstAndScalePaths(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	t.Run("ConstVec_ScaleX", func(t *testing.T) {
		v1 := []types.Decimal64{randD64(rng)}
		v2 := make([]types.Decimal64, 16)
		for i := range v2 {
			v2[i] = randD64(rng)
			if v2[i] == 0 {
				v2[i] = 1
			}
		}
		rs := make([]types.Decimal64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 2, 6, nul, false))
	})

	t.Run("VecConst_ScaleX", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = randD64(rng)
		}
		v2 := []types.Decimal64{types.Decimal64(uint64(rng.Intn(1000)) + 1)}
		rs := make([]types.Decimal64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 2, 6, nul, false))
	})

	t.Run("ConstVec_DiffScale_NotScaleX", func(t *testing.T) {
		v1 := []types.Decimal64{randD64(rng)}
		v2 := make([]types.Decimal64, 16)
		for i := range v2 {
			v2[i] = randD64(rng)
			if v2[i] == 0 {
				v2[i] = 1
			}
		}
		rs := make([]types.Decimal64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 6, 2, nul, false))
	})

	t.Run("VecConst_DiffScale_NotScaleX", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = randD64(rng)
		}
		v2 := []types.Decimal64{types.Decimal64(uint64(rng.Intn(1000)) + 1)}
		rs := make([]types.Decimal64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 6, 2, nul, false))
	})

	t.Run("ConstVec_SameScale", func(t *testing.T) {
		v1 := []types.Decimal64{randD64(rng)}
		v2 := make([]types.Decimal64, 16)
		for i := range v2 {
			v2[i] = randD64(rng)
			if v2[i] == 0 {
				v2[i] = 1
			}
		}
		rs := make([]types.Decimal64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 4, 4, nul, false))
	})
}

// TestD64IntDiv_InlineFallbackPaths covers d64 intdiv inline fallback paths.
func TestD64IntDiv_InlineFallbackPaths(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	t.Run("VecVec_LargeValues", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		v2 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = types.Decimal64(uint64(rng.Int63n(1e18)))
			v2[i] = types.Decimal64(uint64(rng.Intn(1000)) + 1)
		}
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64IntDiv(v1, v2, rs, 10, 2, nul, false))
	})

	t.Run("ConstVec_LargeValues", func(t *testing.T) {
		v1 := []types.Decimal64{types.Decimal64(uint64(rng.Int63n(1e18)))}
		v2 := make([]types.Decimal64, 16)
		for i := range v2 {
			v2[i] = types.Decimal64(uint64(rng.Intn(1000)) + 1)
		}
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64IntDiv(v1, v2, rs, 10, 2, nul, false))
	})

	t.Run("VecConst_LargeValues", func(t *testing.T) {
		v1 := make([]types.Decimal64, 16)
		for i := range v1 {
			v1[i] = types.Decimal64(uint64(rng.Int63n(1e18)))
		}
		v2 := []types.Decimal64{types.Decimal64(uint64(rng.Intn(1000)) + 1)}
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d64IntDiv(v1, v2, rs, 10, 2, nul, false))
	})
}

// TestD128Mod_ConstAndLargeScalePaths covers d128 mod const-vec and vec-const paths with DiffScale.
func TestD128Mod_ConstAndLargeScalePaths(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	t.Run("ConstVec_DiffScale_ScaleX", func(t *testing.T) {
		v1 := []types.Decimal128{randD128(rng)}
		v2 := make([]types.Decimal128, 16)
		for i := range v2 {
			v2[i] = randD128(rng)
			if d128IsZero(v2[i]) {
				v2[i].B0_63 = 1
			}
		}
		rs := make([]types.Decimal128, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 2, 8, nul, false))
	})

	t.Run("VecConst_DiffScale_ScaleX", func(t *testing.T) {
		v1 := make([]types.Decimal128, 16)
		for i := range v1 {
			v1[i] = randD128(rng)
		}
		v2 := []types.Decimal128{randD128(rng)}
		if d128IsZero(v2[0]) {
			v2[0].B0_63 = 1
		}
		rs := make([]types.Decimal128, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 2, 8, nul, false))
	})

	t.Run("ConstVec_SameScale", func(t *testing.T) {
		v1 := []types.Decimal128{randD128(rng)}
		v2 := make([]types.Decimal128, 16)
		for i := range v2 {
			v2[i] = randD128(rng)
			if d128IsZero(v2[i]) {
				v2[i].B0_63 = 1
			}
		}
		rs := make([]types.Decimal128, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecConst_SameScale", func(t *testing.T) {
		v1 := make([]types.Decimal128, 16)
		for i := range v1 {
			v1[i] = randD128(rng)
		}
		v2 := []types.Decimal128{randD128(rng)}
		if d128IsZero(v2[0]) {
			v2[0].B0_63 = 1
		}
		rs := make([]types.Decimal128, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecConst_SameScale_Large", func(t *testing.T) {
		v1 := make([]types.Decimal128, 16)
		for i := range v1 {
			v1[i] = largeD128(rng)
		}
		v2 := []types.Decimal128{largeD128(rng)}
		if d128IsZero(v2[0]) {
			v2[0].B0_63 = 1
		}
		rs := make([]types.Decimal128, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 4, 4, nul, false))
	})
}

// TestD128IntDiv_InlineFallbackPaths covers d128 integer division inline fallback paths.
func TestD128IntDiv_InlineFallbackPaths(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	makeLargeVec := func(n int) []types.Decimal128 {
		v := make([]types.Decimal128, n)
		for i := range v {
			v[i] = largeD128(rng)
		}
		return v
	}
	makeSmallDiv := func(n int) []types.Decimal128 {
		v := make([]types.Decimal128, n)
		for i := range v {
			v[i].B0_63 = uint64(rng.Intn(1000)) + 1
		}
		return v
	}

	t.Run("VecVec_NoNull", func(t *testing.T) {
		v1 := makeLargeVec(16)
		v2 := makeSmallDiv(16)
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("ConstVec_NoNull", func(t *testing.T) {
		v1 := makeLargeVec(1)
		v2 := makeSmallDiv(16)
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecConst_NoNull", func(t *testing.T) {
		v1 := makeLargeVec(16)
		v2 := makeSmallDiv(1)
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d128IntDiv(v1, v2, rs, 4, 4, nul, false))
	})

	t.Run("VecConst_WithNull", func(t *testing.T) {
		v1 := makeLargeVec(16)
		v2 := makeSmallDiv(1)
		rs := make([]int64, 16)
		nul := makeNulls(16)
		require.NoError(t, d128IntDiv(v1, v2, rs, 4, 4, nul, false))
	})
}

// TestD256IntDivViaD128_ConstAndNullPaths covers D256 intdiv const dispatch variants.
func TestD256IntDivViaD128_ConstAndNullPaths(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	t.Run("ConstVec_NoNull", func(t *testing.T) {
		v1 := []types.Decimal256{randD256Small(rng)}
		v2 := make([]types.Decimal256, 16)
		for i := range v2 {
			v2[i] = randD256Small(rng)
		}
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, false, 4, 4, !nul.IsEmpty(), nul.GetBitmap()))
	})

	t.Run("ConstVec_WithNull", func(t *testing.T) {
		v1 := []types.Decimal256{randD256Small(rng)}
		v2 := make([]types.Decimal256, 16)
		for i := range v2 {
			v2[i] = randD256Small(rng)
		}
		rs := make([]int64, 16)
		nul := makeNulls(16)
		require.NoError(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, false, 4, 4, !nul.IsEmpty(), nul.GetBitmap()))
	})

	t.Run("VecConst_NoNull", func(t *testing.T) {
		v1 := make([]types.Decimal256, 16)
		for i := range v1 {
			v1[i] = randD256Small(rng)
		}
		v2 := []types.Decimal256{randD256Small(rng)}
		rs := make([]int64, 16)
		nul := &nulls.Nulls{}
		require.NoError(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, false, 4, 4, !nul.IsEmpty(), nul.GetBitmap()))
	})

	t.Run("VecConst_WithNull", func(t *testing.T) {
		v1 := make([]types.Decimal256, 16)
		for i := range v1 {
			v1[i] = randD256Small(rng)
		}
		v2 := []types.Decimal256{randD256Small(rng)}
		rs := make([]int64, 16)
		nul := makeNulls(16)
		require.NoError(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, false, 4, 4, !nul.IsEmpty(), nul.GetBitmap()))
	})
}

// TestD64ScaleIntoRs_ConstPaths covers d64ScaleIntoRs with null and no-null paths.
func TestD64ScaleIntoRs_ConstPaths(t *testing.T) {
	for _, tc := range []struct {
		name      string
		input     []types.Decimal64
		scale     int32
		nullRows  []uint64
		want      []types.Decimal64
		wantError uint16
	}{
		{"SmallValues_NoNull", []types.Decimal64{92233720368547758, 18354510353341003858}, 2, nil, []types.Decimal64{9223372036854775800, 9223372036854775816}, 0},
		{"SmallValues_WithNull", []types.Decimal64{92233720368547758, 0, 18354510353341003858}, 2, []uint64{1}, []types.Decimal64{9223372036854775800, 0, 9223372036854775816}, 0},
		{"LargeValues_Fallback_NoNull", []types.Decimal64{92233720368547759}, 2, nil, nil, moerr.ErrInvalidInput},
		{"LargeValues_Fallback_WithNull", []types.Decimal64{0, 92233720368547759}, 2, []uint64{0}, nil, moerr.ErrInvalidInput},
		{"MaskedOverflowContinues", []types.Decimal64{92233720368547758, 92233720368547759, 18354510353341003858}, 2, []uint64{1}, []types.Decimal64{9223372036854775800, 0, 9223372036854775816}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := nulls.NewWithSize(len(tc.input))
			for _, row := range tc.nullRows {
				n.Add(row)
			}
			got := make([]types.Decimal64, len(tc.input))
			err := d64ScaleIntoRs(tc.input, got, len(got), tc.scale, n)
			if tc.wantError != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.wantError), "error: %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.nullRows), n.Count())
			for i := range got {
				masked := false
				for _, row := range tc.nullRows {
					masked = masked || row == uint64(i)
				}
				require.Equal(t, masked, n.Contains(uint64(i)))
				if !masked && tc.wantError == 0 {
					require.Equal(t, tc.want[i], got[i], "row %d", i)
				}
			}
		})
	}
}

// TestD64Mod_ScaleXConstPaths covers d64Mod scaleX dispatch (scale2 > scale1)
// for const×vec, vec×const, including div-by-zero with shouldError=true/false.
func TestD64Mod_ScaleXConstPaths(t *testing.T) {
	// scaleX path: scale2 > scale1
	t.Run("ScaleX_VecVec_NoNull", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200, 300, 400}
		v2 := []types.Decimal64{3, 7, 11, 13}
		rs := make([]types.Decimal64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 2, 6, nul, false))
	})
	t.Run("ScaleX_ConstVec_NoNull", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{3, 7, 11, 13}
		rs := make([]types.Decimal64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 2, 6, nul, false))
	})
	t.Run("ScaleX_ConstVec_WithNull", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{3, 7, 11, 13}
		rs := make([]types.Decimal64, 4)
		nul := makeNulls(4)
		require.NoError(t, d64Mod(v1, v2, rs, 2, 6, nul, false))
	})
	t.Run("ScaleX_VecConst_NoNull", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200, 300, 400}
		v2 := []types.Decimal64{7}
		rs := make([]types.Decimal64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 2, 6, nul, false))
	})
	t.Run("ScaleX_VecConst_WithNull", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200, 300, 400}
		v2 := []types.Decimal64{7}
		rs := make([]types.Decimal64, 4)
		nul := makeNulls(4)
		require.NoError(t, d64Mod(v1, v2, rs, 2, 6, nul, false))
	})
	// Div-by-zero shouldError=true in scaleX paths
	t.Run("ScaleX_VecVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{0, 3}
		rs := make([]types.Decimal64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64Mod(v1, v2, rs, 2, 6, nul, true))
	})
	t.Run("ScaleX_ConstVec_DivZero_Nullify", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{0, 3, 0, 7}
		rs := make([]types.Decimal64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 2, 6, nul, false))
	})
	t.Run("ScaleX_ConstVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{0, 3}
		rs := make([]types.Decimal64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64Mod(v1, v2, rs, 2, 6, nul, true))
	})
	t.Run("ScaleX_VecConst_Zero_Nullify", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{0}
		rs := make([]types.Decimal64, 2)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 2, 6, nul, false))
	})
	t.Run("ScaleX_VecConst_Zero_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{0}
		rs := make([]types.Decimal64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64Mod(v1, v2, rs, 2, 6, nul, true))
	})
	// shouldError=true in same-scale paths
	t.Run("SameScale_VecVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{0, 3}
		rs := make([]types.Decimal64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64Mod(v1, v2, rs, 4, 4, nul, true))
	})
	t.Run("SameScale_ConstVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{0, 3}
		rs := make([]types.Decimal64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64Mod(v1, v2, rs, 4, 4, nul, true))
	})
	t.Run("SameScale_VecConst_Zero_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{0}
		rs := make([]types.Decimal64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64Mod(v1, v2, rs, 4, 4, nul, true))
	})
}

// TestD64Mod_NonScaleXConstPaths covers d64Mod non-scaleX (scale1 > scale2) const dispatch.
func TestD64Mod_NonScaleXConstPaths(t *testing.T) {
	// non-scaleX path: scale1 > scale2, modFn = d128ModDiffScaleYPow10
	t.Run("NonScaleX_VecVec_NoNull", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200, 300, 400}
		v2 := []types.Decimal64{3, 7, 11, 13}
		rs := make([]types.Decimal64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("NonScaleX_ConstVec_NoNull", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{3, 7, 11, 13}
		rs := make([]types.Decimal64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("NonScaleX_ConstVec_WithNull", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{3, 7, 11, 13}
		rs := make([]types.Decimal64, 4)
		nul := makeNulls(4)
		require.NoError(t, d64Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("NonScaleX_VecConst_NoNull", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200, 300, 400}
		v2 := []types.Decimal64{7}
		rs := make([]types.Decimal64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("NonScaleX_VecConst_WithNull", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200, 300, 400}
		v2 := []types.Decimal64{7}
		rs := make([]types.Decimal64, 4)
		nul := makeNulls(4)
		require.NoError(t, d64Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("NonScaleX_DivZero_ConstVec_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{0, 3}
		rs := make([]types.Decimal64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64Mod(v1, v2, rs, 6, 2, nul, true))
	})
	t.Run("NonScaleX_DivZero_VecConst_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{0}
		rs := make([]types.Decimal64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64Mod(v1, v2, rs, 6, 2, nul, true))
	})
	t.Run("NonScaleX_DivZero_VecConst_Nullify", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{0}
		rs := make([]types.Decimal64, 2)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("NonScaleX_DivZero_ConstVec_Nullify", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{0, 3, 0, 7}
		rs := make([]types.Decimal64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d64Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("NonScaleX_DivZero_VecVec_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{0, 3}
		rs := make([]types.Decimal64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64Mod(v1, v2, rs, 6, 2, nul, true))
	})
}

// TestD128Mod_ConstAndShouldError covers d128Mod const×vec/vec×const with shouldError=true.
func TestD128Mod_ConstAndShouldError(t *testing.T) {
	mkD128 := func(v int64) types.Decimal128 {
		return types.Decimal128{B0_63: uint64(v), B64_127: uint64(v >> 63)}
	}
	zero := types.Decimal128{}

	t.Run("SameScale_VecVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100), mkD128(200)}
		v2 := []types.Decimal128{zero, mkD128(3)}
		rs := make([]types.Decimal128, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d128Mod(v1, v2, rs, 4, 4, nul, true))
	})
	t.Run("SameScale_ConstVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100)}
		v2 := []types.Decimal128{zero, mkD128(3)}
		rs := make([]types.Decimal128, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d128Mod(v1, v2, rs, 4, 4, nul, true))
	})
	t.Run("SameScale_VecConst_Zero_Error", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100), mkD128(200)}
		v2 := []types.Decimal128{zero}
		rs := make([]types.Decimal128, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d128Mod(v1, v2, rs, 4, 4, nul, true))
	})
	t.Run("DiffScale_ConstVec_NoNull", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100)}
		v2 := []types.Decimal128{mkD128(3), mkD128(7), mkD128(11), mkD128(13)}
		rs := make([]types.Decimal128, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("DiffScale_ConstVec_WithNull", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100)}
		v2 := []types.Decimal128{mkD128(3), mkD128(7), mkD128(11), mkD128(13)}
		rs := make([]types.Decimal128, 4)
		nul := makeNulls(4)
		require.NoError(t, d128Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("DiffScale_VecConst_NoNull", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100), mkD128(200), mkD128(300), mkD128(400)}
		v2 := []types.Decimal128{mkD128(7)}
		rs := make([]types.Decimal128, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("DiffScale_VecConst_WithNull", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100), mkD128(200), mkD128(300), mkD128(400)}
		v2 := []types.Decimal128{mkD128(7)}
		rs := make([]types.Decimal128, 4)
		nul := makeNulls(4)
		require.NoError(t, d128Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("DiffScale_DivZero_ConstVec_Error", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100)}
		v2 := []types.Decimal128{zero, mkD128(3)}
		rs := make([]types.Decimal128, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d128Mod(v1, v2, rs, 6, 2, nul, true))
	})
	t.Run("DiffScale_DivZero_VecConst_Error", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100), mkD128(200)}
		v2 := []types.Decimal128{zero}
		rs := make([]types.Decimal128, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d128Mod(v1, v2, rs, 6, 2, nul, true))
	})
	t.Run("DiffScale_DivZero_VecConst_Nullify", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100), mkD128(200)}
		v2 := []types.Decimal128{zero}
		rs := make([]types.Decimal128, 2)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("DiffScale_DivZero_ConstVec_Nullify", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100)}
		v2 := []types.Decimal128{zero, mkD128(3), zero, mkD128(7)}
		rs := make([]types.Decimal128, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d128Mod(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("DiffScale_DivZero_VecVec_Error", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100), mkD128(200)}
		v2 := []types.Decimal128{zero, mkD128(3)}
		rs := make([]types.Decimal128, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d128Mod(v1, v2, rs, 6, 2, nul, true))
	})
}

// TestD256IntDivViaD128_ShouldErrorPaths covers d256IntDivViaD128 shouldError=true paths.
func TestD256IntDivViaD128_ShouldErrorPaths(t *testing.T) {
	mkD256 := func(v int64) types.Decimal256 {
		return types.Decimal256{B0_63: uint64(v), B64_127: uint64(v >> 63)}
	}
	zero := types.Decimal256{}

	t.Run("VecVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal256{mkD256(100), mkD256(200)}
		v2 := []types.Decimal256{zero, mkD256(3)}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, true, 4, 4, false, nil))
	})
	t.Run("ConstVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal256{mkD256(100)}
		v2 := []types.Decimal256{zero, mkD256(3)}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, true, 4, 4, false, nil))
	})
	t.Run("VecConst_Zero_Error", func(t *testing.T) {
		v1 := []types.Decimal256{mkD256(100), mkD256(200)}
		v2 := []types.Decimal256{zero}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, true, 4, 4, false, nil))
	})
	t.Run("ConstVec_DivZero_Nullify", func(t *testing.T) {
		v1 := []types.Decimal256{mkD256(100)}
		v2 := []types.Decimal256{zero, mkD256(3), zero, mkD256(7)}
		rs := make([]int64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, false, 4, 4, false, nil))
	})
	t.Run("VecConst_Zero_Nullify", func(t *testing.T) {
		v1 := []types.Decimal256{mkD256(100), mkD256(200)}
		v2 := []types.Decimal256{zero}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.NoError(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, false, 4, 4, false, nil))
	})
	t.Run("ConstVec_WithNull", func(t *testing.T) {
		v1 := []types.Decimal256{mkD256(100)}
		v2 := []types.Decimal256{mkD256(3), mkD256(7), mkD256(11), mkD256(13)}
		rs := make([]int64, 4)
		nul := makeNulls(4)
		bmp := nul.GetBitmap()
		require.NoError(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, false, 4, 4, true, bmp))
	})
	t.Run("VecConst_WithNull", func(t *testing.T) {
		v1 := []types.Decimal256{mkD256(100), mkD256(200), mkD256(300), mkD256(400)}
		v2 := []types.Decimal256{mkD256(7)}
		rs := make([]int64, 4)
		nul := makeNulls(4)
		bmp := nul.GetBitmap()
		require.NoError(t, d256IntDivViaD128(v1, v2, rs, 4, 6, nul, false, 4, 4, true, bmp))
	})
}

// TestD128IntDiv_ShouldErrorAndConst covers d128IntDiv shouldError+const dispatch.
func TestD128IntDiv_ShouldErrorAndConst(t *testing.T) {
	mkD128 := func(v int64) types.Decimal128 {
		return types.Decimal128{B0_63: uint64(v), B64_127: uint64(v >> 63)}
	}
	zero := types.Decimal128{}

	t.Run("SameScale_VecVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100), mkD128(200)}
		v2 := []types.Decimal128{zero, mkD128(3)}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d128IntDiv(v1, v2, rs, 4, 4, nul, true))
	})
	t.Run("SameScale_ConstVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100)}
		v2 := []types.Decimal128{zero, mkD128(3)}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d128IntDiv(v1, v2, rs, 4, 4, nul, true))
	})
	t.Run("SameScale_VecConst_Zero_Error", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100), mkD128(200)}
		v2 := []types.Decimal128{zero}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d128IntDiv(v1, v2, rs, 4, 4, nul, true))
	})
	t.Run("DiffScale_ConstVec_NoNull", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100)}
		v2 := []types.Decimal128{mkD128(3), mkD128(7), mkD128(11), mkD128(13)}
		rs := make([]int64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d128IntDiv(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("DiffScale_VecConst_NoNull", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100), mkD128(200), mkD128(300), mkD128(400)}
		v2 := []types.Decimal128{mkD128(7)}
		rs := make([]int64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d128IntDiv(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("DiffScale_ConstVec_WithNull", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100)}
		v2 := []types.Decimal128{mkD128(3), mkD128(7), mkD128(11), mkD128(13)}
		rs := make([]int64, 4)
		nul := makeNulls(4)
		require.NoError(t, d128IntDiv(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("DiffScale_DivZero_ConstVec_Error", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100)}
		v2 := []types.Decimal128{zero, mkD128(3)}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d128IntDiv(v1, v2, rs, 6, 2, nul, true))
	})
	t.Run("DiffScale_DivZero_VecConst_Error", func(t *testing.T) {
		v1 := []types.Decimal128{mkD128(100), mkD128(200)}
		v2 := []types.Decimal128{zero}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d128IntDiv(v1, v2, rs, 6, 2, nul, true))
	})
}

// TestD64IntDiv_ShouldErrorPaths covers d64IntDiv shouldError=true paths.
func TestD64IntDiv_ShouldErrorPaths(t *testing.T) {
	zero := types.Decimal64(0)

	t.Run("VecVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{zero, 3}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64IntDiv(v1, v2, rs, 4, 4, nul, true))
	})
	t.Run("ConstVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{zero, 3}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64IntDiv(v1, v2, rs, 4, 4, nul, true))
	})
	t.Run("VecConst_Zero_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{zero}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64IntDiv(v1, v2, rs, 4, 4, nul, true))
	})
}

// TestD64IntDiv_ConstDivZeroShouldError covers d64IntDiv const×vec/vec×const shouldError=true.
func TestD64IntDiv_ConstDivZeroShouldError(t *testing.T) {
	zero := types.Decimal64(0)

	t.Run("ConstVec_DivZero_Nullify", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{zero, 3, zero, 7}
		rs := make([]int64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d64IntDiv(v1, v2, rs, 4, 4, nul, false))
	})
	t.Run("ConstVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{zero, 3}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64IntDiv(v1, v2, rs, 4, 4, nul, true))
	})
	t.Run("VecConst_Zero_Nullify", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{zero}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.NoError(t, d64IntDiv(v1, v2, rs, 4, 4, nul, false))
	})
	t.Run("DiffScale_ConstVec_NoNull", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{3, 7, 11, 13}
		rs := make([]int64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d64IntDiv(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("DiffScale_VecConst_NoNull", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200, 300, 400}
		v2 := []types.Decimal64{7}
		rs := make([]int64, 4)
		nul := &nulls.Nulls{}
		require.NoError(t, d64IntDiv(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("DiffScale_ConstVec_DivZero_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{zero, 3}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64IntDiv(v1, v2, rs, 6, 2, nul, true))
	})
	t.Run("DiffScale_VecConst_Zero_Error", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{zero}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.Error(t, d64IntDiv(v1, v2, rs, 6, 2, nul, true))
	})
	t.Run("DiffScale_VecConst_Zero_Nullify", func(t *testing.T) {
		v1 := []types.Decimal64{100, 200}
		v2 := []types.Decimal64{zero}
		rs := make([]int64, 2)
		nul := &nulls.Nulls{}
		require.NoError(t, d64IntDiv(v1, v2, rs, 6, 2, nul, false))
	})
	t.Run("DiffScale_ConstVec_WithNull", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{3, 7, 11, 13}
		rs := make([]int64, 4)
		nul := makeNulls(4)
		require.NoError(t, d64IntDiv(v1, v2, rs, 6, 2, nul, false))
	})
}

// TestD128DivOneDispatch_Paths covers d128DivOneDispatch, d128DivOne, and
// d128DivInline overflow sub-branches.
func TestD128DivOneDispatch_Paths(t *testing.T) {
	smallY := types.Decimal128{B0_63: 5}
	// x that triggers d128DivInline overflow path at line 1090
	// B64_127 != 0 after abs, so crossHi path is taken.
	// scaleFactor is large enough to overflow.
	bigX := types.Decimal128{B0_63: 0xFFFFFFFFFFFFFFFF, B64_127: 0x7FFFFFFFFFFFFFFF}
	zero := types.Decimal128{}

	t.Run("d128DivOne_DivByZero_ShouldError", func(t *testing.T) {
		var dst types.Decimal128
		nul := &nulls.Nulls{}
		err := d128DivOne(bigX, zero, &dst, 12, nul, 0, true, 2, 2)
		if err == nil {
			t.Fatal("expected div-by-zero error")
		}
	})
	t.Run("d128DivOne_DivByZero_NoError", func(t *testing.T) {
		var dst types.Decimal128
		nul := &nulls.Nulls{}
		_ = d128DivOne(bigX, zero, &dst, 12, nul, 0, false, 2, 2)
	})
	t.Run("d128DivOne_NormalPath", func(t *testing.T) {
		var dst types.Decimal128
		nul := &nulls.Nulls{}
		_ = d128DivOne(bigX, smallY, &dst, 6, nul, 0, false, 2, 2)
	})
	t.Run("d128DivOneDispatch_ShouldError_DivZero", func(t *testing.T) {
		var dst types.Decimal128
		nul := &nulls.Nulls{}
		// canInline=true, shouldError=true, y=zero
		err := d128DivOneDispatch(bigX, zero, &dst, 6, types.Pow10[6], true, nul, 0, true, 2, 2)
		if err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("d128DivOneDispatch_Inline_Success", func(t *testing.T) {
		var dst types.Decimal128
		nul := &nulls.Nulls{}
		x := types.Decimal128{B0_63: 100}
		// canInline=true, small x → d128DivInline succeeds
		err := d128DivOneDispatch(x, smallY, &dst, 6, types.Pow10[6], true, nul, 0, false, 2, 2)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("d128DivOneDispatch_Inline_Fails", func(t *testing.T) {
		var dst types.Decimal128
		nul := &nulls.Nulls{}
		// bigX with large scaleAdj → inline fails, falls back to d128DivOne
		_ = d128DivOneDispatch(bigX, smallY, &dst, 12, types.Pow10[12], true, nul, 0, false, 2, 2)
	})
	t.Run("d128DivOneDispatch_NoInline", func(t *testing.T) {
		var dst types.Decimal128
		nul := &nulls.Nulls{}
		// canInline=false → calls d128DivOne directly
		_ = d128DivOneDispatch(bigX, smallY, &dst, 20, 0, false, nul, 0, false, 2, 2)
	})
}
