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
	type decimal = types.Decimal64
	// S: same scale; X/Y: scale dividend/divisor; 64/W: small/wide divisor.
	// N: empty bitmap; M: masked strict success; P: mask plus new NULL;
	// E/Z: strict/permissive zero. Expected coefficients are independent literals.
	for _, tc := range []struct {
		name                    string
		x, y, want              []decimal
		s1, s2                  int32
		initialNulls, wantNulls []uint64
		strict, throughFactory  bool
		wantError               uint16
	}{
		{"S_VV_N", []decimal{100, 0x8000000000000000, 0xffffffffffffffef, 18, 0, 0x8000000000000000, 0x8000000000000000}, []decimal{0, 7, 0xfffffffffffffffb, 5, 7, 0xffffffffffffffff, 0x8000000000000000}, []decimal{0, 0xffffffffffffffff, 0xfffffffffffffffe, 3, 0, 0, 0}, 2, 2, nil, []uint64{0}, false, false, 0},
		{"S_SV_N", []decimal{0xffffffffffffff9b}, []decimal{0, 7, 0xfffffffffffffff5}, []decimal{0, 0xfffffffffffffffd, 0xfffffffffffffffe}, 2, 2, nil, []uint64{0}, false, false, 0},
		{"S_VS_N", []decimal{100, 0xffffffffffffff9b, 102}, []decimal{0xfffffffffffffff9}, []decimal{2, 0xfffffffffffffffd, 4}, 2, 2, nil, nil, false, false, 0},
		{"S_VV_M", []decimal{0, 0xffffffffffffffef, 0, 18, 0}, []decimal{0, 5, 0, 7, 0}, []decimal{0, 0xfffffffffffffffe, 0, 4, 0}, 2, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"S_SV_M", []decimal{0xffffffffffffff9b}, []decimal{0, 7, 0, 0xfffffffffffffff5, 0}, []decimal{0, 0xfffffffffffffffd, 0, 0xfffffffffffffffe, 0}, 2, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"S_VS_M", []decimal{0, 0xffffffffffffffef, 0, 18, 0}, []decimal{0xfffffffffffffff9}, []decimal{0, 0xfffffffffffffffd, 0, 4, 0}, 2, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"X_VV_N", []decimal{100, 0xffffffffffffff9b, 102, 0}, []decimal{0, 7, 0xfffffffffffffff5, 7}, []decimal{0, 0xfffffffffffffffc, 8, 0}, 2, 5, nil, []uint64{0}, false, false, 0},
		{"X_SV_N", []decimal{0xffffffffffffff9b}, []decimal{0, 7, 0xfffffffffffffff5}, []decimal{0, 0xfffffffffffffffd, 0xfffffffffffffffe}, 2, 8, nil, []uint64{0}, false, false, 0},
		{"X_VS_N", []decimal{100, 0xffffffffffffff9b, 102}, []decimal{0xfffffffffffffff9}, []decimal{1, 0xfffffffffffffffb, 2}, 2, 6, nil, nil, false, false, 0},
		{"X_VV_M", []decimal{0, 0xffffffffffffff9b, 0, 102, 0}, []decimal{0, 7, 0, 0xfffffffffffffff5, 0}, []decimal{0, 0xfffffffffffffffd, 0, 3, 0}, 2, 8, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"X_SV_M", []decimal{0xffffffffffffff9b}, []decimal{0, 7, 0, 0xfffffffffffffff5, 0}, []decimal{0, 0xfffffffffffffffd, 0, 0xfffffffffffffffe, 0}, 2, 8, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"X_VS_M", []decimal{0, 0xffffffffffffff9b, 0, 102, 0}, []decimal{0xfffffffffffffff9}, []decimal{0, 0xfffffffffffffffd, 0, 4, 0}, 2, 8, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"X_VV_P", []decimal{100, 0xffffffffffffff9b, 102, 103, 104}, []decimal{0, 7, 0, 0xfffffffffffffff5, 7}, []decimal{0, 0xfffffffffffffffb, 0, 4, 3}, 2, 6, []uint64{0}, []uint64{0, 2}, false, false, 0},
		{"Y_VV_N", []decimal{70001, 0xfffffffffffddd1e, 210003, 0}, []decimal{0, 7, 0xfffffffffffffff9, 7}, []decimal{0, 0xfffffffffffffffe, 3, 0}, 6, 2, nil, []uint64{0}, false, false, 0},
		{"Y_SV_N", []decimal{210003}, []decimal{0, 7, 0xfffffffffffffff5}, []decimal{0, 3, 100003}, 6, 2, nil, []uint64{0}, false, false, 0},
		{"Y_VS_N", []decimal{70001, 0xfffffffffffddd1e, 210003}, []decimal{0xfffffffffffffff9}, []decimal{1, 0xfffffffffffffffe, 3}, 6, 2, nil, nil, false, false, 0},
		{"Y_VV_M", []decimal{0, 0xfffffffffffddd1e, 0, 210003, 0}, []decimal{0, 7, 0, 0xfffffffffffffff9, 0}, []decimal{0, 0xfffffffffffffffe, 0, 3, 0}, 6, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"Y_SV_M", []decimal{210003}, []decimal{0, 7, 0, 0xfffffffffffffff5, 0}, []decimal{0, 3, 0, 100003, 0}, 6, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"Y_VS_M", []decimal{0, 0xfffffffffffddd1e, 0, 210003, 0}, []decimal{0xfffffffffffffff9}, []decimal{0, 0xfffffffffffffffe, 0, 3, 0}, 6, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"X_EndpointAlignment", []decimal{0x7fffffffffffffff, 0x8000000000000000}, []decimal{7, 0xfffffffffffffff9}, []decimal{0, 0xffffffffffffffff}, 0, 18, nil, nil, true, false, 0},
		{"Y_WideDivisor", []decimal{7, 0x8000000000000000}, []decimal{0x7fffffffffffffff, 0x8000000000000001}, []decimal{7, 0x8000000000000000}, 18, 0, nil, nil, true, false, 0},
		{"Kernel", []decimal{100}, []decimal{3}, []decimal{1}, 2, 2, nil, nil, true, true, 0},
		{"S_VV_E", []decimal{100, 101}, []decimal{0, 7}, []decimal{0, 3}, 4, 4, nil, nil, true, false, moerr.ErrDivByZero},
		{"S_SV_E", []decimal{100}, []decimal{0, 7}, []decimal{0, 2}, 4, 4, nil, nil, true, false, moerr.ErrDivByZero},
		{"S_VS_E", []decimal{100, 101}, []decimal{0}, []decimal{0, 0}, 4, 4, []uint64{0, 1}, []uint64{0, 1}, true, false, moerr.ErrDivByZero},
		{"S_VS_Z", []decimal{100, 101, 102}, []decimal{0}, []decimal{0, 0, 0}, 4, 4, []uint64{0}, []uint64{0, 1, 2}, false, false, 0},
		{"X_VV_E", []decimal{100, 101}, []decimal{0, 7}, []decimal{0, 5}, 2, 6, nil, nil, true, false, moerr.ErrDivByZero},
		{"X_SV_E", []decimal{100}, []decimal{0, 7}, []decimal{0, 1}, 2, 6, nil, nil, true, false, moerr.ErrDivByZero},
		{"X_VS_E", []decimal{100, 101}, []decimal{0}, []decimal{0, 0}, 2, 6, []uint64{0, 1}, []uint64{0, 1}, true, false, moerr.ErrDivByZero},
		{"X_VS_Z", []decimal{100, 101, 102}, []decimal{0}, []decimal{0, 0, 0}, 2, 6, []uint64{0}, []uint64{0, 1, 2}, false, false, 0},
		{"Y_VV_E", []decimal{100, 101}, []decimal{0, 7}, []decimal{0, 101}, 6, 2, nil, nil, true, false, moerr.ErrDivByZero},
		{"Y_SV_E", []decimal{100}, []decimal{0, 7}, []decimal{0, 100}, 6, 2, nil, nil, true, false, moerr.ErrDivByZero},
		{"Y_VS_E", []decimal{100, 101}, []decimal{0}, []decimal{0, 0}, 6, 2, []uint64{0, 1}, []uint64{0, 1}, true, false, moerr.ErrDivByZero},
		{"Y_VS_Z", []decimal{100, 101, 102}, []decimal{0}, []decimal{0, 0, 0}, 6, 2, []uint64{0}, []uint64{0, 1, 2}, false, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := make([]decimal, len(tc.want))
			for i := range rs {
				rs[i] = decimal(9000 + i)
			}
			nul := nulls.NewWithSize(len(rs))
			for _, i := range tc.initialNulls {
				nul.Add(i)
			}
			var err error
			if tc.throughFactory {
				err = d64ModKernel(tc.strict)(tc.x, tc.y, rs, tc.s1, tc.s2, nul)
			} else {
				err = d64Mod(tc.x, tc.y, rs, tc.s1, tc.s2, nul, tc.strict)
			}
			if tc.wantError != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.wantError), "got %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.wantNulls), nul.Count())
			for _, i := range tc.wantNulls {
				require.True(t, nul.Contains(i), "NULL row %d", i)
			}
			if tc.wantError != 0 {
				return // Error paths do not promise rollback of scratch results.
			}
			for _, i := range tc.initialNulls {
				require.Equal(t, decimal(9000+i), rs[i], "masked row %d", i)
			}
			for i, want := range tc.want {
				if !nul.Contains(uint64(i)) {
					require.Equal(t, want, rs[i], "row %d", i)
				}
			}
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
	type decimal = types.Decimal128
	negative101 := decimal{B0_63: 0xffffffffffffff9b, B64_127: 0xffffffffffffffff}
	negative17 := decimal{B0_63: 0xffffffffffffffef, B64_127: 0xffffffffffffffff}
	negative11 := decimal{B0_63: 0xfffffffffffffff5, B64_127: 0xffffffffffffffff}
	negative7 := decimal{B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff}
	negative2 := decimal{B0_63: 0xfffffffffffffffe, B64_127: 0xffffffffffffffff}
	negative3 := decimal{B0_63: 0xfffffffffffffffd, B64_127: 0xffffffffffffffff}
	negative4 := decimal{B0_63: 0xfffffffffffffffc, B64_127: 0xffffffffffffffff}
	negative5 := decimal{B0_63: 0xfffffffffffffffb, B64_127: 0xffffffffffffffff}
	negative140002 := decimal{B0_63: 0xfffffffffffddd1e, B64_127: 0xffffffffffffffff}
	wideDividend := decimal{B0_63: 100, B64_127: 3}
	negativeWideDividend := decimal{B0_63: 0xffffffffffffff9c, B64_127: 0xfffffffffffffffc}
	wideDivisor := decimal{B0_63: 7, B64_127: 1}
	negativeWideDivisor := decimal{B0_63: 0xfffffffffffffff9, B64_127: 0xfffffffffffffffe}
	doubleWideDivisor := decimal{B0_63: 7, B64_127: 2}
	negativeDoubleWideDivisor := decimal{B0_63: 0xfffffffffffffff9, B64_127: 0xfffffffffffffffd}
	maximum := decimal{B0_63: 0xffffffffffffffff, B64_127: 0x7fffffffffffffff}
	negativeMaximum := decimal{B0_63: 1, B64_127: 0x8000000000000000}
	wideRemainder := decimal{B0_63: 93, B64_127: 1}
	negativeWideRemainder := decimal{B0_63: 0xffffffffffffffa3, B64_127: 0xfffffffffffffffe}
	// S: same scale; X/Y: scale dividend/divisor; 64/W: small/wide divisor.
	// N: empty bitmap; M: masked strict success; P: mask plus new NULL;
	// E/Z: strict/permissive zero. Expected coefficients are independent literals.
	for _, tc := range []struct {
		name                    string
		x, y, want              []decimal
		s1, s2                  int32
		initialNulls, wantNulls []uint64
		strict, throughFactory  bool
		wantError               uint16
	}{
		{"S64_VV_N", []decimal{{B0_63: 100}, {B0_63: 100, B64_127: 2}, {B0_63: 100, B64_127: 2}, {B64_127: 0x8000000000000000}, {}}, []decimal{{}, {B0_63: 0xffffffffffffffff}, {B0_63: 1, B64_127: 0xffffffffffffffff}, {B0_63: 7}, {B0_63: 7}}, []decimal{{}, {B0_63: 102}, {B0_63: 102}, negative2, {}}, 2, 2, nil, []uint64{0}, false, false, 0},
		{"S64_SV_N", []decimal{negative101}, []decimal{{}, {B0_63: 7}, negative11}, []decimal{{}, negative3, negative2}, 2, 2, nil, []uint64{0}, false, false, 0},
		{"S64_VS_N", []decimal{{B0_63: 100}, negative101, {B0_63: 102}}, []decimal{negative7}, []decimal{{B0_63: 2}, negative3, {B0_63: 4}}, 2, 2, nil, nil, false, false, 0},
		{"S64_VV_M", []decimal{{}, negative17, {}, {B0_63: 18}, {}}, []decimal{{}, {B0_63: 5}, {}, {B0_63: 7}, {}}, []decimal{{}, negative2, {}, {B0_63: 4}, {}}, 2, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"S64_SV_M", []decimal{negative101}, []decimal{{}, {B0_63: 7}, {}, negative11, {}}, []decimal{{}, negative3, {}, negative2, {}}, 2, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"S64_VS_M", []decimal{{}, negative17, {}, {B0_63: 18}, {}}, []decimal{negative7}, []decimal{{}, negative3, {}, {B0_63: 4}, {}}, 2, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"SW_VV_N", []decimal{{B0_63: 100, B64_127: 2}, wideDividend, negativeWideDividend, {B0_63: 100, B64_127: 2}, {}}, []decimal{{}, doubleWideDivisor, doubleWideDivisor, {B64_127: 0xffffffffffffffff}, wideDivisor}, []decimal{{}, wideRemainder, negativeWideRemainder, {B0_63: 100}, {}}, 2, 2, nil, []uint64{0}, false, false, 0},
		{"SW_SV_N", []decimal{wideDividend}, []decimal{{}, doubleWideDivisor, {B64_127: 0xffffffffffffffff}, wideDivisor}, []decimal{{}, wideRemainder, {B0_63: 100}, {B0_63: 79}}, 2, 2, nil, []uint64{0}, false, false, 0},
		{"SW_VS_N", []decimal{wideDividend, negativeWideDividend, {B0_63: 100, B64_127: 2}}, []decimal{doubleWideDivisor}, []decimal{wideRemainder, negativeWideRemainder, {B0_63: 93}}, 2, 2, nil, nil, false, false, 0},
		{"SW_VV_M", []decimal{{}, wideDividend, {}, negativeWideDividend, {}}, []decimal{{}, doubleWideDivisor, {}, negativeWideDivisor, {}}, []decimal{{}, wideRemainder, {}, {B0_63: 0xffffffffffffffb1, B64_127: 0xffffffffffffffff}, {}}, 2, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"SW_SV_M", []decimal{wideDividend}, []decimal{{}, doubleWideDivisor, {}, negativeWideDivisor, {}}, []decimal{{}, wideRemainder, {}, {B0_63: 79}, {}}, 2, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"SW_VS_M", []decimal{{}, wideDividend, {}, negativeWideDividend, {}}, []decimal{doubleWideDivisor}, []decimal{{}, wideRemainder, {}, negativeWideRemainder, {}}, 2, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"X64_VV_N", []decimal{{B0_63: 100}, negative101, {B0_63: 102}, {}}, []decimal{{}, {B0_63: 7}, negative11, {B0_63: 7}}, []decimal{{}, negative4, {B0_63: 8}, {}}, 2, 5, nil, []uint64{0}, false, false, 0},
		{"X64_SV_N", []decimal{negative101}, []decimal{{}, {B0_63: 7}, negative11}, []decimal{{}, negative4, {B0_63: 0xfffffffffffffff7, B64_127: 0xffffffffffffffff}}, 3, 6, nil, []uint64{0}, false, false, 0},
		{"X64_VS_N", []decimal{{B0_63: 100}, negative101, {B0_63: 102}}, []decimal{negative7}, []decimal{{B0_63: 1}, negative5, {B0_63: 2}}, 2, 6, nil, nil, false, false, 0},
		{"X64_VV_M", []decimal{{}, negative101, {}, {B0_63: 102}, {}}, []decimal{{}, {B0_63: 7}, {}, negative11, {}}, []decimal{{}, negative4, {}, {B0_63: 8}, {}}, 2, 5, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"X64_SV_M", []decimal{negative101}, []decimal{{}, {B0_63: 7}, {}, negative11, {}}, []decimal{{}, negative4, {}, {B0_63: 0xfffffffffffffff7, B64_127: 0xffffffffffffffff}, {}}, 2, 5, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"X64_VS_M", []decimal{{}, negative101, {}, {B0_63: 102}, {}}, []decimal{negative7}, []decimal{{}, negative4, {}, {B0_63: 3}, {}}, 2, 5, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"X64_VV_P", []decimal{{B0_63: 100}, negative101, {B0_63: 102}, {B0_63: 103}, {B0_63: 104}}, []decimal{{}, {B0_63: 7}, {}, negative11, {B0_63: 7}}, []decimal{{}, negative5, {}, {B0_63: 4}, {B0_63: 3}}, 2, 6, []uint64{0}, []uint64{0, 2}, false, false, 0},
		{"XW_VV_N", []decimal{wideDividend, wideDividend, negativeWideDividend, {B0_63: 101, B64_127: 3}}, []decimal{{}, wideDivisor, negativeWideDivisor, wideDivisor}, []decimal{{}, {B0_63: 79000000}, {B0_63: 0xfffffffffb4a8e40, B64_127: 0xffffffffffffffff}, {B0_63: 80000000}}, 2, 8, nil, []uint64{0}, false, false, 0},
		{"XW_SV_N", []decimal{negativeWideDividend}, []decimal{{}, wideDivisor, negativeDoubleWideDivisor}, []decimal{{}, {B0_63: 0xfffffffffb4a8e40, B64_127: 0xffffffffffffffff}, {B0_63: 0xfffffffffaaa56a0, B64_127: 0xffffffffffffffff}}, 2, 8, nil, []uint64{0}, false, false, 0},
		{"XW_VS_N", []decimal{wideDividend, negativeWideDividend, {B0_63: 101, B64_127: 3}}, []decimal{negativeWideDivisor}, []decimal{{B0_63: 79000000}, {B0_63: 0xfffffffffb4a8e40, B64_127: 0xffffffffffffffff}, {B0_63: 80000000}}, 2, 8, nil, nil, false, false, 0},
		{"XW_VV_M", []decimal{{}, wideDividend, {}, negativeWideDividend, {}}, []decimal{{}, wideDivisor, {}, negativeWideDivisor, {}}, []decimal{{}, {B0_63: 79000000}, {}, {B0_63: 0xfffffffffb4a8e40, B64_127: 0xffffffffffffffff}, {}}, 2, 8, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"XW_SV_M", []decimal{wideDividend}, []decimal{{}, wideDivisor, {}, negativeDoubleWideDivisor, {}}, []decimal{{}, {B0_63: 79000000}, {}, {B0_63: 89500000}, {}}, 2, 8, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"XW_VS_M", []decimal{{}, wideDividend, {}, negativeWideDividend, {}}, []decimal{negativeWideDivisor}, []decimal{{}, {B0_63: 79000000}, {}, {B0_63: 0xfffffffffb4a8e40, B64_127: 0xffffffffffffffff}, {}}, 2, 8, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"XW_VV_P", []decimal{maximum, wideDividend, maximum, negativeWideDividend, {B0_63: 101, B64_127: 3}}, []decimal{{}, wideDivisor, {}, negativeWideDivisor, wideDivisor}, []decimal{{}, {B0_63: 79000000}, {}, {B0_63: 0xfffffffffb4a8e40, B64_127: 0xffffffffffffffff}, {B0_63: 80000000}}, 2, 8, []uint64{0}, []uint64{0, 2}, false, false, 0},
		{"Y_VV_N", []decimal{{B0_63: 70001}, negative140002, {B0_63: 1000, B64_127: 20000}, {}}, []decimal{{}, {B0_63: 7}, wideDivisor, {B0_63: 7}}, []decimal{{}, negative2, {B0_63: 0xfffffffffffef278, B64_127: 9999}, {}}, 6, 2, nil, []uint64{0}, false, false, 0},
		{"Y_SV_N", []decimal{{B0_63: 1000, B64_127: 20000}}, []decimal{{}, wideDivisor, negativeDoubleWideDivisor}, []decimal{{}, {B0_63: 0xfffffffffffef278, B64_127: 9999}, {B0_63: 1000, B64_127: 20000}}, 6, 2, nil, []uint64{0}, false, false, 0},
		{"Y_VS_N", []decimal{{B0_63: 70001}, negative140002, {B0_63: 210003}}, []decimal{negative7}, []decimal{{B0_63: 1}, negative2, {B0_63: 3}}, 6, 2, nil, nil, false, false, 0},
		{"Y_VV_M", []decimal{{}, negative140002, {}, {B0_63: 1000, B64_127: 20000}, {}}, []decimal{{}, {B0_63: 7}, {}, wideDivisor, {}}, []decimal{{}, negative2, {}, {B0_63: 0xfffffffffffef278, B64_127: 9999}, {}}, 6, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"Y_SV_M", []decimal{{B0_63: 1000, B64_127: 20000}}, []decimal{{}, wideDivisor, {}, negativeDoubleWideDivisor, {}}, []decimal{{}, {B0_63: 0xfffffffffffef278, B64_127: 9999}, {}, {B0_63: 1000, B64_127: 20000}, {}}, 6, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"Y_VS_M", []decimal{{}, negative140002, {}, {B0_63: 210003}, {}}, []decimal{negative7}, []decimal{{}, negative2, {}, {B0_63: 3}, {}}, 6, 2, []uint64{0, 2, 4}, []uint64{0, 2, 4}, true, false, 0},
		{"YW_VS_N", []decimal{{B0_63: 100, B64_127: 20}, {B0_63: 0xffffffffffffff9c, B64_127: 0xffffffffffffffeb}, {B0_63: 100, B64_127: 30}}, []decimal{wideDivisor}, []decimal{{B0_63: 30, B64_127: 10}, {B0_63: 0xffffffffffffffe2, B64_127: 0xfffffffffffffff5}, {B0_63: 0xffffffffffffffd8, B64_127: 9}}, 1, 0, nil, nil, true, false, 0},
		{"X64_Factor19", []decimal{{B0_63: 17}, negative17}, []decimal{{B0_63: 7}}, []decimal{{B0_63: 2}, negative2}, 0, 19, nil, nil, true, false, 0},
		{"X64_Factor20Carry", []decimal{{B0_63: 17}, {B0_63: 0x2f394219248446bb}}, []decimal{{B0_63: 7}}, []decimal{{B0_63: 6}, {B0_63: 3}}, 0, 20, nil, nil, true, false, 0},
		{"X64_Factor38SignedRange", []decimal{{B0_63: 1}, {B0_63: 2}, negative2}, []decimal{{B0_63: 7}}, []decimal{{B0_63: 2}, {B0_63: 4}, negative4}, 0, 38, nil, nil, true, false, 0},
		{"X64_Fallback_VV", []decimal{maximum, negativeMaximum}, []decimal{{B0_63: 7}, negative11}, []decimal{{B0_63: 3}, negative5}, 0, 1, nil, nil, true, false, 0},
		{"X64_Fallback_SV", []decimal{maximum}, []decimal{{B0_63: 7}, negative11}, []decimal{{B0_63: 3}, {B0_63: 5}}, 0, 1, nil, nil, true, false, 0},
		{"XW_Fallback_VV", []decimal{maximum, negativeMaximum}, []decimal{wideDivisor, negativeWideDivisor}, []decimal{{B0_63: 235}, {B0_63: 0xffffffffffffff15, B64_127: 0xffffffffffffffff}}, 0, 1, nil, nil, true, false, 0},
		{"Y_Fallback", []decimal{{B0_63: 7}, negative7}, []decimal{maximum, negativeMaximum}, []decimal{{B0_63: 7}, negative7}, 2, 0, nil, nil, true, false, 0},
		{"Kernel", []decimal{{B0_63: 100}}, []decimal{{B0_63: 3}}, []decimal{{B0_63: 1}}, 2, 2, nil, nil, true, true, 0},
		{"S64_VV_E", []decimal{{B0_63: 100}, {B0_63: 101}}, []decimal{{}, {B0_63: 7}}, []decimal{{}, {B0_63: 3}}, 4, 4, nil, nil, true, false, moerr.ErrDivByZero},
		{"S64_SV_E", []decimal{{B0_63: 100}}, []decimal{{}, {B0_63: 7}}, []decimal{{}, {B0_63: 2}}, 4, 4, nil, nil, true, false, moerr.ErrDivByZero},
		{"S64_VS_E", []decimal{{B0_63: 100}, {B0_63: 101}}, []decimal{{}}, []decimal{{}, {}}, 4, 4, []uint64{0, 1}, []uint64{0, 1}, true, false, moerr.ErrDivByZero},
		{"S64_VS_Z", []decimal{{B0_63: 100}, {B0_63: 101}, {B0_63: 102}}, []decimal{{}}, []decimal{{}, {}, {}}, 4, 4, []uint64{0}, []uint64{0, 1, 2}, false, false, 0},
		{"X64_VV_E", []decimal{{B0_63: 100}, {B0_63: 101}}, []decimal{{}, {B0_63: 7}}, []decimal{{}, {B0_63: 5}}, 2, 6, nil, nil, true, false, moerr.ErrDivByZero},
		{"X64_SV_E", []decimal{{B0_63: 100}}, []decimal{{}, {B0_63: 7}}, []decimal{{}, {B0_63: 1}}, 2, 6, nil, nil, true, false, moerr.ErrDivByZero},
		{"X64_VS_E", []decimal{{B0_63: 100}, {B0_63: 101}}, []decimal{{}}, []decimal{{}, {}}, 2, 6, []uint64{0, 1}, []uint64{0, 1}, true, false, moerr.ErrDivByZero},
		{"X64_VS_Z", []decimal{{B0_63: 100}, {B0_63: 101}, {B0_63: 102}}, []decimal{{}}, []decimal{{}, {}, {}}, 2, 6, []uint64{0}, []uint64{0, 1, 2}, false, false, 0},
		{"Y_VV_E", []decimal{{B0_63: 100}, {B0_63: 101}}, []decimal{{}, {B0_63: 7}}, []decimal{{}, {B0_63: 101}}, 6, 2, nil, nil, true, false, moerr.ErrDivByZero},
		{"Y_SV_E", []decimal{{B0_63: 100}}, []decimal{{}, {B0_63: 7}}, []decimal{{}, {B0_63: 100}}, 6, 2, nil, nil, true, false, moerr.ErrDivByZero},
		{"Y_VS_E", []decimal{{B0_63: 100}, {B0_63: 101}}, []decimal{{}}, []decimal{{}, {}}, 6, 2, []uint64{0, 1}, []uint64{0, 1}, true, false, moerr.ErrDivByZero},
		{"Y_VS_Z", []decimal{{B0_63: 100}, {B0_63: 101}, {B0_63: 102}}, []decimal{{}}, []decimal{{}, {}, {}}, 6, 2, []uint64{0}, []uint64{0, 1, 2}, false, false, 0},
		{"SW_VV_E", []decimal{maximum, wideDividend}, []decimal{{}, wideDivisor}, []decimal{{}, {B0_63: 79}}, 4, 4, nil, nil, true, false, moerr.ErrDivByZero},
		{"SW_SV_E", []decimal{maximum}, []decimal{{}, wideDivisor}, []decimal{{}, {B0_63: 0x800000000000001b}}, 4, 4, nil, nil, true, false, moerr.ErrDivByZero},
		{"XW_VV_E", []decimal{maximum, wideDividend}, []decimal{{}, wideDivisor}, []decimal{{}, {B0_63: 790}}, 0, 1, nil, nil, true, false, moerr.ErrDivByZero},
		{"XW_SV_E", []decimal{maximum}, []decimal{{}, wideDivisor}, []decimal{{}, {B0_63: 235}}, 0, 1, nil, nil, true, false, moerr.ErrDivByZero},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := make([]decimal, len(tc.want))
			for i := range rs {
				rs[i] = decimal{B0_63: uint64(9000 + i), B64_127: 99}
			}
			nul := nulls.NewWithSize(len(rs))
			for _, i := range tc.initialNulls {
				nul.Add(i)
			}
			var err error
			if tc.throughFactory {
				err = d128ModKernel(tc.strict)(tc.x, tc.y, rs, tc.s1, tc.s2, nul)
			} else {
				err = d128Mod(tc.x, tc.y, rs, tc.s1, tc.s2, nul, tc.strict)
			}
			if tc.wantError != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.wantError), "got %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.wantNulls), nul.Count())
			for _, i := range tc.wantNulls {
				require.True(t, nul.Contains(i), "NULL row %d", i)
			}
			if tc.wantError != 0 {
				return // Error paths do not promise rollback of scratch results.
			}
			for _, i := range tc.initialNulls {
				require.Equal(t, decimal{B0_63: 9000 + i, B64_127: 99}, rs[i], "masked row %d", i)
			}
			for i, want := range tc.want {
				if !nul.Contains(uint64(i)) {
					require.Equal(t, want, rs[i], "row %d", i)
				}
			}
		})
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
		// Vector alignment at the bounded-factorization boundary.
		{
			"scale_boundary_38_left_vector",
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			[]types.Decimal256{{B0_63: 0x3}},
			0, 38, nil,
			[]types.Decimal256{{B0_63: 0x98a224000000003, B64_127: 0x4b3b4ca85a86c47a}, {B0_63: 0x1314448000000003, B64_127: 0x96769950b50d88f4}}, 0,
		},
		// Vector alignment at the bounded-factorization boundary.
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
		// Vector alignment at the bounded-factorization boundary.
		{
			"scale_boundary_39_left_vector",
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			[]types.Decimal256{{B0_63: 0x3}},
			0, 39, nil,
			[]types.Decimal256{{B0_63: 0x5f65568000000003, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0xbecaad0000000003, B64_127: 0xe0a1fd2712875988, B128_191: 0x5}}, 0,
		},
		// Vector alignment at the bounded-factorization boundary.
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
		// Raw signed-256 domain: coefficients at scale76 may exceed SQL precision65.
		{"scale_39_VV_left", []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 0, 39, nil, []types.Decimal256{{B0_63: 0x5f65568000000003, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0xa09aa97ffffffff9, B64_127: 0xfaf016c76bc533b, B128_191: 0xfffffffffffffffd, B192_255: 0xffffffffffffffff}}, 0},
		{"scale_39_VV_right", []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 39, 0, nil, []types.Decimal256{{B0_63: 0x5f65568000000003, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0xa09aa97ffffffff9, B64_127: 0xfaf016c76bc533b, B128_191: 0xfffffffffffffffd, B192_255: 0xffffffffffffffff}}, 0},
		{"scale_57_VV_left", []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 0, 57, nil, []types.Decimal256{{B0_63: 0x4a00000000000003, B64_127: 0xebfdcb54864ada83, B128_191: 0x28c87cb5c89a2571}, {B0_63: 0xb5fffffffffffff9, B64_127: 0x140234ab79b5257c, B128_191: 0xd737834a3765da8e, B192_255: 0xffffffffffffffff}}, 0},
		{"scale_57_VV_right", []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 57, 0, nil, []types.Decimal256{{B0_63: 0x4a00000000000003, B64_127: 0xebfdcb54864ada83, B128_191: 0x28c87cb5c89a2571}, {B0_63: 0xb5fffffffffffff9, B64_127: 0x140234ab79b5257c, B128_191: 0xd737834a3765da8e, B192_255: 0xffffffffffffffff}}, 0},
		{"scale_65_VV_left", []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 0, 65, nil, []types.Decimal256{{B0_63: 0x3, B64_127: 0x4e3945ef7a25360a, B128_191: 0x1c7fc3908a8bef46, B192_255: 0xf31627}, {B0_63: 0xfffffffffffffff9, B64_127: 0xb1c6ba1085dac9f5, B128_191: 0xe3803c6f757410b9, B192_255: 0xffffffffff0ce9d8}}, 0},
		{"scale_65_VV_right", []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 65, 0, nil, []types.Decimal256{{B0_63: 0x3, B64_127: 0x4e3945ef7a25360a, B128_191: 0x1c7fc3908a8bef46, B192_255: 0xf31627}, {B0_63: 0xfffffffffffffff9, B64_127: 0xb1c6ba1085dac9f5, B128_191: 0xe3803c6f757410b9, B192_255: 0xffffffffff0ce9d8}}, 0},
		{"scale_76_VV_left", []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 0, 76, nil, []types.Decimal256{{B0_63: 0x3, B64_127: 0x7775a5f171951000, B128_191: 0x764b4abe8652979, B192_255: 0x161bcca7119915b5}, {B0_63: 0xfffffffffffffff9, B64_127: 0x888a5a0e8e6aefff, B128_191: 0xf89b4b54179ad686, B192_255: 0xe9e43358ee66ea4a}}, 0},
		{"scale_76_VV_right", []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 76, 0, nil, []types.Decimal256{{B0_63: 0x3, B64_127: 0x7775a5f171951000, B128_191: 0x764b4abe8652979, B192_255: 0x161bcca7119915b5}, {B0_63: 0xfffffffffffffff9, B64_127: 0x888a5a0e8e6aefff, B128_191: 0xf89b4b54179ad686, B192_255: 0xe9e43358ee66ea4a}}, 0},
		{"scale_76_masked_alignment_overflow", []types.Decimal256{maxCoefficient, {B0_63: 1}}, []types.Decimal256{{B0_63: 0x3}}, 0, 76, []uint64{0}, []types.Decimal256{{}, {B0_63: 0x3, B64_127: 0x7775a5f171951000, B128_191: 0x764b4abe8652979, B192_255: 0x161bcca7119915b5}}, 0},
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
		// Vector alignment at the bounded-factorization boundary.
		{
			"scale_boundary_38_left_vector",
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			[]types.Decimal256{{B0_63: 0x3}},
			0, 38, nil,
			[]types.Decimal256{{B0_63: 0x98a223ffffffffd, B64_127: 0x4b3b4ca85a86c47a}, {B0_63: 0x1314447ffffffffd, B64_127: 0x96769950b50d88f4}}, 0,
		},
		// Vector alignment at the bounded-factorization boundary.
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
		// Vector alignment at the bounded-factorization boundary.
		{
			"scale_boundary_39_left_vector",
			[]types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}},
			[]types.Decimal256{{B0_63: 0x3}},
			0, 39, nil,
			[]types.Decimal256{{B0_63: 0x5f65567ffffffffd, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0xbecaacfffffffffd, B64_127: 0xe0a1fd2712875988, B128_191: 0x5}}, 0,
		},
		// Vector alignment at the bounded-factorization boundary.
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
		// Raw signed-256 domain: coefficients at scale76 may exceed SQL precision65.
		{"scale_39_VV_left", []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 0, 39, nil, []types.Decimal256{{B0_63: 0x5f65567ffffffffd, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0xa09aa98000000007, B64_127: 0xfaf016c76bc533b, B128_191: 0xfffffffffffffffd, B192_255: 0xffffffffffffffff}}, 0},
		{"scale_39_VV_right", []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 39, 0, nil, []types.Decimal256{{B0_63: 0xa09aa98000000003, B64_127: 0xfaf016c76bc533b, B128_191: 0xfffffffffffffffd, B192_255: 0xffffffffffffffff}, {B0_63: 0x5f65567ffffffff9, B64_127: 0xf050fe938943acc4, B128_191: 0x2}}, 0},
		{"scale_57_VV_left", []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 0, 57, nil, []types.Decimal256{{B0_63: 0x49fffffffffffffd, B64_127: 0xebfdcb54864ada83, B128_191: 0x28c87cb5c89a2571}, {B0_63: 0xb600000000000007, B64_127: 0x140234ab79b5257c, B128_191: 0xd737834a3765da8e, B192_255: 0xffffffffffffffff}}, 0},
		{"scale_57_VV_right", []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 57, 0, nil, []types.Decimal256{{B0_63: 0xb600000000000003, B64_127: 0x140234ab79b5257c, B128_191: 0xd737834a3765da8e, B192_255: 0xffffffffffffffff}, {B0_63: 0x49fffffffffffff9, B64_127: 0xebfdcb54864ada83, B128_191: 0x28c87cb5c89a2571}}, 0},
		{"scale_65_VV_left", []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 0, 65, nil, []types.Decimal256{{B0_63: 0xfffffffffffffffd, B64_127: 0x4e3945ef7a253609, B128_191: 0x1c7fc3908a8bef46, B192_255: 0xf31627}, {B0_63: 0x7, B64_127: 0xb1c6ba1085dac9f6, B128_191: 0xe3803c6f757410b9, B192_255: 0xffffffffff0ce9d8}}, 0},
		{"scale_65_VV_right", []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 65, 0, nil, []types.Decimal256{{B0_63: 0x3, B64_127: 0xb1c6ba1085dac9f6, B128_191: 0xe3803c6f757410b9, B192_255: 0xffffffffff0ce9d8}, {B0_63: 0xfffffffffffffff9, B64_127: 0x4e3945ef7a253609, B128_191: 0x1c7fc3908a8bef46, B192_255: 0xf31627}}, 0},
		{"scale_76_VV_left", []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 0, 76, nil, []types.Decimal256{{B0_63: 0xfffffffffffffffd, B64_127: 0x7775a5f171950fff, B128_191: 0x764b4abe8652979, B192_255: 0x161bcca7119915b5}, {B0_63: 0x7, B64_127: 0x888a5a0e8e6af000, B128_191: 0xf89b4b54179ad686, B192_255: 0xe9e43358ee66ea4a}}, 0},
		{"scale_76_VV_right", []types.Decimal256{{B0_63: 0x3}, {B0_63: 0xfffffffffffffff9, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, []types.Decimal256{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, 76, 0, nil, []types.Decimal256{{B0_63: 0x3, B64_127: 0x888a5a0e8e6af000, B128_191: 0xf89b4b54179ad686, B192_255: 0xe9e43358ee66ea4a}, {B0_63: 0xfffffffffffffff9, B64_127: 0x7775a5f171950fff, B128_191: 0x764b4abe8652979, B192_255: 0x161bcca7119915b5}}, 0},
		{"scale_76_masked_alignment_overflow", []types.Decimal256{maxCoefficient, {B0_63: 1}}, []types.Decimal256{{B0_63: 0x3}}, 0, 76, []uint64{0}, []types.Decimal256{{}, {B0_63: 0xfffffffffffffffd, B64_127: 0x7775a5f171950fff, B128_191: 0x764b4abe8652979, B192_255: 0x161bcca7119915b5}}, 0},
		// The final chunk crosses the signed bound after a successful first row.
		{"scale_76_late_alignment_overflow", []types.Decimal256{{B0_63: 1}, {B0_63: 6}}, []types.Decimal256{{B0_63: 3}}, 0, 76, nil, nil, moerr.ErrInvalidInput},
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
			name: "negative_power_divisor_VS",
			x:    []decimal{{B0_63: 17}, {B0_63: 19}},
			y:    []decimal{{B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			want: []decimal{{B0_63: 17}, {B0_63: 19}},
		},
		{
			name: "negative_power_divisor_late_VV",
			x:    []decimal{{B0_63: 17}, {B0_63: 19}},
			y:    []decimal{{B0_63: 5}, {B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			want: []decimal{{B0_63: 2}, {B0_63: 19}},
		},
		{
			name: "negative_power_divisor_late_SV",
			x:    []decimal{{B0_63: 17}},
			y:    []decimal{{B0_63: 5}, {B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}},
			want: []decimal{{B0_63: 2}, {B0_63: 17}},
		},

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
		{name: "scale_38_SS_left", x: []decimal{{B0_63: 0x6}}, y: []decimal{{B0_63: 0x7}}, want: []decimal{{B0_63: 0x5}}, s1: 0, s2: 38},
		{name: "scale_38_SS_right", x: []decimal{{B0_63: 0x3}}, y: []decimal{{B0_63: 0x1}}, want: []decimal{{B0_63: 0x3}}, s1: 38, s2: 0},
		{name: "scale_38_SV_left", x: []decimal{{B0_63: 0x6}}, y: []decimal{{B0_63: 0x7}, {B0_63: 0x7}}, want: []decimal{{B0_63: 0x5}, {B0_63: 0x5}}, s1: 0, s2: 38},
		{name: "scale_38_SV_right", x: []decimal{{B0_63: 0x3}}, y: []decimal{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, want: []decimal{{B0_63: 0x3}, {B0_63: 0x3}}, s1: 38, s2: 0},
		{name: "scale_38_VS_left", x: []decimal{{B0_63: 0x6}, {B0_63: 0xfffffffffffffffe, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, y: []decimal{{B0_63: 0x7}}, want: []decimal{{B0_63: 0x5}, {B0_63: 0xfffffffffffffffc, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, s1: 0, s2: 38},
		{name: "scale_38_VS_right", x: []decimal{{B0_63: 0x3}, {B0_63: 0x7}}, y: []decimal{{B0_63: 0x1}}, want: []decimal{{B0_63: 0x3}, {B0_63: 0x7}}, s1: 38, s2: 0},
		{name: "scale_38_VV_left", x: []decimal{{B0_63: 0x6}, {B0_63: 0xfffffffffffffffe, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, y: []decimal{{B0_63: 0x7}, {B0_63: 0x7}}, want: []decimal{{B0_63: 0x5}, {B0_63: 0xfffffffffffffffc, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, s1: 0, s2: 38},
		{name: "scale_38_VV_right", x: []decimal{{B0_63: 0x3}, {B0_63: 0x7}}, y: []decimal{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, want: []decimal{{B0_63: 0x3}, {B0_63: 0x7}}, s1: 38, s2: 0},
		{name: "scale_39_SS_left", x: []decimal{{B0_63: 0x6}}, y: []decimal{{B0_63: 0x7}}, want: []decimal{{B0_63: 0x1}}, s1: 0, s2: 39},
		{name: "scale_39_SS_right", x: []decimal{{B0_63: 0x3}}, y: []decimal{{B0_63: 0x1}}, want: []decimal{{B0_63: 0x3}}, s1: 39, s2: 0},
		{name: "scale_39_SV_left", x: []decimal{{B0_63: 0x6}}, y: []decimal{{B0_63: 0x7}, {B0_63: 0x7}}, want: []decimal{{B0_63: 0x1}, {B0_63: 0x1}}, s1: 0, s2: 39},
		{name: "scale_39_SV_right", x: []decimal{{B0_63: 0x3}}, y: []decimal{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, want: []decimal{{B0_63: 0x3}, {B0_63: 0x3}}, s1: 39, s2: 0},
		{name: "scale_39_VS_left", x: []decimal{{B0_63: 0x6}, {B0_63: 0xfffffffffffffffe, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, y: []decimal{{B0_63: 0x7}}, want: []decimal{{B0_63: 0x1}, {B0_63: 0xfffffffffffffffb, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, s1: 0, s2: 39},
		{name: "scale_39_VS_right", x: []decimal{{B0_63: 0x3}, {B0_63: 0x7}}, y: []decimal{{B0_63: 0x1}}, want: []decimal{{B0_63: 0x3}, {B0_63: 0x7}}, s1: 39, s2: 0},
		{name: "scale_39_VV_left", x: []decimal{{B0_63: 0x6}, {B0_63: 0xfffffffffffffffe, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, y: []decimal{{B0_63: 0x7}, {B0_63: 0x7}}, want: []decimal{{B0_63: 0x1}, {B0_63: 0xfffffffffffffffb, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, s1: 0, s2: 39},
		{name: "scale_39_VV_right", x: []decimal{{B0_63: 0x3}, {B0_63: 0x7}}, y: []decimal{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, want: []decimal{{B0_63: 0x3}, {B0_63: 0x7}}, s1: 39, s2: 0},
		{name: "scale_65_alignment_recovery", x: []decimal{{B0_63: 0x6}, {B0_63: 0xfffffffffffffffe, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, y: []decimal{{B0_63: 0x7}}, want: []decimal{{B0_63: 0x2}, {B0_63: 0xfffffffffffffffd, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, s1: 0, s2: 65},
		{name: "scale_76_alignment_recovery", x: []decimal{{B0_63: 0x6}, {B0_63: 0xfffffffffffffffe, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, y: []decimal{{B0_63: 0x7}}, want: []decimal{{B0_63: 0x3}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}, s1: 0, s2: 76},
		{name: "high_bits_VV", x: []decimal{{B0_63: 2000000000000000000}, {B0_63: 4000000000000000000}}, y: []decimal{{B64_127: 1 << 62}, {B64_127: 1 << 62}}, want: []decimal{{B64_127: 0x2eeb4be2e32a2000}, {B64_127: 0x1dd697c5c6544000}}, s1: 0, s2: 58},
		{name: "high_bits_SV", x: []decimal{{B0_63: 2000000000000000000}}, y: []decimal{{}, {B64_127: 1 << 62}}, want: []decimal{{}, {B64_127: 0x2eeb4be2e32a2000}}, s1: 0, s2: 58, masked: []uint64{0}},
		{name: "high_bits_VS", x: []decimal{{}, {B0_63: 2000000000000000000}, {B0_63: 4000000000000000000}}, y: []decimal{{B64_127: 1 << 62}}, want: []decimal{{}, {B64_127: 0x2eeb4be2e32a2000}, {B64_127: 0x1dd697c5c6544000}}, s1: 0, s2: 58, masked: []uint64{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nul := nulls.NewWithSize(len(tc.want))
			for _, i := range tc.masked {
				nul.Add(i)
			}
			got := make([]decimal, len(tc.want))
			for i := range got {
				got[i] = decimal{B0_63: 99}
			}
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
				} else {
					require.Equal(t, decimal{B0_63: 99}, got[i], "masked output row %d", i)
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

func TestD256ModHighBitsZeroPolicy(t *testing.T) {
	values := []types.Decimal256{{B0_63: 2000000000000000000}, {}, {B0_63: 4000000000000000000}}
	divisors := []types.Decimal256{{}, {}, {B64_127: 1 << 62}}
	for _, strict := range []bool{false, true} {
		masked := nulls.NewWithSize(3)
		masked.Add(1)
		sentinel := types.Decimal256FromInt64(99)
		got := []types.Decimal256{sentinel, sentinel, sentinel}
		err := d256Mod(values, divisors, got, 0, 58, masked, strict)
		if strict {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrDivByZero), "%v", err)
			require.Equal(t, []types.Decimal256{sentinel, sentinel, sentinel}, got)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, 2, masked.Count())
		require.True(t, masked.Contains(0))
		require.True(t, masked.Contains(1))
		require.False(t, masked.Contains(2))
		require.Equal(t, sentinel, got[0])
		require.Equal(t, sentinel, got[1])
		require.Equal(t, types.Decimal256{B64_127: 0x1dd697c5c6544000}, got[2])
	}
}

func TestD256IntDivHighMagnitude(t *testing.T) {
	// 2^254 / (2^253+1) truncates to one. The numerator cannot be doubled.
	x := types.Decimal256{B192_255: 1 << 62}
	y := types.Decimal256{B0_63: 1, B192_255: 1 << 61}
	for _, tc := range []struct {
		x, y types.Decimal256
		want int64
	}{
		{x, y, 1}, {x.Minus(), y, -1}, {x, y.Minus(), -1}, {x.Minus(), y.Minus(), 1},
		{types.Decimal256{B192_255: 1 << 63}, x, -2},
	} {
		got := make([]int64, 1)
		require.NoError(t, d256IntDiv([]types.Decimal256{tc.x}, []types.Decimal256{tc.y}, got, 0, 0, nulls.NewWithSize(1), true))
		require.Equal(t, tc.want, got[0])
	}
	got := make([]int64, 1)
	err := d256IntDiv([]types.Decimal256{x}, []types.Decimal256{types.Decimal256FromInt64(1)}, got, 0, 0, nulls.NewWithSize(1), true)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
	// Decimal128's widening consumer must also terminate and reject a quotient
	// outside Decimal128, rather than hang in the widened primitive.
	var widened types.Decimal128
	err = d128IntDivOne(types.Decimal128{B0_63: 2000000000000000000}, types.Decimal128{B64_127: 1 << 62}, &widened, 58, nulls.NewWithSize(1), 0, true, 0, 58)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
	require.Contains(t, err.Error(), "Decimal128 IntDiv overflow")
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

func TestD64IntDiv(t *testing.T) {
	type decimal = types.Decimal64
	for _, tc := range []struct {
		name               string
		x, y               []decimal
		want               []int64
		s1, s2             int32
		initial, wantNulls []uint64
		strict             bool
		errorCode          uint16
	}{
		{"S_VV_N", []decimal{18446744073709551515, 101, 0}, []decimal{3, 18446744073709551613, 7}, []int64{-33, -33, 0}, 0, 0, nil, nil, true, 0},
		{"S_SV_N", []decimal{101}, []decimal{3, 18446744073709551613, 7}, []int64{33, -33, 14}, 0, 0, nil, nil, true, 0},
		{"S_VS_N", []decimal{18446744073709551515, 101, 0}, []decimal{3}, []int64{-33, 33, 0}, 0, 0, nil, nil, true, 0},
		{"S_VV_P", []decimal{101, 101, 0}, []decimal{3, 0, 7}, []int64{33, 0, 0}, 0, 0, nil, []uint64{1}, false, 0},
		{"S_SV_P", []decimal{101}, []decimal{3, 0, 18446744073709551609}, []int64{33, 0, -14}, 0, 0, nil, []uint64{1}, false, 0},
		{"S_VS_Z", []decimal{101, 18446744073709551515, 0}, []decimal{0}, []int64{0, 0, 0}, 0, 0, nil, []uint64{0, 1, 2}, false, 0},
		{"S_VV_M", []decimal{101, 101, 18446744073709551515, 101}, []decimal{0, 0, 3, 18446744073709551613}, []int64{0, 0, -33, -33}, 0, 0, []uint64{0}, []uint64{0, 1}, false, 0},
		{"S_SV_M", []decimal{101}, []decimal{0, 0, 18446744073709551613, 7}, []int64{0, 0, -33, 14}, 0, 0, []uint64{0}, []uint64{0, 1}, false, 0},
		{"S_VS_M", []decimal{101, 18446744073709551515, 101}, []decimal{3}, []int64{0, -33, 33}, 0, 0, []uint64{0}, []uint64{0}, false, 0},
		{"S_VV_E", []decimal{101, 101, 0}, []decimal{3, 0, 7}, nil, 0, 0, nil, nil, true, moerr.ErrDivByZero},
		{"S_SV_E", []decimal{101}, []decimal{3, 0, 7}, nil, 0, 0, nil, nil, true, moerr.ErrDivByZero},
		{"S_VS_E", []decimal{101, 18446744073709551515, 0}, []decimal{0}, nil, 0, 0, nil, nil, true, moerr.ErrDivByZero},
		{"Y2_VV_N", []decimal{18446744073586094827, 123456789, 1}, []decimal{3, 18446744073709551613, 7}, []int64{-411522, -411522, 0}, 2, 0, nil, nil, true, 0},
		{"Y4_SV_N", []decimal{123456789}, []decimal{3, 18446744073709551613, 7}, []int64{4115, -4115, 1763}, 4, 0, nil, nil, false, 0},
		{"Y4_VS_N", []decimal{123456789, 18446744073586094827, 1}, []decimal{18446744073709551613}, []int64{-4115, 4115, 0}, 4, 0, nil, nil, false, 0},
		{"Y4_SV_M", []decimal{123456789}, []decimal{0, 18446744073709551613, 7}, []int64{0, -4115, 1763}, 4, 0, []uint64{0}, []uint64{0}, false, 0},
		{"Y4_VS_M", []decimal{123456789, 18446744073586094827, 1}, []decimal{18446744073709551613}, []int64{0, 4115, 0}, 4, 0, []uint64{0}, []uint64{0}, false, 0},
		{"Y4_VV_M", []decimal{123456789, 18446744073586094827, 1}, []decimal{0, 18446744073709551613, 7}, []int64{0, 4115, 0}, 4, 0, []uint64{0}, []uint64{0}, false, 0},
		{"Y4_SV_E", []decimal{123456789}, []decimal{3, 0, 7}, nil, 4, 0, nil, nil, true, moerr.ErrDivByZero},
		{"Y4_VS_E", []decimal{123456789, 18446744073586094827, 1}, []decimal{0}, nil, 4, 0, nil, nil, true, moerr.ErrDivByZero},
		{"Y4_VS_Z", []decimal{123456789, 18446744073586094827, 1}, []decimal{0}, []int64{0, 0, 0}, 4, 0, nil, []uint64{0, 1, 2}, false, 0},
		{"S_VV_strict_masked_zero", []decimal{101, 18446744073709551515, 101}, []decimal{0, 3, 18446744073709551613}, []int64{0, -33, -33}, 0, 0, []uint64{0}, []uint64{0}, true, 0},
		{"S_SV_strict_masked_zero", []decimal{101}, []decimal{0, 18446744073709551613, 7}, []int64{0, -33, 14}, 0, 0, []uint64{0}, []uint64{0}, true, 0},
		{"S_VS_all_masked_zero", []decimal{101, 18446744073709551515, 0}, []decimal{0}, nil, 0, 0, []uint64{0, 1, 2}, []uint64{0, 1, 2}, true, moerr.ErrDivByZero},
		{"int64_minimum", []decimal{9223372036854775808, 0}, []decimal{9223372036854775808, 1}, []int64{1, 0}, 0, 0, nil, nil, true, 0},
		{"int64_positive_overflow", []decimal{9223372036854775808, 0}, []decimal{18446744073709551615, 1}, nil, 0, 0, nil, nil, true, moerr.ErrOutOfRange},
		{"X14_VV_N", []decimal{18446744073709551515, 101, 0}, []decimal{3, 18446744073709551613, 7}, []int64{-3366666666666666, -3366666666666666, 0}, 0, 14, nil, nil, false, 0},
		{"X14_VV_M", []decimal{18446744073709551515, 101, 0}, []decimal{0, 18446744073709551613, 7}, []int64{0, -3366666666666666, 0}, 0, 14, []uint64{0}, []uint64{0}, false, 0},
		{"X14_SV_N", []decimal{101}, []decimal{3, 18446744073709551613, 7}, []int64{3366666666666666, -3366666666666666, 1442857142857142}, 0, 14, nil, nil, false, 0},
		{"X14_SV_M", []decimal{101}, []decimal{0, 18446744073709551613, 7}, []int64{0, -3366666666666666, 1442857142857142}, 0, 14, []uint64{0}, []uint64{0}, false, 0},
		{"X14_VS_N", []decimal{18446744073709551515, 101, 0}, []decimal{3}, []int64{-3366666666666666, 3366666666666666, 0}, 0, 14, nil, nil, false, 0},
		{"X14_VS_M", []decimal{18446744073709551515, 101, 0}, []decimal{3}, []int64{0, 3366666666666666, 0}, 0, 14, []uint64{0}, []uint64{0}, false, 0},
		{"Y8_VV_N", []decimal{999999999999999999, 17446744073709551617, 1}, []decimal{3, 18446744073709551613, 7}, []int64{3333333333, 3333333333, 0}, 8, 0, nil, nil, false, 0},
		{"Y8_SV_N", []decimal{999999999999999999}, []decimal{3, 18446744073709551613, 7}, []int64{3333333333, -3333333333, 1428571428}, 8, 0, nil, nil, false, 0},
		{"Y8_VS_N", []decimal{999999999999999999, 17446744073709551617, 1}, []decimal{3}, []int64{3333333333, -3333333333, 0}, 8, 0, nil, nil, false, 0},
		{"Y16_VV_N", []decimal{9223372036854775808, 9223372036854775807, 1}, []decimal{1, 18446744073709551615, 1}, []int64{-922, -922, 0}, 16, 0, nil, nil, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := max(len(tc.x), len(tc.y))
			rs := make([]int64, n)
			for i := range rs {
				rs[i] = 9000 + int64(i)
			}
			nul := nulls.NewWithSize(n)
			for _, i := range tc.initial {
				nul.Add(i)
			}
			err := d64IntDiv(tc.x, tc.y, rs, tc.s1, tc.s2, nul, tc.strict)
			if tc.errorCode != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.errorCode), "got %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.wantNulls), nul.Count())
			for _, i := range tc.wantNulls {
				require.True(t, nul.Contains(i), "NULL row %d", i)
			}
			if tc.errorCode != 0 {
				return
			} // Scratch results are not rolled back on error.
			require.Len(t, tc.want, n)
			for _, i := range tc.initial {
				require.Equal(t, int64(9000)+int64(i), rs[i], "masked row %d", i)
			}
			for i, want := range tc.want {
				if !nul.Contains(uint64(i)) {
					require.Equal(t, want, rs[i], "row %d", i)
				}
			}
		})
	}
}

func TestD128IntDiv(t *testing.T) {
	type decimal = types.Decimal128
	for _, tc := range []struct {
		name               string
		x, y               []decimal
		want               []int64
		s1, s2             int32
		initial, wantNulls []uint64
		strict             bool
		errorCode          uint16
	}{
		{"S_VV_N", []decimal{{B0_63: 18446744073709551515, B64_127: 18446744073709551615}, {B0_63: 101}, {}}, []decimal{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}}, []int64{-33, -33, 0}, 0, 0, nil, nil, true, 0},
		{"S_SV_N", []decimal{{B0_63: 101}}, []decimal{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}}, []int64{33, -33, 14}, 0, 0, nil, nil, true, 0},
		{"S_VS_N", []decimal{{B0_63: 18446744073709551515, B64_127: 18446744073709551615}, {B0_63: 101}, {}}, []decimal{{B0_63: 3}}, []int64{-33, 33, 0}, 0, 0, nil, nil, true, 0},
		{"S_VV_P", []decimal{{B0_63: 101}, {B0_63: 101}, {}}, []decimal{{B0_63: 3}, {}, {B0_63: 7}}, []int64{33, 0, 0}, 0, 0, nil, []uint64{1}, false, 0},
		{"S_SV_P", []decimal{{B0_63: 101}}, []decimal{{B0_63: 3}, {}, {B0_63: 18446744073709551609, B64_127: 18446744073709551615}}, []int64{33, 0, -14}, 0, 0, nil, []uint64{1}, false, 0},
		{"S_VS_Z", []decimal{{B0_63: 101}, {B0_63: 18446744073709551515, B64_127: 18446744073709551615}, {}}, []decimal{{}}, []int64{0, 0, 0}, 0, 0, nil, []uint64{0, 1, 2}, false, 0},
		{"S_VV_M", []decimal{{B0_63: 101}, {B0_63: 101}, {B0_63: 18446744073709551515, B64_127: 18446744073709551615}, {B0_63: 101}}, []decimal{{}, {}, {B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}}, []int64{0, 0, -33, -33}, 0, 0, []uint64{0}, []uint64{0, 1}, false, 0},
		{"S_SV_M", []decimal{{B0_63: 101}}, []decimal{{}, {}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}}, []int64{0, 0, -33, 14}, 0, 0, []uint64{0}, []uint64{0, 1}, false, 0},
		{"S_VS_M", []decimal{{B0_63: 101}, {B0_63: 18446744073709551515, B64_127: 18446744073709551615}, {B0_63: 101}}, []decimal{{B0_63: 3}}, []int64{0, -33, 33}, 0, 0, []uint64{0}, []uint64{0}, false, 0},
		{"S_VV_E", []decimal{{B0_63: 101}, {B0_63: 101}, {}}, []decimal{{B0_63: 3}, {}, {B0_63: 7}}, nil, 0, 0, nil, nil, true, moerr.ErrDivByZero},
		{"S_SV_E", []decimal{{B0_63: 101}}, []decimal{{B0_63: 3}, {}, {B0_63: 7}}, nil, 0, 0, nil, nil, true, moerr.ErrDivByZero},
		{"S_VS_E", []decimal{{B0_63: 101}, {B0_63: 18446744073709551515, B64_127: 18446744073709551615}, {}}, []decimal{{}}, nil, 0, 0, nil, nil, true, moerr.ErrDivByZero},
		{"Y2_VV_N", []decimal{{B0_63: 18446744073586094827, B64_127: 18446744073709551615}, {B0_63: 123456789}, {B0_63: 1}}, []decimal{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}}, []int64{-411522, -411522, 0}, 2, 0, nil, nil, true, 0},
		{"Y4_SV_N", []decimal{{B0_63: 123456789}}, []decimal{{B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}}, []int64{4115, -4115, 1763}, 4, 0, nil, nil, false, 0},
		{"Y4_VS_N", []decimal{{B0_63: 123456789}, {B0_63: 18446744073586094827, B64_127: 18446744073709551615}, {B0_63: 1}}, []decimal{{B0_63: 18446744073709551613, B64_127: 18446744073709551615}}, []int64{-4115, 4115, 0}, 4, 0, nil, nil, false, 0},
		{"Y4_SV_M", []decimal{{B0_63: 123456789}}, []decimal{{}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}}, []int64{0, -4115, 1763}, 4, 0, []uint64{0}, []uint64{0}, false, 0},
		{"Y4_VS_M", []decimal{{B0_63: 123456789}, {B0_63: 18446744073586094827, B64_127: 18446744073709551615}, {B0_63: 1}}, []decimal{{B0_63: 18446744073709551613, B64_127: 18446744073709551615}}, []int64{0, 4115, 0}, 4, 0, []uint64{0}, []uint64{0}, false, 0},
		{"Y4_VV_M", []decimal{{B0_63: 123456789}, {B0_63: 18446744073586094827, B64_127: 18446744073709551615}, {B0_63: 1}}, []decimal{{}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}}, []int64{0, 4115, 0}, 4, 0, []uint64{0}, []uint64{0}, false, 0},
		{"Y4_SV_E", []decimal{{B0_63: 123456789}}, []decimal{{B0_63: 3}, {}, {B0_63: 7}}, nil, 4, 0, nil, nil, true, moerr.ErrDivByZero},
		{"Y4_VS_E", []decimal{{B0_63: 123456789}, {B0_63: 18446744073586094827, B64_127: 18446744073709551615}, {B0_63: 1}}, []decimal{{}}, nil, 4, 0, nil, nil, true, moerr.ErrDivByZero},
		{"Y4_VS_Z", []decimal{{B0_63: 123456789}, {B0_63: 18446744073586094827, B64_127: 18446744073709551615}, {B0_63: 1}}, []decimal{{}}, []int64{0, 0, 0}, 4, 0, nil, []uint64{0, 1, 2}, false, 0},
		{"S_VV_strict_masked_zero", []decimal{{B0_63: 101}, {B0_63: 18446744073709551515, B64_127: 18446744073709551615}, {B0_63: 101}}, []decimal{{}, {B0_63: 3}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}}, []int64{0, -33, -33}, 0, 0, []uint64{0}, []uint64{0}, true, 0},
		{"S_SV_strict_masked_zero", []decimal{{B0_63: 101}}, []decimal{{}, {B0_63: 18446744073709551613, B64_127: 18446744073709551615}, {B0_63: 7}}, []int64{0, -33, 14}, 0, 0, []uint64{0}, []uint64{0}, true, 0},
		{"S_VS_all_masked_zero", []decimal{{B0_63: 101}, {B0_63: 18446744073709551515, B64_127: 18446744073709551615}, {}}, []decimal{{}}, nil, 0, 0, []uint64{0, 1, 2}, []uint64{0, 1, 2}, true, moerr.ErrDivByZero},
		{"int64_minimum", []decimal{{B0_63: 9223372036854775808, B64_127: 18446744073709551615}, {}}, []decimal{{B0_63: 9223372036854775808, B64_127: 18446744073709551615}, {B0_63: 1}}, []int64{1, 0}, 0, 0, nil, nil, true, 0},
		{"int64_positive_overflow", []decimal{{B0_63: 9223372036854775808, B64_127: 18446744073709551615}, {}}, []decimal{{B0_63: 18446744073709551615, B64_127: 18446744073709551615}, {B0_63: 1}}, nil, 0, 0, nil, nil, true, moerr.ErrOutOfRange},
		{"W_VV_P", []decimal{{B0_63: 101}, {B0_63: 5, B64_127: 1}, {B0_63: 18446744073709551611, B64_127: 18446744073709551614}, {B0_63: 101}}, []decimal{{B64_127: 1}, {B64_127: 1}, {B64_127: 1}, {}}, []int64{0, 1, -1, 0}, 0, 0, nil, []uint64{3}, false, 0},
		{"W_SV_P", []decimal{{B0_63: 5, B64_127: 1}}, []decimal{{B64_127: 1}, {B64_127: 18446744073709551615}, {}}, []int64{1, -1, 0}, 0, 0, nil, []uint64{2}, false, 0},
		{"W_VS_N", []decimal{{B0_63: 101}, {B0_63: 5, B64_127: 1}, {B0_63: 18446744073709551611, B64_127: 18446744073709551614}}, []decimal{{B64_127: 1}}, []int64{0, 1, -1}, 0, 0, nil, nil, false, 0},
		{"W_VV_M", []decimal{{B0_63: 5, B64_127: 1}, {B0_63: 5, B64_127: 1}, {B0_63: 18446744073709551611, B64_127: 18446744073709551614}}, []decimal{{}, {B64_127: 1}, {B64_127: 1}}, []int64{0, 1, -1}, 0, 0, []uint64{0}, []uint64{0}, false, 0},
		{"W_SV_M", []decimal{{B0_63: 5, B64_127: 1}}, []decimal{{}, {B64_127: 18446744073709551615}, {}, {B64_127: 5}}, []int64{0, -1, 0, 0}, 0, 0, []uint64{0}, []uint64{0, 2}, false, 0},
		{"Y4W_VV_N", []decimal{{B0_63: 5, B64_127: 1}, {B0_63: 18446744073709551611, B64_127: 18446744073709551614}, {}}, []decimal{{B64_127: 1}, {B64_127: 18446744073709551615}, {B64_127: 1}}, []int64{0, 0, 0}, 4, 0, nil, nil, false, 0},
		{"Y16W_VV_N", []decimal{{B0_63: 18446744073709551615, B64_127: 9223372036854775807}, {B0_63: 1, B64_127: 9223372036854775808}, {B0_63: 1}}, []decimal{{B64_127: 1}, {B64_127: 1}, {B64_127: 1}}, []int64{922, -922, 0}, 16, 0, nil, nil, false, 0},
		{"H_VV_N", []decimal{{B0_63: 5, B64_127: 100}, {B0_63: 18446744073709551611, B64_127: 18446744073709551515}, {B0_63: 101}}, []decimal{{B0_63: 999}, {B0_63: 18446744073709550617, B64_127: 18446744073709551615}, {B0_63: 3}}, []int64{1846520928299254416, 1846520928299254416, 33}, 0, 0, nil, nil, false, 0},
		{"H_SV_N", []decimal{{B0_63: 5, B64_127: 17}}, []decimal{{B0_63: 999}, {B0_63: 18446744073709550617, B64_127: 18446744073709551615}, {B0_63: 1000}}, []int64{313908557810873250, -313908557810873250, 313594649253062377}, 0, 0, nil, nil, false, 0},
		{"H_VS_N", []decimal{{B0_63: 5, B64_127: 100}, {B0_63: 18446744073709551611, B64_127: 18446744073709551515}, {B0_63: 101}}, []decimal{{B0_63: 999}}, []int64{1846520928299254416, -1846520928299254416, 0}, 0, 0, nil, nil, false, 0},
		{"H_VS_M", []decimal{{B0_63: 5, B64_127: 100}, {B0_63: 18446744073709551611, B64_127: 18446744073709551515}, {B0_63: 101}}, []decimal{{B0_63: 999}}, []int64{0, -1846520928299254416, 0}, 0, 0, []uint64{0}, []uint64{0}, false, 0},
		{"mixed_late_wide", []decimal{{B0_63: 101}, {B0_63: 5, B64_127: 1}, {B0_63: 18446744073709551611, B64_127: 18446744073709551614}}, []decimal{{B0_63: 3}, {B64_127: 1}, {B64_127: 1}}, []int64{33, 1, -1}, 0, 0, nil, nil, true, 0},
		{"admission18_VV", []decimal{{B0_63: 1}, {B0_63: 14326257276626284429, B64_127: 18446744073709551606}}, []decimal{{B0_63: 1}, {B0_63: 18446744073709551615}}, []int64{1000000000000000000, -9223372036854775808}, 0, 18, nil, nil, true, 0},
		{"fallback18_VV", []decimal{{B0_63: 1}, {B0_63: 14326257276626284428, B64_127: 18446744073709551606}}, []decimal{{B0_63: 1}, {B0_63: 18446744073709551615}}, []int64{1000000000000000000, -9223372036854775808}, 0, 18, nil, nil, true, 0},
		{"fallback18_SV", []decimal{{B0_63: 14326257276626284428, B64_127: 18446744073709551606}}, []decimal{{B0_63: 18446744073709551615}, {B0_63: 18446744073709551615}}, []int64{-9223372036854775808, -9223372036854775808}, 0, 18, nil, nil, true, 0},
		{"fallback18_VS", []decimal{{B0_63: 14326257276626284429, B64_127: 18446744073709551606}, {B0_63: 14326257276626284428, B64_127: 18446744073709551606}}, []decimal{{B0_63: 18446744073709551615}}, []int64{-9223372036854775808, -9223372036854775808}, 0, 18, nil, nil, true, 0},
		{"fallback18_masked", []decimal{{B0_63: 14326257276626284409, B64_127: 18446744073709551606}, {B0_63: 14326257276626284428, B64_127: 18446744073709551606}}, []decimal{{B0_63: 18446744073709551615}, {B0_63: 18446744073709551615}}, []int64{0, -9223372036854775808}, 0, 18, []uint64{0}, []uint64{0}, true, 0},
		{"noninline20_VV", []decimal{{B0_63: 18446744073709551615, B64_127: 18446744073709551615}, {B0_63: 1}, {}}, []decimal{{B0_63: 100}, {B0_63: 18446744073709551516, B64_127: 18446744073709551615}, {B0_63: 1}}, []int64{-1000000000000000000, -1000000000000000000, 0}, 0, 20, nil, nil, true, 0},
		{"minimum128_dividend", []decimal{{B64_127: 9223372036854775808}, {B0_63: 18446744073709551615, B64_127: 18446744073709551615}}, []decimal{{B0_63: 18446744073709551615}, {B0_63: 1}}, []int64{-9223372036854775808, -1}, 0, 0, nil, nil, true, 0},
		{"minimum128_divisor", []decimal{{B0_63: 18446744073709551615, B64_127: 9223372036854775807}, {B0_63: 18446744073709551614, B64_127: 9223372036854775807}}, []decimal{{B64_127: 9223372036854775808}}, []int64{0, 0}, 0, 0, nil, nil, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := max(len(tc.x), len(tc.y))
			rs := make([]int64, n)
			for i := range rs {
				rs[i] = 9000 + int64(i)
			}
			nul := nulls.NewWithSize(n)
			for _, i := range tc.initial {
				nul.Add(i)
			}
			err := d128IntDiv(tc.x, tc.y, rs, tc.s1, tc.s2, nul, tc.strict)
			if tc.errorCode != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.errorCode), "got %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.wantNulls), nul.Count())
			for _, i := range tc.wantNulls {
				require.True(t, nul.Contains(i), "NULL row %d", i)
			}
			if tc.errorCode != 0 {
				return
			} // Scratch results are not rolled back on error.
			require.Len(t, tc.want, n)
			for _, i := range tc.initial {
				require.Equal(t, int64(9000)+int64(i), rs[i], "masked row %d", i)
			}
			for i, want := range tc.want {
				if !nul.Contains(uint64(i)) {
					require.Equal(t, want, rs[i], "row %d", i)
				}
			}
		})
	}
}

func TestD256IntDiv(t *testing.T) {
	type decimal = types.Decimal256
	d := types.Decimal256FromInt64
	x := []decimal{d(25), d(-25), d(11)}
	y := []decimal{d(4), d(4), d(2)}
	sx, sy := []decimal{d(25)}, []decimal{d(4)}
	signedY := []decimal{d(4), d(-4), d(2)}
	scaledX := []decimal{d(250000), d(-250000), d(110000)}
	wideScale := decimal{B0_63: 7766279631452241920, B64_127: 5} // 10^20
	wideX, wideY := decimal{B128_191: 5}, decimal{B128_191: 2}
	gx, gy := []decimal{wideX, wideX.Minus(), wideX}, []decimal{wideY, wideY, wideY.Minus()}
	// This coefficient times ten exceeds signed128, but its negative quotient
	// by MaxUint64 is exactly MinInt64. Both operands still select D128 dispatch.
	rejected := decimal{B0_63: 0xcccccccccccccccd, B64_127: 0x0ccccccccccccccc}
	divisor := decimal{B0_63: ^uint64(0)}
	rx, ry := []decimal{rejected, rejected}, []decimal{divisor, divisor}
	nx := []decimal{rejected.Minus(), rejected.Minus()}
	for _, tc := range []struct {
		name               string
		x, y               []decimal
		s1, s2             int32
		initial, wantNulls []uint64
		strict             bool
		want               []int64
		errorCode          uint16
	}{
		{"VecVec_NoNull", x, y, 4, 4, nil, nil, false, []int64{6, -6, 5}, 0},
		{"VecVec_Nulls", x, y, 4, 4, []uint64{1}, []uint64{1}, false, []int64{6, 9001, 5}, 0},
		{"ConstLeft_NoNull", sx, signedY, 4, 4, nil, nil, false, []int64{6, -6, 12}, 0},
		{"ConstLeft_Nulls", sx, signedY, 4, 4, []uint64{1}, []uint64{1}, false, []int64{6, 9001, 12}, 0},
		{"ConstRight_NoNull", x, sy, 4, 4, nil, nil, false, []int64{6, -6, 2}, 0},
		{"ConstRight_Nulls", x, sy, 4, 4, []uint64{1}, []uint64{1}, false, []int64{6, 9001, 2}, 0},
		{"DiffScale_VecVec_NoNull", scaledX, y, 6, 2, nil, nil, false, []int64{6, -6, 5}, 0},
		{"DiffScale_VecVec_Nulls", scaledX, y, 6, 2, []uint64{1}, []uint64{1}, false, []int64{6, 9001, 5}, 0},
		{"PositiveAdjustmentGeneral", []decimal{d(3), d(-3), d(11)}, []decimal{wideScale, wideScale, wideScale}, 0, 20, nil, nil, false, []int64{3, -3, 11}, 0},
		{"GenericTruncationSigns", gx, gy, 0, 0, nil, nil, true, []int64{2, -2, -2}, 0},
		{"GenericSmall_VV", []decimal{d(25), d(-25)}, []decimal{wideY, wideY}, 2, 2, nil, nil, false, []int64{0, 0}, 0},
		{"GenericSmall_SV", sx, []decimal{wideY, wideY.Minus()}, 2, 2, nil, nil, false, []int64{0, 0}, 0},
		{"GenericSmall_VS", []decimal{d(25), d(-25)}, []decimal{wideY}, 2, 2, nil, nil, false, []int64{0, 0}, 0},
		{"GenericConstLeftMasked", []decimal{wideX}, []decimal{wideY, wideY.Minus(), wideY}, 0, 0, []uint64{1}, []uint64{1}, false, []int64{2, 9001, 2}, 0},
		{"GenericConstRightMasked", gx, []decimal{wideY}, 0, 0, []uint64{1}, []uint64{1}, false, []int64{2, 9001, 2}, 0},
		{"LateGenericPrescan", []decimal{d(25), wideX}, []decimal{d(4), wideY}, 0, 0, nil, nil, false, []int64{6, 2}, 0},
		{"NarrowTwoLimbDivisor", []decimal{wideScale, wideScale.Minus()}, []decimal{wideScale, wideScale}, 0, 0, nil, nil, false, []int64{1, -1}, 0},
		{"SingleZeroNull", []decimal{d(100)}, []decimal{{}}, 2, 2, nil, []uint64{0}, false, []int64{0}, 0},
		{"ScaleZero_VV", x, []decimal{d(4), {}, d(2)}, 0, 6, nil, []uint64{1}, false, []int64{6250000, 0, 5500000}, 0},
		{"ScaleZero_SV", sx, []decimal{{}, d(4), {}}, 0, 6, nil, []uint64{0, 2}, false, []int64{0, 6250000, 0}, 0},
		{"ScaleZeroMasked_SV", sx, []decimal{{}, d(4), {}}, 0, 6, []uint64{0}, []uint64{0, 2}, false, []int64{9000, 6250000, 0}, 0},
		{"ScaleZero_VS", x, []decimal{{}}, 0, 6, nil, []uint64{0, 1, 2}, false, []int64{0, 0, 0}, 0},
		{"GenericZero_VS", gx, []decimal{{}}, 0, 0, nil, []uint64{0, 1, 2}, false, []int64{0, 0, 0}, 0},
		{"Scale_SV", sx, signedY, 0, 6, nil, nil, false, []int64{6250000, -6250000, 12500000}, 0},
		{"ScaleMasked_SV", sx, signedY, 0, 6, []uint64{1}, []uint64{1}, false, []int64{6250000, 9001, 12500000}, 0},
		{"Scale_VS", x, sy, 0, 6, nil, nil, false, []int64{6250000, -6250000, 2750000}, 0},
		{"ScaleMasked_VS", x, sy, 0, 6, []uint64{1}, []uint64{1}, false, []int64{6250000, 9001, 2750000}, 0},
		{"StrictZero_VV", x, []decimal{{}, d(4), d(2)}, 0, 6, nil, nil, true, nil, moerr.ErrDivByZero},
		{"StrictZero_SV", sx, []decimal{{}, d(4), d(2)}, 0, 6, nil, nil, true, nil, moerr.ErrDivByZero},
		{"StrictZero_VS", x, []decimal{{}}, 0, 6, nil, nil, true, nil, moerr.ErrDivByZero},
		{"StrictMaskedZero_VV", x, []decimal{d(4), {}, d(2)}, 0, 6, []uint64{1}, []uint64{1}, true, []int64{6250000, 9001, 5500000}, 0},
		{"StrictAllMaskedZero_SV", sx, []decimal{{}, {}}, 0, 6, []uint64{0, 1}, []uint64{0, 1}, true, []int64{9000, 9001}, 0},
		{"StrictAllMaskedZero_VS", []decimal{d(25), d(-25)}, []decimal{{}}, 0, 6, []uint64{0, 1}, []uint64{0, 1}, true, nil, moerr.ErrDivByZero},
		{"GenericStrictAllMaskedZero_VS", []decimal{wideX, wideX.Minus()}, []decimal{{}}, 0, 0, []uint64{0, 1}, []uint64{0, 1}, true, nil, moerr.ErrDivByZero},
		{"GenericStrictMaskedZero_VV", gx, []decimal{wideY, {}, wideY}, 0, 0, []uint64{1}, []uint64{1}, true, []int64{2, 9001, 2}, 0},
		{"GenericStrictZero_SV", []decimal{wideX}, []decimal{{}, wideY}, 0, 0, nil, nil, true, nil, moerr.ErrDivByZero},
		{"InlineRejectNegative_VV", nx, ry, 0, 1, nil, nil, true, []int64{-9223372036854775808, -9223372036854775808}, 0},
		{"InlineRejectNegative_SV", nx[:1], ry, 0, 1, nil, nil, true, []int64{-9223372036854775808, -9223372036854775808}, 0},
		{"InlineRejectNegative_VS", nx, ry[:1], 0, 1, nil, nil, true, []int64{-9223372036854775808, -9223372036854775808}, 0},
		{"InlineRejectPositive_VV", rx, ry, 0, 1, nil, nil, true, nil, moerr.ErrOutOfRange},
		{"InlineRejectPositive_SV", rx[:1], ry, 0, 1, nil, nil, true, nil, moerr.ErrOutOfRange},
		{"InlineRejectPositive_VS", rx, ry[:1], 0, 1, nil, nil, true, nil, moerr.ErrOutOfRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := max(len(tc.x), len(tc.y))
			rs := make([]int64, n)
			for i := range rs {
				rs[i] = 9000 + int64(i)
			}
			nul := nulls.NewWithSize(n)
			for _, i := range tc.initial {
				nul.Add(i)
			}
			err := d256IntDiv(tc.x, tc.y, rs, tc.s1, tc.s2, nul, tc.strict)
			if tc.errorCode != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.errorCode), "got %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.wantNulls), nul.Count())
			for _, i := range tc.wantNulls {
				require.True(t, nul.Contains(i), "NULL row %d", i)
			}
			// Scratch results are not rolled back on errors. Initial masked rows are
			// never evaluated, including when a later row fails.
			for _, i := range tc.initial {
				require.Equal(t, 9000+int64(i), rs[i], "masked row %d", i)
			}
			if tc.errorCode != 0 {
				return
			}
			require.Len(t, tc.want, n)
			for i, want := range tc.want {
				if !nul.Contains(uint64(i)) {
					require.Equal(t, want, rs[i], "row %d", i)
				}
			}
		})
	}
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

// ---- D256 Mod with diff-scale ----

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
		name     string
		input    []types.Decimal128
		scale    int32
		nullRows []uint64
		want     []types.Decimal128
	}{
		{"AllFitInt64_NoNull", []types.Decimal128{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff}}, 5, nil, []types.Decimal128{{B0_63: 0x186a0}, {B0_63: 0xfffffffffffe7960, B64_127: 0xffffffffffffffff}}},
		{"AllFitInt64_WithNull", []types.Decimal128{{B0_63: 0x1}, {}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff}}, 5, []uint64{1}, []types.Decimal128{{B0_63: 0x186a0}, {}, {B0_63: 0xfffffffffffe7960, B64_127: 0xffffffffffffffff}}},
		{"LargeValues_NoNull", []types.Decimal128{{B0_63: 0x1, B64_127: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xfffffffffffffffe}}, 5, nil, []types.Decimal128{{B0_63: 0x186a0, B64_127: 0x186a0}, {B0_63: 0xfffffffffffe7960, B64_127: 0xfffffffffffe795f}}},
		{"LargeValues_WithNull", []types.Decimal128{{B0_63: 0x1, B64_127: 0x1}, {}, {B0_63: 0xffffffffffffffff, B64_127: 0xfffffffffffffffe}}, 5, []uint64{1}, []types.Decimal128{{B0_63: 0x186a0, B64_127: 0x186a0}, {}, {B0_63: 0xfffffffffffe7960, B64_127: 0xfffffffffffe795f}}},
		{"NineteenDigitFactor", []types.Decimal128{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff}}, 19, nil, []types.Decimal128{{B0_63: 0x8ac7230489e80000}, {B0_63: 0x7538dcfb76180000, B64_127: 0xffffffffffffffff}}},
		{"TwoStepThirtyEight", []types.Decimal128{{B0_63: 0x1}, {B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff}}, 38, nil, []types.Decimal128{{B0_63: 0x98a224000000000, B64_127: 0x4b3b4ca85a86c47a}, {B0_63: 0xf675ddc000000000, B64_127: 0xb4c4b357a5793b85}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := nulls.NewWithSize(len(tc.input))
			for _, row := range tc.nullRows {
				n.Add(row)
			}
			got := make([]types.Decimal128, len(tc.input))
			err := d128ScaleIntoRs(tc.input, got, len(got), tc.scale, n)
			require.NoError(t, err)
			require.Equal(t, len(tc.nullRows), n.Count())
			for i := range got {
				masked := false
				for _, row := range tc.nullRows {
					masked = masked || row == uint64(i)
				}
				require.Equal(t, masked, n.Contains(uint64(i)))
				if !masked {
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
	// Literal limbs are independently derived from integer coefficients times 10^n.
	for _, tc := range []struct {
		name    string
		n       int32
		x, want types.Decimal256
	}{
		{"scale_19", 19, types.Decimal256{B0_63: 0x1}, types.Decimal256{B0_63: 0x8ac7230489e80000}},
		{"scale_20", 20, types.Decimal256{B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}, types.Decimal256{B0_63: 0x9438a1d29cf00000, B64_127: 0xfffffffffffffffa, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}},
		{"scale_38", 38, types.Decimal256{B0_63: 0x1}, types.Decimal256{B0_63: 0x98a224000000000, B64_127: 0x4b3b4ca85a86c47a}},
		{"scale_39", 39, types.Decimal256{B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}, types.Decimal256{B0_63: 0xa09aa98000000000, B64_127: 0xfaf016c76bc533b, B128_191: 0xfffffffffffffffd, B192_255: 0xffffffffffffffff}},
		{"scale_57", 57, types.Decimal256{}, types.Decimal256{}},
		{"scale_65", 65, types.Decimal256{B0_63: 0xffffffffffffffff, B64_127: 0xffffffffffffffff, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}, types.Decimal256{B64_127: 0xb1c6ba1085dac9f6, B128_191: 0xe3803c6f757410b9, B192_255: 0xffffffffff0ce9d8}},
		{"scale_76", 76, types.Decimal256{B0_63: 0x1}, types.Decimal256{B64_127: 0x7775a5f171951000, B128_191: 0x764b4abe8652979, B192_255: 0x161bcca7119915b5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, remaining, b := d256ScaleUpFactors(tc.n)
			x := tc.x
			require.True(t, d256ScaleUpPow10(&x, a, remaining, b))
			require.Equal(t, tc.want, x)
		})
	}

	t.Run("ScaleUpPow10_OneStep", func(t *testing.T) {
		x := types.Decimal256{B0_63: 42}
		ok := d256ScaleUpPow10(&x, types.Pow10[3], 0, 0)
		require.True(t, ok)
		require.Equal(t, types.Decimal256{B0_63: 0xa410}, x)
	})

	t.Run("ScaleUpPow10_TwoStep", func(t *testing.T) {
		x := types.Decimal256{B0_63: 1}
		ok := d256ScaleUpPow10(&x, types.Pow10[19], 5, types.Pow10[5])
		require.True(t, ok)
		require.Equal(t, types.Decimal256{B0_63: 0x1bcecceda1000000, B64_127: 0xd3c2}, x)
	})

	t.Run("ScaleUpPow10_Negative", func(t *testing.T) {
		x := types.Decimal256{B0_63: ^uint64(42) + 1, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}
		ok := d256ScaleUpPow10(&x, types.Pow10[3], 0, 0)
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
		require.Equal(t, types.Decimal128{B0_63: 2}, r)
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
		require.Equal(t, types.Decimal128{B0_63: 93}, r)
	})
	// The absolute value of minimum D128 has bit 127 set. Both comparisons
	// in the unsigned remainder owner must handle that magnitude.
	for _, tc := range []struct {
		name       string
		x, y, want types.Decimal128
	}{
		{"MinimumWideDivisor", types.Decimal128{B64_127: 0x8000000000000000}, types.Decimal128{B64_127: 1}, types.Decimal128{}},
		{"MinimumCorrection", types.Decimal128{B64_127: 0x8000000000000000}, types.Decimal128{B0_63: 1, B64_127: 0x4000000000000000}, types.Decimal128{B0_63: 1, B64_127: 0xc000000000000000}},
		{"MinimumDivisor", types.Decimal128{B0_63: ^uint64(0), B64_127: 0x7fffffffffffffff}, types.Decimal128{B64_127: 0x8000000000000000}, types.Decimal128{B0_63: ^uint64(0), B64_127: 0x7fffffffffffffff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, d128ModOne(tc.x, tc.y))
		})
	}

}

func TestD128ModDiffScaleXPow10_Coverage(t *testing.T) {
	t.Run("OneStep", func(t *testing.T) {
		x := types.Decimal128{B0_63: 17}
		y := types.Decimal128{B0_63: 50}
		r, ok := d128ModDiffScaleXPow10(x, y, types.Pow10[1], false, 0) // scale x up by 10
		require.True(t, ok)
		// 170 % 50 = 20
		require.Equal(t, types.Decimal128{B0_63: 20}, r)
	})

	t.Run("TwoStep", func(t *testing.T) {
		x := types.Decimal128{B0_63: 1}
		y := types.Decimal128{B0_63: 7}
		r, ok := d128ModDiffScaleXPow10(x, y, types.Pow10[10], true, types.Pow10[5])
		require.True(t, ok)
		require.Equal(t, types.Decimal128{B0_63: 6}, r)
	})

	t.Run("Overflow", func(t *testing.T) {
		// Very large x that overflows when scaled
		x := types.Decimal128{B0_63: ^uint64(0), B64_127: 0x7FFFFFFFFFFFFFFF}
		y := types.Decimal128{B0_63: 3}
		_, ok := d128ModDiffScaleXPow10(x, y, types.Pow10[18], false, 0)
		require.False(t, ok)
	})
}

// TestMiscEdgePaths retains strict constant-zero IntDiv errors for both widths.
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

}

// =============================================================================
// Block-coverage tests: target uncovered dispatch branches
// =============================================================================

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
		{name: "scale_39_zero_strict", x: []decimal{{B0_63: 0x6}, {B0_63: 0x2}}, y: []decimal{{}, {B0_63: 0x7}}, want: []decimal{{}, {B0_63: 0x5}}, s1: 0, s2: 39, masked: []uint64{}, errorCode: moerr.ErrDivByZero, permissive: false},
		{name: "scale_39_zero_permissive", x: []decimal{{B0_63: 0x6}, {B0_63: 0x2}}, y: []decimal{{}, {B0_63: 0x7}}, want: []decimal{{}, {B0_63: 0x5}}, s1: 0, s2: 39, masked: []uint64{}, errorCode: 0, permissive: true},
		{name: "scale_39_zero_masked", x: []decimal{{B0_63: 0x6}, {B0_63: 0x2}}, y: []decimal{{}, {B0_63: 0x7}}, want: []decimal{{}, {B0_63: 0x5}}, s1: 0, s2: 39, masked: []uint64{0}, errorCode: 0, permissive: false},
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
