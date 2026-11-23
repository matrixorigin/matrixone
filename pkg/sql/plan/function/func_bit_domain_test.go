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
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// issue #32122: explicit CAST(BIT AS BINARY)/(BINARY(n)) must encode exactly
// ceil(declaredWidth/8) big-endian bytes of the source declared width, before
// the target-length truncation (most significant bytes kept, MySQL rule) and
// right-padding. Oracle values: BIT(64) 258 -> 0000000000000102, 255 ->
// 00000000000000ff, uint64max -> ffffffffffffffff, NULL stays NULL, and the
// non-symmetric multi-byte case must not become the little-endian minimal
// 0201.
func TestExplicitCastBitToBinaryDeclaredWidthBytes(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)

	bit64 := types.New(types.T_bit, 64, 0)
	zeros8 := strings.Repeat("\x00", 8)
	zeros16 := strings.Repeat("\x00", 16)

	// byteFull returns the expected ceil(width/8) big-endian bytes of v.
	byteFull := func(sourceWidth int32, v uint64) string {
		byteWidth := int(sourceWidth+7) / 8
		if byteWidth < 1 {
			byteWidth = 1
		}
		var buf [8]byte
		for i := 0; i < byteWidth; i++ {
			buf[i] = byte(v >> (8 * (byteWidth - 1 - i)))
		}
		return string(buf[:byteWidth])
	}

	for _, tc := range []struct {
		name        string
		sourceWidth int32
		vals        []uint64
		nulls       []bool
		targetWidth int32 // -1 => CAST(b AS BINARY), else BINARY(n)
		targetScale int32
		want        []string
		wantNulls   []bool
	}{
		{"bit64_asymmetric_258_binary", 64, []uint64{258}, nil, -1, -1,
			[]string{byteFull(64, 258)}, nil},
		{"bit64_255_binary", 64, []uint64{255}, nil, -1, -1,
			[]string{byteFull(64, 255)}, nil},
		{"bit64_zero_binary", 64, []uint64{0}, nil, -1, -1,
			[]string{zeros8}, nil},
		{"bit64_uint64max_binary", 64, []uint64{math.MaxUint64}, nil, -1, -1,
			[]string{byteFull(64, math.MaxUint64)}, nil},
		{"bit64_null_binary", 64, []uint64{0}, []bool{true}, -1, -1,
			[]string{}, []bool{true}},
		{"bit64_asymmetric_258_binary8", 64, []uint64{258}, nil, 8, -1,
			[]string{byteFull(64, 258)}, nil},
		{"bit64_asymmetric_258_binary4_truncates_msb", 64, []uint64{258}, nil, 4, -1,
			[]string{string([]byte{0x00, 0x00, 0x00, 0x00})}, nil},
		{"bit64_asymmetric_258_binary2_truncates_msb", 64, []uint64{258}, nil, 2, -1,
			[]string{string([]byte{0x00, 0x00})}, nil},
		{"bit64_asymmetric_258_binary1", 64, []uint64{258}, nil, 1, -1,
			[]string{string([]byte{0x00})}, nil},
		{"bit64_asymmetric_258_binary16_right_pads", 64, []uint64{258}, nil, 16, -1,
			[]string{byteFull(64, 258) + zeros16[0:8]}, nil},
		{"bit64_uint64max_binary4", 64, []uint64{math.MaxUint64}, nil, 4, -1,
			[]string{strings.Repeat("\xff", 4)}, nil},
		{"bit64_value_and_null_rows", 64, []uint64{258, 0, math.MaxUint64}, []bool{false, true, false}, -1, -1,
			[]string{byteFull(64, 258), "", byteFull(64, math.MaxUint64)}, []bool{false, true, false}},
		{"bit1_flags_binary", 1, []uint64{0, 1}, nil, -1, -1,
			[]string{"\x00", "\x01"}, nil},
		{"bit8_255_binary", 8, []uint64{255}, nil, -1, -1,
			[]string{"\xff"}, nil},
		{"bit9_asymmetric_258_binary", 9, []uint64{258}, nil, -1, -1,
			[]string{"\x01\x02"}, nil},
		{"bit16_asymmetric_258_binary", 16, []uint64{258}, nil, -1, -1,
			[]string{"\x01\x02"}, nil},
		{"bit16_asymmetric_258_binary1_truncates_msb", 16, []uint64{258}, nil, 1, -1,
			[]string{"\x01"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.New(types.T_bit, tc.sourceWidth, 0), tc.vals, tc.nulls),
					NewFunctionTestInput(types.New(types.T_binary, tc.targetWidth, tc.targetScale), []string{}, nil),
				},
				NewFunctionTestResult(types.New(types.T_binary, tc.targetWidth, tc.targetScale), false,
					tc.want, tc.wantNulls), NewExplicitCast)
			succeed, info := tcc.RunAndFree()
			require.True(t, succeed, info)
		})
	}

	// Controls: the assignment-cast path (castModeAssignment, no explicit
	// Scale == -1 marker) keeps its current declared behavior - issue #32122
	// covers the explicit contract only; the assignment contract belongs to
	// the binary capacity/assignment work (#29057/#32002).
	t.Run("assignment_bit64_258_binary8_unchanged", func(t *testing.T) {
		tcc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(bit64, []uint64{258}, nil),
				NewFunctionTestInput(types.New(types.T_binary, 8, 0), []string{}, nil),
			},
			NewFunctionTestResult(types.New(types.T_binary, 8, 0), false,
				[]string{"\x01\x02" + strings.Repeat("\x00", 6)}, nil), NewAssignCast)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, info)
	})

	// Control: explicit CAST(BIT AS VARBINARY(n)) - the varbinary explicit
	// target is not the #32122 BINARY contract, keep its current bytes.
	t.Run("explicit_bit64_258_varbinary4_unchanged", func(t *testing.T) {
		tcc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(bit64, []uint64{258}, nil),
				NewFunctionTestInput(types.New(types.T_varbinary, 4, -1), []string{}, nil),
			},
			NewFunctionTestResult(types.New(types.T_varbinary, 4, -1), false,
				[]string{"\x01\x02"}, nil), NewExplicitCast)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, info)
	})
}

// issue #32125: native BIT input to the five value windows (LAG / LEAD /
// FIRST_VALUE / LAST_VALUE / NTH_VALUE) adopts the unsigned integer result
// domain: the binder inserts a real BIT -> UNSIGNED cast before the operator
// so the executed vectors and the derived column types (CTAS/VIEW) are
// BIGINT UNSIGNED, matching MySQL.
func TestValueWindowBitDomainNormalization(t *testing.T) {
	bit64 := types.New(types.T_bit, 64, 0)
	bit32 := types.New(types.T_bit, 32, 0)
	bit1 := types.New(types.T_bit, 1, 0)
	bigint := types.T_int64.ToType()
	int2 := types.T_int16.ToType()

	for _, in := range [][]types.Type{
		{bit64},
		{bit32},
		{bit1},
	} {
		t.Run(fmt.Sprintf("bit_width_%d", in[0].Width), func(t *testing.T) {
			res := checkValueWindowDomain(in)
			require.Equal(t, succeedWithCast, res.status)
			require.Len(t, res.finalType, len(in))
			require.Equal(t, types.T_uint64, res.finalType[0].Oid)
		})
	}

	t.Run("lag_bit_bit_default_coerced_with_result_domain", func(t *testing.T) {
		res := checkValueWindowDomain([]types.Type{bit64, bigint, bit64})
		require.Equal(t, succeedWithCast, res.status)
		require.Equal(t, types.T_uint64, res.finalType[0].Oid)
		require.Equal(t, bigint, res.finalType[1])
		require.Equal(t, types.T_uint64, res.finalType[2].Oid)
	})

	t.Run("lag_bit_non_bit_default_kept", func(t *testing.T) {
		res := checkValueWindowDomain([]types.Type{bit64, bigint, int2})
		require.Equal(t, succeedWithCast, res.status)
		require.Equal(t, types.T_uint64, res.finalType[0].Oid)
		require.Equal(t, int2, res.finalType[2])
	})

	t.Run("lag_bit_null_default_marker_kept", func(t *testing.T) {
		res := checkValueWindowDomain([]types.Type{bit64, bigint, types.T_any.ToType()})
		require.Equal(t, succeedWithCast, res.status)
		require.Equal(t, types.T_uint64, res.finalType[0].Oid)
		require.Equal(t, types.T_any, res.finalType[2].Oid)
	})

	// Controls: existing domains keep their contracts.
	t.Run("any_domain_null_uses_text_transport", func(t *testing.T) {
		res := checkValueWindowDomain([]types.Type{types.T_any.ToType()})
		require.Equal(t, succeedWithCast, res.status)
		require.Equal(t, types.T_text, res.finalType[0].Oid)
	})

	t.Run("signed_integer_keeps_domain_no_cast", func(t *testing.T) {
		res := checkValueWindowDomain([]types.Type{bigint})
		require.Equal(t, succeedMatched, res.status)
	})

	t.Run("varchar_keeps_domain_no_cast", func(t *testing.T) {
		res := checkValueWindowDomain([]types.Type{types.T_varchar.ToType()})
		require.Equal(t, succeedMatched, res.status)
	})
}
