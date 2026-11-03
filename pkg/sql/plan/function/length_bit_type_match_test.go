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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Issue #31183: LENGTH/OCTET_LENGTH/CHAR_LENGTH rejected a BIT column argument
// with "invalid argument function ..., bad value [BIT]". MySQL evaluates the
// family over the BIT value's fixed-width binary string form
// (Field_bit::val_str), so a BIT argument must bind the dedicated BIT
// overload without any cast, keeping the declared width M visible to the
// executor.
func TestLengthFamilyAdmitsBitArgument(t *testing.T) {
	proc := testutil.NewProcess(t)
	overloads := map[string]int32{
		"length":           4,
		"octet_length":     4,
		"char_length":      6,
		"character_length": 6,
	}
	for name, wantOverload := range overloads {
		for _, width := range []int32{1, 8, 9, 16, 64} {
			bitType := types.New(types.T_bit, width, 0)
			resolved, err := GetFunctionByName(proc.Ctx, name, []types.Type{bitType})
			require.NoError(t, err, "%s(bit(%d))", name, width)
			_, needCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, needCast, "%s(bit(%d)) must bind the BIT overload cast-free", name, width)
			require.Equal(t, types.T_int64, resolved.GetReturnType().Oid, "%s(bit(%d))", name, width)
			_, overload := DecodeOverloadID(resolved.GetEncodedOverloadID())
			require.Equal(t, wantOverload, overload, "%s(bit(%d))", name, width)
		}
	}
}

// MySQL's Field_bit::val_str materializes ceil(M/8) big-endian bytes with
// leading zero bytes preserved, so every non-NULL value of a BIT(M) column
// has the same length: the declared byte width. The value-0 and value-1 rows
// at widths 9 and 64 are the counterexamples against a minimal-encoding
// semantics (which would return 1 there); the 0xc3a9 row at width 16 also
// rejects UTF-8 code-point counting (which would return 1).
func TestLengthFamilyBitWidthSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	cases := []struct {
		width  int32
		values []uint64
		want   []int64
	}{
		{1, []uint64{0, 1}, []int64{1, 1}},
		{8, []uint64{0, 0x0f, 0xff}, []int64{1, 1, 1}},
		{9, []uint64{0, 1, 0x100}, []int64{2, 2, 2}},
		{16, []uint64{0, 0xc3a9, 0xffff}, []int64{2, 2, 2}},
		{64, []uint64{0, 1, 0x8000000000000000}, []int64{8, 8, 8}},
	}
	for _, c := range cases {
		values := append(append([]uint64{}, c.values...), 0)
		nullList := make([]bool, len(values))
		nullList[len(values)-1] = true
		want := append(append([]int64{}, c.want...), 0)

		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.New(types.T_bit, c.width, 0), values, nullList),
			},
			NewFunctionTestResult(types.T_int64.ToType(), false, want, nullList),
			LengthBit)
		succeed, info := tc.RunAndFree()
		require.True(t, succeed, "bit(%d): %s", c.width, info)
	}
}

// A BIT vector without a valid declared width falls back to the value's
// minimal big-endian byte count, keeping the executor total for planner
// expressions that lost their width metadata.
func TestLengthFamilyBitWidthFallback(t *testing.T) {
	proc := testutil.NewProcess(t)
	values := []uint64{0, 0xff, 0x0100, 0x8000000000000000}
	for _, width := range []int32{0, -1, 65} {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.New(types.T_bit, width, 0), values, nil),
			},
			NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 1, 2, 8}, nil),
			LengthBit)
		succeed, info := tc.RunAndFree()
		require.True(t, succeed, "bit(%d): %s", width, info)
	}
}

// Non-BIT inputs keep the existing string-domain binding: a native string is
// preserved without a cast and other scalar families keep their historical
// formatted-cast behavior.
func TestLengthFamilyKeepsNonBitBinding(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, typ := range []types.Type{
		types.T_varchar.ToType(),
		types.T_char.ToType(),
		types.T_text.ToType(),
		types.T_blob.ToType(),
		types.T_varbinary.ToType(),
	} {
		for _, name := range []string{"length", "octet_length", "char_length"} {
			resolved, err := GetFunctionByName(proc.Ctx, name, []types.Type{typ})
			require.NoError(t, err, "%s(%s)", name, typ.Oid)
			_, needCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, needCast, "%s(%s) must stay cast-free", name, typ.Oid)
			require.Equal(t, types.T_int64, resolved.GetReturnType().Oid)
		}
	}

	for _, name := range []string{"length", "octet_length", "char_length"} {
		resolved, err := GetFunctionByName(proc.Ctx, name, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err, "%s(int64)", name)
		targets, needCast := resolved.ShouldDoImplicitTypeCast()
		require.True(t, needCast, "%s(int64) keeps its historical cast", name)
		require.Len(t, targets, 1)
		require.Equal(t, types.T_varchar, targets[0].Oid)
	}
}
