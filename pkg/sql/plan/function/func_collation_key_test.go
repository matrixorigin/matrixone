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

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCollationKeyVectorAndTupleAgree(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8)
	values := []string{"Alpha", "alpha ", "a\x00", " ", "中😀", "ignored"}
	nsp := new(nulls.Nulls)
	nsp.Add(5)
	input := newVectorByType(proc.Mp(), typ, values, nsp)
	defer input.Free(proc.Mp())
	charset := mustNewConstFixed(t, types.T_uint64.ToType(), uint64(types.CharsetUTF8), proc)
	defer charset.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_varbinary.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(len(values)))
	require.NoError(t, internalCollationKey([]*vector.Vector{input, charset}, result, proc, len(values), nil))
	part, err := types.ResolveStringKeyPart(typ, types.PADSpaceKeyV1)
	require.NoError(t, err)
	p := types.NewPacker()
	defer p.Close()
	for i, v := range values {
		if i == 5 {
			require.True(t, result.GetResultVector().IsNull(5))
			continue
		}
		p.Reset()
		_, err = part.Encode(p, nil, []byte(v))
		require.NoError(t, err)
		decoded, _, err := part.Decode(p.GetBuf())
		require.NoError(t, err)
		require.Equal(t, decoded.Bytes, result.GetResultVector().GetBytesAt(i))
	}
	require.Equal(t, result.GetResultVector().GetBytesAt(0), result.GetResultVector().GetBytesAt(1))
	resolved, err := GetFunctionByName(proc.Ctx, "internal_collation_key", []types.Type{typ, types.T_uint64.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.CharsetBinary, resolved.GetReturnType().Charset)
}

func TestCollationKeyConstantsAndSelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := types.T_varchar.ToType()
	charset := mustNewConstFixed(t, types.T_uint64.ToType(), uint64(types.CharsetUTF8), proc)
	defer charset.Free(proc.Mp())
	constant, err := vector.NewConstBytes(typ, []byte("Alpha"), 3, proc.Mp())
	require.NoError(t, err)
	defer constant.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_varbinary.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(3))
	require.NoError(t, internalCollationKey([]*vector.Vector{constant, charset}, result, proc, 3, nil))
	for i := 1; i < 3; i++ {
		require.Equal(t, result.GetResultVector().GetBytesAt(0), result.GetResultVector().GetBytesAt(i))
	}
	bad := newVectorByType(proc.Mp(), typ, []string{"Alpha", "\xff", "alpha"}, nil)
	defer bad.Free(proc.Mp())
	require.NoError(t, result.PreExtendAndReset(3))
	selection := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}
	require.NoError(t, internalCollationKey([]*vector.Vector{bad, charset}, result, proc, 3, selection))
	require.True(t, result.GetResultVector().IsNull(1))
	require.NoError(t, result.PreExtendAndReset(3))
	require.Error(t, internalCollationKey([]*vector.Vector{bad, charset}, result, proc, 3, nil))
	unknown := mustNewConstFixed(t, types.T_uint64.ToType(), uint64(256), proc)
	defer unknown.Free(proc.Mp())
	require.Error(t, internalCollationKey([]*vector.Vector{constant, unknown}, result, proc, 3, nil))
	nonconst := newVectorByType(proc.Mp(), types.T_uint64.ToType(), []uint64{3, 3, 3}, nil)
	defer nonconst.Free(proc.Mp())
	require.Error(t, internalCollationKey([]*vector.Vector{constant, nonconst}, result, proc, 3, nil))
}

func TestCollationKeyConvertsBinaryProtocolInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	inputType := types.NewWithCharset(types.T_varbinary, 32, 0, types.CharsetBinary)
	input := newVectorByType(proc.Mp(), inputType, []string{"Alpha", "alpha"}, nil)
	defer input.Free(proc.Mp())
	charset := mustNewConstFixed(
		t, types.T_uint64.ToType(), uint64(types.CharsetUTF8), proc,
	)
	defer charset.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_varbinary.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(2))
	require.NoError(t, internalCollationKey([]*vector.Vector{input, charset}, result, proc, 2, nil))

	part, err := types.ResolveStringKeyPart(
		types.NewWithCharsetVersion(types.T_varchar, 32, 0, types.CharsetUTF8, types.CollationVersionV1),
		types.PADSpaceKeyV1,
	)
	require.NoError(t, err)
	want, err := part.Key(nil, []byte("Alpha"))
	require.NoError(t, err)
	require.Equal(t, want, result.GetResultVector().GetBytesAt(0))
	require.Equal(t, want, result.GetResultVector().GetBytesAt(1))
}

func TestNative0900CollationKeysPreserveIdentityRules(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, charset := range []uint8{types.CharsetUTF8MB40900AI, types.CharsetUTF8MB40900Bin} {
		typ := types.NewWithCharset(types.T_varchar, 64, 0, charset)
		input := newVectorByType(proc.Mp(), typ, []string{"Alpha", "alpha", "Alpha "}, nil)
		defer input.Free(proc.Mp())
		charsetVec := mustNewConstFixed(t, types.T_uint64.ToType(), uint64(charset), proc)
		defer charsetVec.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(types.T_varbinary.ToType(), proc.Mp())
		require.NoError(t, result.PreExtendAndReset(3))
		require.NoError(t, internalCollationKey([]*vector.Vector{input, charsetVec}, result, proc, 3, nil))
		if charset == types.CharsetUTF8MB40900AI {
			require.Equal(t, result.GetResultVector().GetBytesAt(0), result.GetResultVector().GetBytesAt(1))
		} else {
			require.NotEqual(t, result.GetResultVector().GetBytesAt(0), result.GetResultVector().GetBytesAt(1))
		}
		require.NotEqual(t, result.GetResultVector().GetBytesAt(0), result.GetResultVector().GetBytesAt(2))
		result.Free()
	}
}
