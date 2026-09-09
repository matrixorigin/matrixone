// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/collationkey"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func collationKeyV2TestInputs(t *testing.T, proc *process.Process, procType types.Type, value []byte, charset int64) []*vector.Vector {
	t.Helper()
	valueVec, err := vector.NewConstBytes(procType, value, 1, proc.Mp())
	require.NoError(t, err)
	prefixVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(0), 1, proc.Mp())
	require.NoError(t, err)
	charsetVec, err := vector.NewConstFixed(types.T_int64.ToType(), charset, 1, proc.Mp())
	require.NoError(t, err)
	return []*vector.Vector{valueVec, prefixVec, charsetVec}
}

func TestBuiltInCollationKeyV2UsesSharedIdentity(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8)

	encode := func(t *testing.T, value []byte, charset int64) []byte {
		t.Helper()
		inputs := collationKeyV2TestInputs(t, proc, typ, value, charset)
		for _, input := range inputs {
			t.Cleanup(func() { input.Free(proc.Mp()) })
		}
		out, err := RunFunctionDirectly(proc, CollationKeyV2FunctionEncodedID, inputs, 1)
		require.NoError(t, err)
		t.Cleanup(func() { out.Free(proc.Mp()) })
		return append([]byte(nil), out.GetBytesAt(0)...)
	}

	require.Equal(t, encode(t, []byte("Alpha"), int64(types.CharsetUTF8)), encode(t, []byte("alpha "), int64(types.CharsetUTF8)))
	binType := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8MB4Bin)
	binInputs := func(value []byte) []*vector.Vector {
		inputs := collationKeyV2TestInputs(t, proc, binType, value, int64(types.CharsetUTF8MB4Bin))
		for _, input := range inputs {
			t.Cleanup(func() { input.Free(proc.Mp()) })
		}
		return inputs
	}
	aInputs, bInputs := binInputs([]byte("Alpha")), binInputs([]byte("alpha"))
	a, err := RunFunctionDirectly(proc, CollationKeyV2FunctionEncodedID, aInputs, 1)
	require.NoError(t, err)
	b, err := RunFunctionDirectly(proc, CollationKeyV2FunctionEncodedID, bInputs, 1)
	require.NoError(t, err)
	require.NotEqual(t, a.GetBytesAt(0), b.GetBytesAt(0))
	a.Free(proc.Mp())
	b.Free(proc.Mp())

	// The expected bytes are produced by the package shared with future
	// sidecar writers, rather than by a duplicate test normalizer.
	want, err := collationkey.EncodePart(nil, collationkey.Part{
		Domain: collationkey.Domain{Type: collationkey.Text, Charset: collationkey.CharsetUTF8, Unit: collationkey.PrefixCharacters},
		Value:  []byte("Alpha"),
	})
	require.NoError(t, err)
	require.True(t, bytes.Equal(want, encode(t, []byte("Alpha"), int64(types.CharsetUTF8))))
}

func TestBuiltInCollationKeyV2PreservesNullAndRejectsBadDescriptors(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8)
	value := vector.NewConstNull(typ, 1, proc.Mp())
	prefix, err := vector.NewConstFixed(types.T_int64.ToType(), int64(0), 1, proc.Mp())
	require.NoError(t, err)
	charset, err := vector.NewConstFixed(types.T_int64.ToType(), int64(types.CharsetUTF8), 1, proc.Mp())
	require.NoError(t, err)
	defer value.Free(proc.Mp())
	defer prefix.Free(proc.Mp())
	defer charset.Free(proc.Mp())
	out, err := RunFunctionDirectly(proc, CollationKeyV2FunctionEncodedID, []*vector.Vector{value, prefix, charset}, 1)
	require.NoError(t, err)
	require.True(t, out.IsNull(0))
	out.Free(proc.Mp())

	badPrefix, err := vector.NewConstFixed(types.T_int64.ToType(), int64(-1), 1, proc.Mp())
	require.NoError(t, err)
	defer badPrefix.Free(proc.Mp())
	_, err = RunFunctionDirectly(proc, CollationKeyV2FunctionEncodedID, []*vector.Vector{value, badPrefix, charset}, 1)
	require.Error(t, err)

	badCharset, err := vector.NewConstFixed(types.T_int64.ToType(), int64(types.CharsetBinary), 1, proc.Mp())
	require.NoError(t, err)
	defer badCharset.Free(proc.Mp())
	_, err = RunFunctionDirectly(proc, CollationKeyV2FunctionEncodedID, []*vector.Vector{value, prefix, badCharset}, 1)
	require.Error(t, err)

	invalid, err := vector.NewConstBytes(typ, []byte{0xff}, 1, proc.Mp())
	require.NoError(t, err)
	defer invalid.Free(proc.Mp())
	_, err = RunFunctionDirectly(proc, CollationKeyV2FunctionEncodedID, []*vector.Vector{invalid, prefix, charset}, 1)
	require.Error(t, err)
}

func TestBuiltInCollationCompositeKeyV2UsesFramedParts(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8)
	newInputs := func(v1, v2 string) []*vector.Vector {
		one, err := vector.NewConstBytes(typ, []byte(v1), 1, proc.Mp())
		require.NoError(t, err)
		two, err := vector.NewConstBytes(typ, []byte(v2), 1, proc.Mp())
		require.NoError(t, err)
		prefix1, err := vector.NewConstFixed(types.T_int64.ToType(), int64(0), 1, proc.Mp())
		require.NoError(t, err)
		prefix2, err := vector.NewConstFixed(types.T_int64.ToType(), int64(0), 1, proc.Mp())
		require.NoError(t, err)
		charset1, err := vector.NewConstFixed(types.T_int64.ToType(), int64(types.CharsetUTF8), 1, proc.Mp())
		require.NoError(t, err)
		charset2, err := vector.NewConstFixed(types.T_int64.ToType(), int64(types.CharsetUTF8), 1, proc.Mp())
		require.NoError(t, err)
		return []*vector.Vector{one, prefix1, charset1, two, prefix2, charset2}
	}
	inputs := newInputs("Alpha", "Beta")
	for _, input := range inputs {
		t.Cleanup(func() { input.Free(proc.Mp()) })
	}
	out, err := RunFunctionDirectly(proc, CollationCompositeKeyV2FunctionEncodedID, inputs, 1)
	require.NoError(t, err)
	t.Cleanup(func() { out.Free(proc.Mp()) })
	want, err := collationkey.EncodeComposite(nil, []collationkey.Part{
		{Domain: collationkey.Domain{Type: collationkey.Text, Charset: collationkey.CharsetUTF8, Unit: collationkey.PrefixCharacters}, Value: []byte("Alpha")},
		{Domain: collationkey.Domain{Type: collationkey.Text, Charset: collationkey.CharsetUTF8, Unit: collationkey.PrefixCharacters}, Value: []byte("Beta")},
	})
	require.NoError(t, err)
	require.Equal(t, want, out.GetBytesAt(0))

	equivalent := newInputs("alpha ", "BETA")
	for _, input := range equivalent {
		t.Cleanup(func() { input.Free(proc.Mp()) })
	}
	eqOut, err := RunFunctionDirectly(proc, CollationCompositeKeyV2FunctionEncodedID, equivalent, 1)
	require.NoError(t, err)
	t.Cleanup(func() { eqOut.Free(proc.Mp()) })
	require.Equal(t, out.GetBytesAt(0), eqOut.GetBytesAt(0))
}

func TestCollationKeyV2IsPlannerOnly(t *testing.T) {
	_, ok := GetFunctionByNameWithoutError("__mo_collation_key_v2", []types.Type{
		types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
	})
	require.False(t, ok)

	check := collationKeyV2TypeMatch(nil, []types.Type{
		types.T_text.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
	})
	require.Equal(t, succeedMatched, check.status)
	check = collationKeyV2TypeMatch(nil, []types.Type{
		types.T_char.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
	})
	require.Equal(t, failedFunctionParametersWrong, check.status)
}
