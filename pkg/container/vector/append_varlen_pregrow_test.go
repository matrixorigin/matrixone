// Copyright 2021 Matrix Origin
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

package vector

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestAppendVarlenListPregrow(t *testing.T) {
	values := make([][]byte, 32)
	for i := range values {
		switch i % 5 {
		case 0:
			values[i] = bytes.Repeat([]byte("中x"), 24)
		case 1:
			values[i] = []byte("short")
		case 2:
			values[i] = []byte{}
		case 3:
			values[i] = bytes.Repeat([]byte("a"), types.VarlenaInlineSize)
		case 4:
			values[i] = bytes.Repeat([]byte("b"), types.VarlenaInlineSize+1)
		}
	}
	strings := make([]string, len(values))
	expectedArea := 0
	for i, value := range values {
		strings[i] = string(value)
		if len(value) > types.VarlenaInlineSize {
			expectedArea += len(value)
		}
	}
	for _, tc := range []struct {
		name string
		oid  types.T
		add  func(*Vector, *mpool.MPool) error
	}{
		{"bytes", types.T_varbinary, func(v *Vector, mp *mpool.MPool) error {
			return AppendBytesList(v, values, nil, mp)
		}},
		{"string", types.T_varchar, func(v *Vector, mp *mpool.MPool) error {
			return AppendStringList(v, strings, nil, mp)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec := NewVec(tc.oid.ToType())
			reference := NewVec(tc.oid.ToType())
			decoded := NewVec(tc.oid.ToType())
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				vec.Free(mp)
				reference.Free(mp)
				decoded.Free(mp)
				require.Zero(t, mp.CurrNB())
				require.Zero(t, mp.OnHeapCurrNB())
			})
			require.NoError(t, tc.add(vec, mp))
			require.Equal(t, len(values), vec.Length())
			require.Equal(t, expectedArea, len(vec.area))
			require.GreaterOrEqual(t, cap(vec.area), len(vec.area))
			for i, value := range values {
				require.NoError(t, AppendBytes(reference, value, false, mp))
				require.Equal(t, value, vec.GetBytesAt(i))
				require.False(t, vec.IsNull(uint64(i)))
				require.Equal(t, PrepareParamNone, vec.GetPrepareParamKindAt(i))
				require.Equal(t, types.StringSourceExpression, vec.GetStringSourceAt(i))
			}
			wire, err := vec.MarshalBinary()
			require.NoError(t, err)
			referenceWire, err := reference.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, referenceWire, wire)
			require.NoError(t, decoded.UnmarshalBinary(wire))
			for i, value := range values {
				require.Equal(t, value, decoded.GetBytesAt(i))
			}
		})
	}
}

func TestAppendVarlenListPregrowGuards(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_varbinary.ToType())
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})
	long := bytes.Repeat([]byte("x"), 64)
	require.NoError(t, AppendBytesList(vec, [][]byte{long, long}, []bool{true, false}, mp))
	require.True(t, vec.IsNull(0))
	require.False(t, vec.IsNull(1))
	require.Equal(t, len(long), len(vec.area))
	require.NoError(t, AppendBytesList(vec, [][]byte{vec.GetBytesAt(1)}, nil, mp))
	require.Equal(t, long, vec.GetBytesAt(2))

	nullState := newTestVectorAllocationAccount(t, 2048, 8)
	t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, nullState) })
	nullVec := newAccountedTestVector(t, types.T_varbinary.ToType(), nullState.selection)
	t.Cleanup(func() { nullVec.Free(mp) })
	deadPayload := bytes.Repeat([]byte("x"), 256)
	nullValues := make([][]byte, 32)
	nullMask := make([]bool, len(nullValues))
	for i := range nullValues {
		nullValues[i] = deadPayload
		nullMask[i] = true
	}
	nullValues[1], nullMask[1] = []byte("ok"), false
	require.NoError(t, AppendBytesList(nullVec, nullValues, nullMask, mp))
	require.Equal(t, len(nullValues), nullVec.Length())
	require.Zero(t, len(nullVec.area))
	for i := range nullValues {
		require.Equal(t, nullMask[i], nullVec.IsNull(uint64(i)))
	}
	require.Equal(t, []byte("ok"), nullVec.GetBytesAt(1))

	state := newTestVectorAllocationAccount(t, 2048, 8)
	t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state) })
	unicode := newAccountedTestVector(t, types.NewWithCharset(types.T_varchar, 128, 0, types.CharsetUTF8MB4UnicodeCI), state.selection)
	t.Cleanup(func() { unicode.Free(mp) })
	invalid := string(append([]byte{0xff}, bytes.Repeat([]byte("x"), 127)...))
	values := make([]string, 32)
	for i := range values {
		values[i] = invalid
	}
	err := AppendStringList(unicode, values, nil, mp)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "got %v", err)
	require.Zero(t, unicode.Length())
}

func TestAppendVarlenListPregrowBudgetRollback(t *testing.T) {
	for _, tc := range []struct {
		name string
		oid  types.T
		add  func(*Vector, *mpool.MPool) error
	}{
		{"bytes", types.T_varbinary, func(v *Vector, mp *mpool.MPool) error {
			values := make([][]byte, 128)
			for i := range values {
				values[i] = bytes.Repeat([]byte("x"), 64)
			}
			return AppendBytesList(v, values, nil, mp)
		}},
		{"string", types.T_varchar, func(v *Vector, mp *mpool.MPool) error {
			values := make([]string, 128)
			for i := range values {
				values[i] = string(bytes.Repeat([]byte("x"), 64))
			}
			return AppendStringList(v, values, nil, mp)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestVectorAllocationAccount(t, 4096, 8)
			t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state) })
			mp := mpool.MustNewZero()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				require.Zero(t, mp.CurrNB())
				require.Zero(t, mp.OnHeapCurrNB())
			})
			vec := newAccountedTestVector(t, tc.oid.ToType(), state.selection)
			t.Cleanup(func() { vec.Free(mp) })
			require.ErrorIs(t, tc.add(vec, mp), mpool.ErrAllocationAccountCapacity)
			require.Zero(t, vec.Length())
			require.Zero(t, len(vec.area))
			require.NoError(t, AppendBytesList(vec, [][]byte{[]byte("ok")}, nil, mp))
			require.Equal(t, []byte("ok"), vec.GetBytesAt(0))
		})
	}
}
