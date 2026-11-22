// Copyright 2021 - 2026 Matrix Origin
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

package vector

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// newAppendTestVec returns a vector and pool that are freed, and checked for
// leaks, when the test ends.
func newAppendTestVec(t *testing.T, typ types.Type) (*Vector, *mpool.MPool) {
	t.Helper()
	mp := mpool.MustNewZero()
	vec := NewVec(typ)
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})
	return vec, mp
}

// TestAppendFixedListBulkCopy covers the null-free bulk copy, with no mask
// and with an all-false mask.
func TestAppendFixedListBulkCopy(t *testing.T) {
	values := make([]int64, 32)
	for i := range values {
		values[i] = int64(i * 11)
	}
	for _, mask := range [][]bool{nil, make([]bool, len(values))} {
		vec, mp := newAppendTestVec(t, types.T_int64.ToType())
		require.NoError(t, vec.PreExtend(len(values), mp))
		require.NoError(t, AppendFixedList(vec, values, mask, mp))
		require.Equal(t, values, MustFixedColWithTypeCheck[int64](vec))
		for i := range values {
			require.False(t, vec.IsNull(uint64(i)))
		}
	}
}

// TestAppendFixedListBulkCopyAfterNullablePrefix appends a null-free list
// after rows that already carry a null bitmap.
func TestAppendFixedListBulkCopyAfterNullablePrefix(t *testing.T) {
	vec, mp := newAppendTestVec(t, types.T_int64.ToType())
	require.NoError(t, AppendFixedList(vec, []int64{1, 2}, []bool{false, true}, mp))
	values := make([]int64, 32)
	for i := range values {
		values[i] = int64(i + 10)
	}
	require.NoError(t, AppendFixedList(vec, values, nil, mp))
	require.Equal(t, 34, vec.Length())
	require.Equal(t, int64(1), GetFixedAtNoTypeCheck[int64](vec, 0))
	require.True(t, vec.IsNull(1))
	for i, value := range values {
		require.False(t, vec.IsNull(uint64(i+2)))
		require.Equal(t, value, GetFixedAtNoTypeCheck[int64](vec, i+2))
	}
}

// TestAppendFixedListAliasedInput appends a slice of the vector's own unused
// capacity, which overlaps the destination: the rows must equal the input as
// it was before the call.
func TestAppendFixedListAliasedInput(t *testing.T) {
	vec, mp := newAppendTestVec(t, types.T_int64.ToType())
	require.NoError(t, vec.PreExtend(17, mp))
	require.NoError(t, AppendFixedList(vec, []int64{7}, nil, mp))
	require.GreaterOrEqual(t, vec.Capacity(), 17)
	backing := MustFixedColWithTypeCheck[int64](vec)[:17]
	for i := 1; i < len(backing); i++ {
		backing[i] = int64(i)
	}
	input := append([]int64(nil), backing[:16]...)
	require.NoError(t, AppendFixedList(vec, backing[:16], nil, mp))
	require.Equal(t, append([]int64{7}, input...), MustFixedColWithTypeCheck[int64](vec))
}

// TestAppendFixedListAliasedInputAfterGrowth appends a vector's own rows when
// the append must grow, so the source would be released by the growth.
func TestAppendFixedListAliasedInputAfterGrowth(t *testing.T) {
	vec, mp := newAppendTestVec(t, types.T_int64.ToType())
	values := make([]int64, 16)
	for i := range values {
		values[i] = int64(i + 4)
	}
	require.NoError(t, AppendFixedList(vec, values, nil, mp))
	if remaining := vec.Capacity() - vec.Length(); remaining > 0 {
		filler := make([]int64, remaining)
		for i := range filler {
			filler[i] = int64(i + len(values) + 4)
		}
		require.NoError(t, AppendFixedList(vec, filler, nil, mp))
	}
	vals := MustFixedColWithTypeCheck[int64](vec)
	require.Equal(t, vec.Capacity(), len(vals))
	before := append([]int64(nil), vals...)
	require.NoError(t, AppendFixedList(vec, vals, nil, mp))
	require.Equal(t, append(before, before...), MustFixedColWithTypeCheck[int64](vec))
}

// TestAppendVarlenListPregrow checks that a pre-grown bulk append encodes
// byte-for-byte like row-by-row appends, across inline and area payloads.
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
	strs := make([]string, len(values))
	expectedArea := 0
	for i, value := range values {
		strs[i] = string(value)
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
			return AppendStringList(v, strs, nil, mp)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vec, mp := newAppendTestVec(t, tc.oid.ToType())
			reference := NewVec(tc.oid.ToType())
			decoded := NewVec(tc.oid.ToType())
			t.Cleanup(func() {
				reference.Free(mp)
				decoded.Free(mp)
			})
			require.True(t, canPregrowRawByteList(vec, nil))
			require.NoError(t, tc.add(vec, mp))
			require.Equal(t, len(values), vec.Length())
			require.Equal(t, expectedArea, len(vec.area))
			for i, value := range values {
				require.NoError(t, AppendBytes(reference, value, false, mp))
				require.Equal(t, value, vec.GetBytesAt(i))
				require.False(t, vec.IsNull(uint64(i)))
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

// TestAppendVarlenListSlowPath covers the inputs that skip pre-growing: a null
// mask, an input aliasing the vector's own area, and a unicode collation.
func TestAppendVarlenListSlowPath(t *testing.T) {
	t.Run("null payloads are not stored", func(t *testing.T) {
		vec, mp := newAppendTestVec(t, types.T_varbinary.ToType())
		deadPayload := bytes.Repeat([]byte("x"), 256)
		values := make([][]byte, 32)
		mask := make([]bool, len(values))
		for i := range values {
			values[i], mask[i] = deadPayload, true
		}
		values[1], mask[1] = []byte("ok"), false
		require.False(t, canPregrowRawByteList(vec, mask))
		require.NoError(t, AppendBytesList(vec, values, mask, mp))
		require.Equal(t, len(values), vec.Length())
		require.Zero(t, len(vec.area))
		require.Less(t, cap(vec.area), len(deadPayload), "null payloads must not be reserved")
		for i := range values {
			require.Equal(t, mask[i], vec.IsNull(uint64(i)))
		}
		require.Equal(t, []byte("ok"), vec.GetBytesAt(1))
	})

	t.Run("input aliasing the area", func(t *testing.T) {
		vec, mp := newAppendTestVec(t, types.T_varbinary.ToType())
		long := bytes.Repeat([]byte("x"), 64)
		require.NoError(t, AppendBytesList(vec, [][]byte{long, long}, []bool{true, false}, mp))
		require.Equal(t, len(long), len(vec.area))
		require.NoError(t, AppendBytesList(vec, [][]byte{vec.GetBytesAt(1)}, nil, mp))
		require.Equal(t, long, vec.GetBytesAt(2))
	})

	t.Run("unicode validation rolls back", func(t *testing.T) {
		vec, mp := newAppendTestVec(t, types.NewWithCharset(types.T_varchar, 128, 0, types.CharsetUTF8MB4UnicodeCI))
		require.False(t, canPregrowRawByteList(vec, nil))
		invalid := string(append([]byte{0xff}, bytes.Repeat([]byte("x"), 127)...))
		values := make([]string, 32)
		for i := range values {
			values[i] = invalid
		}
		err := AppendStringList(vec, values, nil, mp)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "got %v", err)
		require.Zero(t, vec.Length())
	})
}

// TestAppendVarlenListPregrowFailureRollsBack fails the pre-grow on a full
// pool and checks the vector is left empty and still usable.
func TestAppendVarlenListPregrowFailureRollsBack(t *testing.T) {
	const rows, payload = 128, 64
	for _, tc := range []struct {
		name string
		oid  types.T
		add  func(*Vector, *mpool.MPool) error
	}{
		{"bytes", types.T_varbinary, func(v *Vector, mp *mpool.MPool) error {
			values := make([][]byte, rows)
			for i := range values {
				values[i] = bytes.Repeat([]byte("x"), payload)
			}
			return AppendBytesList(v, values, nil, mp)
		}},
		{"string", types.T_varchar, func(v *Vector, mp *mpool.MPool) error {
			values := make([]string, rows)
			for i := range values {
				values[i] = string(bytes.Repeat([]byte("x"), payload))
			}
			return AppendStringList(v, values, nil, mp)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const poolCap = 1 << 20
			mp, err := mpool.NewMPool("append-pregrow-rollback", poolCap, 0)
			require.NoError(t, err)
			vec := NewOffHeapVecWithType(tc.oid.ToType())
			// Leave room for the row headers, not for the rows*payload area.
			filler, err := mp.Alloc(poolCap-rows*payload/2, true)
			require.NoError(t, err)
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				vec.Free(mp)
				mp.Free(filler)
				require.Zero(t, mp.CurrNB())
			})
			err = tc.add(vec, mp)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrMPoolCapacity), "got %v", err)
			require.Zero(t, vec.Length())
			require.Zero(t, len(vec.area))
			require.NoError(t, AppendBytesList(vec, [][]byte{[]byte("ok")}, nil, mp))
			require.Equal(t, []byte("ok"), vec.GetBytesAt(0))
		})
	}
}
