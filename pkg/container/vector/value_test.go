// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestMustAppendValueAndGetValue(t *testing.T) {
	mp := mpool.MustNewZero()

	ints := NewVec(types.T_int32.ToType(), mp)
	defer ints.Free(mp)
	ints.MustAppendValue(int32(7), false)
	ints.MustAppendValue(nil, true)
	require.Equal(t, 2, ints.Length())
	require.Equal(t, int32(7), ints.GetValue(0))
	require.True(t, ints.IsNull(1))

	strs := NewVec(types.T_varchar.ToType(), mp)
	defer strs.Free(mp)
	strs.MustAppendValue([]byte("a value longer than an inline varlena"), false)
	strs.MustAppendValue(nil, true)
	got := strs.GetValue(0).([]byte)
	require.Equal(t, []byte("a value longer than an inline varlena"), got)
	got[0] = 'X' // GetValue copies a varlena value
	require.Equal(t, byte('a'), strs.GetBytesAt(0)[0])
	require.Equal(t, []byte(nil), strs.GetValue(1))

	constNull := NewConstNull(types.T_int64.ToType(), 3, mp)
	defer constNull.Free(mp)
	require.Nil(t, constNull.GetValue(2))

	// a read-only vector has no pool to grow from
	view, err := ints.Window(0, 1)
	require.NoError(t, err)
	require.Panics(t, func() { view.MustAppendValue(int32(1), false) })
	// so does a value of the wrong type
	require.Panics(t, func() { ints.MustAppendValue("seven", false) })
}

func TestNullCount(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int32.ToType(), mp)
	defer vec.Free(mp)
	require.Zero(t, vec.NullCount())
	vec.MustAppendValue(int32(1), false)
	vec.MustAppendValue(nil, true)
	vec.MustAppendValue(nil, true)
	require.Equal(t, 2, vec.NullCount())

	constNull := NewConstNull(types.T_int32.ToType(), 5, mp)
	defer constNull.Free(mp)
	require.Equal(t, 5, constNull.NullCount())

	constVal, err := NewConstFixed(types.T_int32.ToType(), int32(3), 5, mp)
	require.NoError(t, err)
	defer constVal.Free(mp)
	require.Zero(t, constVal.NullCount())
}

func TestVectorEqual(t *testing.T) {
	mp := mpool.MustNewZero()
	build := func(typ types.Type, vals []any) *Vector {
		vec := NewVec(typ, mp)
		t.Cleanup(func() { vec.Free(mp) })
		for _, v := range vals {
			vec.MustAppendValue(v, v == nil)
		}
		return vec
	}
	i32 := types.T_int32.ToType()
	a := build(i32, []any{int32(1), nil, int32(3)})
	require.True(t, a.Equal(a))
	require.True(t, a.Equal(build(i32, []any{int32(1), nil, int32(3)})))
	require.False(t, a.Equal(build(i32, []any{int32(1), int32(2), int32(3)})), "NULL in other rows")
	require.False(t, a.Equal(build(i32, []any{int32(1), nil, int32(4)})), "different value")
	require.False(t, a.Equal(build(i32, []any{int32(1), nil})), "different length")
	require.False(t, a.Equal(build(types.T_int64.ToType(), []any{int64(1), nil, int64(3)})), "different type")

	vc := types.T_varchar.ToType()
	s := build(vc, []any{[]byte("x"), nil})
	require.True(t, s.Equal(build(vc, []any{[]byte("x"), nil})))
	require.False(t, s.Equal(build(vc, []any{[]byte("y"), nil})))

	// const and flat vectors of the same rows are equal
	constVal, err := NewConstFixed(i32, int32(9), 2, mp)
	require.NoError(t, err)
	defer constVal.Free(mp)
	require.True(t, constVal.Equal(build(i32, []any{int32(9), int32(9)})))
}
