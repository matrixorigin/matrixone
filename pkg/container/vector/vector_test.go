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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestIsBin(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := New(FLAT, types.New(types.T_int8, 0, 0, 0))
	vec.SetIsBin(true)
	require.Equal(t, true, vec.GetIsBin())
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestLength(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := New(FLAT, types.New(types.T_int8, 0, 0, 0))
	err := AppendList(vec, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	require.Equal(t, 3, vec.Length())
	vec.SetLength(2)
	require.Equal(t, 2, vec.Length())
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestSize(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := New(FLAT, types.New(types.T_int8, 0, 0, 0))
	require.Equal(t, 0, vec.Size())
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestConst(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := New(CONSTANT, types.New(types.T_int8, 0, 0, 0))
	require.Equal(t, true, vec.IsConst())
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestAppend(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := New(FLAT, types.New(types.T_int8, 0, 0, 0))
	err := Append(vec, int(0), false, mp)
	require.NoError(t, err)
	err = Append(vec, int(0), true, mp)
	require.NoError(t, err)
	err = AppendList(vec, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestAppendBytes(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := New(FLAT, types.New(types.T_varchar, 0, 0, 0))
	err := AppendBytes(vec, []byte("x"), false, mp)
	require.NoError(t, err)
	err = AppendBytes(vec, nil, true, mp)
	require.NoError(t, err)
	err = AppendBytesList(vec, [][]byte{[]byte("x"), []byte("y")}, nil, mp)
	require.NoError(t, err)
	vs, data := MustVarlenaRawData(vec)
	for _, v := range vs {
		v.GetByteSlice(data)
	}
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestDup(t *testing.T) {
	mp := mpool.MustNewZero()
	v := New(FLAT, types.New(types.T_int8, 0, 0, 0))
	err := AppendList(v, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	w, err := v.Dup(mp)
	require.NoError(t, err)
	vs := MustTCols[int8](v)
	ws := MustTCols[int8](w)
	require.Equal(t, vs, ws)
	v.Free(mp)
	w.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestShrink(t *testing.T) {
	mp := mpool.MustNewZero()
	{ // bool
		v := New(FLAT, types.New(types.T_bool, 0, 0, 0))
		err := AppendList(v, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[bool](v)
		require.Equal(t, []bool{false, true}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		v := New(FLAT, types.New(types.T_int8, 0, 0, 0))
		err := AppendList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[int8](v)
		require.Equal(t, []int8{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := New(FLAT, types.New(types.T_int16, 0, 0, 0))
		err := AppendList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{0, 3})
		vs := MustTCols[int16](v)
		require.Equal(t, []int16{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := New(FLAT, types.New(types.T_int32, 0, 0, 0))
		err := AppendList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[int32](v)
		require.Equal(t, []int32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := New(FLAT, types.New(types.T_int64, 0, 0, 0))
		err := AppendList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[int64](v)
		require.Equal(t, []int64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := New(FLAT, types.New(types.T_uint8, 0, 0, 0))
		err := AppendList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[uint8](v)
		require.Equal(t, []uint8{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := New(FLAT, types.New(types.T_uint16, 0, 0, 0))
		err := AppendList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{0, 3})
		vs := MustTCols[uint16](v)
		require.Equal(t, []uint16{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := New(FLAT, types.New(types.T_uint32, 0, 0, 0))
		err := AppendList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[uint32](v)
		require.Equal(t, []uint32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := New(FLAT, types.New(types.T_uint64, 0, 0, 0))
		err := AppendList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[uint64](v)
		require.Equal(t, []uint64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := New(FLAT, types.New(types.T_float32, 0, 0, 0))
		err := AppendList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[float32](v)
		require.Equal(t, []float32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := New(FLAT, types.New(types.T_float64, 0, 0, 0))
		err := AppendList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[float64](v)
		require.Equal(t, []float64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := New(FLAT, types.New(types.T_text, 0, 0, 0))
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustStrCols(v)
		require.Equal(t, []string{"2", "3"}, vs)
		require.Equal(t, [][]byte{[]byte("2"), []byte("3")}, MustBytesCols(v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := New(FLAT, types.New(types.T_date, 0, 0, 0))
		err := AppendList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[types.Date](v)
		require.Equal(t, []types.Date{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // datetime
		v := New(FLAT, types.New(types.T_datetime, 0, 0, 0))
		err := AppendList(v, []types.Datetime{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[types.Datetime](v)
		require.Equal(t, []types.Datetime{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		v := New(FLAT, types.New(types.T_time, 0, 0, 0))
		err := AppendList(v, []types.Time{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[types.Time](v)
		require.Equal(t, []types.Time{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		v := New(FLAT, types.New(types.T_timestamp, 0, 0, 0))
		err := AppendList(v, []types.Timestamp{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		vs := MustTCols[types.Timestamp](v)
		require.Equal(t, []types.Timestamp{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		vs := make([]types.Decimal64, 4)
		v := New(FLAT, types.New(types.T_decimal64, 0, 0, 0))
		err := AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		require.Equal(t, vs[1:3], MustTCols[types.Decimal64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		vs := make([]types.Decimal128, 4)
		v := New(FLAT, types.New(types.T_decimal128, 0, 0, 0))
		err := AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		require.Equal(t, vs[1:3], MustTCols[types.Decimal128](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		vs := make([]types.Uuid, 4)
		v := New(FLAT, types.New(types.T_uuid, 0, 0, 0))
		err := AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		require.Equal(t, vs[1:3], MustTCols[types.Uuid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := New(FLAT, types.New(types.T_TS, 0, 0, 0))
		err := AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		require.Equal(t, vs[1:3], MustTCols[types.TS](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := New(FLAT, types.New(types.T_Rowid, 0, 0, 0))
		err := AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2})
		require.Equal(t, vs[1:3], MustTCols[types.Rowid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestShuffle(t *testing.T) {
	mp := mpool.MustNewZero()
	{ // bool
		v := New(FLAT, types.New(types.T_bool, 0, 0, 0))
		err := AppendList(v, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{2, 1}, mp)
		vs := MustTCols[bool](v)
		require.Equal(t, []bool{true, false}, vs)
		require.Equal(t, "[true false]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		v := New(FLAT, types.New(types.T_int8, 0, 0, 0))
		err := AppendList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustTCols[int8](v)
		require.Equal(t, []int8{2, 3}, vs)
		require.Equal(t, "[2 3]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := New(FLAT, types.New(types.T_int16, 0, 0, 0))
		err := AppendList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{0, 3}, mp)
		vs := MustTCols[int16](v)
		require.Equal(t, []int16{1, 4}, vs)
		require.Equal(t, "[1 4]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := New(FLAT, types.New(types.T_int32, 0, 0, 0))
		err := AppendList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustTCols[int32](v)
		require.Equal(t, []int32{2, 3}, vs)
		require.Equal(t, "[2 3]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := New(FLAT, types.New(types.T_int64, 0, 0, 0))
		err := AppendList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustTCols[int64](v)
		require.Equal(t, []int64{2, 3}, vs)
		require.Equal(t, "[2 3]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := New(FLAT, types.New(types.T_uint8, 0, 0, 0))
		err := AppendList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustTCols[uint8](v)
		require.Equal(t, []uint8{2, 3}, vs)
		require.Equal(t, "[2 3]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := New(FLAT, types.New(types.T_uint16, 0, 0, 0))
		err := AppendList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{0, 3}, mp)
		vs := MustTCols[uint16](v)
		require.Equal(t, []uint16{1, 4}, vs)
		require.Equal(t, "[1 4]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := New(FLAT, types.New(types.T_uint32, 0, 0, 0))
		err := AppendList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustTCols[uint32](v)
		require.Equal(t, []uint32{2, 3}, vs)
		require.Equal(t, "[2 3]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := New(FLAT, types.New(types.T_uint64, 0, 0, 0))
		err := AppendList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustTCols[uint64](v)
		require.Equal(t, []uint64{2, 3}, vs)
		require.Equal(t, "[2 3]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := New(FLAT, types.New(types.T_float32, 0, 0, 0))
		err := AppendList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustTCols[float32](v)
		require.Equal(t, []float32{2, 3}, vs)
		require.Equal(t, "[2 3]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := New(FLAT, types.New(types.T_float64, 0, 0, 0))
		err := AppendList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustTCols[float64](v)
		require.Equal(t, []float64{2, 3}, vs)
		require.Equal(t, "[2 3]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := New(FLAT, types.New(types.T_text, 0, 0, 0))
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustStrCols(v)
		require.Equal(t, []string{"2", "3"}, vs)
		require.Equal(t, [][]byte{[]byte("2"), []byte("3")}, MustBytesCols(v))
		require.Equal(t, "[2 3]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := New(FLAT, types.New(types.T_date, 0, 0, 0))
		err := AppendList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustTCols[types.Date](v)
		require.Equal(t, []types.Date{2, 3}, vs)
		require.Equal(t, "[0001-01-03 0001-01-04]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // datetime
		v := New(FLAT, types.New(types.T_datetime, 0, 0, 0))
		err := AppendList(v, []types.Datetime{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustTCols[types.Datetime](v)
		require.Equal(t, []types.Datetime{2, 3}, vs)
		require.Equal(t, "[0001-01-01 00:00:00 0001-01-01 00:00:00]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		v := New(FLAT, types.New(types.T_time, 0, 0, 0))
		err := AppendList(v, []types.Time{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustTCols[types.Time](v)
		require.Equal(t, []types.Time{2, 3}, vs)
		require.Equal(t, "[00:00:00 00:00:00]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		v := New(FLAT, types.New(types.T_timestamp, 0, 0, 0))
		err := AppendList(v, []types.Timestamp{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustTCols[types.Timestamp](v)
		require.Equal(t, []types.Timestamp{2, 3}, vs)
		require.Equal(t, "[0001-01-01 00:00:00.000002 UTC 0001-01-01 00:00:00.000003 UTC]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		vs := make([]types.Decimal64, 4)
		v := New(FLAT, types.New(types.T_decimal64, 0, 0, 0))
		err := AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustTCols[types.Decimal64](v))
		require.Equal(t, "[0E-398 0E-398]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		vs := make([]types.Decimal128, 4)
		v := New(FLAT, types.New(types.T_decimal128, 0, 0, 0))
		err := AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustTCols[types.Decimal128](v))
		require.Equal(t, "[0E-6176 0E-6176]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		vs := make([]types.Uuid, 4)
		v := New(FLAT, types.New(types.T_uuid, 0, 0, 0))
		err := AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustTCols[types.Uuid](v))
		require.Equal(t, "[[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0] [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := New(FLAT, types.New(types.T_TS, 0, 0, 0))
		err := AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustTCols[types.TS](v))
		require.Equal(t, "[[0 0 0 0 0 0 0 0 0 0 0 0] [0 0 0 0 0 0 0 0 0 0 0 0]]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := New(FLAT, types.New(types.T_Rowid, 0, 0, 0))
		err := AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustTCols[types.Rowid](v))
		require.Equal(t, "[[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0] [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]]-&{<nil>}", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestCopy(t *testing.T) {
	mp := mpool.MustNewZero()
	{ // fixed
		v := New(FLAT, types.New(types.T_int8, 0, 0, 0))
		AppendList(v, []int8{0, 0, 1, 0}, nil, mp)
		w := New(FLAT, types.New(types.T_int8, 0, 0, 0))
		AppendList(w, []int8{0, 0, 0, 0}, nil, mp)
		err := v.Copy(w, 2, 0, mp)
		require.NoError(t, err)
		require.Equal(t, MustTCols[int8](v), MustTCols[int8](w))
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // string
		v := New(FLAT, types.New(types.T_char, 10, 0, 0))
		AppendBytesList(v, [][]byte{
			[]byte("hello"),
			[]byte("hello"),
			[]byte("nihao"),
			[]byte("hello"),
		}, nil, mp)
		w := New(FLAT, types.New(types.T_char, 10, 0, 0))
		AppendBytesList(w, [][]byte{
			[]byte("hello"),
			[]byte("hello"),
			[]byte("hello"),
			[]byte("hello"),
		}, nil, mp)
		err := v.Copy(w, 2, 0, mp)
		require.NoError(t, err)
		require.Equal(t, MustStrCols(v), MustStrCols(w))
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestUnionOne(t *testing.T) {
	mp := mpool.MustNewZero()
	{ // bool
		v := New(FLAT, types.New(types.T_bool, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_bool, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[bool](v)[:1], MustTCols[bool](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		v := New(FLAT, types.New(types.T_int8, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_int8, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[int8](v)[:1], MustTCols[int8](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := New(FLAT, types.New(types.T_int16, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_int16, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[int16](v)[:1], MustTCols[int16](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := New(FLAT, types.New(types.T_int32, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_int32, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[int32](v)[:1], MustTCols[int32](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := New(FLAT, types.New(types.T_int64, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_int64, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[int64](v)[:1], MustTCols[int64](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := New(FLAT, types.New(types.T_uint8, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_uint8, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[uint8](v)[:1], MustTCols[uint8](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := New(FLAT, types.New(types.T_uint16, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_uint16, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[uint16](v)[:1], MustTCols[uint16](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := New(FLAT, types.New(types.T_uint32, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_uint32, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[uint32](v)[:1], MustTCols[uint32](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := New(FLAT, types.New(types.T_uint64, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_uint64, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[uint64](v)[:1], MustTCols[uint64](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := New(FLAT, types.New(types.T_float32, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_float32, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[float32](v)[:1], MustTCols[float32](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := New(FLAT, types.New(types.T_float64, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_float64, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[float64](v)[:1], MustTCols[float64](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := New(FLAT, types.New(types.T_text, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_text, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustStrCols(v)[:1], MustStrCols(w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := New(FLAT, types.New(types.T_date, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_date, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[types.Date](v)[:1], MustTCols[types.Date](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // datetime
		v := New(FLAT, types.New(types.T_datetime, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []types.Datetime{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_datetime, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[types.Datetime](v)[:1], MustTCols[types.Datetime](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		v := New(FLAT, types.New(types.T_time, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []types.Time{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_time, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[types.Time](v)[:1], MustTCols[types.Time](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		v := New(FLAT, types.New(types.T_timestamp, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []types.Timestamp{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_timestamp, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[types.Timestamp](v)[:1], MustTCols[types.Timestamp](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		vs := make([]types.Decimal64, 4)
		v := New(FLAT, types.New(types.T_decimal64, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_decimal64, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[types.Decimal64](v)[:1], MustTCols[types.Decimal64](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		vs := make([]types.Decimal128, 4)
		v := New(FLAT, types.New(types.T_decimal128, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_decimal128, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[types.Decimal128](v)[:1], MustTCols[types.Decimal128](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		vs := make([]types.Uuid, 4)
		v := New(FLAT, types.New(types.T_uuid, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_uuid, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[types.Uuid](v)[:1], MustTCols[types.Uuid](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := New(FLAT, types.New(types.T_TS, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_TS, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[types.TS](v)[:1], MustTCols[types.TS](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := New(FLAT, types.New(types.T_Rowid, 0, 0, 0))
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		w := New(FLAT, types.New(types.T_Rowid, 0, 0, 0))
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustTCols[types.Rowid](v)[:1], MustTCols[types.Rowid](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestMarshalAndUnMarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	v := New(FLAT, types.New(types.T_int8, 0, 0, 0))
	err := AppendList(v, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	w := new(Vector)
	err = w.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, MustTCols[int8](v), MustTCols[int8](w))
	w = new(Vector)
	err = w.UnmarshalBinaryWithMpool(data, mp)
	require.NoError(t, err)
	require.Equal(t, MustTCols[int8](v), MustTCols[int8](w))
	require.NoError(t, err)
	v.Free(mp)
	w.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestStrMarshalAndUnMarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	v := New(FLAT, types.New(types.T_text, 0, 0, 0))
	err := AppendBytesList(v, [][]byte{[]byte("x"), []byte("y")}, nil, mp)
	require.NoError(t, err)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	w := new(Vector)
	err = w.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, MustStrCols(v), MustStrCols(w))
	w = new(Vector)
	err = w.UnmarshalBinaryWithMpool(data, mp)
	require.NoError(t, err)
	require.Equal(t, MustStrCols(v), MustStrCols(w))
	v.Free(mp)
	w.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}
