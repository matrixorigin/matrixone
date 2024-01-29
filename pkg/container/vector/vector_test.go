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

func TestLength(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType())
	err := AppendFixedList(vec, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	require.Equal(t, 3, vec.Length())
	vec.SetLength(2)
	require.Equal(t, 2, vec.Length())
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	{
		//Array Float32
		mp := mpool.MustNewZero()
		vec := NewVec(types.New(types.T_array_float32, 3, 0))
		err := AppendArrayList[float32](vec, [][]float32{{1, 2, 3}, {4, 5, 6}}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, 2, vec.Length())
		vec.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{
		//Array Float64
		mp := mpool.MustNewZero()
		vec := NewVec(types.New(types.T_array_float64, 3, 0))
		err := AppendArrayList[float64](vec, [][]float64{{1, 2, 3}, {4, 5, 6}}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, 2, vec.Length())
		vec.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestSize(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType())
	require.Equal(t, 0, vec.Size())
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
	{
		//Array Float32
		mp := mpool.MustNewZero()
		vec := NewVec(types.New(types.T_array_float32, 4, 0))
		require.Equal(t, 0, vec.Size())
		vec.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{
		//Array Float64
		mp := mpool.MustNewZero()
		vec := NewVec(types.New(types.T_array_float64, 4, 0))
		require.Equal(t, 0, vec.Size())
		vec.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestGetUnionOneFunction(t *testing.T) {

	{ // test const vector
		mp := mpool.MustNewZero()
		v := NewVec(types.T_int8.ToType())
		w := NewVec(types.T_int8.ToType())
		err := AppendFixed(w, int8(0), false, mp)
		require.NoError(t, err)
		uf := GetUnionOneFunction(*w.GetType(), mp)
		err = uf(v, w, 0)
		require.NoError(t, err)
		w.Free(mp)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // test const vector
		mp := mpool.MustNewZero()
		v := NewVec(types.T_varchar.ToType())
		w := NewVec(types.T_varchar.ToType())
		err := AppendBytes(w, []byte("x"), false, mp)
		require.NoError(t, err)
		uf := GetUnionOneFunction(*w.GetType(), mp)
		err = uf(v, w, 0)
		require.NoError(t, err)
		w.Free(mp)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // test const Array Float32 vector
		mp := mpool.MustNewZero()
		v := NewVec(types.New(types.T_array_float32, 4, 0))
		w := NewVec(types.New(types.T_array_float32, 4, 0))
		err := AppendArrayList[float32](w, [][]float32{{1, 2, 3, 0}, {4, 5, 6, 0}}, nil, mp)
		require.NoError(t, err)
		uf := GetUnionOneFunction(*w.GetType(), mp)
		err = uf(v, w, 0)
		require.NoError(t, err)
		w.Free(mp)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

	}
	{ // test const Array Float64 vector
		mp := mpool.MustNewZero()
		v := NewVec(types.New(types.T_array_float64, 4, 0))
		w := NewVec(types.New(types.T_array_float64, 4, 0))
		err := AppendArrayList[float64](w, [][]float64{{1, 2, 3, 0}, {4, 5, 6, 0}}, nil, mp)
		require.NoError(t, err)
		uf := GetUnionOneFunction(*w.GetType(), mp)
		err = uf(v, w, 0)
		require.NoError(t, err)
		w.Free(mp)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

	}
}

func TestConst(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewConstNull(types.T_int8.ToType(), 1, mp)
	require.Equal(t, true, vec.IsConst())
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

/*
func TestWindowWithNulls(t *testing.T) {
	v0 := NewVec(types.T_int8.ToType())
	mp := mpool.MustNewZero()

	_ = AppendFixed(v0, int8(0), false, mp)
	_ = AppendFixed(v0, int8(1), false, mp)
	_ = AppendFixed(v0, int8(2), false, mp)
	_ = AppendFixed(v0, int8(-1), true, mp) // v0[3] = null
	_ = AppendFixed(v0, int8(6), false, mp)
	_ = AppendFixed(v0, int8(-1), true, mp) // v0[5] = null
	_ = AppendFixed(v0, int8(-1), true, mp) // v0[6] = null
	_ = AppendFixed(v0, int8(6), false, mp)
	_ = AppendFixed(v0, int8(7), false, mp)
	_ = AppendFixed(v0, int8(8), false, mp)

	require.Equal(t, []uint64{3, 5, 6}, v0.GetNulls().Np.ToArray())

	start, end := 1, 7
	v0Window := NewVec(types.T_int8.ToType())
	//v0Window = Window(v0, start, end, v0Window)
	require.Equal(t, MustFixedCol[int8](v0)[start:end], MustFixedCol[int8](v0Window))
	require.Equal(t, []uint64{2, 4, 5}, v0Window.GetNulls().Np.ToArray())

	//t.Log(v0.String())
	//t.Log(v0Window.String())
}
*/

func TestAppend(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType())
	err := AppendFixed(vec, int8(0), false, mp)
	require.NoError(t, err)
	err = AppendFixed(vec, int8(0), true, mp)
	require.NoError(t, err)
	err = AppendFixedList(vec, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	{
		// Array Float32
		mp := mpool.MustNewZero()
		vec := NewVec(types.New(types.T_array_float32, 4, 0))
		err := AppendArray[float32](vec, []float32{1, 2, 3, 0}, false, mp)
		require.NoError(t, err)
		require.Equal(t, 1, vec.Length())
		err = AppendArray[float32](vec, []float32{2, 4, 5, 6}, true, mp)
		require.NoError(t, err)
		require.Equal(t, 2, vec.Length())
		err = AppendArrayList[float32](vec, [][]float32{{4, 4, 4, 6}, {2, 5, 5, 3}}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, 4, vec.Length())
		vec.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{
		// Array Float64
		mp := mpool.MustNewZero()
		vec := NewVec(types.New(types.T_array_float64, 4, 0))
		err := AppendArray[float64](vec, []float64{1, 2, 3, 0}, false, mp)
		require.NoError(t, err)
		require.Equal(t, 1, vec.Length())
		err = AppendArray[float64](vec, []float64{2, 4, 5, 6}, true, mp)
		require.NoError(t, err)
		require.Equal(t, 2, vec.Length())
		err = AppendArrayList[float64](vec, [][]float64{{4, 4, 4, 6}, {2, 5, 5, 3}}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, 4, vec.Length())
		vec.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestAppendBytes(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_varchar.ToType())
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

func TestAppendArray(t *testing.T) {
	{
		// Array Float32
		mp := mpool.MustNewZero()
		vec := NewVec(types.T_array_float32.ToType())
		err := AppendArray[float32](vec, []float32{1, 1, 1}, false, mp)
		require.NoError(t, err)
		err = AppendArray[float32](vec, nil, true, mp)
		require.NoError(t, err)
		err = AppendArrayList[float32](vec, [][]float32{{2, 2, 2}, {3, 3, 3}}, nil, mp)
		require.NoError(t, err)
		vs, data := MustVarlenaRawData(vec)
		for _, v := range vs {
			types.GetArray[float32](&v, data)
		}
		vec.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{
		// Array Float64
		mp := mpool.MustNewZero()
		vec := NewVec(types.T_array_float64.ToType())
		err := AppendArray[float64](vec, []float64{1, 1, 1}, false, mp)
		require.NoError(t, err)
		err = AppendArray[float64](vec, nil, true, mp)
		require.NoError(t, err)
		err = AppendArrayList[float64](vec, [][]float64{{2, 2, 2}, {3, 3, 3}}, nil, mp)
		require.NoError(t, err)
		vs, data := MustVarlenaRawData(vec)
		for _, v := range vs {
			types.GetArray[float64](&v, data)
		}
		vec.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestDup(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.T_int8.ToType())
	err := AppendFixedList(v, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	w, err := v.Dup(mp)
	require.NoError(t, err)
	vs := MustFixedCol[int8](v)
	ws := MustFixedCol[int8](w)
	require.Equal(t, vs, ws)
	v.Free(mp)
	w.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestShrink(t *testing.T) {
	mp := mpool.MustNewZero()
	{ // Array Float32
		v := NewVec(types.T_array_float32.ToType())
		err := AppendArrayList[float32](v, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, [][]float32{{2, 2, 2}, {3, 3, 3}}, MustArrayCol[float32](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // Array Float64
		v := NewVec(types.T_array_float64.ToType())
		err := AppendArrayList[float64](v, [][]float64{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, [][]float64{{2, 2, 2}, {3, 3, 3}}, MustArrayCol[float64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bool
		v := NewVec(types.T_bool.ToType())
		err := AppendFixedList(v, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[bool](v)
		require.Equal(t, []bool{false, true}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		v := NewVec(types.T_int8.ToType())
		err := AppendFixedList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[int8](v)
		require.Equal(t, []int8{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := NewVec(types.T_int16.ToType())
		err := AppendFixedList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{0, 3}, false)
		vs := MustFixedCol[int16](v)
		require.Equal(t, []int16{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := NewVec(types.T_int32.ToType())
		err := AppendFixedList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[int32](v)
		require.Equal(t, []int32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := NewVec(types.T_int64.ToType())
		err := AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[int64](v)
		require.Equal(t, []int64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := NewVec(types.T_uint8.ToType())
		err := AppendFixedList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[uint8](v)
		require.Equal(t, []uint8{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := NewVec(types.T_uint16.ToType())
		err := AppendFixedList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{0, 3}, false)
		vs := MustFixedCol[uint16](v)
		require.Equal(t, []uint16{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := NewVec(types.T_uint32.ToType())
		err := AppendFixedList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[uint32](v)
		require.Equal(t, []uint32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := NewVec(types.T_uint64.ToType())
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[uint64](v)
		require.Equal(t, []uint64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := NewVec(types.T_float32.ToType())
		err := AppendFixedList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[float32](v)
		require.Equal(t, []float32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := NewVec(types.T_float64.ToType())
		err := AppendFixedList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[float64](v)
		require.Equal(t, []float64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := NewVec(types.T_text.ToType())
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustStrCol(v)
		require.Equal(t, []string{"2", "3"}, vs)
		require.Equal(t, [][]byte{[]byte("2"), []byte("3")}, MustBytesCol(v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := NewVec(types.T_date.ToType())
		err := AppendFixedList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[types.Date](v)
		require.Equal(t, []types.Date{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // datetime
		v := NewVec(types.T_datetime.ToType())
		err := AppendFixedList(v, []types.Datetime{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[types.Datetime](v)
		require.Equal(t, []types.Datetime{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		v := NewVec(types.T_time.ToType())
		err := AppendFixedList(v, []types.Time{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[types.Time](v)
		require.Equal(t, []types.Time{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		v := NewVec(types.T_timestamp.ToType())
		err := AppendFixedList(v, []types.Timestamp{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedCol[types.Timestamp](v)
		require.Equal(t, []types.Timestamp{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		vs := make([]types.Decimal64, 4)
		v := NewVec(types.T_decimal64.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, vs[1:3], MustFixedCol[types.Decimal64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		vs := make([]types.Decimal128, 4)
		v := NewVec(types.T_decimal128.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, vs[1:3], MustFixedCol[types.Decimal128](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		vs := make([]types.Uuid, 4)
		v := NewVec(types.T_uuid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, vs[1:3], MustFixedCol[types.Uuid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := NewVec(types.T_TS.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, vs[1:3], MustFixedCol[types.TS](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := NewVec(types.T_Rowid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, vs[1:3], MustFixedCol[types.Rowid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // blockid
		vs := make([]types.Blockid, 4)
		v := NewVec(types.T_Blockid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, vs[1:3], MustFixedCol[types.Blockid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestShuffle(t *testing.T) {
	mp := mpool.MustNewZero()

	{ // Array Float32
		v := NewVec(types.T_array_float32.ToType())
		err := AppendArrayList[float32](v, [][]float32{{1, 1}, {2, 2}, {3, 3}}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, [][]float32{{2, 2}, {3, 3}}, MustArrayCol[float32](v))
		require.Equal(t, "[2, 2] [3, 3]-[]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // Array Float64
		v := NewVec(types.T_array_float64.ToType())
		err := AppendArrayList[float64](v, [][]float64{{1, 1}, {2, 2}, {3, 3}}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, [][]float64{{2, 2}, {3, 3}}, MustArrayCol[float64](v))
		require.Equal(t, "[2, 2] [3, 3]-[]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bool
		v := NewVec(types.T_bool.ToType())
		err := AppendFixedList(v, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{2, 1}, mp)
		vs := MustFixedCol[bool](v)
		require.Equal(t, []bool{true, false}, vs)
		require.Equal(t, "[true false]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		v := NewVec(types.T_int8.ToType())
		err := AppendFixedList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedCol[int8](v)
		require.Equal(t, []int8{2, 3}, vs)
		require.Equal(t, "[2 3]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := NewVec(types.T_int16.ToType())
		err := AppendFixedList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{0, 3}, mp)
		vs := MustFixedCol[int16](v)
		require.Equal(t, []int16{1, 4}, vs)
		require.Equal(t, "[1 4]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := NewVec(types.T_int32.ToType())
		err := AppendFixedList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedCol[int32](v)
		require.Equal(t, []int32{2, 3}, vs)
		require.Equal(t, "[2 3]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := NewVec(types.T_int64.ToType())
		err := AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedCol[int64](v)
		require.Equal(t, []int64{2, 3}, vs)
		require.Equal(t, "[2 3]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := NewVec(types.T_uint8.ToType())
		err := AppendFixedList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedCol[uint8](v)
		require.Equal(t, []uint8{2, 3}, vs)
		require.Equal(t, "[2 3]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := NewVec(types.T_uint16.ToType())
		err := AppendFixedList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{0, 3}, mp)
		vs := MustFixedCol[uint16](v)
		require.Equal(t, []uint16{1, 4}, vs)
		require.Equal(t, "[1 4]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := NewVec(types.T_uint32.ToType())
		err := AppendFixedList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedCol[uint32](v)
		require.Equal(t, []uint32{2, 3}, vs)
		require.Equal(t, "[2 3]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := NewVec(types.T_uint64.ToType())
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedCol[uint64](v)
		require.Equal(t, []uint64{2, 3}, vs)
		require.Equal(t, "[2 3]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := NewVec(types.T_float32.ToType())
		err := AppendFixedList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedCol[float32](v)
		require.Equal(t, []float32{2, 3}, vs)
		require.Equal(t, "[2 3]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := NewVec(types.T_float64.ToType())
		err := AppendFixedList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedCol[float64](v)
		require.Equal(t, []float64{2, 3}, vs)
		require.Equal(t, "[2 3]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := NewVec(types.T_text.ToType())
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustStrCol(v)
		require.Equal(t, []string{"2", "3"}, vs)
		require.Equal(t, [][]byte{[]byte("2"), []byte("3")}, MustBytesCol(v))
		require.Equal(t, "[2 3]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := NewVec(types.T_date.ToType())
		err := AppendFixedList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedCol[types.Date](v)
		require.Equal(t, []types.Date{2, 3}, vs)
		require.Equal(t, "[0001-01-03 0001-01-04]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // datetime
		v := NewVec(types.T_datetime.ToType())
		err := AppendFixedList(v, []types.Datetime{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedCol[types.Datetime](v)
		require.Equal(t, []types.Datetime{2, 3}, vs)
		require.Equal(t, "[0001-01-01 00:00:00 0001-01-01 00:00:00]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		v := NewVec(types.T_time.ToType())
		err := AppendFixedList(v, []types.Time{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedCol[types.Time](v)
		require.Equal(t, []types.Time{2, 3}, vs)
		require.Equal(t, "[00:00:00 00:00:00]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		v := NewVec(types.T_timestamp.ToType())
		err := AppendFixedList(v, []types.Timestamp{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedCol[types.Timestamp](v)
		require.Equal(t, []types.Timestamp{2, 3}, vs)
		require.Equal(t, "[0001-01-01 00:00:00.000002 UTC 0001-01-01 00:00:00.000003 UTC]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		vs := make([]types.Decimal64, 4)
		v := NewVec(types.T_decimal64.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustFixedCol[types.Decimal64](v))
		require.Equal(t, "[0 0]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		vs := make([]types.Decimal128, 4)
		v := NewVec(types.T_decimal128.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustFixedCol[types.Decimal128](v))
		require.Equal(t, "[{0 0} {0 0}]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		vs := make([]types.Uuid, 4)
		v := NewVec(types.T_uuid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustFixedCol[types.Uuid](v))
		require.Equal(t, "[[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0] [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := NewVec(types.T_TS.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustFixedCol[types.TS](v))
		require.Equal(t, "[[0 0 0 0 0 0 0 0 0 0 0 0] [0 0 0 0 0 0 0 0 0 0 0 0]]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := NewVec(types.T_Rowid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustFixedCol[types.Rowid](v))
		require.Equal(t, "[[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0] [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // blockid
		vs := make([]types.Blockid, 4)
		v := NewVec(types.T_Blockid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustFixedCol[types.Blockid](v))
		require.Equal(t, "[[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0] [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestCopy(t *testing.T) {
	mp := mpool.MustNewZero()
	{ // fixed
		v := NewVec(types.T_int8.ToType())
		AppendFixedList(v, []int8{0, 0, 1, 0}, nil, mp)
		w := NewVec(types.T_int8.ToType())
		AppendFixedList(w, []int8{0, 0, 0, 0}, nil, mp)
		err := v.Copy(w, 2, 0, mp)
		require.NoError(t, err)
		require.Equal(t, MustFixedCol[int8](v), MustFixedCol[int8](w))
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // Array Float32
		v := NewVec(types.New(types.T_array_float32, 10, 0))
		AppendArrayList[float32](v, [][]float32{{0, 0}, {0, 0}, {1, 1}, {0, 0}}, nil, mp)
		w := NewVec(types.New(types.T_array_float32, 10, 0))
		AppendArrayList[float32](w, [][]float32{{0, 0}, {0, 0}, {0, 0}, {0, 0}}, nil, mp)
		err := v.Copy(w, 2, 0, mp)
		require.NoError(t, err)
		require.Equal(t, MustArrayCol[float32](v), MustArrayCol[float32](w))
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // Array Float64
		v := NewVec(types.New(types.T_array_float64, 10, 0))
		AppendArrayList[float64](v, [][]float64{{0, 0}, {0, 0}, {1, 1}, {0, 0}}, nil, mp)
		w := NewVec(types.New(types.T_array_float64, 10, 0))
		AppendArrayList[float64](w, [][]float64{{0, 0}, {0, 0}, {0, 0}, {0, 0}}, nil, mp)
		err := v.Copy(w, 2, 0, mp)
		require.NoError(t, err)
		require.Equal(t, MustArrayCol[float32](v), MustArrayCol[float32](w))
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // string
		v := NewVec(types.New(types.T_char, 10, 0))
		AppendBytesList(v, [][]byte{
			[]byte("hello"),
			[]byte("hello"),
			[]byte("nihao"),
			[]byte("hello"),
		}, nil, mp)
		w := NewVec(types.New(types.T_char, 10, 0))
		AppendBytesList(w, [][]byte{
			[]byte("hello"),
			[]byte("hello"),
			[]byte("hello"),
			[]byte("hello"),
		}, nil, mp)
		err := v.Copy(w, 2, 0, mp)
		require.NoError(t, err)
		require.Equal(t, MustStrCol(v), MustStrCol(w))
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestCloneWindow(t *testing.T) {
	mp := mpool.MustNewZero()
	v1 := NewConstNull(types.T_int32.ToType(), 10, mp)
	defer v1.Free(mp)
	v2, err := v1.CloneWindow(3, 5, mp)
	defer v2.Free(mp)
	require.NoError(t, err)
	require.True(t, v2.IsConstNull())
	require.Equal(t, 2, v2.Length())

	v3, _ := NewConstFixed[int32](types.T_int32.ToType(), 10, 20, mp)
	defer v3.Free(mp)
	v4, err := v3.CloneWindow(3, 5, mp)
	defer v4.Free(mp)
	require.NoError(t, err)
	require.True(t, v4.IsConst())
	require.Equal(t, 2, v4.Length())
	require.Equal(t, int32(10), GetFixedAt[int32](v4, 0))
	require.Equal(t, int32(10), GetFixedAt[int32](v4, 1))
}

func TestCloneWindowWithMpNil(t *testing.T) {
	mp := mpool.MustNewZero()
	vec1 := NewVec(types.T_int32.ToType())
	AppendFixed(vec1, int32(1), false, mp)
	AppendFixed(vec1, int32(2), true, mp)
	AppendFixed(vec1, int32(3), false, mp)
	require.False(t, vec1.NeedDup())

	vec2, err := vec1.CloneWindow(0, vec1.Length(), nil)
	require.NoError(t, err)
	vec1.Free(mp)

	t.Log(vec2.String())
	require.True(t, vec2.NeedDup())
	require.Equal(t, int32(1), GetFixedAt[int32](vec2, 0))
	require.True(t, vec2.GetNulls().Contains(uint64(1)))
	require.Equal(t, int32(3), GetFixedAt[int32](vec2, 2))

	vec3 := NewVec(types.T_char.ToType())
	AppendBytes(vec3, []byte("h"), false, mp)
	AppendBytes(vec3, []byte("xx"), true, mp)
	AppendBytes(vec3, []byte("uuu"), false, mp)
	require.False(t, vec3.NeedDup())

	vec4, err := vec3.CloneWindow(0, vec3.Length(), nil)
	require.NoError(t, err)
	vec3.Free(mp)

	require.True(t, vec4.NeedDup())
	require.Equal(t, 1, len(vec4.GetBytesAt(0)))
	require.Equal(t, 3, len(vec4.GetBytesAt(2)))
	require.True(t, vec4.GetNulls().Contains(uint64(1)))

	{ //Array Float32
		mp := mpool.MustNewZero()
		vec5 := NewVec(types.New(types.T_array_float32, 2, 0))
		AppendArray[float32](vec5, []float32{1, 1}, false, mp)
		AppendArray[float32](vec5, []float32{2, 2}, true, mp)
		AppendArray[float32](vec5, []float32{3, 3}, false, mp)
		require.False(t, vec5.NeedDup())

		vec6, err := vec5.CloneWindow(0, vec5.Length(), nil)
		require.NoError(t, err)
		vec5.Free(mp)

		t.Log(vec6.String())
		require.True(t, vec6.NeedDup())
		require.Equal(t, []float32{1, 1}, GetArrayAt[float32](vec6, 0))
		require.True(t, vec6.GetNulls().Contains(uint64(1)))
		require.Equal(t, []float32{3, 3}, GetArrayAt[float32](vec6, 2))
	}
	{ //Array Float64
		mp := mpool.MustNewZero()
		vec5 := NewVec(types.New(types.T_array_float64, 2, 0))
		AppendArray(vec5, []float64{1, 1}, false, mp)
		AppendArray(vec5, []float64{2, 2}, true, mp)
		AppendArray(vec5, []float64{3, 3}, false, mp)
		require.False(t, vec5.NeedDup())

		vec6, err := vec5.CloneWindow(0, vec5.Length(), nil)
		require.NoError(t, err)
		vec5.Free(mp)

		t.Log(vec6.String())
		require.True(t, vec6.NeedDup())
		require.Equal(t, []float64{1, 1}, GetArrayAt[float64](vec6, 0))
		require.True(t, vec6.GetNulls().Contains(uint64(1)))
		require.Equal(t, []float64{3, 3}, GetArrayAt[float64](vec6, 2))
	}
}

/*
func TestUnionOne(t *testing.T) {
	mp := mpool.MustNewZero()
	{ // bool
		v := NewVector(types.T_bool.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_bool.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[bool](v)[:1], MustFixedCol[bool](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		v := NewVector(types.T_int8.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_int8.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[int8](v)[:1], MustFixedCol[int8](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := NewVector(types.T_int16.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_int16.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[int16](v)[:1], MustFixedCol[int16](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := NewVector(types.T_int32.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_int32.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[int32](v)[:1], MustFixedCol[int32](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := NewVector(types.T_int64.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_int64.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[int64](v)[:1], MustFixedCol[int64](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := NewVector(types.T_uint8.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_uint8.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[uint8](v)[:1], MustFixedCol[uint8](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := NewVector(types.T_uint16.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_uint16.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[uint16](v)[:1], MustFixedCol[uint16](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := NewVector(types.T_uint32.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_uint32.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[uint32](v)[:1], MustFixedCol[uint32](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := NewVector(types.T_uint64.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_uint64.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[uint64](v)[:1], MustFixedCol[uint64](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := NewVector(types.T_float32.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_float32.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[float32](v)[:1], MustFixedCol[float32](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := NewVector(types.T_float64.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_float64.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[float64](v)[:1], MustFixedCol[float64](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := NewVector(types.T_text.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_text.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustStrCols(v)[:1], MustStrCols(w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := NewVector(types.T_date.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_date.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[types.Date](v)[:1], MustFixedCol[types.Date](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // datetime
		v := NewVector(types.T_datetime.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []types.Datetime{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_datetime.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[types.Datetime](v)[:1], MustFixedCol[types.Datetime](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		v := NewVector(types.T_time.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []types.Time{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_time.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[types.Time](v)[:1], MustFixedCol[types.Time](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		v := NewVector(types.T_timestamp.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, []types.Timestamp{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_timestamp.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[types.Timestamp](v)[:1], MustFixedCol[types.Timestamp](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		vs := make([]types.Decimal64, 4)
		v := NewVector(types.T_decimal64.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_decimal64.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[types.Decimal64](v)[:1], MustFixedCol[types.Decimal64](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		vs := make([]types.Decimal128, 4)
		v := NewVector(types.T_decimal128.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_decimal128.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[types.Decimal128](v)[:1], MustFixedCol[types.Decimal128](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		vs := make([]types.Uuid, 4)
		v := NewVector(types.T_uuid.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_uuid.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[types.Uuid](v)[:1], MustFixedCol[types.Uuid](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := NewVector(types.T_TS.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_TS.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[types.TS](v)[:1], MustFixedCol[types.TS](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := NewVector(types.T_Rowid.ToType())
		err := v.PreExtend(10, mp)
		require.NoError(t, err)
		err = AppendList(v, vs, nil, mp)
		require.NoError(t, err)
		w := NewVector(types.T_Rowid.ToType())
		w.UnionOne(v, 0, false, mp)
		require.Equal(t, MustFixedCol[types.Rowid](v)[:1], MustFixedCol[types.Rowid](w))
		w.UnionOne(v, 0, true, mp)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}
*/

func TestMarshalAndUnMarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.T_int8.ToType())
	err := AppendFixedList(v, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	w := new(Vector)
	err = w.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, MustFixedCol[int8](v), MustFixedCol[int8](w))
	w = new(Vector)
	err = w.UnmarshalBinaryWithCopy(data, mp)
	require.NoError(t, err)
	require.Equal(t, MustFixedCol[int8](v), MustFixedCol[int8](w))
	require.NoError(t, err)
	v.Free(mp)
	w.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestStrMarshalAndUnMarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.T_text.ToType())
	err := AppendBytesList(v, [][]byte{[]byte("x"), []byte("y")}, nil, mp)
	require.NoError(t, err)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	w := new(Vector)
	err = w.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, MustStrCol(v), MustStrCol(w))
	w = new(Vector)
	err = w.UnmarshalBinaryWithCopy(data, mp)
	require.NoError(t, err)
	require.Equal(t, MustStrCol(v), MustStrCol(w))
	v.Free(mp)
	w.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestArrayMarshalAndUnMarshal(t *testing.T) {

	{
		// Array Float32
		mp := mpool.MustNewZero()
		v := NewVec(types.New(types.T_array_float32, 2, 0))
		err := AppendArrayList(v, [][]float32{{0, 0}, {1, 1}, {2, 2}}, nil, mp)
		require.NoError(t, err)
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		w := new(Vector)
		err = w.UnmarshalBinary(data)
		require.NoError(t, err)
		require.Equal(t, MustArrayCol[float32](v), MustArrayCol[float32](w))
		w = new(Vector)
		err = w.UnmarshalBinaryWithCopy(data, mp)
		require.NoError(t, err)
		require.Equal(t, MustArrayCol[float32](v), MustArrayCol[float32](w))
		require.NoError(t, err)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}

	{
		// Array Float64
		mp := mpool.MustNewZero()
		v := NewVec(types.New(types.T_array_float64, 2, 0))
		err := AppendArrayList(v, [][]float64{{0, 0}, {1, 1}, {2, 2}}, nil, mp)
		require.NoError(t, err)
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		w := new(Vector)
		err = w.UnmarshalBinary(data)
		require.NoError(t, err)
		require.Equal(t, MustArrayCol[float64](v), MustArrayCol[float64](w))
		w = new(Vector)
		err = w.UnmarshalBinaryWithCopy(data, mp)
		require.NoError(t, err)
		require.Equal(t, MustArrayCol[float64](v), MustArrayCol[float64](w))
		require.NoError(t, err)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestWindowWith(t *testing.T) {
	mp := mpool.MustNewZero()
	vec1 := NewVec(types.T_int32.ToType())
	AppendFixed(vec1, int32(1), false, mp)
	AppendFixed(vec1, int32(2), true, mp)
	AppendFixed(vec1, int32(3), false, mp)
	require.False(t, vec1.NeedDup())

	vec2, err := vec1.Window(0, vec1.Length())
	require.NoError(t, err)

	t.Log(vec2.String())
	require.True(t, vec2.NeedDup())
	require.Equal(t, int32(1), GetFixedAt[int32](vec2, 0))
	require.True(t, vec2.GetNulls().Contains(uint64(1)))
	require.Equal(t, int32(3), GetFixedAt[int32](vec2, 2))
	vec2.Free(mp)

	vec6, err := vec1.Window(1, vec1.Length())
	require.NoError(t, err)

	t.Log(vec6.String())
	require.True(t, vec6.NeedDup())
	require.True(t, vec6.GetNulls().Contains(uint64(0)))
	require.Equal(t, int32(3), GetFixedAt[int32](vec6, 1))
	vec6.Free(mp)

	require.False(t, vec1.NeedDup())
	require.Equal(t, int32(1), GetFixedAt[int32](vec1, 0))
	require.True(t, vec1.GetNulls().Contains(uint64(1)))
	require.Equal(t, int32(3), GetFixedAt[int32](vec1, 2))
	vec1.Free(mp)

	vec3 := NewVec(types.T_char.ToType())
	AppendBytes(vec3, []byte("h"), false, mp)
	AppendBytes(vec3, []byte("xx"), true, mp)
	AppendBytes(vec3, []byte("uuu"), false, mp)
	require.False(t, vec3.NeedDup())

	vec4, err := vec3.Window(0, vec3.Length())
	require.NoError(t, err)

	require.True(t, vec4.NeedDup())
	require.Equal(t, "h", string(vec4.GetBytesAt(0)))
	require.Equal(t, "uuu", string(vec4.GetBytesAt(2)))
	require.True(t, vec4.GetNulls().Contains(uint64(1)))
	vec4.Free(mp)

	vec5, err := vec3.Window(1, vec3.Length())
	require.NoError(t, err)

	require.True(t, vec5.NeedDup())
	require.Equal(t, "uuu", string(vec5.GetBytesAt(1)))
	require.True(t, vec5.GetNulls().Contains(uint64(0)))
	vec5.Free(mp)

	require.False(t, vec3.NeedDup())
	require.Equal(t, "h", string(vec3.GetBytesAt(0)))
	require.Equal(t, "uuu", string(vec3.GetBytesAt(2)))
	require.True(t, vec3.GetNulls().Contains(uint64(1)))
	vec3.Free(mp)

	{
		//Array Float32

		vec7 := NewVec(types.T_array_float32.ToType())
		AppendArray(vec7, []float32{1, 1, 1}, false, mp)
		AppendArray(vec7, []float32{2, 2, 2}, true, mp)
		AppendArray(vec7, []float32{3, 3, 3}, false, mp)
		require.False(t, vec7.NeedDup())

		vec8, err := vec7.Window(0, vec7.Length())
		require.NoError(t, err)

		require.True(t, vec8.NeedDup())
		require.Equal(t, []float32{1, 1, 1}, GetArrayAt[float32](vec8, 0))
		require.Equal(t, []float32{3, 3, 3}, GetArrayAt[float32](vec8, 2))
		require.True(t, vec8.GetNulls().Contains(uint64(1)))
		vec8.Free(mp)

		vec9, err := vec7.Window(1, vec7.Length())
		require.NoError(t, err)

		require.True(t, vec9.NeedDup())
		require.Equal(t, []float32{3, 3, 3}, GetArrayAt[float32](vec9, 1))
		require.True(t, vec9.GetNulls().Contains(uint64(0)))
		vec9.Free(mp)

		require.False(t, vec7.NeedDup())
		require.Equal(t, []float32{1, 1, 1}, GetArrayAt[float32](vec7, 0))
		require.Equal(t, []float32{3, 3, 3}, GetArrayAt[float32](vec7, 2))
		require.True(t, vec7.GetNulls().Contains(uint64(1)))
		vec7.Free(mp)
	}

	{
		//Array Float64

		vec7 := NewVec(types.T_array_float64.ToType())
		AppendArray(vec7, []float64{1, 1, 1}, false, mp)
		AppendArray(vec7, []float64{2, 2, 2}, true, mp)
		AppendArray(vec7, []float64{3, 3, 3}, false, mp)
		require.False(t, vec7.NeedDup())

		vec8, err := vec7.Window(0, vec7.Length())
		require.NoError(t, err)

		require.True(t, vec8.NeedDup())
		require.Equal(t, []float64{1, 1, 1}, GetArrayAt[float64](vec8, 0))
		require.Equal(t, []float64{3, 3, 3}, GetArrayAt[float64](vec8, 2))
		require.True(t, vec8.GetNulls().Contains(uint64(1)))
		vec8.Free(mp)

		vec9, err := vec7.Window(1, vec7.Length())
		require.NoError(t, err)

		require.True(t, vec9.NeedDup())
		require.Equal(t, []float64{3, 3, 3}, GetArrayAt[float64](vec9, 1))
		require.True(t, vec9.GetNulls().Contains(uint64(0)))
		vec9.Free(mp)

		require.False(t, vec7.NeedDup())
		require.Equal(t, []float64{1, 1, 1}, GetArrayAt[float64](vec7, 0))
		require.Equal(t, []float64{3, 3, 3}, GetArrayAt[float64](vec7, 2))
		require.True(t, vec7.GetNulls().Contains(uint64(1)))
		vec7.Free(mp)
	}
}

func TestSetFunction(t *testing.T) {
	mp := mpool.MustNewZero()
	{ // bool
		v := NewVec(types.T_bool.ToType())
		w := NewConstNull(types.T_bool.ToType(), 0, mp)
		err := AppendFixedList(v, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_bool.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustFixedCol[bool](w)
		require.Equal(t, []bool{false}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		v := NewVec(types.T_int8.ToType())
		w := NewConstNull(types.T_int8.ToType(), 0, mp)
		err := AppendFixedList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_int8.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustFixedCol[int8](w)
		require.Equal(t, []int8{2}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := NewVec(types.T_int16.ToType())
		w := NewConstNull(types.T_int16.ToType(), 0, mp)
		err := AppendFixedList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_int16.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustFixedCol[int16](w)
		require.Equal(t, []int16{2}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := NewVec(types.T_int32.ToType())
		w := NewConstNull(types.T_int32.ToType(), 0, mp)
		err := AppendFixedList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_int32.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustFixedCol[int32](w)
		require.Equal(t, []int32{2}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := NewVec(types.T_int64.ToType())
		w := NewConstNull(types.T_int64.ToType(), 0, mp)
		err := AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_int64.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustFixedCol[int64](w)
		require.Equal(t, []int64{2}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := NewVec(types.T_uint8.ToType())
		w := NewConstNull(types.T_uint8.ToType(), 0, mp)
		err := AppendFixedList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_uint8.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustFixedCol[uint8](w)
		require.Equal(t, []uint8{2}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := NewVec(types.T_uint16.ToType())
		w := NewConstNull(types.T_uint16.ToType(), 0, mp)
		err := AppendFixedList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_uint16.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustFixedCol[uint16](w)
		require.Equal(t, []uint16{2}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := NewVec(types.T_uint32.ToType())
		w := NewConstNull(types.T_uint32.ToType(), 0, mp)
		err := AppendFixedList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_uint32.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustFixedCol[uint32](w)
		require.Equal(t, []uint32{2}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := NewVec(types.T_uint64.ToType())
		w := NewConstNull(types.T_uint64.ToType(), 0, mp)
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_uint64.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustFixedCol[uint64](w)
		require.Equal(t, []uint64{2}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := NewVec(types.T_float32.ToType())
		w := NewConstNull(types.T_float32.ToType(), 0, mp)
		err := AppendFixedList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_float32.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustFixedCol[float32](w)
		require.Equal(t, []float32{2}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := NewVec(types.T_float64.ToType())
		w := NewConstNull(types.T_float64.ToType(), 0, mp)
		err := AppendFixedList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_float64.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustFixedCol[float64](w)
		require.Equal(t, []float64{2}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := NewVec(types.T_text.ToType())
		w := NewConstNull(types.T_text.ToType(), 0, mp)
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_text.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustStrCol(w)
		require.Equal(t, []string{"2"}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // Array Float32
		v := NewVec(types.T_array_float32.ToType())
		w := NewConstNull(types.T_array_float32.ToType(), 0, mp)
		err := AppendArrayList(v, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_array_float32.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustArrayCol[float32](w)
		require.Equal(t, [][]float32{{2, 2, 2}}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // Array Float64
		v := NewVec(types.T_array_float64.ToType())
		w := NewConstNull(types.T_array_float64.ToType(), 0, mp)
		err := AppendArrayList(v, [][]float64{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_array_float64.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustArrayCol[float64](w)
		require.Equal(t, [][]float64{{2, 2, 2}}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestSetFunction2(t *testing.T) {
	// set vec to const value -> const null -> const value -> const null.
	// bool type
	{
		mp := mpool.MustNewZero()

		w := NewConstNull(types.T_bool.ToType(), 0, mp)
		v := NewVec(types.T_bool.ToType())
		err := AppendFixedList(v, []bool{true, false, true, false}, []bool{false, false, true, true}, mp)
		require.NoError(t, err)

		sf := GetConstSetFunction(types.T_bool.ToType(), mp)
		// set to const value true
		{
			err = sf(w, v, 0, 1)
			require.NoError(t, err)
			ws := MustFixedCol[bool](w)
			require.Equal(t, []bool{true}, ws)
		}
		// set to const null
		{
			err = sf(w, v, 2, 1)
			require.NoError(t, err)
			require.True(t, w.IsConstNull())
		}
		// set to const value false
		{
			err = sf(w, v, 1, 1)
			require.NoError(t, err)
			ws := MustFixedCol[bool](w)
			require.Equal(t, []bool{false}, ws)
		}
		// set to const null
		{
			err = sf(w, v, 3, 1)
			require.NoError(t, err)
			require.True(t, w.IsConstNull())
		}
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}

	// byte type
	{
		mp := mpool.MustNewZero()

		w := NewConstNull(types.T_varchar.ToType(), 0, mp)
		v := NewVec(types.T_varchar.ToType())
		err := AppendBytesList(v, [][]byte{
			[]byte("a"), []byte("abcdefabcdefabcdefabcdef12345"), []byte("c"), []byte("d")},
			[]bool{false, false, true, true}, mp)
		require.NoError(t, err)

		sf := GetConstSetFunction(types.T_varchar.ToType(), mp)
		// set to const value a
		{
			err = sf(w, v, 0, 1)
			require.NoError(t, err)
			ws := MustBytesCol(w)
			require.Equal(t, "a", string(ws[0]))
		}
		// set to const null
		{
			err = sf(w, v, 2, 1)
			require.NoError(t, err)
			require.True(t, w.IsConstNull())
		}
		// set to const value b
		{
			err = sf(w, v, 1, 1)
			require.NoError(t, err)
			ws := MustBytesCol(w)
			require.Equal(t, "abcdefabcdefabcdefabcdef12345", string(ws[0]))
		}
		// set to const null
		{
			err = sf(w, v, 3, 1)
			require.NoError(t, err)
			require.True(t, w.IsConstNull())
		}
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}

	// Array Float32 type
	{
		mp := mpool.MustNewZero()

		w := NewConstNull(types.T_array_float32.ToType(), 0, mp)
		v := NewVec(types.T_array_float32.ToType())
		err := AppendArrayList[float32](v, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}}, []bool{false, false, true, true}, mp)
		require.NoError(t, err)

		sf := GetConstSetFunction(types.T_array_float32.ToType(), mp)
		// set to const value a
		{
			err = sf(w, v, 0, 1)
			require.NoError(t, err)
			ws := MustArrayCol[float32](w)
			require.Equal(t, []float32{1, 1, 1}, ws[0])
		}
		// set to const null
		{
			err = sf(w, v, 2, 1)
			require.NoError(t, err)
			require.True(t, w.IsConstNull())
		}
		// set to const value b
		{
			err = sf(w, v, 1, 1)
			require.NoError(t, err)
			ws := MustArrayCol[float32](w)
			require.Equal(t, []float32{2, 2, 2}, ws[0])
		}
		// set to const null
		{
			err = sf(w, v, 3, 1)
			require.NoError(t, err)
			require.True(t, w.IsConstNull())
		}
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType())
	AppendAny(vec, int8(42), false, mp)
	data, err := vec.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := vec.UnmarshalBinary(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkToTypedSlice(b *testing.B) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType())
	AppendAny(vec, int8(42), false, mp)
	var slice []int8
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ToSlice(vec, &slice)
	}
	if slice[0] != 42 {
		b.Fatalf("got %v", slice)
	}
}

func BenchmarkToFixedCol(b *testing.B) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType())
	AppendAny(vec, int8(42), false, mp)
	b.ResetTimer()
	var slice []int8
	for i := 0; i < b.N; i++ {
		ToFixedCol[int8](vec, &slice)
	}
}

func BenchmarkMustFixedCol(b *testing.B) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType())
	AppendAny(vec, int8(42), false, mp)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MustFixedCol[int8](vec)
	}
}
