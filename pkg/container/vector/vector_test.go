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
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestLength(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType())
	err := AppendFixedList(vec, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	require.Equal(t, 3, vec.Length())
	vec.length = 2
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

func TestUnionOne(t *testing.T) {
	{ // test const vector
		mp := mpool.MustNewZero()
		v := NewVec(types.T_int8.ToType())
		w := NewVec(types.T_int8.ToType())
		err := AppendFixed(w, int8(0), false, mp)
		require.NoError(t, err)
		err = v.UnionOne(w, 0, mp)
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
		err = v.UnionOne(w, 0, mp)
		require.NoError(t, err)
		w.Free(mp)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // test const Array Float32 vector
		mp := mpool.MustNewZero()
		v := NewVec(types.New(types.T_array_float32, 4, 0))
		w := NewVec(types.New(types.T_array_float32, 4, 0))
		err := AppendArrayList(w, [][]float32{{1, 2, 3, 0}, {4, 5, 6, 0}}, nil, mp)
		require.NoError(t, err)
		err = v.UnionOne(w, 0, mp)
		require.NoError(t, err)
		w.Free(mp)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

	}
	{ // test const Array Float64 vector
		mp := mpool.MustNewZero()
		v := NewVec(types.New(types.T_array_float64, 4, 0))
		w := NewVec(types.New(types.T_array_float64, 4, 0))
		err := AppendArrayList(w, [][]float64{{1, 2, 3, 0}, {4, 5, 6, 0}}, nil, mp)
		require.NoError(t, err)
		err = v.UnionOne(w, 0, mp)
		require.NoError(t, err)
		w.Free(mp)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // test bit vector
		mp := mpool.MustNewZero()
		v := NewVec(types.New(types.T_bit, 10, 0))
		w := NewVec(types.New(types.T_bit, 10, 0))
		err := AppendFixedList(w, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		err = v.UnionOne(w, 0, mp)
		require.NoError(t, err)
		require.Equal(t, 1, v.Length())
		vs := MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, uint64(1), vs[0])

		w.Free(mp)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bool
		mp := mpool.MustNewZero()
		v := NewVec(types.T_bool.ToType())
		w := NewVec(types.T_bool.ToType())
		err := AppendFixedList(w, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		err = v.UnionOne(w, 0, mp)
		require.NoError(t, err)
		require.Equal(t, 1, v.Length())
		vs := MustFixedColNoTypeCheck[bool](v)
		require.Equal(t, true, vs[0])

		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		mp := mpool.MustNewZero()
		v := NewVec(types.T_int8.ToType())
		w := NewVec(types.T_int8.ToType())
		err := AppendFixedList(w, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		err = v.UnionOne(w, 0, mp)
		require.NoError(t, err)
		require.Equal(t, 1, v.Length())
		vs := MustFixedColNoTypeCheck[int8](v)
		require.Equal(t, int8(1), vs[0])

		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		mp := mpool.MustNewZero()
		v := NewVec(types.T_int16.ToType())
		w := NewVec(types.T_int16.ToType())
		err := AppendFixedList(w, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		err = v.UnionOne(w, 0, mp)
		require.NoError(t, err)
		require.Equal(t, 1, v.Length())
		vs := MustFixedColNoTypeCheck[int16](v)
		require.Equal(t, int16(1), vs[0])

		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		mp := mpool.MustNewZero()
		v := NewVec(types.T_int32.ToType())
		w := NewVec(types.T_int32.ToType())
		err := AppendFixedList(w, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		err = v.UnionOne(w, 0, mp)
		require.NoError(t, err)
		require.Equal(t, 1, v.Length())
		vs := MustFixedColNoTypeCheck[int32](v)
		require.Equal(t, int32(1), vs[0])

		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		mp := mpool.MustNewZero()
		v := NewVec(types.T_int64.ToType())
		w := NewVec(types.T_int64.ToType())
		err := AppendFixedList(w, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		err = v.UnionOne(w, 0, mp)
		require.NoError(t, err)
		require.Equal(t, 1, v.Length())
		vs := MustFixedColNoTypeCheck[int64](v)
		require.Equal(t, int64(1), vs[0])

		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		mp := mpool.MustNewZero()
		v := NewVec(types.T_uint8.ToType())
		w := NewVec(types.T_uint8.ToType())
		err := AppendFixedList(w, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		err = v.UnionOne(w, 0, mp)
		require.NoError(t, err)
		require.Equal(t, 1, v.Length())
		vs := MustFixedColNoTypeCheck[uint8](v)
		require.Equal(t, uint8(1), vs[0])

		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		mp := mpool.MustNewZero()
		v := NewVec(types.T_text.ToType())
		w := NewVec(types.T_text.ToType())
		err := AppendBytesList(w, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)

		err = v.UnionOne(w, 0, mp)
		require.NoError(t, err)
		require.Equal(t, 1, v.Length())
		vs, area := MustVarlenaRawData(v)
		require.Equal(t, "1", vs[0].GetString(area))

		v.Free(mp)
		w.Free(mp)
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
	require.Equal(t, MustFixedColWithTypeCheck[int8](v0)[start:end], MustFixedColWithTypeCheck[int8](v0Window))
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
	vs := MustFixedColWithTypeCheck[int8](v)
	ws := MustFixedColWithTypeCheck[int8](w)
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
		vs := MustFixedColWithTypeCheck[bool](v)
		require.Equal(t, []bool{false, true}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		v := NewVec(types.T_int8.ToType())
		err := AppendFixedList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[int8](v)
		require.Equal(t, []int8{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := NewVec(types.T_int16.ToType())
		err := AppendFixedList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{0, 3}, false)
		vs := MustFixedColWithTypeCheck[int16](v)
		require.Equal(t, []int16{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := NewVec(types.T_int32.ToType())
		err := AppendFixedList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[int32](v)
		require.Equal(t, []int32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := NewVec(types.T_int64.ToType())
		err := AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[int64](v)
		require.Equal(t, []int64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := NewVec(types.T_uint8.ToType())
		err := AppendFixedList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[uint8](v)
		require.Equal(t, []uint8{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := NewVec(types.T_uint16.ToType())
		err := AppendFixedList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{0, 3}, false)
		vs := MustFixedColWithTypeCheck[uint16](v)
		require.Equal(t, []uint16{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := NewVec(types.T_uint32.ToType())
		err := AppendFixedList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[uint32](v)
		require.Equal(t, []uint32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := NewVec(types.T_uint64.ToType())
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, []uint64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := NewVec(types.T_float32.ToType())
		err := AppendFixedList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[float32](v)
		require.Equal(t, []float32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := NewVec(types.T_float64.ToType())
		err := AppendFixedList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[float64](v)
		require.Equal(t, []float64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := NewVec(types.T_text.ToType())
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := InefficientMustStrCol(v)
		require.Equal(t, []string{"2", "3"}, vs)
		require.Equal(t, [][]byte{[]byte("2"), []byte("3")}, InefficientMustBytesCol(v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := NewVec(types.T_date.ToType())
		err := AppendFixedList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[types.Date](v)
		require.Equal(t, []types.Date{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // datetime
		v := NewVec(types.T_datetime.ToType())
		err := AppendFixedList(v, []types.Datetime{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[types.Datetime](v)
		require.Equal(t, []types.Datetime{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		v := NewVec(types.T_time.ToType())
		err := AppendFixedList(v, []types.Time{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[types.Time](v)
		require.Equal(t, []types.Time{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		v := NewVec(types.T_timestamp.ToType())
		err := AppendFixedList(v, []types.Timestamp{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[types.Timestamp](v)
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
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Decimal64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		vs := make([]types.Decimal128, 4)
		v := NewVec(types.T_decimal128.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Decimal128](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		vs := make([]types.Uuid, 4)
		v := NewVec(types.T_uuid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Uuid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := NewVec(types.T_TS.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.TS](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := NewVec(types.T_Rowid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Rowid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // blockid
		vs := make([]types.Blockid, 4)
		v := NewVec(types.T_Blockid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Blockid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bit
		v := NewVec(types.T_bit.ToType())
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shrink([]int64{1, 2}, false)
		vs := MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, []uint64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestShrinkByMask(t *testing.T) {
	mp := mpool.MustNewZero()
	var bm bitmap.Bitmap
	bm.InitWithSize(2)
	bm.AddMany([]uint64{0, 1})
	var bmask bitmap.Bitmap
	bmask.InitWith(&bm)

	//{ // Array Float32
	//	v := NewVec(types.T_array_float32.ToType())
	//	err := AppendArrayList[float32](v, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}, nil, mp)
	//	require.NoError(t, err)
	//	v.ShrinkByMask(&bmask, false, 1)
	//	require.Equal(t, [][]float32{{2, 2, 2}, {3, 3, 3}}, MustArrayCol[float32](v))
	//	v.Free(mp)
	//	require.Equal(t, int64(0), mp.CurrNB())
	//
	//	v = NewVec(types.T_array_float32.ToType())
	//	err = AppendArrayList[float32](v, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}, nil, mp)
	//	require.NoError(t, err)
	//	v.ShrinkByMask(&bmask, true, 1)
	//	require.Equal(t, [][]float32{{1, 1, 1}}, MustArrayCol[float32](v))
	//	v.Free(mp)
	//	require.Equal(t, int64(0), mp.CurrNB())
	//}
	{ // Array Float64
		v := NewVec(types.T_array_float64.ToType())
		err := AppendArrayList[float64](v, [][]float64{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		require.Equal(t, [][]float64{{2, 2, 2}, {3, 3, 3}}, MustArrayCol[float64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_array_float64.ToType())
		err = AppendArrayList[float64](v, [][]float64{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		require.Equal(t, [][]float64{{1, 1, 1}}, MustArrayCol[float64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bool
		v := NewVec(types.T_bool.ToType())
		err := AppendFixedList(v, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[bool](v)
		require.Equal(t, []bool{false, true}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_bool.ToType())
		err = AppendFixedList(v, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[bool](v)
		require.Equal(t, []bool{true, false}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		v := NewVec(types.T_int8.ToType())
		err := AppendFixedList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[int8](v)
		require.Equal(t, []int8{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_int8.ToType())
		err = AppendFixedList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[int8](v)
		require.Equal(t, []int8{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := NewVec(types.T_int16.ToType())
		err := AppendFixedList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[int16](v)
		require.Equal(t, []int16{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_int16.ToType())
		err = AppendFixedList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[int16](v)
		require.Equal(t, []int16{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := NewVec(types.T_int32.ToType())
		err := AppendFixedList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[int32](v)
		require.Equal(t, []int32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_int32.ToType())
		err = AppendFixedList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[int32](v)
		require.Equal(t, []int32{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := NewVec(types.T_int64.ToType())
		err := AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[int64](v)
		require.Equal(t, []int64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_int64.ToType())
		err = AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[int64](v)
		require.Equal(t, []int64{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := NewVec(types.T_uint8.ToType())
		err := AppendFixedList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[uint8](v)
		require.Equal(t, []uint8{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_uint8.ToType())
		err = AppendFixedList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[uint8](v)
		require.Equal(t, []uint8{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := NewVec(types.T_uint16.ToType())
		err := AppendFixedList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[uint16](v)
		require.Equal(t, []uint16{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_uint16.ToType())
		err = AppendFixedList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[uint16](v)
		require.Equal(t, []uint16{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := NewVec(types.T_uint32.ToType())
		err := AppendFixedList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[uint32](v)
		require.Equal(t, []uint32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_uint32.ToType())
		err = AppendFixedList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[uint32](v)
		require.Equal(t, []uint32{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := NewVec(types.T_uint64.ToType())
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, []uint64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_uint64.ToType())
		err = AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, []uint64{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := NewVec(types.T_float32.ToType())
		err := AppendFixedList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[float32](v)
		require.Equal(t, []float32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_float32.ToType())
		err = AppendFixedList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[float32](v)
		require.Equal(t, []float32{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := NewVec(types.T_float64.ToType())
		err := AppendFixedList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[float64](v)
		require.Equal(t, []float64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_float64.ToType())
		err = AppendFixedList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[float64](v)
		require.Equal(t, []float64{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := NewVec(types.T_text.ToType())
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := InefficientMustStrCol(v)
		require.Equal(t, []string{"2", "3"}, vs)
		require.Equal(t, [][]byte{[]byte("2"), []byte("3")}, InefficientMustBytesCol(v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_text.ToType())
		err = AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = InefficientMustStrCol(v)
		require.Equal(t, []string{"1", "4"}, vs)
		require.Equal(t, [][]byte{[]byte("1"), []byte("4")}, InefficientMustBytesCol(v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := NewVec(types.T_date.ToType())
		err := AppendFixedList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[types.Date](v)
		require.Equal(t, []types.Date{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_date.ToType())
		err = AppendFixedList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[types.Date](v)
		require.Equal(t, []types.Date{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // datetime
		v := NewVec(types.T_datetime.ToType())
		err := AppendFixedList(v, []types.Datetime{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[types.Datetime](v)
		require.Equal(t, []types.Datetime{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_datetime.ToType())
		err = AppendFixedList(v, []types.Datetime{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[types.Datetime](v)
		require.Equal(t, []types.Datetime{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		v := NewVec(types.T_time.ToType())
		err := AppendFixedList(v, []types.Time{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[types.Time](v)
		require.Equal(t, []types.Time{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_time.ToType())
		err = AppendFixedList(v, []types.Time{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[types.Time](v)
		require.Equal(t, []types.Time{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		v := NewVec(types.T_timestamp.ToType())
		err := AppendFixedList(v, []types.Timestamp{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[types.Timestamp](v)
		require.Equal(t, []types.Timestamp{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_timestamp.ToType())
		err = AppendFixedList(v, []types.Timestamp{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[types.Timestamp](v)
		require.Equal(t, []types.Timestamp{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		vs := make([]types.Decimal64, 4)
		v := NewVec(types.T_decimal64.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Decimal64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		vs = make([]types.Decimal64, 4)
		v = NewVec(types.T_decimal64.ToType())
		err = AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		require.Equal(t, append(vs[:1], vs[3]), MustFixedColWithTypeCheck[types.Decimal64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		vs := make([]types.Decimal128, 4)
		v := NewVec(types.T_decimal128.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Decimal128](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		vs = make([]types.Decimal128, 4)
		v = NewVec(types.T_decimal128.ToType())
		err = AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		require.Equal(t, append(vs[:1], vs[3]), MustFixedColWithTypeCheck[types.Decimal128](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		vs := make([]types.Uuid, 4)
		v := NewVec(types.T_uuid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Uuid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		vs = make([]types.Uuid, 4)
		v = NewVec(types.T_uuid.ToType())
		err = AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		require.Equal(t, append(vs[:1], vs[3]), MustFixedColWithTypeCheck[types.Uuid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := NewVec(types.T_TS.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.TS](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		vs = make([]types.TS, 4)
		v = NewVec(types.T_TS.ToType())
		err = AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		require.Equal(t, append(vs[:1], vs[3]), MustFixedColWithTypeCheck[types.TS](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := NewVec(types.T_Rowid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Rowid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		vs = make([]types.Rowid, 4)
		v = NewVec(types.T_Rowid.ToType())
		err = AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		require.Equal(t, append(vs[:1], vs[3]), MustFixedColWithTypeCheck[types.Rowid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // blockid
		vs := make([]types.Blockid, 4)
		v := NewVec(types.T_Blockid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Blockid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		vs = make([]types.Blockid, 4)
		v = NewVec(types.T_Blockid.ToType())
		err = AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		require.Equal(t, append(vs[:1], vs[3]), MustFixedColWithTypeCheck[types.Blockid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bit
		v := NewVec(types.T_bit.ToType())
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, false, 1)
		vs := MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, []uint64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_bit.ToType())
		err = AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.ShrinkByMask(&bmask, true, 1)
		vs = MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, []uint64{1, 4}, vs)
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
		vs := MustFixedColWithTypeCheck[bool](v)
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
		vs := MustFixedColWithTypeCheck[int8](v)
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
		vs := MustFixedColWithTypeCheck[int16](v)
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
		vs := MustFixedColWithTypeCheck[int32](v)
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
		vs := MustFixedColWithTypeCheck[int64](v)
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
		vs := MustFixedColWithTypeCheck[uint8](v)
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
		vs := MustFixedColWithTypeCheck[uint16](v)
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
		vs := MustFixedColWithTypeCheck[uint32](v)
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
		vs := MustFixedColWithTypeCheck[uint64](v)
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
		vs := MustFixedColWithTypeCheck[float32](v)
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
		vs := MustFixedColWithTypeCheck[float64](v)
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
		vs := InefficientMustStrCol(v)
		require.Equal(t, []string{"2", "3"}, vs)
		require.Equal(t, [][]byte{[]byte("2"), []byte("3")}, InefficientMustBytesCol(v))
		require.Equal(t, "[2 3]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := NewVec(types.T_date.ToType())
		err := AppendFixedList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		vs := MustFixedColWithTypeCheck[types.Date](v)
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
		vs := MustFixedColWithTypeCheck[types.Datetime](v)
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
		vs := MustFixedColWithTypeCheck[types.Time](v)
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
		vs := MustFixedColWithTypeCheck[types.Timestamp](v)
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
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Decimal64](v))
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
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Decimal128](v))
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
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Uuid](v))
		require.Equal(t, "[00000000-0000-0000-0000-000000000000 00000000-0000-0000-0000-000000000000]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := NewVec(types.T_TS.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.TS](v))
		require.Equal(t, "[0-0 0-0]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := NewVec(types.T_Rowid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Rowid](v))
		require.Equal(t, "[00000000-0000-0000-0000-000000000000-0-0-0 00000000-0000-0000-0000-000000000000-0-0-0]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // blockid
		vs := make([]types.Blockid, 4)
		v := NewVec(types.T_Blockid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Blockid](v))
		require.Equal(t, "[[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0] [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bit
		v := NewVec(types.T_bit.ToType())
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		err = v.Shuffle([]int64{1, 2}, mp)
		require.NoError(t, err)
		vs := MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, []uint64{2, 3}, vs)
		require.Equal(t, "[2 3]", v.String())
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
		require.Equal(t, MustFixedColWithTypeCheck[int8](v), MustFixedColWithTypeCheck[int8](w))
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bit
		v := NewVec(types.T_bit.ToType())
		err := AppendFixedList(v, []uint64{0, 0, 1, 0}, nil, mp)
		require.NoError(t, err)
		w := NewVec(types.T_bit.ToType())
		err = AppendFixedList(w, []uint64{0, 0, 0, 0}, nil, mp)
		require.NoError(t, err)
		err = v.Copy(w, 2, 0, mp)
		require.NoError(t, err)
		require.Equal(t, MustFixedColWithTypeCheck[uint64](v), MustFixedColWithTypeCheck[uint64](w))
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
		require.Equal(t, InefficientMustStrCol(v), InefficientMustStrCol(w))
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
	require.Equal(t, int32(10), GetFixedAtWithTypeCheck[int32](v4, 0))
	require.Equal(t, int32(10), GetFixedAtWithTypeCheck[int32](v4, 1))
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
	require.Equal(t, int32(1), GetFixedAtWithTypeCheck[int32](vec2, 0))
	require.True(t, vec2.GetNulls().Contains(uint64(1)))
	require.Equal(t, int32(3), GetFixedAtWithTypeCheck[int32](vec2, 2))

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

func TestMarshalAndUnMarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.T_int8.ToType())
	err := AppendFixedList(v, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	w := NewVecFromReuse()
	err = w.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, MustFixedColWithTypeCheck[int8](v), MustFixedColWithTypeCheck[int8](w))
	w = NewVecFromReuse()
	err = w.UnmarshalBinaryWithCopy(data, mp)
	require.NoError(t, err)
	require.Equal(t, MustFixedColWithTypeCheck[int8](v), MustFixedColWithTypeCheck[int8](w))
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
	w := NewVecFromReuse()
	err = w.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, InefficientMustStrCol(v), InefficientMustStrCol(w))
	w = NewVecFromReuse()
	err = w.UnmarshalBinaryWithCopy(data, mp)
	require.NoError(t, err)
	require.Equal(t, InefficientMustStrCol(v), InefficientMustStrCol(w))
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
		w := NewVecFromReuse()
		err = w.UnmarshalBinary(data)
		require.NoError(t, err)
		require.Equal(t, MustArrayCol[float32](v), MustArrayCol[float32](w))
		w = NewVecFromReuse()
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
		w := NewVecFromReuse()
		err = w.UnmarshalBinary(data)
		require.NoError(t, err)
		require.Equal(t, MustArrayCol[float64](v), MustArrayCol[float64](w))
		w = NewVecFromReuse()
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
	require.Equal(t, int32(1), GetFixedAtWithTypeCheck[int32](vec2, 0))
	require.True(t, vec2.GetNulls().Contains(uint64(1)))
	require.Equal(t, int32(3), GetFixedAtWithTypeCheck[int32](vec2, 2))
	vec2.Free(mp)

	vec6, err := vec1.Window(1, vec1.Length())
	require.NoError(t, err)

	t.Log(vec6.String())
	require.True(t, vec6.NeedDup())
	require.True(t, vec6.GetNulls().Contains(uint64(0)))
	require.Equal(t, int32(3), GetFixedAtWithTypeCheck[int32](vec6, 1))
	vec6.Free(mp)

	require.False(t, vec1.NeedDup())
	require.Equal(t, int32(1), GetFixedAtWithTypeCheck[int32](vec1, 0))
	require.True(t, vec1.GetNulls().Contains(uint64(1)))
	require.Equal(t, int32(3), GetFixedAtWithTypeCheck[int32](vec1, 2))
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
		ws := MustFixedColWithTypeCheck[bool](w)
		require.Equal(t, []bool{false}, ws)
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bit
		v := NewVec(types.T_bit.ToType())
		w := NewConstNull(types.T_uint64.ToType(), 0, mp)
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		sf := GetConstSetFunction(types.T_uint64.ToType(), mp)
		err = sf(w, v, 1, 1)
		require.NoError(t, err)
		ws := MustFixedColWithTypeCheck[uint64](w)
		require.Equal(t, []uint64{2}, ws)
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
		ws := MustFixedColWithTypeCheck[int8](w)
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
		ws := MustFixedColWithTypeCheck[int16](w)
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
		ws := MustFixedColWithTypeCheck[int32](w)
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
		ws := MustFixedColWithTypeCheck[int64](w)
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
		ws := MustFixedColWithTypeCheck[uint8](w)
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
		ws := MustFixedColWithTypeCheck[uint16](w)
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
		ws := MustFixedColWithTypeCheck[uint32](w)
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
		ws := MustFixedColWithTypeCheck[uint64](w)
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
		ws := MustFixedColWithTypeCheck[float32](w)
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
		ws := MustFixedColWithTypeCheck[float64](w)
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
		ws := InefficientMustStrCol(w)
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

func TestSortAndCompact(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.New(types.T_array_float32, 4, 0))
	err := AppendArrayList(v, [][]float32{{1, 2, 3, 0}, {1, 2, 3, 0}}, nil, mp)
	require.NoError(t, err)
	v.InplaceSortAndCompact()
	require.Equal(t, v.length, 1)

	v = NewVec(types.New(types.T_array_float64, 4, 0))
	err = AppendArrayList(v, [][]float64{{1.1, 2, 3, 0}, {1.1, 2, 3, 0}}, nil, mp)
	require.NoError(t, err)
	v.InplaceSortAndCompact()
	require.Equal(t, v.length, 1)
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
			ws := MustFixedColWithTypeCheck[bool](w)
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
			ws := MustFixedColWithTypeCheck[bool](w)
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
			ws := InefficientMustBytesCol(w)
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
			ws := InefficientMustBytesCol(w)
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

func TestMisc(t *testing.T) {
	vec := NewVec(types.T_int8.ToType())
	var gsp nulls.Nulls
	vec.SetGrouping(&gsp)
	require.False(t, vec.HasGrouping())
	gsp.Add(1, 3)
	vec.SetGrouping(&gsp)
	require.True(t, vec.HasGrouping())
	require.True(t, vec.GetGrouping().Contains(1))
	require.True(t, vec.GetGrouping().Contains(3))

	mp := mpool.MustNewZero()
	vec2 := NewRollupConst(types.T_int8.ToType(), 5, mp)
	defer vec2.Free(mp)
	vec3 := NewVec(types.T_int8.ToType())
	defer vec3.Free(mp)
	require.False(t, vec3.HasGrouping())
	err := vec3.UnionOne(vec2, 0, mp)
	require.NoError(t, err)
	require.True(t, vec3.HasGrouping())
	require.True(t, vec3.GetGrouping().Contains(0))

	vec4 := NewVec(types.T_int8.ToType())
	defer vec4.Free(mp)
	err = vec4.UnionMulti(vec2, 1, 2, mp)
	require.NoError(t, err)
	require.True(t, vec4.HasGrouping())
	require.True(t, vec4.GetGrouping().Contains(0))
	require.True(t, vec4.GetGrouping().Contains(1))
	require.False(t, vec4.GetGrouping().Contains(2))

	vec5 := NewVec(types.T_int8.ToType())
	defer vec5.Free(mp)
	vec6 := NewConstNull(types.T_int8.ToType(), 5, mp)
	defer vec6.Free(mp)

	err = AppendFixed(vec5, int8(1), false, mp)
	require.NoError(t, err)
	err = vec5.UnionMulti(vec6, 1, 2, mp)
	require.NoError(t, err)
	require.False(t, vec5.GetNulls().Contains(0))
	require.True(t, vec5.GetNulls().Contains(1))
	require.True(t, vec5.GetNulls().Contains(2))
	require.False(t, vec5.GetNulls().Contains(3))

	vec7 := NewVec(types.T_char.ToType())
	defer vec7.Free(mp)
	err = AppendMultiBytes(vec7, nil, true, 2, mp)
	require.NoError(t, err)
	require.True(t, vec7.GetNulls().Contains(0))
	require.True(t, vec7.GetNulls().Contains(1))
	require.False(t, vec7.GetNulls().Contains(2))
	require.Equal(t, 2, vec7.Length())

	fixSizedTypes := []types.Type{
		types.T_int8.ToType(),
		types.T_int16.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_uint8.ToType(),
		types.T_uint16.ToType(),
		types.T_uint32.ToType(),
		types.T_uint64.ToType(),
		types.T_float32.ToType(),
		types.T_float64.ToType(),
		types.T_bool.ToType(),
		types.T_bit.ToType(),
		types.T_Rowid.ToType(),
		types.T_TS.ToType(),
		types.T_uuid.ToType(),
		types.T_datetime.ToType(),
		types.T_timestamp.ToType(),
	}
	gsp.Clear()
	gsp.Add(0, 1, 2)
	for _, fType := range fixSizedTypes {
		v1 := NewVec(fType)
		v2 := NewVec(fType)
		defer v1.Free(mp)
		defer v2.Free(mp)
		switch fType.Oid {
		case types.T_int8:
			vals := make([]int8, 2)
			err = AppendFixedList[int8](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]int8, 3)
			err = AppendFixedList[int8](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_int16:
			vals := make([]int16, 2)
			err = AppendFixedList[int16](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]int16, 3)
			err = AppendFixedList[int16](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_int32:
			vals := make([]int32, 2)
			err = AppendFixedList[int32](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]int32, 3)
			err = AppendFixedList[int32](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_int64:
			vals := make([]int64, 2)
			err = AppendFixedList[int64](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]int64, 3)
			err = AppendFixedList[int64](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_uint8:
			vals := make([]uint8, 2)
			err = AppendFixedList[uint8](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint8, 3)
			err = AppendFixedList[uint8](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_uint16:
			vals := make([]uint16, 2)
			err = AppendFixedList[uint16](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint16, 3)
			err = AppendFixedList[uint16](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_uint32:
			vals := make([]uint32, 2)
			err = AppendFixedList[uint32](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint32, 3)
			err = AppendFixedList[uint32](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_uint64:
			vals := make([]uint64, 2)
			err = AppendFixedList[uint64](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint64, 3)
			err = AppendFixedList[uint64](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_float32:
			vals := make([]float32, 2)
			err = AppendFixedList[float32](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]float32, 3)
			err = AppendFixedList[float32](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_float64:
			vals := make([]float64, 2)
			err = AppendFixedList[float64](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]float64, 3)
			err = AppendFixedList[float64](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_bool:
			vals := make([]bool, 2)
			err = AppendFixedList[bool](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]bool, 3)
			err = AppendFixedList[bool](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_bit:
			vals := make([]uint64, 2)
			err = AppendFixedList[uint64](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint64, 3)
			err = AppendFixedList[uint64](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_Rowid:
			vals := make([]types.Rowid, 2)
			err = AppendFixedList[types.Rowid](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.Rowid, 3)
			err = AppendFixedList[types.Rowid](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_TS:
			vals := make([]types.TS, 2)
			err = AppendFixedList[types.TS](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.TS, 3)
			err = AppendFixedList[types.TS](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_uuid:
			vals := make([]types.Uuid, 2)
			err = AppendFixedList[types.Uuid](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.Uuid, 3)
			err = AppendFixedList[types.Uuid](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_datetime:
			vals := make([]types.Datetime, 2)
			err = AppendFixedList[types.Datetime](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.Datetime, 3)
			err = AppendFixedList[types.Datetime](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		case types.T_timestamp:
			vals := make([]types.Timestamp, 2)
			err = AppendFixedList[types.Timestamp](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.Timestamp, 3)
			err = AppendFixedList[types.Timestamp](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
			v2.SetGrouping(&gsp)
		}
		union := GetUnionAllFunction(fType, mp)
		err = union(v1, v2)
		require.NoError(t, err)
		require.Equal(t, 5, v1.Length())
		require.Equal(t, 5, v1.GetNulls().Count())
		require.Equal(t, 3, v1.GetGrouping().Count())
	}
}

func TestGetAny(t *testing.T) {
	{ // test const vector
		mp := mpool.MustNewZero()
		v := NewVec(types.T_int8.ToType())
		err := AppendFixed(v, int8(0), false, mp)
		require.NoError(t, err)
		s := GetAny(v, 0, false)
		v.Free(mp)
		require.Equal(t, int8(0), s.(int8))
	}
	{ // test const vector
		mp := mpool.MustNewZero()
		w := NewVec(types.T_varchar.ToType())
		err := AppendBytes(w, []byte("x"), false, mp)
		require.NoError(t, err)
		s := GetAny(w, 0, false)
		require.Equal(t, []byte("x"), s.([]byte))
		w.Free(mp)
	}
	{ // bool
		mp := mpool.MustNewZero()
		w := NewVec(types.T_bool.ToType())
		err := AppendFixedList(w, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		s := GetAny(w, 0, false)
		require.Equal(t, true, s.(bool))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		mp := mpool.MustNewZero()
		w := NewVec(types.T_int8.ToType())
		err := AppendFixedList(w, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, int8(1), s.(int8))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		mp := mpool.MustNewZero()
		w := NewVec(types.T_int16.ToType())
		err := AppendFixedList(w, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, int16(1), s.(int16))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		mp := mpool.MustNewZero()
		w := NewVec(types.T_int32.ToType())
		err := AppendFixedList(w, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, int32(1), s.(int32))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		mp := mpool.MustNewZero()
		w := NewVec(types.T_int64.ToType())
		err := AppendFixedList(w, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, int64(1), s.(int64))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		mp := mpool.MustNewZero()
		w := NewVec(types.T_uint8.ToType())
		err := AppendFixedList(w, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, uint8(1), s.(uint8))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		mp := mpool.MustNewZero()
		w := NewVec(types.T_uint16.ToType())
		err := AppendFixedList(w, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, uint16(1), s.(uint16))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		mp := mpool.MustNewZero()
		w := NewVec(types.T_uint32.ToType())
		err := AppendFixedList(w, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, uint32(1), s.(uint32))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		mp := mpool.MustNewZero()
		w := NewVec(types.T_uint64.ToType())
		err := AppendFixedList(w, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, uint64(1), s.(uint64))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		mp := mpool.MustNewZero()
		v := NewVec(types.T_text.ToType())
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)

		s := GetAny(v, 0, false)
		require.Equal(t, []byte("1"), s.([]byte))

		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		mp := mpool.MustNewZero()
		v := NewVec(types.T_time.ToType())
		err := AppendFixedList(v, []types.Time{12 * 3600 * 1000 * 1000, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		s := GetAny(v, 0, false)
		require.Equal(t, types.Time(12*3600*1000*1000), s.(types.Time))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		mp := mpool.MustNewZero()
		v := NewVec(types.T_timestamp.ToType())
		err := AppendFixedList(v, []types.Timestamp{10000000, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		s := GetAny(v, 0, false)
		require.Equal(t, types.Timestamp(10000000), s.(types.Timestamp))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		mp := mpool.MustNewZero()
		typ := types.T_decimal64.ToType()
		typ.Scale = 2
		v := NewVec(typ)
		err := AppendFixedList(v, []types.Decimal64{1234, 2000}, nil, mp)
		require.NoError(t, err)
		s := GetAny(v, 0, false)
		require.Equal(t, types.Decimal64(1234), s.(types.Decimal64))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		mp := mpool.MustNewZero()
		typ := types.T_decimal128.ToType()
		typ.Scale = 2
		v := NewVec(typ)
		err := AppendFixedList(v, []types.Decimal128{{B0_63: 1234, B64_127: 0}, {B0_63: 2345, B64_127: 0}}, nil, mp)
		require.NoError(t, err)
		s := GetAny(v, 0, false)
		require.Equal(t, types.Decimal128{B0_63: 1234, B64_127: 0}, s.(types.Decimal128))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		mp := mpool.MustNewZero()
		vs := make([]types.Uuid, 4)
		v := NewVec(types.T_uuid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		s := GetAny(v, 0, false)
		require.Equal(t, "00000000-0000-0000-0000-000000000000", fmt.Sprint(s))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		mp := mpool.MustNewZero()
		vs := make([]types.TS, 4)
		v := NewVec(types.T_TS.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		s := GetAny(v, 0, false)
		require.Equal(t, types.TS(types.TS{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}), s)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		mp := mpool.MustNewZero()
		vs := make([]types.Rowid, 4)
		v := NewVec(types.T_Rowid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		s := GetAny(v, 0, false)
		require.Equal(t, types.Rowid(types.Rowid{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}), s)
		v.Free(mp)
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
	b.Run("ToSlice", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ToSlice(vec, &slice)
		}
	})
	if slice[0] != 42 {
		b.Fatalf("got %v", slice)
	}
	b.Run("ToSliceNoTypeCheck", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ToSliceNoTypeCheck(vec, &slice)
		}
	})
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
		MustFixedColWithTypeCheck[int8](vec)
	}
}

func TestRowToString(t *testing.T) {
	mp := mpool.MustNewZero()

	{ // Array Float32
		v := NewVec(types.T_array_float32.ToType())
		err := AppendArrayList(v, [][]float32{{1, 1}}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "[1, 1]", v.RowToString(0))
		err = AppendArrayList(v, [][]float32{{2, 2}, {3, 3}}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "[2, 2]", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // Array Float64
		v := NewVec(types.T_array_float64.ToType())
		err := AppendArrayList(v, [][]float64{{1, 1}}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "[1, 1]", v.RowToString(0))
		err = AppendArrayList(v, [][]float64{{2, 2}, {3, 3}}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "[2, 2]", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bool
		v := NewVec(types.T_bool.ToType())
		err := AppendFixed(v, true, false, mp)
		require.NoError(t, err)
		require.Equal(t, "true", v.RowToString(0))
		err = AppendFixed(v, false, true, mp)
		require.NoError(t, err)
		require.Equal(t, "null", v.RowToString(1))
		v.Free(mp)

		v0 := NewVec(types.T_bool.ToType())
		err = AppendFixed(v0, false, true, mp)
		require.NoError(t, err)
		require.Equal(t, "null", v0.RowToString(0))
		err = AppendFixed(v0, true, false, mp)
		require.NoError(t, err)
		require.Equal(t, "true", v0.RowToString(1))
		v0.Free(mp)

		v1 := NewConstNull(types.T_bool.ToType(), 1, mp)
		require.Equal(t, "null", v1.RowToString(1))
		v1.Free(mp)

		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		v := NewVec(types.T_int8.ToType())
		err := AppendFixedList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := NewVec(types.T_int16.ToType())
		err := AppendFixedList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := NewVec(types.T_int32.ToType())
		err := AppendFixedList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := NewVec(types.T_int64.ToType())
		err := AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := NewVec(types.T_uint8.ToType())
		err := AppendFixedList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := NewVec(types.T_uint16.ToType())
		err := AppendFixedList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := NewVec(types.T_uint32.ToType())
		err := AppendFixedList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := NewVec(types.T_uint64.ToType())
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := NewVec(types.T_float32.ToType())
		err := AppendFixedList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := NewVec(types.T_float64.ToType())
		err := AppendFixedList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := NewVec(types.T_text.ToType())
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := NewVec(types.T_date.ToType())
		err := AppendFixedList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "0001-01-03", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // datetime
		// Test 1: Non-const vector with non-null value
		v := NewVec(types.T_datetime.ToType())
		scale := types.Datetime(types.MicroSecsPerSec * types.SecsPerDay)
		err := AppendFixedList(v, []types.Datetime{1 * scale, 2 * scale, 3 * scale, 4 * scale}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "0001-01-03 00:00:00", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		// Test 2: Non-const vector with null value
		v2 := NewVec(types.T_datetime.ToType())
		err = AppendFixedList(v2, []types.Datetime{1 * scale, 2 * scale, 3 * scale, 4 * scale}, []bool{false, false, true, false}, mp)
		require.NoError(t, err)
		require.Equal(t, "null", v2.RowToString(2))
		v2.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		// Test 3: Const null vector
		v3 := NewConstNull(types.T_datetime.ToType(), 1, mp)
		require.Equal(t, "null", v3.RowToString(0))
		v3.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		// Test 4: Const vector with null
		v4, err := NewConstFixed(types.T_datetime.ToType(), 1*scale, 1, mp)
		require.NoError(t, err)
		nulls.Add(&v4.nsp, 0)
		require.Equal(t, "null", v4.RowToString(0))
		v4.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		// Test 5: Const vector with non-null value
		v5, err := NewConstFixed(types.T_datetime.ToType(), 2*scale, 1, mp)
		require.NoError(t, err)
		require.Equal(t, "0001-01-03 00:00:00", v5.RowToString(0))
		v5.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		// Test 6: Non-const vector with different scale
		v6 := NewVec(types.T_datetime.ToType())
		v6.SetTypeScale(3)
		err = AppendFixedList(v6, []types.Datetime{1 * scale, 2 * scale}, nil, mp)
		require.NoError(t, err)
		// The output should include microseconds with scale 3
		result := v6.RowToString(0)
		require.Contains(t, result, "0001-01-02")
		v6.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		// Test 7: Const vector with different scale
		v7, err := NewConstFixed(types.T_datetime.ToType(), 2*scale, 1, mp)
		require.NoError(t, err)
		v7.SetTypeScale(6)
		result2 := v7.RowToString(0)
		require.Contains(t, result2, "0001-01-03")
		v7.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		// Test 8: Non-const vector with null at index 0
		v8 := NewVec(types.T_datetime.ToType())
		err = AppendFixedList(v8, []types.Datetime{1 * scale, 2 * scale}, []bool{true, false}, mp)
		require.NoError(t, err)
		require.Equal(t, "null", v8.RowToString(0))
		require.Equal(t, "0001-01-03 00:00:00", v8.RowToString(1))
		v8.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		// Test 9: Ensure all code paths are covered - const vector with nulls.Add but not const null
		// This tests the else branch at line 2747-2748 when const is true but not IsConstNull
		v9, err := NewConstFixed(types.T_datetime.ToType(), 3*scale, 1, mp)
		require.NoError(t, err)
		// Ensure it's not const null (has data)
		require.False(t, v9.IsConstNull())
		// Ensure nsp doesn't contain 0 (line 2745 check should be false)
		require.False(t, nulls.Contains(&v9.nsp, 0))
		result9 := v9.RowToString(0)
		require.Contains(t, result9, "0001-01-04")
		v9.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		// Test 10: Ensure all code paths are covered - non-const vector else branch at line 2753-2754
		v10 := NewVec(types.T_datetime.ToType())
		err = AppendFixedList(v10, []types.Datetime{5 * scale, 6 * scale}, []bool{false, false}, mp)
		require.NoError(t, err)
		// Ensure idx 0 is not null (line 2751 check should be false)
		require.False(t, v10.nsp.Contains(0))
		result10 := v10.RowToString(0)
		require.Contains(t, result10, "0001-01-06")
		v10.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		v := NewVec(types.T_time.ToType())
		scale := types.Time(types.MicroSecsPerSec)
		err := AppendFixedList(v, []types.Time{1 * scale, 2 * scale, 3 * scale, 4 * scale}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "00:00:02", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		v := NewVec(types.T_timestamp.ToType())
		// Use FromClockZone with UTC to create timestamp that will display correctly
		// RowToString uses time.Local, so we need to create timestamp that accounts for local timezone
		// If we want to display "1970-01-01 00:00:00" in local time, we need to create timestamp
		// that represents that time in local timezone
		utc := time.UTC
		ts := types.FromClockZone(utc, 1970, 1, 1, 0, 0, 0, 0)
		err := AppendFixedList(v, []types.Timestamp{1, ts, 3, 4}, nil, mp)
		require.NoError(t, err)
		// RowToString uses time.Local, so the displayed time will be in local timezone
		// If local timezone is UTC+8, UTC time 1970-01-01 00:00:00 will display as 1970-01-01 08:00:00
		// So we need to adjust the expected value based on local timezone offset
		_, offset := time.Now().In(time.Local).Zone()
		expectedHour := offset / 3600
		expectedStr := fmt.Sprintf("1970-01-01 %02d:00:00", expectedHour)
		require.Equal(t, expectedStr, v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		vs := make([]types.Decimal64, 4)
		v := NewVec(types.T_decimal64.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "0", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		vs := make([]types.Decimal128, 4)
		v := NewVec(types.T_decimal128.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "0", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		vs := make([]types.Uuid, 4)
		v := NewVec(types.T_uuid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "00000000-0000-0000-0000-000000000000", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := NewVec(types.T_TS.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "0-0", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := NewVec(types.T_Rowid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "00000000-0000-0000-0000-000000000000-0-0-0", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // blockid
		vs := make([]types.Blockid, 4)
		v := NewVec(types.T_Blockid.ToType())
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bit
		v := NewVec(types.T_bit.ToType())
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestIntersection2VectorOrdered(t *testing.T) {
	const ll = 100
	const cnt = 10

	mp := mpool.MustNewZero()

	for range cnt {
		lenA := rand.Intn(ll) + ll/5
		lenB := rand.Intn(ll) + ll/5

		var a []int32 = make([]int32, lenA)
		var b []int32 = make([]int32, lenB)

		for i := 0; i < lenA; i++ {
			a[i] = rand.Int31() % (ll / 2)
		}

		for i := 0; i < lenB; i++ {
			b[i] = rand.Int31() % (ll / 2)
		}

		cmp := func(x, y int32) int {
			return int(x) - int(y)
		}

		slices.SortFunc(a, cmp)
		slices.SortFunc(b, cmp)

		ret := NewVec(types.T_int32.ToType())
		Intersection2VectorOrdered(a, b, ret, mp, cmp)

		mm := make(map[int32]struct{})

		for i := range a {
			for j := range b {
				if cmp(a[i], b[j]) == 0 {
					mm[a[i]] = struct{}{}
				}
			}
		}

		col := MustFixedColWithTypeCheck[int32](ret)

		require.Equal(t, len(mm), len(col))

		for i := range col {
			_, ok := mm[col[i]]
			require.True(t, ok)
		}
	}
}

func TestIntersection2VectorVarlen(t *testing.T) {
	const ll = 100
	const cnt = 10

	mp := mpool.MustNewZero()

	for range cnt {
		lenA := rand.Intn(ll) + ll/5
		lenB := rand.Intn(ll) + ll/5

		var a = make([]string, lenA)
		var b = make([]string, lenB)

		va := NewVec(types.T_text.ToType())
		vb := NewVec(types.T_text.ToType())

		for i := 0; i < lenA; i++ {
			x := rand.Int31() % (ll / 2)
			a[i] = fmt.Sprintf("%d", x)
		}

		for i := 0; i < lenB; i++ {
			x := rand.Int31() % (ll / 2)
			b[i] = fmt.Sprintf("%d", x)
		}

		cmp := func(x, y string) int {
			return strings.Compare(string(x), string(y))
		}

		slices.SortFunc(a, cmp)
		slices.SortFunc(b, cmp)

		for i := 0; i < lenA; i++ {
			AppendBytes(va, []byte(a[i]), false, mp)
		}

		for i := 0; i < lenB; i++ {
			AppendBytes(vb, []byte(b[i]), false, mp)
		}

		ret := NewVec(types.T_text.ToType())
		Intersection2VectorVarlen(va, vb, ret, mp)

		mm := make(map[string]struct{})

		for i := range a {
			for j := range b {
				if cmp(a[i], b[j]) == 0 {
					mm[a[i]] = struct{}{}
				}
			}
		}

		col, area := MustVarlenaRawData(ret)

		require.Equal(t, len(mm), len(col))

		for i := range col {
			_, ok := mm[col[i].GetString(area)]
			require.True(t, ok)
		}
	}
}

func TestUnion2VectorOrdered(t *testing.T) {
	const ll = 100
	const cnt = 10

	mp := mpool.MustNewZero()

	for range cnt {
		lenA := rand.Intn(ll) + ll/5
		lenB := rand.Intn(ll) + ll/5

		var a []int32 = make([]int32, lenA)
		var b []int32 = make([]int32, lenB)

		for i := 0; i < lenA; i++ {
			a[i] = rand.Int31() % (ll / 2)
		}

		for i := 0; i < lenB; i++ {
			b[i] = rand.Int31() % (ll / 2)
		}

		cmp := func(x, y int32) int {
			return int(x) - int(y)
		}

		slices.SortFunc(a, cmp)
		slices.SortFunc(b, cmp)

		ret := NewVec(types.T_int32.ToType())
		Union2VectorOrdered(a, b, ret, mp, cmp)

		mm := make(map[int32]struct{})

		for i := range a {
			mm[a[i]] = struct{}{}
		}

		for i := range b {
			mm[b[i]] = struct{}{}
		}

		col := MustFixedColWithTypeCheck[int32](ret)

		require.Equal(t, len(mm), len(col))

		for i := range col {
			_, ok := mm[col[i]]
			require.True(t, ok)
		}
	}
}

func TestUnion2VectorVarlen(t *testing.T) {
	const ll = 100
	const cnt = 10

	mp := mpool.MustNewZero()

	for range cnt {
		lenA := rand.Intn(ll) + ll/5
		lenB := rand.Intn(ll) + ll/5

		var a = make([]string, lenA)
		var b = make([]string, lenB)

		va := NewVec(types.T_text.ToType())
		vb := NewVec(types.T_text.ToType())

		for i := 0; i < lenA; i++ {
			x := rand.Int31() % (ll / 2)
			a[i] = fmt.Sprintf("%d", x)
		}

		for i := 0; i < lenB; i++ {
			x := rand.Int31() % (ll / 2)
			b[i] = fmt.Sprintf("%d", x)
		}

		cmp := func(x, y string) int {
			return strings.Compare(string(x), string(y))
		}

		slices.SortFunc(a, cmp)
		slices.SortFunc(b, cmp)

		for i := 0; i < lenA; i++ {
			AppendBytes(va, []byte(a[i]), false, mp)
		}

		for i := 0; i < lenB; i++ {
			AppendBytes(vb, []byte(b[i]), false, mp)
		}

		ret := NewVec(types.T_text.ToType())
		Union2VectorValen(va, vb, ret, mp)

		mm := make(map[string]struct{})

		for i := range a {
			mm[a[i]] = struct{}{}
		}

		for i := range b {
			mm[b[i]] = struct{}{}
		}

		col, area := MustVarlenaRawData(ret)

		require.Equal(t, len(mm), len(col))

		for i := range col {
			_, ok := mm[col[i].GetString(area)]
			require.True(t, ok)
		}
	}
}

func TestProtoVector(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_char.ToType())
	defer vec.Free(mp)
	ss := "xxxxxx"
	err := AppendBytes(vec, []byte(ss), false, mp)
	require.NoError(t, err)
	vec.ResetWithSameType()
	vec2, err := VectorToProtoVector(vec)
	require.NoError(t, err)
	_, err = ProtoVectorToVector(vec2)
	require.NoError(t, err)
}

func TestVectorPoolTypeChangeBug_Issue23295(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	// Step 1: Create vector with int8 type, allocate small buffer (8 bytes)
	vec := NewVec(types.T_int8.ToType())
	err = AppendMultiFixed(vec, int8(1), false, 8, mp)
	require.NoError(t, err)
	// Now: cap(data)=8, col.Cap=8 (for int8, 8 bytes = 8 elements)

	// Step 2: Reset to TS type (12 bytes per element)
	// cap(data)=8 < 12, so setFromVector's condition `cap(v.data) >= sz` fails
	// Without the fix, col.Ptr and col.Cap keep stale values (Cap=8)
	tsType := types.T_TS.ToType()
	vec.ResetWithNewType(&tsType)

	// Step 3: ToSlice with stale Cap=8 would create invalid slice
	// It thinks there are 8 TS elements (96 bytes), but buffer is only 8 bytes!
	// With -race, checkType will panic on type mismatch if col.Ptr is stale
	var col []types.TS
	ToSlice(vec, &col)
	require.Equal(t, 0, cap(col)) // After fix: cap should be 0, not stale value 8

	// AppendMultiFixed(vec, types.TS{}, false, 0, mp) also triggers this bug,
	// because it calls ToSlice without extending buffer

	vec.Free(mp)
}
