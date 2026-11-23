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
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestLength(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType(), mp)
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
		vec := NewVec(types.New(types.T_array_float32, 3, 0), mp)
		err := AppendArrayList[float32](vec, [][]float32{{1, 2, 3}, {4, 5, 6}}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, 2, vec.Length())
		vec.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{
		//Array Float64
		mp := mpool.MustNewZero()
		vec := NewVec(types.New(types.T_array_float64, 3, 0), mp)
		err := AppendArrayList[float64](vec, [][]float64{{1, 2, 3}, {4, 5, 6}}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, 2, vec.Length())
		vec.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestAppendCheckpointRollback(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_varchar.ToType(), mp)
	defer vec.Free(mp)
	first := strings.Repeat("a", 64)
	require.NoError(t, AppendBytes(vec, []byte(first), false, mp))
	vec.SetSorted(true)
	checkpoint := vec.MakeAppendCheckpoint()

	require.NoError(t, AppendBytes(vec, []byte(strings.Repeat("b", 96)), false, mp))
	vec.GetNulls().Set(1)
	vec.SetSorted(false)
	vec.RollbackAppend(checkpoint, 2)

	require.Equal(t, 1, vec.Length())
	require.Equal(t, []string{first}, InefficientMustStrCol(vec))
	require.False(t, vec.GetNulls().Contains(0))
	require.False(t, vec.GetNulls().Contains(1))
	require.True(t, vec.GetSorted())
}

func TestAppendCheckpointScratch(t *testing.T) {
	checkpoints, required, err := AppendCheckpointScratch(nil, 2)
	require.NoError(t, err)
	require.Nil(t, checkpoints)
	require.Positive(t, required)

	storage := make([]byte, required)
	checkpoints, exact, err := AppendCheckpointScratch(storage, 2)
	require.NoError(t, err)
	require.Equal(t, required, exact)
	require.Len(t, checkpoints, 2)

	checkpoints, required, err = AppendCheckpointScratch(nil, 0)
	require.NoError(t, err)
	require.Nil(t, checkpoints)
	require.Zero(t, required)

	_, _, err = AppendCheckpointScratch(nil, -1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, _, err = AppendCheckpointScratch(nil, math.MaxInt)
	require.ErrorIs(t, err, mpool.ErrAllocationAllocatorLimit)

	unaligned := make([]byte, exact+1)[1:]
	_, _, err = AppendCheckpointScratch(unaligned, 2)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
}

func TestCapacityForUntypedNull(t *testing.T) {
	vec := NewVec(types.T_any.ToType(), mpool.MustNewZero())
	require.Equal(t, 0, vec.Capacity())
}

func TestSize(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType(), mp)
	require.Equal(t, 0, vec.Size())
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
	{
		//Array Float32
		mp := mpool.MustNewZero()
		vec := NewVec(types.New(types.T_array_float32, 4, 0), mp)
		require.Equal(t, 0, vec.Size())
		vec.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{
		//Array Float64
		mp := mpool.MustNewZero()
		vec := NewVec(types.New(types.T_array_float64, 4, 0), mp)
		require.Equal(t, 0, vec.Size())
		vec.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestUnionOne(t *testing.T) {
	{ // test const vector
		mp := mpool.MustNewZero()
		v := NewVec(types.T_int8.ToType(), mp)
		w := NewVec(types.T_int8.ToType(), mp)
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
		v := NewVec(types.T_varchar.ToType(), mp)
		w := NewVec(types.T_varchar.ToType(), mp)
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
		v := NewVec(types.New(types.T_array_float32, 4, 0), mp)
		w := NewVec(types.New(types.T_array_float32, 4, 0), mp)
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
		v := NewVec(types.New(types.T_array_float64, 4, 0), mp)
		w := NewVec(types.New(types.T_array_float64, 4, 0), mp)
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
		v := NewVec(types.New(types.T_bit, 10, 0), mp)
		w := NewVec(types.New(types.T_bit, 10, 0), mp)
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
		v := NewVec(types.T_bool.ToType(), mp)
		w := NewVec(types.T_bool.ToType(), mp)
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
		v := NewVec(types.T_int8.ToType(), mp)
		w := NewVec(types.T_int8.ToType(), mp)
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
		v := NewVec(types.T_int16.ToType(), mp)
		w := NewVec(types.T_int16.ToType(), mp)
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
		v := NewVec(types.T_int32.ToType(), mp)
		w := NewVec(types.T_int32.ToType(), mp)
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
		v := NewVec(types.T_int64.ToType(), mp)
		w := NewVec(types.T_int64.ToType(), mp)
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
		v := NewVec(types.T_uint8.ToType(), mp)
		w := NewVec(types.T_uint8.ToType(), mp)
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
		v := NewVec(types.T_text.ToType(), mp)
		w := NewVec(types.T_text.ToType(), mp)
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

func TestAppend(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType(), mp)
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
		vec := NewVec(types.New(types.T_array_float32, 4, 0), mp)
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
		vec := NewVec(types.New(types.T_array_float64, 4, 0), mp)
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
	vec := NewVec(types.T_varchar.ToType(), mp)
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

func TestAppendBytesNullUsesVectorPhysicalType(t *testing.T) {
	for _, typ := range []types.Type{
		types.T_bool.ToType(),
		types.T_decimal128.ToType(),
		types.T_varchar.ToType(),
	} {
		t.Run(typ.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec := NewVec(typ, mp)

			// AppendBytes is the generic null path used by expression
			// evaluation, including for fixed-width result vectors.
			for i := range 17 {
				require.NoError(t, AppendBytes(vec, nil, true, mp))
				require.True(t, vec.IsNull(uint64(i)))
			}
			require.Equal(t, 17, vec.Length())

			vec.Free(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAppendArray(t *testing.T) {
	{
		// Array Float32
		mp := mpool.MustNewZero()
		vec := NewVec(types.T_array_float32.ToType(), mp)
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
		vec := NewVec(types.T_array_float64.ToType(), mp)
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
	v := NewVec(types.T_int8.ToType(), mp)
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

func TestNewVecWithDataCopyOwnsBackingData(t *testing.T) {
	mp := mpool.MustNewZero()
	data := []byte("external-data")
	area := []byte("external-area")
	vec, err := NewVecWithDataCopy(types.T_text.ToType(), 1, data, area, mp)
	require.NoError(t, err)
	require.Equal(t, data, vec.GetData())
	require.Equal(t, area, vec.GetArea())

	data[0] = 'X'
	area[0] = 'Y'
	require.Equal(t, byte('e'), vec.GetData()[0])
	require.Equal(t, byte('e'), vec.GetArea()[0])
	require.NotPanics(t, func() { vec.Free(mp) })
	require.Nil(t, vec.GetData())
	require.Nil(t, vec.GetArea())
}

func TestShrink(t *testing.T) {
	mp := mpool.MustNewZero()
	{ // Array Float32
		v := NewVec(types.T_array_float32.ToType(), mp)
		err := AppendArrayList[float32](v, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		require.Equal(t, [][]float32{{2, 2, 2}, {3, 3, 3}}, MustArrayCol[float32](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // Array Float64
		v := NewVec(types.T_array_float64.ToType(), mp)
		err := AppendArrayList[float64](v, [][]float64{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		require.Equal(t, [][]float64{{2, 2, 2}, {3, 3, 3}}, MustArrayCol[float64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bool
		v := NewVec(types.T_bool.ToType(), mp)
		err := AppendFixedList(v, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[bool](v)
		require.Equal(t, []bool{false, true}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		v := NewVec(types.T_int8.ToType(), mp)
		err := AppendFixedList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[int8](v)
		require.Equal(t, []int8{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := NewVec(types.T_int16.ToType(), mp)
		err := AppendFixedList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{0, 3}, false, mp))
		vs := MustFixedColWithTypeCheck[int16](v)
		require.Equal(t, []int16{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := NewVec(types.T_int32.ToType(), mp)
		err := AppendFixedList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[int32](v)
		require.Equal(t, []int32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := NewVec(types.T_int64.ToType(), mp)
		err := AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[int64](v)
		require.Equal(t, []int64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := NewVec(types.T_uint8.ToType(), mp)
		err := AppendFixedList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[uint8](v)
		require.Equal(t, []uint8{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := NewVec(types.T_uint16.ToType(), mp)
		err := AppendFixedList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{0, 3}, false, mp))
		vs := MustFixedColWithTypeCheck[uint16](v)
		require.Equal(t, []uint16{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := NewVec(types.T_uint32.ToType(), mp)
		err := AppendFixedList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[uint32](v)
		require.Equal(t, []uint32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := NewVec(types.T_uint64.ToType(), mp)
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, []uint64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := NewVec(types.T_float32.ToType(), mp)
		err := AppendFixedList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[float32](v)
		require.Equal(t, []float32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := NewVec(types.T_float64.ToType(), mp)
		err := AppendFixedList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[float64](v)
		require.Equal(t, []float64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := NewVec(types.T_text.ToType(), mp)
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := InefficientMustStrCol(v)
		require.Equal(t, []string{"2", "3"}, vs)
		require.Equal(t, [][]byte{[]byte("2"), []byte("3")}, InefficientMustBytesCol(v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := NewVec(types.T_date.ToType(), mp)
		err := AppendFixedList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[types.Date](v)
		require.Equal(t, []types.Date{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // datetime
		v := NewVec(types.T_datetime.ToType(), mp)
		err := AppendFixedList(v, []types.Datetime{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[types.Datetime](v)
		require.Equal(t, []types.Datetime{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		v := NewVec(types.T_time.ToType(), mp)
		err := AppendFixedList(v, []types.Time{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[types.Time](v)
		require.Equal(t, []types.Time{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		v := NewVec(types.T_timestamp.ToType(), mp)
		err := AppendFixedList(v, []types.Timestamp{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		vs := MustFixedColWithTypeCheck[types.Timestamp](v)
		require.Equal(t, []types.Timestamp{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		vs := make([]types.Decimal64, 4)
		v := NewVec(types.T_decimal64.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Decimal64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		vs := make([]types.Decimal128, 4)
		v := NewVec(types.T_decimal128.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Decimal128](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		vs := make([]types.Uuid, 4)
		v := NewVec(types.T_uuid.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Uuid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := NewVec(types.T_TS.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.TS](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := NewVec(types.T_Rowid.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Rowid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // blockid
		vs := make([]types.Blockid, 4)
		v := NewVec(types.T_Blockid.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Blockid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bit
		v := NewVec(types.T_bit.ToType(), mp)
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
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
		v := NewVec(types.T_array_float64.ToType(), mp)
		err := AppendArrayList[float64](v, [][]float64{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		require.Equal(t, [][]float64{{2, 2, 2}, {3, 3, 3}}, MustArrayCol[float64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_array_float64.ToType(), mp)
		err = AppendArrayList[float64](v, [][]float64{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		require.Equal(t, [][]float64{{1, 1, 1}}, MustArrayCol[float64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bool
		v := NewVec(types.T_bool.ToType(), mp)
		err := AppendFixedList(v, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[bool](v)
		require.Equal(t, []bool{false, true}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_bool.ToType(), mp)
		err = AppendFixedList(v, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[bool](v)
		require.Equal(t, []bool{true, false}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		v := NewVec(types.T_int8.ToType(), mp)
		err := AppendFixedList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[int8](v)
		require.Equal(t, []int8{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_int8.ToType(), mp)
		err = AppendFixedList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[int8](v)
		require.Equal(t, []int8{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := NewVec(types.T_int16.ToType(), mp)
		err := AppendFixedList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[int16](v)
		require.Equal(t, []int16{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_int16.ToType(), mp)
		err = AppendFixedList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[int16](v)
		require.Equal(t, []int16{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := NewVec(types.T_int32.ToType(), mp)
		err := AppendFixedList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[int32](v)
		require.Equal(t, []int32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_int32.ToType(), mp)
		err = AppendFixedList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[int32](v)
		require.Equal(t, []int32{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := NewVec(types.T_int64.ToType(), mp)
		err := AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[int64](v)
		require.Equal(t, []int64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_int64.ToType(), mp)
		err = AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[int64](v)
		require.Equal(t, []int64{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := NewVec(types.T_uint8.ToType(), mp)
		err := AppendFixedList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[uint8](v)
		require.Equal(t, []uint8{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_uint8.ToType(), mp)
		err = AppendFixedList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[uint8](v)
		require.Equal(t, []uint8{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := NewVec(types.T_uint16.ToType(), mp)
		err := AppendFixedList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[uint16](v)
		require.Equal(t, []uint16{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_uint16.ToType(), mp)
		err = AppendFixedList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[uint16](v)
		require.Equal(t, []uint16{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := NewVec(types.T_uint32.ToType(), mp)
		err := AppendFixedList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[uint32](v)
		require.Equal(t, []uint32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_uint32.ToType(), mp)
		err = AppendFixedList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[uint32](v)
		require.Equal(t, []uint32{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := NewVec(types.T_uint64.ToType(), mp)
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, []uint64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_uint64.ToType(), mp)
		err = AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, []uint64{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := NewVec(types.T_float32.ToType(), mp)
		err := AppendFixedList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[float32](v)
		require.Equal(t, []float32{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_float32.ToType(), mp)
		err = AppendFixedList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[float32](v)
		require.Equal(t, []float32{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := NewVec(types.T_float64.ToType(), mp)
		err := AppendFixedList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[float64](v)
		require.Equal(t, []float64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_float64.ToType(), mp)
		err = AppendFixedList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[float64](v)
		require.Equal(t, []float64{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := NewVec(types.T_text.ToType(), mp)
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := InefficientMustStrCol(v)
		require.Equal(t, []string{"2", "3"}, vs)
		require.Equal(t, [][]byte{[]byte("2"), []byte("3")}, InefficientMustBytesCol(v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_text.ToType(), mp)
		err = AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = InefficientMustStrCol(v)
		require.Equal(t, []string{"1", "4"}, vs)
		require.Equal(t, [][]byte{[]byte("1"), []byte("4")}, InefficientMustBytesCol(v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := NewVec(types.T_date.ToType(), mp)
		err := AppendFixedList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[types.Date](v)
		require.Equal(t, []types.Date{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_date.ToType(), mp)
		err = AppendFixedList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[types.Date](v)
		require.Equal(t, []types.Date{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // datetime
		v := NewVec(types.T_datetime.ToType(), mp)
		err := AppendFixedList(v, []types.Datetime{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[types.Datetime](v)
		require.Equal(t, []types.Datetime{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_datetime.ToType(), mp)
		err = AppendFixedList(v, []types.Datetime{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[types.Datetime](v)
		require.Equal(t, []types.Datetime{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		v := NewVec(types.T_time.ToType(), mp)
		err := AppendFixedList(v, []types.Time{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[types.Time](v)
		require.Equal(t, []types.Time{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_time.ToType(), mp)
		err = AppendFixedList(v, []types.Time{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[types.Time](v)
		require.Equal(t, []types.Time{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		v := NewVec(types.T_timestamp.ToType(), mp)
		err := AppendFixedList(v, []types.Timestamp{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[types.Timestamp](v)
		require.Equal(t, []types.Timestamp{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_timestamp.ToType(), mp)
		err = AppendFixedList(v, []types.Timestamp{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[types.Timestamp](v)
		require.Equal(t, []types.Timestamp{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		vs := make([]types.Decimal64, 4)
		v := NewVec(types.T_decimal64.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Decimal64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		vs = make([]types.Decimal64, 4)
		v = NewVec(types.T_decimal64.ToType(), mp)
		err = AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		require.Equal(t, append(vs[:1], vs[3]), MustFixedColWithTypeCheck[types.Decimal64](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		vs := make([]types.Decimal128, 4)
		v := NewVec(types.T_decimal128.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Decimal128](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		vs = make([]types.Decimal128, 4)
		v = NewVec(types.T_decimal128.ToType(), mp)
		err = AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		require.Equal(t, append(vs[:1], vs[3]), MustFixedColWithTypeCheck[types.Decimal128](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		vs := make([]types.Uuid, 4)
		v := NewVec(types.T_uuid.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Uuid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		vs = make([]types.Uuid, 4)
		v = NewVec(types.T_uuid.ToType(), mp)
		err = AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		require.Equal(t, append(vs[:1], vs[3]), MustFixedColWithTypeCheck[types.Uuid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := NewVec(types.T_TS.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.TS](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		vs = make([]types.TS, 4)
		v = NewVec(types.T_TS.ToType(), mp)
		err = AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		require.Equal(t, append(vs[:1], vs[3]), MustFixedColWithTypeCheck[types.TS](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := NewVec(types.T_Rowid.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Rowid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		vs = make([]types.Rowid, 4)
		v = NewVec(types.T_Rowid.ToType(), mp)
		err = AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		require.Equal(t, append(vs[:1], vs[3]), MustFixedColWithTypeCheck[types.Rowid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // blockid
		vs := make([]types.Blockid, 4)
		v := NewVec(types.T_Blockid.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Blockid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		vs = make([]types.Blockid, 4)
		v = NewVec(types.T_Blockid.ToType(), mp)
		err = AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		require.Equal(t, append(vs[:1], vs[3]), MustFixedColWithTypeCheck[types.Blockid](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bit
		v := NewVec(types.T_bit.ToType(), mp)
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, false, 1, mp))
		vs := MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, []uint64{2, 3}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		v = NewVec(types.T_bit.ToType(), mp)
		err = AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.NoError(t, v.ShrinkByMask(&bmask, true, 1, mp))
		vs = MustFixedColWithTypeCheck[uint64](v)
		require.Equal(t, []uint64{1, 4}, vs)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestShrinkByMaskKeepsNullsAlignedWithOffset(t *testing.T) {
	mp := mpool.MustNewZero()
	var mask bitmap.Bitmap
	mask.InitWithSize(3)
	mask.AddMany([]uint64{0, 2})

	t.Run("negate", func(t *testing.T) {
		vec := NewVec(types.T_int32.ToType(), mp)
		defer vec.Free(mp)
		require.NoError(t, AppendFixedList(
			vec,
			[]int32{10, 11, 12, 13, 14, 15, 16, 17},
			[]bool{false, false, false, true, false, true, false, false},
			mp,
		))

		require.NoError(t, vec.ShrinkByMask(&mask, true, 2, mp))
		require.Equal(t, []int32{10, 11, 0, 0, 16, 17}, MustFixedColWithTypeCheck[int32](vec))
		require.Equal(t, []uint64{2, 3}, vec.GetNulls().ToArray())
	})

	t.Run("select", func(t *testing.T) {
		vec := NewVec(types.T_int32.ToType(), mp)
		defer vec.Free(mp)
		require.NoError(t, AppendFixedList(
			vec,
			[]int32{10, 11, 12, 13, 14, 15, 16, 17},
			[]bool{false, false, true, false, false, false, false, false},
			mp,
		))

		require.NoError(t, vec.ShrinkByMask(&mask, false, 2, mp))
		require.Equal(t, []int32{0, 14}, MustFixedColWithTypeCheck[int32](vec))
		require.Equal(t, []uint64{0}, vec.GetNulls().ToArray())
	})
}

func TestShuffle(t *testing.T) {
	mp := mpool.MustNewZero()

	{ // Array Float32
		v := NewVec(types.T_array_float32.ToType(), mp)
		err := AppendArrayList[float32](v, [][]float32{{1, 1}, {2, 2}, {3, 3}}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, [][]float32{{2, 2}, {3, 3}}, MustArrayCol[float32](v))
		require.Equal(t, "[2, 2] [3, 3]-[]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // Array Float64
		v := NewVec(types.T_array_float64.ToType(), mp)
		err := AppendArrayList[float64](v, [][]float64{{1, 1}, {2, 2}, {3, 3}}, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, [][]float64{{2, 2}, {3, 3}}, MustArrayCol[float64](v))
		require.Equal(t, "[2, 2] [3, 3]-[]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bool
		v := NewVec(types.T_bool.ToType(), mp)
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
		v := NewVec(types.T_int8.ToType(), mp)
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
		v := NewVec(types.T_int16.ToType(), mp)
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
		v := NewVec(types.T_int32.ToType(), mp)
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
		v := NewVec(types.T_int64.ToType(), mp)
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
		v := NewVec(types.T_uint8.ToType(), mp)
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
		v := NewVec(types.T_uint16.ToType(), mp)
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
		v := NewVec(types.T_uint32.ToType(), mp)
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
		v := NewVec(types.T_uint64.ToType(), mp)
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
		v := NewVec(types.T_float32.ToType(), mp)
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
		v := NewVec(types.T_float64.ToType(), mp)
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
		v := NewVec(types.T_text.ToType(), mp)
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
		v := NewVec(types.T_date.ToType(), mp)
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
		v := NewVec(types.T_datetime.ToType(), mp)
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
		v := NewVec(types.T_time.ToType(), mp)
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
		v := NewVec(types.T_timestamp.ToType(), mp)
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
		v := NewVec(types.T_decimal64.ToType(), mp)
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
		v := NewVec(types.T_decimal128.ToType(), mp)
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
		v := NewVec(types.T_uuid.ToType(), mp)
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
		v := NewVec(types.T_TS.ToType(), mp)
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
		v := NewVec(types.T_Rowid.ToType(), mp)
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
		v := NewVec(types.T_Blockid.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		v.Shuffle([]int64{1, 2}, mp)
		require.Equal(t, vs[1:3], MustFixedColWithTypeCheck[types.Blockid](v))
		require.Equal(t, "[[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0] [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]]", v.String())
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bit
		v := NewVec(types.T_bit.ToType(), mp)
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
	{ // fixed null provenance
		dst := NewVec(types.T_int32.ToType(), mp)
		src := NewVec(types.T_int32.ToType(), mp)
		require.NoError(t, AppendFixedList(dst, []int32{0, 0}, []bool{false, true}, mp))
		require.NoError(t, AppendFixedList(src, []int32{1, 2}, []bool{true, false}, mp))
		require.NoError(t, dst.Copy(src, 0, 0, mp))
		require.NoError(t, dst.Copy(src, 1, 1, mp))
		require.True(t, dst.GetNulls().Contains(0))
		require.False(t, dst.GetNulls().Contains(1))
		require.Equal(t, int32(2), GetFixedAtNoTypeCheck[int32](dst, 1))
		dst.Free(mp)
		src.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // fixed
		v := NewVec(types.T_int8.ToType(), mp)
		AppendFixedList(v, []int8{0, 0, 1, 0}, nil, mp)
		w := NewVec(types.T_int8.ToType(), mp)
		AppendFixedList(w, []int8{0, 0, 0, 0}, nil, mp)
		err := v.Copy(w, 2, 0, mp)
		require.NoError(t, err)
		require.Equal(t, MustFixedColWithTypeCheck[int8](v), MustFixedColWithTypeCheck[int8](w))
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bit
		v := NewVec(types.T_bit.ToType(), mp)
		err := AppendFixedList(v, []uint64{0, 0, 1, 0}, nil, mp)
		require.NoError(t, err)
		w := NewVec(types.T_bit.ToType(), mp)
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
		v := NewVec(types.New(types.T_array_float32, 10, 0), mp)
		AppendArrayList[float32](v, [][]float32{{0, 0}, {0, 0}, {1, 1}, {0, 0}}, nil, mp)
		w := NewVec(types.New(types.T_array_float32, 10, 0), mp)
		AppendArrayList[float32](w, [][]float32{{0, 0}, {0, 0}, {0, 0}, {0, 0}}, nil, mp)
		err := v.Copy(w, 2, 0, mp)
		require.NoError(t, err)
		require.Equal(t, MustArrayCol[float32](v), MustArrayCol[float32](w))
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // Array Float64
		v := NewVec(types.New(types.T_array_float64, 10, 0), mp)
		AppendArrayList[float64](v, [][]float64{{0, 0}, {0, 0}, {1, 1}, {0, 0}}, nil, mp)
		w := NewVec(types.New(types.T_array_float64, 10, 0), mp)
		AppendArrayList[float64](w, [][]float64{{0, 0}, {0, 0}, {0, 0}, {0, 0}}, nil, mp)
		err := v.Copy(w, 2, 0, mp)
		require.NoError(t, err)
		require.Equal(t, MustArrayCol[float32](v), MustArrayCol[float32](w))
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // string
		v := NewVec(types.New(types.T_char, 10, 0), mp)
		AppendBytesList(v, [][]byte{
			[]byte("hello"),
			[]byte("hello"),
			[]byte("nihao"),
			[]byte("hello"),
		}, nil, mp)
		w := NewVec(types.New(types.T_char, 10, 0), mp)
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
	{ // string null with stale varlena metadata
		v := NewVec(types.T_varchar.ToType(), mp)
		err := AppendBytes(v, []byte("seed"), false, mp)
		require.NoError(t, err)
		w := NewVec(types.T_varchar.ToType(), mp)
		err = AppendBytes(w, nil, true, mp)
		require.NoError(t, err)
		ws := MustFixedColNoTypeCheck[types.Varlena](w)
		ws[0].SetOffsetLen(25, 8)
		err = v.Copy(w, 0, 0, mp)
		require.NoError(t, err)
		require.True(t, v.GetNulls().Contains(0))
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // array null with stale varlena metadata
		v := NewVec(types.T_array_float64.ToType(), mp)
		err := AppendArray[float64](v, []float64{1, 2}, false, mp)
		require.NoError(t, err)
		w := NewVec(types.T_array_float64.ToType(), mp)
		err = AppendArray[float64](w, nil, true, mp)
		require.NoError(t, err)
		ws := MustFixedColNoTypeCheck[types.Varlena](w)
		ws[0].SetOffsetLen(25, 16)
		err = v.Copy(w, 0, 0, mp)
		require.NoError(t, err)
		require.True(t, v.GetNulls().Contains(0))
		v.Free(mp)
		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestCopyPreallocatedNullVectorKeepsScalarMetadata(t *testing.T) {
	const rows = 1024
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	source := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixed(source, int64(42), false, mp))
	defer source.Free(mp)

	destination := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, destination.PreExtend(rows, mp))
	destination.SetLength(rows)
	destination.SetAllNulls(rows)
	defer destination.Free(mp)

	for row := rows - 1; row >= 0; row-- {
		require.NoError(t, destination.Copy(source, int64(row), 0, mp))
	}
	require.False(t, destination.HasNull())
	require.Equal(t, int64(42), MustFixedColNoTypeCheck[int64](destination)[0])
	require.Equal(t, int64(42), MustFixedColNoTypeCheck[int64](destination)[rows-1])
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

	payload := []byte(strings.Repeat("x", 128))
	v5, err := NewConstBytes(types.T_varchar.ToType(), payload, 10, mp)
	require.NoError(t, err)
	defer v5.Free(mp)
	v6 := NewVec(types.T_varchar.ToType(), mp)
	defer v6.Free(mp)
	require.NoError(t, v5.CloneWindowTo(v6, 3, 5, mp))
	require.True(t, v6.IsConst())
	require.Equal(t, 2, v6.Length())
	require.Equal(t, payload, v6.GetBytesAt(0))
	require.Equal(t, payload, v6.GetBytesAt(1))
	require.Equal(t, 10, v5.Length(), "cloning must not mutate the source")
	require.Equal(t, payload, v5.GetBytesAt(0))
}

func TestRollbackAppendIgnoresStaleNullExtent(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	vec := NewVec(types.T_text.ToType(), mp)
	for row := 0; row < 130; row++ {
		require.NoError(t, AppendBytes(vec, []byte("v"), row == 129, mp))
	}
	defer vec.Free(mp)
	vec.SetLength(2)
	require.True(t, vec.GetNulls().Contains(129), "SetLength preserves stale bitmap extent")
	checkpoint := vec.MakeAppendCheckpoint()

	source := NewVec(types.T_text.ToType(), mp)
	require.NoError(t, AppendBytes(source, []byte("appended"), false, mp))
	defer source.Free(mp)
	require.NoError(t, vec.UnionBatch(source, 0, 1, nil, mp))
	require.Equal(t, 3, vec.Length())

	require.NotPanics(t, func() { vec.RollbackAppend(checkpoint, 1) })
	require.Equal(t, 2, vec.Length())
	require.Equal(t, "v", vec.GetStringAt(0))
	require.Equal(t, "v", vec.GetStringAt(1))
}

func TestOwnedVarlenaMarshalKeepsBulkLayout(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_varchar.ToType(), mp)
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	values := [][]byte{
		[]byte("ordinary inline"),
		[]byte("ordinary long value that exceeds inline storage"),
		[]byte("second ordinary long value that exceeds inline storage"),
	}
	require.NoError(t, AppendBytesList(vec, values, nil, mp))

	plan, err := vec.PrepareMarshalBinary()
	require.NoError(t, err)
	require.False(t, plan.canonicalVarlen,
		"an ordinary owned append-built vector should retain bulk serialization")
	require.Equal(t, uint32(vec.Length()*vec.GetType().TypeSize()+len(vec.GetArea())),
		plan.dataLength+plan.areaLength)

	encoded, err := vec.MarshalBinary()
	require.NoError(t, err)
	decoded := new(Vector)
	defer decoded.Free(nil)
	require.NoError(t, decoded.UnmarshalBinary(encoded))
	require.Equal(t, values[0], decoded.GetBytesAt(0))
	require.Equal(t, values[1], decoded.GetBytesAt(1))
	require.Equal(t, values[2], decoded.GetBytesAt(2))

	nullable := NewVec(types.T_varchar.ToType(), mp)
	defer nullable.Free(mp)
	require.NoError(t, AppendBytes(nullable, values[0], false, mp))
	require.NoError(t, AppendBytes(nullable, values[1], true, mp))
	nullablePlan, err := nullable.PrepareMarshalBinary()
	require.NoError(t, err)
	require.False(t, nullablePlan.canonicalVarlen,
		"an ordinary owned vector with a safe NULL descriptor should retain bulk serialization")
	require.Nil(t, nullablePlan.normalizedVarlenData)
	nullableEncoded, err := nullable.MarshalBinary()
	require.NoError(t, err)
	nullableDecoded := new(Vector)
	defer nullableDecoded.Free(nil)
	require.NoError(t, nullableDecoded.UnmarshalBinary(nullableEncoded))
	require.True(t, nullableDecoded.IsNull(1))

	stale := NewVec(types.T_varchar.ToType(), mp)
	defer stale.Free(mp)
	require.NoError(t, AppendBytes(stale, values[1], false, mp))
	stale.SetNull(0)
	stalePlan, err := stale.PrepareMarshalBinary()
	require.NoError(t, err)
	require.False(t, stalePlan.canonicalVarlen,
		"an owned stale NULL descriptor should use bulk serialization with normalization")
	require.NotNil(t, stalePlan.normalizedVarlenData)

	staleEncoded, err := stale.MarshalBinary()
	require.NoError(t, err)
	staleDecoded := new(Vector)
	defer staleDecoded.Free(nil)
	require.NoError(t, staleDecoded.UnmarshalBinary(staleEncoded))
	require.True(t, staleDecoded.IsNull(0))

	// A window is an owned copy holding only its rows' payload.
	window, err := vec.Window(2, 3)
	require.NoError(t, err)
	defer window.Free(window.MPool())
	windowEncoded, err := window.MarshalBinary()
	require.NoError(t, err)
	windowDecoded := new(Vector)
	defer windowDecoded.Free(nil)
	require.NoError(t, windowDecoded.UnmarshalBinary(windowEncoded))
	require.Equal(t, values[2], windowDecoded.GetBytesAt(0))
}

func TestNullableFixedWidthMarshalDoesNotCastAsVarlena(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int64.ToType(), mp)
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, AppendFixedList(vec, []int64{11, 22, 33}, []bool{false, true, false}, mp))

	encoded, err := vec.MarshalBinary()
	require.NoError(t, err)
	size, err := vec.MarshalBinarySize()
	require.NoError(t, err)
	require.Equal(t, len(encoded), size)
	decoded := new(Vector)
	defer decoded.Free(nil)
	require.NoError(t, decoded.UnmarshalBinary(encoded))
	require.Equal(t, []int64{11, 0, 33}, MustFixedColNoTypeCheck[int64](decoded))
	require.True(t, decoded.IsNull(1))
}

func TestMarshalAndUnMarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.T_int8.ToType(), mp)
	err := AppendFixedList(v, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	size, err := v.MarshalBinarySize()
	require.NoError(t, err)
	require.Equal(t, len(data), size)
	var streamed bytes.Buffer
	require.NoError(t, v.MarshalBinaryTo(&streamed))
	require.Equal(t, data, streamed.Bytes())
	require.ErrorIs(t, v.MarshalBinaryTo(shortVectorMarshalWriter{}), io.ErrShortWrite)
	w := new(Vector)
	err = w.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, MustFixedColWithTypeCheck[int8](v), MustFixedColWithTypeCheck[int8](w))
	w = NewVec(types.T_any.ToType(), mp)
	err = w.UnmarshalBinaryWithCopy(data, mp)
	require.NoError(t, err)
	require.Equal(t, MustFixedColWithTypeCheck[int8](v), MustFixedColWithTypeCheck[int8](w))
	require.NoError(t, err)
	v.Free(mp)
	w.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

type shortVectorMarshalWriter struct{}

func (shortVectorMarshalWriter) Write(value []byte) (int, error) {
	return len(value) - 1, nil
}

func TestMarshalBinarySizeRejectsInvalidVector(t *testing.T) {
	var nilVector *Vector
	_, err := nilVector.MarshalBinarySize()
	require.Error(t, err)

	typ := types.T_int64.ToType()
	typ.Size = -1
	invalidType := NewVec(typ, mpool.MustNewZero())
	_, err = invalidType.MarshalBinarySize()
	require.Error(t, err)

	shortData := NewVec(types.T_int64.ToType(), invalidType.mp)
	shortData.SetLength(1)
	_, err = shortData.MarshalBinarySize()
	require.Error(t, err)
}

func TestUnmarshalBinaryAcceptsNullBitmapCoveragePastLength(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixed(source, int64(0), false, mp))
	source.GetNulls().AddRange(0, 1)

	data, err := source.MarshalBinary()
	require.NoError(t, err)

	target := new(Vector)
	require.NoError(t, target.UnmarshalBinary(data))
	require.Equal(t, 1, target.Length())
	require.True(t, target.IsNull(0))

	source.Free(mp)
	target.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestUnmarshalBinaryAcceptsStaleVarlenaInNullRow(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytes(source, []byte("value longer than inline storage"), false, mp))
	source.SetNull(0)
	source.ResetArea()

	data, err := source.MarshalBinary()
	require.NoError(t, err)

	target := new(Vector)
	require.NoError(t, target.UnmarshalBinary(data))
	require.Equal(t, 1, target.Length())
	require.True(t, target.IsNull(0))

	source.Free(mp)
	target.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestUnmarshalBinaryRejectsOverflowingNullBitmapLength(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixed(source, int64(0), true, mp))
	data, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(mp)

	nspLenOffset := 1 + types.TSize + 4 + 4 + types.T_int64.TypeLen() + 4
	nspDataOffset := nspLenOffset + 4
	corrupted := append([]byte(nil), data[:nspDataOffset+24]...)
	corrupted = append(corrupted, data[len(data)-1])
	nspLen := uint32(24)
	count := int64(0)
	bitmapLen := ^uint64(0)
	bitmapDataLen := uint64(0)
	copy(corrupted[nspLenOffset:nspDataOffset], types.EncodeUint32(&nspLen))
	copy(corrupted[nspDataOffset:nspDataOffset+8], types.EncodeInt64(&count))
	copy(corrupted[nspDataOffset+8:nspDataOffset+16], types.EncodeUint64(&bitmapLen))
	copy(corrupted[nspDataOffset+16:nspDataOffset+24], types.EncodeUint64(&bitmapDataLen))

	target := new(Vector)
	require.Error(t, target.UnmarshalBinary(corrupted))
}

func TestUnmarshalBinaryRejectsMisalignedArrayPayload(t *testing.T) {
	for _, test := range []struct {
		name       string
		values     []float32
		corruptLen func([]byte, int)
	}{
		{
			name:   "out_of_line",
			values: make([]float32, 10),
			corruptLen: func(data []byte, varlenOffset int) {
				misalignedLength := uint32(3)
				copy(data[varlenOffset+8:varlenOffset+12], types.EncodeUint32(&misalignedLength))
			},
		},
		{
			name:   "inline",
			values: []float32{0},
			corruptLen: func(data []byte, varlenOffset int) {
				data[varlenOffset] = 3
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := NewVec(types.New(types.T_array_float32, 10, 0), mp)
			require.NoError(t, AppendArray(source, test.values, false, mp))
			data, err := source.MarshalBinary()
			require.NoError(t, err)
			source.Free(mp)

			// The array payload remains in bounds, but cannot be decoded as a
			// []float32. Cover both Varlena storage forms.
			corrupted := append([]byte(nil), data...)
			varlenOffset := 1 + types.TSize + 4 + 4
			test.corruptLen(corrupted, varlenOffset)

			target := new(Vector)
			var unmarshalErr error
			require.NotPanics(t, func() {
				unmarshalErr = target.UnmarshalBinary(corrupted)
				if unmarshalErr == nil {
					_ = GetArrayAt[float32](target, 0)
				}
			})
			require.Error(t, unmarshalErr)
		})
	}
}

func TestUnmarshalBinaryRejectsUnsupportedZeroSizeType(t *testing.T) {
	for _, oid := range []types.T{types.T_interval, types.T_tuple} {
		t.Run(oid.String(), func(t *testing.T) {
			source := NewVec(types.Type{Oid: oid}, mpool.MustNewZero())
			data, err := source.MarshalBinary()
			require.NoError(t, err)

			target := new(Vector)
			var unmarshalErr error
			require.NotPanics(t, func() {
				unmarshalErr = target.UnmarshalBinary(data)
			})
			require.Error(t, unmarshalErr)
		})
	}
}

func TestUnmarshalBinaryTrustedKeepsStructuralChecks(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytes(source, []byte("value longer than inline storage"), false, mp))
	data, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(mp)

	for end := len(data) - 1; end >= 0; end-- {
		target := new(Vector)
		var unmarshalErr error
		require.NotPanics(t, func() {
			unmarshalErr = target.UnmarshalBinaryTrusted(data[:end])
		}, "truncation at %d bytes", end)
		require.Error(t, unmarshalErr, "truncation at %d bytes", end)
	}
}

func TestUnmarshalBinaryTrustedRequiresPriorSemanticValidation(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytes(source, []byte("value longer than inline storage"), false, mp))
	data, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(mp)

	// Preserve the complete frame while forging an out-of-range Varlena
	// offset. The checked boundary rejects it; the trusted bind intentionally
	// relies on a previous checked decode and immutable bytes.
	corrupted := append([]byte(nil), data...)
	varlenOffset := 1 + types.TSize + 4 + 4
	invalidOffset := uint32(len(data) + 1)
	copy(corrupted[varlenOffset+4:varlenOffset+8], types.EncodeUint32(&invalidOffset))

	checked := new(Vector)
	require.Error(t, checked.UnmarshalBinary(corrupted))

	trusted := new(Vector)
	require.NoError(t, trusted.UnmarshalBinaryTrusted(corrupted))
}

func TestStrMarshalAndUnMarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.T_text.ToType(), mp)
	err := AppendBytesList(v, [][]byte{[]byte("x"), []byte("y")}, nil, mp)
	require.NoError(t, err)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	w := new(Vector)
	err = w.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, InefficientMustStrCol(v), InefficientMustStrCol(w))
	w = NewVec(types.T_any.ToType(), mp)
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
		v := NewVec(types.New(types.T_array_float32, 2, 0), mp)
		err := AppendArrayList(v, [][]float32{{0, 0}, {1, 1}, {2, 2}}, nil, mp)
		require.NoError(t, err)
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		w := new(Vector)
		err = w.UnmarshalBinary(data)
		require.NoError(t, err)
		require.Equal(t, MustArrayCol[float32](v), MustArrayCol[float32](w))
		w = NewVec(types.T_any.ToType(), mp)
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
		v := NewVec(types.New(types.T_array_float64, 2, 0), mp)
		err := AppendArrayList(v, [][]float64{{0, 0}, {1, 1}, {2, 2}}, nil, mp)
		require.NoError(t, err)
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		w := new(Vector)
		err = w.UnmarshalBinary(data)
		require.NoError(t, err)
		require.Equal(t, MustArrayCol[float64](v), MustArrayCol[float64](w))
		w = NewVec(types.T_any.ToType(), mp)
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
	vec1 := NewVec(types.T_int32.ToType(), mp)
	AppendFixed(vec1, int32(1), false, mp)
	AppendFixed(vec1, int32(2), true, mp)
	AppendFixed(vec1, int32(3), false, mp)
	require.False(t, vec1.IsReadOnly())

	vec2, err := vec1.Window(0, vec1.Length())
	require.NoError(t, err)

	t.Log(vec2.String())
	require.True(t, vec2.IsReadOnly(), "a window is a read-only reference")
	require.Equal(t, int32(1), GetFixedAtWithTypeCheck[int32](vec2, 0))
	require.True(t, vec2.GetNulls().Contains(uint64(1)))
	require.Equal(t, int32(3), GetFixedAtWithTypeCheck[int32](vec2, 2))
	vec2.Free(mp)

	vec6, err := vec1.Window(1, vec1.Length())
	require.NoError(t, err)

	t.Log(vec6.String())
	require.True(t, vec6.IsReadOnly(), "a window is a read-only reference")
	require.True(t, vec6.GetNulls().Contains(uint64(0)))
	require.Equal(t, int32(3), GetFixedAtWithTypeCheck[int32](vec6, 1))
	vec6.Free(mp)

	require.False(t, vec1.IsReadOnly())
	require.Equal(t, int32(1), GetFixedAtWithTypeCheck[int32](vec1, 0))
	require.True(t, vec1.GetNulls().Contains(uint64(1)))
	require.Equal(t, int32(3), GetFixedAtWithTypeCheck[int32](vec1, 2))
	vec1.Free(mp)

	vec3 := NewVec(types.T_char.ToType(), mp)
	AppendBytes(vec3, []byte("h"), false, mp)
	AppendBytes(vec3, []byte("xx"), true, mp)
	AppendBytes(vec3, []byte("uuu"), false, mp)
	require.False(t, vec3.IsReadOnly())

	vec4, err := vec3.Window(0, vec3.Length())
	require.NoError(t, err)

	require.True(t, vec4.IsReadOnly(), "a window is a read-only reference")
	require.Equal(t, "h", string(vec4.GetBytesAt(0)))
	require.Equal(t, "uuu", string(vec4.GetBytesAt(2)))
	require.True(t, vec4.GetNulls().Contains(uint64(1)))
	vec4.Free(mp)

	vec5, err := vec3.Window(1, vec3.Length())
	require.NoError(t, err)

	require.True(t, vec5.IsReadOnly(), "a window is a read-only reference")
	require.Equal(t, "uuu", string(vec5.GetBytesAt(1)))
	require.True(t, vec5.GetNulls().Contains(uint64(0)))
	vec5.Free(mp)

	require.False(t, vec3.IsReadOnly())
	require.Equal(t, "h", string(vec3.GetBytesAt(0)))
	require.Equal(t, "uuu", string(vec3.GetBytesAt(2)))
	require.True(t, vec3.GetNulls().Contains(uint64(1)))
	vec3.Free(mp)

	{
		//Array Float32

		vec7 := NewVec(types.T_array_float32.ToType(), mp)
		AppendArray(vec7, []float32{1, 1, 1}, false, mp)
		AppendArray(vec7, []float32{2, 2, 2}, true, mp)
		AppendArray(vec7, []float32{3, 3, 3}, false, mp)
		require.False(t, vec7.IsReadOnly())

		vec8, err := vec7.Window(0, vec7.Length())
		require.NoError(t, err)

		require.True(t, vec8.IsReadOnly(), "a window is a read-only reference")
		require.Equal(t, []float32{1, 1, 1}, GetArrayAt[float32](vec8, 0))
		require.Equal(t, []float32{3, 3, 3}, GetArrayAt[float32](vec8, 2))
		require.True(t, vec8.GetNulls().Contains(uint64(1)))
		vec8.Free(mp)

		vec9, err := vec7.Window(1, vec7.Length())
		require.NoError(t, err)

		require.True(t, vec9.IsReadOnly(), "a window is a read-only reference")
		require.Equal(t, []float32{3, 3, 3}, GetArrayAt[float32](vec9, 1))
		require.True(t, vec9.GetNulls().Contains(uint64(0)))
		vec9.Free(mp)

		require.False(t, vec7.IsReadOnly())
		require.Equal(t, []float32{1, 1, 1}, GetArrayAt[float32](vec7, 0))
		require.Equal(t, []float32{3, 3, 3}, GetArrayAt[float32](vec7, 2))
		require.True(t, vec7.GetNulls().Contains(uint64(1)))
		vec7.Free(mp)
	}

	{
		//Array Float64

		vec7 := NewVec(types.T_array_float64.ToType(), mp)
		AppendArray(vec7, []float64{1, 1, 1}, false, mp)
		AppendArray(vec7, []float64{2, 2, 2}, true, mp)
		AppendArray(vec7, []float64{3, 3, 3}, false, mp)
		require.False(t, vec7.IsReadOnly())

		vec8, err := vec7.Window(0, vec7.Length())
		require.NoError(t, err)

		require.True(t, vec8.IsReadOnly(), "a window is a read-only reference")
		require.Equal(t, []float64{1, 1, 1}, GetArrayAt[float64](vec8, 0))
		require.Equal(t, []float64{3, 3, 3}, GetArrayAt[float64](vec8, 2))
		require.True(t, vec8.GetNulls().Contains(uint64(1)))
		vec8.Free(mp)

		vec9, err := vec7.Window(1, vec7.Length())
		require.NoError(t, err)

		require.True(t, vec9.IsReadOnly(), "a window is a read-only reference")
		require.Equal(t, []float64{3, 3, 3}, GetArrayAt[float64](vec9, 1))
		require.True(t, vec9.GetNulls().Contains(uint64(0)))
		vec9.Free(mp)

		require.False(t, vec7.IsReadOnly())
		require.Equal(t, []float64{1, 1, 1}, GetArrayAt[float64](vec7, 0))
		require.Equal(t, []float64{3, 3, 3}, GetArrayAt[float64](vec7, 2))
		require.True(t, vec7.GetNulls().Contains(uint64(1)))
		vec7.Free(mp)
	}
}

func TestSetFunction(t *testing.T) {
	mp := mpool.MustNewZero()
	{ // bool
		v := NewVec(types.T_bool.ToType(), mp)
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
		v := NewVec(types.T_bit.ToType(), mp)
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
		v := NewVec(types.T_int8.ToType(), mp)
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
		v := NewVec(types.T_int16.ToType(), mp)
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
		v := NewVec(types.T_int32.ToType(), mp)
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
		v := NewVec(types.T_int64.ToType(), mp)
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
		v := NewVec(types.T_uint8.ToType(), mp)
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
		v := NewVec(types.T_uint16.ToType(), mp)
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
		v := NewVec(types.T_uint32.ToType(), mp)
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
		v := NewVec(types.T_uint64.ToType(), mp)
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
		v := NewVec(types.T_float32.ToType(), mp)
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
		v := NewVec(types.T_float64.ToType(), mp)
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
		v := NewVec(types.T_text.ToType(), mp)
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
		v := NewVec(types.T_array_float32.ToType(), mp)
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
		v := NewVec(types.T_array_float64.ToType(), mp)
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

func TestSetAnyConstNull(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_any.ToType()
	set := GetConstSetFunction(typ, mp)
	constant := NewConstNull(typ, 2, mp)
	flat := NewVec(typ, mp)
	flat.SetLength(2)
	flat.SetNull(1)
	dst := NewVec(typ, mp)

	for _, src := range []*Vector{constant, flat} {
		require.NoError(t, set(dst, src, 1, 3))
		require.True(t, dst.IsConstNull())
		require.Equal(t, 3, dst.Length())
	}
	require.ErrorContains(t, set(dst, flat, 0, 1), "non-NULL value of type ANY")
	constant.Free(mp)
	flat.Free(mp)
	dst.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestSortAndCompact(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.New(types.T_array_float32, 4, 0), mp)
	err := AppendArrayList(v, [][]float32{{1, 2, 3, 0}, {1, 2, 3, 0}}, nil, mp)
	require.NoError(t, err)
	require.NoError(t, v.InplaceSortAndCompact(mp))
	require.Equal(t, v.length, 1)

	v = NewVec(types.New(types.T_array_float64, 4, 0), mp)
	err = AppendArrayList(v, [][]float64{{1.1, 2, 3, 0}, {1.1, 2, 3, 0}}, nil, mp)
	require.NoError(t, err)
	require.NoError(t, v.InplaceSortAndCompact(mp))
	require.Equal(t, v.length, 1)

	v = NewVec(types.T_geometry.ToType(), mp)
	err = AppendBytesList(v, [][]byte{[]byte("POINT(2 2)"), []byte("POINT(1 1)"), []byte("POINT(1 1)")}, nil, mp)
	require.NoError(t, err)
	require.NoError(t, v.InplaceSortAndCompact(mp))
	require.Equal(t, 2, v.length)
	require.Equal(t, [][]byte{[]byte("POINT(1 1)"), []byte("POINT(2 2)")}, InefficientMustBytesCol(v))
	v.Free(mp)
}

func TestGeometryVarlenPlumbing(t *testing.T) {
	mp := mpool.MustNewZero()

	src := NewVec(types.T_geometry.ToType(), mp)
	err := AppendBytesList(src, [][]byte{
		[]byte("POINT(1 1)"),
		[]byte("POINT(2 2)"),
		[]byte("POINT(3 3)"),
		[]byte("POINT(4 4)"),
	}, nil, mp)
	require.NoError(t, err)

	dst := NewVec(types.T_geometry.ToType(), mp)
	err = dst.UnionOne(src, 1, mp)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("POINT(2 2)")}, InefficientMustBytesCol(dst))

	require.NoError(t, dst.Shrink([]int64{0}, false, mp))
	require.Equal(t, [][]byte{[]byte("POINT(2 2)")}, InefficientMustBytesCol(dst))

	err = dst.UnionOne(src, 2, mp)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("POINT(2 2)"), []byte("POINT(3 3)")}, InefficientMustBytesCol(dst))

	err = dst.Shuffle([]int64{1, 0}, mp)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("POINT(3 3)"), []byte("POINT(2 2)")}, InefficientMustBytesCol(dst))
	require.Equal(t, "[POINT(3 3) POINT(2 2)]", dst.String())
	require.Equal(t, "POINT(2 2)", dst.RowToString(1))
	require.Equal(t, []byte("POINT(3 3)"), GetAny(dst, 0, false).([]byte))

	var shuffleBuf []byte
	err = dst.ShuffleWithBuf([]int64{1, 0}, mp, &shuffleBuf)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("POINT(2 2)"), []byte("POINT(3 3)")}, InefficientMustBytesCol(dst))

	sf := GetConstSetFunction(types.T_geometry.ToType(), mp)
	constVec := NewConstNull(types.T_geometry.ToType(), 0, mp)
	err = sf(constVec, src, 2, 1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("POINT(3 3)")}, InefficientMustBytesCol(constVec))

	unionAll := GetUnionAllFunction(types.T_geometry.ToType(), mp)
	unionDst := NewVec(types.T_geometry.ToType(), mp)
	err = unionAll(unionDst, src)
	require.NoError(t, err)
	require.Equal(t, [][]byte{
		[]byte("POINT(1 1)"),
		[]byte("POINT(2 2)"),
		[]byte("POINT(3 3)"),
		[]byte("POINT(4 4)"),
	}, InefficientMustBytesCol(unionDst))

	minmax := NewVec(types.T_geometry.ToType(), mp)
	err = AppendAny(minmax, []byte("POINT(2 2)"), false, mp)
	require.NoError(t, err)
	err = AppendAny(minmax, []byte("POINT(1 1)"), false, mp)
	require.NoError(t, err)
	err = AppendAny(minmax, []byte("POINT(3 3)"), false, mp)
	require.NoError(t, err)
	ok, minv, maxv := minmax.GetMinMaxValue()
	require.True(t, ok)
	require.Equal(t, []byte("POINT(1 1)"), minv)
	require.Equal(t, []byte("POINT(3 3)"), maxv)
	require.NoError(t, minmax.InplaceSort(mp))
	require.Equal(t, [][]byte{
		[]byte("POINT(1 1)"),
		[]byte("POINT(2 2)"),
		[]byte("POINT(3 3)"),
	}, InefficientMustBytesCol(minmax))

	var bm bitmap.Bitmap
	bm.InitWithSize(2)
	bm.AddMany([]uint64{0, 1})
	maskVec := NewVec(types.T_geometry.ToType(), mp)
	err = AppendBytesList(maskVec, [][]byte{
		[]byte("POINT(1 1)"),
		[]byte("POINT(2 2)"),
		[]byte("POINT(3 3)"),
		[]byte("POINT(4 4)"),
	}, nil, mp)
	require.NoError(t, err)
	require.NoError(t, maskVec.ShrinkByMask(&bm, false, 1, mp))
	require.Equal(t, [][]byte{[]byte("POINT(2 2)"), []byte("POINT(3 3)")}, InefficientMustBytesCol(maskVec))

	maskVec.Free(mp)
	minmax.Free(mp)
	unionDst.Free(mp)
	constVec.Free(mp)
	dst.Free(mp)
	src.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestSetFunction2(t *testing.T) {
	// set vec to const value -> const null -> const value -> const null.
	// bool type
	{
		mp := mpool.MustNewZero()

		w := NewConstNull(types.T_bool.ToType(), 0, mp)
		v := NewVec(types.T_bool.ToType(), mp)
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
		v := NewVec(types.T_varchar.ToType(), mp)
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
		v := NewVec(types.T_array_float32.ToType(), mp)
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
	mp := mpool.MustNewZero()
	vec5 := NewVec(types.T_int8.ToType(), mp)
	defer vec5.Free(mp)
	vec6 := NewConstNull(types.T_int8.ToType(), 5, mp)
	defer vec6.Free(mp)

	err := AppendFixed(vec5, int8(1), false, mp)
	require.NoError(t, err)
	err = vec5.UnionMulti(vec6, 1, 2, mp)
	require.NoError(t, err)
	require.False(t, vec5.GetNulls().Contains(0))
	require.True(t, vec5.GetNulls().Contains(1))
	require.True(t, vec5.GetNulls().Contains(2))
	require.False(t, vec5.GetNulls().Contains(3))

	vec7 := NewVec(types.T_char.ToType(), mp)
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
	for _, fType := range fixSizedTypes {
		v1 := NewVec(fType, mp)
		v2 := NewVec(fType, mp)
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
		case types.T_int16:
			vals := make([]int16, 2)
			err = AppendFixedList[int16](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]int16, 3)
			err = AppendFixedList[int16](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_int32:
			vals := make([]int32, 2)
			err = AppendFixedList[int32](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]int32, 3)
			err = AppendFixedList[int32](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_int64:
			vals := make([]int64, 2)
			err = AppendFixedList[int64](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]int64, 3)
			err = AppendFixedList[int64](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_uint8:
			vals := make([]uint8, 2)
			err = AppendFixedList[uint8](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint8, 3)
			err = AppendFixedList[uint8](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_uint16:
			vals := make([]uint16, 2)
			err = AppendFixedList[uint16](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint16, 3)
			err = AppendFixedList[uint16](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_uint32:
			vals := make([]uint32, 2)
			err = AppendFixedList[uint32](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint32, 3)
			err = AppendFixedList[uint32](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_uint64:
			vals := make([]uint64, 2)
			err = AppendFixedList[uint64](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint64, 3)
			err = AppendFixedList[uint64](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_float32:
			vals := make([]float32, 2)
			err = AppendFixedList[float32](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]float32, 3)
			err = AppendFixedList[float32](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_float64:
			vals := make([]float64, 2)
			err = AppendFixedList[float64](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]float64, 3)
			err = AppendFixedList[float64](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_bool:
			vals := make([]bool, 2)
			err = AppendFixedList[bool](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]bool, 3)
			err = AppendFixedList[bool](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_bit:
			vals := make([]uint64, 2)
			err = AppendFixedList[uint64](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint64, 3)
			err = AppendFixedList[uint64](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_Rowid:
			vals := make([]types.Rowid, 2)
			err = AppendFixedList[types.Rowid](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.Rowid, 3)
			err = AppendFixedList[types.Rowid](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_TS:
			vals := make([]types.TS, 2)
			err = AppendFixedList[types.TS](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.TS, 3)
			err = AppendFixedList[types.TS](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_uuid:
			vals := make([]types.Uuid, 2)
			err = AppendFixedList[types.Uuid](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.Uuid, 3)
			err = AppendFixedList[types.Uuid](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_datetime:
			vals := make([]types.Datetime, 2)
			err = AppendFixedList[types.Datetime](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.Datetime, 3)
			err = AppendFixedList[types.Datetime](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_timestamp:
			vals := make([]types.Timestamp, 2)
			err = AppendFixedList[types.Timestamp](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.Timestamp, 3)
			err = AppendFixedList[types.Timestamp](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		}
		union := GetUnionAllFunction(fType, mp)
		err = union(v1, v2)
		require.NoError(t, err)
		require.Equal(t, 5, v1.Length())
		require.Equal(t, 5, v1.GetNulls().Count())
	}
}

func TestGetAny(t *testing.T) {
	{ // test const vector
		mp := mpool.MustNewZero()
		v := NewVec(types.T_int8.ToType(), mp)
		err := AppendFixed(v, int8(0), false, mp)
		require.NoError(t, err)
		s := GetAny(v, 0, false)
		v.Free(mp)
		require.Equal(t, int8(0), s.(int8))
	}
	{ // test const vector
		mp := mpool.MustNewZero()
		w := NewVec(types.T_varchar.ToType(), mp)
		err := AppendBytes(w, []byte("x"), false, mp)
		require.NoError(t, err)
		s := GetAny(w, 0, false)
		require.Equal(t, []byte("x"), s.([]byte))
		w.Free(mp)
	}
	{ // bool
		mp := mpool.MustNewZero()
		w := NewVec(types.T_bool.ToType(), mp)
		err := AppendFixedList(w, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		s := GetAny(w, 0, false)
		require.Equal(t, true, s.(bool))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		mp := mpool.MustNewZero()
		w := NewVec(types.T_int8.ToType(), mp)
		err := AppendFixedList(w, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, int8(1), s.(int8))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		mp := mpool.MustNewZero()
		w := NewVec(types.T_int16.ToType(), mp)
		err := AppendFixedList(w, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, int16(1), s.(int16))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		mp := mpool.MustNewZero()
		w := NewVec(types.T_int32.ToType(), mp)
		err := AppendFixedList(w, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, int32(1), s.(int32))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		mp := mpool.MustNewZero()
		w := NewVec(types.T_int64.ToType(), mp)
		err := AppendFixedList(w, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, int64(1), s.(int64))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		mp := mpool.MustNewZero()
		w := NewVec(types.T_uint8.ToType(), mp)
		err := AppendFixedList(w, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, uint8(1), s.(uint8))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		mp := mpool.MustNewZero()
		w := NewVec(types.T_uint16.ToType(), mp)
		err := AppendFixedList(w, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, uint16(1), s.(uint16))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		mp := mpool.MustNewZero()
		w := NewVec(types.T_uint32.ToType(), mp)
		err := AppendFixedList(w, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, uint32(1), s.(uint32))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		mp := mpool.MustNewZero()
		w := NewVec(types.T_uint64.ToType(), mp)
		err := AppendFixedList(w, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s := GetAny(w, 0, false)
		require.Equal(t, uint64(1), s.(uint64))

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		mp := mpool.MustNewZero()
		v := NewVec(types.T_text.ToType(), mp)
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)

		s := GetAny(v, 0, false)
		require.Equal(t, []byte("1"), s.([]byte))

		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		mp := mpool.MustNewZero()
		v := NewVec(types.T_time.ToType(), mp)
		err := AppendFixedList(v, []types.Time{12 * 3600 * 1000 * 1000, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		s := GetAny(v, 0, false)
		require.Equal(t, types.Time(12*3600*1000*1000), s.(types.Time))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		mp := mpool.MustNewZero()
		v := NewVec(types.T_timestamp.ToType(), mp)
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
		v := NewVec(typ, mp)
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
		v := NewVec(typ, mp)
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
		v := NewVec(types.T_uuid.ToType(), mp)
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
		v := NewVec(types.T_TS.ToType(), mp)
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
		v := NewVec(types.T_Rowid.ToType(), mp)
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
	source := NewVec(types.T_int8.ToType(), mp)
	AppendAny(source, int8(42), false, mp)
	data, err := source.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	source.Free(mp)
	var target Vector
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := target.UnmarshalBinary(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkToTypedSlice(b *testing.B) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType(), mp)
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
	vec := NewVec(types.T_int8.ToType(), mp)
	AppendAny(vec, int8(42), false, mp)
	b.ResetTimer()
	var slice []int8
	for i := 0; i < b.N; i++ {
		ToFixedCol[int8](vec, &slice)
	}
}

func BenchmarkMustFixedCol(b *testing.B) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int8.ToType(), mp)
	AppendAny(vec, int8(42), false, mp)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MustFixedColWithTypeCheck[int8](vec)
	}
}

func TestRowToString(t *testing.T) {
	t.Run("low_precision", func(t *testing.T) {
		testLowPrecisionFloatRowStrings(t, types.T_bf16,
			[]types.BF16{0x3fc0, 0xc000, 0, 0x8000, 0x7f7f, 1},
			[]float32{1.5, -2, 0, float32(math.Copysign(0, -1)), math.Float32frombits(0x7f7f0000), float32(math.Ldexp(1, -133))})
		testLowPrecisionFloatRowStrings(t, types.T_float16,
			[]types.Float16{0x3e00, 0xc000, 0, 0x8000, 0x7bff, 1},
			[]float32{1.5, -2, 0, float32(math.Copysign(0, -1)), 65504, float32(math.Ldexp(1, -24))})
		testLowPrecisionFloatRowStrings(t, types.T_float8,
			[]types.Float8{0x3c, 0xc0, 0, 0x80, 0x7e, 1},
			[]float32{1.5, -2, 0, float32(math.Copysign(0, -1)), 448, 1.0 / 512})
		testLowPrecisionFloatRowStrings(t, types.T_float4,
			[]types.Float4{3, 12, 0, 8, 7, 1},
			[]float32{1.5, -2, 0, float32(math.Copysign(0, -1)), 6, 0.5})
	})
	mp := mpool.MustNewZero()

	{ // Array Float32
		v := NewVec(types.T_array_float32.ToType(), mp)
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
		v := NewVec(types.T_array_float64.ToType(), mp)
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
		v := NewVec(types.T_bool.ToType(), mp)
		err := AppendFixed(v, true, false, mp)
		require.NoError(t, err)
		require.Equal(t, "true", v.RowToString(0))
		err = AppendFixed(v, false, true, mp)
		require.NoError(t, err)
		require.Equal(t, "null", v.RowToString(1))
		v.Free(mp)

		v0 := NewVec(types.T_bool.ToType(), mp)
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
		v := NewVec(types.T_int8.ToType(), mp)
		err := AppendFixedList(v, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		v := NewVec(types.T_int16.ToType(), mp)
		err := AppendFixedList(v, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		v := NewVec(types.T_int32.ToType(), mp)
		err := AppendFixedList(v, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		v := NewVec(types.T_int64.ToType(), mp)
		err := AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		v := NewVec(types.T_uint8.ToType(), mp)
		err := AppendFixedList(v, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint16
		v := NewVec(types.T_uint16.ToType(), mp)
		err := AppendFixedList(v, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint32
		v := NewVec(types.T_uint32.ToType(), mp)
		err := AppendFixedList(v, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint64
		v := NewVec(types.T_uint64.ToType(), mp)
		err := AppendFixedList(v, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float32
		v := NewVec(types.T_float32.ToType(), mp)
		err := AppendFixedList(v, []float32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // float64
		v := NewVec(types.T_float64.ToType(), mp)
		err := AppendFixedList(v, []float64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		v := NewVec(types.T_text.ToType(), mp)
		err := AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "2", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // date
		v := NewVec(types.T_date.ToType(), mp)
		err := AppendFixedList(v, []types.Date{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "0001-01-03", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // datetime
		// Test 1: Non-const vector with non-null value
		v := NewVec(types.T_datetime.ToType(), mp)
		scale := types.Datetime(types.MicroSecsPerSec * types.SecsPerDay)
		err := AppendFixedList(v, []types.Datetime{1 * scale, 2 * scale, 3 * scale, 4 * scale}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "0001-01-03 00:00:00", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		// Test 2: Non-const vector with null value
		v2 := NewVec(types.T_datetime.ToType(), mp)
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
		v4.nsp.Add(0)
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
		v6 := NewVec(types.T_datetime.ToType(), mp)
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
		v8 := NewVec(types.T_datetime.ToType(), mp)
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
		require.False(t, v9.nsp.Contains(0))
		result9 := v9.RowToString(0)
		require.Contains(t, result9, "0001-01-04")
		v9.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())

		// Test 10: Ensure all code paths are covered - non-const vector else branch at line 2753-2754
		v10 := NewVec(types.T_datetime.ToType(), mp)
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
		v := NewVec(types.T_time.ToType(), mp)
		scale := types.Time(types.MicroSecsPerSec)
		err := AppendFixedList(v, []types.Time{1 * scale, 2 * scale, 3 * scale, 4 * scale}, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "00:00:02", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		func() {
			v := NewVec(types.T_timestamp.ToType(), mp)
			defer v.Free(mp)
			ts := types.FromClockZone(time.UTC, 1970, 1, 1, 0, 0, 0, 0)
			err := AppendFixedList(v, []types.Timestamp{1, ts, 3, 4}, nil, mp)
			require.NoError(t, err)
			// Use the instant's historical offset, including date and minute changes.
			want := time.Unix(0, 0).In(time.Local).Format("2006-01-02 15:04:05")
			require.Equal(t, want, v.RowToString(1))
		}()
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		vs := make([]types.Decimal64, 4)
		v := NewVec(types.T_decimal64.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "0", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		vs := make([]types.Decimal128, 4)
		v := NewVec(types.T_decimal128.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "0", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		vs := make([]types.Uuid, 4)
		v := NewVec(types.T_uuid.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "00000000-0000-0000-0000-000000000000", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		vs := make([]types.TS, 4)
		v := NewVec(types.T_TS.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "0-0", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		vs := make([]types.Rowid, 4)
		v := NewVec(types.T_Rowid.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "00000000-0000-0000-0000-000000000000-0-0-0", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // blockid
		vs := make([]types.Blockid, 4)
		v := NewVec(types.T_Blockid.ToType(), mp)
		err := AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		require.Equal(t, "[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]", v.RowToString(1))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // bit
		v := NewVec(types.T_bit.ToType(), mp)
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

		ret := NewVec(types.T_int32.ToType(), mp)
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

		va := NewVec(types.T_text.ToType(), mp)
		vb := NewVec(types.T_text.ToType(), mp)

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

		ret := NewVec(types.T_text.ToType(), mp)
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

		ret := NewVec(types.T_int32.ToType(), mp)
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

		va := NewVec(types.T_text.ToType(), mp)
		vb := NewVec(types.T_text.ToType(), mp)

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

		ret := NewVec(types.T_text.ToType(), mp)
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
	vec := NewVec(types.T_char.ToType(), mp)
	defer vec.Free(mp)
	ss := "xxxxxx"
	err := AppendBytes(vec, []byte(ss), false, mp)
	require.NoError(t, err)
	vec.ResetWithSameType()
	vec2, err := VectorToProtoVector(vec)
	require.NoError(t, err)
	_, err = ProtoVectorToVector(vec2, mp)
	require.NoError(t, err)
}

func TestVectorPoolTypeChangeBug_Issue23295(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	// Step 1: Create vector with int8 type, allocate small buffer (8 bytes)
	vec := NewVec(types.T_int8.ToType(), mp)
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

func TestResetWithSameTypeResetsClass(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	vec := NewVec(types.T_varchar.ToType(), mp)
	defer vec.Free(mp)

	err := appendOneBytes(vec, []byte("hello"), false, mp)
	require.NoError(t, err)
	vec.ToConst()
	require.True(t, vec.IsConst())

	vec.ResetWithSameType()
	require.False(t, vec.IsConst())
	require.Equal(t, FLAT, vec.class)
	require.Equal(t, 0, vec.Length())

	// after reset, should be able to append normally
	err = appendOneBytes(vec, []byte("world"), false, mp)
	require.NoError(t, err)
	require.Equal(t, 1, vec.Length())
	require.False(t, vec.IsConst())
}

func TestFunctionResultAppendNullAfterToConst(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	wrapper := NewFunctionResultWrapper(types.T_json.ToType(), mp)
	defer wrapper.Free()

	result := MustFunctionResult[types.Varlena](wrapper)

	// first round: append null, then fold to const
	require.NoError(t, wrapper.PreExtendAndReset(1))
	require.NoError(t, result.AppendBytes(nil, true))
	result.vec.ToConst()
	require.True(t, result.vec.IsConstNull())

	// second round: simulate doFold reuse — PreExtendAndReset should reset class to FLAT
	require.NoError(t, wrapper.PreExtendAndReset(1))
	require.False(t, result.vec.IsConst()) // class should be FLAT now
	require.NoError(t, result.AppendBytes(nil, true))
	result.vec.ToConst()
	require.True(t, result.vec.IsConstNull()) // must still be recognized as const null
}

func TestInplaceSortAndCompactMarksUniqueVectorsSorted(t *testing.T) {
	mp := mpool.MustNew(t.Name())

	fixed := NewVec(types.T_int64.ToType(), mp)
	for _, value := range []int64{3, 1, 2} {
		require.NoError(t, AppendFixed(fixed, value, false, mp))
	}
	require.NoError(t, fixed.InplaceSortAndCompact(mp))
	require.Equal(t, []int64{1, 2, 3}, MustFixedColNoTypeCheck[int64](fixed))
	require.True(t, fixed.GetSorted())
	fixed.Free(mp)

	varlen := NewVec(types.T_varchar.ToType(), mp)
	for _, value := range []string{"c", "a", "b"} {
		require.NoError(t, AppendBytes(varlen, []byte(value), false, mp))
	}
	require.NoError(t, varlen.InplaceSortAndCompact(mp))
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, InefficientMustBytesCol(varlen))
	require.True(t, varlen.GetSorted())
	varlen.Free(mp)

	unsupported := NewVec(types.T_any.ToType(), mp)
	require.NoError(t, unsupported.InplaceSortAndCompact(mp))
	require.False(t, unsupported.GetSorted())
}

func TestInplaceSortMarksSupportedVectorsSortedWithoutCompacting(t *testing.T) {
	mp := mpool.MustNew(t.Name())

	fixed := NewVec(types.T_int64.ToType(), mp)
	for _, value := range []int64{3, 1, 1, 2} {
		require.NoError(t, AppendFixed(fixed, value, false, mp))
	}
	require.NoError(t, fixed.InplaceSort(mp))
	require.Equal(t, []int64{1, 1, 2, 3}, MustFixedColNoTypeCheck[int64](fixed))
	require.True(t, fixed.GetSorted())
	fixed.Free(mp)

	varlen := NewVec(types.T_varchar.ToType(), mp)
	for _, value := range []string{"c", "a", "a", "b"} {
		require.NoError(t, AppendBytes(varlen, []byte(value), false, mp))
	}
	require.NoError(t, varlen.InplaceSort(mp))
	require.Equal(t,
		[][]byte{[]byte("a"), []byte("a"), []byte("b"), []byte("c")},
		InefficientMustBytesCol(varlen))
	require.True(t, varlen.GetSorted())
	varlen.Free(mp)

	unsupported := NewVec(types.T_any.ToType(), mp)
	require.NoError(t, unsupported.InplaceSort(mp))
	require.False(t, unsupported.GetSorted())
}

// lowPrecValueOrderingCase exercises the container ordering/aggregation paths for one
// low-precision float type, asserting they order/aggregate by FLOAT VALUE, not raw bits.
func lowPrecValueOrderingCase[T types.LowPrecFloat](
	t *testing.T, oid types.T, from func(float32) T,
) {
	mp := mpool.MustNew(t.Name())

	// Values exactly representable in ALL four formats (float4 magnitudes are
	// {0,.5,1,1.5,2,3,4,6}). Negatives are the discriminator: a negative float's raw
	// uint bits exceed a positive's, so a bit-wise sort would reverse them.
	vals := []float32{2.0, -1.0, -3.0, 0.5}
	wantSorted := []float32{-3.0, -1.0, 0.5, 2.0}

	build := func(fs []float32) *Vector {
		v := NewVec(oid.ToType(), mp)
		for _, f := range fs {
			require.NoError(t, AppendFixed(v, from(f), false, mp))
		}
		return v
	}
	toF := func(v *Vector) []float32 {
		col := MustFixedColNoTypeCheck[T](v)
		out := make([]float32, len(col))
		for i := range col {
			out[i] = col[i].ToFloat32()
		}
		return out
	}

	// InplaceSort: value order (negatives first), no compaction.
	v := build(vals)
	require.True(t, supportsInplaceSort(oid))
	require.NoError(t, v.InplaceSort(mp))
	require.Equal(t, wantSorted, toF(v), "InplaceSort")
	require.True(t, v.GetSorted())
	v.Free(mp)

	// compareVectorRows: -3.0 (row 2) < 2.0 (row 0); a raw-bit compare would say the
	// negative is larger.
	v = build(vals)
	require.Less(t, compareVectorRows(v, 2, 0), 0, "compareVectorRows neg<pos")
	require.Greater(t, compareVectorRows(v, 0, 1), 0, "compareVectorRows 2.0>-1.0")
	v.Free(mp)

	// InplaceSortAndCompact: dedup by value (+0/-0 collapse; here plain dups).
	v = build([]float32{1.0, -1.0, 1.0, -1.0})
	require.NoError(t, v.InplaceSortAndCompact(mp))
	require.Equal(t, []float32{-1.0, 1.0}, toF(v), "InplaceSortAndCompact")
	require.True(t, v.GetSorted())
	v.Free(mp)

	// GetSumValue: widened float64 sum = 2-1-3+0.5 = -1.5.
	v = build(vals)
	ok, sumBytes := v.GetSumValue()
	require.True(t, ok)
	require.InDelta(t, -1.5, types.DecodeFloat64(sumBytes), 1e-6, "GetSumValue")
	v.Free(mp)

	// GetMinMaxValue: by value, min=-3.0 max=2.0.
	v = build(vals)
	ok, minBytes, maxBytes := v.GetMinMaxValue()
	require.True(t, ok)
	require.Equal(t, float32(-3.0), types.DecodeFixed[T](minBytes).ToFloat32(), "GetMinMaxValue min")
	require.Equal(t, float32(2.0), types.DecodeFixed[T](maxBytes).ToFloat32(), "GetMinMaxValue max")
	v.Free(mp)
}

// TestLowPrecFloatContainerValueOrdering covers the Layer-4 container paths for the
// scalar low-precision float types (#20567): they must order/aggregate by float value
// (via ToFloat32), never by the raw uint bits, whose sign bit inverts float order.
func TestLowPrecFloatContainerValueOrdering(t *testing.T) {
	t.Run("bf16", func(t *testing.T) {
		lowPrecValueOrderingCase(t, types.T_bf16, types.BF16FromFloat32)
	})
	t.Run("float16", func(t *testing.T) {
		lowPrecValueOrderingCase(t, types.T_float16, types.Float16FromFloat32)
	})
	t.Run("float8", func(t *testing.T) {
		lowPrecValueOrderingCase(t, types.T_float8, types.Float8FromFloat32)
	})
	t.Run("float4", func(t *testing.T) {
		lowPrecValueOrderingCase(t, types.T_float4, types.Float4FromFloat32)
	})
}

// TestLowPrecFloatGetMinMaxSkipsNaN verifies NaN never poisons the zonemap bounds
// (bf16/float16/float8 have a NaN encoding; float4 has none).
func TestLowPrecFloatGetMinMaxSkipsNaN(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	nan := float32(math.NaN())

	// bf16 with a NaN mixed in: min/max come from the finite values only.
	v := NewVec(types.T_bf16.ToType(), mp)
	for _, f := range []float32{nan, -2.0, 4.0, nan} {
		require.NoError(t, AppendFixed(v, types.BF16FromFloat32(f), false, mp))
	}
	ok, minB, maxB := v.GetMinMaxValue()
	require.True(t, ok)
	require.Equal(t, float32(-2.0), types.DecodeFixed[types.BF16](minB).ToFloat32())
	require.Equal(t, float32(4.0), types.DecodeFixed[types.BF16](maxB).ToFloat32())
	v.Free(mp)

	// All-NaN bf16: no comparable value → ok=false, zonemap left uninitialized.
	v = NewVec(types.T_bf16.ToType(), mp)
	for i := 0; i < 3; i++ {
		require.NoError(t, AppendFixed(v, types.BF16FromFloat32(nan), false, mp))
	}
	ok, _, _ = v.GetMinMaxValue()
	require.False(t, ok)
	v.Free(mp)
}

func TestVarlenaAreaDisjointLifecycle(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	typ := types.T_varchar.ToType()
	payload := []byte(strings.Repeat("x", 128))

	flat := NewVec(typ, mp)
	require.True(t, flat.VarlenaAreaIsDisjoint())
	for range 2 {
		require.NoError(t, AppendBytes(flat, payload, false, mp))
	}
	require.True(t, flat.VarlenaAreaIsDisjoint(),
		"ordinary appends own disjoint area ranges")

	values, _ := MustVarlenaRawData(flat)
	require.True(t, flat.VarlenaAreaIsDisjoint(),
		"read access must not mutate vector metadata")
	require.NoError(t, SetFixedAtNoTypeCheck(flat, 1, values[0]))
	require.False(t, flat.VarlenaAreaIsDisjoint(),
		"installing an arbitrary descriptor must invalidate the proof")

	flat.ResetWithSameType()
	require.True(t, flat.VarlenaAreaIsDisjoint())
	require.NoError(t, AppendBytes(flat, payload, false, mp))
	flat.ResetWithSameType()
	require.NoError(t, AppendBytes(flat, nil, true, mp))
	values, _ = MustVarlenaRawData(flat)
	require.True(t, values[0].IsSmall(),
		"a null append must clear a stale descriptor from reused capacity")
	flat.GetNulls().Del(0)
	require.True(t, flat.VarlenaAreaIsDisjoint(),
		"null-bitmap changes cannot invalidate a descriptor-level proof")

	flat.ResetWithSameType()
	for range 2 {
		require.NoError(t, AppendBytes(flat, payload, false, mp))
	}
	copied := NewVec(typ, mp)
	require.NoError(t, copied.UnionBatch(flat, 0, flat.Length(), nil, mp))
	require.True(t, copied.VarlenaAreaIsDisjoint(),
		"full in-order copy preserves independent payload ranges")
	copied.Free(mp)

	require.NoError(t, flat.Shrink([]int64{0, 0}, false, mp))
	require.False(t, flat.VarlenaAreaIsDisjoint(),
		"selection can duplicate a descriptor")
	flat.Free(mp)

	constant, err := NewConstBytes(typ, payload, 2, mp)
	require.NoError(t, err)
	shared := NewVec(typ, mp)
	require.NoError(t, shared.UnionBatch(constant, 0, 2, nil, mp))
	require.False(t, shared.VarlenaAreaIsDisjoint(),
		"const broadcast shares one non-inline descriptor")
	compact, err := shared.CloneToFlatCompact(mp)
	require.NoError(t, err)
	require.True(t, compact.VarlenaAreaIsDisjoint(),
		"compaction materializes independent payload ranges")

	compact.Free(mp)
	shared.Free(mp)
	constant.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestVarlenaAppendFailureReleasesMemory(t *testing.T) {
	mp, err := mpool.NewMPool("varlena-disjoint-failure", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	vec := NewVec(types.T_varchar.ToType(), mp)
	err = AppendBytesList(
		vec,
		[][]byte{make([]byte, 128), make([]byte, 2<<20)},
		nil,
		mp,
	)
	require.Error(t, err)

	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestConstSetFunctionCopiesSelectedMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	source := NewVec(types.T_text.ToType(), mp)
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendBytes(source, []byte("5"), false, mp))
	require.NoError(t, AppendBytes(source, []byte("5"), false, mp))
	destination := NewVec(types.T_text.ToType(), mp)
	t.Cleanup(func() { destination.Free(mp) })
	set := GetConstSetFunction(types.T_text.ToType(), mp)
	require.NoError(t, set(destination, source, 1, 3))
	require.Equal(t, 3, destination.Length())
	require.Equal(t, "5", destination.GetStringAt(2))

	nullSource := NewVec(types.T_text.ToType(), mp)
	t.Cleanup(func() { nullSource.Free(mp) })
	require.NoError(t, AppendBytes(nullSource, nil, true, mp))
	require.NoError(t, set(destination, nullSource, 0, 2))
	require.True(t, destination.IsConstNull())

	require.NoError(t, set(destination, source, 1, 3))
	require.False(t, destination.IsConstNull())
	require.Equal(t, "5", destination.GetStringAt(0))
	require.NoError(t, set(destination, source, 1, 0))
	require.Zero(t, destination.Length())
}

func TestSelectedBatchPreflightProtocol(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	destination := NewVec(types.T_varchar.ToType(), mp)
	defer func() {
		destination.Free(mp)
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	for _, value := range []string{
		strings.Repeat("a", 64),
		strings.Repeat("b", 80),
		strings.Repeat("c", 96),
	} {
		require.NoError(t, AppendBytes(source, []byte(value), false, mp))
	}
	flags := []uint8{1, 0, 1}
	var preflight SelectedBatchPreflight
	require.ErrorIs(t,
		destination.UnionBatchPreflighted(source, 0, len(flags), flags, &preflight, mp),
		mpool.ErrAllocationAccountInvariant)
	require.ErrorIs(t,
		destination.UnionBatchPreflighted(source, 0, len(flags), flags, nil, mp),
		mpool.ErrAllocationAccountInvariant)

	preflight, err := destination.PreExtendSelectedBatch(
		source, 0, len(flags), flags, 2, mp)
	require.NoError(t, err)
	require.True(t, preflight.Ready())
	admitted := mp.CurrNB()
	require.NoError(t,
		destination.UnionBatchPreflighted(source, 0, len(flags), flags, &preflight, mp))
	require.Equal(t, admitted, mp.CurrNB(),
		"publication must use only capacity admitted by preflight")
	require.Equal(t, 2, destination.Length())
	require.Equal(t, source.GetBytesAt(0), destination.GetBytesAt(0))
	require.Equal(t, source.GetBytesAt(2), destination.GetBytesAt(1))
	require.False(t, preflight.Ready())
	require.ErrorIs(t,
		destination.UnionBatchPreflighted(source, 0, len(flags), flags, &preflight, mp),
		mpool.ErrAllocationAccountInvariant,
		"the preflight is consumed exactly once")

	preflight, err = destination.PreExtendSelectedBatch(
		source, 0, len(flags), []uint8{1, 2, 0}, 2, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	require.False(t, preflight.Ready(),
		"a failed preflight must not return a publishable result")

	destination.ResetWithSameType()
	preflight, err = destination.PreExtendSelectedBatchValidated(
		source, 0, len(flags), flags, 2, mp)
	require.NoError(t, err)
	require.NoError(t,
		destination.UnionBatchPreflighted(source, 0, len(flags), flags, &preflight, mp))
	require.Equal(t, 2, destination.Length())

	constant, err := NewConstBytes(
		types.T_varchar.ToType(), []byte(strings.Repeat("constant", 8)),
		len(flags)+4, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	destination.ResetWithSameType()
	preflight, err = destination.PreExtendSelectedBatchValidated(
		constant, 2, len(flags), flags, 2, mp)
	require.NoError(t, err)
	require.NoError(t,
		destination.UnionBatchPreflighted(constant, 2, len(flags), flags, &preflight, mp))
	require.Equal(t, 2, destination.Length())
	require.Equal(t, constant.GetBytesAt(0), destination.GetBytesAt(0))
	require.Equal(t, constant.GetBytesAt(0), destination.GetBytesAt(1))

	preflight, err = destination.PreExtendSelectedBatch(
		destination, 0, 2, []uint8{1, 0}, 3, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid,
		"a preflight cannot retain an aliased source for publication")
	require.False(t, preflight.Ready(),
		"a rejected aliased preflight must not return a publishable result")
}

func TestUnionAppendMetadataTransitions(t *testing.T) {
	for _, copies := range []int{1, 3} {
		for _, metadata := range []string{"ordinary"} {
			t.Run(fmt.Sprintf("%s/copies=%d", metadata, copies), func(t *testing.T) {
				mp := mpool.MustNewZero()
				source := NewVec(types.T_varchar.ToType(), mp)
				destination := NewVec(types.T_varchar.ToType(), mp)
				var changed, constant *Vector
				t.Cleanup(func() {
					source.Free(mp)
					changed.Free(mp)
					constant.Free(mp)
					destination.Free(mp)
					require.Zero(t, mp.CurrNB())
				})
				require.NoError(t, AppendBytesList(source, [][]byte{
					[]byte("value"), nil, []byte(strings.Repeat("long", 16)),
				}, []bool{false, true, false}, mp))
				changed, err := source.Dup(mp)
				require.NoError(t, err)
				changed.GetNulls().Add(0)
				constant, err = NewConstBytes(types.T_varchar.ToType(), []byte("constant"), 4, mp)
				require.NoError(t, err)
				rows := []struct {
					vec *Vector
					row int
				}{{source, 1}, {constant, 3}, {source, 2}, {changed, 1},
					{changed, 0}, {changed, 2}, {source, 0}, {source, 1}}
				for range 2 { // Reuse must not retain a previous mixed representation.
					destination.ResetWithSameType()
					for end, input := range rows {
						if copies == 1 {
							require.NoError(t, destination.UnionOne(input.vec, int64(input.row), mp))
						} else {
							require.NoError(t, destination.UnionMulti(input.vec, int64(input.row), copies, mp))
						}
						require.Equal(t, (end+1)*copies, destination.Length())
						for row := range destination.Length() {
							expected := rows[row/copies]
							isNull := expected.vec.IsNull(uint64(expected.row))
							require.Equal(t, isNull, destination.IsNull(uint64(row)))
							if !isNull {
								require.Equal(t, expected.vec.GetBytesAt(expected.row), destination.GetBytesAt(row))
							}
						}
					}
				}
			})
		}
	}
}

func BenchmarkUnionMultiUniformMetadata(b *testing.B) {
	for _, oid := range []types.T{types.T_int64, types.T_varchar} {
		for _, copies := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("%s/copies=%d", oid, copies), func(b *testing.B) {
				mp := mpool.MustNewZero()
				source := NewVec(oid.ToType(), mp)
				defer source.Free(mp)
				destination := NewVec(oid.ToType(), mp)
				defer destination.Free(mp)
				if oid == types.T_int64 {
					require.NoError(b, AppendFixed(source, int64(42), false, mp))
				} else {
					require.NoError(b, AppendBytes(source, []byte("ordinary column value"), false, mp))
				}
				require.NoError(b, destination.PreExtend(copies, mp))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					destination.ResetWithSameType()
					if err := destination.UnionMulti(source, 0, copies, mp); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestUnionOneEmptyMarkerSets(t *testing.T) {
	for _, oid := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_decimal128, types.T_decimal256} {
		t.Run(oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			source := NewVec(oid.ToType(), mp)
			t.Cleanup(func() { source.Free(mp) })
			destination := NewVec(oid.ToType(), mp)
			t.Cleanup(func() { destination.Free(mp) })
			require.NoError(t, source.PreExtend(3, mp))
			source.SetLength(3)
			tlen := source.GetType().TypeSize()
			// Distinct fixed-width byte patterns make every selected row and
			// byte observable, including widths handled by the generic copy.
			for row := range 3 {
				for column := range tlen {
					source.data[row*tlen+column] = byte(row*32 + column + 1)
				}
			}
			require.NoError(t, destination.PreExtend(3, mp))
			capacity := cap(destination.data)
			for range 2 {
				destination.ResetWithSameType()
				for row, sel := range []int64{2, 0, 1} {
					require.NoError(t, destination.UnionOne(source, sel, mp))
					require.Equal(t, source.data[int(sel)*tlen:(int(sel)+1)*tlen], destination.data[row*tlen:(row+1)*tlen])
					require.False(t, destination.IsNull(uint64(row)))
				}
				require.Equal(t, 3, destination.Length())
				require.Equal(t, capacity, len(destination.data), "append retains extendWithBitmaps' slice-to-cap contract")
			}
			require.NoError(t, destination.UnionOne(source, 1, mp))
			require.Equal(t, source.data[tlen:2*tlen], destination.data[3*tlen:4*tlen])
		})
	}
}

func TestUnionOneSourceMarkerLookup(t *testing.T) {
	for _, scenario := range []string{"source_null", "source_all_null", "null_prefix", "value_prefix", "source_const", "source_const_null", "source_const_null_with_data", "destination_const", "nil_mpool"} {
		t.Run(scenario, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			source := NewVec(types.T_int64.ToType(), mp)
			t.Cleanup(func() { source.Free(mp) })
			destination := NewVec(types.T_int64.ToType(), mp)
			t.Cleanup(func() { destination.Free(mp) })
			require.NoError(t, AppendFixedList(source, []int64{11, 22, 33}, nil, mp))
			require.NoError(t, destination.PreExtend(4, mp))
			offset := 0
			appendMP := mp
			switch scenario {
			case "source_null":
				source.GetNulls().Set(1)
			case "source_all_null":
				for row := range source.Length() {
					source.GetNulls().Set(uint64(row))
				}
			case "null_prefix":
				require.NoError(t, AppendFixed(destination, int64(0), true, mp))
				offset = 1
			case "value_prefix":
				require.NoError(t, AppendFixed(destination, int64(44), false, mp))
				offset = 1
			case "source_const_null":
				source.Free(mp)
				source = NewConstNull(types.T_int64.ToType(), 3, mp)
			case "source_const", "source_const_null_with_data":
				source.Free(mp)
				var err error
				source, err = NewConstFixed(types.T_int64.ToType(), int64(77), 3, mp)
				require.NoError(t, err)
				if scenario == "source_const_null_with_data" {
					source.GetNulls().Set(0)
				}
			case "destination_const":
				destination.SetClass(CONSTANT)
			case "nil_mpool":
				appendMP = nil
			}
			if appendMP == nil {
				// A call must pass the vector's own pool.
				require.Error(t, destination.UnionOne(source, 0, appendMP))
				return
			}
			for row, sel := range []int64{0, 1, 2} {
				require.NoError(t, destination.UnionOne(source, sel, appendMP))
				require.Equal(t, source.IsNull(uint64(sel)), destination.IsNull(uint64(row+offset)))
				if !source.IsNull(uint64(sel)) {
					// Assert physical row bytes as well as constant-class retention.
					physicalSourceRow := int(sel)
					if source.IsConst() {
						physicalSourceRow = 0
					}
					require.Equal(t, source.data[physicalSourceRow*8:(physicalSourceRow+1)*8], destination.data[(row+offset)*8:(row+offset+1)*8])
				}
			}
			require.Equal(t, 3+offset, destination.Length())
			if scenario == "null_prefix" {
				require.True(t, destination.IsNull(0))
			}
			if scenario == "value_prefix" {
				require.False(t, destination.IsNull(0))
				require.Equal(t, int64(44), GetFixedAtWithTypeCheck[int64](destination, 0))
			}
			if scenario == "destination_const" {
				require.True(t, destination.IsConst())
				require.Equal(t, int64(11), GetFixedAtWithTypeCheck[int64](destination, 2))
			}
		})
	}
}

func TestUnionOneFixedGrowthFailureKeepsPrefix(t *testing.T) {
	const poolCap = int64(1 << 20)
	mp, err := mpool.NewMPool(t.Name(), poolCap, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	destination := NewVec(types.T_int64.ToType(), mp)
	t.Cleanup(func() { destination.Free(mp) })
	require.NoError(t, destination.PreExtend(1, mp))
	rows := cap(destination.data) / 8
	for range rows {
		require.NoError(t, AppendFixed(destination, int64(7), false, mp))
	}
	sourceMP := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, sourceMP.CurrNB()) })
	source := NewVec(types.T_int64.ToType(), sourceMP)
	t.Cleanup(func() { source.Free(sourceMP) })
	require.NoError(t, AppendFixed(source, int64(9), false, sourceMP))
	fill, err := mp.Alloc(int(poolCap-mp.CurrNB()), true)
	require.NoError(t, err)
	t.Cleanup(func() { mp.Free(fill) })
	require.Error(t, destination.UnionOne(source, 0, mp))
	require.Equal(t, rows, destination.Length())
	for _, value := range MustFixedColNoTypeCheck[int64](destination) {
		require.Equal(t, int64(7), value)
	}
	mp.Free(fill)
	fill = nil
	require.NoError(t, destination.UnionOne(source, 0, mp))
	require.Equal(t, rows+1, destination.Length())
	require.Equal(t, int64(9), GetFixedAtWithTypeCheck[int64](destination, rows))
}

func BenchmarkUnionOneUniformMetadata(b *testing.B) {
	for _, oid := range []types.T{types.T_int64, types.T_varchar} {
		{
			b.Run(oid.String(), func(b *testing.B) {
				const rows = 1024
				mp := mpool.MustNewZero()
				source := NewVec(oid.ToType(), mp)
				destination := NewVec(oid.ToType(), mp)
				defer source.Free(mp)
				defer destination.Free(mp)
				if oid == types.T_int64 {
					require.NoError(b, AppendFixedList(source, make([]int64, rows), nil, mp))
				} else {
					for range rows {
						require.NoError(b, AppendBytes(source, []byte("ordinary column value"), false, mp))
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					destination.ResetWithSameType()
					for row := range rows {
						if err := destination.UnionOne(source, int64(row), mp); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

func BenchmarkCopyPreallocatedNullVectorReverseFill(b *testing.B) {
	const rows = 16 << 10
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType(), mp)
	require.NoError(b, AppendFixed(source, int64(42), false, mp))
	defer source.Free(mp)

	b.ReportMetric(float64(rows), "rows/op")
	for b.Loop() {
		b.StopTimer()
		destination := NewVec(types.T_int64.ToType(), mp)
		if err := destination.PreExtend(rows, mp); err != nil {
			b.Fatal(err)
		}
		destination.SetLength(rows)
		destination.SetAllNulls(rows)
		b.StartTimer()
		for row := rows - 1; row >= 0; row-- {
			if err := destination.Copy(source, int64(row), 0, mp); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		destination.Free(mp)
		b.StartTimer()
	}
}

func TestUnionBatchCapacityFailurePreservesDestinationArea(t *testing.T) {
	oldCapLimit := mpool.CapLimit
	mpool.CapLimit = 64 << 10
	t.Cleanup(func() {
		mpool.CapLimit = oldCapLimit
	})

	mp := mpool.MustNewZeroNoFixed()
	destination := NewVec(types.New(types.T_array_float32, 1024, 0), mp)
	source := NewVec(types.New(types.T_array_float32, 1024, 0), mp)
	defer func() {
		destination.Free(mp)
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	value := make([]float32, 1024)
	for range 15 {
		require.NoError(t, AppendArray(destination, value, false, mp))
	}
	require.NoError(t, AppendArray(source, value, false, mp))
	beforeLength := destination.Length()
	beforeArea := bytes.Clone(destination.GetArea())

	err := destination.UnionBatch(source, 0, 1, nil, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAllocatorLimit)
	require.Equal(t, beforeLength, destination.Length())
	require.Equal(t, beforeArea, destination.GetArea())
}

func BenchmarkUnionBatchFixedRange(b *testing.B) {
	for _, test := range []struct {
		name      string
		nullEvery int
	}{
		{name: "no-null"},
		{name: "sparse", nullEvery: 100},
		{name: "dense", nullEvery: 2},
		{name: "all-null", nullEvery: 1},
	} {
		b.Run(test.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			source := NewVec(types.T_int64.ToType(), mp)
			defer source.Free(mp)
			for i := 0; i < 1024; i++ {
				isNull := test.nullEvery != 0 && i%test.nullEvery == 0
				require.NoError(b, AppendFixed(source, int64(i), isNull, mp))
			}
			destination := NewVec(types.T_int64.ToType(), mp)
			defer destination.Free(mp)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				destination.ResetWithSameType()
				if err := destination.UnionBatch(source, 0, source.Length(), nil, mp); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnionBatchStaticBinaryNoMetadata(b *testing.B) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varbinary.ToType(), mp)
	defer source.Free(mp)
	for i := 0; i < 1024; i++ {
		require.NoError(b, AppendBytes(source, []byte("value"), false, mp))
	}
	destination := NewVec(types.T_varbinary.ToType(), mp)
	defer destination.Free(mp)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		destination.ResetWithSameType()
		if err := destination.UnionBatch(source, 0, source.Length(), nil, mp); err != nil {
			b.Fatal(err)
		}
	}
}

// TestLowPrecFloatAccessMovement exercises the access/movement/string switches for the
// scalar low-precision float types (bf16/float16/float8/float4) so each per-type arm is
// covered: GetAny/AppendAny round-trip, Shrink, ShrinkByMask, Shuffle, ShuffleWithBuf,
// String, RowToString, and NewFunctionResultWrapper (#20567).
func TestLowPrecFloatAccessMovement(t *testing.T) {
	mp := mpool.MustNew(t.Name())

	cases := []struct {
		oid types.T
		mk  func(float32) any
	}{
		{types.T_bf16, func(f float32) any { return types.BF16FromFloat32(f) }},
		{types.T_float16, func(f float32) any { return types.Float16FromFloat32(f) }},
		{types.T_float8, func(f float32) any { return types.Float8FromFloat32(f) }},
		{types.T_float4, func(f float32) any { return types.Float4FromFloat32(f) }},
	}

	for _, c := range cases {
		t.Run(c.oid.String(), func(t *testing.T) {
			// AppendAny builds the column; GetAny reads it back.
			v := NewVec(c.oid.ToType(), mp)
			for _, f := range []float32{1.0, -2.0, 0.5, 4.0} {
				require.NoError(t, AppendAny(v, c.mk(f), false, mp))
			}
			require.Equal(t, c.mk(1.0), GetAny(v, 0, true))
			require.Equal(t, c.mk(-2.0), GetAny(v, 1, true))

			// String and RowToString must not panic and render the row.
			_ = v.String()
			require.NotEmpty(t, v.RowToString(0))

			// NewFunctionResultWrapper builds a result vector for the type.
			rw := NewFunctionResultWrapper(c.oid.ToType(), mp)
			require.NotNil(t, rw)
			rw.Free()

			// Shuffle reorders in place.
			require.NoError(t, v.Shuffle([]int64{3, 2, 1, 0}, mp))
			require.Equal(t, c.mk(4.0), GetAny(v, 0, true))
			v.Free(mp)

			// ShuffleWithBuf.
			v2 := NewVec(c.oid.ToType(), mp)
			for _, f := range []float32{1.0, 2.0, 3.0} {
				require.NoError(t, AppendAny(v2, c.mk(f), false, mp))
			}
			var buf []byte
			require.NoError(t, v2.ShuffleWithBuf([]int64{2, 1, 0}, mp, &buf))
			require.Equal(t, c.mk(3.0), GetAny(v2, 0, true))
			v2.Free(mp)

			// Shrink keeps selected rows.
			v3 := NewVec(c.oid.ToType(), mp)
			for _, f := range []float32{1.0, 2.0, 3.0, 4.0} {
				require.NoError(t, AppendAny(v3, c.mk(f), false, mp))
			}
			require.NoError(t, v3.Shrink([]int64{0, 2}, false, mp))
			require.Equal(t, 2, v3.Length())
			require.Equal(t, c.mk(1.0), GetAny(v3, 0, true))
			require.Equal(t, c.mk(3.0), GetAny(v3, 1, true))
			v3.Free(mp)

			// ShrinkByMask.
			v4 := NewVec(c.oid.ToType(), mp)
			for _, f := range []float32{1.0, 2.0, 3.0, 4.0} {
				require.NoError(t, AppendAny(v4, c.mk(f), false, mp))
			}
			var bm bitmap.Bitmap
			bm.InitWithSize(4)
			bm.Add(1)
			bm.Add(3)
			require.NoError(t, v4.ShrinkByMask(&bm, false, 0, mp))
			require.Equal(t, 2, v4.Length())
			require.Equal(t, c.mk(2.0), GetAny(v4, 0, true))
			v4.Free(mp)
		})
	}
}

func TestBoolMinMax(t *testing.T) {
	for _, tc := range []struct {
		name         string
		values       []bool
		nulls        []int
		ok, min, max bool
	}{
		{name: "empty"},
		{name: "false", values: []bool{false, false}, ok: true},
		{name: "true", values: []bool{true, true}, ok: true, min: true, max: true},
		{name: "mixed", values: []bool{false, true}, ok: true, max: true},
		{name: "mixed reversed", values: []bool{true, false}, ok: true, max: true},
		{name: "nullable mixed", values: []bool{true, false, true}, nulls: []int{0}, ok: true, max: true},
		{name: "null false", values: []bool{false, true}, nulls: []int{0}, ok: true, min: true, max: true},
		{name: "null true", values: []bool{false, true}, nulls: []int{1}, ok: true},
		{name: "all null", values: []bool{false, true}, nulls: []int{0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			v := NewVec(types.T_bool.ToType(), mp)
			defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()) }()
			for i, value := range tc.values {
				require.NoError(t, AppendFixed(v, value, slices.Contains(tc.nulls, i), mp))
			}
			ok, min, max := v.GetMinMaxValue()
			require.Equal(t, tc.ok, ok)
			if ok {
				require.Equal(t, tc.min, types.DecodeBool(min))
				require.Equal(t, tc.max, types.DecodeBool(max))
			}
		})
	}
}

func TestCloneWindowToConstNullReuse(t *testing.T) {
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		for _, nilMP := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/nilMP=%v", typ.Oid, nilMP), func(t *testing.T) {
				mp := mpool.MustNewZero()
				src := NewConstNull(typ, 2, mp)
				t.Cleanup(func() { src.Free(mp) })
				dst := NewVec(typ, mp)
				t.Cleanup(func() { dst.Free(mp) })
				require.NoError(t, dst.PreExtend(2, mp))
				backing := dst.GetData()
				before := mp.CurrNB()
				require.Positive(t, before)
				cloneMP := mp
				if nilMP {
					cloneMP = nil
				}
				if nilMP {
					// The destination's own pool must be passed.
					require.Error(t, src.CloneWindowTo(dst, 0, 2, cloneMP))
					return
				}
				require.NoError(t, src.CloneWindowTo(dst, 0, 2, cloneMP))
				require.True(t, dst.IsConstNull())
				require.Equal(t, 2, dst.Length())
				require.Empty(t, dst.GetData())
				require.Equal(t, cap(backing), cap(dst.GetData()))
				require.Equal(t, before, mp.CurrNB())
				encoded, err := dst.MarshalBinary()
				require.NoError(t, err)
				decoded := NewVec(typ, mp)
				t.Cleanup(func() { decoded.Free(mp) })
				require.NoError(t, decoded.UnmarshalBinary(encoded))
				require.True(t, decoded.IsConstNull())
				require.Equal(t, 2, decoded.Length())

				dst.ResetWithNewType(&typ)
				if typ.IsVarlen() {
					require.NoError(t, AppendBytes(dst, []byte("next"), false, mp))
					require.Equal(t, []byte("next"), dst.GetBytesAt(0))
				} else {
					require.NoError(t, AppendFixed(dst, int64(7), false, mp))
					require.Equal(t, int64(7), GetFixedAtNoTypeCheck[int64](dst, 0))
				}
				require.True(t, &backing[:cap(backing)][0] == &dst.GetData()[0])
				dst.Free(mp)
				require.Zero(t, mp.CurrNB())
			})
		}
	}
}

func BenchmarkConstSetSelection(b *testing.B) {
	for _, shape := range []string{"flat", "flat_null", "constant", "constant_null", "text", "zero"} {
		b.Run(shape, func(b *testing.B) {
			mp := mpool.MustNewZero()
			b.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(b, mp.CurrNB()) })
			typ := types.T_int64.ToType()
			if shape == "text" {
				typ = types.T_text.ToType()
			}
			source := NewVec(typ, mp)
			var err error
			switch shape {
			case "constant":
				source.Free(mp)
				source, err = NewConstFixed(typ, int64(7), 2, mp)
			case "constant_null":
				source.Free(mp)
				source = NewConstNull(typ, 2, mp)
			case "text":
				err = AppendBytesList(source, [][]byte{[]byte("first"), []byte("second")}, nil, mp)
			default:
				err = AppendFixedList(source, []int64{7, 8}, nil, mp)
			}
			require.NoError(b, err)
			b.Cleanup(func() { source.Free(mp) })
			if shape == "flat_null" {
				source.GetNulls().Add(1)
			}
			destination := NewConstNull(typ, 0, mp)
			b.Cleanup(func() { destination.Free(mp) })
			set := GetConstSetFunction(typ, mp)
			length := 1
			if shape == "zero" {
				length = 0
			}
			require.NoError(b, set(destination, source, 1, length))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := set(destination, source, 1, length); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Every vector wire format (protobuf, MarshalBinary with and without copying
// on decode, and legacy V1) keeps a string type's collation identity and
// revision, and every decoder rejects an unknown revision.
func TestVectorWireFormatsKeepCollationMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, domain := range []struct{ id, revision uint8 }{{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 1}, {5, 1}, {7, 1}, {12, 1}} {
		typ := types.NewWithCharset(types.T_varchar, 32, 0, domain.id)
		typ.CollationVersion = domain.revision
		src := NewVec(typ, mp)
		require.NoError(t, AppendBytes(src, []byte("😀"), false, mp))
		proto, err := VectorToProtoVector(src)
		require.NoError(t, err)
		fromProto, err := ProtoVectorToVector(proto, mp)
		require.NoError(t, err)
		require.Equal(t, typ, *fromProto.GetType())
		require.Equal(t, "😀", fromProto.GetStringAt(0))
		fromProto.Free(mp)
		wire, err := src.MarshalBinary()
		require.NoError(t, err)
		for _, copyData := range []bool{false, true} {
			restored := NewVec(types.T_any.ToType(), mp)
			if copyData {
				err = restored.UnmarshalBinaryWithCopy(wire, mp)
			} else {
				err = restored.UnmarshalBinary(wire)
			}
			require.NoError(t, err)
			require.Equal(t, typ, *restored.GetType())
			require.Equal(t, "😀", restored.GetStringAt(0))
			restored.Free(mp)
		}
		var legacy bytes.Buffer
		require.NoError(t, src.MarshalBinaryWithBufferV1(&legacy))
		restored := NewVec(types.T_any.ToType(), mp)
		require.NoError(t, restored.UnmarshalBinaryV1(legacy.Bytes()))
		require.Equal(t, typ, *restored.GetType())
		restored.Free(mp)
		proto.Type.CollationVersion = 257
		bad, err := ProtoVectorToVector(proto, mp)
		require.Error(t, err)
		require.Nil(t, bad)
		// Byte 3 within the native Type is its revision, after the vector class.
		wire[1+3] = 2
		for _, decode := range []func(*Vector) error{
			func(v *Vector) error { return v.UnmarshalBinary(wire) },
			func(v *Vector) error { return v.UnmarshalBinaryTrusted(wire) },
			func(v *Vector) error { return v.UnmarshalBinaryWithCopy(wire, mp) },
			func(v *Vector) error { legacy.Bytes()[1+3] = 2; return v.UnmarshalBinaryV1(legacy.Bytes()) },
		} {
			v := NewVec(types.T_any.ToType(), mp)
			require.Error(t, decode(v))
			v.Free(mp)
		}
		src.Free(mp)
	}
	require.Zero(t, mp.CurrNB())
}
