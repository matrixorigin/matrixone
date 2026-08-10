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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
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

func TestAppendCheckpointRollback(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_varchar.ToType())
	defer vec.Free(mp)
	first := strings.Repeat("a", 64)
	require.NoError(t, AppendBytes(vec, []byte(first), false, mp))
	vec.GetGrouping().Set(0)
	vec.SetSorted(true)
	checkpoint := vec.MakeAppendCheckpoint()
	vec.SetPrepareParamKind(PrepareParamFloat)

	require.NoError(t, AppendBytes(vec, []byte(strings.Repeat("b", 96)), false, mp))
	vec.GetNulls().Set(1)
	vec.GetGrouping().Set(1)
	// Grouping publication can precede a failed varlen copy and therefore can
	// extend beyond the length reached by the copy itself.
	vec.GetGrouping().Set(2)
	vec.SetSorted(false)
	vec.RollbackAppend(checkpoint, 2)

	require.Equal(t, 1, vec.Length())
	require.Equal(t, []string{first}, InefficientMustStrCol(vec))
	require.False(t, vec.GetNulls().Contains(1))
	require.True(t, vec.GetGrouping().Contains(0))
	require.False(t, vec.GetGrouping().Contains(1))
	require.Equal(t, PrepareParamNone, vec.GetPrepareParamKind(),
		"the checkpoint predates the explicit provenance assignment")
	require.False(t, vec.GetGrouping().Contains(2))
	require.True(t, vec.GetSorted())
}

func TestCapacityForUntypedNull(t *testing.T) {
	vec := NewVec(types.T_any.ToType())
	require.Equal(t, 0, vec.Capacity())
}

func TestDupOffHeap(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_varchar.ToType())
	require.NoError(t, AppendBytesList(vec, [][]byte{[]byte("a"), []byte("longer value")}, nil, mp))

	dup, err := vec.DupOffHeap(mp)
	require.NoError(t, err)
	require.True(t, dup.offHeap)
	require.Equal(t, vec.Length(), dup.Length())
	require.Equal(t, vec.GetBytesAt(0), dup.GetBytesAt(0))
	require.Equal(t, vec.GetBytesAt(1), dup.GetBytesAt(1))

	dup.Free(mp)
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
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

func TestAppendBytesNullUsesVectorPhysicalType(t *testing.T) {
	for _, typ := range []types.Type{
		types.T_bool.ToType(),
		types.T_decimal128.ToType(),
		types.T_varchar.ToType(),
	} {
		t.Run(typ.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec := NewVec(typ)

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
	{ // fixed grouping provenance
		dst := NewVec(types.T_int32.ToType())
		src := NewVec(types.T_int32.ToType())
		require.NoError(t, AppendFixedList(dst, []int32{0, 0}, nil, mp))
		require.NoError(t, AppendFixedList(src, []int32{1, 2}, nil, mp))
		src.GetGrouping().Add(0)
		dst.GetGrouping().Add(1)
		require.NoError(t, dst.Copy(src, 0, 0, mp))
		require.NoError(t, dst.Copy(src, 1, 1, mp))
		require.True(t, dst.GetGrouping().Contains(0))
		require.False(t, dst.GetGrouping().Contains(1))
		dst.Free(mp)
		src.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
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
	{ // string null with stale varlena metadata
		v := NewVec(types.T_varchar.ToType())
		err := AppendBytes(v, []byte("seed"), false, mp)
		require.NoError(t, err)
		w := NewVec(types.T_varchar.ToType())
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
		v := NewVec(types.T_array_float64.ToType())
		err := AppendArray[float64](v, []float64{1, 2}, false, mp)
		require.NoError(t, err)
		w := NewVec(types.T_array_float64.ToType())
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
	v6 := NewOffHeapVecWithType(types.T_varchar.ToType())
	defer v6.Free(mp)
	require.NoError(t, v5.CloneWindowTo(v6, 3, 5, mp))
	require.True(t, v6.IsConst())
	require.Equal(t, 2, v6.Length())
	require.Equal(t, payload, v6.GetBytesAt(0))
	require.Equal(t, payload, v6.GetBytesAt(1))
	require.Equal(t, 10, v5.Length(), "cloning must not mutate the source")
	require.Equal(t, payload, v5.GetBytesAt(0))
}

func TestBinaryStringMetadataSurvivesPublicCopies(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(source, []byte{0xe4, 0xbd, 0xa0, 0xff}, false, mp))
	source.SetIsBinaryString(true)
	t.Cleanup(func() {
		source.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})

	dup, err := source.Dup(mp)
	require.NoError(t, err)
	require.True(t, dup.GetIsBinaryString())
	dup.Free(mp)

	window, err := source.Window(0, 1)
	require.NoError(t, err)
	require.True(t, window.GetIsBinaryString())
	window.Free(mp)

	cloneWindow, err := source.CloneWindow(0, 1, mp)
	require.NoError(t, err)
	require.True(t, cloneWindow.GetIsBinaryString())
	cloneWindow.Free(mp)

	cloneTo := NewVec(types.T_text.ToType())
	require.NoError(t, source.CloneWindowTo(cloneTo, 0, 1, mp))
	require.True(t, cloneTo.GetIsBinaryString())
	cloneTo.Free(mp)

	compact, err := source.CloneToFlatCompact(mp)
	require.NoError(t, err)
	require.True(t, compact.GetIsBinaryString())
	compact.Free(mp)
}

func TestMixedBinaryStringMetadataSurvivesMaterialization(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_text.ToType())
	for _, value := range []string{"a", "你", "b"} {
		require.NoError(t, AppendBytes(source, []byte(value), false, mp))
	}
	source.SetIsBinaryStringAt(0, true)
	source.SetIsBinaryStringAt(2, true)
	t.Cleanup(func() {
		source.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})

	assertRows := func(t *testing.T, vec *Vector, want []bool) {
		t.Helper()
		for row, expected := range want {
			require.Equal(t, expected, vec.GetIsBinaryStringAt(row), "row %d", row)
		}
	}
	assertRows(t, source, []bool{true, false, true})

	dup, err := source.Dup(mp)
	require.NoError(t, err)
	assertRows(t, dup, []bool{true, false, true})
	dup.Free(mp)

	window, err := source.Window(1, 3)
	require.NoError(t, err)
	assertRows(t, window, []bool{false, true})
	window.Free(mp)

	cloneWindow, err := source.CloneWindow(1, 3, mp)
	require.NoError(t, err)
	assertRows(t, cloneWindow, []bool{false, true})
	cloneWindow.Free(mp)

	cloneTo := NewVec(types.T_text.ToType())
	require.NoError(t, source.CloneWindowTo(cloneTo, 1, 3, mp))
	assertRows(t, cloneTo, []bool{false, true})
	cloneTo.Free(mp)

	destination := NewVec(types.T_text.ToType())
	require.NoError(t, destination.UnionBatch(source, 0, source.Length(), nil, mp))
	assertRows(t, destination, []bool{true, false, true})
	destination.Free(mp)

	shrunk, err := source.Dup(mp)
	require.NoError(t, err)
	shrunk.Shrink([]int64{1, 2}, false)
	assertRows(t, shrunk, []bool{false, true})
	shrunk.Free(mp)

	shuffled, err := source.Dup(mp)
	require.NoError(t, err)
	require.NoError(t, shuffled.Shuffle([]int64{2, 1, 0}, mp))
	assertRows(t, shuffled, []bool{true, false, true})
	shuffled.Free(mp)

	copied, err := source.Dup(mp)
	require.NoError(t, err)
	require.NoError(t, copied.Copy(source, 0, 1, mp))
	assertRows(t, copied, []bool{false, false, true})
	copied.Free(mp)

	staticBinary := NewVec(types.T_varbinary.ToType())
	require.NoError(t, AppendBytes(staticBinary, nil, true, mp))
	require.False(t, staticBinary.GetIsBinaryStringAt(0), "NULL rows have no selected-value provenance")
	staticBinary.Free(mp)

	nullable := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(nullable, []byte("a"), false, mp))
	require.NoError(t, AppendBytes(nullable, nil, true, mp))
	require.NoError(t, AppendBytes(nullable, []byte("b"), false, mp))
	nullable.SetIsBinaryString(true)
	nullable.SetIsBinaryStringAt(0, false)
	nullable.SetIsBinaryStringAt(2, false)
	require.False(t, nullable.GetIsBinaryString())
	require.False(t, nullable.HasBinaryStringRows())
	nullable.Free(mp)

	rollback, err := source.Dup(mp)
	require.NoError(t, err)
	checkpoint := rollback.MakeAppendCheckpoint()
	require.NoError(t, AppendBytes(rollback, []byte("c"), false, mp))
	rollback.SetIsBinaryStringAt(3, true)
	rollback.RollbackAppend(checkpoint, 1)
	assertRows(t, rollback, []bool{true, false, true})
	require.Equal(t, 3, rollback.Length())
	rollback.Free(mp)
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
	size, err := v.MarshalBinarySize()
	require.NoError(t, err)
	require.Equal(t, len(data), size)
	var streamed bytes.Buffer
	require.NoError(t, v.MarshalBinaryTo(&streamed))
	require.Equal(t, data, streamed.Bytes())
	require.ErrorIs(t, v.MarshalBinaryTo(shortVectorMarshalWriter{}), io.ErrShortWrite)
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
	invalidType := NewVec(typ)
	_, err = invalidType.MarshalBinarySize()
	require.Error(t, err)

	shortData := NewVec(types.T_int64.ToType())
	shortData.SetLength(1)
	_, err = shortData.MarshalBinarySize()
	require.Error(t, err)
}

func TestUnmarshalBinaryAcceptsNullBitmapCoveragePastLength(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType())
	require.NoError(t, AppendFixed(source, int64(0), false, mp))
	source.GetNulls().AddRange(0, 1)

	data, err := source.MarshalBinary()
	require.NoError(t, err)

	target := NewVecFromReuse()
	require.NoError(t, target.UnmarshalBinary(data))
	require.Equal(t, 1, target.Length())
	require.True(t, target.IsNull(0))

	source.Free(mp)
	target.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestUnmarshalBinaryAcceptsStaleVarlenaInNullRow(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType())
	require.NoError(t, AppendBytes(source, []byte("value longer than inline storage"), false, mp))
	source.SetNull(0)
	source.ResetArea()

	data, err := source.MarshalBinary()
	require.NoError(t, err)

	target := NewVecFromReuse()
	require.NoError(t, target.UnmarshalBinary(data))
	require.Equal(t, 1, target.Length())
	require.True(t, target.IsNull(0))

	source.Free(mp)
	target.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestUnmarshalBinaryRejectsOverflowingNullBitmapLength(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType())
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

	target := NewVecFromReuse()
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
			source := NewVec(types.New(types.T_array_float32, 10, 0))
			require.NoError(t, AppendArray(source, test.values, false, mp))
			data, err := source.MarshalBinary()
			require.NoError(t, err)
			source.Free(mp)

			// The array payload remains in bounds, but cannot be decoded as a
			// []float32. Cover both Varlena storage forms.
			corrupted := append([]byte(nil), data...)
			varlenOffset := 1 + types.TSize + 4 + 4
			test.corruptLen(corrupted, varlenOffset)

			target := NewVecFromReuse()
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
			source := NewVec(types.Type{Oid: oid})
			data, err := source.MarshalBinary()
			require.NoError(t, err)

			target := NewVecFromReuse()
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
	source := NewVec(types.T_varchar.ToType())
	require.NoError(t, AppendBytes(source, []byte("value longer than inline storage"), false, mp))
	data, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(mp)

	for end := len(data) - 1; end >= 0; end-- {
		target := NewVecFromReuse()
		var unmarshalErr error
		require.NotPanics(t, func() {
			unmarshalErr = target.UnmarshalBinaryTrusted(data[:end])
		}, "truncation at %d bytes", end)
		require.Error(t, unmarshalErr, "truncation at %d bytes", end)
	}
}

func TestUnmarshalBinaryTrustedRequiresPriorSemanticValidation(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType())
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

	checked := NewVecFromReuse()
	require.Error(t, checked.UnmarshalBinary(corrupted))

	trusted := NewVecFromReuse()
	require.NoError(t, trusted.UnmarshalBinaryTrusted(corrupted))
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

	v = NewVec(types.T_geometry.ToType())
	err = AppendBytesList(v, [][]byte{[]byte("POINT(2 2)"), []byte("POINT(1 1)"), []byte("POINT(1 1)")}, nil, mp)
	require.NoError(t, err)
	v.InplaceSortAndCompact()
	require.Equal(t, 2, v.length)
	require.Equal(t, [][]byte{[]byte("POINT(1 1)"), []byte("POINT(2 2)")}, InefficientMustBytesCol(v))
	v.Free(mp)
}

func TestGeometryVarlenPlumbing(t *testing.T) {
	mp := mpool.MustNewZero()

	src := NewVec(types.T_geometry.ToType())
	err := AppendBytesList(src, [][]byte{
		[]byte("POINT(1 1)"),
		[]byte("POINT(2 2)"),
		[]byte("POINT(3 3)"),
		[]byte("POINT(4 4)"),
	}, nil, mp)
	require.NoError(t, err)

	dst := NewVec(types.T_geometry.ToType())
	err = dst.UnionOne(src, 1, mp)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("POINT(2 2)")}, InefficientMustBytesCol(dst))

	dst.Shrink([]int64{0}, false)
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
	unionDst := NewVec(types.T_geometry.ToType())
	err = unionAll(unionDst, src)
	require.NoError(t, err)
	require.Equal(t, [][]byte{
		[]byte("POINT(1 1)"),
		[]byte("POINT(2 2)"),
		[]byte("POINT(3 3)"),
		[]byte("POINT(4 4)"),
	}, InefficientMustBytesCol(unionDst))

	minmax := NewVec(types.T_geometry.ToType())
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
	minmax.InplaceSort()
	require.Equal(t, [][]byte{
		[]byte("POINT(1 1)"),
		[]byte("POINT(2 2)"),
		[]byte("POINT(3 3)"),
	}, InefficientMustBytesCol(minmax))

	var bm bitmap.Bitmap
	bm.InitWithSize(2)
	bm.AddMany([]uint64{0, 1})
	maskVec := NewVec(types.T_geometry.ToType())
	err = AppendBytesList(maskVec, [][]byte{
		[]byte("POINT(1 1)"),
		[]byte("POINT(2 2)"),
		[]byte("POINT(3 3)"),
		[]byte("POINT(4 4)"),
	}, nil, mp)
	require.NoError(t, err)
	maskVec.ShrinkByMask(&bm, false, 1)
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
	source := NewVec(types.T_int8.ToType())
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

func TestResetWithSameTypeResetsClass(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	vec := NewVec(types.T_varchar.ToType())
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

	fixed := NewVec(types.T_int64.ToType())
	for _, value := range []int64{3, 1, 2} {
		require.NoError(t, AppendFixed(fixed, value, false, mp))
	}
	fixed.InplaceSortAndCompact()
	require.Equal(t, []int64{1, 2, 3}, MustFixedColNoTypeCheck[int64](fixed))
	require.True(t, fixed.GetSorted())
	fixed.Free(mp)

	varlen := NewVec(types.T_varchar.ToType())
	for _, value := range []string{"c", "a", "b"} {
		require.NoError(t, AppendBytes(varlen, []byte(value), false, mp))
	}
	varlen.InplaceSortAndCompact()
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, InefficientMustBytesCol(varlen))
	require.True(t, varlen.GetSorted())
	varlen.Free(mp)

	unsupported := NewVec(types.T_any.ToType())
	unsupported.InplaceSortAndCompact()
	require.False(t, unsupported.GetSorted())
}

func TestVarlenaAreaDisjointLifecycle(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	typ := types.T_varchar.ToType()
	payload := []byte(strings.Repeat("x", 128))

	flat := NewVec(typ)
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
	flat.Shrink([]int64{0, 0}, false)
	require.False(t, flat.VarlenaAreaIsDisjoint(),
		"selection can duplicate a descriptor")
	flat.Free(mp)

	constant, err := NewConstBytes(typ, payload, 2, mp)
	require.NoError(t, err)
	shared := NewVec(typ)
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

func TestVarlenaAreaDisjointAppendFailureFailsClosed(t *testing.T) {
	mp, err := mpool.NewMPool("varlena-disjoint-failure", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	vec := NewVec(types.T_varchar.ToType())
	vec.SetOffHeap(true)
	err = AppendBytesList(
		vec,
		[][]byte{make([]byte, 128), make([]byte, 2<<20)},
		nil,
		mp,
	)
	require.Error(t, err)
	require.False(t, vec.VarlenaAreaIsDisjoint(),
		"a partially initialized logical range must never retain the fast proof")

	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestPrepareParamKindValueLifecycle(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	source, err := NewConstBytes(types.T_text.ToType(), []byte("5"), 2, mp)
	require.NoError(t, err)
	source.SetPrepareParamKind(PrepareParamFloat)
	defer source.Free(mp)

	duplicate, err := source.Dup(mp)
	require.NoError(t, err)
	require.Equal(t, PrepareParamFloat, duplicate.GetPrepareParamKind())
	duplicate.Free(mp)

	window, err := source.Window(0, 1)
	require.NoError(t, err)
	require.Equal(t, PrepareParamFloat, window.GetPrepareParamKind())
	window.Free(mp)

	clone, err := source.CloneWindow(0, 1, mp)
	require.NoError(t, err)
	require.Equal(t, PrepareParamFloat, clone.GetPrepareParamKind())
	clone.ResetWithSameType()
	require.Equal(t, PrepareParamNone, clone.GetPrepareParamKind())
	clone.SetPrepareParamKind(PrepareParamDecimal)
	clone.Reset(types.T_varchar.ToType())
	require.Equal(t, PrepareParamNone, clone.GetPrepareParamKind())
	clone.SetPrepareParamKind(PrepareParamInteger)
	blobType := types.T_blob.ToType()
	clone.ResetWithNewType(&blobType)
	require.Equal(t, PrepareParamNone, clone.GetPrepareParamKind())
	clone.SetPrepareParamKind(PrepareParamBoolean)
	clone.CleanOnlyData()
	require.Equal(t, PrepareParamNone, clone.GetPrepareParamKind())
	clone.Free(mp)
}

func TestPrepareParamKindPropagationAcrossAppendAndClone(t *testing.T) {
	mp := mpool.MustNewZero()
	numeric := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(numeric, []byte("5.5"), false, mp))
	numeric.SetPrepareParamKind(PrepareParamFloat)
	defer numeric.Free(mp)

	for name, appendFn := range map[string]func(*Vector) error{
		"one":       func(dst *Vector) error { return dst.UnionOne(numeric, 0, mp) },
		"multi":     func(dst *Vector) error { return dst.UnionMulti(numeric, 0, 2, mp) },
		"selection": func(dst *Vector) error { return dst.Union(numeric, []int64{0}, mp) },
		"batch":     func(dst *Vector) error { return dst.UnionBatch(numeric, 0, 1, nil, mp) },
	} {
		t.Run(name, func(t *testing.T) {
			dst := NewVec(types.T_text.ToType())
			defer dst.Free(mp)
			require.NoError(t, appendFn(dst))
			require.Equal(t, PrepareParamFloat, dst.GetPrepareParamKind())
		})
	}

	ordinary := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(ordinary, []byte("text"), false, mp))
	defer ordinary.Free(mp)
	dst := NewVec(types.T_text.ToType())
	require.NoError(t, dst.UnionBatch(numeric, 0, 1, nil, mp))
	require.NoError(t, dst.UnionBatch(ordinary, 0, 1, nil, mp))
	require.Equal(t, PrepareParamNone, dst.GetPrepareParamKind(),
		"mixed prepared and ordinary sources must be conservative")
	dst.Free(mp)

	clone, err := numeric.CloneToFlatCompact(mp)
	require.NoError(t, err)
	require.Equal(t, PrepareParamFloat, clone.GetPrepareParamKind())
	clone.Free(mp)
	dup, err := numeric.Dup(mp)
	require.NoError(t, err)
	require.Equal(t, PrepareParamFloat, dup.GetPrepareParamKind())
	dup.Free(mp)
	window, err := numeric.Window(0, 1)
	require.NoError(t, err)
	require.Equal(t, PrepareParamFloat, window.GetPrepareParamKind())
	window.Free(mp)
}

func TestPrepareParamKindEmptyReuseCopyAndRollback(t *testing.T) {
	mp := mpool.MustNewZero()
	decimal := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(decimal, []byte("5.9"), false, mp))
	decimal.SetPrepareParamKind(PrepareParamDecimal)
	defer decimal.Free(mp)

	for name, makeDestination := range map[string]func() *Vector{
		"empty": func() *Vector { return NewVec(types.T_text.ToType()) },
		"all-null": func() *Vector {
			v := NewVec(types.T_text.ToType())
			require.NoError(t, AppendBytes(v, nil, true, mp))
			v.SetPrepareParamKind(PrepareParamFloat)
			return v
		},
	} {
		t.Run(name, func(t *testing.T) {
			dst := makeDestination()
			defer dst.Free(mp)
			require.NoError(t, dst.UnionOne(decimal, 0, mp))
			require.Equal(t, PrepareParamDecimal, dst.GetPrepareParamKind())
		})
	}

	ordinary := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(ordinary, []byte("ordinary"), false, mp))
	defer ordinary.Free(mp)
	require.NoError(t, ordinary.Copy(decimal, 0, 0, mp))
	require.Equal(t, PrepareParamNone, ordinary.GetPrepareParamKind())

	union := NewVec(types.T_text.ToType())
	require.NoError(t, GetUnionAllFunction(types.T_text.ToType(), mp)(union, decimal))
	require.Equal(t, PrepareParamDecimal, union.GetPrepareParamKind())
	union.Free(mp)

	rollback := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(rollback, []byte("5.5"), false, mp))
	rollback.SetPrepareParamKind(PrepareParamFloat)
	defer rollback.Free(mp)
	checkpoint := rollback.MakeAppendCheckpoint()
	require.NoError(t, rollback.UnionOne(decimal, 0, mp))
	require.Equal(t, PrepareParamNone, rollback.GetPrepareParamKind())
	rollback.RollbackAppend(checkpoint, 1)
	require.Equal(t, PrepareParamFloat, rollback.GetPrepareParamKind(),
		"rollback must restore the mixed-source provenance")
}

func TestCopyOrdinaryPrepareParamKindKeepsScalarMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType())
	destination := NewVec(types.T_int64.ToType())
	t.Cleanup(func() {
		destination.Free(mp)
		source.Free(mp)
		if got := mp.CurrNB(); got != 0 {
			t.Errorf("mpool retains %d bytes after vector cleanup", got)
		}
	})

	require.NoError(t, AppendFixed(source, int64(1), false, mp))
	require.NoError(t, AppendFixedList(destination, make([]int64, 100), nil, mp))
	require.False(t, source.HasPrepareParamKind())
	require.False(t, destination.HasPrepareParamKind())

	before := mp.CurrNB()
	require.NoError(t, destination.Copy(source, 50, 0, mp))
	require.Equal(t, int64(0), mp.CurrNB()-before,
		"copying ordinary metadata must not materialize a row sidecar")
	require.True(t, destination.HasPrepareParamKind())
	require.Equal(t, PrepareParamNone, destination.GetPrepareParamKind())
	require.Nil(t, destination.GetPrepareParamKinds())
	require.Equal(t, PrepareParamNone, destination.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamNone, destination.GetPrepareParamKindAt(50))
}

func TestCopyPrepareParamKindMaterializesOnlyDivergence(t *testing.T) {
	newVector := func(t *testing.T, mp *mpool.MPool, rows int) *Vector {
		t.Helper()
		vec := NewVec(types.T_int64.ToType())
		require.NoError(t, AppendFixedList(vec, make([]int64, rows), nil, mp))
		return vec
	}

	t.Run("scalar none and non-none", func(t *testing.T) {
		mp := mpool.MustNewZero()
		source := newVector(t, mp, 1)
		destination := newVector(t, mp, 2)
		t.Cleanup(func() {
			destination.Free(mp)
			source.Free(mp)
		})
		source.SetPrepareParamKind(PrepareParamFloat)
		destination.SetPrepareParamKind(PrepareParamNone)

		require.NoError(t, destination.Copy(source, 1, 0, mp))
		require.Equal(t, []PrepareParamKind{
			PrepareParamNone,
			PrepareParamFloat,
		}, destination.GetPrepareParamKinds())
	})

	t.Run("scalar non-none and none", func(t *testing.T) {
		mp := mpool.MustNewZero()
		source := newVector(t, mp, 1)
		destination := newVector(t, mp, 2)
		t.Cleanup(func() {
			destination.Free(mp)
			source.Free(mp)
		})
		destination.SetPrepareParamKind(PrepareParamFloat)

		require.NoError(t, destination.Copy(source, 1, 0, mp))
		require.Equal(t, []PrepareParamKind{
			PrepareParamFloat,
			PrepareParamNone,
		}, destination.GetPrepareParamKinds())
	})

	t.Run("existing sidecar", func(t *testing.T) {
		mp := mpool.MustNewZero()
		source := newVector(t, mp, 1)
		destination := newVector(t, mp, 3)
		t.Cleanup(func() {
			destination.Free(mp)
			source.Free(mp)
		})
		source.SetPrepareParamKind(PrepareParamBoolean)
		require.NoError(t, destination.SetPrepareParamKindsWithMP([]PrepareParamKind{
			PrepareParamInteger,
			PrepareParamFloat,
			PrepareParamDecimal,
		}, mp))
		before := mp.CurrNB()

		require.NoError(t, destination.Copy(source, 1, 0, mp))
		require.Equal(t, before, mp.CurrNB())
		require.Equal(t, []PrepareParamKind{
			PrepareParamInteger,
			PrepareParamBoolean,
			PrepareParamDecimal,
		}, destination.GetPrepareParamKinds())
	})
}

func TestPrepareParamKindPerRowMaterialization(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(source, []byte("5"), false, mp))
	require.NoError(t, AppendBytes(source, []byte("5"), false, mp))
	source.SetPrepareParamKinds([]PrepareParamKind{PrepareParamInteger, PrepareParamNone})
	require.Equal(t, PrepareParamNone, source.GetPrepareParamKind())
	require.Equal(t, PrepareParamInteger, source.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamNone, source.GetPrepareParamKindAt(1))

	dst := NewVec(types.T_text.ToType())
	require.NoError(t, dst.Union(source, []int64{1, 0}, mp))
	require.Equal(t, PrepareParamNone, dst.GetPrepareParamKind())
	require.Equal(t, PrepareParamNone, dst.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamInteger, dst.GetPrepareParamKindAt(1))

	clone, err := dst.CloneToFlatCompact(mp)
	require.NoError(t, err)
	require.Equal(t, PrepareParamNone, clone.GetPrepareParamKind())
	require.Equal(t, PrepareParamNone, clone.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamInteger, clone.GetPrepareParamKindAt(1))
	clone.Free(mp)

	resized := NewVec(types.T_text.ToType())
	require.NoError(t, resized.PreExtend(3, mp))
	resized.SetLength(3)
	resized.SetAllNulls(3)
	require.NoError(t, resized.Copy(source, 2, 0, mp))
	require.NoError(t, resized.Copy(source, 0, 1, mp))
	require.Equal(t, PrepareParamInteger, resized.GetPrepareParamKindAt(2))
	require.Equal(t, PrepareParamNone, resized.GetPrepareParamKindAt(0))
	resized.Free(mp)
	dst.Free(mp)
	source.Free(mp)
}

func makePrepareParamKindReaderVector(t *testing.T, mp *mpool.MPool, rows int) *Vector {
	t.Helper()
	vec := NewVec(types.T_int8.ToType())
	values := make([]int8, rows)
	for i := range values {
		values[i] = int8(i + 1)
	}
	require.NoError(t, AppendFixedList(vec, values, nil, mp))
	return vec
}

func kindsToBytes(kinds []PrepareParamKind) []byte {
	data := make([]byte, len(kinds))
	for i, kind := range kinds {
		data[i] = byte(kind)
	}
	return data
}

// unexpectedEOFReader models a transport that has delivered a partial
// payload and reports the truncation on the next read. bytes.Reader returns
// io.EOF when that next read has no bytes, which is a distinct failure mode.
type unexpectedEOFReader struct {
	reader *bytes.Reader
}

func (r *unexpectedEOFReader) Read(p []byte) (int, error) {
	if r.reader.Len() == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	return r.reader.Read(p)
}

func TestSetPrepareParamKindsFromReaderCollapsesUniformAndNullRows(t *testing.T) {
	tests := []struct {
		name        string
		kinds       []PrepareParamKind
		nullRows    []uint64
		wantKind    PrepareParamKind
		wantSeen    bool
		wantSidecar bool
	}{
		{
			name:        "mixed",
			kinds:       []PrepareParamKind{PrepareParamInteger, PrepareParamFloat, PrepareParamNone},
			wantKind:    PrepareParamNone,
			wantSeen:    true,
			wantSidecar: true,
		},
		{
			name:     "uniform",
			kinds:    []PrepareParamKind{PrepareParamDecimal, PrepareParamDecimal, PrepareParamDecimal},
			wantKind: PrepareParamDecimal,
			wantSeen: true,
		},
		{
			name:     "all-null",
			kinds:    []PrepareParamKind{PrepareParamBoolean, PrepareParamInteger, PrepareParamFloat},
			nullRows: []uint64{0, 1, 2},
			wantKind: PrepareParamNone,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec := makePrepareParamKindReaderVector(t, mp, len(tc.kinds))
			defer vec.Free(mp)
			for _, row := range tc.nullRows {
				vec.GetNulls().Add(row)
			}
			before := mp.CurrNB()

			err := vec.SetPrepareParamKindsFromReader(
				bytes.NewReader(kindsToBytes(tc.kinds)), len(tc.kinds), mp)
			require.NoError(t, err)
			require.Equal(t, tc.wantKind, vec.GetPrepareParamKind())
			require.Equal(t, tc.wantSeen, vec.prepareParamKindSeen)
			if tc.wantSidecar {
				require.Equal(t, tc.kinds, vec.GetPrepareParamKinds())
				require.Greater(t, mp.CurrNB(), before)
			} else {
				require.Nil(t, vec.GetPrepareParamKinds())
				require.Equal(t, before, mp.CurrNB(),
					"uniform/all-null metadata must release its temporary sidecar")
			}
		})
	}
}

func TestSetPrepareParamKindsAndBinaryStringFromReader(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := makePrepareParamKindReaderVector(t, mp, 3)
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	require.NoError(t, vec.SetPrepareParamKindsAndBinaryStringFromReader(
		bytes.NewReader([]byte{
			byte(PrepareParamInteger) | 0x80,
			byte(PrepareParamFloat),
			byte(PrepareParamNone) | 0x80,
		}),
		3,
		mp,
		0x80,
	))
	require.Equal(t, PrepareParamInteger, vec.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamFloat, vec.GetPrepareParamKindAt(1))
	require.Equal(t, PrepareParamNone, vec.GetPrepareParamKindAt(2))
	require.True(t, vec.GetIsBinaryStringAt(0))
	require.False(t, vec.GetIsBinaryStringAt(1))
	require.True(t, vec.GetIsBinaryStringAt(2))

	before := mp.CurrNB()
	err := vec.SetPrepareParamKindsAndBinaryStringFromReader(
		bytes.NewReader([]byte{byte(PrepareParamDecimal) | 0x80}),
		3,
		mp,
		0x80,
	)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, before, mp.CurrNB(), "a failed generation must release its temporary MPool slice")
	// The last complete generation remains available after a truncated frame.
	require.Equal(t, PrepareParamFloat, vec.GetPrepareParamKindAt(1))
	require.True(t, vec.GetIsBinaryStringAt(2))
}

func TestSetPrepareParamKindsFromReaderErrorsReleaseTemporarySidecar(t *testing.T) {
	tests := []struct {
		name       string
		reader     io.Reader
		rowCount   int
		wantErr    error
		wantString string
	}{
		{
			name:     "nil reader",
			rowCount: 2,
			wantErr:  io.ErrClosedPipe,
		},
		{
			name:       "row count mismatch",
			reader:     bytes.NewReader([]byte{byte(PrepareParamFloat)}),
			rowCount:   1,
			wantString: "row count 1 does not match vector length 2",
		},
		{
			name:     "no-byte EOF",
			reader:   bytes.NewReader(nil),
			rowCount: 2,
			wantErr:  io.EOF,
		},
		{
			name: "partial-read unexpected EOF",
			reader: &unexpectedEOFReader{
				reader: bytes.NewReader([]byte{byte(PrepareParamFloat)}),
			},
			rowCount: 2,
			wantErr:  io.ErrUnexpectedEOF,
		},
		{
			name:       "invalid kind",
			reader:     bytes.NewReader([]byte{byte(PrepareParamFloat), 0xff}),
			rowCount:   2,
			wantString: "invalid prepared parameter row kind 255",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec := makePrepareParamKindReaderVector(t, mp, 2)
			before := mp.CurrNB()
			err := vec.SetPrepareParamKindsFromReader(tc.reader, tc.rowCount, mp)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.ErrorContains(t, err, tc.wantString)
			}
			require.Equal(t, before, mp.CurrNB(),
				"failed metadata generation must release its temporary allocation")
			require.Nil(t, vec.GetPrepareParamKinds())
			require.Equal(t, PrepareParamNone, vec.GetPrepareParamKind())
			vec.Free(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestSetPrepareParamKindsFromReaderFailedGenerationCanReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := makePrepareParamKindReaderVector(t, mp, 2)
	defer vec.Free(mp)
	before := mp.CurrNB()

	err := vec.SetPrepareParamKindsFromReader(
		&unexpectedEOFReader{
			reader: bytes.NewReader([]byte{byte(PrepareParamInteger)}),
		}, 2, mp)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, before, mp.CurrNB())

	require.NoError(t, vec.SetPrepareParamKindsFromReader(
		bytes.NewReader(kindsToBytes([]PrepareParamKind{PrepareParamInteger, PrepareParamFloat})),
		2, mp))
	require.Equal(t, []PrepareParamKind{PrepareParamInteger, PrepareParamFloat}, vec.GetPrepareParamKinds())
	require.True(t, vec.prepareParamKindSeen)

	require.NoError(t, vec.SetPrepareParamKindsFromReader(
		bytes.NewReader(kindsToBytes([]PrepareParamKind{PrepareParamDecimal, PrepareParamDecimal})),
		2, mp))
	require.Nil(t, vec.GetPrepareParamKinds())
	require.Equal(t, PrepareParamDecimal, vec.GetPrepareParamKind())
	require.Equal(t, before, mp.CurrNB(),
		"reuse must release the failed generation and collapsed sidecar")
}

func TestPrepareParamKindReordersWithoutSidecarAllocation(t *testing.T) {
	mp := mpool.MustNewZero()
	makeVector := func() *Vector {
		v := NewVec(types.T_text.ToType())
		for _, value := range []string{"1", "2", "3", "4"} {
			require.NoError(t, AppendBytes(v, []byte(value), false, mp))
		}
		require.NoError(t, v.SetPrepareParamKindsWithMP([]PrepareParamKind{
			PrepareParamInteger,
			PrepareParamFloat,
			PrepareParamNone,
			PrepareParamDecimal,
		}, mp))
		return v
	}

	vec := makeVector()
	before := mp.CurrNB()
	vec.Shrink([]int64{1, 3}, false)
	require.Equal(t, before, mp.CurrNB(), "ordered shrink must reuse the sidecar")
	require.Equal(t, PrepareParamFloat, vec.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamDecimal, vec.GetPrepareParamKindAt(1))
	vec.Free(mp)

	vec = makeVector()
	before = mp.CurrNB()
	var mask bitmap.Bitmap
	mask.InitWithSize(2)
	mask.AddMany([]uint64{0, 1})
	vec.ShrinkByMask(&mask, false, 1)
	require.Equal(t, before, mp.CurrNB(), "mask shrink must reuse the sidecar")
	require.Equal(t, PrepareParamFloat, vec.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamNone, vec.GetPrepareParamKindAt(1))
	vec.Free(mp)

	vec = makeVector()
	before = mp.CurrNB()
	require.NoError(t, vec.Shuffle([]int64{3, 1, 3}, mp))
	require.Equal(t, before, mp.CurrNB(), "shuffle must not allocate a replacement sidecar")
	require.Equal(t, PrepareParamDecimal, vec.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamFloat, vec.GetPrepareParamKindAt(1))
	require.Equal(t, PrepareParamDecimal, vec.GetPrepareParamKindAt(2))
	var scratch []byte
	require.NoError(t, vec.ShuffleWithBuf([]int64{1, 0, 1}, mp, &scratch))
	require.Equal(t, []PrepareParamKind{
		PrepareParamFloat,
		PrepareParamDecimal,
		PrepareParamFloat,
	}, []PrepareParamKind{
		vec.GetPrepareParamKindAt(0),
		vec.GetPrepareParamKindAt(1),
		vec.GetPrepareParamKindAt(2),
	})
	vec.Free(mp)

	vec = makeVector()
	require.NoError(t, vec.Shuffle([]int64{3, 1, 0, 3, 2}, mp))
	require.Equal(t, []PrepareParamKind{
		PrepareParamDecimal,
		PrepareParamFloat,
		PrepareParamInteger,
		PrepareParamDecimal,
		PrepareParamNone,
	}, vec.GetPrepareParamKinds())
	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestConstSetFunctionCopiesSelectedPrepareParamKind(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(source, []byte("5"), false, mp))
	require.NoError(t, AppendBytes(source, []byte("5"), false, mp))
	require.NoError(t, source.SetPrepareParamKindsWithMP(
		[]PrepareParamKind{PrepareParamInteger, PrepareParamFloat}, mp))
	destination := NewVec(types.T_text.ToType())
	set := GetConstSetFunction(types.T_text.ToType(), mp)
	require.NoError(t, set(destination, source, 1, 3))
	require.Equal(t, PrepareParamFloat, destination.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamFloat, destination.GetPrepareParamKindAt(2))

	nullSource := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(nullSource, nil, true, mp))
	require.NoError(t, set(destination, nullSource, 0, 2))
	require.False(t, destination.HasPrepareParamKind())

	destination.Free(mp)
	nullSource.Free(mp)
	source.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestPrepareParamKindCheckpointRollbackRetainsSidecarOwnership(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(vec, []byte("5"), false, mp))
	require.NoError(t, AppendBytes(vec, []byte("text"), false, mp))
	require.NoError(t, vec.SetPrepareParamKindsWithMP(
		[]PrepareParamKind{PrepareParamInteger, PrepareParamNone}, mp))
	before := mp.CurrNB()
	checkpoint := vec.MakeAppendCheckpoint()
	ordinary := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(ordinary, []byte("later"), false, mp))
	require.NoError(t, vec.UnionOne(ordinary, 0, mp))
	afterAppend := mp.CurrNB()
	vec.RollbackAppend(checkpoint, 1)
	require.Equal(t, PrepareParamInteger, vec.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamNone, vec.GetPrepareParamKindAt(1))
	require.GreaterOrEqual(t, afterAppend, before)
	require.Equal(t, afterAppend, mp.CurrNB(),
		"rollback should retain admitted sidecar capacity for reuse")
	ordinary.Free(mp)
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPrepareParamKindCheckpointDoesNotCopySidecar(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytesList(
		vec, [][]byte{[]byte("5"), []byte("text")}, nil, mp))
	require.NoError(t, vec.SetPrepareParamKindsWithMP(
		[]PrepareParamKind{PrepareParamInteger, PrepareParamNone}, mp))

	require.Zero(t, testing.AllocsPerRun(100, func() {
		_ = vec.MakeAppendCheckpoint()
	}))

	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestPrepareParamKindMetadataBoundaryLifecycle(t *testing.T) {
	var nilVec *Vector
	require.False(t, nilVec.HasPrepareParamKind())
	require.Equal(t, PrepareParamNone, nilVec.GetPrepareParamKindAt(0))
	require.NoError(t, nilVec.SetPrepareParamKindAtWithMP(0, PrepareParamInteger, nil))
	require.NoError(t, nilVec.CopyPrepareParamMetadataToWithMP(nil, nil))

	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int64.ToType())
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, AppendFixedList(vec, []int64{1, 2, 3}, nil, mp))

	// Invalid and empty inputs leave the existing scalar representation intact
	// while a length mismatch is rejected before touching metadata.
	vec.SetPrepareParamKind(PrepareParamFloat)
	require.ErrorContains(t, vec.SetPrepareParamKindsWithMP([]PrepareParamKind{PrepareParamInteger}, mp), "row count")
	require.NoError(t, vec.SetPrepareParamKindsWithMP(nil, mp))
	require.False(t, vec.HasPrepareParamKind())

	// Uniform and all-NULL rows stay on the scalar fast path; a real conflict
	// promotes exactly once to the owned sidecar.
	require.NoError(t, vec.SetPrepareParamKindsWithMP(
		[]PrepareParamKind{PrepareParamInteger, PrepareParamInteger, PrepareParamInteger}, mp))
	require.Equal(t, PrepareParamInteger, vec.GetPrepareParamKind())
	require.Nil(t, vec.GetPrepareParamKinds())
	for row := uint64(0); row < 3; row++ {
		vec.GetNulls().Add(row)
	}
	require.NoError(t, vec.SetPrepareParamKindsWithMP(
		[]PrepareParamKind{PrepareParamFloat, PrepareParamDecimal, PrepareParamBoolean}, mp))
	require.False(t, vec.HasPrepareParamKind())
	require.Nil(t, vec.GetPrepareParamKinds())
	vec.GetNulls().Clear()
	require.NoError(t, vec.SetPrepareParamKindsWithMP(
		[]PrepareParamKind{PrepareParamInteger, PrepareParamFloat, PrepareParamDecimal}, mp))
	require.Len(t, vec.GetPrepareParamKinds(), 3)

	// Sidecar resize exercises both in-capacity clearing and owner-preserving
	// growth. A NULL write clears one row and all-NULL resets the sidecar.
	vec.SetLength(2)
	require.Len(t, vec.GetPrepareParamKinds(), 2)
	vec.SetLength(6)
	require.Len(t, vec.GetPrepareParamKinds(), 6)
	vec.SetPrepareParamKindAt(-1, PrepareParamBoolean)
	vec.SetPrepareParamKindAt(99, PrepareParamBoolean)
	vec.GetNulls().Add(0)
	require.NoError(t, vec.SetPrepareParamKindAtWithMP(0, PrepareParamBoolean, mp))
	vec.SetAllNulls(vec.Length())
	require.False(t, vec.HasPrepareParamKind())
	require.Nil(t, vec.GetPrepareParamKinds())

	// Reader zero-row input and scalar row updates are no-op/reset boundaries.
	zero := NewVec(types.T_int8.ToType())
	require.NoError(t, zero.SetPrepareParamKindsFromReader(bytes.NewReader(nil), 0, mp))
	zero.Free(mp)
	vec.SetNulls(nil)
	vec.SetLength(3)
	vec.SetPrepareParamKindAt(0, PrepareParamDecimal)
	require.Equal(t, PrepareParamDecimal, vec.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamNone, vec.GetPrepareParamKindAt(-1))
	require.Equal(t, PrepareParamNone, vec.GetPrepareParamKindAt(vec.Length()))
}

func TestAppendPrepareParamKindsContinueAfterDivergence(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType())
	destination := NewVec(types.T_int64.ToType())
	batchDestination := NewVec(types.T_int64.ToType())
	allDestination := NewVec(types.T_int64.ToType())
	defer func() {
		source.Free(mp)
		destination.Free(mp)
		batchDestination.Free(mp)
		allDestination.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	require.NoError(t, AppendFixedList(source, []int64{1, 2, 3, 4}, nil, mp))
	want := []PrepareParamKind{
		PrepareParamInteger,
		PrepareParamFloat,
		PrepareParamDecimal,
		PrepareParamBoolean,
	}
	require.NoError(t, source.SetPrepareParamKindsWithMP(want, mp))

	for row := range want {
		require.NoError(t, destination.UnionOne(source, int64(row), mp))
	}
	require.Len(t, destination.GetPrepareParamKinds(), destination.Length())
	for row, kind := range want {
		require.Equal(t, kind, destination.GetPrepareParamKindAt(row))
	}

	// Once the exact representation exists, a raw ordinary append must extend
	// the sidecar and initialize the new row to None.
	require.NoError(t, AppendFixed(destination, int64(5), false, mp))
	require.Len(t, destination.GetPrepareParamKinds(), destination.Length())
	require.Equal(t, PrepareParamNone, destination.GetPrepareParamKindAt(4))

	// Batch and whole-vector appends share the same row-parallel growth
	// boundary. Split each operation after the first divergence so the second
	// call must extend and populate an existing sidecar.
	require.NoError(t, batchDestination.UnionBatch(source, 0, 2, nil, mp))
	require.NoError(t, batchDestination.UnionBatch(source, 2, 2, nil, mp))
	for row, kind := range want {
		require.Equal(t, kind, batchDestination.GetPrepareParamKindAt(row))
	}
	require.Len(t, batchDestination.GetPrepareParamKinds(), batchDestination.Length())

	first, err := source.Window(0, 2)
	require.NoError(t, err)
	defer first.Free(mp)
	second, err := source.Window(2, 4)
	require.NoError(t, err)
	defer second.Free(mp)
	unionAll := GetUnionAllFunction(types.T_int64.ToType(), mp)
	require.NoError(t, unionAll(allDestination, first))
	require.NoError(t, unionAll(allDestination, second))
	for row, kind := range want {
		require.Equal(t, kind, allDestination.GetPrepareParamKindAt(row))
	}
	require.Len(t, allDestination.GetPrepareParamKinds(), allDestination.Length())
}

func TestRawAppendOrdinaryRowsDivergeFromScalarPrepareParamKind(t *testing.T) {
	jsonValue, err := bytejson.ParseFromString(`{"value":"ordinary"}`)
	require.NoError(t, err)
	tests := []struct {
		name       string
		typ        types.Type
		seed       func(*Vector, *mpool.MPool) error
		appendRows func(*Vector, *mpool.MPool) error
		wantNull   map[int]bool
	}{
		{
			name: "fixed one",
			typ:  types.T_int64.ToType(),
			seed: func(vec *Vector, mp *mpool.MPool) error {
				return AppendFixed(vec, int64(1), false, mp)
			},
			appendRows: func(vec *Vector, mp *mpool.MPool) error {
				return AppendFixed(vec, int64(2), false, mp)
			},
		},
		{
			name: "fixed multi",
			typ:  types.T_int64.ToType(),
			seed: func(vec *Vector, mp *mpool.MPool) error {
				return AppendFixed(vec, int64(1), false, mp)
			},
			appendRows: func(vec *Vector, mp *mpool.MPool) error {
				return AppendMultiFixed(vec, int64(2), false, 2, mp)
			},
		},
		{
			name: "fixed list with null",
			typ:  types.T_int64.ToType(),
			seed: func(vec *Vector, mp *mpool.MPool) error {
				return AppendFixed(vec, int64(1), false, mp)
			},
			appendRows: func(vec *Vector, mp *mpool.MPool) error {
				return AppendFixedList(vec, []int64{2, 3}, []bool{true, false}, mp)
			},
			wantNull: map[int]bool{1: true},
		},
		{
			name: "bytes one",
			typ:  types.T_varchar.ToType(),
			seed: func(vec *Vector, mp *mpool.MPool) error {
				return AppendBytes(vec, []byte("seed"), false, mp)
			},
			appendRows: func(vec *Vector, mp *mpool.MPool) error {
				return AppendBytes(vec, []byte("ordinary"), false, mp)
			},
		},
		{
			name: "bytes multi",
			typ:  types.T_varchar.ToType(),
			seed: func(vec *Vector, mp *mpool.MPool) error {
				return AppendBytes(vec, []byte("seed"), false, mp)
			},
			appendRows: func(vec *Vector, mp *mpool.MPool) error {
				return AppendMultiBytes(vec, []byte("ordinary"), false, 2, mp)
			},
		},
		{
			name: "bytes list with null",
			typ:  types.T_varchar.ToType(),
			seed: func(vec *Vector, mp *mpool.MPool) error {
				return AppendBytes(vec, []byte("seed"), false, mp)
			},
			appendRows: func(vec *Vector, mp *mpool.MPool) error {
				return AppendBytesList(vec, [][]byte{nil, []byte("ordinary")}, []bool{true, false}, mp)
			},
			wantNull: map[int]bool{1: true},
		},
		{
			name: "string list",
			typ:  types.T_varchar.ToType(),
			seed: func(vec *Vector, mp *mpool.MPool) error {
				return AppendBytes(vec, []byte("seed"), false, mp)
			},
			appendRows: func(vec *Vector, mp *mpool.MPool) error {
				return AppendStringList(vec, []string{"ordinary", "ordinary"}, nil, mp)
			},
		},
		{
			name: "bytejson one",
			typ:  types.T_json.ToType(),
			seed: func(vec *Vector, mp *mpool.MPool) error {
				return AppendByteJson(vec, jsonValue, false, mp)
			},
			appendRows: func(vec *Vector, mp *mpool.MPool) error {
				return AppendByteJson(vec, jsonValue, false, mp)
			},
		},
		{
			name: "bytejson encoded",
			typ:  types.T_json.ToType(),
			seed: func(vec *Vector, mp *mpool.MPool) error {
				return AppendByteJson(vec, jsonValue, false, mp)
			},
			appendRows: func(vec *Vector, mp *mpool.MPool) error {
				return AppendByteJsonEncoded(vec, testByteJsonEncoder{value: jsonValue}, mp)
			},
		},
		{
			name: "array one",
			typ:  types.New(types.T_array_float32, 3, 0),
			seed: func(vec *Vector, mp *mpool.MPool) error {
				return AppendArray(vec, []float32{1, 2, 3}, false, mp)
			},
			appendRows: func(vec *Vector, mp *mpool.MPool) error {
				return AppendArray(vec, []float32{4, 5, 6}, false, mp)
			},
		},
		{
			name: "array list",
			typ:  types.New(types.T_array_float32, 3, 0),
			seed: func(vec *Vector, mp *mpool.MPool) error {
				return AppendArray(vec, []float32{1, 2, 3}, false, mp)
			},
			appendRows: func(vec *Vector, mp *mpool.MPool) error {
				return AppendArrayList(vec, [][]float32{{4, 5, 6}, {7, 8, 9}}, nil, mp)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNew(t.Name())
			vec := NewVec(test.typ)
			t.Cleanup(func() {
				vec.Free(mp)
				require.Zero(t, mp.CurrNB())
			})
			require.NoError(t, test.seed(vec, mp))
			vec.SetPrepareParamKind(PrepareParamFloat)
			require.NoError(t, test.appendRows(vec, mp))

			require.Equal(t, PrepareParamFloat, vec.GetPrepareParamKindAt(0))
			require.Len(t, vec.GetPrepareParamKinds(), vec.Length())
			for row := 1; row < vec.Length(); row++ {
				require.Equal(t, PrepareParamNone, vec.GetPrepareParamKindAt(row))
				require.Equal(t, test.wantNull[row], vec.IsNull(uint64(row)))
			}
		})
	}
}

func TestRawAppendPrepareParamKindFastPathsDoNotAllocate(t *testing.T) {
	for _, test := range []struct {
		name      string
		kind      PrepareParamKind
		isNull    bool
		wantKind  PrepareParamKind
		wantSeen  bool
		wantNulls bool
	}{
		{name: "unobserved ordinary", wantKind: PrepareParamNone},
		{name: "observed ordinary", kind: PrepareParamNone, wantKind: PrepareParamNone, wantSeen: true},
		{name: "prepared null", kind: PrepareParamFloat, isNull: true, wantKind: PrepareParamFloat, wantSeen: true, wantNulls: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNew(t.Name())
			vec := NewVec(types.T_int64.ToType())
			t.Cleanup(func() {
				vec.Free(mp)
				require.Zero(t, mp.CurrNB())
			})
			require.NoError(t, vec.PreExtend(2, mp))
			require.NoError(t, AppendFixed(vec, int64(1), false, mp))
			if test.wantSeen {
				vec.SetPrepareParamKind(test.kind)
			}
			before := mp.CurrNB()
			require.NoError(t, AppendFixed(vec, int64(2), test.isNull, mp))
			require.Equal(t, before, mp.CurrNB())
			require.Nil(t, vec.GetPrepareParamKinds())
			require.Equal(t, test.wantKind, vec.GetPrepareParamKindAt(0))
			require.Equal(t, test.wantNulls, vec.IsNull(1))
		})
	}
}

func TestRawAppendPrepareParamKindOwnerlessPrefixStaysScalar(t *testing.T) {
	for _, test := range []struct {
		name string
		seed func(*Vector, *mpool.MPool) error
	}{
		{name: "empty"},
		{
			name: "all null",
			seed: func(vec *Vector, mp *mpool.MPool) error {
				return AppendFixed(vec, int64(0), true, mp)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNew(t.Name())
			vec := NewVec(types.T_int64.ToType())
			t.Cleanup(func() {
				vec.Free(mp)
				require.Zero(t, mp.CurrNB())
			})
			require.NoError(t, vec.PreExtend(2, mp))
			if test.seed != nil {
				require.NoError(t, test.seed(vec, mp))
			}
			vec.SetPrepareParamKind(PrepareParamFloat)
			before := mp.CurrNB()
			require.NoError(t, AppendFixed(vec, int64(1), false, mp))

			require.Equal(t, before, mp.CurrNB())
			require.Nil(t, vec.GetPrepareParamKinds())
			require.False(t, vec.HasPrepareParamKind())
			require.Equal(t, PrepareParamNone, vec.GetPrepareParamKindAt(vec.Length()-1))
		})
	}
}

func TestRawAppendPrepareParamKindOOMDoesNotPublishRow(t *testing.T) {
	const poolCap = int64(1 << 20)
	mp, err := mpool.NewMPool(t.Name(), poolCap, mpool.NoLock)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	vec := NewVec(types.T_int64.ToType())
	require.NoError(t, vec.PreExtend(2, mp))
	require.NoError(t, AppendFixed(vec, int64(1), false, mp))
	vec.SetPrepareParamKind(PrepareParamFloat)
	fill, err := mp.Alloc(int(poolCap-mp.CurrNB()), true)
	require.NoError(t, err)
	defer func() {
		mp.Free(fill)
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	err = AppendFixed(vec, int64(2), false, mp)
	require.Error(t, err)
	require.Equal(t, 1, vec.Length())
	require.Equal(t, PrepareParamFloat, vec.GetPrepareParamKindAt(0))
	require.Nil(t, vec.GetPrepareParamKinds())

	mp.Free(fill)
	fill = nil
	require.NoError(t, AppendFixed(vec, int64(2), false, mp))
	require.Equal(t, 2, vec.Length())
	require.Equal(t, PrepareParamFloat, vec.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamNone, vec.GetPrepareParamKindAt(1))
}

func TestRawAppendPrepareParamKindRollbackRestoresScalar(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	vec := NewVec(types.T_int64.ToType())
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, vec.PreExtend(2, mp))
	require.NoError(t, AppendFixed(vec, int64(1), false, mp))
	vec.SetPrepareParamKind(PrepareParamFloat)
	before := mp.CurrNB()
	checkpoint := vec.MakeAppendCheckpoint()

	require.NoError(t, AppendFixed(vec, int64(2), false, mp))
	require.NotNil(t, vec.GetPrepareParamKinds())
	vec.RollbackAppend(checkpoint, 1)

	require.Equal(t, 1, vec.Length())
	require.Nil(t, vec.GetPrepareParamKinds())
	require.Equal(t, PrepareParamFloat, vec.GetPrepareParamKindAt(0))
	require.Equal(t, before, mp.CurrNB())
}

func TestPrepareParamKindWindowRetainsSidecarOnlyForDivergence(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType())
	require.NoError(t, AppendFixedList(source, []int64{1, 2, 3}, nil, mp))
	source.GetNulls().Add(1)
	require.NoError(t, source.SetPrepareParamKindsWithMP([]PrepareParamKind{
		PrepareParamInteger,
		PrepareParamFloat,
		PrepareParamDecimal,
	}, mp))
	defer func() {
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	uniform, err := source.Window(0, 1)
	require.NoError(t, err)
	require.Nil(t, uniform.GetPrepareParamKinds())
	require.Equal(t, PrepareParamInteger, uniform.GetPrepareParamKind())
	uniform.Free(mp)

	nullOnly, err := source.Window(1, 2)
	require.NoError(t, err)
	require.Nil(t, nullOnly.GetPrepareParamKinds())
	require.False(t, nullOnly.HasPrepareParamKind())
	nullOnly.Free(mp)

	mixed, err := source.Window(0, 3)
	require.NoError(t, err)
	require.Len(t, mixed.GetPrepareParamKinds(), mixed.Length())
	require.Equal(t, PrepareParamInteger, mixed.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamDecimal, mixed.GetPrepareParamKindAt(2))
	mixed.Free(mp)
}

func BenchmarkUnionOnePrepareParamKindLateDivergence(b *testing.B) {
	const rows = 16 * 1024
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType())
	destination := NewVec(types.T_int64.ToType())
	defer source.Free(mp)
	defer destination.Free(mp)

	values := make([]int64, rows)
	require.NoError(b, AppendFixedList(source, values, nil, mp))
	kinds := make([]PrepareParamKind, rows)
	for row := range kinds {
		kinds[row] = PrepareParamInteger
	}
	kinds[rows/2] = PrepareParamFloat
	require.NoError(b, source.SetPrepareParamKindsWithMP(kinds, mp))

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
}

func BenchmarkUnionBatchPrepareParamKind(b *testing.B) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType())
	defer source.Free(mp)
	for i := 0; i < 1024; i++ {
		require.NoError(b, AppendFixed(source, int64(i), false, mp))
	}
	source.SetPrepareParamKind(PrepareParamFloat)
	destination := NewVec(types.T_int64.ToType())
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
