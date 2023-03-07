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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

var testMp = mpool.MustNewZero()

func TestReset(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_varchar)})
	Reset(v0)
	require.Equal(t, 0, len(v0.data))
}

func TestClean(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	AppendFixed(v0, []int8{1, 2, 3}, testMp)
	Clean(v0, testMp)
	require.Equal(t, 0, len(v0.data))
}

func TestSetCol(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	SetCol(v0, []int8{1, 2, 3})
	require.Equal(t, v0.Col, []int8{1, 2, 3})
}

func TestLength(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.Col = []int8{1, 2, 3, 4, 5}
	require.Equal(t, 5, Length(v0))
	v1 := New(types.Type{Oid: types.T(types.T_char)})
	va1, _, _ := types.BuildVarlena([]byte("foo"), nil, testMp)
	va2, _, _ := types.BuildVarlena([]byte("bar"), nil, testMp)
	v1.Col = []types.Varlena{va1, va2}
	require.Equal(t, 2, Length(v1))
}

func setLenTest[T any](t *testing.T, tt types.T, mp *mpool.MPool, arg []T) {
	v0 := New(types.Type{Oid: types.T(tt)})
	v0.Col = arg
	FillVectorData[T](v0, mp)
	SetLength(v0, 3)
	require.Equal(t, 3, len(v0.Col.([]T)))
}

func TestSetLength(t *testing.T) {
	mp := mpool.MustNewZero()
	setLenTest(t, types.T_int8, mp, []int8{1, 2, 3, 4, 5, 6, 7, 8})
	setLenTest(t, types.T_int16, mp, []int16{1, 2, 3, 4, 5, 6, 7, 8})
	setLenTest(t, types.T_int32, mp, []int32{1, 2, 3, 4, 5, 6, 7, 8})
	setLenTest(t, types.T_int64, mp, []int64{1, 2, 3, 4, 5, 6, 7, 8})

	setLenTest(t, types.T_uint8, mp, []uint8{1, 2, 3, 4, 5, 6, 7, 8})
	setLenTest(t, types.T_uint16, mp, []uint16{1, 2, 3, 4, 5, 6, 7, 8})
	setLenTest(t, types.T_uint32, mp, []uint32{1, 2, 3, 4, 5, 6, 7, 8})
	setLenTest(t, types.T_uint64, mp, []uint64{1, 2, 3, 4, 5, 6, 7, 8})

	setLenTest(t, types.T_float32, mp, []float32{1, 2, 3, 4, 5, 6, 7, 8})
	setLenTest(t, types.T_float64, mp, []float64{1, 2, 3, 4, 5, 6, 7, 8})

	setLenTest(t, types.T_date, mp, []types.Date{1, 2, 3, 4, 5, 6, 7, 8})
	setLenTest(t, types.T_datetime, mp, []types.Datetime{1, 2, 3, 4, 5, 6, 7, 8})
}

func mustMakeSliceArgsAsBytes[T any](mp *mpool.MPool, args ...T) []byte {
	ret, err := mpool.MakeSliceArgs(mp, args...)
	if err != nil {
		panic(err)
	}
	return types.EncodeSlice(ret)
}

func dupTest[T any](t *testing.T, tt types.T, mp *mpool.MPool, args ...T) {
	v0 := New(types.Type{Oid: types.T(tt)})
	v0.data = mustMakeSliceArgsAsBytes(mp, args...)
	v0.Col = types.DecodeSlice[T](v0.data)
	v0Duplicate, _ := Dup(v0, mp)
	require.Equal(t, v0, v0Duplicate)
}

func TestDup(t *testing.T) {
	mp := mpool.MustNewZero()
	dupTest[int8](t, types.T_int8, mp, 1, 2, 3, 4)
	dupTest[int16](t, types.T_int16, mp, 1, 2, 3, 4)
	dupTest[int32](t, types.T_int32, mp, 1, 2, 3, 4)
	dupTest[int64](t, types.T_int64, mp, 1, 2, 3, 4)
	dupTest[uint8](t, types.T_uint8, mp, 1, 2, 3, 4)
	dupTest[uint16](t, types.T_uint16, mp, 1, 2, 3, 4)
	dupTest[uint32](t, types.T_uint32, mp, 1, 2, 3, 4)
	dupTest[uint64](t, types.T_uint64, mp, 1, 2, 3, 4)
	dupTest[types.Date](t, types.T_date, mp, 1, 2, 3, 4)
	dupTest[types.Datetime](t, types.T_datetime, mp, 1, 2, 3, 4)

	v10 := New(types.Type{Oid: types.T(types.T_char)})
	AppendBytes(v10, [][]byte{
		[]byte("hello"),
		[]byte("Gut"),
		[]byte("knoichiwa"),
		[]byte("nihao"),
	}, mp)
	v10Duplicate, err := Dup(v10, mp)
	require.Equal(t, err, nil)
	require.Equal(t, v10.data, v10Duplicate.data)
	require.Equal(t, v10.area, v10Duplicate.area)
	require.Equal(t, MustStrCols(v10), MustStrCols(v10Duplicate))
	require.Equal(t, v10.GetString(0), v10Duplicate.GetString(0))
	require.Equal(t, v10.GetString(1), v10Duplicate.GetString(1))
	require.Equal(t, v10.GetString(2), v10Duplicate.GetString(2))
	require.Equal(t, v10.GetString(3), v10Duplicate.GetString(3))
}

func TestGetUnionOneFunction(t *testing.T) {
	{ // test const vector
		mp := mpool.MustNewZero()
		v := New(types.New(types.T_int8, 0, 0))
		w := New(types.New(types.T_int8, 0, 0))
		err := w.Append(int8(0), false, mp)
		require.NoError(t, err)
		uf := GetUnionOneFunction(w.GetType(), mp)
		err = uf(v, w, 0)
		require.NoError(t, err)
		w.Free(mp)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // test const vector
		mp := mpool.MustNewZero()
		v := New(types.New(types.T_varchar, 0, 0))
		w := New(types.New(types.T_varchar, 0, 0))
		err := w.Append([]byte("x"), false, mp)
		require.NoError(t, err)
		uf := GetUnionOneFunction(w.GetType(), mp)
		err = uf(v, w, 0)
		require.NoError(t, err)
		w.Free(mp)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestWindow(t *testing.T) {
	mp := mpool.MustNewZero()
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.data = mustMakeSliceArgsAsBytes[int8](mp, 1, 2, 3, 4, 5, 6, 7, 8)
	v0.Col = types.DecodeSlice[int8](v0.data)
	v0Window := New(types.Type{Oid: types.T(types.T_int8)})
	start, end := 1, 3
	v0Window = Window(v0, start, end, v0Window)
	require.Equal(t, v0.Col.([]int8)[start:end], v0Window.Col)

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	v1.data = mustMakeSliceArgsAsBytes[int16](mp, 1, 2, 3, 4, 5, 6, 7, 8)
	v1.Col = types.DecodeSlice[int16](v1.data)
	v1Window := New(types.Type{Oid: types.T(types.T_int16)})
	v1Window = Window(v1, start, end, v1Window)
	require.Equal(t, v1.Col.([]int16)[start:end], v1Window.Col)

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	v2.data = mustMakeSliceArgsAsBytes[int32](mp, 1, 2, 3, 4, 5, 6, 7, 8)
	v2.Col = types.DecodeSlice[int32](v2.data)
	v2Window := New(types.Type{Oid: types.T(types.T_int32)})
	v2Window = Window(v2, start, end, v2Window)
	require.Equal(t, v2.Col.([]int32)[start:end], v2Window.Col)

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	v3.data = mustMakeSliceArgsAsBytes[int64](mp, 1, 2, 3, 4, 5, 6, 7, 8)
	v3.Col = types.DecodeSlice[int64](v3.data)
	v3Window := New(types.Type{Oid: types.T(types.T_int64)})
	v3Window = Window(v3, start, end, v3Window)
	require.Equal(t, v3.Col.([]int64)[start:end], v3Window.Col)

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	v4.data = mustMakeSliceArgsAsBytes[uint8](mp, 1, 2, 3, 4, 5, 6, 7, 8)
	v4.Col = types.DecodeSlice[uint8](v4.data)
	v4Window := New(types.Type{Oid: types.T(types.T_uint8)})
	v4Window = Window(v4, start, end, v4Window)
	require.Equal(t, v4.Col.([]uint8)[start:end], v4Window.Col)

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	v5.data = mustMakeSliceArgsAsBytes[uint16](mp, 1, 2, 3, 4, 5, 6, 7, 8)
	v5.Col = types.DecodeSlice[uint16](v5.data)
	v5Window := New(types.Type{Oid: types.T(types.T_uint16)})
	v5Window = Window(v5, start, end, v5Window)
	require.Equal(t, v5.Col.([]uint16)[start:end], v5Window.Col)

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	v6.data = mustMakeSliceArgsAsBytes[uint32](mp, 1, 2, 3, 4, 5, 6, 7, 8)
	v6.Col = types.DecodeSlice[uint32](v6.data)
	v6Window := New(types.Type{Oid: types.T(types.T_uint32)})
	v6Window = Window(v6, start, end, v6Window)
	require.Equal(t, v6.Col.([]uint32)[start:end], v6Window.Col)

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	v7.data = mustMakeSliceArgsAsBytes[uint64](mp, 1, 2, 3, 4, 5, 6, 7, 8)
	v7.Col = types.DecodeSlice[uint64](v7.data)
	v7Window := New(types.Type{Oid: types.T(types.T_uint64)})
	v7Window = Window(v7, start, end, v7Window)
	require.Equal(t, v7.Col.([]uint64)[start:end], v7Window.Col)

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	v8.data = mustMakeSliceArgsAsBytes[float32](mp, 1, 2, 3, 4, 5, 6, 7, 8)
	v8.Col = types.DecodeSlice[float32](v8.data)
	v8Window := New(types.Type{Oid: types.T(types.T_float32)})
	v8Window = Window(v8, start, end, v8Window)
	require.Equal(t, v8.Col.([]float32)[start:end], v8Window.Col)

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	v9.data = mustMakeSliceArgsAsBytes[float64](mp, 1, 2, 3, 4, 5, 6, 7, 8)
	v9.Col = types.DecodeSlice[float64](v9.data)
	v9Window := New(types.Type{Oid: types.T(types.T_float64)})
	v9Window = Window(v9, start, end, v9Window)
	require.Equal(t, v9.Col.([]float64)[start:end], v9Window.Col)

	v11 := New(types.Type{Oid: types.T(types.T_char)})
	AppendBytes(v11, [][]byte{
		[]byte("hello"),
		[]byte("Gut"),
		[]byte("konichiwa"),
		[]byte("nihao"),
	}, mp)
	v11Window := New(types.Type{Oid: types.T(types.T_char)})
	v11Window = Window(v11, start, end, v11Window)
	vs11 := MustStrCols(v11)
	ws11 := MustStrCols(v11Window)
	require.Equal(t, vs11[start:end], ws11)

	v12 := New(types.Type{Oid: types.T(types.T_date)})
	v12.data = mustMakeSliceArgsAsBytes[types.Date](mp, 1, 2, 3, 4, 5, 6, 7, 8)
	v12.Col = types.DecodeSlice[types.Date](v12.data)
	v12Window := New(types.Type{Oid: types.T(types.T_date)})
	v12Window = Window(v12, start, end, v12Window)
	require.Equal(t, v12.Col.([]types.Date)[start:end], v12Window.Col)

	v13 := New(types.Type{Oid: types.T(types.T_datetime)})
	v13.data = mustMakeSliceArgsAsBytes[types.Datetime](mp, 1, 2, 3, 4, 5, 6, 7, 8)
	v13.Col = types.DecodeSlice[types.Datetime](v13.data)
	v13Window := New(types.Type{Oid: types.T(types.T_datetime)})
	v13Window = Window(v13, start, end, v13Window)
	require.Equal(t, v13.Col.([]types.Datetime)[start:end], v13Window.Col)
}

func TestWindowWithNulls(t *testing.T) {
	v0 := New(types.T_int8.ToType())
	mp := mpool.MustNewZero()

	_ = v0.Append(int8(0), false, mp)
	_ = v0.Append(int8(1), false, mp)
	_ = v0.Append(int8(2), false, mp)
	_ = v0.Append(int8(-1), true, mp) // v0[3] = null
	_ = v0.Append(int8(6), false, mp)
	_ = v0.Append(int8(-1), true, mp) // v0[5] = null
	_ = v0.Append(int8(-1), true, mp) // v0[6] = null
	_ = v0.Append(int8(6), false, mp)
	_ = v0.Append(int8(7), false, mp)
	_ = v0.Append(int8(8), false, mp)

	require.Equal(t, []uint64{3, 5, 6}, v0.Nsp.Np.ToArray())

	start, end := 1, 7
	v0Window := New(types.T_int8.ToType())
	v0Window = Window(v0, start, end, v0Window)
	require.Equal(t, v0.Col.([]int8)[start:end], v0Window.Col)
	require.Equal(t, []uint64{2, 4, 5}, v0Window.Nsp.Np.ToArray())

	//t.Log(v0.String())
	//t.Log(v0Window.String())
}

func TestAppend(t *testing.T) {
	mp := mpool.MustNewZero()
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	int8Slice := []int8{1, 2, 3, 4, 5, 6, 7, 8}
	v0.data = mustMakeSliceArgsAsBytes(mp, int8Slice...)
	v0.Col = types.DecodeSlice[int8](v0.data)
	appendInt8Slice := []int8{21, 22, 23}
	err := AppendFixed(v0, appendInt8Slice, mp)
	require.NoError(t, err)
	require.Equal(t, append(int8Slice, appendInt8Slice...), v0.Col.([]int8))

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	int16Slice := []int16{1, 2, 3, 4, 5, 6, 7, 8}
	v1.data = mustMakeSliceArgsAsBytes(mp, int16Slice...)
	v1.Col = types.DecodeSlice[int16](v1.data)
	appendInt16Slice := []int16{21, 22, 23}
	err = AppendFixed(v1, appendInt16Slice, mp)
	require.NoError(t, err)
	require.Equal(t, append(int16Slice, appendInt16Slice...), v1.Col.([]int16))

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	int32Slice := []int32{1, 2, 3, 4, 5, 6, 7, 8}
	v2.data = mustMakeSliceArgsAsBytes(mp, int32Slice...)
	v2.Col = types.DecodeSlice[int32](v2.data)
	appendInt32Slice := []int32{21, 22, 23}
	err = AppendFixed(v2, appendInt32Slice, mp)
	require.NoError(t, err)
	require.Equal(t, append(int32Slice, appendInt32Slice...), v2.Col.([]int32))

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	int64Slice := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	v3.data = mustMakeSliceArgsAsBytes(mp, int64Slice...)
	v3.Col = types.DecodeSlice[int64](v3.data)
	appendInt64Slice := []int64{21, 22, 23}
	err = AppendFixed(v3, appendInt64Slice, mp)
	require.NoError(t, err)
	require.Equal(t, append(int64Slice, appendInt64Slice...), v3.Col.([]int64))

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	uint8Slice := []uint8{1, 2, 3, 4, 5, 6, 7, 8}
	v4.data = mustMakeSliceArgsAsBytes(mp, uint8Slice...)
	v4.Col = types.DecodeSlice[uint8](v4.data)
	appendUint8Slice := []uint8{21, 22, 23}
	err = AppendFixed(v4, appendUint8Slice, mp)
	require.NoError(t, err)
	require.Equal(t, append(uint8Slice, appendUint8Slice...), v4.Col.([]uint8))

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	uint16Slice := []uint16{1, 2, 3, 4, 5, 6, 7, 8}
	v5.data = mustMakeSliceArgsAsBytes(mp, uint16Slice...)
	v5.Col = types.DecodeSlice[uint16](v5.data)
	appendUint16Slice := []uint16{21, 22, 23}
	err = AppendFixed(v5, appendUint16Slice, mp)
	require.NoError(t, err)
	require.Equal(t, append(uint16Slice, appendUint16Slice...), v5.Col.([]uint16))

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	uint32Slice := []uint32{1, 2, 3, 4, 5, 6, 7, 8}
	v6.data = mustMakeSliceArgsAsBytes(mp, uint32Slice...)
	v6.Col = types.DecodeSlice[uint32](v6.data)
	appendUint32Slice := []uint32{21, 22, 23}
	err = AppendFixed(v6, appendUint32Slice, mp)
	require.NoError(t, err)
	require.Equal(t, append(uint32Slice, appendUint32Slice...), v6.Col.([]uint32))

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	uint64Slice := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	v7.data = mustMakeSliceArgsAsBytes(mp, uint64Slice...)
	v7.Col = types.DecodeSlice[uint64](v7.data)
	appendUint64Slice := []uint64{21, 22, 23}
	err = AppendFixed(v7, appendUint64Slice, mp)
	require.NoError(t, err)
	require.Equal(t, append(uint64Slice, appendUint64Slice...), v7.Col.([]uint64))

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	float32Slice := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	v8.data = mustMakeSliceArgsAsBytes(mp, float32Slice...)
	v8.Col = types.DecodeSlice[float32](v8.data)
	appendFloat32Slice := []float32{21, 22, 23}
	err = AppendFixed(v8, appendFloat32Slice, mp)
	require.NoError(t, err)
	require.Equal(t, append(float32Slice, appendFloat32Slice...), v8.Col.([]float32))

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	float64Slice := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	v9.data = mustMakeSliceArgsAsBytes(mp, float64Slice...)
	v9.Col = types.DecodeSlice[float64](v9.data)
	appendFloat64Slice := []float64{21, 22, 23}
	err = AppendFixed(v9, appendFloat64Slice, mp)
	require.NoError(t, err)
	require.Equal(t, append(float64Slice, appendFloat64Slice...), v9.Col.([]float64))

	v10 := New(types.Type{Oid: types.T(types.T_date)})
	dateSlice := []types.Date{1, 2, 3, 4, 5, 6, 7, 8}
	v10.data = mustMakeSliceArgsAsBytes(mp, dateSlice...)
	v10.Col = types.DecodeSlice[types.Date](v10.data)
	appendDateSlice := []types.Date{21, 22, 23}
	err = AppendFixed(v10, appendDateSlice, mp)
	require.NoError(t, err)
	require.Equal(t, append(dateSlice, appendDateSlice...), v10.Col.([]types.Date))

	v11 := New(types.Type{Oid: types.T(types.T_datetime)})
	datetimeSlice := []types.Datetime{1, 2, 3, 4, 5, 6, 7, 8}
	v11.data = mustMakeSliceArgsAsBytes(mp, datetimeSlice...)
	v11.Col = types.DecodeSlice[types.Datetime](v11.data)
	appendDatetimeSlice := []types.Datetime{21, 22, 23}
	err = AppendFixed(v11, appendDatetimeSlice, mp)
	require.NoError(t, err)
	require.Equal(t, append(datetimeSlice, appendDatetimeSlice...), v11.Col.([]types.Datetime))
}

func shrinkTest[T any](t *testing.T, tt types.T, mp *mpool.MPool, args ...T) {
	v0 := New(types.Type{Oid: tt})
	v0.data = mustMakeSliceArgsAsBytes(mp, args...)
	v0.Col = types.DecodeSlice[T](v0.data)
	sels := []int64{1, 3, 5}
	Shrink(v0, sels)

	st := v0.Col.([]T)
	require.Equal(t, 3, len(st))
	require.Equal(t, args[1], st[0])
	require.Equal(t, args[3], st[1])
	require.Equal(t, args[5], st[2])
}

func TestShrink(t *testing.T) {
	mp := mpool.MustNewZero()

	shrinkTest[int8](t, types.T_int8, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shrinkTest[int16](t, types.T_int16, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shrinkTest[int32](t, types.T_int32, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shrinkTest[int64](t, types.T_int64, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)

	shrinkTest[uint8](t, types.T_uint8, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shrinkTest[uint16](t, types.T_uint16, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shrinkTest[uint32](t, types.T_uint32, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shrinkTest[uint64](t, types.T_uint64, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)

	shrinkTest[float32](t, types.T_float32, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shrinkTest[float64](t, types.T_float64, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)

	shrinkTest[types.Date](t, types.T_date, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shrinkTest[types.Datetime](t, types.T_datetime, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
}

func shuffleTest[T any](t *testing.T, tt types.T, mp *mpool.MPool, args ...T) {
	v0 := New(types.Type{Oid: tt})
	v0.data = mustMakeSliceArgsAsBytes(mp, args...)
	v0.Col = types.DecodeSlice[T](v0.data)
	sels := []int64{1, 3, 5}
	err := Shuffle(v0, sels, mp)
	require.NoError(t, err)
	st := v0.Col.([]T)
	require.Equal(t, 3, len(st))
	require.Equal(t, args[1], st[0])
	require.Equal(t, args[3], st[1])
	require.Equal(t, args[5], st[2])
}

func TestShuffle(t *testing.T) {
	mp := mpool.MustNewZero()

	shuffleTest[int8](t, types.T_int8, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shuffleTest[int16](t, types.T_int16, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shuffleTest[int32](t, types.T_int32, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shuffleTest[int64](t, types.T_int64, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)

	shuffleTest[uint8](t, types.T_uint8, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shuffleTest[uint16](t, types.T_uint16, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shuffleTest[uint32](t, types.T_uint32, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shuffleTest[uint64](t, types.T_uint64, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)

	shuffleTest[float32](t, types.T_float32, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shuffleTest[float64](t, types.T_float64, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)

	shuffleTest[types.Date](t, types.T_date, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	shuffleTest[types.Datetime](t, types.T_datetime, mp, 0, 1, 2, 3, 4, 5, 6, 7, 8)
}

func TestCopy(t *testing.T) {
	mp := mpool.MustNewZero()
	w0 := New(types.Type{Oid: types.T(types.T_char)})
	AppendBytes(w0, [][]byte{
		[]byte("nihao"),
		[]byte("nihao"),
		[]byte("nihao"),
		[]byte("nihao"),
	}, mp)
	v0 := New(types.Type{Oid: types.T(types.T_char)})
	AppendBytes(v0, [][]byte{
		[]byte("hello"),
		[]byte("hello"),
		[]byte("hello"),
		[]byte("hello"),
	}, mp)
	err := Copy(v0, w0, 2, 0, mp)
	require.NoError(t, err)

	expectvec := New(types.Type{Oid: types.T(types.T_char)})
	AppendBytes(expectvec, [][]byte{
		[]byte("hello"),
		[]byte("hello"),
		[]byte("nihao"),
		[]byte("hello"),
	}, mp)
	require.Equal(t, GetStrVectorValues(expectvec), GetStrVectorValues(v0))
}

func unionOneTest[T any](t *testing.T, tt types.T, mp *mpool.MPool, arg1 []T, arg2 []T, arg3 []T) {
	w0 := New(types.Type{Oid: types.T(tt)})
	w0.data = mustMakeSliceArgsAsBytes(mp, arg1...)
	w0.Col = types.DecodeSlice[T](w0.data)
	v0 := New(types.Type{Oid: types.T(tt)})
	err := UnionOne(v0, w0, 3, mp)
	require.NoError(t, err)
	require.Equal(t, arg2, v0.Col.([]T))
	v0.data = mustMakeSliceArgsAsBytes(mp, arg1...)
	v0.Col = types.DecodeSlice[T](v0.data)
	err = UnionOne(v0, w0, 3, mp)
	require.NoError(t, err)
	require.Equal(t, arg3, v0.Col.([]T))
}

func TestUnionOne(t *testing.T) {
	mp := mpool.MustNewZero()
	unionOneTest(t, types.T_int8, mp, []int8{0, 1, 2, 3, 4, 5, 6, 7, 8}, []int8{3}, []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 3})
	unionOneTest(t, types.T_int16, mp, []int16{0, 1, 2, 3, 4, 5, 6, 7, 8}, []int16{3}, []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 3})
	unionOneTest(t, types.T_int32, mp, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8}, []int32{3}, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3})
	unionOneTest(t, types.T_int64, mp, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8}, []int64{3}, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3})

	unionOneTest(t, types.T_uint8, mp, []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8}, []uint8{3}, []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 3})
	unionOneTest(t, types.T_uint16, mp, []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8}, []uint16{3}, []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 3})
	unionOneTest(t, types.T_uint32, mp, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8}, []uint32{3}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3})
	unionOneTest(t, types.T_uint64, mp, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8}, []uint64{3}, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3})

	unionOneTest(t, types.T_float32, mp, []float32{0, 1, 2, 3, 4, 5, 6, 7, 8}, []float32{3}, []float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3})
	unionOneTest(t, types.T_float64, mp, []float64{0, 1, 2, 3, 4, 5, 6, 7, 8}, []float64{3}, []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3})

	unionOneTest(t, types.T_date, mp, []types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8}, []types.Date{3}, []types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8, 3})
	unionOneTest(t, types.T_datetime, mp, []types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8}, []types.Datetime{3}, []types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8, 3})

	w10 := New(types.Type{Oid: types.T(types.T_char), Width: types.MaxCharLen})
	AppendString(w10, []string{"nihao", "nihao", "nihao", "nihao"}, mp)
	v10 := New(types.Type{Oid: types.T(types.T_char), Width: types.MaxCharLen})
	err := UnionOne(v10, w10, 3, mp)
	require.NoError(t, err)
	v10vals := GetStrVectorValues(v10)
	require.Equal(t, []string{"nihao"}, v10vals)

	v10 = New(types.Type{Oid: types.T(types.T_char), Width: types.MaxCharLen})
	AppendString(v10, []string{"hello", "hello", "hello"}, mp)
	err = UnionOne(v10, w10, 3, mp)
	require.NoError(t, err)
	v10vals = GetStrVectorValues(v10)
	require.Equal(t, []string{"hello", "hello", "hello", "nihao"}, v10vals)

	w101 := New(types.Type{Oid: types.T(types.T_char), Width: types.MaxCharLen})
	AppendString(w101, []string{"thanks"}, mp)
	w101.isConst = true
	err = UnionOne(v10, w101, 2, mp)
	require.NoError(t, err)
	v10vals = GetStrVectorValues(v10)
	require.Equal(t, []string{"hello", "hello", "hello", "nihao", "thanks"}, v10vals)

	// Long string test
	astr := "a123456789012345678901234567890"
	bstr := "b123456789012345678901234567890AKQJ1098765432"

	w102 := New(types.Type{Oid: types.T(types.T_char), Width: types.MaxCharLen})
	AppendString(w102, []string{astr, astr, astr, astr}, mp)
	v102 := New(types.Type{Oid: types.T(types.T_char), Width: types.MaxCharLen})
	err = UnionOne(v102, w102, 3, mp)
	require.NoError(t, err)
	v102vals := GetStrVectorValues(v102)
	require.Equal(t, []string{astr}, v102vals)

	v102 = New(types.Type{Oid: types.T(types.T_char), Width: types.MaxCharLen})
	AppendString(v102, []string{bstr, bstr, bstr}, mp)
	err = UnionOne(v102, w102, 3, mp)
	require.NoError(t, err)
	v102vals = GetStrVectorValues(v102)
	require.Equal(t, []string{bstr, bstr, bstr, astr}, v102vals)
}

func unionBatchTest[T any](t *testing.T, tt types.T, mp *mpool.MPool, arg1 []T, arg2 []T, arg3 []T) {
	w0 := New(types.Type{Oid: types.T(tt)})
	w0.data = mustMakeSliceArgsAsBytes(mp, arg1...)
	w0.Col = types.DecodeSlice[T](w0.data)
	v0 := New(types.Type{Oid: types.T(tt)})
	err := UnionBatch(v0, w0, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, arg2, v0.Col.([]T))
	v0.data = mustMakeSliceArgsAsBytes(mp, arg1...)
	v0.Col = types.DecodeSlice[T](v0.data)
	err = UnionBatch(v0, w0, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, arg3, v0.Col.([]T))
}

func TestUnionBatch(t *testing.T) {
	mp := mpool.MustNewZero()
	unionBatchTest(t, types.T_int8, mp, []int8{0, 1, 2, 3, 4, 5, 6, 7, 8}, []int8{3, 4}, []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4})
	unionBatchTest(t, types.T_int16, mp, []int16{0, 1, 2, 3, 4, 5, 6, 7, 8}, []int16{3, 4}, []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4})
	unionBatchTest(t, types.T_int32, mp, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8}, []int32{3, 4}, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4})
	unionBatchTest(t, types.T_int64, mp, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8}, []int64{3, 4}, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4})

	unionBatchTest(t, types.T_uint8, mp, []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8}, []uint8{3, 4}, []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4})
	unionBatchTest(t, types.T_uint16, mp, []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8}, []uint16{3, 4}, []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4})
	unionBatchTest(t, types.T_uint32, mp, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8}, []uint32{3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4})
	unionBatchTest(t, types.T_uint64, mp, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8}, []uint64{3, 4}, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4})

	unionBatchTest(t, types.T_float32, mp, []float32{0, 1, 2, 3, 4, 5, 6, 7, 8}, []float32{3, 4}, []float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4})
	unionBatchTest(t, types.T_float64, mp, []float64{0, 1, 2, 3, 4, 5, 6, 7, 8}, []float64{3, 4}, []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4})

	unionBatchTest(t, types.T_date, mp, []types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8}, []types.Date{3, 4}, []types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4})
	unionBatchTest(t, types.T_datetime, mp, []types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8}, []types.Datetime{3, 4}, []types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4})

	w10 := New(types.Type{Oid: types.T(types.T_char), Width: types.MaxCharLen})
	AppendString(w10, []string{"nihao", "nihao", "nihao", "nihao"}, mp)
	v10 := New(types.Type{Oid: types.T(types.T_char), Width: types.MaxCharLen})
	err := UnionBatch(v10, w10, 1, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []string{"nihao", "nihao"}, GetStrVectorValues(v10))

	Clean(v10, mp)
	AppendString(v10, []string{"hello", "hello", "hello", "hello"}, mp)
	err = UnionBatch(v10, w10, 1, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []string{"hello", "hello", "hello", "hello", "nihao", "nihao"}, GetStrVectorValues(v10))
}

func strTest[T any](t *testing.T, tt types.T, mp *mpool.MPool, arg1 ...T) {
	v0 := New(types.Type{Oid: types.T(tt)})
	v0.data = mustMakeSliceArgsAsBytes(mp, arg1...)
	v0.Col = types.DecodeSlice[T](v0.data)
	result := v0.String()
	require.Equal(t, "[0 1 2]-&{<nil>}", result)
}

func TestVector_String(t *testing.T) {
	mp := mpool.MustNewZero()
	strTest[int8](t, types.T_int8, mp, 0, 1, 2)
	strTest[int16](t, types.T_int16, mp, 0, 1, 2)
	strTest[int32](t, types.T_int32, mp, 0, 1, 2)
	strTest[int64](t, types.T_int64, mp, 0, 1, 2)
}

func FillVectorData[T any](v *Vector, mp *mpool.MPool) {
	v.data = mustMakeSliceArgsAsBytes(mp, v.Col.([]T)...)
}

func TestVector_Marshal(t *testing.T) {
	vals := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	vec := NewWithFixed(types.T_int64.ToType(), vals, nil, testMp)
	nulls.Add(vec.Nsp, 1)
	nulls.Add(vec.Nsp, 3)
	nulls.Add(vec.Nsp, 5)
	nulls.Add(vec.Nsp, 7)
	nulls.Add(vec.Nsp, 9)

	bs, err := vec.MarshalBinary()
	require.NoError(t, err)

	vec2 := New(types.T_int64.ToType())
	err = vec2.UnmarshalBinary(bs)
	require.NoError(t, err)

	require.Equal(t, vec.Length(), vec2.Length())
	require.True(t, vec2.Nsp.Contains(1))
	require.True(t, vec2.Nsp.Contains(3))
	require.True(t, vec2.Nsp.Contains(5))
	require.True(t, vec2.Nsp.Contains(7))
	require.True(t, vec2.Nsp.Contains(9))

	tv1 := MustTCols[int64](vec)
	tv2 := MustTCols[int64](vec2)
	for i := 0; i < vec.Length(); i++ {
		ui := uint64(i)
		require.Equal(t, vec2.Nsp.Contains(ui), i%2 != 0)
		require.Equal(t, vec2.Nsp.Contains(ui), vec.Nsp.Contains(ui))
		if !vec2.Nsp.Contains(ui) {
			require.Equal(t, tv1[i], tv2[i])
		}
	}
}
