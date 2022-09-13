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

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
)

func TestReset(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_varchar)})
	Reset(v0)
	require.Equal(t, 0, len(v0.data))
}

func TestClean(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.data = []byte("hello")
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	Clean(v0, mp)
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
	va1, _, _ := types.BuildVarlena([]byte("foo"), nil, nil)
	va2, _, _ := types.BuildVarlena([]byte("bar"), nil, nil)
	v1.Col = []types.Varlena{va1, va2}
	require.Equal(t, 2, Length(v1))
}

func TestSetLength(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.Col = []int8{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v0)
	SetLength(v0, 3)
	require.Equal(t, 3, len(v0.Col.([]int8)))

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	v1.Col = []int16{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v1)
	SetLength(v1, 3)
	require.Equal(t, 3, len(v1.Col.([]int16)))

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	v2.Col = []int32{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v2)
	SetLength(v2, 3)
	require.Equal(t, 3, len(v2.Col.([]int32)))

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	v3.Col = []int64{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v3)
	SetLength(v3, 3)
	require.Equal(t, 3, len(v3.Col.([]int64)))

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	v4.Col = []uint8{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v4)
	SetLength(v4, 3)
	require.Equal(t, 3, len(v4.Col.([]uint8)))

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	v5.Col = []uint16{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v5)
	SetLength(v5, 3)
	require.Equal(t, 3, len(v5.Col.([]uint16)))

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	v6.Col = []uint32{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v6)
	SetLength(v6, 3)
	require.Equal(t, 3, len(v6.Col.([]uint32)))

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	v7.Col = []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v7)
	SetLength(v7, 3)
	require.Equal(t, 3, len(v7.Col.([]uint64)))

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	v8.Col = []float32{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v8)
	SetLength(v8, 3)
	require.Equal(t, 3, len(v8.Col.([]float32)))

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	v9.Col = []float64{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v9)
	SetLength(v9, 3)
	require.Equal(t, 3, len(v9.Col.([]float64)))

	v12 := New(types.Type{Oid: types.T(types.T_date)})
	v12.Col = []types.Date{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v12)
	SetLength(v12, 3)
	require.Equal(t, 3, len(v12.Col.([]types.Date)))

	v13 := New(types.Type{Oid: types.T(types.T_datetime)})
	v13.Col = []types.Datetime{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v13)
	SetLength(v13, 3)
	require.Equal(t, 3, len(v13.Col.([]types.Datetime)))
}

func TestDup(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.data = types.EncodeInt8Slice([]int8{1, 2, 3, 4})
	v0.Col = types.DecodeInt8Slice(v0.data)
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	v0Duplicate, _ := Dup(v0, mp)
	require.Equal(t, v0, v0Duplicate)

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	v1.data = types.EncodeInt16Slice([]int16{1, 2, 3, 4})
	v1.Col = types.DecodeInt16Slice(v1.data)
	v1Duplicate, _ := Dup(v1, mp)
	require.Equal(t, v1, v1Duplicate)

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	v2.data = types.EncodeInt32Slice([]int32{1, 2, 3, 4})
	v2.Col = types.DecodeInt32Slice(v2.data)
	v2Duplicate, _ := Dup(v2, mp)
	require.Equal(t, v2, v2Duplicate)

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	v3.data = types.EncodeInt64Slice([]int64{1, 2, 3, 4})
	v3.Col = types.DecodeInt64Slice(v3.data)
	v3Duplicate, _ := Dup(v3, mp)
	require.Equal(t, v3, v3Duplicate)

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	v4.data = types.EncodeUint8Slice([]uint8{1, 2, 3, 4})
	v4.Col = types.DecodeUint8Slice(v4.data)
	v4Duplicate, _ := Dup(v4, mp)
	require.Equal(t, v4, v4Duplicate)

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	v5.data = types.EncodeUint16Slice([]uint16{1, 2, 3, 4})
	v5.Col = types.DecodeUint16Slice(v5.data)
	v5Duplicate, _ := Dup(v5, mp)
	require.Equal(t, v5, v5Duplicate)

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	v6.data = types.EncodeUint32Slice([]uint32{1, 2, 3, 4})
	v6.Col = types.DecodeUint32Slice(v6.data)
	v6Duplicate, _ := Dup(v6, mp)
	require.Equal(t, v6, v6Duplicate)

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	v7.data = types.EncodeUint64Slice([]uint64{1, 2, 3, 4})
	v7.Col = types.DecodeUint64Slice(v7.data)
	v7Duplicate, _ := Dup(v7, mp)
	require.Equal(t, v7, v7Duplicate)

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	v8.data = types.EncodeFloat32Slice([]float32{1, 2, 3, 4})
	v8.Col = types.DecodeFloat32Slice(v8.data)
	v8Duplicate, _ := Dup(v8, mp)
	require.Equal(t, v8, v8Duplicate)

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	v9.data = types.EncodeFloat64Slice([]float64{1, 2, 3, 4})
	v9.Col = types.DecodeFloat64Slice(v9.data)
	v9Duplicate, _ := Dup(v9, mp)
	require.Equal(t, v9, v9Duplicate)

	v10 := New(types.Type{Oid: types.T(types.T_char)})
	AppendBytes(v10, [][]byte{
		[]byte("hello"),
		[]byte("Gut"),
		[]byte("knoichiwa"),
		[]byte("nihao"),
	}, nil)
	v10Duplicate, err := Dup(v10, mp)
	require.Equal(t, err, nil)
	require.Equal(t, v10.data, v10Duplicate.data)
	require.Equal(t, v10.area, v10Duplicate.area)
	require.Equal(t, MustStrCols(v10), MustStrCols(v10Duplicate))
	require.Equal(t, v10.GetString(0), v10Duplicate.GetString(0))
	require.Equal(t, v10.GetString(1), v10Duplicate.GetString(1))
	require.Equal(t, v10.GetString(2), v10Duplicate.GetString(2))
	require.Equal(t, v10.GetString(3), v10Duplicate.GetString(3))

	v11 := New(types.Type{Oid: types.T(types.T_date)})
	v11.data = types.EncodeDateSlice([]types.Date{1, 2, 3, 4})
	v11.Col = types.DecodeDateSlice(v11.data)
	v11Duplicate, _ := Dup(v11, mp)
	require.Equal(t, v11, v11Duplicate)

	v12 := New(types.Type{Oid: types.T(types.T_datetime)})
	v12.data = types.EncodeDatetimeSlice([]types.Datetime{1, 2, 3, 4})
	v12.Col = types.DecodeDatetimeSlice(v12.data)
	v12Duplicate, _ := Dup(v12, mp)
	require.Equal(t, v12, v12Duplicate)
}

func TestWindow(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.data = types.EncodeInt8Slice([]int8{1, 2, 3, 4, 5, 6, 7, 8})
	v0.Col = types.DecodeInt8Slice(v0.data)
	v0Window := New(types.Type{Oid: types.T(types.T_int8)})
	start, end := 1, 3
	v0Window = Window(v0, start, end, v0Window)
	require.Equal(t, v0.Col.([]int8)[start:end], v0Window.Col)

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	v1.data = types.EncodeInt16Slice([]int16{1, 2, 3, 4, 5, 6, 7, 8})
	v1.Col = types.DecodeInt16Slice(v1.data)
	v1Window := New(types.Type{Oid: types.T(types.T_int16)})
	v1Window = Window(v1, start, end, v1Window)
	require.Equal(t, v1.Col.([]int16)[start:end], v1Window.Col)

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	v2.data = types.EncodeInt32Slice([]int32{1, 2, 3, 4, 5, 6, 7, 8})
	v2.Col = types.DecodeInt32Slice(v2.data)
	v2Window := New(types.Type{Oid: types.T(types.T_int32)})
	v2Window = Window(v2, start, end, v2Window)
	require.Equal(t, v2.Col.([]int32)[start:end], v2Window.Col)

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	v3.data = types.EncodeInt64Slice([]int64{1, 2, 3, 4, 5, 6, 7, 8})
	v3.Col = types.DecodeInt64Slice(v3.data)
	v3Window := New(types.Type{Oid: types.T(types.T_int64)})
	v3Window = Window(v3, start, end, v3Window)
	require.Equal(t, v3.Col.([]int64)[start:end], v3Window.Col)

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	v4.data = types.EncodeUint8Slice([]uint8{1, 2, 3, 4, 5, 6, 7, 8})
	v4.Col = types.DecodeUint8Slice(v4.data)
	v4Window := New(types.Type{Oid: types.T(types.T_uint8)})
	v4Window = Window(v4, start, end, v4Window)
	require.Equal(t, v4.Col.([]uint8)[start:end], v4Window.Col)

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	v5.data = types.EncodeUint16Slice([]uint16{1, 2, 3, 4, 5, 6, 7, 8})
	v5.Col = types.DecodeUint16Slice(v5.data)
	v5Window := New(types.Type{Oid: types.T(types.T_uint16)})
	v5Window = Window(v5, start, end, v5Window)
	require.Equal(t, v5.Col.([]uint16)[start:end], v5Window.Col)

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	v6.data = types.EncodeUint32Slice([]uint32{1, 2, 3, 4, 5, 6, 7, 8})
	v6.Col = types.DecodeUint32Slice(v6.data)
	v6Window := New(types.Type{Oid: types.T(types.T_uint32)})
	v6Window = Window(v6, start, end, v6Window)
	require.Equal(t, v6.Col.([]uint32)[start:end], v6Window.Col)

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	v7.data = types.EncodeUint64Slice([]uint64{1, 2, 3, 4, 5, 6, 7, 8})
	v7.Col = types.DecodeUint64Slice(v7.data)
	v7Window := New(types.Type{Oid: types.T(types.T_uint64)})
	v7Window = Window(v7, start, end, v7Window)
	require.Equal(t, v7.Col.([]uint64)[start:end], v7Window.Col)

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	v8.data = types.EncodeFloat32Slice([]float32{1, 2, 3, 4, 5, 6, 7, 8})
	v8.Col = types.DecodeFloat32Slice(v8.data)
	v8Window := New(types.Type{Oid: types.T(types.T_float32)})
	v8Window = Window(v8, start, end, v8Window)
	require.Equal(t, v8.Col.([]float32)[start:end], v8Window.Col)

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	v9.data = types.EncodeFloat64Slice([]float64{1, 2, 3, 4, 5, 6, 7, 8})
	v9.Col = types.DecodeFloat64Slice(v9.data)
	v9Window := New(types.Type{Oid: types.T(types.T_float64)})
	v9Window = Window(v9, start, end, v9Window)
	require.Equal(t, v9.Col.([]float64)[start:end], v9Window.Col)

	v11 := New(types.Type{Oid: types.T(types.T_char)})
	AppendBytes(v11, [][]byte{
		[]byte("hello"),
		[]byte("Gut"),
		[]byte("konichiwa"),
		[]byte("nihao"),
	}, nil)
	v11Window := New(types.Type{Oid: types.T(types.T_char)})
	v11Window = Window(v11, start, end, v11Window)
	vs11 := MustStrCols(v11)
	ws11 := MustStrCols(v11Window)
	require.Equal(t, vs11[start:end], ws11)

	v12 := New(types.Type{Oid: types.T(types.T_date)})
	v12.data = types.EncodeDateSlice([]types.Date{1, 2, 3, 4, 5, 6, 7, 8})
	v12.Col = types.DecodeDateSlice(v12.data)
	v12Window := New(types.Type{Oid: types.T(types.T_date)})
	v12Window = Window(v12, start, end, v12Window)
	require.Equal(t, v12.Col.([]types.Date)[start:end], v12Window.Col)

	v13 := New(types.Type{Oid: types.T(types.T_datetime)})
	v13.data = types.EncodeDatetimeSlice([]types.Datetime{1, 2, 3, 4, 5, 6, 7, 8})
	v13.Col = types.DecodeDatetimeSlice(v13.data)
	v13Window := New(types.Type{Oid: types.T(types.T_datetime)})
	v13Window = Window(v13, start, end, v13Window)
	require.Equal(t, v13.Col.([]types.Datetime)[start:end], v13Window.Col)
}

func TestAppend(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	int8Slice := []int8{1, 2, 3, 4, 5, 6, 7, 8}
	v0.data = types.EncodeInt8Slice(int8Slice)
	v0.Col = types.DecodeInt8Slice(v0.data)
	appendInt8Slice := []int8{21, 22, 23}
	err := AppendFixed(v0, appendInt8Slice, nil)
	require.NoError(t, err)
	require.Equal(t, append(int8Slice, appendInt8Slice...), v0.Col.([]int8))

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	int16Slice := []int16{1, 2, 3, 4, 5, 6, 7, 8}
	v1.data = types.EncodeInt16Slice(int16Slice)
	v1.Col = types.DecodeInt16Slice(v1.data)
	appendInt16Slice := []int16{21, 22, 23}
	err = AppendFixed(v1, appendInt16Slice, nil)
	require.NoError(t, err)
	require.Equal(t, append(int16Slice, appendInt16Slice...), v1.Col.([]int16))

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	int32Slice := []int32{1, 2, 3, 4, 5, 6, 7, 8}
	v2.data = types.EncodeInt32Slice(int32Slice)
	v2.Col = types.DecodeInt32Slice(v2.data)
	appendInt32Slice := []int32{21, 22, 23}
	err = AppendFixed(v2, appendInt32Slice, nil)
	require.NoError(t, err)
	require.Equal(t, append(int32Slice, appendInt32Slice...), v2.Col.([]int32))

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	int64Slice := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	v3.data = types.EncodeInt64Slice(int64Slice)
	v3.Col = types.DecodeInt64Slice(v3.data)
	appendInt64Slice := []int64{21, 22, 23}
	err = AppendFixed(v3, appendInt64Slice, nil)
	require.NoError(t, err)
	require.Equal(t, append(int64Slice, appendInt64Slice...), v3.Col.([]int64))

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	uint8Slice := []uint8{1, 2, 3, 4, 5, 6, 7, 8}
	v4.data = types.EncodeUint8Slice(uint8Slice)
	v4.Col = types.DecodeUint8Slice(v4.data)
	appendUint8Slice := []uint8{21, 22, 23}
	err = AppendFixed(v4, appendUint8Slice, nil)
	require.NoError(t, err)
	require.Equal(t, append(uint8Slice, appendUint8Slice...), v4.Col.([]uint8))

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	uint16Slice := []uint16{1, 2, 3, 4, 5, 6, 7, 8}
	v5.data = types.EncodeUint16Slice(uint16Slice)
	v5.Col = types.DecodeUint16Slice(v5.data)
	appendUint16Slice := []uint16{21, 22, 23}
	err = AppendFixed(v5, appendUint16Slice, nil)
	require.NoError(t, err)
	require.Equal(t, append(uint16Slice, appendUint16Slice...), v5.Col.([]uint16))

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	uint32Slice := []uint32{1, 2, 3, 4, 5, 6, 7, 8}
	v6.data = types.EncodeUint32Slice(uint32Slice)
	v6.Col = types.DecodeUint32Slice(v6.data)
	appendUint32Slice := []uint32{21, 22, 23}
	err = AppendFixed(v6, appendUint32Slice, nil)
	require.NoError(t, err)
	require.Equal(t, append(uint32Slice, appendUint32Slice...), v6.Col.([]uint32))

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	uint64Slice := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	v7.data = types.EncodeUint64Slice(uint64Slice)
	v7.Col = types.DecodeUint64Slice(v7.data)
	appendUint64Slice := []uint64{21, 22, 23}
	err = AppendFixed(v7, appendUint64Slice, nil)
	require.NoError(t, err)
	require.Equal(t, append(uint64Slice, appendUint64Slice...), v7.Col.([]uint64))

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	float32Slice := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	v8.data = types.EncodeFloat32Slice(float32Slice)
	v8.Col = types.DecodeFloat32Slice(v8.data)
	appendFloat32Slice := []float32{21, 22, 23}
	err = AppendFixed(v8, appendFloat32Slice, nil)
	require.NoError(t, err)
	require.Equal(t, append(float32Slice, appendFloat32Slice...), v8.Col.([]float32))

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	float64Slice := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	v9.data = types.EncodeFloat64Slice(float64Slice)
	v9.Col = types.DecodeFloat64Slice(v9.data)
	appendFloat64Slice := []float64{21, 22, 23}
	err = AppendFixed(v9, appendFloat64Slice, nil)
	require.NoError(t, err)
	require.Equal(t, append(float64Slice, appendFloat64Slice...), v9.Col.([]float64))

	v10 := New(types.Type{Oid: types.T(types.T_date)})
	dateSlice := []types.Date{1, 2, 3, 4, 5, 6, 7, 8}
	v10.data = types.EncodeDateSlice(dateSlice)
	v10.Col = types.DecodeDateSlice(v10.data)
	appendDateSlice := []types.Date{21, 22, 23}
	err = AppendFixed(v10, appendDateSlice, nil)
	require.NoError(t, err)
	require.Equal(t, append(dateSlice, appendDateSlice...), v10.Col.([]types.Date))

	v11 := New(types.Type{Oid: types.T(types.T_datetime)})
	datetimeSlice := []types.Datetime{1, 2, 3, 4, 5, 6, 7, 8}
	v11.data = types.EncodeDatetimeSlice(datetimeSlice)
	v11.Col = types.DecodeDatetimeSlice(v11.data)
	appendDatetimeSlice := []types.Datetime{21, 22, 23}
	err = AppendFixed(v11, appendDatetimeSlice, nil)
	require.NoError(t, err)
	require.Equal(t, append(datetimeSlice, appendDatetimeSlice...), v11.Col.([]types.Datetime))
}

func TestShrink(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.data = types.EncodeInt8Slice([]int8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v0.Col = types.DecodeInt8Slice(v0.data)
	sels := []int64{1, 3, 5}
	Shrink(v0, sels)
	require.Equal(t, []int8{1, 3, 5}, v0.Col.([]int8))

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	v1.data = types.EncodeInt16Slice([]int16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v1.Col = types.DecodeInt16Slice(v1.data)
	Shrink(v1, sels)
	require.Equal(t, []int16{1, 3, 5}, v1.Col.([]int16))

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	v2.data = types.EncodeInt32Slice([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v2.Col = types.DecodeInt32Slice(v2.data)
	Shrink(v2, sels)
	require.Equal(t, []int32{1, 3, 5}, v2.Col.([]int32))

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	v3.data = types.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v3.Col = types.DecodeInt64Slice(v3.data)
	Shrink(v3, sels)
	require.Equal(t, []int64{1, 3, 5}, v3.Col.([]int64))

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	v4.data = types.EncodeUint8Slice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v4.Col = types.DecodeUint8Slice(v4.data)
	Shrink(v4, sels)
	require.Equal(t, []uint8{1, 3, 5}, v4.Col.([]uint8))

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	v5.data = types.EncodeUint16Slice([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v5.Col = types.DecodeUint16Slice(v5.data)
	Shrink(v5, sels)
	require.Equal(t, []uint16{1, 3, 5}, v5.Col.([]uint16))

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	v6.data = types.EncodeUint32Slice([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v6.Col = types.DecodeUint32Slice(v6.data)
	Shrink(v6, sels)
	require.Equal(t, []uint32{1, 3, 5}, v6.Col.([]uint32))

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	v7.data = types.EncodeUint64Slice([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v7.Col = types.DecodeUint64Slice(v7.data)
	Shrink(v7, sels)
	require.Equal(t, []uint64{1, 3, 5}, v7.Col.([]uint64))

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	v8.data = types.EncodeFloat32Slice([]float32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v8.Col = types.DecodeFloat32Slice(v8.data)
	Shrink(v8, sels)
	require.Equal(t, []float32{1, 3, 5}, v8.Col.([]float32))

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	v9.data = types.EncodeFloat64Slice([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v9.Col = types.DecodeFloat64Slice(v9.data)
	Shrink(v9, sels)
	require.Equal(t, []float64{1, 3, 5}, v9.Col.([]float64))

	v11 := New(types.Type{Oid: types.T(types.T_date)})
	v11.data = types.EncodeDateSlice([]types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v11.Col = types.DecodeDateSlice(v11.data)
	Shrink(v11, sels)
	require.Equal(t, []types.Date{1, 3, 5}, v11.Col.([]types.Date))

	v12 := New(types.Type{Oid: types.T(types.T_datetime)})
	v12.data = types.EncodeDatetimeSlice([]types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v12.Col = types.DecodeDatetimeSlice(v12.data)
	Shrink(v12, sels)
	require.Equal(t, []types.Datetime{1, 3, 5}, v12.Col.([]types.Datetime))
}

func TestShuffle(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.data = types.EncodeInt8Slice([]int8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v0.Col = types.DecodeInt8Slice(v0.data)
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	sels := []int64{1, 3, 5}
	err := Shuffle(v0, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []int8{1, 3, 5}, v0.Col.([]int8))

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	v1.data = types.EncodeInt16Slice([]int16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v1.Col = types.DecodeInt16Slice(v1.data)
	err = Shuffle(v1, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []int16{1, 3, 5}, v1.Col.([]int16))

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	v2.data = types.EncodeInt32Slice([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v2.Col = types.DecodeInt32Slice(v2.data)
	err = Shuffle(v2, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []int32{1, 3, 5}, v2.Col.([]int32))

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	v3.data = types.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v3.Col = types.DecodeInt64Slice(v3.data)
	err = Shuffle(v3, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3, 5}, v3.Col.([]int64))

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	v4.data = types.EncodeUint8Slice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v4.Col = types.DecodeUint8Slice(v4.data)
	err = Shuffle(v4, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []uint8{1, 3, 5}, v4.Col.([]uint8))

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	v5.data = types.EncodeUint16Slice([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v5.Col = types.DecodeUint16Slice(v5.data)
	err = Shuffle(v5, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []uint16{1, 3, 5}, v5.Col.([]uint16))

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	v6.data = types.EncodeUint32Slice([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v6.Col = types.DecodeUint32Slice(v6.data)
	err = Shuffle(v6, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []uint32{1, 3, 5}, v6.Col.([]uint32))

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	v7.data = types.EncodeUint64Slice([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v7.Col = types.DecodeUint64Slice(v7.data)
	err = Shuffle(v7, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3, 5}, v7.Col.([]uint64))

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	v8.data = types.EncodeFloat32Slice([]float32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v8.Col = types.DecodeFloat32Slice(v8.data)
	err = Shuffle(v8, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []float32{1, 3, 5}, v8.Col.([]float32))

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	v9.data = types.EncodeFloat64Slice([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v9.Col = types.DecodeFloat64Slice(v9.data)
	err = Shuffle(v9, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []float64{1, 3, 5}, v9.Col.([]float64))

	v11 := New(types.Type{Oid: types.T(types.T_date)})
	v11.data = types.EncodeDateSlice([]types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v11.Col = types.DecodeDateSlice(v11.data)
	err = Shuffle(v11, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Date{1, 3, 5}, v11.Col.([]types.Date))

	v12 := New(types.Type{Oid: types.T(types.T_datetime)})
	v12.data = types.EncodeDatetimeSlice([]types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v12.Col = types.DecodeDatetimeSlice(v12.data)
	err = Shuffle(v12, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Datetime{1, 3, 5}, v12.Col.([]types.Datetime))
}

func TestCopy(t *testing.T) {
	w0 := New(types.Type{Oid: types.T(types.T_char)})
	AppendBytes(w0, [][]byte{
		[]byte("nihao"),
		[]byte("nihao"),
		[]byte("nihao"),
		[]byte("nihao"),
	}, nil)
	v0 := New(types.Type{Oid: types.T(types.T_char)})
	AppendBytes(v0, [][]byte{
		[]byte("hello"),
		[]byte("hello"),
		[]byte("hello"),
		[]byte("hello"),
	}, nil)
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	err := Copy(v0, w0, 2, 0, mp)
	require.NoError(t, err)

	expectvec := New(types.Type{Oid: types.T(types.T_char)})
	AppendBytes(expectvec, [][]byte{
		[]byte("hello"),
		[]byte("hello"),
		[]byte("nihao"),
		[]byte("hello"),
	}, nil)
	require.Equal(t, GetStrVectorValues(expectvec), GetStrVectorValues(v0))
}

func TestUnionOne(t *testing.T) {
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	w0 := New(types.Type{Oid: types.T(types.T_int8)})
	w0.data = types.EncodeInt8Slice([]int8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w0.Col = types.DecodeInt8Slice(w0.data)
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	err := UnionOne(v0, w0, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int8{3}, v0.Col.([]int8))
	v0.data = types.EncodeInt8Slice([]int8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v0.Col = types.DecodeInt8Slice(v0.data)
	err = UnionOne(v0, w0, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v0.Col.([]int8))

	w1 := New(types.Type{Oid: types.T(types.T_int16)})
	w1.data = types.EncodeInt16Slice([]int16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w1.Col = types.DecodeInt16Slice(w1.data)
	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	err = UnionOne(v1, w1, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int16{3}, v1.Col.([]int16))
	v1.data = types.EncodeInt16Slice([]int16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v1.Col = types.DecodeInt16Slice(v1.data)
	err = UnionOne(v1, w1, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v1.Col.([]int16))

	w2 := New(types.Type{Oid: types.T(types.T_int32)})
	w2.data = types.EncodeInt32Slice([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w2.Col = types.DecodeInt32Slice(w2.data)
	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	err = UnionOne(v2, w2, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int32{3}, v2.Col.([]int32))
	v2.data = types.EncodeInt32Slice([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v2.Col = types.DecodeInt32Slice(v2.data)
	err = UnionOne(v2, w2, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v2.Col.([]int32))

	w3 := New(types.Type{Oid: types.T(types.T_int64)})
	w3.data = types.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w3.Col = types.DecodeInt64Slice(w3.data)
	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	err = UnionOne(v3, w3, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{3}, v3.Col.([]int64))
	v3.data = types.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v3.Col = types.DecodeInt64Slice(v3.data)
	err = UnionOne(v3, w3, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v3.Col.([]int64))

	w4 := New(types.Type{Oid: types.T(types.T_uint8)})
	w4.data = types.EncodeUint8Slice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w4.Col = types.DecodeUint8Slice(w4.data)
	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	err = UnionOne(v4, w4, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint8{3}, v4.Col.([]uint8))
	v4.data = types.EncodeUint8Slice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v4.Col = types.DecodeUint8Slice(v4.data)
	err = UnionOne(v4, w4, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v4.Col.([]uint8))

	w5 := New(types.Type{Oid: types.T(types.T_uint16)})
	w5.data = types.EncodeUint16Slice([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w5.Col = types.DecodeUint16Slice(w5.data)
	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	err = UnionOne(v5, w5, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint16{3}, v5.Col.([]uint16))
	v5.data = types.EncodeUint16Slice([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v5.Col = types.DecodeUint16Slice(v5.data)
	err = UnionOne(v5, w5, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v5.Col.([]uint16))

	w6 := New(types.Type{Oid: types.T(types.T_uint32)})
	w6.data = types.EncodeUint32Slice([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w6.Col = types.DecodeUint32Slice(w6.data)
	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	err = UnionOne(v6, w6, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint32{3}, v6.Col.([]uint32))
	v6.data = types.EncodeUint32Slice([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v6.Col = types.DecodeUint32Slice(v6.data)
	err = UnionOne(v6, w6, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v6.Col.([]uint32))

	w7 := New(types.Type{Oid: types.T(types.T_uint64)})
	w7.data = types.EncodeUint64Slice([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w7.Col = types.DecodeUint64Slice(w7.data)
	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	err = UnionOne(v7, w7, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, v7.Col.([]uint64))
	v7.data = types.EncodeUint64Slice([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v7.Col = types.DecodeUint64Slice(v7.data)
	err = UnionOne(v7, w7, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v7.Col.([]uint64))

	w8 := New(types.Type{Oid: types.T(types.T_float32)})
	w8.data = types.EncodeFloat32Slice([]float32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w8.Col = types.DecodeFloat32Slice(w8.data)
	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	err = UnionOne(v8, w8, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []float32{3}, v8.Col.([]float32))
	v8.data = types.EncodeFloat32Slice([]float32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v8.Col = types.DecodeFloat32Slice(v8.data)
	err = UnionOne(v8, w8, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v8.Col.([]float32))

	w9 := New(types.Type{Oid: types.T(types.T_float64)})
	w9.data = types.EncodeFloat64Slice([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w9.Col = types.DecodeFloat64Slice(w9.data)
	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	err = UnionOne(v9, w9, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []float64{3}, v9.Col.([]float64))
	v9.data = types.EncodeFloat64Slice([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v9.Col = types.DecodeFloat64Slice(v9.data)
	err = UnionOne(v9, w9, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v9.Col.([]float64))

	w10 := New(types.Type{Oid: types.T(types.T_char)})
	AppendString(w10, []string{"nihao", "nihao", "nihao", "nihao"}, nil)
	v10 := New(types.Type{Oid: types.T(types.T_char)})
	err = UnionOne(v10, w10, 3, mp)
	require.NoError(t, err)
	v10vals := GetStrVectorValues(v10)
	require.Equal(t, []string{"nihao"}, v10vals)

	v10 = New(types.Type{Oid: types.T(types.T_char)})
	AppendString(v10, []string{"hello", "hello", "hello"}, nil)
	err = UnionOne(v10, w10, 3, mp)
	require.NoError(t, err)
	v10vals = GetStrVectorValues(v10)
	require.Equal(t, []string{"hello", "hello", "hello", "nihao"}, v10vals)

	// Long string test
	astr := "a123456789012345678901234567890"
	bstr := "b123456789012345678901234567890AKQJ1098765432"

	w102 := New(types.Type{Oid: types.T(types.T_char)})
	AppendString(w102, []string{astr, astr, astr, astr}, nil)
	v102 := New(types.Type{Oid: types.T(types.T_char)})
	err = UnionOne(v102, w102, 3, mp)
	require.NoError(t, err)
	v102vals := GetStrVectorValues(v102)
	require.Equal(t, []string{astr}, v102vals)

	v102 = New(types.Type{Oid: types.T(types.T_char)})
	AppendString(v102, []string{bstr, bstr, bstr}, nil)
	err = UnionOne(v102, w102, 3, mp)
	require.NoError(t, err)
	v102vals = GetStrVectorValues(v102)
	require.Equal(t, []string{bstr, bstr, bstr, astr}, v102vals)

	w11 := New(types.Type{Oid: types.T(types.T_date)})
	w11.data = types.EncodeDateSlice([]types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w11.Col = types.DecodeDateSlice(w11.data)
	v11 := New(types.Type{Oid: types.T(types.T_date)})
	err = UnionOne(v11, w11, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Date{3}, v11.Col.([]types.Date))
	v11.data = types.EncodeDateSlice([]types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v11.Col = types.DecodeDateSlice(v11.data)
	err = UnionOne(v11, w11, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v11.Col.([]types.Date))

	w12 := New(types.Type{Oid: types.T(types.T_datetime)})
	w12.data = types.EncodeDatetimeSlice([]types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w12.Col = types.DecodeDatetimeSlice(w12.data)
	v12 := New(types.Type{Oid: types.T(types.T_datetime)})
	err = UnionOne(v12, w12, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Datetime{3}, v12.Col.([]types.Datetime))
	v12.data = types.EncodeDatetimeSlice([]types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v12.Col = types.DecodeDatetimeSlice(v12.data)
	err = UnionOne(v12, w12, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v12.Col.([]types.Datetime))
}

func TestUnionBatch(t *testing.T) {
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	w0 := New(types.Type{Oid: types.T(types.T_int8)})
	w0.data = types.EncodeInt8Slice([]int8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w0.Col = types.DecodeInt8Slice(w0.data)
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	err := UnionBatch(v0, w0, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int8{3, 4}, v0.Col.([]int8))
	v0.data = types.EncodeInt8Slice([]int8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v0.Col = types.DecodeInt8Slice(v0.data)
	err = UnionBatch(v0, w0, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v0.Col.([]int8))

	w1 := New(types.Type{Oid: types.T(types.T_int16)})
	w1.data = types.EncodeInt16Slice([]int16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w1.Col = types.DecodeInt16Slice(w1.data)
	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	err = UnionBatch(v1, w1, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int16{3, 4}, v1.Col.([]int16))
	v1.data = types.EncodeInt16Slice([]int16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v1.Col = types.DecodeInt16Slice(v1.data)
	err = UnionBatch(v1, w1, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v1.Col.([]int16))

	w2 := New(types.Type{Oid: types.T(types.T_int32)})
	w2.data = types.EncodeInt32Slice([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w2.Col = types.DecodeInt32Slice(w2.data)
	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	err = UnionBatch(v2, w2, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int32{3, 4}, v2.Col.([]int32))
	v2.data = types.EncodeInt32Slice([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v2.Col = types.DecodeInt32Slice(v2.data)
	err = UnionBatch(v2, w2, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v2.Col.([]int32))

	w3 := New(types.Type{Oid: types.T(types.T_int64)})
	w3.data = types.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w3.Col = types.DecodeInt64Slice(w3.data)
	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	err = UnionBatch(v3, w3, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, v3.Col.([]int64))
	v3.data = types.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v3.Col = types.DecodeInt64Slice(v3.data)
	err = UnionBatch(v3, w3, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v3.Col.([]int64))

	w4 := New(types.Type{Oid: types.T(types.T_uint8)})
	w4.data = types.EncodeUint8Slice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w4.Col = types.DecodeUint8Slice(w4.data)
	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	err = UnionBatch(v4, w4, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint8{3, 4}, v4.Col.([]uint8))
	v4.data = types.EncodeUint8Slice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v4.Col = types.DecodeUint8Slice(v4.data)
	err = UnionBatch(v4, w4, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v4.Col.([]uint8))

	w5 := New(types.Type{Oid: types.T(types.T_uint16)})
	w5.data = types.EncodeUint16Slice([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w5.Col = types.DecodeUint16Slice(w5.data)
	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	err = UnionBatch(v5, w5, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint16{3, 4}, v5.Col.([]uint16))
	v5.data = types.EncodeUint16Slice([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v5.Col = types.DecodeUint16Slice(v5.data)
	err = UnionBatch(v5, w5, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v5.Col.([]uint16))

	w6 := New(types.Type{Oid: types.T(types.T_uint32)})
	w6.data = types.EncodeUint32Slice([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w6.Col = types.DecodeUint32Slice(w6.data)
	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	err = UnionBatch(v6, w6, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint32{3, 4}, v6.Col.([]uint32))
	v6.data = types.EncodeUint32Slice([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v6.Col = types.DecodeUint32Slice(v6.data)
	err = UnionBatch(v6, w6, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v6.Col.([]uint32))

	w7 := New(types.Type{Oid: types.T(types.T_uint64)})
	w7.data = types.EncodeUint64Slice([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w7.Col = types.DecodeUint64Slice(w7.data)
	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	err = UnionBatch(v7, w7, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 4}, v7.Col.([]uint64))
	v7.data = types.EncodeUint64Slice([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v7.Col = types.DecodeUint64Slice(v7.data)
	err = UnionBatch(v7, w7, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v7.Col.([]uint64))

	w8 := New(types.Type{Oid: types.T(types.T_float32)})
	w8.data = types.EncodeFloat32Slice([]float32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w8.Col = types.DecodeFloat32Slice(w8.data)
	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	err = UnionBatch(v8, w8, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []float32{3, 4}, v8.Col.([]float32))
	v8.data = types.EncodeFloat32Slice([]float32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v8.Col = types.DecodeFloat32Slice(v8.data)
	err = UnionBatch(v8, w8, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v8.Col.([]float32))

	w9 := New(types.Type{Oid: types.T(types.T_float64)})
	w9.data = types.EncodeFloat64Slice([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w9.Col = types.DecodeFloat64Slice(w9.data)
	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	err = UnionBatch(v9, w9, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []float64{3, 4}, v9.Col.([]float64))
	v9.data = types.EncodeFloat64Slice([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v9.Col = types.DecodeFloat64Slice(v9.data)
	err = UnionBatch(v9, w9, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v9.Col.([]float64))

	w10 := New(types.Type{Oid: types.T(types.T_char)})
	AppendString(w10, []string{"nihao", "nihao", "nihao", "nihao"}, nil)
	v10 := New(types.Type{Oid: types.T(types.T_char)})
	err = UnionBatch(v10, w10, 1, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []string{"nihao", "nihao"}, GetStrVectorValues(v10))

	Clean(v10, mp)
	AppendString(v10, []string{"hello", "hello", "hello", "hello"}, nil)
	err = UnionBatch(v10, w10, 1, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []string{"hello", "hello", "hello", "hello", "nihao", "nihao"}, GetStrVectorValues(v10))

	w11 := New(types.Type{Oid: types.T(types.T_date)})
	w11.data = types.EncodeDateSlice([]types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w11.Col = types.DecodeDateSlice(w11.data)
	v11 := New(types.Type{Oid: types.T(types.T_date)})
	err = UnionBatch(v11, w11, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Date{3, 4}, v11.Col.([]types.Date))
	v11.data = types.EncodeDateSlice([]types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v11.Col = types.DecodeDateSlice(v11.data)
	err = UnionBatch(v11, w11, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v11.Col.([]types.Date))

	w12 := New(types.Type{Oid: types.T(types.T_datetime)})
	w12.data = types.EncodeDatetimeSlice([]types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w12.Col = types.DecodeDatetimeSlice(w12.data)
	v12 := New(types.Type{Oid: types.T(types.T_datetime)})
	err = UnionBatch(v12, w12, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Datetime{3, 4}, v12.Col.([]types.Datetime))
	v12.data = types.EncodeDatetimeSlice([]types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v12.Col = types.DecodeDatetimeSlice(v12.data)
	err = UnionBatch(v12, w12, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v12.Col.([]types.Datetime))
}

func TestVector_String(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.data = types.EncodeInt8Slice([]int8{0, 1, 2})
	v0.Col = types.DecodeInt8Slice(v0.data)
	result := v0.String()
	require.Equal(t, "[0 1 2]-&{<nil>}", result)

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	v1.data = types.EncodeInt16Slice([]int16{0, 1, 2})
	v1.Col = types.DecodeInt16Slice(v1.data)
	result = v1.String()
	require.Equal(t, "[0 1 2]-&{<nil>}", result)

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	v2.data = types.EncodeInt32Slice([]int32{0, 1, 2})
	v2.Col = types.DecodeInt32Slice(v2.data)
	result = v2.String()
	require.Equal(t, "[0 1 2]-&{<nil>}", result)

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	v3.data = types.EncodeInt64Slice([]int64{0, 1, 2})
	v3.Col = types.DecodeInt64Slice(v3.data)
	result = v3.String()
	require.Equal(t, "[0 1 2]-&{<nil>}", result)
}

func FillVectorData(v *Vector) {
	switch v.Typ.Oid {
	case types.T_bool:
		v.data = types.EncodeFixedSlice(v.Col.([]bool), 1)
	case types.T_int8:
		v.data = types.EncodeFixedSlice(v.Col.([]int8), 1)
	case types.T_int16:
		v.data = types.EncodeFixedSlice(v.Col.([]int16), 2)
	case types.T_int32:
		v.data = types.EncodeFixedSlice(v.Col.([]int32), 4)
	case types.T_int64:
		v.data = types.EncodeFixedSlice(v.Col.([]int64), 8)
	case types.T_uint8:
		v.data = types.EncodeFixedSlice(v.Col.([]uint8), 1)
	case types.T_uint16:
		v.data = types.EncodeFixedSlice(v.Col.([]uint16), 2)
	case types.T_uint32:
		v.data = types.EncodeFixedSlice(v.Col.([]uint32), 4)
	case types.T_uint64:
		v.data = types.EncodeFixedSlice(v.Col.([]uint64), 8)
	case types.T_float32:
		v.data = types.EncodeFixedSlice(v.Col.([]float32), 4)
	case types.T_float64:
		v.data = types.EncodeFixedSlice(v.Col.([]float64), 8)
	case types.T_date:
		v.data = types.EncodeFixedSlice(v.Col.([]types.Date), 4)
	case types.T_datetime:
		v.data = types.EncodeFixedSlice(v.Col.([]types.Datetime), 8)
	case types.T_timestamp:
		v.data = types.EncodeFixedSlice(v.Col.([]types.Timestamp), 8)
	case types.T_decimal64:
		v.data = types.EncodeFixedSlice(v.Col.([]types.Decimal64), 8)
	case types.T_decimal128:
		v.data = types.EncodeFixedSlice(v.Col.([]types.Decimal128), 16)
	}
}

func TestVector_Marshial(t *testing.T) {
	vals := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	vec := NewWithFixed(types.T_int64.ToType(), vals, nil, nil)
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
