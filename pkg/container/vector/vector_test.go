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
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	require.Equal(t, v0, &Vector{Typ: types.Type{Oid: types.T(types.T_int8)}, Col: []int8{},
		Nsp: &nulls.Nulls{}})

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	require.Equal(t, v1, &Vector{Typ: types.Type{Oid: types.T(types.T_int16)}, Col: []int16{},
		Nsp: &nulls.Nulls{}})

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	require.Equal(t, v2, &Vector{Typ: types.Type{Oid: types.T(types.T_int32)}, Col: []int32{},
		Nsp: &nulls.Nulls{}})

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	require.Equal(t, v3, &Vector{Typ: types.Type{Oid: types.T(types.T_int64)}, Col: []int64{},
		Nsp: &nulls.Nulls{}})

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	require.Equal(t, v4, &Vector{Typ: types.Type{Oid: types.T(types.T_uint8)}, Col: []uint8{},
		Nsp: &nulls.Nulls{}})

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	require.Equal(t, v5, &Vector{Typ: types.Type{Oid: types.T(types.T_uint16)}, Col: []uint16{},
		Nsp: &nulls.Nulls{}})

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	require.Equal(t, v6, &Vector{Typ: types.Type{Oid: types.T(types.T_uint32)}, Col: []uint32{},
		Nsp: &nulls.Nulls{}})

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	require.Equal(t, v7, &Vector{Typ: types.Type{Oid: types.T(types.T_uint64)}, Col: []uint64{},
		Nsp: &nulls.Nulls{}})

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	require.Equal(t, v8, &Vector{Typ: types.Type{Oid: types.T(types.T_float32)}, Col: []float32{},
		Nsp: &nulls.Nulls{}})

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	require.Equal(t, v9, &Vector{Typ: types.Type{Oid: types.T(types.T_float64)}, Col: []float64{},
		Nsp: &nulls.Nulls{}})

	v10 := New(types.Type{Oid: types.T(types.T_varchar)})
	require.Equal(t, v10, &Vector{Typ: types.Type{Oid: types.T(types.T_varchar)}, Col: &types.Bytes{},
		Nsp: &nulls.Nulls{}})
}

func TestReset(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_varchar)})
	Reset(v0)
	require.Equal(t, 0, len(v0.Data))
}

func TestFree(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.Ref = 1
	v0.Data = []byte("hello")
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	Free(v0, mp)
	require.Equal(t, uint64(0), v0.Ref)
	require.Equal(t, 0, len(v0.Data))
}

func TestClean(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.Ref = 1
	v0.Data = []byte("hello")
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	Clean(v0, mp)
	require.Equal(t, 0, len(v0.Data))
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
	v1.Col = &types.Bytes{
		Data:    []byte("helloGut"),
		Offsets: []uint32{0, 5},
		Lengths: []uint32{5, 3},
	}
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

	v10 := New(types.Type{Oid: types.T(types.T_sel)})
	v10.Col = []int64{1, 2, 3, 4, 5, 6, 7, 8}
	FillVectorData(v10)
	SetLength(v10, 3)
	require.Equal(t, 3, len(v10.Col.([]int64)))

	v11 := New(types.Type{Oid: types.T(types.T_char)})
	v11.Col = &types.Bytes{
		Data:    []byte("helloGutkonichiwanihao"),
		Offsets: []uint32{0, 5, 8, 17},
		Lengths: []uint32{5, 3, 9, 5},
	}
	SetLength(v11, 3)
	require.Equal(t, 3, len(v11.Col.(*types.Bytes).Offsets))

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
	v0.Data = encoding.EncodeInt8Slice([]int8{1, 2, 3, 4})
	v0.Col = encoding.DecodeInt8Slice(v0.Data)
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	v0Duplicate, _ := Dup(v0, mp)
	require.Equal(t, v0, v0Duplicate)

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	v1.Data = encoding.EncodeInt16Slice([]int16{1, 2, 3, 4})
	v1.Col = encoding.DecodeInt16Slice(v1.Data)
	v1Duplicate, _ := Dup(v1, mp)
	require.Equal(t, v1, v1Duplicate)

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	v2.Data = encoding.EncodeInt32Slice([]int32{1, 2, 3, 4})
	v2.Col = encoding.DecodeInt32Slice(v2.Data)
	v2Duplicate, _ := Dup(v2, mp)
	require.Equal(t, v2, v2Duplicate)

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	v3.Data = encoding.EncodeInt64Slice([]int64{1, 2, 3, 4})
	v3.Col = encoding.DecodeInt64Slice(v3.Data)
	v3Duplicate, _ := Dup(v3, mp)
	require.Equal(t, v3, v3Duplicate)

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	v4.Data = encoding.EncodeUint8Slice([]uint8{1, 2, 3, 4})
	v4.Col = encoding.DecodeUint8Slice(v4.Data)
	v4Duplicate, _ := Dup(v4, mp)
	require.Equal(t, v4, v4Duplicate)

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	v5.Data = encoding.EncodeUint16Slice([]uint16{1, 2, 3, 4})
	v5.Col = encoding.DecodeUint16Slice(v5.Data)
	v5Duplicate, _ := Dup(v5, mp)
	require.Equal(t, v5, v5Duplicate)

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	v6.Data = encoding.EncodeUint32Slice([]uint32{1, 2, 3, 4})
	v6.Col = encoding.DecodeUint32Slice(v6.Data)
	v6Duplicate, _ := Dup(v6, mp)
	require.Equal(t, v6, v6Duplicate)

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	v7.Data = encoding.EncodeUint64Slice([]uint64{1, 2, 3, 4})
	v7.Col = encoding.DecodeUint64Slice(v7.Data)
	v7Duplicate, _ := Dup(v7, mp)
	require.Equal(t, v7, v7Duplicate)

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	v8.Data = encoding.EncodeFloat32Slice([]float32{1, 2, 3, 4})
	v8.Col = encoding.DecodeFloat32Slice(v8.Data)
	v8Duplicate, _ := Dup(v8, mp)
	require.Equal(t, v8, v8Duplicate)

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	v9.Data = encoding.EncodeFloat64Slice([]float64{1, 2, 3, 4})
	v9.Col = encoding.DecodeFloat64Slice(v9.Data)
	v9Duplicate, _ := Dup(v9, mp)
	require.Equal(t, v9, v9Duplicate)

	v10 := New(types.Type{Oid: types.T(types.T_char)})
	v10.Col = &types.Bytes{
		Data:    []byte("helloGutkonichiwanihao"),
		Offsets: []uint32{0, 5, 8, 17},
		Lengths: []uint32{5, 3, 9, 5},
	}
	v10.Data = v10.Col.(*types.Bytes).Data
	v10Duplicate, _ := Dup(v10, mp)
	require.Equal(t, v10, v10Duplicate)

	v11 := New(types.Type{Oid: types.T(types.T_date)})
	v11.Data = encoding.EncodeDateSlice([]types.Date{1, 2, 3, 4})
	v11.Col = encoding.DecodeDateSlice(v11.Data)
	v11Duplicate, _ := Dup(v11, mp)
	require.Equal(t, v11, v11Duplicate)

	v12 := New(types.Type{Oid: types.T(types.T_datetime)})
	v12.Data = encoding.EncodeDatetimeSlice([]types.Datetime{1, 2, 3, 4})
	v12.Col = encoding.DecodeDatetimeSlice(v12.Data)
	v12Duplicate, _ := Dup(v12, mp)
	require.Equal(t, v12, v12Duplicate)
}

func TestWindow(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.Data = encoding.EncodeInt8Slice([]int8{1, 2, 3, 4, 5, 6, 7, 8})
	v0.Col = encoding.DecodeInt8Slice(v0.Data)
	v0Window := New(types.Type{Oid: types.T(types.T_int8)})
	start, end := 1, 3
	v0Window = Window(v0, start, end, v0Window)
	require.Equal(t, v0.Col.([]int8)[start:end], v0Window.Col)

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	v1.Data = encoding.EncodeInt16Slice([]int16{1, 2, 3, 4, 5, 6, 7, 8})
	v1.Col = encoding.DecodeInt16Slice(v1.Data)
	v1Window := New(types.Type{Oid: types.T(types.T_int16)})
	v1Window = Window(v1, start, end, v1Window)
	require.Equal(t, v1.Col.([]int16)[start:end], v1Window.Col)

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	v2.Data = encoding.EncodeInt32Slice([]int32{1, 2, 3, 4, 5, 6, 7, 8})
	v2.Col = encoding.DecodeInt32Slice(v2.Data)
	v2Window := New(types.Type{Oid: types.T(types.T_int32)})
	v2Window = Window(v2, start, end, v2Window)
	require.Equal(t, v2.Col.([]int32)[start:end], v2Window.Col)

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	v3.Data = encoding.EncodeInt64Slice([]int64{1, 2, 3, 4, 5, 6, 7, 8})
	v3.Col = encoding.DecodeInt64Slice(v3.Data)
	v3Window := New(types.Type{Oid: types.T(types.T_int64)})
	v3Window = Window(v3, start, end, v3Window)
	require.Equal(t, v3.Col.([]int64)[start:end], v3Window.Col)

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	v4.Data = encoding.EncodeUint8Slice([]uint8{1, 2, 3, 4, 5, 6, 7, 8})
	v4.Col = encoding.DecodeUint8Slice(v4.Data)
	v4Window := New(types.Type{Oid: types.T(types.T_uint8)})
	v4Window = Window(v4, start, end, v4Window)
	require.Equal(t, v4.Col.([]uint8)[start:end], v4Window.Col)

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	v5.Data = encoding.EncodeUint16Slice([]uint16{1, 2, 3, 4, 5, 6, 7, 8})
	v5.Col = encoding.DecodeUint16Slice(v5.Data)
	v5Window := New(types.Type{Oid: types.T(types.T_uint16)})
	v5Window = Window(v5, start, end, v5Window)
	require.Equal(t, v5.Col.([]uint16)[start:end], v5Window.Col)

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	v6.Data = encoding.EncodeUint32Slice([]uint32{1, 2, 3, 4, 5, 6, 7, 8})
	v6.Col = encoding.DecodeUint32Slice(v6.Data)
	v6Window := New(types.Type{Oid: types.T(types.T_uint32)})
	v6Window = Window(v6, start, end, v6Window)
	require.Equal(t, v6.Col.([]uint32)[start:end], v6Window.Col)

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	v7.Data = encoding.EncodeUint64Slice([]uint64{1, 2, 3, 4, 5, 6, 7, 8})
	v7.Col = encoding.DecodeUint64Slice(v7.Data)
	v7Window := New(types.Type{Oid: types.T(types.T_uint64)})
	v7Window = Window(v7, start, end, v7Window)
	require.Equal(t, v7.Col.([]uint64)[start:end], v7Window.Col)

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	v8.Data = encoding.EncodeFloat32Slice([]float32{1, 2, 3, 4, 5, 6, 7, 8})
	v8.Col = encoding.DecodeFloat32Slice(v8.Data)
	v8Window := New(types.Type{Oid: types.T(types.T_float32)})
	v8Window = Window(v8, start, end, v8Window)
	require.Equal(t, v8.Col.([]float32)[start:end], v8Window.Col)

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	v9.Data = encoding.EncodeFloat64Slice([]float64{1, 2, 3, 4, 5, 6, 7, 8})
	v9.Col = encoding.DecodeFloat64Slice(v9.Data)
	v9Window := New(types.Type{Oid: types.T(types.T_float64)})
	v9Window = Window(v9, start, end, v9Window)
	require.Equal(t, v9.Col.([]float64)[start:end], v9Window.Col)

	v10 := New(types.Type{Oid: types.T(types.T_sel)})
	v10.Data = encoding.EncodeInt64Slice([]int64{1, 2, 3, 4, 5, 6, 7, 8})
	v10.Col = encoding.DecodeInt64Slice(v10.Data)
	v10Window := New(types.Type{Oid: types.T(types.T_sel)})
	v10Window = Window(v10, start, end, v10Window)
	require.Equal(t, v10.Col.([]int64)[start:end], v10Window.Col)

	v11 := New(types.Type{Oid: types.T(types.T_char)})
	v11.Col = &types.Bytes{
		Data:    []byte("helloGutkonichiwanihao"),
		Offsets: []uint32{0, 5, 8, 17},
		Lengths: []uint32{5, 3, 9, 5},
	}
	v11.Data = v11.Col.(*types.Bytes).Data
	v11Window := New(types.Type{Oid: types.T(types.T_char)})
	v11Window = Window(v11, start, end, v11Window)
	require.Equal(t, v11.Col.(*types.Bytes).Offsets[start:end], v11Window.Col.(*types.Bytes).Offsets)
	require.Equal(t, v11.Col.(*types.Bytes).Lengths[start:end], v11Window.Col.(*types.Bytes).Lengths)
	// v11Window = Window(v11, start, end, v11Window)

	v12 := New(types.Type{Oid: types.T(types.T_date)})
	v12.Data = encoding.EncodeDateSlice([]types.Date{1, 2, 3, 4, 5, 6, 7, 8})
	v12.Col = encoding.DecodeDateSlice(v12.Data)
	v12Window := New(types.Type{Oid: types.T(types.T_date)})
	v12Window = Window(v12, start, end, v12Window)
	require.Equal(t, v12.Col.([]types.Date)[start:end], v12Window.Col)

	v13 := New(types.Type{Oid: types.T(types.T_datetime)})
	v13.Data = encoding.EncodeDatetimeSlice([]types.Datetime{1, 2, 3, 4, 5, 6, 7, 8})
	v13.Col = encoding.DecodeDatetimeSlice(v13.Data)
	v13Window := New(types.Type{Oid: types.T(types.T_datetime)})
	v13Window = Window(v13, start, end, v13Window)
	require.Equal(t, v13.Col.([]types.Datetime)[start:end], v13Window.Col)
}

func TestAppend(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	int8Slice := []int8{1, 2, 3, 4, 5, 6, 7, 8}
	v0.Data = encoding.EncodeInt8Slice(int8Slice)
	v0.Col = encoding.DecodeInt8Slice(v0.Data)
	appendInt8Slice := []int8{21, 22, 23}
	err := Append(v0, appendInt8Slice)
	require.NoError(t, err)
	require.Equal(t, append(int8Slice, appendInt8Slice...), v0.Col.([]int8))

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	int16Slice := []int16{1, 2, 3, 4, 5, 6, 7, 8}
	v1.Data = encoding.EncodeInt16Slice(int16Slice)
	v1.Col = encoding.DecodeInt16Slice(v1.Data)
	appendInt16Slice := []int16{21, 22, 23}
	err = Append(v1, appendInt16Slice)
	require.NoError(t, err)
	require.Equal(t, append(int16Slice, appendInt16Slice...), v1.Col.([]int16))

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	int32Slice := []int32{1, 2, 3, 4, 5, 6, 7, 8}
	v2.Data = encoding.EncodeInt32Slice(int32Slice)
	v2.Col = encoding.DecodeInt32Slice(v2.Data)
	appendInt32Slice := []int32{21, 22, 23}
	err = Append(v2, appendInt32Slice)
	require.NoError(t, err)
	require.Equal(t, append(int32Slice, appendInt32Slice...), v2.Col.([]int32))

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	int64Slice := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	v3.Data = encoding.EncodeInt64Slice(int64Slice)
	v3.Col = encoding.DecodeInt64Slice(v3.Data)
	appendInt64Slice := []int64{21, 22, 23}
	err = Append(v3, appendInt64Slice)
	require.NoError(t, err)
	require.Equal(t, append(int64Slice, appendInt64Slice...), v3.Col.([]int64))

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	uint8Slice := []uint8{1, 2, 3, 4, 5, 6, 7, 8}
	v4.Data = encoding.EncodeUint8Slice(uint8Slice)
	v4.Col = encoding.DecodeUint8Slice(v4.Data)
	appendUint8Slice := []uint8{21, 22, 23}
	err = Append(v4, appendUint8Slice)
	require.NoError(t, err)
	require.Equal(t, append(uint8Slice, appendUint8Slice...), v4.Col.([]uint8))

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	uint16Slice := []uint16{1, 2, 3, 4, 5, 6, 7, 8}
	v5.Data = encoding.EncodeUint16Slice(uint16Slice)
	v5.Col = encoding.DecodeUint16Slice(v5.Data)
	appendUint16Slice := []uint16{21, 22, 23}
	err = Append(v5, appendUint16Slice)
	require.NoError(t, err)
	require.Equal(t, append(uint16Slice, appendUint16Slice...), v5.Col.([]uint16))

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	uint32Slice := []uint32{1, 2, 3, 4, 5, 6, 7, 8}
	v6.Data = encoding.EncodeUint32Slice(uint32Slice)
	v6.Col = encoding.DecodeUint32Slice(v6.Data)
	appendUint32Slice := []uint32{21, 22, 23}
	err = Append(v6, appendUint32Slice)
	require.NoError(t, err)
	require.Equal(t, append(uint32Slice, appendUint32Slice...), v6.Col.([]uint32))

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	uint64Slice := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	v7.Data = encoding.EncodeUint64Slice(uint64Slice)
	v7.Col = encoding.DecodeUint64Slice(v7.Data)
	appendUint64Slice := []uint64{21, 22, 23}
	err = Append(v7, appendUint64Slice)
	require.NoError(t, err)
	require.Equal(t, append(uint64Slice, appendUint64Slice...), v7.Col.([]uint64))

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	float32Slice := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	v8.Data = encoding.EncodeFloat32Slice(float32Slice)
	v8.Col = encoding.DecodeFloat32Slice(v8.Data)
	appendFloat32Slice := []float32{21, 22, 23}
	err = Append(v8, appendFloat32Slice)
	require.NoError(t, err)
	require.Equal(t, append(float32Slice, appendFloat32Slice...), v8.Col.([]float32))

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	float64Slice := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	v9.Data = encoding.EncodeFloat64Slice(float64Slice)
	v9.Col = encoding.DecodeFloat64Slice(v9.Data)
	appendFloat64Slice := []float64{21, 22, 23}
	err = Append(v9, appendFloat64Slice)
	require.NoError(t, err)
	require.Equal(t, append(float64Slice, appendFloat64Slice...), v9.Col.([]float64))

	v10 := New(types.Type{Oid: types.T(types.T_date)})
	dateSlice := []types.Date{1, 2, 3, 4, 5, 6, 7, 8}
	v10.Data = encoding.EncodeDateSlice(dateSlice)
	v10.Col = encoding.DecodeDateSlice(v10.Data)
	appendDateSlice := []types.Date{21, 22, 23}
	err = Append(v10, appendDateSlice)
	require.NoError(t, err)
	require.Equal(t, append(dateSlice, appendDateSlice...), v10.Col.([]types.Date))

	v11 := New(types.Type{Oid: types.T(types.T_datetime)})
	datetimeSlice := []types.Datetime{1, 2, 3, 4, 5, 6, 7, 8}
	v11.Data = encoding.EncodeDatetimeSlice(datetimeSlice)
	v11.Col = encoding.DecodeDatetimeSlice(v11.Data)
	appendDatetimeSlice := []types.Datetime{21, 22, 23}
	err = Append(v11, appendDatetimeSlice)
	require.NoError(t, err)
	require.Equal(t, append(datetimeSlice, appendDatetimeSlice...), v11.Col.([]types.Datetime))

	v12 := New(types.Type{Oid: types.T(types.T_sel)})
	selSlice := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	v12.Data = encoding.EncodeInt64Slice(selSlice)
	v12.Col = encoding.DecodeInt64Slice(v12.Data)
	appendInt64Slice = []int64{21, 22, 23}
	err = Append(v12, appendInt64Slice)
	require.NoError(t, err)
	require.Equal(t, append(selSlice, appendInt64Slice...), v12.Col.([]int64))
}

func TestShrink(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.Data = encoding.EncodeInt8Slice([]int8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v0.Col = encoding.DecodeInt8Slice(v0.Data)
	sels := []int64{1, 3, 5}
	Shrink(v0, sels)
	require.Equal(t, []int8{1, 3, 5}, v0.Col.([]int8))

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	v1.Data = encoding.EncodeInt16Slice([]int16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v1.Col = encoding.DecodeInt16Slice(v1.Data)
	Shrink(v1, sels)
	require.Equal(t, []int16{1, 3, 5}, v1.Col.([]int16))

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	v2.Data = encoding.EncodeInt32Slice([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v2.Col = encoding.DecodeInt32Slice(v2.Data)
	Shrink(v2, sels)
	require.Equal(t, []int32{1, 3, 5}, v2.Col.([]int32))

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	v3.Data = encoding.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v3.Col = encoding.DecodeInt64Slice(v3.Data)
	Shrink(v3, sels)
	require.Equal(t, []int64{1, 3, 5}, v3.Col.([]int64))

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	v4.Data = encoding.EncodeUint8Slice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v4.Col = encoding.DecodeUint8Slice(v4.Data)
	Shrink(v4, sels)
	require.Equal(t, []uint8{1, 3, 5}, v4.Col.([]uint8))

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	v5.Data = encoding.EncodeUint16Slice([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v5.Col = encoding.DecodeUint16Slice(v5.Data)
	Shrink(v5, sels)
	require.Equal(t, []uint16{1, 3, 5}, v5.Col.([]uint16))

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	v6.Data = encoding.EncodeUint32Slice([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v6.Col = encoding.DecodeUint32Slice(v6.Data)
	Shrink(v6, sels)
	require.Equal(t, []uint32{1, 3, 5}, v6.Col.([]uint32))

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	v7.Data = encoding.EncodeUint64Slice([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v7.Col = encoding.DecodeUint64Slice(v7.Data)
	Shrink(v7, sels)
	require.Equal(t, []uint64{1, 3, 5}, v7.Col.([]uint64))

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	v8.Data = encoding.EncodeFloat32Slice([]float32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v8.Col = encoding.DecodeFloat32Slice(v8.Data)
	Shrink(v8, sels)
	require.Equal(t, []float32{1, 3, 5}, v8.Col.([]float32))

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	v9.Data = encoding.EncodeFloat64Slice([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v9.Col = encoding.DecodeFloat64Slice(v9.Data)
	Shrink(v9, sels)
	require.Equal(t, []float64{1, 3, 5}, v9.Col.([]float64))

	v10 := New(types.Type{Oid: types.T(types.T_sel)})
	v10.Data = encoding.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v10.Col = encoding.DecodeInt64Slice(v10.Data)
	Shrink(v10, sels)
	require.Equal(t, []int64{1, 3, 5}, v10.Col.([]int64))

	v11 := New(types.Type{Oid: types.T(types.T_date)})
	v11.Data = encoding.EncodeDateSlice([]types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v11.Col = encoding.DecodeDateSlice(v11.Data)
	Shrink(v11, sels)
	require.Equal(t, []types.Date{1, 3, 5}, v11.Col.([]types.Date))

	v12 := New(types.Type{Oid: types.T(types.T_datetime)})
	v12.Data = encoding.EncodeDatetimeSlice([]types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v12.Col = encoding.DecodeDatetimeSlice(v12.Data)
	Shrink(v12, sels)
	require.Equal(t, []types.Datetime{1, 3, 5}, v12.Col.([]types.Datetime))
}

func TestShuffle(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.Data = encoding.EncodeInt8Slice([]int8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v0.Col = encoding.DecodeInt8Slice(v0.Data)
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	sels := []int64{1, 3, 5}
	err := Shuffle(v0, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []int8{1, 3, 5}, v0.Col.([]int8))

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	v1.Data = encoding.EncodeInt16Slice([]int16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v1.Col = encoding.DecodeInt16Slice(v1.Data)
	err = Shuffle(v1, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []int16{1, 3, 5}, v1.Col.([]int16))

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	v2.Data = encoding.EncodeInt32Slice([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v2.Col = encoding.DecodeInt32Slice(v2.Data)
	err = Shuffle(v2, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []int32{1, 3, 5}, v2.Col.([]int32))

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	v3.Data = encoding.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v3.Col = encoding.DecodeInt64Slice(v3.Data)
	err = Shuffle(v3, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3, 5}, v3.Col.([]int64))

	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	v4.Data = encoding.EncodeUint8Slice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v4.Col = encoding.DecodeUint8Slice(v4.Data)
	err = Shuffle(v4, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []uint8{1, 3, 5}, v4.Col.([]uint8))

	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	v5.Data = encoding.EncodeUint16Slice([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v5.Col = encoding.DecodeUint16Slice(v5.Data)
	err = Shuffle(v5, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []uint16{1, 3, 5}, v5.Col.([]uint16))

	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	v6.Data = encoding.EncodeUint32Slice([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v6.Col = encoding.DecodeUint32Slice(v6.Data)
	err = Shuffle(v6, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []uint32{1, 3, 5}, v6.Col.([]uint32))

	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	v7.Data = encoding.EncodeUint64Slice([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v7.Col = encoding.DecodeUint64Slice(v7.Data)
	err = Shuffle(v7, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3, 5}, v7.Col.([]uint64))

	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	v8.Data = encoding.EncodeFloat32Slice([]float32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v8.Col = encoding.DecodeFloat32Slice(v8.Data)
	err = Shuffle(v8, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []float32{1, 3, 5}, v8.Col.([]float32))

	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	v9.Data = encoding.EncodeFloat64Slice([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v9.Col = encoding.DecodeFloat64Slice(v9.Data)
	err = Shuffle(v9, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []float64{1, 3, 5}, v9.Col.([]float64))

	v10 := New(types.Type{Oid: types.T(types.T_sel)})
	v10.Data = encoding.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v10.Col = encoding.DecodeInt64Slice(v10.Data)
	err = Shuffle(v10, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3, 5}, v10.Col.([]int64))

	v11 := New(types.Type{Oid: types.T(types.T_date)})
	v11.Data = encoding.EncodeDateSlice([]types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v11.Col = encoding.DecodeDateSlice(v11.Data)
	err = Shuffle(v11, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Date{1, 3, 5}, v11.Col.([]types.Date))

	v12 := New(types.Type{Oid: types.T(types.T_datetime)})
	v12.Data = encoding.EncodeDatetimeSlice([]types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v12.Col = encoding.DecodeDatetimeSlice(v12.Data)
	err = Shuffle(v12, sels, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Datetime{1, 3, 5}, v12.Col.([]types.Datetime))
}

func TestCopy(t *testing.T) {
	w0 := New(types.Type{Oid: types.T(types.T_char)})
	w0.Col = &types.Bytes{
		Data:    []byte("nihaonihaonihaonihao"),
		Offsets: []uint32{0, 5, 10, 15},
		Lengths: []uint32{5, 5, 5, 5},
	}
	w0.Data = w0.Col.(*types.Bytes).Data
	v0 := New(types.Type{Oid: types.T(types.T_char)})
	v0.Col = &types.Bytes{
		Data:    []byte("hellohellohellohello"),
		Offsets: []uint32{0, 5, 10, 15},
		Lengths: []uint32{5, 5, 5, 5},
	}
	v0.Data = v0.Col.(*types.Bytes).Data
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	err := Copy(v0, w0, 2, 0, mp)
	require.NoError(t, err)
	require.Equal(t, []byte("hellohellonihaohello"), v0.Data)
	v0.Col = &types.Bytes{
		Data:    []byte("hihihihi"),
		Offsets: []uint32{0, 2, 4, 6},
		Lengths: []uint32{2, 2, 2, 2},
	}
	err = Copy(v0, w0, 2, 0, mp)
	require.NoError(t, err)
	require.Equal(t, []byte("hihinihaohi"), v0.Data)
}

func TestUnionOne(t *testing.T) {
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	w0 := New(types.Type{Oid: types.T(types.T_int8)})
	w0.Data = encoding.EncodeInt8Slice([]int8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w0.Col = encoding.DecodeInt8Slice(w0.Data)
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	err := UnionOne(v0, w0, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int8{3}, v0.Col.([]int8))
	v0.Data = encoding.EncodeInt8Slice([]int8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v0.Col = encoding.DecodeInt8Slice(v0.Data)
	err = UnionOne(v0, w0, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v0.Col.([]int8))

	w1 := New(types.Type{Oid: types.T(types.T_int16)})
	w1.Data = encoding.EncodeInt16Slice([]int16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w1.Col = encoding.DecodeInt16Slice(w1.Data)
	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	err = UnionOne(v1, w1, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int16{3}, v1.Col.([]int16))
	v1.Data = encoding.EncodeInt16Slice([]int16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v1.Col = encoding.DecodeInt16Slice(v1.Data)
	err = UnionOne(v1, w1, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v1.Col.([]int16))

	w2 := New(types.Type{Oid: types.T(types.T_int32)})
	w2.Data = encoding.EncodeInt32Slice([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w2.Col = encoding.DecodeInt32Slice(w2.Data)
	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	err = UnionOne(v2, w2, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int32{3}, v2.Col.([]int32))
	v2.Data = encoding.EncodeInt32Slice([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v2.Col = encoding.DecodeInt32Slice(v2.Data)
	err = UnionOne(v2, w2, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v2.Col.([]int32))

	w3 := New(types.Type{Oid: types.T(types.T_int64)})
	w3.Data = encoding.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w3.Col = encoding.DecodeInt64Slice(w3.Data)
	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	err = UnionOne(v3, w3, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{3}, v3.Col.([]int64))
	v3.Data = encoding.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v3.Col = encoding.DecodeInt64Slice(v3.Data)
	err = UnionOne(v3, w3, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v3.Col.([]int64))

	w4 := New(types.Type{Oid: types.T(types.T_uint8)})
	w4.Data = encoding.EncodeUint8Slice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w4.Col = encoding.DecodeUint8Slice(w4.Data)
	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	err = UnionOne(v4, w4, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint8{3}, v4.Col.([]uint8))
	v4.Data = encoding.EncodeUint8Slice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v4.Col = encoding.DecodeUint8Slice(v4.Data)
	err = UnionOne(v4, w4, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v4.Col.([]uint8))

	w5 := New(types.Type{Oid: types.T(types.T_uint16)})
	w5.Data = encoding.EncodeUint16Slice([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w5.Col = encoding.DecodeUint16Slice(w5.Data)
	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	err = UnionOne(v5, w5, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint16{3}, v5.Col.([]uint16))
	v5.Data = encoding.EncodeUint16Slice([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v5.Col = encoding.DecodeUint16Slice(v5.Data)
	err = UnionOne(v5, w5, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v5.Col.([]uint16))

	w6 := New(types.Type{Oid: types.T(types.T_uint32)})
	w6.Data = encoding.EncodeUint32Slice([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w6.Col = encoding.DecodeUint32Slice(w6.Data)
	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	err = UnionOne(v6, w6, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint32{3}, v6.Col.([]uint32))
	v6.Data = encoding.EncodeUint32Slice([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v6.Col = encoding.DecodeUint32Slice(v6.Data)
	err = UnionOne(v6, w6, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v6.Col.([]uint32))

	w7 := New(types.Type{Oid: types.T(types.T_uint64)})
	w7.Data = encoding.EncodeUint64Slice([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w7.Col = encoding.DecodeUint64Slice(w7.Data)
	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	err = UnionOne(v7, w7, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, v7.Col.([]uint64))
	v7.Data = encoding.EncodeUint64Slice([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v7.Col = encoding.DecodeUint64Slice(v7.Data)
	err = UnionOne(v7, w7, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v7.Col.([]uint64))

	w8 := New(types.Type{Oid: types.T(types.T_float32)})
	w8.Data = encoding.EncodeFloat32Slice([]float32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w8.Col = encoding.DecodeFloat32Slice(w8.Data)
	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	err = UnionOne(v8, w8, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []float32{3}, v8.Col.([]float32))
	v8.Data = encoding.EncodeFloat32Slice([]float32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v8.Col = encoding.DecodeFloat32Slice(v8.Data)
	err = UnionOne(v8, w8, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v8.Col.([]float32))

	w9 := New(types.Type{Oid: types.T(types.T_float64)})
	w9.Data = encoding.EncodeFloat64Slice([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w9.Col = encoding.DecodeFloat64Slice(w9.Data)
	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	err = UnionOne(v9, w9, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []float64{3}, v9.Col.([]float64))
	v9.Data = encoding.EncodeFloat64Slice([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v9.Col = encoding.DecodeFloat64Slice(v9.Data)
	err = UnionOne(v9, w9, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v9.Col.([]float64))

	w10 := New(types.Type{Oid: types.T(types.T_char)})
	w10.Col = &types.Bytes{
		Data:    []byte("nihaonihaonihaonihao"),
		Offsets: []uint32{0, 5, 10, 15},
		Lengths: []uint32{5, 5, 5, 5},
	}
	w10.Data = w10.Col.(*types.Bytes).Data
	v10 := New(types.Type{Oid: types.T(types.T_char)})
	err = UnionOne(v10, w10, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []byte("nihao"), v10.Col.(*types.Bytes).Data)
	v10.Col = &types.Bytes{
		Data:    []byte("hellohellohellohello"),
		Offsets: []uint32{0, 5, 10, 15},
		Lengths: []uint32{5, 5, 5, 5},
	}
	v10.Data = v10.Col.(*types.Bytes).Data
	err = UnionOne(v10, w10, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []byte("hellohellohellohellonihao"), v10.Col.(*types.Bytes).Data)

	w11 := New(types.Type{Oid: types.T(types.T_date)})
	w11.Data = encoding.EncodeDateSlice([]types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w11.Col = encoding.DecodeDateSlice(w11.Data)
	v11 := New(types.Type{Oid: types.T(types.T_date)})
	err = UnionOne(v11, w11, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Date{3}, v11.Col.([]types.Date))
	v11.Data = encoding.EncodeDateSlice([]types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v11.Col = encoding.DecodeDateSlice(v11.Data)
	err = UnionOne(v11, w11, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v11.Col.([]types.Date))

	w12 := New(types.Type{Oid: types.T(types.T_datetime)})
	w12.Data = encoding.EncodeDatetimeSlice([]types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w12.Col = encoding.DecodeDatetimeSlice(w12.Data)
	v12 := New(types.Type{Oid: types.T(types.T_datetime)})
	err = UnionOne(v12, w12, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Datetime{3}, v12.Col.([]types.Datetime))
	v12.Data = encoding.EncodeDatetimeSlice([]types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v12.Col = encoding.DecodeDatetimeSlice(v12.Data)
	err = UnionOne(v12, w12, 3, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8, 3}, v12.Col.([]types.Datetime))
}

func TestUnionBatch(t *testing.T) {
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	w0 := New(types.Type{Oid: types.T(types.T_int8)})
	w0.Data = encoding.EncodeInt8Slice([]int8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w0.Col = encoding.DecodeInt8Slice(w0.Data)
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	err := UnionBatch(v0, w0, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int8{3, 4}, v0.Col.([]int8))
	v0.Data = encoding.EncodeInt8Slice([]int8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v0.Col = encoding.DecodeInt8Slice(v0.Data)
	err = UnionBatch(v0, w0, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v0.Col.([]int8))

	w1 := New(types.Type{Oid: types.T(types.T_int16)})
	w1.Data = encoding.EncodeInt16Slice([]int16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w1.Col = encoding.DecodeInt16Slice(w1.Data)
	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	err = UnionBatch(v1, w1, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int16{3, 4}, v1.Col.([]int16))
	v1.Data = encoding.EncodeInt16Slice([]int16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v1.Col = encoding.DecodeInt16Slice(v1.Data)
	err = UnionBatch(v1, w1, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v1.Col.([]int16))

	w2 := New(types.Type{Oid: types.T(types.T_int32)})
	w2.Data = encoding.EncodeInt32Slice([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w2.Col = encoding.DecodeInt32Slice(w2.Data)
	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	err = UnionBatch(v2, w2, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int32{3, 4}, v2.Col.([]int32))
	v2.Data = encoding.EncodeInt32Slice([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v2.Col = encoding.DecodeInt32Slice(v2.Data)
	err = UnionBatch(v2, w2, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v2.Col.([]int32))

	w3 := New(types.Type{Oid: types.T(types.T_int64)})
	w3.Data = encoding.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w3.Col = encoding.DecodeInt64Slice(w3.Data)
	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	err = UnionBatch(v3, w3, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, v3.Col.([]int64))
	v3.Data = encoding.EncodeInt64Slice([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v3.Col = encoding.DecodeInt64Slice(v3.Data)
	err = UnionBatch(v3, w3, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v3.Col.([]int64))

	w4 := New(types.Type{Oid: types.T(types.T_uint8)})
	w4.Data = encoding.EncodeUint8Slice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w4.Col = encoding.DecodeUint8Slice(w4.Data)
	v4 := New(types.Type{Oid: types.T(types.T_uint8)})
	err = UnionBatch(v4, w4, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint8{3, 4}, v4.Col.([]uint8))
	v4.Data = encoding.EncodeUint8Slice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v4.Col = encoding.DecodeUint8Slice(v4.Data)
	err = UnionBatch(v4, w4, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v4.Col.([]uint8))

	w5 := New(types.Type{Oid: types.T(types.T_uint16)})
	w5.Data = encoding.EncodeUint16Slice([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w5.Col = encoding.DecodeUint16Slice(w5.Data)
	v5 := New(types.Type{Oid: types.T(types.T_uint16)})
	err = UnionBatch(v5, w5, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint16{3, 4}, v5.Col.([]uint16))
	v5.Data = encoding.EncodeUint16Slice([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v5.Col = encoding.DecodeUint16Slice(v5.Data)
	err = UnionBatch(v5, w5, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v5.Col.([]uint16))

	w6 := New(types.Type{Oid: types.T(types.T_uint32)})
	w6.Data = encoding.EncodeUint32Slice([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w6.Col = encoding.DecodeUint32Slice(w6.Data)
	v6 := New(types.Type{Oid: types.T(types.T_uint32)})
	err = UnionBatch(v6, w6, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint32{3, 4}, v6.Col.([]uint32))
	v6.Data = encoding.EncodeUint32Slice([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v6.Col = encoding.DecodeUint32Slice(v6.Data)
	err = UnionBatch(v6, w6, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v6.Col.([]uint32))

	w7 := New(types.Type{Oid: types.T(types.T_uint64)})
	w7.Data = encoding.EncodeUint64Slice([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w7.Col = encoding.DecodeUint64Slice(w7.Data)
	v7 := New(types.Type{Oid: types.T(types.T_uint64)})
	err = UnionBatch(v7, w7, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 4}, v7.Col.([]uint64))
	v7.Data = encoding.EncodeUint64Slice([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v7.Col = encoding.DecodeUint64Slice(v7.Data)
	err = UnionBatch(v7, w7, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v7.Col.([]uint64))

	w8 := New(types.Type{Oid: types.T(types.T_float32)})
	w8.Data = encoding.EncodeFloat32Slice([]float32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w8.Col = encoding.DecodeFloat32Slice(w8.Data)
	v8 := New(types.Type{Oid: types.T(types.T_float32)})
	err = UnionBatch(v8, w8, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []float32{3, 4}, v8.Col.([]float32))
	v8.Data = encoding.EncodeFloat32Slice([]float32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v8.Col = encoding.DecodeFloat32Slice(v8.Data)
	err = UnionBatch(v8, w8, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v8.Col.([]float32))

	w9 := New(types.Type{Oid: types.T(types.T_float64)})
	w9.Data = encoding.EncodeFloat64Slice([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w9.Col = encoding.DecodeFloat64Slice(w9.Data)
	v9 := New(types.Type{Oid: types.T(types.T_float64)})
	err = UnionBatch(v9, w9, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []float64{3, 4}, v9.Col.([]float64))
	v9.Data = encoding.EncodeFloat64Slice([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v9.Col = encoding.DecodeFloat64Slice(v9.Data)
	err = UnionBatch(v9, w9, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v9.Col.([]float64))

	w10 := New(types.Type{Oid: types.T(types.T_char)})
	w10.Col = &types.Bytes{
		Data:    []byte("nihaonihaonihaonihao"),
		Offsets: []uint32{0, 5, 10, 15},
		Lengths: []uint32{5, 5, 5, 5},
	}
	w10.Data = w10.Col.(*types.Bytes).Data
	v10 := New(types.Type{Oid: types.T(types.T_char)})
	err = UnionBatch(v10, w10, 1, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []byte("nihaonihao"), v10.Col.(*types.Bytes).Data)
	v10.Col = &types.Bytes{
		Data:    []byte("hellohellohellohello"),
		Offsets: []uint32{0, 5, 10, 15},
		Lengths: []uint32{5, 5, 5, 5},
	}
	v10.Data = v10.Col.(*types.Bytes).Data
	err = UnionBatch(v10, w10, 1, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []byte("hellohellohellohellonihaonihao"), v10.Col.(*types.Bytes).Data)

	w11 := New(types.Type{Oid: types.T(types.T_date)})
	w11.Data = encoding.EncodeDateSlice([]types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w11.Col = encoding.DecodeDateSlice(w11.Data)
	v11 := New(types.Type{Oid: types.T(types.T_date)})
	err = UnionBatch(v11, w11, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Date{3, 4}, v11.Col.([]types.Date))
	v11.Data = encoding.EncodeDateSlice([]types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v11.Col = encoding.DecodeDateSlice(v11.Data)
	err = UnionBatch(v11, w11, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Date{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v11.Col.([]types.Date))

	w12 := New(types.Type{Oid: types.T(types.T_datetime)})
	w12.Data = encoding.EncodeDatetimeSlice([]types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w12.Col = encoding.DecodeDatetimeSlice(w12.Data)
	v12 := New(types.Type{Oid: types.T(types.T_datetime)})
	err = UnionBatch(v12, w12, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Datetime{3, 4}, v12.Col.([]types.Datetime))
	v12.Data = encoding.EncodeDatetimeSlice([]types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8})
	v12.Col = encoding.DecodeDatetimeSlice(v12.Data)
	err = UnionBatch(v12, w12, 3, 2, []uint8{1, 1}, mp)
	require.NoError(t, err)
	require.Equal(t, []types.Datetime{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4}, v12.Col.([]types.Datetime))
}

func TestVector_String(t *testing.T) {
	v0 := New(types.Type{Oid: types.T(types.T_int8)})
	v0.Data = encoding.EncodeInt8Slice([]int8{0, 1, 2})
	v0.Col = encoding.DecodeInt8Slice(v0.Data)
	result := v0.String()
	require.Equal(t, "[0 1 2]-&{<nil>}", result)

	v1 := New(types.Type{Oid: types.T(types.T_int16)})
	v1.Data = encoding.EncodeInt16Slice([]int16{0, 1, 2})
	v1.Col = encoding.DecodeInt16Slice(v1.Data)
	result = v1.String()
	require.Equal(t, "[0 1 2]-&{<nil>}", result)

	v2 := New(types.Type{Oid: types.T(types.T_int32)})
	v2.Data = encoding.EncodeInt32Slice([]int32{0, 1, 2})
	v2.Col = encoding.DecodeInt32Slice(v2.Data)
	result = v2.String()
	require.Equal(t, "[0 1 2]-&{<nil>}", result)

	v3 := New(types.Type{Oid: types.T(types.T_int64)})
	v3.Data = encoding.EncodeInt64Slice([]int64{0, 1, 2})
	v3.Col = encoding.DecodeInt64Slice(v3.Data)
	result = v3.String()
	require.Equal(t, "[0 1 2]-&{<nil>}", result)
}

/*
func TestVector(t *testing.T) {
	v := New(types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0})
	w := New(types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0})
	{
		vs := make([][]byte, 10)
		for i := 0; i < 10; i++ {
			vs[i] = []byte(fmt.Sprintf("%v", i*i))
		}
		vs[9] = []byte("abcd")
		if err := Append(v, vs); err != nil {
			log.Fatal(err)
		}
	}
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	for i := 0; i < 5; i++ {
		if err := UnionOne(w, v, int64(i), mp); err != nil {
			log.Fatal(err)
		}
	}
	{
		fmt.Printf("v: %v\n", v)
		fmt.Printf("w: %v\n", w)
	}
	{
		if err := Copy(w, v, 1, 9, mp); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("w[1] = v[9]: %v\n", w)
	}
	w.Ref = 1
	Free(w, mp)
	fmt.Printf("guest: %v, host: %v\n", gm.Size(), gm.HostSize())
}
*/

func FillVectorData(v *Vector) {
	switch v.Typ.Oid {
	case types.T_bool:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]bool), 1)
	case types.T_int8:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]int8), 1)
	case types.T_int16:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]int16), 2)
	case types.T_int32:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]int32), 4)
	case types.T_int64:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]int64), 8)
	case types.T_uint8:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]uint8), 1)
	case types.T_uint16:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]uint16), 2)
	case types.T_uint32:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]uint32), 4)
	case types.T_uint64:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]uint64), 8)
	case types.T_float32:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]float32), 4)
	case types.T_float64:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]float64), 8)
	case types.T_date:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]types.Date), 4)
	case types.T_datetime:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]types.Datetime), 8)
	case types.T_timestamp:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]types.Timestamp), 8)
	case types.T_decimal64:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]types.Decimal64), 8)
	case types.T_decimal128:
		v.Data = encoding.EncodeFixedSlice(v.Col.([]types.Decimal128), 16)
	}
}
