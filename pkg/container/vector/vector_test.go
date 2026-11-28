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
	"errors"
	"fmt"
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

// An invalid checkpoint is rejected before any mutation, with an invariant
// error that callers never treat as a retryable capacity failure.
func TestRollbackAppendRejectsBeforeMutation(t *testing.T) {
	for _, name := range []string{"length", "area", "negative", "overflow"} {
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			v := NewVec(types.T_varchar.ToType(), mp)
			defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()); require.Zero(t, mp.OnHeapCurrNB()) }()
			require.NoError(t, AppendBytes(v, []byte(strings.Repeat("a", 64)), false, mp))
			v.SetSorted(true)
			cp := v.MakeAppendCheckpoint()
			attempted := 1
			switch name {
			case "length":
				cp.length++
			case "area":
				cp.areaLength++
			case "negative":
				attempted = -1
			case "overflow":
				attempted = math.MaxInt
			}
			err := v.RollbackAppend(cp, attempted)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
			require.False(t, mpool.IsRetryableAllocationCapacity(errors.Join(mpool.ErrAllocationAccountCapacity, err)))
			require.Equal(t, 1, v.Length())
			require.Equal(t, 64, len(v.GetArea()))
			require.True(t, v.GetSorted())
			require.Equal(t, strings.Repeat("a", 64), v.GetStringAt(0))
		})
	}
}

// An append beyond the allocator size limit keeps the error's identity
// through the rollback and keeps the rows already appended.
func TestAppendAllocatorLimitReturnsErrorAndKeepsPrefix(t *testing.T) {
	old := mpool.CapLimit
	mpool.CapLimit = 128
	t.Cleanup(func() { mpool.CapLimit = old })
	mp := mpool.MustNewZero()
	v := NewVec(types.T_varchar.ToType(), mp)
	defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()); require.Zero(t, mp.OnHeapCurrNB()) }()
	first := strings.Repeat("a", 64)
	require.NoError(t, AppendBytes(v, []byte(first), false, mp))
	err := AppendBytes(v, []byte(strings.Repeat("b", 64)), false, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAllocatorLimit)
	require.Equal(t, 1, v.Length())
	require.Equal(t, first, v.GetStringAt(0))
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

func TestSizeAndCapacity(t *testing.T) {
	mp := mpool.MustNewZero()
	fixed := NewVec(types.T_int32.ToType(), mp)
	require.Zero(t, fixed.Size())
	require.NoError(t, AppendFixedList(fixed, []int32{1, 2, 3}, nil, mp))
	require.Equal(t, 3*4, fixed.Size())
	require.GreaterOrEqual(t, fixed.Capacity(), 3)

	long := strings.Repeat("x", types.VarlenaInlineSize+1)
	varlen := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytes(varlen, []byte(long), false, mp))
	require.Equal(t, types.VarlenaSize+len(long), varlen.Size())

	// T_any has no physical width; Capacity must not divide by it.
	untyped := NewVec(types.T_any.ToType(), mp)
	require.Zero(t, untyped.Capacity())

	fixed.Free(mp)
	varlen.Free(mp)
	untyped.Free(mp)
	require.Zero(t, mp.CurrNB())
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
	require.NoError(t, bm.InitWithSize(2, mpool.MustNewZero()))
	bm.AddMany([]uint64{0, 1})
	var bmask bitmap.Bitmap
	require.NoError(t, bmask.InitWith(&bm, bm.MPool()))

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
	require.NoError(t, mask.InitWithSize(3, mpool.MustNewZero()))
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
	require.NoError(t, bm.InitWithSize(2, mpool.MustNewZero()))
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
		require.Equal(t, "00000000-0000-0000-0000-000000000000-0-0", v.RowToString(1))
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
		"an installed descriptor that shares bytes is not disjoint")

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
		"the null bitmap does not change descriptor disjointness")

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

// Exercise every ordered deletion set, including empty, prefix, suffix, and all rows.
func TestShrinkNegateOrderedSubsets(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for n := 0; n <= 7; n++ {
		for mask := 0; mask < 1<<n; mask++ {
			func() {
				v := NewVec(types.T_int64.ToType(), mp)
				defer v.Free(mp)
				values := make([]int64, n)
				sels := make([]int64, 0, n)
				want := make([]int64, 0, n)
				for i := range values {
					values[i] = int64(i + 10)
					if mask&(1<<i) != 0 {
						sels = append(sels, int64(i))
					} else {
						want = append(want, values[i])
					}
				}
				require.NoError(t, AppendFixedList(v, values, nil, mp), "n=%d mask=%d", n, mask)
				require.NoError(t, v.Shrink(sels, true, mp), "n=%d mask=%d", n, mask)
				got := MustFixedColNoTypeCheck[int64](v)
				require.Equal(t, len(want), len(got), "n=%d mask=%d", n, mask)
				for i := range want {
					require.Equal(t, want[i], got[i], "n=%d mask=%d index=%d", n, mask, i)
				}
			}()
		}
	}
	require.Zero(t, mp.CurrNB())
}

func TestVarlenaAreaIsDisjointEdgeCases(t *testing.T) {
	mp := mpool.MustNewZero()
	long := func(c string) []byte { return []byte(strings.Repeat(c, 40)) }

	build := func(t *testing.T) *Vector {
		v := NewVec(types.T_varchar.ToType(), mp)
		for _, c := range []string{"a", "b", "c"} {
			require.NoError(t, AppendBytes(v, long(c), false, mp))
		}
		require.NoError(t, AppendBytes(v, []byte("inline"), false, mp))
		return v
	}

	t.Run("append built", func(t *testing.T) {
		v := build(t)
		defer v.Free(mp)
		require.True(t, v.VarlenaAreaIsDisjoint())
	})

	t.Run("shuffled stays disjoint", func(t *testing.T) {
		v := build(t)
		defer v.Free(mp)
		require.NoError(t, v.Shuffle([]int64{2, 0, 3, 1}, mp))
		require.True(t, v.VarlenaAreaIsDisjoint())
		require.Equal(t, long("c"), v.GetBytesAt(0))
	})

	t.Run("aliased descriptors", func(t *testing.T) {
		v := build(t)
		defer v.Free(mp)
		values := MustFixedColNoTypeCheck[types.Varlena](v)
		values[2] = values[0]
		require.False(t, v.VarlenaAreaIsDisjoint())
		// A NULL row's stale descriptor is not live.
		v.SetNull(2)
		require.True(t, v.VarlenaAreaIsDisjoint())
	})

	t.Run("out of area", func(t *testing.T) {
		v := build(t)
		defer v.Free(mp)
		v.area = v.area[:len(v.area)-1]
		require.False(t, v.VarlenaAreaIsDisjoint())
	})

	t.Run("const and fixed never report", func(t *testing.T) {
		c, err := NewConstBytes(types.T_varchar.ToType(), long("z"), 3, mp)
		require.NoError(t, err)
		defer c.Free(mp)
		require.False(t, c.VarlenaAreaIsDisjoint())
		f := NewVec(types.T_int64.ToType(), mp)
		require.False(t, f.VarlenaAreaIsDisjoint())
	})
}

func TestTypedReadLogicalBoundsAndConstantRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	flat := NewVec(types.T_int64.ToType(), mp)
	defer flat.Free(mp)
	require.NoError(t, flat.PreExtend(32, mp))
	require.NoError(t, AppendFixedList(flat, []int64{11, 22}, nil, mp))
	require.Equal(t, int64(11), GetFixedAtNoTypeCheck[int64](flat, 0))
	require.Equal(t, int64(22), GetFixedAtNoTypeCheck[int64](flat, 1))
	for _, row := range []int{-1, 2, 31} {
		require.Panics(t, func() { _ = GetFixedAtNoTypeCheck[int64](flat, row) })
	}
	constant, err := NewConstFixed(types.T_int64.ToType(), int64(42), 2, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	for _, row := range []int{-1, 0, 1, 100} {
		require.Equal(t, int64(42), GetFixedAtNoTypeCheck[int64](constant, row))
	}
	flat.ResetWithSameType()
	require.Panics(t, func() { _ = GetFixedAtNoTypeCheck[int64](flat, 0) })
	constantNull := NewConstNull(types.T_int64.ToType(), 2, mp)
	defer constantNull.Free(mp)
	require.Panics(t, func() { _ = GetFixedAtNoTypeCheck[int64](constantNull, 0) })
}

func TestTypedReadBytesRetainsAliasesAndBounds(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	vec := NewVec(types.T_varbinary.ToType(), mp)
	defer vec.Free(mp)
	require.NoError(t, vec.PreExtend(32, mp))
	values := [][]byte{[]byte("inline"), bytes.Repeat([]byte("x"), 64), {}}
	require.NoError(t, AppendBytesList(vec, values, nil, mp))
	for row, want := range values {
		require.Equal(t, want, vec.GetBytesAt(row))
	}
	for _, row := range []int{0, 1} {
		value := vec.GetBytesAt(row)
		value[0] = 'z'
		require.Equal(t, byte('z'), vec.GetBytesAt(row)[0])
	}
	for _, row := range []int{-1, 3, 31} {
		require.Panics(t, func() { _ = vec.GetBytesAt(row) })
	}
	constant, err := NewConstBytes(types.T_varbinary.ToType(), []byte("constant"), 2, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	for _, row := range []int{-1, 0, 1, 100} {
		require.Equal(t, []byte("constant"), constant.GetBytesAt(row))
	}
	vec.ResetWithSameType()
	require.Panics(t, func() { _ = vec.GetBytesAt(0) })
}

var typedReadBenchSink int64

func BenchmarkVectorTypedRead(b *testing.B) {
	const rows = 2048
	for _, constant := range []bool{false, true} {
		name := "int64_flat_2048"
		if constant {
			name = "int64_const_2048"
		}
		b.Run(name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			vec := NewVec(types.T_int64.ToType(), mp)
			defer func() { vec.Free(mp) }()
			if constant {
				vec.Free(mp)
				var err error
				vec, err = NewConstFixed(types.T_int64.ToType(), int64(42), rows, mp)
				require.NoError(b, err)
			} else {
				values := make([]int64, rows)
				for i := range values {
					values[i] = 42
				}
				require.NoError(b, AppendFixedList(vec, values, nil, mp))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var sum int64
				for row := 0; row < rows; row++ {
					sum += GetFixedAtNoTypeCheck[int64](vec, row)
				}
				typedReadBenchSink = sum
			}
			b.StopTimer()
			require.Equal(b, int64(rows*42), typedReadBenchSink)
		})
	}
	b.Run("bool_flat_2048", func(b *testing.B) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		vec := NewVec(types.T_bool.ToType(), mp)
		defer vec.Free(mp)
		values := make([]bool, rows)
		for row := range values {
			values[row] = row%2 == 0
		}
		require.NoError(b, AppendFixedList(vec, values, nil, mp))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var sum int64
			for row := 0; row < rows; row++ {
				if GetFixedAtNoTypeCheck[bool](vec, row) {
					sum++
				}
			}
			typedReadBenchSink = sum
		}
		b.StopTimer()
		require.Equal(b, int64(rows/2), typedReadBenchSink)
	})
	for _, size := range []int{6, 64} {
		name := "bytes_inline_2048"
		if size == 64 {
			name = "bytes_area_2048"
		}
		b.Run(name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			vec := NewVec(types.T_varbinary.ToType(), mp)
			defer vec.Free(mp)
			values := make([][]byte, rows)
			for row := range values {
				values[row] = bytes.Repeat([]byte("x"), size)
			}
			require.NoError(b, AppendBytesList(vec, values, nil, mp))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var sum int64
				for row := 0; row < rows; row++ {
					value := vec.GetBytesAt(row)
					sum += int64(len(value)) + int64(value[0])
				}
				typedReadBenchSink = sum
			}
			b.StopTimer()
			require.Equal(b, int64(rows*(size+int('x'))), typedReadBenchSink)
		})
	}
}

// exerciseNarrowArray drives the generic narrow-vector (bf16/f16/int8/uint8) array
// functions added for quantized vector columns: Append*, GetArrayAt, NewConstArray,
// SetConstArray, and the String()/RowToString() narrow branches.
func exerciseNarrowArray[T types.ArrayElement](t *testing.T, oid types.T, a, b []T) {
	mp := mpool.MustNewZero()
	dim := int32(len(a))

	// AppendArray (value + null) then AppendArrayList
	vec := NewVec(types.New(oid, dim, 0), mp)
	require.NoError(t, AppendArray[T](vec, a, false, mp))
	require.NoError(t, AppendArray[T](vec, nil, true, mp)) // null row
	require.NoError(t, AppendArrayList[T](vec, [][]T{a, b}, nil, mp))
	require.Equal(t, a, GetArrayAt[T](vec, 0))
	require.Equal(t, b, GetArrayAt[T](vec, 3))
	_ = vec.String()       // String() narrow branch (multi-row + null bitmap)
	_ = vec.RowToString(0) // RowToString -> implArrayRowToString narrow branch
	_ = vec.RowToString(2)
	vec.Free(mp)

	// NewConstArray + GetArrayAt + single-row String()
	cv, err := NewConstArray[T](types.New(oid, dim, 0), a, 2, mp)
	require.NoError(t, err)
	require.Equal(t, a, GetArrayAt[T](cv, 0))
	_ = cv.String()
	_ = cv.RowToString(0)
	cv.Free(mp)

	// SetConstArray
	sv := NewVec(types.New(oid, dim, 0), mp)
	require.NoError(t, SetConstArray[T](sv, b, 3, mp))
	require.Equal(t, b, GetArrayAt[T](sv, 0))
	sv.Free(mp)

	// single-row String()/RowToString — the len(col)==1 narrow branch
	one := NewVec(types.New(oid, dim, 0), mp)
	require.NoError(t, AppendArray[T](one, a, false, mp))
	_ = one.String()
	_ = one.RowToString(0)
	one.Free(mp)

	// InplaceSort narrow branch (unsorted input with a duplicate)
	srt := NewVec(types.New(oid, dim, 0), mp)
	require.NoError(t, AppendArrayList[T](srt, [][]T{b, a, a}, nil, mp))
	require.NoError(t, srt.InplaceSort(mp))
	srt.Free(mp)

	// InplaceSortAndCompact narrow branch (sort + dedup)
	cmp := NewVec(types.New(oid, dim, 0), mp)
	require.NoError(t, AppendArrayList[T](cmp, [][]T{b, a, a}, nil, mp))
	require.NoError(t, cmp.InplaceSortAndCompact(mp))
	cmp.Free(mp)

	// GetMinMaxValue narrow branch
	mm := NewVec(types.New(oid, dim, 0), mp)
	require.NoError(t, AppendArrayList[T](mm, [][]T{a, b}, nil, mp))
	_, _, _ = mm.GetMinMaxValue()
	mm.Free(mp)
}

func TestNarrowArrayVectorOps(t *testing.T) {
	t.Run("bf16", func(t *testing.T) {
		exerciseNarrowArray(t, types.T_array_bf16,
			types.Float32ToBF16Slice([]float32{1, 2, 3}), types.Float32ToBF16Slice([]float32{4, 5, 6}))
	})
	t.Run("f16", func(t *testing.T) {
		exerciseNarrowArray(t, types.T_array_float16,
			types.Float32ToFloat16Slice([]float32{1, 2, 3}), types.Float32ToFloat16Slice([]float32{4, 5, 6}))
	})
	t.Run("int8", func(t *testing.T) {
		exerciseNarrowArray(t, types.T_array_int8, []int8{1, 2, 3}, []int8{4, 5, 6})
	})
	t.Run("uint8", func(t *testing.T) {
		exerciseNarrowArray(t, types.T_array_uint8, []uint8{1, 2, 3}, []uint8{4, 5, 6})
	})
}

// A read-only vector can own its nulls bitmap (DetachNulls). WindowInto
// would drop that bitmap without freeing it, so it rejects such a target.
func TestWindowIntoRejectsTargetOwningNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	nspMP := mpool.MustNewZero()
	src := NewVec(types.T_int64.ToType(), mp)
	for i := 0; i < 8; i++ {
		require.NoError(t, AppendFixed(src, int64(i), i%2 == 0, mp))
	}
	w, err := src.Window(0, 8)
	require.NoError(t, err)
	require.True(t, w.IsReadOnly())
	require.NoError(t, w.DetachNulls(nspMP))
	require.False(t, w.GetNulls().IsReadOnly())
	require.Greater(t, nspMP.CurrNB(), int64(0))

	require.Error(t, src.WindowInto(w, 0, 4))
	require.True(t, w.GetNulls().Contains(0), "the rejected target is unchanged")

	w.Free(nil)
	require.Zero(t, nspMP.CurrNB())
	var fresh Vector
	require.NoError(t, src.WindowInto(&fresh, 0, 4))
	require.Equal(t, 4, fresh.Length())
	require.True(t, fresh.GetNulls().Contains(2))
	src.Free(mp)
	require.Zero(t, mp.CurrNB())
}
