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

package hashmap

import (
	"io"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestIntHashMapRejectsJoinNaN(t *testing.T) {
	mp := mpool.MustNewZero()
	m, err := NewIntHashMap(false, mp)
	require.NoError(t, err)
	require.NoError(t, m.SetRejectNaN())
	defer func() {
		m.Free()
		require.Zero(t, mp.CurrNB())
	}()

	keys := vector.NewVec(types.T_float64.ToType())
	defer keys.Free(mp)
	require.NoError(t, vector.AppendFixed(keys, math.NaN(), false, mp))
	require.NoError(t, vector.AppendFixed(keys, float64(7), false, mp))

	values, zValues, err := m.NewIterator().Insert(0, 2, []*vector.Vector{keys})
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1}, values)
	require.Equal(t, []int64{1, 1}, zValues)

	encoded, err := m.MarshalBinary()
	require.NoError(t, err)
	restored := &IntHashMap{}
	require.NoError(t, restored.UnmarshalBinary(encoded, mp))
	defer restored.Free()
	require.True(t, restored.rejectNaN)

	values, zValues, err = restored.NewIterator().Find(0, 2, []*vector.Vector{keys})
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1}, values)
	require.Equal(t, []int64{1, 1}, zValues)

	constNaN, err := vector.NewConstFixed(types.T_float64.ToType(), math.NaN(), 2, mp)
	require.NoError(t, err)
	defer constNaN.Free(mp)
	values, zValues, err = restored.NewIterator().Find(
		0, 2, []*vector.Vector{constNaN},
	)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 0}, values)
	require.Equal(t, []int64{1, 1}, zValues)
}

func TestIntHashMapProbeGroupingDoesNotMatchRawKey(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewIntHashMap(false, mp)
	require.NoError(t, err)
	defer hashMap.Free()
	raw := vector.NewVec(types.T_uint8.ToType())
	require.NoError(t, vector.AppendFixed(raw, uint8(0), false, mp))
	grouping := vector.NewRollupConst(types.T_uint8.ToType(), 1, mp)
	defer raw.Free(mp)
	defer grouping.Free(mp)

	iterator := hashMap.NewIterator()
	_, _, err = iterator.Insert(0, 1, []*vector.Vector{raw})
	require.NoError(t, err)
	values, zValues, err := iterator.Find(0, 1, []*vector.Vector{grouping})
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, values)
	require.Equal(t, []int64{0}, zValues)
}

func TestIntHashMapPartialGroupingRowsDoNotMatchRawKeys(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewIntHashMap(false, mp)
	require.NoError(t, err)
	defer hashMap.Free()
	build := vector.NewVec(types.T_int32.ToType())
	probe := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixedList(build, []int32{7, 8}, nil, mp))
	require.NoError(t, vector.AppendFixedList(probe, []int32{7, 8}, nil, mp))
	probe.GetGrouping().Add(0)
	defer build.Free(mp)
	defer probe.Free(mp)

	iterator := hashMap.NewIterator()
	_, _, err = iterator.Insert(0, 2, []*vector.Vector{build})
	require.NoError(t, err)
	values, zValues, err := iterator.Find(0, 2, []*vector.Vector{probe})
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 2}, values)
	require.Equal(t, []int64{0, 1}, zValues)
}

func TestIntHashMap_Iterator(t *testing.T) {
	{
		m := mpool.MustNewZero()
		mp, err := NewIntHashMap(false, m)
		require.NoError(t, err)
		rowCount := 10
		vecs := []*vector.Vector{
			newVector(rowCount, types.T_int32.ToType(), m, false, []int32{
				-1, -1, -1, 2, 2, 2, 3, 3, 3, 4,
			}),
			newVector(rowCount, types.T_uint32.ToType(), m, false, []uint32{
				1, 1, 1, 2, 2, 2, 3, 3, 3, 4,
			}),
		}
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, rowCount, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 1, 1, 2, 2, 2, 3, 3, 3, 4}, vs)
		vs, _, err = itr.Find(0, rowCount, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 1, 1, 2, 2, 2, 3, 3, 3, 4}, vs)
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewIntHashMap(true, m)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0),
			types.New(types.T_int16, 0, 0),
		}
		vecs := newVectorsWithNull(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		vs, _, err = itr.Find(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewIntHashMap(true, m)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_int64, 0, 0),
		}
		vecs := newVectorsWithNull(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		vs, _, err = itr.Find(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewIntHashMap(true, m)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_char, 1, 0),
		}
		vecs := newVectorsWithNull(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		vs, _, err = itr.Find(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewIntHashMap(true, m)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_char, 1, 0),
		}
		vecs := newVectors(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		vs, _, err = itr.Find(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewIntHashMap(false, m)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_char, 1, 0),
		}
		vecs := newVectorsWithNull(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{0, 1, 0, 2, 0, 3, 0, 4, 0, 5}, vs[:Rows])
		vs, _, err = itr.Find(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{0, 1, 0, 2, 0, 3, 0, 4, 0, 5}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
}

func TestIntHashMap_MarshalUnmarshal(t *testing.T) {
	m := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}()

	t.Run("Empty Map", func(t *testing.T) {
		mp, err := NewIntHashMap(false, m)
		require.NoError(t, err)
		defer mp.Free()

		data, err := mp.MarshalBinary()
		require.NoError(t, err)

		unmarshaledMp := &IntHashMap{}
		err = unmarshaledMp.UnmarshalBinary(data, m)
		require.NoError(t, err)
		defer unmarshaledMp.Free()

		require.Equal(t, uint64(0), unmarshaledMp.GroupCount())
		require.Equal(t, mp.HasNull(), unmarshaledMp.HasNull())
	})

	t.Run("Single Element (No Nulls)", func(t *testing.T) {
		mp, err := NewIntHashMap(false, m)
		require.NoError(t, err)
		defer mp.Free()

		rowCount := 1
		vecs := []*vector.Vector{
			newVector(rowCount, types.T_int64.ToType(), m, false, []int64{12345}),
		}
		defer func() {
			for _, vec := range vecs {
				vec.Free(m)
			}
		}()

		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, rowCount, vecs)
		require.NoError(t, err)
		expectedMappedValue := vs
		expectedGroupCount := mp.GroupCount()

		data, err := mp.MarshalBinary()
		require.NoError(t, err)

		unmarshaledMp := &IntHashMap{}
		err = unmarshaledMp.UnmarshalBinary(data, m)
		require.NoError(t, err)
		defer unmarshaledMp.Free()

		require.Equal(t, expectedGroupCount, unmarshaledMp.GroupCount())
		require.Equal(t, mp.HasNull(), unmarshaledMp.HasNull())

		foundVs, _, err := unmarshaledMp.NewIterator().Find(0, rowCount, vecs)
		require.NoError(t, err)
		require.Equal(t, expectedMappedValue, foundVs)
	})

	t.Run("Multiple Elements (With Resize, With Nulls, Mixed Types)", func(t *testing.T) {
		mp, err := NewIntHashMap(true, m) // Test with nulls enabled
		require.NoError(t, err)
		defer mp.Free()

		numElements := 128
		ts := []types.Type{
			types.New(types.T_int32, 0, 0),
			types.New(types.T_uint32, 0, 0),
		}
		vecs := newVectorsWithNull(ts, true, numElements, m) // Random data with nulls
		defer func() {
			for _, vec := range vecs {
				vec.Free(m)
			}
		}()

		itr := mp.NewIterator()
		originalVs, originalZvs, err := itr.Insert(0, numElements, vecs)
		require.NoError(t, err)
		expectedGroupCount := mp.GroupCount()

		data, err := mp.MarshalBinary()
		require.NoError(t, err)

		unmarshaledMp := &IntHashMap{}
		err = unmarshaledMp.UnmarshalBinary(data, m)
		require.NoError(t, err)
		defer unmarshaledMp.Free()

		require.Equal(t, expectedGroupCount, unmarshaledMp.GroupCount())
		require.Equal(t, mp.HasNull(), unmarshaledMp.HasNull())

		foundVs, foundZvs, err := unmarshaledMp.NewIterator().Find(0, numElements, vecs)
		require.NoError(t, err)
		for i := 0; i < numElements; i++ {
			require.Equal(t, originalVs[i], foundVs[i], "Mismatch at index %d for mapped value", i)
			require.Equal(t, originalZvs[i], foundZvs[i], "Mismatch at index %d for zValue", i)
		}
	})

	t.Run("bad input", func(t *testing.T) {
		var m IntHashMap
		err := m.UnmarshalBinary(nil, nil)
		if err != io.EOF {
			t.Fatal()
		}
		err = m.UnmarshalBinary([]byte{1, 0}, nil)
		if err != io.ErrUnexpectedEOF {
			t.Fatalf("got %v", err)
		}
		err = m.UnmarshalBinary([]byte{1, 1, 2, 3, 4, 5, 6, 7, 8, 0}, nil)
		if err != io.ErrUnexpectedEOF {
			t.Fatalf("got %v", err)
		}
	})

}
