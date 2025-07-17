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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestIntHashMap_Iterator(t *testing.T) {
	{
		m := mpool.MustNewZero()
		mp, err := NewIntHashMap(false)
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
		vs, _ = itr.Find(0, rowCount, vecs)
		require.Equal(t, []uint64{1, 1, 1, 2, 2, 2, 3, 3, 3, 4}, vs)
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewIntHashMap(true)
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
		vs, _ = itr.Find(0, Rows, vecs)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewIntHashMap(true)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_int64, 0, 0),
		}
		vecs := newVectorsWithNull(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, vecs)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewIntHashMap(true)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_char, 1, 0),
		}
		vecs := newVectorsWithNull(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, vecs)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewIntHashMap(true)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_char, 1, 0),
		}
		vecs := newVectors(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, vecs)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewIntHashMap(false)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_char, 1, 0),
		}
		vecs := newVectorsWithNull(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{0, 1, 0, 2, 0, 3, 0, 4, 0, 5}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, vecs)
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
		mp, err := NewIntHashMap(false)
		require.NoError(t, err)
		defer mp.Free()

		data, err := mp.MarshalBinary()
		require.NoError(t, err)

		unmarshaledMp := &IntHashMap{}
		err = unmarshaledMp.UnmarshalBinary(data, hashtable.DefaultAllocator())
		require.NoError(t, err)
		defer unmarshaledMp.Free()

		require.Equal(t, uint64(0), unmarshaledMp.GroupCount())
		require.Equal(t, mp.HasNull(), unmarshaledMp.HasNull())
	})

	t.Run("Single Element (No Nulls)", func(t *testing.T) {
		mp, err := NewIntHashMap(false)
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
		err = unmarshaledMp.UnmarshalBinary(data, hashtable.DefaultAllocator())
		require.NoError(t, err)
		defer unmarshaledMp.Free()

		require.Equal(t, expectedGroupCount, unmarshaledMp.GroupCount())
		require.Equal(t, mp.HasNull(), unmarshaledMp.HasNull())

		foundVs, _ := unmarshaledMp.NewIterator().Find(0, rowCount, vecs)
		require.Equal(t, expectedMappedValue, foundVs)
	})

	t.Run("Multiple Elements (With Resize, With Nulls, Mixed Types)", func(t *testing.T) {
		mp, err := NewIntHashMap(true) // Test with nulls enabled
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
		err = unmarshaledMp.UnmarshalBinary(data, hashtable.DefaultAllocator())
		require.NoError(t, err)
		defer unmarshaledMp.Free()

		require.Equal(t, expectedGroupCount, unmarshaledMp.GroupCount())
		require.Equal(t, mp.HasNull(), unmarshaledMp.HasNull())

		foundVs, foundZvs := unmarshaledMp.NewIterator().Find(0, numElements, vecs)
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
