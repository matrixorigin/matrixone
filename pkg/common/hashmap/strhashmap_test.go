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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10
)

func TestInsert(t *testing.T) {
	mp := NewStrMap(false)
	ts := []types.Type{
		types.New(types.T_int8, 0, 0, 0),
		types.New(types.T_int16, 0, 0, 0),
		types.New(types.T_int32, 0, 0, 0),
		types.New(types.T_int64, 0, 0, 0),
		types.New(types.T_decimal64, 0, 0, 0),
		types.New(types.T_char, 0, 0, 0),
	}
	m := testutil.NewMheap()
	bat := testutil.NewBatch(ts, false, Rows, m)
	for i := 0; i < Rows; i++ {
		ok := mp.Insert(bat.Vecs, i)
		require.Equal(t, true, ok)
	}
	bat.Clean(m)
	require.Equal(t, int64(0), m.Size())
}

func TestInertValue(t *testing.T) {
	mp := NewStrMap(false)
	ok := mp.InsertValue(int8(0))
	require.Equal(t, true, ok)
	ok = mp.InsertValue(int16(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(int32(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(int64(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(uint8(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(uint16(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(uint32(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(uint64(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue([]byte{})
	require.Equal(t, false, ok)
	ok = mp.InsertValue(types.Date(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(types.Datetime(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(types.Timestamp(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(types.Decimal64{})
	require.Equal(t, false, ok)
	ok = mp.InsertValue(types.Decimal128{})
	require.Equal(t, false, ok)
}

func TestIterator(t *testing.T) {
	{
		mp := NewStrMap(false)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0, 0),
			types.New(types.T_int16, 0, 0, 0),
			types.New(types.T_int32, 0, 0, 0),
			types.New(types.T_int64, 0, 0, 0),
			types.New(types.T_decimal64, 0, 0, 0),
			types.New(types.T_char, 0, 0, 0),
		}
		m := testutil.NewMheap()
		bat := testutil.NewBatch(ts, false, Rows, m)
		itr := mp.NewIterator(0, 0)
		vs, _ := itr.Insert(0, Rows, bat.Vecs, make([]int32, Rows))
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, bat.Vecs, nil, make([]int32, Rows))
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		bat.Clean(m)
		require.Equal(t, int64(0), m.Size())
	}
	{
		mp := NewStrMap(true)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0, 0),
			types.New(types.T_int16, 0, 0, 0),
			types.New(types.T_int32, 0, 0, 0),
			types.New(types.T_int64, 0, 0, 0),
			types.New(types.T_decimal64, 0, 0, 0),
			types.New(types.T_char, 0, 0, 0),
		}
		m := testutil.NewMheap()
		bat := testutil.NewBatch(ts, false, Rows, m)
		itr := mp.NewIterator(0, 0)
		vs, _ := itr.Insert(0, Rows, bat.Vecs, make([]int32, Rows))
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, bat.Vecs, nil, make([]int32, Rows))
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		bat.Clean(m)
		require.Equal(t, int64(0), m.Size())
	}
	{
		mp := NewStrMap(true)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0, 0),
			types.New(types.T_int16, 0, 0, 0),
			types.New(types.T_int32, 0, 0, 0),
			types.New(types.T_int64, 0, 0, 0),
			types.New(types.T_decimal64, 0, 0, 0),
			types.New(types.T_char, 0, 0, 0),
		}
		m := testutil.NewMheap()
		bat := testutil.NewBatchWithNulls(ts, false, Rows, m)
		itr := mp.NewIterator(0, 0)
		vs, _ := itr.Insert(0, Rows, bat.Vecs, make([]int32, Rows))
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, bat.Vecs, nil, make([]int32, Rows))
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		bat.Clean(m)
		require.Equal(t, int64(0), m.Size())
	}
}
