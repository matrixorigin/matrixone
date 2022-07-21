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
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
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
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	bat := testutil.NewBatch(ts, false, Rows, m)
	for i := 0; i < Rows; i++ {
		ok := mp.Insert(bat.Vecs, i)
		require.Equal(t, true, ok)
	}
	bat.Clean(m)
	require.Equal(t, int64(0), mheap.Size(m))
}

func TestIterator(t *testing.T) {
	mp := NewStrMap(false)
	ts := []types.Type{
		types.New(types.T_int8, 0, 0, 0),
		types.New(types.T_int16, 0, 0, 0),
		types.New(types.T_int32, 0, 0, 0),
		types.New(types.T_int64, 0, 0, 0),
		types.New(types.T_decimal64, 0, 0, 0),
		types.New(types.T_char, 0, 0, 0),
	}
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	bat := testutil.NewBatch(ts, false, Rows, m)
	itr := mp.NewIterator()
	vs, _ := itr.Insert(0, Rows, bat.Vecs, make([]int32, Rows))
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
	vs, _ = itr.Find(0, Rows, bat.Vecs, make([]int32, Rows))
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
	bat.Clean(m)
	require.Equal(t, int64(0), mheap.Size(m))
}
