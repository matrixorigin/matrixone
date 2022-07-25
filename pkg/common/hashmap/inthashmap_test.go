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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIntHashMap_Iterator(t *testing.T) {
	mp := NewIntHashMap(false)
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	rowCount := 10
	bat := &batch.Batch{
		Cnt: 1,
		Vecs: []*vector.Vector{
			testutil.NewVector(rowCount, types.T_int32.ToType(), m, false, []int32{
				-1, -1, -1, 2, 2, 2, 3, 3, 3, 4,
			}),
			testutil.NewVector(rowCount, types.T_uint32.ToType(), m, false, []uint32{
				1, 1, 1, 2, 2, 2, 3, 3, 3, 4,
			}),
		},
		Zs: testutil.MakeBatchZs(rowCount, false),
	}
	itr := mp.NewIterator()
	vs, _ := itr.Insert(0, rowCount, bat.Vecs, make([]int32, rowCount))
	require.Equal(t, []uint64{1, 1, 1, 2, 2, 2, 3, 3, 3, 4}, vs)
	vs, _ = itr.Find(0, rowCount, bat.Vecs, make([]int32, rowCount))
	require.Equal(t, []uint64{1, 1, 1, 2, 2, 2, 3, 3, 3, 4}, vs)
	bat.Clean(m)
	require.Equal(t, int64(0), mheap.Size(m))
}
