// Copyright 2021 - 2024 Matrix Origin
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

package disttae

import (
	"bytes"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

func Test_GetUncommittedS3Tombstone(t *testing.T) {
	var statsList []objectio.ObjectStats
	for i := 0; i < 3; i++ {
		row := types.RandomRowid()
		stats := objectio.NewObjectStatsWithObjectID(row.BorrowObjectID(), false, false, true)
		objectio.SetObjectStatsRowCnt(stats, uint32(10+i*10))
		statsList = append(statsList, *stats)
	}

	txn := &Transaction{
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
	}

	txn.cn_flushed_s3_tombstone_object_stats_list.Store(statsList[0], nil)
	txn.cn_flushed_s3_tombstone_object_stats_list.Store(statsList[1], nil)
	txn.cn_flushed_s3_tombstone_object_stats_list.Store(statsList[2], nil)

	objectSlice := objectio.ObjectStatsSlice{}

	require.NoError(t, txn.getUncommittedS3Tombstone(func(stats *objectio.ObjectStats) {
		objectSlice.Append(stats[:])
	}))
	require.Equal(t, len(statsList), objectSlice.Len())

	txn.cn_flushed_s3_tombstone_object_stats_list.Range(func(key, value any) bool {
		ss := key.(objectio.ObjectStats)
		found := false
		for i := range objectSlice.Len() {
			if bytes.Equal(ss[:], objectSlice.Get(i)[:]) {
				found = true
				break
			}
		}

		require.True(t, found)
		return true
	})
}

func Test_BatchAllocNewRowIds(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("A", func(t *testing.T) {
		txn := Transaction{
			proc: proc,
		}

		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

		for i := 0; i < 10; i++ {
			ll := rand.Intn(100) + 1
			vec, err := txn.batchAllocNewRowIds(ll)
			require.NoError(t, err)
			require.Equal(t, ll, vec.Length())

			rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](vec)
			require.Equal(t, int(0), int(rowIds[0].GetRowOffset()))
			require.Equal(t, int(ll-1), int(rowIds[len(rowIds)-1].GetRowOffset()))

			vec.Free(common.DefaultAllocator)
		}
	})

	t.Run("B", func(t *testing.T) {
		txn := Transaction{
			proc: proc,
		}

		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

		ll := options.DefaultBlockMaxRows*11 + 1
		mm1 := make(map[types.Blockid]struct{})
		mm2 := make(map[types.Objectid]struct{})

		vec, err := txn.batchAllocNewRowIds(ll)
		require.NoError(t, err)
		require.Equal(t, ll, vec.Length())

		rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](vec)
		for i := range rowIds {
			if i%options.DefaultBlockMaxRows == 0 {
				require.Equal(t, 0, int(rowIds[i].GetRowOffset()))
				if i > 0 {
					require.Equal(t, int(rowIds[i-1].GetBlockOffset()+1), int(rowIds[i].GetBlockOffset()))
					require.Equal(t, int(options.DefaultBlockMaxRows-1), int(rowIds[i-1].GetRowOffset()))
				}
			}

			mm1[*rowIds[i].BorrowBlockID()] = struct{}{}
			mm2[*rowIds[i].BorrowObjectID()] = struct{}{}
		}

		require.Equal(t, 12, len(mm1))
		require.Equal(t, 1, len(mm2))

		vec.Free(common.DefaultAllocator)
	})

	t.Run("C", func(t *testing.T) {
		txn := Transaction{
			proc: proc,
		}

		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

		ll := math.MaxUint16
		for i := 0; i < ll; i++ {
			err := txn.currentRowId.IncrObj()
			require.NoError(t, err)
		}

		for i := 0; i < ll; i++ {
			err := txn.currentRowId.IncrBlk()
			require.NoError(t, err)
		}

		_, err := txn.batchAllocNewRowIds(1)
		require.Error(t, err)
	})
}
