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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
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

func Test_genRowId(t *testing.T) {
	txn := &Transaction{}
	txn.rowIdGenerator.oid = types.NewObjectid()
	txn.rowIdGenerator.bid = types.NewBlockidWithObjectID(&txn.rowIdGenerator.oid, 0)

	var (
		rowId    types.Rowid
		genRowId func() objectio.Rowid
	)

	for i := 0; i < 100; i++ {
		genRowId = txn.getGenRowIdFunc()

		rowId = genRowId()
		require.Equal(t, uint32(0), rowId.GetRowOffset())
		require.Equal(t, uint16(i), rowId.BorrowBlockID().Sequence())
		require.Equal(t, txn.rowIdGenerator.oid, *rowId.BorrowObjectID())
	}

	genRowId = txn.getGenRowIdFunc()

	mm := make(map[types.Rowid]struct{})
	maxOffset := uint32(0)
	for range options.DefaultBlockMaxRows * 100 {
		rowId = genRowId()
		maxOffset = max(maxOffset, rowId.GetRowOffset())

		_, ok := mm[rowId]
		require.False(t, ok)
		mm[rowId] = struct{}{}
	}

	require.Equal(t, uint32(options.DefaultBlockMaxRows-1), maxOffset)
}

func Benchmark_genRowId(b *testing.B) {
	txn := &Transaction{}
	txn.rowIdGenerator.oid = types.NewObjectid()
	txn.rowIdGenerator.bid = types.NewBlockidWithObjectID(&txn.rowIdGenerator.oid, 0)

	genRowId := txn.getGenRowIdFunc()

	for i := 0; i < b.N; i++ {
		genRowId()
	}
}
