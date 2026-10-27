//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestM0ReadShardsPreserveBlocksAndBindings(t *testing.T) {
	state := new(int)
	data := readutil.NewBlockListRelationData(1, readutil.WithPartitionState(state))
	for i := 1; i <= 32; i++ {
		block := objectio.BlockInfo{PartitionIdx: int32(i), ObjectFlags: int8(i % 2)}
		block.BlockID[0] = byte(i)
		block.MetaLoc[0] = byte(i ^ 0x55)
		data.AppendBlockInfo(&block)
	}
	tombstones := readutil.NewEmptyTombstoneData()
	require.NoError(t, data.AttachTombstones(tombstones))
	shards := activeM0ReadShards(context.Background(), m0TestAppendTableDef(), data, 8, false)
	seen := make(map[int32]bool)
	for i, shard := range shards {
		require.Same(t, state, shard.(*readutil.BlockListRelData).GetPState())
		require.Same(t, tombstones, shard.GetTombstones())
		live := 0
		for j := 0; j < shard.DataCnt(); j++ {
			block := shard.GetBlockInfo(j)
			require.False(t, seen[block.PartitionIdx])
			seen[block.PartitionIdx] = true
			require.Equal(t, data.GetBlockInfo(int(block.PartitionIdx)), block)
			if block.IsMemBlk() {
				require.Zero(t, i)
				require.Zero(t, j)
			}
			if block.PartitionIdx > 24 {
				live++
			}
		}
		require.Equal(t, 1, live, "clustered live suffix must reach every existing reader")
	}
	require.Len(t, seen, data.DataCnt())
	for i := 0; i < data.DataCnt(); i++ {
		require.True(t, seen[int32(i)])
		require.Equal(t, int32(i), data.GetBlockInfo(i).PartitionIdx)
	}
}

func TestM0ReadShardsKeepOrderedAndUnsupportedSplits(t *testing.T) {
	data := readutil.NewBlockListRelationData(0)
	for i := 1; i <= 9; i++ {
		data.AppendBlockInfo(&objectio.BlockInfo{PartitionIdx: int32(i)})
	}
	for _, test := range []struct {
		name    string
		table   *plan.TableDef
		ordered bool
		count   int
	}{
		{"ordered", m0TestAppendTableDef(), true, 2},
		{"outside-domain", nil, false, 2},
		{"one-reader", m0TestAppendTableDef(), false, 1},
		{"short-ranges", m0TestAppendTableDef(), false, 9},
	} {
		t.Run(test.name, func(t *testing.T) {
			want := data.Split(test.count)
			got := activeM0ReadShards(context.Background(), test.table, data, test.count, test.ordered)
			require.Len(t, got, len(want))
			for i := range want {
				require.Equal(t, want[i].GetBlockInfoSlice(), got[i].GetBlockInfoSlice())
			}
		})
	}
}

func m0TestObjectRanges(t *testing.T, memory bool, blocks uint32) *readutil.ObjListRelData {
	t.Helper()
	data := &readutil.ObjListRelData{NeedFirstEmpty: memory, PState: new(int)}
	if memory {
		data.TotalBlocks = 1
	}
	for i := byte(1); i <= 4; i++ {
		id := objectio.ObjectId{i}
		stats := objectio.NewObjectStatsWithObjectID(&id, false, true, false)
		require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, blocks))
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, blocks*objectio.BlockMaxRows))
		data.AppendObj(stats)
	}
	require.NoError(t, data.AttachTombstones(readutil.NewEmptyTombstoneData()))
	return data
}

func TestM0ReadShardsExpandsObjectListWithoutChangingBindings(t *testing.T) {
	data := m0TestObjectRanges(t, true, 8)
	blockList := data.GetBlockInfoSlice()
	expanded := data.DataSlice(0, blockList.Len())
	want := activeM0ReadShards(context.Background(), m0TestAppendTableDef(), expanded, 8, false)
	got := activeM0ReadShards(context.Background(), m0TestAppendTableDef(), data, 8, false)
	for i := range want {
		require.Equal(t, want[i].GetBlockInfoSlice(), got[i].GetBlockInfoSlice())
		require.Same(t, data.PState, got[i].(*readutil.BlockListRelData).GetPState())
		require.Same(t, data.GetTombstones(), got[i].GetTombstones())
	}
}

func TestM0ReadShardsKeepsObjectRangeShuffle(t *testing.T) {
	data := m0TestObjectRanges(t, false, 16)
	data.Rsp = &engine.RangesShuffleParam{CNCNT: 1, Node: &plan.Node{Stats: &plan.Stats{
		HashmapStats: &plan.HashMapStats{Shuffle: true, ShuffleType: plan.ShuffleType_Range},
	}}}
	want := data.Split(4)
	got := activeM0ReadShards(context.Background(), m0TestAppendTableDef(), data, 4, false)
	for i := range want {
		require.Equal(t, want[i].GetBlockInfoSlice(), got[i].GetBlockInfoSlice())
	}
}
