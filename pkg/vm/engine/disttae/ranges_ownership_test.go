// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestRangesOnePartSlowUncommittedOwnership(t *testing.T) {
	ctx := context.Background()
	makeObject := func(id byte) objectio.ObjectStats {
		objID := types.Objectid{id}
		obj := objectio.NewObjectStatsWithObjectID(&objID, false, true, true)
		require.NoError(t, objectio.SetObjectStatsBlkCnt(obj, 1))
		require.NoError(t, objectio.SetObjectStatsRowCnt(obj, 1))
		require.NoError(t, objectio.SetObjectStatsSize(obj, 1))
		return *obj
	}
	uncommitted, committed := makeObject(11), makeObject(22)
	state := logtailreplay.NewPartitionState("", false, 42, false)
	state.UpdateDuration(types.BuildTS(1, 0), types.MaxTs())
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: committed, CreateTime: types.BuildTS(1, 0),
	}, false))
	txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	snapshot := timestamp.Timestamp{PhysicalTime: 2}
	txnOp.EXPECT().SnapshotTS().Return(snapshot).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(&Transaction{engine: &Engine{}}).AnyTimes()
	tbl := &txnTable{db: &txnDatabase{op: txnOp}, tableDef: &plan.TableDef{Name: "test"}}
	proc := testutil.NewProcess(t)
	node := &plan.Node{Stats: plan2.DefaultStats()}
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash
	owner := int32(plan2.SimpleCharHashToRange(uncommitted.ObjectName().ObjectId()[:], 2))
	localCN := 1 - owner
	// An unevaluated fold must retain candidate blocks for residual evaluation,
	// and deliberately uses rangesOnePart's slow path without loading metadata.
	filters := []*plan.Expr{{Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{Id: 0}}}}
	seen := make(map[types.Blockid]int)
	for cn := int32(0); cn < 2; cn++ {
		rsp := &engine.RangesShuffleParam{Node: node, CNCNT: 2, CNIDX: cn, IsLocalCN: cn == localCN}
		param := engine.RangesParam{Rsp: rsp, BlockFilters: filters}
		var workspace []objectio.ObjectStats
		if cn == localCN {
			workspace = []objectio.ObjectStats{uncommitted}
			require.True(t, plan2.ShouldSkipObjByShuffle(rsp, &uncommitted))
		}
		var blocks objectio.BlockInfoSlice
		fast, err := readutil.TryFastFilterBlocks(ctx, snapshot, tbl.tableDef, param, state, nil, workspace, &blocks, nil, nil)
		require.NoError(t, err)
		require.False(t, fast, "regression must exercise the slow path")
		require.Zero(t, blocks.Len())
		require.NoError(t, tbl.rangesOnePart(ctx, state, tbl.tableDef, param, &blocks, proc, workspace))
		var want []types.Blockid
		if cn == localCN {
			want = append(want, uncommitted.ConstructBlockInfo(0).BlockID)
		}
		if !plan2.ShouldSkipObjByShuffle(rsp, &committed) {
			want = append(want, committed.ConstructBlockInfo(0).BlockID)
		}
		var got []types.Blockid
		for i := 0; i < blocks.Len(); i++ {
			id := blocks.Get(i).BlockID
			got = append(got, id)
			seen[id]++
		}
		require.Equal(t, want, got)
	}
	require.Len(t, seen, 2)
	for _, count := range seen {
		require.Equal(t, 1, count)
	}
}
