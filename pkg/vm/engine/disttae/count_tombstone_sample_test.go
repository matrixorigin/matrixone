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
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

func TestSampleCountTombstoneObjects(t *testing.T) {
	ctx := t.Context()
	const (
		dbID    = 11
		tableID = 42
	)
	partition := logtailreplay.NewPartition("", nil, 0, dbID, tableID, nil)
	eng := &Engine{partitions: map[[2]uint64]*logtailreplay.Partition{{dbID, tableID}: partition}}
	txn := &Transaction{engine: eng}
	txn.readOnly.Store(true)
	op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	op.EXPECT().GetWorkspace().Return(txn).AnyTimes()
	snapshot := types.BuildTS(5, 0).ToTimestamp()
	op.EXPECT().SnapshotTS().Return(snapshot).AnyTimes()
	eng.pClient.eng = eng
	eng.pClient.subscribed.m = map[uint64]*subEntry{tableID: {dbID: dbID, state: Subscribed}}
	eng.pClient.receivedLogTailTime.ready.Store(true)
	eng.pClient.receivedLogTailTime.tList = make([]atomic.Value, 1)
	eng.pClient.receivedLogTailTime.tList[0].Store(snapshot.Prev())

	tbl := &txnTable{
		db:      &txnDatabase{databaseId: dbID, op: op},
		tableId: tableID,
		fake:    true,
		relKind: "v", // Supply immutable state to ordinary partition admission in this fixture.
	}
	rel := &txnTableDelegate{origin: tbl}
	sample, err := sampleCountTombstoneObjectsForTest(ctx, rel, 5_000_000)
	require.NoError(t, err)
	require.Empty(t, sample)

	state, publish := partition.MutateState()
	state.UpdateDuration(types.BuildTS(1, 0), types.MaxTs())
	visible := make(map[objectio.ObjectId]bool)
	const totalVisible = 20
	for i := 0; i <= totalVisible; i++ {
		id := objectio.NewObjectid()
		stamp := types.BuildTS(4, 0)
		if i == totalVisible {
			stamp = types.BuildTS(6, 0)
		} else {
			visible[id] = true
		}
		stats := objectio.NewObjectStatsWithObjectID(&id, false, true, true)
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1_000_000))
		require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
		require.NoError(t, objectio.SetObjectStatsSize(stats, 1))
		require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  stamp,
		}, true))
	}
	publish()

	sample, err = sampleCountTombstoneObjectsForTest(ctx, rel, 5_000_000)
	require.NoError(t, err)
	require.Len(t, sample, objectio.MaxObjectCacheProbeIDs)
	for _, id := range sample {
		oid := objectio.ObjectId(id)
		require.True(t, visible[oid], "the sample may only include distinct visible objects")
		delete(visible, oid)
	}
	again, err := sampleCountTombstoneObjectsForTest(ctx, tbl, 5_000_000)
	require.NoError(t, err)
	require.Equal(t, sample, again)

	sample, err = sampleCountTombstoneObjectsForTest(ctx, tbl, totalVisible*1_000_000)
	require.NoError(t, err)
	require.Empty(t, sample, "the threshold is strict and excludes later objects")

	// All entries are invisible at this snapshot. A long run of skipped
	// entries must still poll the budget on each raw index entry.
	traversal := &starCountTraversalBudget{Context: ctx, checksLeft: 3}
	err = partition.Snapshot().VisitVisibleTombstoneObjects(traversal, types.BuildTS(2, 0), func(objectio.ObjectEntry) {
		t.Error("a future tombstone was visited")
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = sampleCountTombstoneObjectsForTest(cancelled, tbl, 0)
	require.ErrorIs(t, err, context.Canceled)

	eng.pClient.receivedLogTailTime.tList[0].Store(timestamp.Timestamp{})
	sample, err = sampleCountTombstoneObjectsForTest(ctx, rel, 5_000_000)
	require.NoError(t, err)
	require.Equal(t, again, sample, "a missing ready snapshot must use ordinary local admission")
	eng.pClient.receivedLogTailTime.tList[0].Store(snapshot.Prev())

	txn.readOnly.Store(false)
	sample, err = sampleCountTombstoneObjectsForTest(ctx, tbl, 0)
	require.NoError(t, err)
	require.Empty(t, sample)
}

func sampleCountTombstoneObjectsForTest(
	ctx context.Context,
	rel interface {
		PrepareCountTombstoneSample(context.Context) (func(context.Context, int) ([][]byte, error), error)
	},
	minRows int,
) ([][]byte, error) {
	sample, err := rel.PrepareCountTombstoneSample(ctx)
	if err != nil || sample == nil {
		return nil, err
	}
	return sample(ctx, minRows)
}

type starCountTraversalBudget struct {
	context.Context
	checksLeft int
}

func (c *starCountTraversalBudget) Err() error {
	if c.checksLeft == 0 {
		return context.DeadlineExceeded
	}
	c.checksLeft--
	return c.Context.Err()
}
