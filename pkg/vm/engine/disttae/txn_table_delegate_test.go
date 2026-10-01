// Copyright 2021-2024 Matrix Origin
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
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizePKCheckErrorPreservesRollingRestart(t *testing.T) {
	rollingRestart := moerr.NewRetryForCNRollingRestart()
	joined := errors.Join(
		moerr.NewReplicaNotFound("other replica"),
		errors.New("replica read failed"),
		rollingRestart,
	)

	err := normalizePKCheckError(joined)
	require.Same(t, rollingRestart, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrRetryForCNRollingRestart))
	require.Same(t, rollingRestart,
		normalizePKCheckError(fmt.Errorf("wrapped: %w", rollingRestart)))

	other := errors.New("other read failure")
	require.Same(t, other, normalizePKCheckError(other))
	joinedOther := errors.Join(other)
	require.Same(t, joinedOther, normalizePKCheckError(joinedOther))
}

func TestTxnTableDelegate_CollectChanges(t *testing.T) {
	table := &txnTableDelegate{}
	require.False(t, table.IsPartitionedRelation())
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()
	require.True(t, table.IsPartitionedRelation())

	handle, err := table.CollectChanges(
		context.Background(),
		types.TS{},
		types.TS{},
		false,
		&mpool.MPool{},
	)
	assert.NoError(t, err)
	assert.NotNil(t, handle)
	assert.NoError(t, handle.Close())
}

func TestTxnTableDelegateRejectsDelegatedSnapshotReads(t *testing.T) {
	table := &txnTableDelegate{origin: &txnTable{tableId: 42}}
	table.shard.is = true
	table.shard.policy = shard.Policy_Hash
	table.shard.tableID = 42

	local, err := table.CanVisitSnapshotLocally()
	require.NoError(t, err)
	require.False(t, local)
	require.ErrorContains(t, table.VisitSnapshotObjects(context.Background(), types.TS{}, func(objectio.ObjectStats, bool) error {
		t.Fatal("delegated snapshot must not enumerate the origin relation")
		return nil
	}), "delegated snapshot")
	_, err = table.HasSnapshotTombstones(context.Background(), 0, types.TS{})
	require.ErrorContains(t, err, "delegated snapshot")

	table.shard.policy = shard.Policy_Partition
	local, err = table.CanVisitSnapshotLocally()
	require.NoError(t, err)
	require.True(t, local)
}

func TestTxnTableDelegate_UpdateConstraint(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.UpdateConstraint(
			context.Background(),
			&engine.ConstraintDef{},
		)
	})
}

func TestTxnTableDelegate_TableRenameInTxn(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.TableRenameInTxn(
			context.Background(),
			[][]byte{},
		)
	})
}

func TestTxnTableDelegate_MaxAndMinValues(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.MaxAndMinValues(context.Background())
	})
}

func TestTxnTableDelegate_Write(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot write data to partition primary table", func() {
		table.Write(context.Background(), &batch.Batch{})
	})
}

func TestTxnTableDelegate_Delete(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot delete data to partition primary table", func() {
		table.Delete(context.Background(), &batch.Batch{}, "")
	})
}

func TestNonlocalStatsRejectLocalWorkspaceBound(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	origin := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	tbl := &txnTableDelegate{origin: origin, isLocal: func() (bool, error) { return false, nil }}
	bat := batch.NewWithSize(0)
	bat.SetRowCount(5)
	txn.writes = []Entry{{typ: INSERT, databaseId: 7, tableId: 42, bat: bat}}
	stats, err := tbl.Stats(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, float64(^uint64(0)), stats.TableCnt,
		"remote completed metadata cannot include this CN's own writes; do not forward a partial local count")
	require.Empty(t, stats.TableName)
	txn.Lock()
	stats, err = tbl.Stats(context.Background(), false)
	txn.Unlock()
	require.NoError(t, err)
	require.Equal(t, float64(^uint64(0)), stats.TableCnt)
}

// statsReadService exercises the real delegate request/response boundary.
type statsReadService struct {
	shardservice.ShardService
	response []byte
	err      error
}

func (s statsReadService) Read(_ context.Context, req shardservice.ReadRequest, _ shardservice.ReadOptions) error {
	if s.err != nil {
		return s.err
	}
	req.Apply(s.response)
	return nil
}

func TestRemoteStatsPreserveByteWidth(t *testing.T) {
	for _, tc := range []struct {
		name        string
		observation *statsinfo.StatsInfo
		bytes       map[string]uint64
		rows        float64
		width       float64
	}{
		{"anonymous overflow", &statsinfo.StatsInfo{TableCnt: 5, SizeMap: map[string]uint64{"v": 40}}, nil, float64(math.MaxUint64), 6.4},
		{"anonymous representable", &statsinfo.StatsInfo{TableCnt: 8, SizeMap: map[string]uint64{"v": 1}}, map[string]uint64{"v": 1 << 61}, float64(math.MaxUint64), 0.125},
		{"completed", &statsinfo.StatsInfo{TableName: "tbl", TableCnt: 5, SizeMap: map[string]uint64{"v": 40}}, map[string]uint64{"v": 40}, 5, 8},
		{"no response", nil, nil, float64(math.MaxUint64), 6.4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := newTransactionWithActivePKTableForTest(t, "pk")
			origin := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
			op := txn.op.(*mock_frontend.MockTxnOperator)
			op.EXPECT().Snapshot().Return(txnpb.CNTxnSnapshot{}, nil).AnyTimes()
			op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
			origin.remoteWorkspace = true
			txn.proc.Base.TxnOperator = op
			origin.proc.Store(txn.proc)
			var response []byte
			if tc.observation != nil {
				var err error
				response, err = tc.observation.Marshal()
				require.NoError(t, err)
			}
			tbl := &txnTableDelegate{origin: origin, isLocal: func() (bool, error) { return false, nil }}
			tbl.shard.service = statsReadService{response: response}
			got, err := tbl.Stats(t.Context(), true)
			require.NoError(t, err)
			require.Equal(t, tc.rows, got.TableCnt)
			assert.Equal(t, tc.bytes, got.SizeMap)
			assertRelationScanWidth(t, got, tc.width)
			if tc.observation != nil {
				require.Equal(t, tc.observation.TableName, got.TableName)
			}
			failure := errors.New("remote stats unavailable")
			tbl.shard.service = statsReadService{err: failure}
			got, err = tbl.Stats(t.Context(), true)
			require.Nil(t, got)
			require.ErrorIs(t, err, failure)
		})
	}
}
