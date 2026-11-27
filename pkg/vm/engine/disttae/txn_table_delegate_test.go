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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pkReadTestTransport struct {
	shardservice.ShardService
	read func(context.Context, shardservice.ReadRequest) error
}

func (s pkReadTestTransport) Read(ctx context.Context, req shardservice.ReadRequest, _ shardservice.ReadOptions) error {
	return s.read(ctx, req)
}

func TestRemotePKCheckReportsUnavailableSubstages(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%t", canceled), func(t *testing.T) {
			txn := newTransactionWithActivePKTableForTest(t, "pk")
			origin := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
			op := txn.op.(*mock_frontend.MockTxnOperator)
			op.EXPECT().Snapshot().Return(txnpb.CNTxnSnapshot{}, nil).AnyTimes()
			op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
			origin.remoteWorkspace = true
			txn.proc.Base.TxnOperator = op
			origin.proc.Store(txn.proc)
			remote := &txnTableDelegate{origin: origin, isLocal: func() (bool, error) { return false, nil }}

			base, cancel := context.WithCancel(t.Context())
			defer cancel()
			remote.shard.service = pkReadTestTransport{read: func(ctx context.Context, req shardservice.ReadRequest) error {
				require.Equal(t, shardservice.ReadPrimaryKeysMayBeModified, req.Method)
				require.NotEmpty(t, req.Param.PrimaryKeysMayBeModifiedParam.KeyVector)
				if canceled {
					cancel()
					return ctx.Err()
				}
				buffer := morpc.NewBuffer()
				defer buffer.Close()
				req.Apply(buffer.EncodeUint16(1))
				return nil
			}}
			mp := txn.proc.Mp()
			bat := makeBatchForTest(mp, 7)
			defer bat.Clean(mp)
			observation := &resource.LockReadObservation{}
			ctx := resource.ContextWithLockReadPK(base, observation)
			start := observation.Start()
			changed, err := remote.PrimaryKeysMayBeModified(ctx, types.BuildTS(10, 0), types.BuildTS(20, 0), bat, 0, -1)
			observation.Finish(ctx, resource.LockReadPKCheck, start, err)

			root := resource.NewRoot(resource.ConnExternal)
			root.MergeLockRead(observation)
			_, snapshot := root.SealWithLockRead(0)
			diagnostic := snapshot.Diagnostic([16]byte{}, 1, err != nil)
			require.True(t, diagnostic.RemotePKSubstagesUnavailable)
			require.Equal(t, uint8(4), diagnostic.Seen)
			require.Positive(t, diagnostic.PKCheckNS)
			require.Zero(t, diagnostic.PKReadinessNS)
			require.Zero(t, diagnostic.LogtailNS)
			if canceled {
				require.ErrorIs(t, err, context.Canceled)
				require.False(t, changed)
				require.Equal(t, "pk_check", diagnostic.LastIncomplete)
				require.True(t, snapshot.Qualifies(0, time.Second))
			} else {
				require.NoError(t, err)
				require.True(t, changed)
				require.Empty(t, diagnostic.LastIncomplete)
				require.False(t, snapshot.Qualifies(0, time.Second))
			}
		})
	}
}

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
		mpool.MustNewZero(),
	)
	assert.NoError(t, err)
	assert.NotNil(t, handle)
	assert.NoError(t, handle.Close())
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
		table.Write(context.Background(), &batch.Batch{}, nil)
	})
}

func TestTxnTableDelegate_Delete(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot delete data to partition primary table", func() {
		table.Delete(context.Background(), &batch.Batch{}, nil, "")
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

func TestRemoteStatsSampleVisibility(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	origin := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	op := txn.op.(*mock_frontend.MockTxnOperator)
	op.EXPECT().Snapshot().Return(txnpb.CNTxnSnapshot{}, nil).AnyTimes()
	op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	txn.proc.Base.TxnOperator = op
	origin.proc.Store(txn.proc)
	observation := &statsinfo.StatsInfo{TableName: "tbl", TableCnt: 5, ByteRangeMap: map[string]*statsinfo.ByteRangeDistribution{"v": {Version: 1}}}
	response, err := observation.Marshal()
	require.NoError(t, err)
	tbl := &txnTableDelegate{origin: origin, isLocal: func() (bool, error) { return false, nil }}
	tbl.shard.service = statsReadService{response: response}
	txn.readOnly.Store(true)
	got, err := tbl.Stats(t.Context(), true)
	require.NoError(t, err)
	require.NotEmpty(t, got.ByteRangeMap)
	txn.readOnly.Store(false)
	txn.writes = []Entry{{typ: DELETE, databaseId: 7, tableId: 42}}
	got, err = tbl.Stats(t.Context(), true)
	require.NoError(t, err)
	require.Empty(t, got.ByteRangeMap, "delete-only workspace is invisible to the remote readonly owner")
	require.Equal(t, 5.0, got.TableCnt)
	require.NotEmpty(t, observation.ByteRangeMap)
}
