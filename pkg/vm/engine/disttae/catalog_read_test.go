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
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCatalogReadFixture(t *testing.T) (*txnTable, *mock_frontend.MockTxnOperator) {
	t.Helper()
	eng, db, workspace, _ := newResultOwnershipFixture(t, nil, false)
	op := workspace.op.(*mock_frontend.MockTxnOperator)
	op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 100}).AnyTimes()
	db.databaseId, db.databaseName = catalog.MO_CATALOG_ID, catalog.MO_CATALOG
	workspace.tnStores = []DNStore{{
		TxnServiceAddress: "tn",
		Shards:            []metadata.TNShard{{TNShardRecord: metadata.TNShardRecord{ShardID: 1}}},
	}}
	eng.catalogReadBase = map[uint64]*logtailreplay.PartitionState{
		catalog.MO_TABLES_ID: logtailreplay.NewPartitionState("", false, catalog.MO_TABLES_ID, false),
	}
	eng.pClient.lazyCatalog = newLazyCatalogCNState()
	eng.pClient.lazyCatalog.enable()
	return &txnTable{eng: eng, db: db, tableId: catalog.MO_TABLES_ID, tableName: catalog.MO_TABLES}, op
}

func TestAuthoritativeCatalogReadAdmission(t *testing.T) {
	tbl, _ := newCatalogReadFixture(t)
	eng := tbl.eng.(*Engine)
	eng.pClient.lazyCatalog.setAccountReady(10, timestamp.Timestamp{PhysicalTime: 50})
	eng.pClient.lazyCatalog.setAccountReady(11, timestamp.Timestamp{PhysicalTime: 150})
	for _, tc := range []struct {
		name    string
		account uint32
		table   uint64
		replay  bool
		want    bool
	}{
		{name: "sys may read cold tenants", table: catalog.MO_TABLES_ID, want: true},
		{name: "cold tenant fallback", account: 20, table: catalog.MO_TABLES_ID, want: true},
		{name: "ready tenant", account: 10, table: catalog.MO_TABLES_ID},
		{name: "snapshot before readiness", account: 11, table: catalog.MO_TABLES_ID, want: true},
		{name: "admitted replay", table: catalog.MO_TABLES_ID, replay: true},
		{name: "ordinary table", table: 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl.tableId = tc.table
			ctx := defines.AttachAccountId(t.Context(), tc.account)
			if tc.replay {
				ctx = context.WithValue(ctx, catalogReplayContextKey{}, true)
			}
			require.Equal(t, tc.want, tbl.needsAuthoritativeCatalogRead(ctx))
		})
	}
	tbl.tableId = catalog.MO_TABLES_ID
	eng.pClient.lazyCatalog.enabled.Store(false)
	require.False(t, tbl.needsAuthoritativeCatalogRead(t.Context()))
}

func TestCatalogReadSnapshotRejectsInvalidResponses(t *testing.T) {
	wantErr := moerr.NewInternalErrorNoCtx("catalog pull failed")
	for _, tc := range []struct {
		name   string
		result *rpc.SendResult
		err    error
	}{
		{name: "transport failure", err: wantErr},
		{name: "missing result"},
		{name: "missing response", result: &rpc.SendResult{}},
		{name: "missing payload owner", result: &rpc.SendResult{Responses: []txn.TxnResponse{{}}}},
		{name: "invalid payload", result: &rpc.SendResult{Responses: []txn.TxnResponse{{CNOpResponse: &txn.CNOpResponse{Payload: []byte{0xff}}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl, op := newCatalogReadFixture(t)
			op.EXPECT().Read(gomock.Any(), gomock.Any()).Return(tc.result, tc.err)
			state, err := tbl.readCatalogSnapshot(t.Context())
			require.Nil(t, state)
			require.Error(t, err)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			}
		})
	}
}

func TestCatalogReadSnapshotDoesNotPublishOrShareMutableState(t *testing.T) {
	tbl, op := newCatalogReadFixture(t)
	eng := tbl.eng.(*Engine)
	base := eng.catalogReadBase[tbl.tableId]
	const readers = 2
	op.EXPECT().Read(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
		_, bounded := ctx.Deadline()
		assert.True(t, bounded)
		if !assert.Len(t, requests, 1) {
			return nil, moerr.NewInternalErrorNoCtx("invalid catalog read request count")
		}
		assert.Equal(t, uint32(api.OpCode_OpGetLogTail), requests[0].CNRequest.OpCode)
		var request api.SyncLogTailReq
		if err := request.UnmarshalBinary(requests[0].CNRequest.Payload); err != nil {
			return nil, err
		}
		assert.Equal(t, int64(100), request.CnWant.PhysicalTime)
		assert.True(t, request.CnHave.IsEmpty())
		return &rpc.SendResult{Responses: []txn.TxnResponse{{CNOpResponse: &txn.CNOpResponse{}}}}, nil
	}).Times(readers)
	states := make(chan *logtailreplay.PartitionState, readers)
	errors := make(chan error, readers)
	var wg sync.WaitGroup
	for range readers {
		wg.Go(func() {
			state, err := tbl.readCatalogSnapshot(t.Context())
			states <- state
			errors <- err
		})
	}
	wg.Wait()
	for range readers {
		require.NoError(t, <-errors)
		state := <-states
		require.NotSame(t, base, state)
		require.True(t, state.CanServe(types.BuildTS(100, 0)))
	}
	require.False(t, base.CanServe(types.BuildTS(100, 0)))
	require.Empty(t, eng.partitions)
	require.False(t, eng.pClient.CanServeAccount(20, timestamp.Timestamp{PhysicalTime: 100}))
}

func TestCatalogReadSnapshotCancellation(t *testing.T) {
	tbl, op := newCatalogReadFixture(t)
	entered := make(chan struct{})
	op.EXPECT().Read(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ []txn.TxnRequest) (*rpc.SendResult, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := tbl.readCatalogSnapshot(ctx)
		result <- err
	}()
	<-entered
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
}
