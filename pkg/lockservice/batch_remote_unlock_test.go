// Copyright 2026 Matrix Origin
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

package lockservice

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

type recordedRemoteUnlock struct {
	method pb.Method
	owner  string
	binds  []pb.LockTable
}

type recordingRemoteUnlockClient struct {
	batchErr error
	requests []recordedRemoteUnlock
}

func (c *recordingRemoteUnlockClient) Send(
	_ context.Context,
	req *pb.Request,
) (*pb.Response, error) {
	recorded := recordedRemoteUnlock{
		method: req.Method,
		owner:  req.LockTable.ServiceID,
	}
	switch req.Method {
	case pb.Method_BatchUnlock:
		recorded.binds = append(recorded.binds, req.BatchUnlock.LockTables...)
		c.requests = append(c.requests, recorded)
		if c.batchErr != nil {
			return nil, c.batchErr
		}
	case pb.Method_Unlock:
		recorded.binds = append(recorded.binds, req.LockTable)
		c.requests = append(c.requests, recorded)
	default:
		return nil, errors.New("unexpected remote-unlock request")
	}
	return acquireResponse(), nil
}

func (*recordingRemoteUnlockClient) AsyncSend(
	context.Context,
	*pb.Request,
) (*morpc.Future, error) {
	return nil, io.ErrClosedPipe
}

func (*recordingRemoteUnlockClient) Close() error { return nil }

type countingForwardUnlockClient struct {
	Client
	batches atomic.Int32
	unlocks atomic.Int32
}

func (c *countingForwardUnlockClient) Send(
	ctx context.Context,
	req *pb.Request,
) (*pb.Response, error) {
	switch req.Method {
	case pb.Method_BatchUnlock:
		c.batches.Add(1)
	case pb.Method_Unlock:
		c.unlocks.Add(1)
	}
	return c.Client.Send(ctx, req)
}

type batchUnlockTableSpec struct {
	owner     string
	table     uint64
	supported bool
}

func newBatchUnlockOriginState(
	t *testing.T,
	client Client,
	specs []batchUnlockTableSpec,
) (*service, *activeTxn) {
	t.Helper()
	logger := getLogger("")
	s := &service{
		serviceID: "origin",
		logger:    logger,
		tableGroups: &lockTableHolders{
			service: "origin",
			logger:  logger,
			holders: make(map[uint32]*lockTableHolder),
		},
	}
	txnID := []byte("batch-origin")
	txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(len(specs)+1), "")
	for _, spec := range specs {
		bind := pb.LockTable{
			Group:     0,
			Table:     spec.table,
			ServiceID: spec.owner,
			Version:   1,
			Valid:     true,
		}
		remote := newRemoteLockTable(
			s.serviceID,
			time.Second,
			bind,
			client,
			nil,
			logger,
		)
		s.tableGroups.set(bind.Group, bind.Table, remote)
		require.NoError(t, txn.lockAdded(
			bind.Group,
			bind,
			[][]byte{{byte(spec.table)}},
			pb.LockOptions{},
			logger,
		))
		txn.setBatchUnlockSupportedLocked(bind.Group, bind.Table, spec.supported)
	}
	return s, txn
}

func closeBatchUnlockOrigin(
	t *testing.T,
	s *service,
	txn *activeTxn,
) {
	t.Helper()
	txn.Lock()
	defer txn.Unlock()
	s.batchRemoteUnlockTables(context.Background(), txn, timestamp.Timestamp{})
	require.NoError(t, txn.closeWithoutFreeWithContext(
		context.Background(),
		txn.txnID,
		timestamp.Timestamp{},
		func(bind pb.LockTable) (lockTable, error) {
			return s.getLockTableForTxnUnlock(bind), nil
		},
		s.logger,
	))
}

func TestBatchRemoteUnlockPartitionsByOwnerAndBound(t *testing.T) {
	client := &recordingRemoteUnlockClient{}
	specs := make([]batchUnlockTableSpec, 0, 69)
	for table := uint64(1); table <= 67; table++ {
		specs = append(specs, batchUnlockTableSpec{
			owner:     "owner-a",
			table:     table,
			supported: true,
		})
	}
	for table := uint64(68); table <= 69; table++ {
		specs = append(specs, batchUnlockTableSpec{
			owner:     "owner-b",
			table:     table,
			supported: true,
		})
	}
	s, txn := newBatchUnlockOriginState(t, client, specs)
	defer reuse.Free(txn, nil)

	closeBatchUnlockOrigin(t, s, txn)

	require.Len(t, client.requests, 3)
	require.Equal(t, "owner-a", client.requests[0].owner)
	require.Len(t, client.requests[0].binds, maxRemoteUnlockBatchSize)
	require.Equal(t, "owner-a", client.requests[1].owner)
	require.Len(t, client.requests[1].binds, 3)
	require.Equal(t, "owner-b", client.requests[2].owner)
	require.Len(t, client.requests[2].binds, 2)
	for _, req := range client.requests {
		require.Equal(t, pb.Method_BatchUnlock, req.method)
		require.LessOrEqual(t, len(req.binds), maxRemoteUnlockBatchSize)
	}
}

func TestBatchRemoteUnlockPreservesTableScopedFallback(t *testing.T) {
	tests := []struct {
		name        string
		specs       []batchUnlockTableSpec
		batchErr    error
		wantBatch   int
		wantUnlocks int
	}{
		{
			name: "owner did not negotiate capability",
			specs: []batchUnlockTableSpec{
				{owner: "legacy-owner", table: 1},
				{owner: "legacy-owner", table: 2},
			},
			wantUnlocks: 2,
		},
		{
			name: "single table stays table scoped",
			specs: []batchUnlockTableSpec{
				{owner: "new-owner", table: 1, supported: true},
			},
			wantUnlocks: 1,
		},
		{
			name: "batch rejection falls back without losing ledgers",
			specs: []batchUnlockTableSpec{
				{owner: "new-owner", table: 1, supported: true},
				{owner: "new-owner", table: 2, supported: true},
			},
			batchErr:    errors.New("batch rejected"),
			wantBatch:   1,
			wantUnlocks: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &recordingRemoteUnlockClient{batchErr: test.batchErr}
			s, txn := newBatchUnlockOriginState(t, client, test.specs)
			defer reuse.Free(txn, nil)

			closeBatchUnlockOrigin(t, s, txn)

			var batches, unlocks int
			for _, req := range client.requests {
				switch req.method {
				case pb.Method_BatchUnlock:
					batches++
				case pb.Method_Unlock:
					unlocks++
				}
			}
			require.Equal(t, test.wantBatch, batches)
			require.Equal(t, test.wantUnlocks, unlocks)
		})
	}
}

func TestBatchRemoteUnlockRejectsInvalidRequestsBeforeAdmission(t *testing.T) {
	owner := &service{serviceID: "owner"}
	bind := func(table uint64) pb.LockTable {
		return pb.LockTable{
			Group:     0,
			Table:     table,
			ServiceID: owner.serviceID,
			Version:   1,
			Valid:     true,
		}
	}
	tooLarge := make([]pb.LockTable, maxRemoteUnlockBatchSize+1)
	for idx := range tooLarge {
		tooLarge[idx] = bind(uint64(idx + 1))
	}

	tests := []struct {
		name  string
		binds []pb.LockTable
	}{
		{name: "single table", binds: []pb.LockTable{bind(1)}},
		{name: "over bound", binds: tooLarge},
		{name: "duplicate table", binds: []pb.LockTable{bind(1), bind(1)}},
		{
			name: "mixed physical owners",
			binds: []pb.LockTable{
				bind(1),
				{Group: 0, Table: 2, ServiceID: "other-owner", Version: 1, Valid: true},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := owner.unlockRemoteLockTables(
				context.Background(),
				test.binds,
				[]byte("invalid-batch"),
				timestamp.Timestamp{},
			)
			require.Error(t, err)
		})
	}
}

func TestBatchRemoteUnlockValidatesAllGenerationsBeforeRelease(t *testing.T) {
	const (
		firstTable  = uint64(2762801)
		secondTable = uint64(2762802)
	)
	row := []byte("row")

	runLockServiceTests(
		t,
		[]string{"owner", "origin"},
		func(_ *lockTableAllocator, services []*service) {
			owner := services[0]
			origin := services[1]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Establish both physical tables on owner before origin acquires them.
			for _, table := range []uint64{firstTable, secondTable} {
				seedTxn := []byte{byte(table)}
				mustAddTestLock(t, ctx, owner, table, seedTxn, [][]byte{row}, pb.Granularity_Row)
				require.NoError(t, owner.Unlock(ctx, seedTxn, timestamp.Timestamp{}))
			}

			txnID := []byte("remote-multi-table")
			mustAddTestLock(t, ctx, origin, firstTable, txnID, [][]byte{row}, pb.Granularity_Row)
			mustAddTestLock(t, ctx, origin, secondTable, txnID, [][]byte{row}, pb.Granularity_Row)
			originTxn := origin.activeTxnHolder.getActiveTxn(txnID, false, "")
			require.NotNil(t, originTxn)
			originTxn.Lock()
			require.True(t, originTxn.isBatchUnlockSupportedLocked(0, firstTable))
			require.True(t, originTxn.isBatchUnlockSupportedLocked(0, secondTable))
			originTxn.Unlock()
			firstTableState := origin.tableGroups.get(0, firstTable)
			secondTableState := origin.tableGroups.get(0, secondTable)
			require.IsType(t, &remoteLockTable{}, firstTableState)
			require.IsType(t, &remoteLockTable{}, secondTableState)
			firstRemote := firstTableState.(*remoteLockTable)
			secondRemote := secondTableState.(*remoteLockTable)
			counter := &countingForwardUnlockClient{Client: firstRemote.client}
			firstRemote.client = counter
			secondRemote.client = counter

			firstBind := owner.tableGroups.get(0, firstTable).getBind()
			secondBind := owner.tableGroups.get(0, secondTable).getBind()
			staleSecond := secondBind
			staleSecond.Version++
			require.Error(t, owner.unlockRemoteLockTables(
				ctx,
				[]pb.LockTable{firstBind, staleSecond},
				txnID,
				timestamp.Timestamp{},
			))

			ownerTxn := owner.activeTxnHolder.getActiveTxn(txnID, false, "")
			require.NotNil(t, ownerTxn)
			require.False(t, ownerTxn.closing.Load(),
				"generation validation must fail before sealing or releasing the first table")

			probeOptions := newTestRowExclusiveOptions()
			probeOptions.Policy = pb.WaitPolicy_FastFail
			probeTxn := []byte("probe-still-held")
			_, err := owner.Lock(ctx, firstTable, [][]byte{row}, probeTxn, probeOptions)
			require.Error(t, err, "the valid first table must remain locked after a stale later bind")
			require.NoError(t, owner.Unlock(ctx, probeTxn, timestamp.Timestamp{}))

			require.NoError(t, origin.Unlock(ctx, txnID, timestamp.Timestamp{}))
			require.Equal(t, int32(1), counter.batches.Load())
			require.Zero(t, counter.unlocks.Load())
			require.Nil(t, owner.activeTxnHolder.getActiveTxn(txnID, false, ""))

			// A lost response can replay the same batch after the owner already
			// completed it. Missing transaction state is an idempotent success.
			require.NoError(t, owner.unlockRemoteLockTables(
				ctx,
				[]pb.LockTable{firstBind, secondBind},
				txnID,
				timestamp.Timestamp{},
			))

			verifyTxn := []byte("verify-released")
			mustAddTestLock(t, ctx, owner, firstTable, verifyTxn, [][]byte{row}, pb.Granularity_Row)
			mustAddTestLock(t, ctx, owner, secondTable, verifyTxn, [][]byte{row}, pb.Granularity_Row)
			require.NoError(t, owner.Unlock(ctx, verifyTxn, timestamp.Timestamp{}))
		},
	)
}
