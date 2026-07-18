// Copyright 2023 Matrix Origin
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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLockBlockedOnRemote(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)

			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, tableID, txn1, [][]byte{row1}, pb.Granularity_Row)
			c := make(chan struct{})
			go func() {
				// txn2 try lock row1 on l2
				mustAddTestLock(t, ctx, l2, tableID, txn2, [][]byte{row1}, pb.Granularity_Row)
				close(c)
			}()
			waitWaiters(t, l1, tableID, row1, 1)
			require.NoError(t, l1.Unlock(ctx, txn1, timestamp.Timestamp{}))
			<-c
		},
	)
}

func TestFetchWhoWaitingMeUsesActiveRemoteWaiterSnapshots(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2", "s3"},
		func(_ *lockTableAllocator, s []*service) {
			owner := s[0]
			holderService := s[1]
			waiterService := s[2]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			holderTxn := []byte("remote-holder")
			activeWaiterTxn := []byte("active-waiter")

			// Bind the table to owner, then acquire it through holderService so
			// fetchWhoWaitingMe has to query a remote lock table.
			mustAddTestLock(t, ctx, owner, tableID, seedTxn, [][]byte{row}, pb.Granularity_Row)
			require.NoError(t, owner.Unlock(ctx, seedTxn, timestamp.Timestamp{}))
			mustAddTestLock(t, ctx, holderService, tableID, holderTxn, [][]byte{row}, pb.Granularity_Row)

			// The waiter must remain live until the holder is explicitly released.
			// Reusing ctx here couples queue admission to all setup RPCs above: once
			// that deadline expires, Lock returns before it can enter owner's queue
			// and the old unbounded waitWaiters call spins forever.
			// Lock may cross MORPC, whose Future contract requires a deadline.
			// Give the waiter a budget independent from and larger than setup while
			// retaining a hard bound if admission or cleanup regresses.
			waiterCtx, cancelWaiter := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelWaiter()
			waitResult := make(chan error, 1)
			holderReleased := false
			waiterDone := false
			go func() {
				_, err := waiterService.Lock(
					waiterCtx,
					tableID,
					[][]byte{row},
					activeWaiterTxn,
					newTestRowExclusiveOptions(),
				)
				waitResult <- err
			}()
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				if !holderReleased {
					_ = holderService.Unlock(cleanupCtx, holderTxn, timestamp.Timestamp{})
				}
				cancelWaiter()
				if !waiterDone {
					select {
					case err := <-waitResult:
						if err == nil {
							_ = waiterService.Unlock(cleanupCtx, activeWaiterTxn, timestamp.Timestamp{})
						}
					case <-cleanupCtx.Done():
						t.Errorf("remote waiter did not exit during cleanup: %v", cleanupCtx.Err())
					}
				}
			}()

			// This is test admission synchronization, not part of the behavior under
			// test. Keep it bounded so an unexpected routing failure produces a
			// useful failure instead of consuming the package's 40-minute timeout.
			require.Eventually(t, func() bool {
				lt, err := owner.getLockTable(0, tableID)
				if err != nil {
					return false
				}
				local, ok := lt.(*localLockTable)
				if !ok {
					return false
				}
				local.mu.Lock()
				defer local.mu.Unlock()
				lock, ok := local.mu.store.Get(row)
				return ok && lock.waiters.size() == 1
			}, 10*time.Second, 10*time.Millisecond, "active remote waiter did not reach owner queue")
			lt, err := owner.getLockTable(0, tableID)
			require.NoError(t, err)
			local := lt.(*localLockTable)
			logger := getLogger("")

			completedWaiter := acquireWaiter(pb.WaitTxn{TxnID: []byte("completed")}, "test", logger)
			completedWaiter.setStatus(completed)
			notifiedWaiter := acquireWaiter(pb.WaitTxn{TxnID: []byte("notified")}, "test", logger)
			notifiedWaiter.setStatus(notified)
			local.mu.Lock()
			lock, ok := local.mu.store.Get(row)
			if !ok {
				local.mu.Unlock()
				t.Fatal("owner lock disappeared before remote snapshot")
			}
			lock.addWaiter(logger, completedWaiter)
			lock.addWaiter(logger, notifiedWaiter)
			local.mu.Unlock()
			defer func() {
				local.mu.Lock()
				lock, ok := local.mu.store.Get(row)
				if ok {
					removed, _ := lock.waiters.remove(completedWaiter)
					require.True(t, removed)
					removed, _ = lock.waiters.remove(notifiedWaiter)
					require.True(t, removed)
				}
				local.mu.Unlock()
				completedWaiter.close("test", logger)
				notifiedWaiter.close("test", logger)
			}()

			txn := holderService.activeTxnHolder.getActiveTxn(holderTxn, false, "")
			require.NotNil(t, txn)
			var waitingTxnIDs [][]byte
			require.True(t, txn.fetchWhoWaitingMe(
				holderService.serviceID,
				holderTxn,
				func(waitTxn pb.WaitTxn, waiterAddress string) bool {
					waitingTxnIDs = append(waitingTxnIDs, waitTxn.TxnID)
					require.Equal(t, owner.serviceID, waiterAddress)
					return true
				},
				holderService.getLockTable,
			))
			require.Equal(t, [][]byte{activeWaiterTxn}, waitingTxnIDs)

			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer releaseCancel()
			require.NoError(t, holderService.Unlock(releaseCtx, holderTxn, timestamp.Timestamp{}))
			holderReleased = true
			select {
			case err := <-waitResult:
				waiterDone = true
				require.NoError(t, err)
			case <-releaseCtx.Done():
				t.Fatalf("remote waiter did not acquire after holder release: %v", releaseCtx.Err())
			}
			require.NoError(t, waiterService.Unlock(releaseCtx, activeWaiterTxn, timestamp.Timestamp{}))
		},
	)
}

func TestGetLocalLockTableUsesGetLockHolderLookupInputs(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			originTableID := uint64(10)
			row := []byte("row1")
			tableID := ShardingByRow(row)
			bind := alloc.Get(l.serviceID, 0, tableID, originTableID, pb.Sharding_ByRow)
			req := &pb.Request{
				Method:    pb.Method_GetLockHolder,
				LockTable: bind,
			}
			req.GetLockHolder.Row = row
			req.GetLockHolder.Sharding = pb.Sharding_ByRow
			resp := &pb.Response{}

			lt, err := l.getLocalLockTable(req, resp)
			require.NoError(t, err)
			require.NotNil(t, lt)
			require.Equal(t, bind, lt.getBind())
		},
	)
}

func TestRemoteLockResponseLogFieldsDoNotRetainRequest(t *testing.T) {
	req := &pb.Request{
		LockTable: pb.LockTable{
			Group:       1,
			Table:       2,
			OriginTable: 3,
			ServiceID:   "bind-service",
			Version:     4,
		},
		Lock: pb.LockRequest{
			TxnID:     []byte{0x01, 0x02},
			ServiceID: "caller-service",
			Rows:      [][]byte{{1}, {2}},
		},
	}
	logFields := remoteLockResponseLogFields(req)

	req.Reset()
	fields := logFields()
	values := make(map[string]any, len(fields))
	for _, field := range fields {
		if field.String != "" {
			values[field.Key] = field.String
			continue
		}
		values[field.Key] = field.Integer
	}

	require.Equal(t, "0102", values["txn"])
	require.Equal(t, "1-2(3)-bind-service-4", values["bind"])
	require.Equal(t, "caller-service", values["caller-service"])
	require.Equal(t, int64(2), values["row-count"])
}

func TestLockResultWithNoConflictOnRemote(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}
			row2 := []byte{2}

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, 1, txn1, [][]byte{row1}, pb.Granularity_Row)
			c := make(chan struct{})
			go func() {
				// txn2 try lock row1 on l2
				res := mustAddTestLock(t, ctx, l2, 1, txn2, [][]byte{row2}, pb.Granularity_Row)
				require.False(t, res.Timestamp.IsEmpty())
				close(c)
			}()
			<-c
		},
	)
}

func TestLockResultWithConflictAndTxnCommittedOnRemote(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)

			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, tableID, txn1, [][]byte{row1}, pb.Granularity_Row)
			c := make(chan struct{})
			go func() {
				defer close(c)
				// txn2 try lock row1 on l2
				// blocked by txn1
				res, err := l2.Lock(
					ctx,
					tableID,
					[][]byte{row1},
					txn2,
					option)
				require.NoError(t, err)
				assert.True(
					t,
					!res.Timestamp.IsEmpty())
			}()
			waitWaiters(t, l1, tableID, row1, 1)
			require.NoError(t, l1.Unlock(
				ctx,
				txn1,
				timestamp.Timestamp{PhysicalTime: 1}))
			<-c
		},
	)
}

func TestLockResultWithConflictAndTxnAbortedOnRemote(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)

			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, tableID, txn1, [][]byte{row1}, pb.Granularity_Row)
			c := make(chan struct{})
			go func() {
				defer close(c)
				// txn2 try lock row1 on l2
				// blocked by txn1
				res, err := l2.Lock(
					ctx,
					tableID,
					[][]byte{row1},
					txn2,
					option)
				require.NoError(t, err)
				assert.False(t, res.Timestamp.IsEmpty())
			}()
			waitWaiters(t, l1, tableID, row1, 1)
			require.NoError(t, l1.Unlock(ctx, txn1, timestamp.Timestamp{}))
			<-c
		},
	)
}

func TestHandleForwardLockRejectsWhenServiceCannotLock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			s.setStatus(pb.Status_ServiceCanRestart)

			req := &pb.Request{
				RequestID: 1,
				Method:    pb.Method_ForwardLock,
				LockTable: pb.LockTable{Table: 10},
				Lock: pb.LockRequest{
					TxnID:     []byte("txn1"),
					ServiceID: "remote-service",
					Rows:      [][]byte{{1}},
					Options:   newTestRowExclusiveOptions(),
				},
			}
			resp := acquireResponse()
			defer releaseResponse(resp)
			cs := &testClientSession{ctx: context.Background()}

			s.handleForwardLock(context.Background(), nil, req, resp, cs)
			require.True(t, cs.writeCalled)
			require.False(t, cs.closeCalled)
			require.True(t, moerr.IsMoErrCode(resp.UnwrapError(), moerr.ErrRetryForCNRollingRestart))
		},
	)
}

func TestHandleForwardLockDoesNotHoldBindChangeLockWhileWaitingForBind(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, services []*service) {
			s := services[0]
			group := uint32(0)
			table := uint64(24905)
			bind := pb.LockTable{
				Group:       group,
				Table:       table,
				OriginTable: table,
				ServiceID:   s.serviceID,
				Valid:       true,
				Version:     alloc.version,
				AllocatorID: alloc.allocatorID,
			}
			waitC := make(chan struct{})
			s.mu.Lock()
			s.mu.allocating[group] = map[uint64]chan struct{}{table: waitC}
			s.mu.Unlock()

			req := &pb.Request{
				RequestID: 1,
				Method:    pb.Method_ForwardLock,
				LockTable: bind,
				Lock: pb.LockRequest{
					TxnID:     []byte("forward-waiting-bind"),
					ServiceID: "remote-service",
					Rows:      [][]byte{{1}},
					Options:   newTestRowExclusiveOptions(),
				},
			}
			resp := acquireResponse()
			defer releaseResponse(resp)
			cs := &testClientSession{ctx: context.Background()}

			done := make(chan struct{})
			go func() {
				defer close(done)
				s.handleForwardLock(context.Background(), nil, req, resp, cs)
			}()
			time.Sleep(20 * time.Millisecond)

			published := make(chan struct{})
			go func() {
				defer close(published)
				s.bindChangeMu.Lock()
				s.tableGroups.set(group, table, s.createLockTableByBind(bind))
				s.bindChangeMu.Unlock()
				s.mu.Lock()
				delete(s.mu.allocating[group], table)
				s.mu.Unlock()
				close(waitC)
			}()

			select {
			case <-published:
			case <-time.After(time.Second):
				require.Fail(t, "bind publish blocked while forwarded lock waited for allocation")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				require.Fail(t, "forwarded lock did not finish after bind publish")
			}
			require.True(t, cs.writeCalled)
			require.NoError(t, resp.UnwrapError())
		},
	)
}

func TestGetActiveTxnWithRemote(t *testing.T) {
	reuse.RunReuseTests(func() {
		hold := newMapBasedTxnHandler(
			"s1",
			getLogger(""),
			newFixedSlicePool(16),
			func(sid string) (bool, error) { return true, nil },
			func(ot []pb.OrphanTxn) (pb.CannotCommitResponse, error) { return pb.CannotCommitResponse{}, nil },
			func(txn pb.WaitTxn) (bool, error) { return true, nil },
		).(*mapBasedTxnHolder)
		defer hold.close()

		txnID := []byte("txn1")
		st := time.Now()
		txn := hold.getActiveTxn(txnID, true, "s1")
		assert.NotNil(t, txn)
		assert.Equal(t, "s1", txn.remoteService)
		assert.Equal(t, 1, len(hold.mu.remoteServices))
		assert.Equal(t, 1, hold.mu.dequeue.Len())
		e := hold.mu.dequeue.PopFront()
		assert.Equal(t, "s1", e.Value.id)
		assert.True(t, e.Value.time.After(st))
	})
}

func TestMapBasedTxnHolderConcurrentGetAndDelete(t *testing.T) {
	reuse.RunReuseTests(func() {
		hold := newMapBasedTxnHandler(
			"s1",
			getLogger(""),
			newFixedSlicePool(16),
			func(string) (bool, error) { return true, nil },
			func([]pb.OrphanTxn) (pb.CannotCommitResponse, error) { return pb.CannotCommitResponse{}, nil },
			func(pb.WaitTxn) (bool, error) { return true, nil },
		).(*mapBasedTxnHolder)
		defer hold.close()

		const workers = 100
		start := make(chan struct{})
		results := make(chan *activeTxn, workers)
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				results <- hold.getActiveTxn([]byte("same-txn"), true, "")
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		var first *activeTxn
		for txn := range results {
			if first == nil {
				first = txn
			}
			require.Same(t, first, txn)
		}
		require.Equal(t, int64(1), hold.activeTxnCount.Load())

		created := make(chan []byte, workers)
		start = make(chan struct{})
		for i := range workers {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				txnID := []byte(fmt.Sprintf("txn-%d", i))
				if hold.getActiveTxn(txnID, true, "") != nil {
					created <- txnID
				}
			}(i)
		}
		close(start)
		wg.Wait()
		close(created)
		txnIDs := make([][]byte, 0, workers)
		for txnID := range created {
			txnIDs = append(txnIDs, txnID)
		}
		require.Len(t, txnIDs, workers)
		require.Equal(t, int64(workers+1), hold.activeTxnCount.Load())

		deleted := make(chan *activeTxn, workers)
		start = make(chan struct{})
		for _, txnID := range txnIDs {
			wg.Add(1)
			go func(txnID []byte) {
				defer wg.Done()
				<-start
				deleted <- hold.deleteActiveTxn(txnID)
			}(txnID)
		}
		close(start)
		wg.Wait()
		close(deleted)
		for txn := range deleted {
			require.NotNil(t, txn)
			reuse.Free(txn, nil)
		}
		for _, txnID := range txnIDs {
			require.Nil(t, hold.deleteActiveTxn(txnID))
		}
		require.Equal(t, int64(1), hold.activeTxnCount.Load())
	})
}

func TestMapBasedTxnHolderVisitsAndClosesAllShards(t *testing.T) {
	reuse.RunReuseTests(func() {
		hold := newMapBasedTxnHandler(
			"s1",
			getLogger(""),
			newFixedSlicePool(16),
			func(string) (bool, error) { return true, nil },
			func([]pb.OrphanTxn) (pb.CannotCommitResponse, error) { return pb.CannotCommitResponse{}, nil },
			func(pb.WaitTxn) (bool, error) { return true, nil },
		).(*mapBasedTxnHolder)

		idsByShard := make(map[*activeTxnShard][]byte, activeTxnHolderShards)
		for i := 0; len(idsByShard) < activeTxnHolderShards; i++ {
			txnID := []byte(fmt.Sprintf("sharded-txn-%d", i))
			shard := hold.getActiveTxnShard(string(txnID))
			if _, ok := idsByShard[shard]; !ok {
				idsByShard[shard] = txnID
			}
		}

		expected := make(map[string]struct{}, activeTxnHolderShards)
		for _, txnID := range idsByShard {
			expected[string(txnID)] = struct{}{}
			require.NotNil(t, hold.getActiveTxn(txnID, true, ""))
		}
		require.False(t, hold.empty())
		require.Equal(t, int64(activeTxnHolderShards), hold.activeTxnCount.Load())

		actual := make(map[string]struct{}, activeTxnHolderShards)
		for _, txnID := range hold.getAllTxnID() {
			actual[string(txnID)] = struct{}{}
		}
		require.Equal(t, expected, actual)

		hold.close()
		require.True(t, hold.empty())
		require.Empty(t, hold.getAllTxnID())
		for _, txnID := range idsByShard {
			require.False(t, hold.hasActiveTxn(txnID))
		}
	})
}

func BenchmarkMapBasedTxnHolderConcurrent(b *testing.B) {
	hold := newMapBasedTxnHandler(
		"s1",
		getLogger(""),
		newFixedSlicePool(16),
		func(string) (bool, error) { return true, nil },
		func([]pb.OrphanTxn) (pb.CannotCommitResponse, error) { return pb.CannotCommitResponse{}, nil },
		func(pb.WaitTxn) (bool, error) { return true, nil },
	).(*mapBasedTxnHolder)
	b.Cleanup(hold.close)

	var next atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			txnID := []byte(fmt.Sprintf("txn-%d", next.Add(1)))
			hold.getActiveTxn(txnID, true, "")
			reuse.Free(hold.deleteActiveTxn(txnID), nil)
		}
	})
}

func TestHandleCheckActiveTxn(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			visited := 0
			s.cfg.TxnIterFunc = func(fn func([]byte) bool) {
				for _, txnID := range [][]byte{
					[]byte("txn1"),
					[]byte("txn2"),
					[]byte("txn3"),
				} {
					visited++
					if !fn(txnID) {
						return
					}
				}
			}

			req := &pb.Request{
				Method: pb.Method_CheckActiveTxn,
				CheckActiveTxn: pb.CheckActiveTxnRequest{
					ServiceID: s.serviceID,
					Txn:       []byte("txn2"),
				},
			}
			resp := acquireResponse()
			defer releaseResponse(resp)
			cs := &testClientSession{ctx: context.Background()}

			s.handleCheckActiveTxn(context.Background(), nil, req, resp, cs)

			require.True(t, cs.writeCalled)
			require.True(t, resp.CheckActiveTxn.Valid)
			require.True(t, resp.CheckActiveTxn.Active)
			require.Equal(t, 2, visited)
		},
	)
}

func TestHandleCheckActiveTxnKeepsOnlyUnknownCommitCleanupActive(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			txnID := []byte("txn1")
			s.activeTxnHolder.getActiveTxn(txnID, true, "")

			req := &pb.Request{
				Method: pb.Method_CheckActiveTxn,
				CheckActiveTxn: pb.CheckActiveTxnRequest{
					ServiceID: s.serviceID,
					Txn:       txnID,
				},
			}
			cs := &testClientSession{ctx: context.Background()}

			resp := acquireResponse()
			s.handleCheckActiveTxn(context.Background(), nil, req, resp, cs)
			require.True(t, resp.CheckActiveTxn.Valid)
			require.False(t, resp.CheckActiveTxn.Active)
			releaseResponse(resp)

			s.unknownCommitResolver.mu.Lock()
			s.unknownCommitResolver.mu.pending[string(txnID)] = unknownCommitTxn{id: txnID}
			s.unknownCommitResolver.mu.Unlock()

			resp = acquireResponse()
			defer releaseResponse(resp)
			s.handleCheckActiveTxn(context.Background(), nil, req, resp, cs)
			require.True(t, resp.CheckActiveTxn.Valid)
			require.True(t, resp.CheckActiveTxn.Active)
		},
	)
}

func TestValidRemoteTxnUsesCheckActiveTxn(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			source := services[0]
			target := services[1]
			target.cfg.TxnIterFunc = func(fn func([]byte) bool) {
				require.False(t, fn([]byte("txn1")))
			}

			valid := source.activeTxnHolder.isValidRemoteTxn(pb.WaitTxn{
				TxnID:     []byte("txn1"),
				CreatedOn: target.serviceID,
			})
			require.True(t, valid)
		},
	)
}

type scriptedActiveTxnClient struct {
	Client
	calls      atomic.Int32
	resets     atomic.Int32
	resetErr   error
	afterReset pb.CheckActiveTxnResponse
}

func (c *scriptedActiveTxnClient) Send(
	_ context.Context,
	req *pb.Request,
) (*pb.Response, error) {
	if req.Method != pb.Method_CheckActiveTxn {
		return nil, moerr.NewInternalErrorNoCtx("unexpected active-txn method")
	}
	resp := acquireResponse()
	if c.calls.Add(1) == 1 {
		// The stale endpoint belongs to a different CN incarnation.
		resp.CheckActiveTxn.Valid = false
		return resp, nil
	}
	resp.CheckActiveTxn = c.afterReset
	return resp, nil
}

func (c *scriptedActiveTxnClient) ResetBackend(context.Context, string) error {
	c.resets.Add(1)
	return c.resetErr
}

func (c *scriptedActiveTxnClient) Close() error { return nil }

func TestValidRemoteTxnRecoversStaleCheckActiveTxnEndpoint(t *testing.T) {
	client := &scriptedActiveTxnClient{
		afterReset: pb.CheckActiveTxnResponse{Valid: true, Active: true},
	}
	var notified atomic.Int32
	holder := newMapBasedTxnHandler(
		"source",
		getLogger(""),
		newFixedSlicePool(16),
		func(string) (bool, error) { return true, nil },
		func([]pb.OrphanTxn) (pb.CannotCommitResponse, error) {
			notified.Add(1)
			return pb.CannotCommitResponse{}, nil
		},
		func(txn pb.WaitTxn) (bool, error) {
			return checkRemoteActiveTxn(client, txn)
		},
	).(*mapBasedTxnHolder)
	defer holder.close()

	require.True(t, holder.isValidRemoteTxn(pb.WaitTxn{
		TxnID:     []byte("live"),
		CreatedOn: "target",
	}))
	require.Equal(t, int32(2), client.calls.Load())
	require.Equal(t, int32(1), client.resets.Load())
	require.Zero(t, notified.Load(), "recovered live txn reached unlock fencing")
}

func TestValidRemoteTxnStaysLiveWhenEndpointResetFails(t *testing.T) {
	client := &scriptedActiveTxnClient{resetErr: moerr.NewBackendClosedNoCtx()}
	var notified atomic.Int32
	holder := newMapBasedTxnHandler(
		"source",
		getLogger(""),
		newFixedSlicePool(16),
		func(string) (bool, error) { return true, nil },
		func([]pb.OrphanTxn) (pb.CannotCommitResponse, error) {
			notified.Add(1)
			return pb.CannotCommitResponse{}, nil
		},
		func(txn pb.WaitTxn) (bool, error) {
			return checkRemoteActiveTxn(client, txn)
		},
	).(*mapBasedTxnHolder)
	defer holder.close()

	require.True(t, holder.isValidRemoteTxn(pb.WaitTxn{
		TxnID:     []byte("live"),
		CreatedOn: "target",
	}))
	require.Equal(t, int32(1), client.calls.Load())
	require.Equal(t, int32(1), client.resets.Load())
	require.Zero(t, notified.Load(), "unknown routing must not reach unlock fencing")
}

func TestValidRemoteTxnFallsBackToGetActiveTxn(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			source := services[0]
			target := services[1]
			target.cfg.TxnIterFunc = func(fn func([]byte) bool) {
				require.True(t, fn([]byte("txn1")))
			}
			target.remote.server.RegisterMethodHandler(
				pb.Method_CheckActiveTxn,
				func(_ context.Context, cancel context.CancelFunc, req *pb.Request, resp *pb.Response, cs morpc.ClientSession) {
					writeResponse(
						target.logger,
						cancel,
						resp,
						moerr.NewNotSupportedNoCtx("check active txn"),
						cs,
					)
				},
			)

			valid := source.activeTxnHolder.isValidRemoteTxn(pb.WaitTxn{
				TxnID:     []byte("txn1"),
				CreatedOn: target.serviceID,
			})
			require.True(t, valid)
		},
	)
}

func TestValidRemoteTxnFallsBackToGetActiveTxnMiss(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			source := services[0]
			target := services[1]
			target.cfg.TxnIterFunc = func(fn func([]byte) bool) {
				require.True(t, fn([]byte("txn2")))
			}
			target.remote.server.RegisterMethodHandler(
				pb.Method_CheckActiveTxn,
				func(_ context.Context, cancel context.CancelFunc, req *pb.Request, resp *pb.Response, cs morpc.ClientSession) {
					writeResponse(
						target.logger,
						cancel,
						resp,
						moerr.NewNotSupportedNoCtx("check active txn"),
						cs,
					)
				},
			)

			valid := source.activeTxnHolder.isValidRemoteTxn(pb.WaitTxn{
				TxnID:     []byte("txn1"),
				CreatedOn: target.serviceID,
			})
			require.False(t, valid)
		},
	)
}

func TestKeepRemoteActiveTxn(t *testing.T) {
	reuse.RunReuseTests(func() {
		hold := newMapBasedTxnHandler(
			"s1",
			getLogger(""),
			newFixedSlicePool(16),
			func(sid string) (bool, error) { return false, nil },
			func(ot []pb.OrphanTxn) (pb.CannotCommitResponse, error) { return pb.CannotCommitResponse{}, nil },
			func(txn pb.WaitTxn) (bool, error) { return true, nil },
		).(*mapBasedTxnHolder)
		defer hold.close()

		txnID1 := []byte("txn1")
		txnID2 := []byte("txn2")
		hold.getActiveTxn(txnID1, true, "s1")
		hold.getActiveTxn(txnID2, true, "s2")
		var ids []string
		hold.mu.dequeue.Iter(0, func(r remote) bool {
			ids = append(ids, r.id)
			return true
		})
		assert.Equal(t, []string{"s1", "s2"}, ids)

		hold.keepRemoteActiveTxn("s1")
		ids = ids[:0]
		hold.mu.dequeue.Iter(0, func(r remote) bool {
			ids = append(ids, r.id)
			return true
		})
		assert.Equal(t, []string{"s2", "s1"}, ids)
	})
}

func TestLockWithBindIsStale(t *testing.T) {
	runBindChangedTests(
		t,
		true,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2, _ *service,
			table uint64) {
			txnID2 := []byte("txn2")
			_, err := l2.Lock(ctx, table, [][]byte{{3}}, txnID2, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) ||
				moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound))

			checkBind(
				t,
				pb.LockTable{ServiceID: l1.serviceID, Version: alloc.version + 1, Table: table, OriginTable: table, Valid: true, AllocatorID: alloc.allocatorID},
				l2)
		},
	)
}

func TestUnlockWithBindIsStable(t *testing.T) {
	runBindChangedTests(
		t,
		true,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2, _ *service,
			table uint64) {

			txnID2 := []byte("txn2")
			l2.Unlock(ctx, txnID2, timestamp.Timestamp{})

			bind := l2.tableGroups.get(0, table).getBind()
			require.Equal(t, l1.serviceID, bind.ServiceID)
			require.Equal(t, table, bind.Table)
			require.Equal(t, table, bind.OriginTable)
			require.True(t, bind.Valid)
			require.True(t,
				bind.Version == alloc.version || bind.Version == alloc.version+1,
				"bind can stay unchanged or be refreshed asynchronously by keepRemoteLock, got %s",
				bind.DebugString())
		},
	)
}

func TestGetLockWithBindIsStable(t *testing.T) {
	runBindChangedTests(
		t,
		true,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2, _ *service,
			table uint64) {

			txnID2 := []byte("txn2")
			lt, err := l2.getLockTable(0, table)
			require.NoError(t, err)
			lt.getLock(txnID2, pb.WaitTxn{TxnID: []byte{1}}, func(l Lock) {})

			checkBind(
				t,
				pb.LockTable{ServiceID: l1.serviceID, Version: alloc.version + 1, Table: table, OriginTable: table, Valid: true, AllocatorID: alloc.allocatorID},
				l2)
		},
	)
}

func TestLockWithBindTimeout(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2, _ *service,
			table uint64) {
			// stop l1 let old bind invalid
			require.NoError(t, l1.Close())

			waitBindDisabled(t, alloc, l1.serviceID)

			txnID2 := []byte("txn2")
			// l2 holds the old bind. Once bind change is detected, the old txn is fenced
			// and the caller must retry with a new txn.
			_, err := l2.Lock(ctx, table, [][]byte{{3}}, txnID2, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))

			_, err = l2.Lock(ctx, table, [][]byte{{3}}, txnID2, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))

			txnID3 := []byte("txn3")
			_, err = l2.Lock(ctx, table, [][]byte{{3}}, txnID3, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.NoError(t, err)
			require.NoError(t, l2.Unlock(ctx, txnID2, timestamp.Timestamp{}))
			require.NoError(t, l2.Unlock(ctx, txnID3, timestamp.Timestamp{}))

			l := l2.tableGroups.get(0, table)
			assert.Equal(t, l2.serviceID, l.getBind().ServiceID)
		},
	)
}

func TestUnlockWithBindTimeout(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2, _ *service,
			table uint64) {
			// stop l1 let old bind invalid
			require.NoError(t, l1.Close())

			waitBindDisabled(t, alloc, l1.serviceID)

			txnID2 := []byte("txn2")
			assert.NoError(t, l2.Unlock(ctx, txnID2, timestamp.Timestamp{}))
			// l2 get the bind
			l := l2.tableGroups.get(0, table)
			assert.Equal(t, l2.serviceID, l.getBind().ServiceID)
		},
	)
}

func TestGetLockWithBindTimeout(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2, _ *service,
			table uint64) {
			// stop l1 let old bind invalid
			require.NoError(t, l1.Close())

			waitBindDisabled(t, alloc, l1.serviceID)

			txnID2 := []byte("txn2")
			lt, err := l2.getLockTable(0, table)
			require.NoError(t, err)
			lt.getLock(txnID2, pb.WaitTxn{TxnID: []byte{1}}, func(l Lock) {})
			// l2 get the bind
			l := l2.tableGroups.get(0, table)
			assert.Equal(t, l2.serviceID, l.getBind().ServiceID)
		},
	)
}

func TestLockWithBindNotFound(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2, l3 *service,
			table uint64) {

			// change l2's bind to s3, no bind in s3
			l2.handleBindChanged(pb.LockTable{
				Table:       table,
				ServiceID:   l3.serviceID,
				Valid:       true,
				Version:     alloc.version + 1,
				OriginTable: table,
			})

			txnID2 := []byte("txn2")
			_, err := l2.Lock(ctx, table, [][]byte{{3}}, txnID2, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			// After bind changed, handleError updates the bind and returns ErrLockTableBindChanged
			// to signal that the caller should retry with the new bind
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))

			txnID3 := []byte("txn3")
			lockCtx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
			defer cancel()
			_, err = l2.Lock(lockCtx, table, [][]byte{{3}}, txnID3, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))

			checkBind(
				t,
				pb.LockTable{ServiceID: l1.serviceID, Version: alloc.version, Table: table, OriginTable: table, Valid: true, AllocatorID: alloc.allocatorID},
				l2)

			require.NoError(t, l2.Unlock(ctx, txnID3, timestamp.Timestamp{}))
			require.NoError(t, l2.Unlock(ctx, txnID2, timestamp.Timestamp{}))
		},
	)
}

func TestUnlockWithBindNotFound(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2, l3 *service,
			table uint64) {

			// change l2's bind to s3, no bind in s3
			l2.handleBindChanged(pb.LockTable{
				Table:       table,
				ServiceID:   l3.serviceID,
				Valid:       true,
				Version:     alloc.version,
				OriginTable: table,
			})

			txnID2 := []byte("txn2")
			l2.Unlock(ctx, txnID2, timestamp.Timestamp{})

			checkBind(
				t,
				pb.LockTable{Table: table, ServiceID: l3.serviceID, Valid: true, Version: alloc.version, OriginTable: table},
				l2)
		},
	)
}

func TestGetLockWithBindNotFound(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2, l3 *service,
			table uint64) {

			// change l2's bind to s3, no bind in s3
			l2.handleBindChanged(pb.LockTable{
				Table:       table,
				ServiceID:   l3.serviceID,
				Valid:       true,
				Version:     alloc.version,
				OriginTable: table,
			})

			txnID2 := []byte("txn2")
			lt, err := l2.getLockTable(0, table)
			require.NoError(t, err)
			lt.getLock(txnID2, pb.WaitTxn{TxnID: []byte{1}}, func(l Lock) {})

			checkBind(
				t,
				pb.LockTable{ServiceID: l1.serviceID, Version: alloc.version, Table: table, OriginTable: table, Valid: true, AllocatorID: alloc.allocatorID},
				l2)
		},
	)
}

func TestIssue12554(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}
			table := uint64(10)

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, table, txn1, [][]byte{row1}, pb.Granularity_Row)

			oldBind := alloc.Get(l1.serviceID, 0, table, 0, pb.Sharding_None)
			// mock l1 restart, changed serviceID
			l1.serviceID = getServiceIdentifier("s1", time.Now().UnixNano())
			l1.tableGroups.removeWithFilter(func(u uint64, lt lockTable) bool { return u == table }, closeReasonBindChanged)
			newLockTable := l1.createLockTableByBind(oldBind)
			l1.tableGroups.set(0, table, newLockTable)

			_, err := l2.Lock(ctx, table, [][]byte{row1}, txn2, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			assert.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))

			assert.Nil(t, l1.tableGroups.get(0, table))
		},
	)
}

func TestRemoteLockRechecksBindChangedAfterGetLocalLockTable(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			table := uint64(12555)
			option := newTestRowExclusiveOptions()
			_, err := l1.Lock(ctx, table, newTestRows(1), newTestTxnID(1), option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, newTestTxnID(1), timestamp.Timestamp{}))

			oldBind := l1.tableGroups.get(0, table).getBind()
			newBind := oldBind
			newBind.ServiceID = "new-lock-owner"
			newBind.Version++

			var changed atomic.Bool
			l1.option.beforeRemoteLockBindCheck = func() {
				if changed.CompareAndSwap(false, true) {
					l1.handleBindChanged(newBind)
				}
			}
			defer func() {
				l1.option.beforeRemoteLockBindCheck = nil
			}()

			_, err = l2.Lock(ctx, table, newTestRows(2), newTestTxnID(2), option)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
			require.True(t, changed.Load())
			checkBind(t, newBind, l1)
			checkBind(t, newBind, l2)
		},
	)
}

func TestIssue14346(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			s1 := s[0]
			s2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			table := uint64(10)
			rows := newTestRows(1)

			// txn1 hold lock row1 on s1
			mustAddTestLock(t, ctx, s1, table, txn1, rows, pb.Granularity_Row)
			require.NoError(t, s1.Unlock(ctx, txn1, timestamp.Timestamp{}))

			// txn1 hold lock row1 on s2
			mustAddTestLock(t, ctx, s2, table, txn2, rows, pb.Granularity_Row)
			require.NoError(t, s2.Unlock(ctx, txn2, timestamp.Timestamp{}))

			// remove s1
			clusterservice.GetMOCluster(s1.GetConfig().ServiceID).RemoveCN("s1")
			clusterservice.GetMOCluster(s2.GetConfig().ServiceID).RemoveCN("s1")

			// wait bind remove on s2
			for {
				select {
				case <-ctx.Done():
					t.Fatal("timeout waiting for bind removal on s2")
				default:
					v, err := s2.getLockTable(0, table)
					require.NoError(t, err)
					if v == nil {
						return
					}
					time.Sleep(time.Millisecond * 100)
				}
			}
		},
	)
}

func runBindChangedTests(
	t *testing.T,
	makeBindChanged bool,
	fn func(
		ctx context.Context,
		alloc *lockTableAllocator,
		l1, l2, l3 *service,
		table uint64)) {
	var skip atomic.Bool
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2", "s3"},
		time.Second,
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txnID1 := []byte("txn1")
			txnID2 := []byte("txn2")
			table1 := uint64(10)
			// make table bind on l1
			mustAddTestLock(t, ctx, l1, table1, txnID1, [][]byte{{1}}, pb.Granularity_Row)

			// l2 get the table1's bind
			mustAddTestLock(t, ctx, l2, table1, txnID2, [][]byte{{2}}, pb.Granularity_Row)
			v, err := l2.getLockTable(0, table1)
			require.NoError(t, err)
			require.Equal(t, l1.serviceID, v.getBind().ServiceID)

			if makeBindChanged {
				// stop l1 keep lock bind
				skip.Store(true)
				lt, err := l1.getLockTable(0, table1)
				require.NoError(t, err)
				old := lt.getBind()
				waitBindDisabled(t, alloc, l1.serviceID)
				skip.Store(false)

				// make l1 get bind again, but version is changed
				waitBindChanged(t, old, l1)
				// Wait for a period of time to ensure that all RPC requests are completed
				time.Sleep(time.Millisecond * 100)
			}

			fn(ctx, alloc, l1, l2, s[2], table1)
		},
		func(c *Config) {
			c.KeepBindDuration.Duration = time.Second
			c.KeepRemoteLockDuration.Duration = time.Second

			c.RPC.BackendOptions = append(c.RPC.BackendOptions,
				morpc.WithBackendFilter(func(m morpc.Message, s string) bool {
					if req, ok := m.(*pb.Request); ok && req.Method == pb.Method_KeepLockTableBind &&
						getUUIDFromServiceIdentifier(req.KeepLockTableBind.ServiceID) == "s1" {
						return !skip.Load()
					}
					return true
				}))
		},
	)
}

func waitBindDisabled(_ *testing.T, alloc *lockTableAllocator, sid string) {
	b := alloc.getServiceBinds(sid)
	if b == nil {
		return
	}
	b.disable()
	alloc.disableTableBinds(b)
}

func waitBindChanged(
	t *testing.T,
	old pb.LockTable,
	l *service) {
	for {
		lt, err := l.getLockTableWithCreate(0, old.Table, nil, pb.Sharding_None)
		require.NoError(t, err)
		new := lt.getBind()
		if new.Changed(old) {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func checkBind(
	t *testing.T,
	bind pb.LockTable,
	s *service) {
	l := s.tableGroups.get(0, bind.Table)
	assert.Equal(t, bind, l.getBind())
}

// TestRemoteLockWaitTimeout_PrecisionIndependentOfLazyCheck verifies that
// the async lock_wait_timeout fires at the waiter's own deadline, NOT on
// the coarse defaultLazyCheckDuration tick.  It temporarily sets the
// global lazy-check interval to 10s, so without the precise AfterFunc
// timer the test would timeout or take >10s.
func TestRemoteLockWaitTimeout_PrecisionIndependentOfLazyCheck(t *testing.T) {
	// Force the lazy-check interval to 10s so any enforcement that relies
	// only on the periodic check() tick would take ≥10s.
	orig := defaultLazyCheckDuration.Load().(time.Duration)
	defaultLazyCheckDuration.Store(10 * time.Second)
	defer defaultLazyCheckDuration.Store(orig)

	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)
			l1 := s[0] // lock-table owner
			l2 := s[1] // remote CN
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}

			// txn1 holds the lock.
			mustAddTestLock(t, ctx, l1, tableID, txn1, [][]byte{row1}, pb.Granularity_Row)

			// txn2 on remote CN requests with 1-second timeout.
			// With the check tick at 10s, a coarse-tick-only approach
			// would need >10s.  The precise AfterFunc timer must fire at ~1s.
			start := time.Now()
			_, err := l2.Lock(
				ctx, tableID, [][]byte{row1}, txn2,
				pb.LockOptions{
					Granularity:     pb.Granularity_Row,
					Mode:            pb.LockMode_Exclusive,
					Policy:          pb.WaitPolicy_Wait,
					LockWaitTimeout: 1,
				})
			elapsed := time.Since(start)

			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockWaitTimeout),
				"expected lock-timeout, got %v", err)
			// Must fire well before the 10s coarse tick.
			require.Less(t, elapsed, 3*time.Second,
				"precise timer should fire at ~1s, elapsed=%v", elapsed)
		},
	)
}

func TestRemoteOwnerLocalDeadlockFastPathBreaksPartialLockRing(t *testing.T) {
	txn1 := []byte("txn1")
	txn2 := []byte("txn2")
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Second*10,
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)
			owner := s[0]
			origin := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			// Pin the lock table to s1, then use s2 as the remote origin.
			mustAddTestLock(t, ctx, owner, tableID, []byte("pin"), [][]byte{{0}}, pb.Granularity_Row)
			require.NoError(t, owner.Unlock(ctx, []byte("pin"), timestamp.Timestamp{}))

			row1 := []byte{1}
			row2 := []byte{2}
			opt := newTestRowExclusiveOptions()

			mustAddTestLock(t, ctx, origin, tableID, txn1, [][]byte{row1}, pb.Granularity_Row)
			mustAddTestLock(t, ctx, origin, tableID, txn2, [][]byte{row2}, pb.Granularity_Row)

			errC := make(chan error, 1)
			go func() {
				_, err := origin.Lock(ctx, tableID, [][]byte{row2}, txn1, opt)
				errC <- err
			}()
			require.NoError(t, WaitWaiters(owner, 0, tableID, row2, 1))

			_, err := origin.Lock(ctx, tableID, [][]byte{row1}, txn2, opt)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrDeadLockDetected), "got %v", err)
			require.NoError(t, origin.Unlock(ctx, txn2, timestamp.Timestamp{}))

			require.NoError(t, <-errC)
			require.NoError(t, origin.Unlock(ctx, txn1, timestamp.Timestamp{}))
		},
		func(cfg *Config) {
			cfg.TxnIterFunc = func(fn func([]byte) bool) {
				fn(txn1)
				fn(txn2)
			}
		},
	)
}

func TestRemoteLockOwnerWaitTimeoutReturnsDedicatedError(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Second*10,
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)
			owner := s[0]
			origin := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}
			mustAddTestLock(t, ctx, owner, tableID, txn1, [][]byte{row1}, pb.Granularity_Row)

			opt := newTestRowExclusiveOptions()
			opt.LockWaitTimeout = 0
			start := time.Now()
			_, err := origin.Lock(ctx, tableID, [][]byte{row1}, txn2, opt)
			elapsed := time.Since(start)

			require.True(t, moerr.IsMoErrCode(err, moerr.ErrRemoteLockWaitTimeout), "got %v", err)
			require.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
			require.Less(t, elapsed, 2*time.Second)
			require.NoError(t, origin.Unlock(ctx, txn2, timestamp.Timestamp{}))
			require.NoError(t, owner.Unlock(ctx, txn1, timestamp.Timestamp{}))
		},
		func(cfg *Config) {
			cfg.RemoteLockOwnerWaitTimeout = &toml.Duration{Duration: 100 * time.Millisecond}
		},
	)
}

// TestRemoteLockWaitTimeout_ReturnsLockTimeout ensures that in a multi-CN
// deployment, when txn2 on a remote CN waits on a lock held by txn1 on the
// lock-table owner CN, txn2 receives ErrLockTimeout (lock timeout) after
// LockWaitTimeout elapses, NOT a retryable connectivity/backend error.
//
// Before the fix, the client-side RPC deadline was set to exactly
// LockWaitTimeout. The lock-table owner started its own wait budget only
// after receiving the RPC, so the client deadline could fire before the
// owner returned ErrLockTimeout.  When that happened the client saw a
// retryable error instead of lock-timeout.
func TestRemoteLockWaitTimeout_ReturnsLockTimeout(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)
			l1 := s[0] // lock-table owner
			l2 := s[1] // remote CN
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}

			// txn1 acquires and holds the lock on the owner.
			mustAddTestLock(t, ctx, l1, tableID, txn1, [][]byte{row1}, pb.Granularity_Row)

			// txn2 on the remote CN tries to lock the same row with a 1-second
			// LockWaitTimeout.
			opt := pb.LockOptions{
				Granularity:     pb.Granularity_Row,
				Mode:            pb.LockMode_Exclusive,
				Policy:          pb.WaitPolicy_Wait,
				LockWaitTimeout: 1, // 1 second
			}

			start := time.Now()
			_, err := l2.Lock(ctx, tableID, [][]byte{row1}, txn2, opt)
			elapsed := time.Since(start)

			require.Error(t, err)
			// Must receive lock-timeout, not connectivity/backend error.
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockWaitTimeout),
				"expected ErrLockWaitTimeout, got %v", err)
			require.Contains(t, err.Error(), "Lock wait timeout exceeded",
				"expected lock timeout message, got %v", err)
			require.GreaterOrEqual(t, elapsed, time.Second,
				"should have waited at least LockWaitTimeout")
			// With the lazy check interval at 50ms in tests, the async timeout
			// should be detected very close to LockWaitTimeout.  Allow a
			// generous upper bound to absorb scheduling jitter.
			require.Less(t, elapsed, 5*time.Second,
				"should not wait beyond LockWaitTimeout + check interval")
		},
	)
}
