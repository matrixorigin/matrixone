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
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowWaiterRetryAfterRangeReplacement(t *testing.T) {
	tests := []struct {
		name   string
		rows   [][]byte
		opts   pb.LockOptions
		remote bool
		cancel bool
	}{
		{name: "shared rows cancel", rows: newTestRows(11, 12), opts: newTestRowSharedOptions(), cancel: true},
		{name: "shared rows success", rows: newTestRows(11, 12), opts: newTestRowSharedOptions()},
		{name: "exclusive rows success", rows: newTestRows(11, 12), opts: newTestRowExclusiveOptions()},
		{name: "multiple ranges success", rows: newTestRows(10, 11, 12, 16), opts: newTestRangeExclusiveOptions()},
		{name: "async shared rows success", rows: newTestRows(11, 12), opts: newTestRowSharedOptions(), remote: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			getRunner(test.remote)(t, 10, func(ctx context.Context, s *service, lt *localLockTable) {
				guardCtx := ctx
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				holder, requester, replacement := newTestTxnID(1), newTestTxnID(2), newTestTxnID(3)
				if test.remote {
					for _, id := range [][]byte{holder, requester, replacement} {
						require.NoError(t, s.RegisterExternalTxn(id))
						defer s.UnregisterExternalTxn(id)
					}
				}
				rangeRows := newTestRows(12, 16)
				lt.mu.RLock()
				firstCommit := lt.mu.tableCommittedAt
				lt.mu.RUnlock()
				firstCommit.PhysicalTime++
				secondCommit := firstCommit
				secondCommit.PhysicalTime++
				_, err := s.Lock(ctx, 10, rangeRows, holder, newTestRangeExclusiveOptions())
				require.NoError(t, err)
				defer func() { assert.NoError(t, s.Unlock(context.Background(), holder, timestamp.Timestamp{})) }()

				waiting := make(chan *waiter, 2)
				notified := make(chan struct{})
				retry := make(chan struct{})
				var afterFirst sync.Once
				restore := SetWaiterEnqueuedHookForTest(func(table uint64, txnID []byte, _ [][]byte) {
					if table != 10 || !bytes.Equal(txnID, requester) {
						return
					}
					lt.mu.store.Iter(func(_ []byte, lock Lock) bool {
						found := false
						lock.waiters.iter(func(w *waiter) bool {
							if w.isTxn(requester) {
								w.ref("test observation", lt.logger)
								waiting <- w
								found = true
							}
							return !found
						})
						return !found
					})
				})
				defer restore()
				lt.options.afterWait = func(c *lockContext) func() {
					return func() {
						if bytes.Equal(c.txn.txnID, requester) {
							afterFirst.Do(func() {
								close(notified)
								select {
								case <-retry:
								case <-ctx.Done():
								}
							})
						}
					}
				}
				type outcome struct {
					result pb.Result
					err    error
				}
				done := make(chan outcome, 1)
				var wg sync.WaitGroup
				wg.Add(1)
				defer func() {
					cancel()
					wg.Wait()
					for len(waiting) > 0 {
						(<-waiting).close("test observation", lt.logger)
					}
					assert.NoError(t, s.Unlock(context.Background(), requester, timestamp.Timestamp{}))
				}()
				go func() {
					defer wg.Done()
					result, err := s.Lock(ctx, 10, test.rows, requester, test.opts)
					done <- outcome{result, err}
				}()
				nextWaiter := func() *waiter {
					select {
					case w := <-waiting:
						return w
					case <-ctx.Done():
						t.Fatalf("request did not reach wait phase: %v", ctx.Err())
						return nil
					}
				}
				first := nextWaiter()
				defer first.close("test observation", lt.logger)
				require.NoError(t, s.Unlock(ctx, holder, firstCommit))
				select {
				case <-notified:
				case <-ctx.Done():
					t.Fatal("request was not notified")
				}
				_, err = s.Lock(ctx, 10, rangeRows, replacement, newTestRangeExclusiveOptions())
				require.NoError(t, err)
				defer func() { assert.NoError(t, s.Unlock(context.Background(), replacement, timestamp.Timestamp{})) }()
				close(retry)
				second := nextWaiter()
				defer second.close("test observation", lt.logger)
				if test.cancel {
					cancel()
				} else {
					require.NoError(t, s.Unlock(ctx, replacement, secondCommit))
				}
				select {
				case outcome := <-done:
					if test.cancel {
						require.ErrorIs(t, outcome.err, context.Canceled)
					} else {
						require.NoError(t, outcome.err)
						require.True(t, outcome.result.Timestamp.GreaterEq(secondCommit))
						require.True(t, outcome.result.HasConflict)
					}
				case <-guardCtx.Done():
					t.Fatal("request did not finish after holder release")
				}
				// Only the observer references may survive a completed request.
				expected := int32(1)
				if first == second {
					expected++
				}
				require.Equal(t, expected, first.refCount.Load(), "previous wait generation leaked an owner")
				require.Same(t, first, second, "retry must preserve the conflicting request's waiter")
			})
		})
	}
}

func TestWaiterReleasedWhenNextConflictDetectsDeadlock(t *testing.T) {
	getRunner(true)(t, 10, func(ctx context.Context, s *service, lt *localLockTable) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		holder, requester, competitor := newTestTxnID(1), newTestTxnID(2), newTestTxnID(3)
		for _, id := range [][]byte{holder, requester, competitor} {
			require.NoError(t, s.RegisterExternalTxn(id))
			defer s.UnregisterExternalTxn(id)
		}
		_, err := s.Lock(ctx, 10, newTestRows(12, 16), holder, newTestRangeExclusiveOptions())
		require.NoError(t, err)
		defer func() { assert.NoError(t, s.Unlock(context.Background(), holder, timestamp.Timestamp{})) }()
		_, err = s.Lock(ctx, 10, newTestRows(18), competitor, newTestRowExclusiveOptions())
		require.NoError(t, err)
		defer func() { assert.NoError(t, s.Unlock(context.Background(), competitor, timestamp.Timestamp{})) }()

		requestWaiting := make(chan *waiter, 1)
		competitorWaiting := make(chan struct{}, 1)
		restore := SetWaiterEnqueuedHookForTest(func(table uint64, txnID []byte, _ [][]byte) {
			if table != 10 {
				return
			}
			switch {
			case bytes.Equal(txnID, requester):
				lt.mu.store.Iter(func(_ []byte, lock Lock) bool {
					found := false
					lock.waiters.iter(func(w *waiter) bool {
						if w.isTxn(requester) {
							w.ref("test observation", lt.logger)
							requestWaiting <- w
							found = true
						}
						return !found
					})
					return !found
				})
			case bytes.Equal(txnID, competitor):
				competitorWaiting <- struct{}{}
			}
		})
		defer restore()
		var wg sync.WaitGroup
		defer func() {
			cancel()
			// Releasing the requester's partial locks also terminates the
			// competing remote request if an assertion stopped this test early.
			assert.NoError(t, s.Unlock(context.Background(), requester, timestamp.Timestamp{}))
			wg.Wait()
			for len(requestWaiting) > 0 {
				(<-requestWaiting).close("test observation", lt.logger)
			}
		}()
		requestDone, competitorDone := make(chan error, 1), make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Lock(ctx, 10, newTestRows(11, 12, 18), requester, newTestRowExclusiveOptions())
			requestDone <- err
		}()
		var first *waiter
		select {
		case first = <-requestWaiting:
		case <-ctx.Done():
			t.Fatal("request did not wait on the range")
		}
		defer first.close("test observation", lt.logger)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Lock(ctx, 10, newTestRows(11), competitor, newTestRowExclusiveOptions())
			competitorDone <- err
		}()
		select {
		case <-competitorWaiting:
		case <-ctx.Done():
			t.Fatal("competitor did not wait on the requester's prefix")
		}
		// The retry admits row 12, then rejects the new conflict at row 18
		// before queue admission because competitor already waits for row 11.
		require.NoError(t, s.Unlock(ctx, holder, timestamp.Timestamp{}))
		select {
		case err := <-requestDone:
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrDeadLockDetected), "unexpected error: %v", err)
		case <-ctx.Done():
			t.Fatal("next conflict did not terminate the request")
		}
		require.Equal(t, int32(1), first.refCount.Load(), "consumed waiter must be retired before the error return")
		require.NoError(t, s.Unlock(ctx, requester, timestamp.Timestamp{}))
		select {
		case err := <-competitorDone:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("competitor did not acquire the released prefix")
		}
	})
}

func TestRowHolderOfSharedRangeUnlock(t *testing.T) {
	for _, rows := range []struct {
		name      string
		keys      [][]byte
		mutations []pb.ExtraMutation
		remote    bool
	}{
		{name: "start", keys: newTestRows(12)},
		{name: "interior", keys: newTestRows(14)},
		{name: "end", keys: newTestRows(16)},
		{name: "multiple rows", keys: newTestRows(12, 14, 16)},
		{name: "range then row", keys: newTestRows(12, 18)},
		{name: "range then row with mutation", keys: newTestRows(12, 18), mutations: []pb.ExtraMutation{{Key: []byte{18}}}},
		{name: "remote range holder", keys: newTestRows(14), remote: true},
	} {
		for _, rowFirst := range []bool{true, false} {
			name := rows.name + "/range first"
			if rowFirst {
				name = rows.name + "/row first"
			}
			t.Run(name, func(t *testing.T) {
				getRunner(rows.remote)(t, 10, func(ctx context.Context, s *service, lt *localLockTable) {
					rangeTxn, rowTxn := newTestTxnID(1), newTestTxnID(2)
					if rows.remote {
						for _, id := range [][]byte{rangeTxn, rowTxn} {
							require.NoError(t, s.RegisterExternalTxn(id))
							defer s.UnregisterExternalTxn(id)
						}
					}
					_, err := s.Lock(ctx, 10, newTestRows(12, 16), rangeTxn, newTestRangeSharedOptions())
					require.NoError(t, err)
					defer func() { assert.NoError(t, s.Unlock(context.Background(), rangeTxn, timestamp.Timestamp{})) }()
					_, err = s.Lock(ctx, 10, rows.keys, rowTxn, newTestRowSharedOptions())
					require.NoError(t, err)
					defer func() { assert.NoError(t, s.Unlock(context.Background(), rowTxn, timestamp.Timestamp{})) }()
					first, last := rangeTxn, rowTxn
					if rowFirst {
						first, last = rowTxn, rangeTxn
					}
					require.NoError(t, s.Unlock(ctx, first, timestamp.Timestamp{}, rows.mutations...))
					for _, key := range [][]byte{{12}, {16}} {
						lt.mu.RLock()
						lock, ok := lt.mu.store.Get(key)
						var holders []pb.WaitTxn
						if ok {
							holders = lock.holders.getTxnSlice()
						}
						lt.mu.RUnlock()
						require.True(t, ok, "remaining holder must retain both range endpoints")
						require.Len(t, holders, 1, "unlock must remove the departing holder")
						require.Equal(t, last, holders[0].TxnID)
					}
					require.NoError(t, s.Unlock(ctx, last, timestamp.Timestamp{}, rows.mutations...))
					lt.mu.RLock()
					remaining := lt.mu.store.Len()
					lt.mu.RUnlock()
					require.Zero(t, remaining, "last unlock must release the complete range")
				})
			})
		}
	}
}

func TestRowRangeHolderBookkeepingFailureIsAtomic(t *testing.T) {
	getRunner(false)(t, 10, func(ctx context.Context, s *service, lt *localLockTable) {
		rangeTxn, rowTxn := newTestTxnID(1), newTestTxnID(2)
		_, err := s.Lock(ctx, 10, newTestRows(12, 16), rangeTxn, newTestRangeSharedOptions())
		require.NoError(t, err)
		defer func() { assert.NoError(t, s.Unlock(context.Background(), rangeTxn, timestamp.Timestamp{})) }()
		txn := s.activeTxnHolder.getActiveTxn(rowTxn, true, "")
		txn.Lock()
		txn.beforeLockAdded = func(_ []byte, keys [][]byte) error {
			assert.Equal(t, newTestRows(12, 16), keys)
			return ErrTxnNotFound
		}
		txn.Unlock()
		defer func() {
			txn.Lock()
			txn.beforeLockAdded = nil
			txn.Unlock()
			assert.NoError(t, s.Unlock(context.Background(), rowTxn, timestamp.Timestamp{}))
		}()
		_, err = s.Lock(ctx, 10, newTestRows(14), rowTxn, newTestRowSharedOptions())
		require.ErrorIs(t, err, ErrTxnNotFound)
		for _, key := range [][]byte{{12}, {16}} {
			lt.mu.RLock()
			lock, ok := lt.mu.store.Get(key)
			count := lock.holders.size()
			contains := lock.holders.contains(rowTxn)
			lt.mu.RUnlock()
			require.True(t, ok)
			require.Equal(t, 1, count)
			require.False(t, contains, "failed bookkeeping must not publish a range holder")
		}
		txn.Lock()
		txn.beforeLockAdded = nil
		txn.Unlock()
		_, err = s.Lock(ctx, 10, newTestRows(14), rowTxn, newTestRowSharedOptions())
		require.NoError(t, err, "request can retry after bookkeeping failure")
	})
}

func TestProxySharedRowOnRangeRetainsOwnership(t *testing.T) {
	for _, row := range []byte{12, 14, 16} {
		t.Run(fmt.Sprintf("row-%d", row), func(t *testing.T) {
			getRunner(true)(t, 10, func(ctx context.Context, origin *service, owner *localLockTable) {
				origin.cfg.EnableRemoteLocalProxy = true
				rangeTxn, representative, follower, writer := newTestTxnID(1), newTestTxnID(2), newTestTxnID(3), newTestTxnID(4)
				for _, id := range [][]byte{rangeTxn, representative, follower, writer} {
					require.NoError(t, origin.RegisterExternalTxn(id))
					defer origin.UnregisterExternalTxn(id)
				}
				_, err := origin.Lock(ctx, 10, newTestRows(12, 16), rangeTxn, newTestRangeSharedOptions())
				require.NoError(t, err)
				defer func() { assert.NoError(t, origin.Unlock(context.Background(), rangeTxn, timestamp.Timestamp{})) }()
				_, err = origin.Lock(ctx, 10, newTestRows(row), representative, newTestRowSharedOptions())
				require.NoError(t, err)
				defer func() { assert.NoError(t, origin.Unlock(context.Background(), representative, timestamp.Timestamp{})) }()
				_, err = origin.Lock(ctx, 10, newTestRows(row), follower, newTestRowSharedOptions())
				require.NoError(t, err)
				defer func() { assert.NoError(t, origin.Unlock(context.Background(), follower, timestamp.Timestamp{})) }()
				require.NoError(t, origin.Unlock(ctx, representative, timestamp.Timestamp{}))
				require.NoError(t, origin.Unlock(ctx, rangeTxn, timestamp.Timestamp{}))
				options := newTestRowExclusiveOptions()
				options.Policy = pb.WaitPolicy_FastFail
				_, err = origin.Lock(ctx, 10, newTestRows(row), writer, options)
				defer func() { assert.NoError(t, origin.Unlock(context.Background(), writer, timestamp.Timestamp{})) }()
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockConflict), "active Shared follower must block an Exclusive writer: %v", err)
				require.NoError(t, origin.Unlock(ctx, follower, timestamp.Timestamp{}))
				_, err = origin.Lock(ctx, 10, newTestRows(row), writer, options)
				require.NoError(t, err, "last follower release must unblock the writer")
				require.NoError(t, origin.Unlock(ctx, writer, timestamp.Timestamp{}))
				owner.mu.RLock()
				remaining := owner.mu.store.Len()
				owner.mu.RUnlock()
				require.Zero(t, remaining)
			})
		})
	}
}

func TestProxyMergedRangeHandoffIsAtomicAndRetryable(t *testing.T) {
	getRunner(true)(t, 10, func(ctx context.Context, origin *service, owner *localLockTable) {
		origin.cfg.EnableRemoteLocalProxy = true
		source, first, second, writer := newTestTxnID(1), newTestTxnID(2), newTestTxnID(3), newTestTxnID(4)
		for _, id := range [][]byte{source, first, second, writer} {
			require.NoError(t, origin.RegisterExternalTxn(id))
			defer origin.UnregisterExternalTxn(id)
			defer func() { assert.NoError(t, origin.Unlock(context.Background(), id, timestamp.Timestamp{})) }()
		}
		for _, row := range []byte{12, 14, 16} {
			_, err := origin.Lock(ctx, 10, newTestRows(row), source, newTestRowSharedOptions())
			require.NoError(t, err)
			follower := second
			if row == 12 {
				follower = first
			}
			_, err = origin.Lock(ctx, 10, newTestRows(row), follower, newTestRowSharedOptions())
			require.NoError(t, err)
		}
		_, err := origin.Lock(ctx, 10, newTestRows(10, 18), source, newTestRangeSharedOptions())
		require.NoError(t, err)
		_, err = origin.Lock(ctx, 10, newTestRows(20), source, newTestRowSharedOptions())
		require.NoError(t, err)
		mutations := []pb.ExtraMutation{
			{Key: []byte{10}, Skip: true},
			{Key: []byte{12}, ReplaceTo: first},
			{Key: []byte{12}, ReplaceTo: writer}, // First logical mutation wins.
			{Key: []byte{14}, ReplaceTo: second},
			{Key: []byte{16}, ReplaceTo: second},
			{Key: []byte{20}},
		}
		ownerSource := owner.txnHolder.getActiveTxn(source, false, "")
		require.NotNil(t, ownerSource)
		ownerSecond := owner.txnHolder.getActiveTxn(second, true, origin.serviceID)
		ownerSecond.Lock()
		ownerSecond.beforeLockAdded = func([]byte, [][]byte) error { return ErrTxnNotFound }
		ownerSecond.Unlock()
		injected := true
		defer func() {
			if injected {
				ownerSecond.Lock()
				ownerSecond.beforeLockAdded = nil
				ownerSecond.Unlock()
			}
		}()
		applyHandoff := func() error {
			ownerSource.Lock()
			defer ownerSource.Unlock()
			return owner.unlockWithContext(ctx, ownerSource, ownerSource.lockHolders[0].tableKeys[10], timestamp.Timestamp{}, mutations...)
		}
		require.ErrorIs(t, applyHandoff(), ErrTxnNotFound)
		owner.mu.RLock()
		lock, ok := owner.mu.store.Get([]byte{18})
		originalOnly := ok && lock.holders.size() == 1 && lock.holders.contains(source)
		owner.mu.RUnlock()
		require.True(t, originalOnly, "failed preparation must preserve source and publish no replacement")
		ownerSecond.Lock()
		ownerSecond.beforeLockAdded = nil
		ownerSecond.Unlock()
		injected = false
		require.NoError(t, applyHandoff())
		require.NoError(t, applyHandoff(), "lost-response replay must not duplicate physical memberships")
		for _, id := range [][]byte{first, second} {
			txn := owner.txnHolder.getActiveTxn(id, false, "")
			require.NotNil(t, txn)
			txn.RLock()
			var keys [][]byte
			txn.lockHolders[0].tableKeys[10].mustGet().iter(func(key []byte) bool {
				keys = append(keys, bytes.Clone(key))
				return true
			})
			txn.RUnlock()
			require.Equal(t, newTestRows(10, 18), keys, "each distinct replacement needs exactly one complete pair")
		}
		// Complete the proxy acknowledgement after the owner applied the
		// handoff more than once, then release replacements independently.
		require.NoError(t, origin.Unlock(ctx, source, timestamp.Timestamp{}))
		require.NoError(t, origin.Unlock(ctx, first, timestamp.Timestamp{}))
		owner.mu.RLock()
		lock, ok = owner.mu.store.Get([]byte{18})
		secondOnly := ok && lock.holders.size() == 1 && lock.holders.contains(second)
		owner.mu.RUnlock()
		require.True(t, secondOnly)
		options := newTestRowExclusiveOptions()
		options.Policy = pb.WaitPolicy_FastFail
		_, err = origin.Lock(ctx, 10, newTestRows(14), writer, options)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockConflict), "remaining replacement must retain physical ownership: %v", err)
		require.NoError(t, origin.Unlock(ctx, second, timestamp.Timestamp{}))
		_, err = origin.Lock(ctx, 10, newTestRows(14), writer, options)
		require.NoError(t, err)
		require.NoError(t, origin.Unlock(ctx, writer, timestamp.Timestamp{}))
		owner.mu.RLock()
		remaining := owner.mu.store.Len()
		owner.mu.RUnlock()
		require.Zero(t, remaining)
	})
}
