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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestResolveCommitUnknownWaitsForAllocatorFence(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		service := services[0]
		holderTxn := []byte("holder")
		waiterTxn := []byte("waiter")
		key := []byte("key")
		options := newTestRowExclusiveOptions()

		_, err := service.Lock(ctx, 1, [][]byte{key}, holderTxn, options)
		require.NoError(t, err)
		_, err = allocator.Valid(service.serviceID, holderTxn, nil)
		require.NoError(t, err)
		require.NoError(t, service.ResolveCommitUnknown(holderTxn))

		type lockResult struct {
			result pb.Result
			err    error
		}
		resultC := make(chan lockResult, 1)
		go func() {
			result, err := service.Lock(ctx, 1, [][]byte{key}, waiterTxn, options)
			resultC <- lockResult{result: result, err: err}
		}()
		waitWaiters(t, service, 1, key, 1)

		require.Never(t, func() bool {
			select {
			case <-resultC:
				return true
			default:
				return false
			}
		}, 200*time.Millisecond, 10*time.Millisecond)

		allocator.FinishCommit(service.serviceID, holderTxn)

		var result lockResult
		require.Eventually(t, func() bool {
			select {
			case result = <-resultC:
				return true
			default:
				return false
			}
		}, 2*time.Second, 10*time.Millisecond)
		require.NoError(t, result.err)
		require.True(t, result.result.HasConflict)
		require.True(t, result.result.HasPrevCommit)
		require.False(t, result.result.Timestamp.IsEmpty())

		require.NoError(t, service.Unlock(ctx, waiterTxn, timestamp.Timestamp{}))
	})
}

func TestUnknownCommitBatchFencesOnlyNonCommittingTxns(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		service := services[0]
		committingTxn := []byte("committing")
		unstartedTxn := []byte("unstarted")

		_, err := allocator.Valid(service.serviceID, committingTxn, nil)
		require.NoError(t, err)

		committing, fenceTS, ok := service.canUnlockUnknownCommits(
			context.Background(),
			[][]byte{committingTxn, unstartedTxn},
		)
		require.True(t, ok)
		_, ok = committing[string(committingTxn)]
		require.True(t, ok)
		_, ok = committing[string(unstartedTxn)]
		require.False(t, ok)
		require.False(t, fenceTS.IsEmpty())

		_, err = allocator.Valid(service.serviceID, unstartedTxn, nil)
		require.Error(t, err, "the batch fence must reject a late Commit")
		allocator.FinishCommit(service.serviceID, committingTxn)
	})
}

func TestUnknownCommitFenceSurvivesLiveServiceCleanup(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		service := services[0]
		txnID := []byte("late-commit")

		_, _, ok := service.canUnlockUnknownCommits(
			context.Background(),
			[][]byte{txnID},
		)
		require.True(t, ok)

		// The source CN is live but its TxnIterFunc no longer contains the
		// closed operator. That is not enough to prove an already buffered
		// Commit cannot still reach TN.
		allocator.cleanCommitStateOnce(
			context.Background(),
			func(context.Context, string) (bool, [][]byte, error) {
				return true, nil, nil
			},
			time.Hour,
		)

		state, exists := allocator.getCtl(service.serviceID).getCommitState(string(txnID))
		require.True(t, exists)
		require.Equal(t, cannotCommitState, state.state)
		require.True(t, state.persist)

		_, err := allocator.Valid(service.serviceID, txnID, nil)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCannotCommitOrphan))
	})
}

func TestUnknownCommitResolverCloseCancelsRemoteUnlock(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(_ *lockTableAllocator, services []*service) {
		service := services[0]
		client := &blockingUnlockClient{unlockStarted: make(chan struct{}, 1)}
		bind := pb.LockTable{
			Group:     0,
			Table:     100,
			ServiceID: "unreachable",
			Version:   1,
			Valid:     true,
		}
		service.tableGroups.set(
			bind.Group,
			bind.Table,
			newRemoteLockTable(
				service.serviceID,
				time.Second,
				bind,
				client,
				service.handleBindChanged,
				service.logger,
			),
		)

		txnID := []byte("unknown-commit-remote-unlock")
		txn := service.activeTxnHolder.getActiveTxn(txnID, true, "")
		txn.Lock()
		require.NoError(t, txn.lockAdded(bind.Group, bind, [][]byte{[]byte("key")}, service.logger))
		txn.Unlock()
		require.NoError(t, service.ResolveCommitUnknown(txnID))

		select {
		case <-client.unlockStarted:
		case <-time.After(time.Second):
			require.FailNow(t, "unknown commit resolver did not start remote unlock")
		}

		closed := make(chan error, 1)
		go func() {
			closed <- service.Close()
		}()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(time.Second):
			require.FailNow(t, "service close blocked on remote unknown-commit unlock")
		}
	})
}
