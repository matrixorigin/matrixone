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
	"sync"
	"testing"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestRemoteBindRefFollowsTxnCleanup(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "source"}, func(_ *lockTableAllocator, services []*service) {
		owner := services[0]
		source := services[1]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		const table = uint64(27535)
		ownerTxn := []byte("owner-anchor")
		txn1 := []byte("source-1")
		txn2 := []byte("source-2")
		options := newTestRowExclusiveOptions()

		_, err := owner.Lock(ctx, table, [][]byte{{9}}, ownerTxn, options)
		require.NoError(t, err)
		defer func() { _ = owner.Unlock(context.Background(), ownerTxn, timestamp.Timestamp{}) }()

		_, err = source.Lock(ctx, table, [][]byte{{1}}, txn1, options)
		require.NoError(t, err)
		defer func() { _ = source.Unlock(context.Background(), txn1, timestamp.Timestamp{}) }()
		_, err = source.Lock(ctx, table, [][]byte{{3}}, txn1, options)
		require.NoError(t, err)
		_, err = source.Lock(ctx, table, [][]byte{{2}}, txn2, options)
		require.NoError(t, err)
		defer func() { _ = source.Unlock(context.Background(), txn2, timestamp.Timestamp{}) }()

		cached := source.tableGroups.get(options.Group, table)
		require.NotNil(t, cached)
		bind := cached.getBind()
		require.Equal(t, owner.serviceID, bind.ServiceID)
		require.Equal(t, []pb.LockTable{bind}, source.collectRemoteLockBinds(nil))

		require.NoError(t, source.Unlock(ctx, txn1, timestamp.Timestamp{}))
		require.Equal(t, []pb.LockTable{bind}, source.collectRemoteLockBinds(nil),
			"another transaction still depends on the same remote bind")

		require.NoError(t, source.Unlock(ctx, txn2, timestamp.Timestamp{}))
		require.Empty(t, source.collectRemoteLockBinds(nil))
		require.Equal(t, bind, source.tableGroups.get(options.Group, table).getBind(),
			"route caching is independent from remote-lock lease ownership")
	})
}

func TestRemoteBindRefExistsBeforeOwnerProcessesLock(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "source"}, func(_ *lockTableAllocator, services []*service) {
		owner := services[0]
		source := services[1]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		const table = uint64(27538)
		options := newTestRowExclusiveOptions()
		ownerTxn := []byte("owner-anchor")
		_, err := owner.Lock(ctx, table, [][]byte{{9}}, ownerTxn, options)
		require.NoError(t, err)
		defer func() { _ = owner.Unlock(context.Background(), ownerTxn, timestamp.Timestamp{}) }()

		ownerStarted := make(chan struct{})
		releaseOwner := make(chan struct{})
		var startOnce sync.Once
		var releaseOnce sync.Once
		owner.option.beforeRemoteLockBindCheck = func() {
			startOnce.Do(func() { close(ownerStarted) })
			<-releaseOwner
		}
		defer func() {
			owner.option.beforeRemoteLockBindCheck = nil
			releaseOnce.Do(func() { close(releaseOwner) })
		}()

		txnID := []byte("inflight-remote-lock")
		lockDone := make(chan error, 1)
		go func() {
			_, err := source.Lock(ctx, table, [][]byte{{1}}, txnID, options)
			lockDone <- err
		}()

		select {
		case <-ownerStarted:
		case <-ctx.Done():
			t.Fatal("remote owner did not receive the lock request")
		}
		bind := owner.tableGroups.get(options.Group, table).getBind()
		require.Equal(t, []pb.LockTable{bind}, source.collectRemoteLockBinds(nil),
			"the source must publish its lease responsibility before the lock RPC can take effect")

		releaseOnce.Do(func() { close(releaseOwner) })
		select {
		case err := <-lockDone:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("remote lock did not finish")
		}
		require.NoError(t, source.Unlock(ctx, txnID, timestamp.Timestamp{}))
		require.Empty(t, source.collectRemoteLockBinds(nil))
	})
}

func TestRemoteBindRefStaysWhileOrdinaryCleanupIsBlocked(t *testing.T) {
	runLockServiceTests(t, []string{"source"}, func(_ *lockTableAllocator, services []*service) {
		source := services[0]
		bind := pb.LockTable{
			Group:       0,
			Table:       27536,
			OriginTable: 27536,
			ServiceID:   "remote-owner",
			Version:     1,
			Valid:       true,
		}
		table := &blockingUnlockTestTable{
			retryableUnlockTestTable: retryableUnlockTestTable{bind: bind},
			started:                  make(chan struct{}),
			release:                  make(chan struct{}),
		}
		var releaseOnce sync.Once
		t.Cleanup(func() {
			releaseOnce.Do(func() { close(table.release) })
		})
		source.tableGroups.set(bind.Group, bind.Table, table)

		txnID := []byte("blocked-cleanup")
		txn := source.activeTxnHolder.getActiveTxn(txnID, true, "")
		txn.Lock()
		txn.lockTableBindTouched(bind)
		require.NoError(t, txn.lockAdded(
			bind.Group, bind, [][]byte{{1}}, pb.LockOptions{}, source.logger))
		txn.Unlock()
		source.acquireRemoteBindRef(bind)

		unlockDone := make(chan error, 1)
		go func() {
			unlockDone <- source.Unlock(context.Background(), txnID, timestamp.Timestamp{})
		}()
		select {
		case <-table.started:
		case <-time.After(time.Second):
			t.Fatal("remote cleanup did not start")
		}

		require.True(t, source.activeTxnHolder.hasActiveTxn(txnID),
			"cleanup keeps the source transaction fenced until every owner acknowledges")
		require.Equal(t, []pb.LockTable{bind}, source.collectRemoteLockBinds(nil),
			"the source-local bind ref must cover the cleanup visibility gap")

		releaseOnce.Do(func() { close(table.release) })
		select {
		case err := <-unlockDone:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("remote cleanup did not finish")
		}
		require.Empty(t, source.collectRemoteLockBinds(nil))
	})
}

func TestRemoteBindRefSurvivesRetryableCleanupFailure(t *testing.T) {
	runLockServiceTests(t, []string{"source"}, func(_ *lockTableAllocator, services []*service) {
		source := services[0]
		bind := pb.LockTable{
			Group:       0,
			Table:       27537,
			OriginTable: 27537,
			ServiceID:   "remote-owner",
			Version:     1,
			Valid:       true,
		}
		table := &retryableUnlockTestTable{bind: bind, failFirst: true}
		source.tableGroups.set(bind.Group, bind.Table, table)

		txnID := []byte("retryable-cleanup")
		txn := source.activeTxnHolder.getActiveTxn(txnID, true, "")
		txn.Lock()
		txn.lockTableBindTouched(bind)
		require.NoError(t, txn.lockAdded(
			bind.Group, bind, [][]byte{{1}}, pb.LockOptions{}, source.logger))
		txn.Unlock()
		source.acquireRemoteBindRef(bind)

		err := source.unlockWithContext(context.Background(), txnID, timestamp.Timestamp{})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.True(t, source.activeTxnHolder.hasActiveTxn(txnID))
		require.Equal(t, []pb.LockTable{bind}, source.collectRemoteLockBinds(nil))

		require.NoError(t, source.unlockWithContext(context.Background(), txnID, timestamp.Timestamp{}))
		require.Empty(t, source.collectRemoteLockBinds(nil))
	})
}
