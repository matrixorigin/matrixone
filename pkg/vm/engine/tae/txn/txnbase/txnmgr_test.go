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

package txnbase

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/stretchr/testify/require"
)

func TestTxnWaiterCancelAndReuse(t *testing.T) {
	var waiter txnWaiter
	waiter.Add()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, waiter.Wait(ctx), context.Canceled)

	firstWait := make(chan error, 1)
	go func() {
		firstWait <- waiter.Wait(context.Background())
	}()
	waiter.Done()
	require.NoError(t, <-firstWait)

	// A canceled wait must not leave a sync.WaitGroup-style waiter that makes
	// the next transaction generation unsafe to start.
	waiter.Add()
	secondWait := make(chan error, 1)
	go func() {
		secondWait <- waiter.Wait(context.Background())
	}()
	select {
	case <-secondWait:
		t.Fatal("new transaction generation reported empty before completion")
	case <-time.After(20 * time.Millisecond):
	}
	waiter.Done()
	require.NoError(t, <-secondWait)
}

func TestLoadOrStoreTxnBalancesWaiterOnHit(t *testing.T) {
	mgr := &TxnManager{}
	mgr.txns.store = new(sync.Map)
	startTS := types.BuildTS(1, 0)
	id := []byte("same-txn")

	first := NewTxn(mgr, new(NoopTxnStore), id, startTS, types.TS{})
	stored, loaded, offline := mgr.loadOrStoreTxn(first, TxnFlag_Normal)
	require.False(t, loaded)
	require.False(t, offline)
	require.Same(t, first, stored)

	duplicate := NewTxn(mgr, new(NoopTxnStore), id, startTS, types.TS{})
	stored, loaded, offline = mgr.loadOrStoreTxn(duplicate, TxnFlag_Normal)
	require.True(t, loaded)
	require.False(t, offline)
	require.Same(t, first, stored)

	_, ok := mgr.loadAndDeleteTxn(first.GetID())
	require.True(t, ok)
	require.NoError(t, mgr.WaitEmpty(context.Background()))
}

func TestLoadOrStoreTxnRejectsNewTxnInReadonlyMode(t *testing.T) {
	newManager := func() *TxnManager {
		mgr := &TxnManager{}
		mgr.txns.store = new(sync.Map)
		return mgr
	}
	newTxn := func(mgr *TxnManager, id []byte) *Txn {
		return NewTxn(mgr, new(NoopTxnStore), id, types.BuildTS(1, 0), types.TS{})
	}

	t.Run("new ID is not managed", func(t *testing.T) {
		mgr := newManager()
		WithTxnSkipFlag(TxnFlag_Normal)(mgr)
		id := []byte("readonly-new-txn")

		txn, loaded, offline := mgr.loadOrStoreTxn(newTxn(mgr, id), TxnFlag_Normal)
		require.False(t, loaded)
		require.True(t, offline)
		require.Equal(t, id, []byte(txn.GetID()))
		_, ok := mgr.loadTxn(txn.GetID())
		require.False(t, ok)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		require.NoError(t, mgr.WaitEmpty(ctx))
	})

	t.Run("managed ID is reused", func(t *testing.T) {
		mgr := newManager()
		id := []byte("readonly-managed-txn")
		managed := newTxn(mgr, id)
		stored, loaded, offline := mgr.loadOrStoreTxn(managed, TxnFlag_Normal)
		require.False(t, loaded)
		require.False(t, offline)
		require.Same(t, managed, stored)

		WithTxnSkipFlag(TxnFlag_Normal)(mgr)
		stored, loaded, offline = mgr.loadOrStoreTxn(newTxn(mgr, id), TxnFlag_Normal)
		require.True(t, loaded)
		require.False(t, offline)
		require.Same(t, managed, stored)

		_, ok := mgr.loadAndDeleteTxn(managed.GetID())
		require.True(t, ok)
		require.NoError(t, mgr.WaitEmpty(context.Background()))
	})
}

func TestTryUpdateMaxCommittedTSNeverMovesBackward(t *testing.T) {
	mgr := &TxnManager{}
	mgr.initMaxCommittedTS()

	newer := types.BuildTS(2, 0)
	older := types.BuildTS(1, 0)
	mgr.TryUpdateMaxCommittedTS(newer)
	mgr.TryUpdateMaxCommittedTS(older)

	require.Equal(t, newer, *mgr.MaxCommittedTS.Load())
}

func TestTryUpdateMaxCommittedTSConcurrent(t *testing.T) {
	mgr := &TxnManager{}
	mgr.initMaxCommittedTS()

	const updates = 100
	var wg sync.WaitGroup
	for i := 1; i <= updates; i++ {
		ts := types.BuildTS(int64(i), 0)
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.TryUpdateMaxCommittedTS(ts)
		}()
	}
	wg.Wait()

	require.Equal(t, types.BuildTS(updates, 0), *mgr.MaxCommittedTS.Load())
}

func TestAllocateAndPublishCommitTSSerializesPublication(t *testing.T) {
	mgr := NewTxnManager(nil, nil, types.NewMockHLCClock(1))
	defer mgr.workers.Release()

	firstStarted := make(chan types.TS, 1)
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := mgr.AllocateAndPublishCommitTS(func(ts types.TS) error {
			firstStarted <- ts
			<-releaseFirst
			return nil
		})
		firstDone <- err
	}()

	firstTS := <-firstStarted
	require.True(t, mgr.MaxCommittedTS.Load().LT(&firstTS))

	secondStarted := make(chan types.TS, 1)
	secondDone := make(chan error, 1)
	go func() {
		_, err := mgr.AllocateAndPublishCommitTS(func(ts types.TS) error {
			secondStarted <- ts
			return nil
		})
		secondDone <- err
	}()

	select {
	case <-secondStarted:
		t.Fatal("later timestamp allocated before earlier state was published")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseFirst)
	require.NoError(t, <-firstDone)
	secondTS := <-secondStarted
	require.True(t, secondTS.GT(&firstTS))
	require.NoError(t, <-secondDone)
	require.Equal(t, secondTS, *mgr.MaxCommittedTS.Load())
}

func TestAllocateAndPublishCommitTSErrorDoesNotPublish(t *testing.T) {
	mgr := NewTxnManager(nil, nil, types.NewMockHLCClock(1))
	defer mgr.workers.Release()

	publishErr := errors.New("publish failed")
	ts, err := mgr.AllocateAndPublishCommitTS(func(types.TS) error {
		return publishErr
	})
	require.ErrorIs(t, err, publishErr)
	require.True(t, mgr.MaxCommittedTS.Load().LT(&ts))
}

func newTxnManagerForLifecycleTest() *TxnManager {
	mgr := &TxnManager{}
	mgr.txns.store = new(sync.Map)
	return mgr
}

func newTxnForLifecycleTest(mgr *TxnManager, id string) *Txn {
	return NewTxn(mgr, &NoopTxnStore{}, []byte(id), types.TS{}, types.TS{})
}

func waitTxnManagerEmpty(t *testing.T, mgr *TxnManager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, mgr.WaitEmpty(ctx))
}

func TestLoadOrStoreTxnBalancesLifecycleWaiterWhenLoaded(t *testing.T) {
	mgr := newTxnManagerForLifecycleTest()
	first, loaded, offline := mgr.loadOrStoreTxn(
		newTxnForLifecycleTest(mgr, "txn"), TxnFlag_Normal)
	require.False(t, loaded)
	require.False(t, offline)

	actual, loaded, offline := mgr.loadOrStoreTxn(
		newTxnForLifecycleTest(mgr, "txn"), TxnFlag_Normal)
	require.True(t, loaded)
	require.False(t, offline)
	require.Same(t, first, actual)

	require.NoError(t, mgr.DeleteTxn("txn"))
	waitTxnManagerEmpty(t, mgr)
}

func TestLoadOrStoreTxnSkipsTrackingInReadonlyMode(t *testing.T) {
	mgr := newTxnManagerForLifecycleTest()
	mgr.txns.skipFlags.Store(uint64(TxnFlag_Normal))
	txn := newTxnForLifecycleTest(mgr, "offline")

	actual, loaded, offline := mgr.loadOrStoreTxn(txn, TxnFlag_Normal)
	require.False(t, loaded)
	require.True(t, offline)
	require.Same(t, txn, actual)
	_, exists := mgr.txns.store.Load("offline")
	require.False(t, exists)
	waitTxnManagerEmpty(t, mgr)
}

func TestSingleTNCommitAndRollbackLifecycle(t *testing.T) {
	newManager := func(t *testing.T) *TxnManager {
		t.Helper()
		mgr := NewTxnManager(NoopStoreFactory, nil, types.NewMockHLCClock(1))
		mgr.Start(context.Background())
		t.Cleanup(mgr.Stop)
		return mgr
	}

	t.Run("commit", func(t *testing.T) {
		mgr := newManager(t)
		txn, err := mgr.StartTxn(nil)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(context.Background()))
		require.Equal(t, txnif.TxnStateCommitted, txn.GetTxnState(false))
		waitTxnManagerEmpty(t, mgr)
	})

	t.Run("rollback", func(t *testing.T) {
		mgr := newManager(t)
		txn, err := mgr.StartTxn(nil)
		require.NoError(t, err)
		require.NoError(t, txn.Rollback(context.Background()))
		require.Equal(t, txnif.TxnStateRollbacked, txn.GetTxnState(false))
		waitTxnManagerEmpty(t, mgr)
	})
}

func TestSingleTNInvalidStateAndReplayRollback(t *testing.T) {
	ctx := NewEmptyTxnCtx()
	require.Error(t, ctx.ToCommittedLocked())

	replayTxn := NewPersistedTxn(
		newTxnManagerForLifecycleTest(),
		NewTxnCtx([]byte("replay"), types.TS{}, types.TS{}),
		&NoopTxnStore{},
		1,
		nil,
		nil,
		nil,
		nil,
	)
	require.Panics(t, func() {
		_ = replayTxn.rollback1PC(context.Background())
	})
}
