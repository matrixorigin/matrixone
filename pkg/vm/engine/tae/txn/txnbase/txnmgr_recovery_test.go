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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func TestTxnCtxSnapshotRecoveryIsAtomic(t *testing.T) {
	ctx := NewTxnCtx([]byte("atomic"), types.BuildTS(1, 0), types.BuildTS(2, 0))
	ctx.State = txnif.TxnStatePrepared
	ctx.PrepareTS = types.BuildTS(3, 0)
	ctx.CommitTS = types.BuildTS(4, 0)
	ctx.Participants = []uint64{1, 2}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			require.NoError(t, ctx.SetCommitTS(types.BuildTS(int64(4+i), 0)))
			require.NoError(t, ctx.SetParticipants([]uint64{uint64(i), uint64(i + 1)}))
		}
	}()
	for i := 0; i < 1000; i++ {
		snapshot, ok := ctx.SnapshotRecovery()
		require.True(t, ok)
		require.Equal(t, txnif.TxnStatePrepared, snapshot.State)
		require.Len(t, snapshot.Participants, 2)
	}
	wg.Wait()

	ctx.Lock()
	ctx.State = txnif.TxnStateCommitted
	ctx.Unlock()
	_, ok := ctx.SnapshotRecovery()
	require.False(t, ok)
}

type transitioningRecoveryTxn struct {
	txnif.AsyncTxn
	stateReads atomic.Int32
}

func (txn *transitioningRecoveryTxn) GetTxnState(bool) txnif.TxnState {
	if txn.stateReads.Add(1) == 1 {
		return txnif.TxnStatePrepared
	}
	return txnif.TxnStateCommittingFinished
}

func TestSnapshotRecoveringTxns(t *testing.T) {
	mgr := NewTxnManager(NoopStoreFactory, nil, clock.NewHLCClock(time.Now().UnixNano, 0))

	prepared := makeRecoveryTestTxn(t, mgr, "prepared", true, []uint64{1, 2}, txnif.TxnStatePrepared)
	committing := makeRecoveryTestTxn(t, mgr, "committing", true, []uint64{1, 2}, txnif.TxnStateCommittingFinished)
	makeRecoveryTestTxn(t, mgr, "normal", false, []uint64{1, 2}, txnif.TxnStatePrepared)
	makeRecoveryTestTxn(t, mgr, "one-phase", true, []uint64{1}, txnif.TxnStatePrepared)
	makeRecoveryTestTxn(t, mgr, "active", true, []uint64{1, 2}, txnif.TxnStateActive)
	makeRecoveryTestTxn(t, mgr, "committed", true, []uint64{1, 2}, txnif.TxnStateCommitted)

	got := mgr.SnapshotRecoveringTxns()
	require.ElementsMatch(t, []RecoveringTxn{
		{
			ID:           []byte("prepared"),
			SnapshotTS:   prepared.GetSnapshotTS(),
			PreparedTS:   prepared.GetPrepareTS(),
			CommitTS:     prepared.GetCommitTS(),
			State:        txnif.TxnStatePrepared,
			Participants: []uint64{1, 2},
		},
		{
			ID:           []byte("committing"),
			SnapshotTS:   committing.GetSnapshotTS(),
			PreparedTS:   committing.GetPrepareTS(),
			CommitTS:     committing.GetCommitTS(),
			State:        txnif.TxnStateCommittingFinished,
			Participants: []uint64{1, 2},
		},
	}, got)
	byID := make(map[string]RecoveringTxn, len(got))
	for _, snapshot := range got {
		byID[string(snapshot.ID)] = snapshot
	}

	prepared.GetCtx()[0] = 'x'
	prepared.GetParticipants()[0] = 99
	require.Equal(t, []byte("prepared"), byID["prepared"].ID)
	require.Equal(t, []uint64{1, 2}, byID["prepared"].Participants)
}

func TestSnapshotRecoveringTxnsRetriesRecoverableStateTransition(t *testing.T) {
	mgr := NewTxnManager(NoopStoreFactory, nil, clock.NewHLCClock(time.Now().UnixNano, 0))
	base := makeRecoveryTestTxn(t, mgr, "transitioning", true, []uint64{1, 2}, txnif.TxnStatePrepared)
	require.NoError(t, mgr.DeleteTxn(base.GetID()))
	transitioning := &transitioningRecoveryTxn{AsyncTxn: base}
	require.NoError(t, mgr.OnReplayTxn(transitioning))

	got := mgr.SnapshotRecoveringTxns()
	require.Equal(t, []RecoveringTxn{
		{
			ID:           []byte("transitioning"),
			SnapshotTS:   base.GetSnapshotTS(),
			PreparedTS:   base.GetPrepareTS(),
			CommitTS:     base.GetCommitTS(),
			State:        txnif.TxnStateCommittingFinished,
			Participants: []uint64{1, 2},
		},
	}, got)
}

func makeRecoveryTestTxn(
	t *testing.T,
	mgr *TxnManager,
	id string,
	replay bool,
	participants []uint64,
	state txnif.TxnState,
) txnif.AsyncTxn {
	ctx := NewTxnCtx([]byte(id), types.BuildTS(1, 0), types.BuildTS(2, 0))
	ctx.Participants = participants
	ctx.PrepareTS = types.BuildTS(3, 0)
	ctx.CommitTS = types.BuildTS(4, 0)
	ctx.State = state

	var txn txnif.AsyncTxn
	if replay {
		txn = NewPersistedTxn(mgr, ctx, new(NoopTxnStore), 1, nil, nil, nil, nil)
	} else {
		txn = NewTxn(mgr, new(NoopTxnStore), []byte(id), ctx.StartTS, ctx.SnapshotTS)
		normal := txn.(*Txn)
		normal.Participants = participants
		normal.PrepareTS = ctx.PrepareTS
		normal.CommitTS = ctx.CommitTS
		normal.State = state
	}
	require.NoError(t, mgr.OnReplayTxn(txn))
	return txn
}
