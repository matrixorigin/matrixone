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

package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

func TestHandleStartRecovery(t *testing.T) {
	local := metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 7, LogShardID: 8},
		ReplicaID:     9,
		Address:       "tn-local",
	}
	mgr := txnbase.NewTxnManager(txnbase.NoopStoreFactory, nil, clock.NewHLCClock(time.Now().UnixNano, 0))
	h := &Handle{db: &db.DB{TxnMgr: mgr, Opts: &options.Options{Shard: local}}}

	prepared := addRPCRecoveryTxn(t, mgr, "prepared", []uint64{7, 11}, txnif.TxnStatePrepared)
	addRPCRecoveryTxn(t, mgr, "committing", []uint64{11, 7}, txnif.TxnStateCommittingFinished)

	ch := make(chan txn.TxnMeta)
	go h.HandleStartRecovery(context.Background(), ch)

	got := make(map[string]txn.TxnMeta)
	for meta := range ch {
		got[string(meta.ID)] = meta
	}
	require.Len(t, got, 2)

	preparedMeta := got["prepared"]
	require.Equal(t, txn.TxnStatus_Prepared, preparedMeta.Status)
	require.Equal(t, prepared.GetSnapshotTS().ToTimestamp(), preparedMeta.SnapshotTS)
	require.Equal(t, prepared.GetPrepareTS().ToTimestamp(), preparedMeta.PreparedTS)
	require.Equal(t, prepared.GetCommitTS().ToTimestamp(), preparedMeta.CommitTS)
	require.Equal(t, []metadata.TNShard{
		local,
		{TNShardRecord: metadata.TNShardRecord{ShardID: 11}},
	}, preparedMeta.TNShards)

	committingMeta := got["committing"]
	require.Equal(t, txn.TxnStatus_Committing, committingMeta.Status)
	require.Equal(t, []metadata.TNShard{
		{TNShardRecord: metadata.TNShardRecord{ShardID: 11}},
		local,
	}, committingMeta.TNShards)
}

func TestHandleStartRecoveryCancellationClosesChannel(t *testing.T) {
	mgr := txnbase.NewTxnManager(txnbase.NoopStoreFactory, nil, clock.NewHLCClock(time.Now().UnixNano, 0))
	h := &Handle{db: &db.DB{TxnMgr: mgr, Opts: &options.Options{}}}
	addRPCRecoveryTxn(t, mgr, "prepared", []uint64{1, 2}, txnif.TxnStatePrepared)

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan txn.TxnMeta)
	done := make(chan struct{})
	go func() {
		h.HandleStartRecovery(ctx, ch)
		close(done)
	}()

	select {
	case <-done:
		require.Fail(t, "recovery returned while its channel send was blocked")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "recovery did not observe context cancellation")
	}

	_, ok := <-ch
	require.False(t, ok)
}

func addRPCRecoveryTxn(
	t *testing.T,
	mgr *txnbase.TxnManager,
	id string,
	participants []uint64,
	state txnif.TxnState,
) txnif.AsyncTxn {
	ctx := txnbase.NewTxnCtx([]byte(id), types.BuildTS(1, 0), types.BuildTS(2, 0))
	ctx.Participants = participants
	ctx.PrepareTS = types.BuildTS(3, 0)
	ctx.CommitTS = types.BuildTS(4, 0)
	ctx.State = state
	replayed := txnbase.NewPersistedTxn(mgr, ctx, new(txnbase.NoopTxnStore), 1, nil, nil, nil, nil)
	require.NoError(t, mgr.OnReplayTxn(replayed))
	return replayed
}
