// Copyright 2021 - 2022 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

///
///
/// impls TxnStorage.
///
/// Theses functions are not used at the moment, move them here to reduce distraction

func (h *Handle) HandleCommitting(
	ctx context.Context,
	meta txn.TxnMeta) (err error) {
	txn, err := h.db.GetTxnByID(meta.GetID())
	if err != nil {
		return err
	}
	txn.SetCommitTS(types.TimestampToTS(meta.GetCommitTS()))
	err = txn.Committing()
	return
}

func (h *Handle) HandlePrepare(
	ctx context.Context,
	meta txn.TxnMeta) (pts timestamp.Timestamp, err error) {
	var txn txnif.AsyncTxn

	//handle pre-commit write for 2PC
	txn, err = h.db.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return
	}
	h.handleRequests(ctx, txn, nil, nil, meta)

	txn, err = h.db.GetTxnByID(meta.GetID())
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	participants := make([]uint64, 0, len(meta.GetTNShards()))
	for _, shard := range meta.GetTNShards() {
		participants = append(participants, shard.GetShardID())
	}
	txn.SetParticipants(participants)
	var ts types.TS
	ts, err = txn.Prepare(ctx)
	pts = ts.ToTimestamp()
	return
}

func (h *Handle) HandleStartRecovery(
	ctx context.Context,
	ch chan txn.TxnMeta) {
	defer close(ch)

	for _, recovering := range h.db.TxnMgr.SnapshotRecoveringTxns() {
		status := txn.TxnStatus_Prepared
		if recovering.State == txnif.TxnStateCommittingFinished {
			status = txn.TxnStatus_Committing
		}

		shards := make([]metadata.TNShard, 0, len(recovering.Participants))
		for _, shardID := range recovering.Participants {
			if h.db.Opts != nil && h.db.Opts.Shard.GetShardID() == shardID {
				shards = append(shards, h.db.Opts.Shard)
				continue
			}
			shards = append(shards, metadata.TNShard{
				TNShardRecord: metadata.TNShardRecord{ShardID: shardID},
			})
		}

		meta := txn.TxnMeta{
			ID:         recovering.ID,
			Status:     status,
			SnapshotTS: recovering.SnapshotTS.ToTimestamp(),
			PreparedTS: recovering.PreparedTS.ToTimestamp(),
			CommitTS:   recovering.CommitTS.ToTimestamp(),
			TNShards:   shards,
		}
		select {
		case <-ctx.Done():
			return
		case ch <- meta:
		}
	}
}
