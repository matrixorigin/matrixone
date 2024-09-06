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

	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	txnCtx, ok := h.txnCtxs.Load(util.UnsafeBytesToString(meta.GetID()))
	var txn txnif.AsyncTxn
	defer func() {
		if ok {
			//delete the txn's context.
			h.txnCtxs.Delete(util.UnsafeBytesToString(meta.GetID()))
		}
	}()
	if ok {
		//handle pre-commit write for 2PC
		txn, err = h.db.GetOrCreateTxnWithMeta(nil, meta.GetID(),
			types.TimestampToTS(meta.GetSnapshotTS()))
		if err != nil {
			return
		}
		h.handleRequests(ctx, txn, txnCtx)
		panic("NYI")
	}
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
	//panic(moerr.NewNYI("HandleStartRecovery is not implemented yet"))
	//TODO:: 1.  Get the 2PC transactions which be in prepared or
	//           committing state from txn engine's recovery.
	//       2.  Feed these transaction into ch.
	close(ch)
}
