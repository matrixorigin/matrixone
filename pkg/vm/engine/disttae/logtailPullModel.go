// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

// updatePartitionOfPull the old method of log tail pull model.
func updatePartitionOfPull(
	primarySeqnum int,
	tbl *txnTable,
	ctx context.Context,
	op client.TxnOperator,
	engine *Engine,
	partition *logtailreplay.Partition,
	tn DNStore,
	req api.SyncLogTailReq,
) error {
	logDebugf(op.Txn(), "updatePartitionOfPull")
	reqs, err := genLogTailReq(tn, req)
	if err != nil {
		return err
	}
	logTails, err := getLogTail(ctx, op, reqs)
	if err != nil {
		return err
	}

	state, doneMutate := partition.MutateState()

	for i := range logTails {
		if err := consumeLogTailOfPull(primarySeqnum, tbl, ctx, engine, state, logTails[i]); err != nil {
			logutil.Errorf("consume %d-%s logtail error: %v\n", tbl.tableId, tbl.tableName, err)
			return err
		}
	}

	doneMutate()

	return nil
}

func getLogTail(ctx context.Context, op client.TxnOperator, reqs []txn.TxnRequest) ([]*api.SyncLogTailResp, error) {
	logDebugf(op.Txn(), "getLogTail")
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	result, err := op.Read(ctx, reqs)
	if err != nil {
		return nil, err
	}
	logTails := make([]*api.SyncLogTailResp, len(result.Responses))
	for i, resp := range result.Responses {
		logTails[i] = new(api.SyncLogTailResp)
		if err := types.Decode(resp.CNOpResponse.Payload, logTails[i]); err != nil {
			return nil, err
		}
	}
	return logTails, nil
}

func consumeLogTailOfPull(
	primarySeqnum int,
	tbl *txnTable,
	ctx context.Context,
	engine *Engine,
	state *logtailreplay.PartitionState,
	logTail *api.SyncLogTailResp,
) (err error) {
	logutil.Debugf("consumeLogTailOfPull table %d %s", tbl.tableId, tbl.tableName)
	var entries []*api.Entry
	var closeCBs []func()

	if entries, closeCBs, err = logtail.LoadCheckpointEntries(
		ctx,
		logTail.CkpLocation,
		tbl.tableId,
		tbl.tableName,
		tbl.db.databaseId,
		tbl.db.databaseName,
		engine.mp,
		engine.fs); err != nil {
		return
	}

	defer func() {
		for _, cb := range closeCBs {
			cb()
		}
	}()

	for _, e := range entries {
		if err = consumeEntry(ctx, primarySeqnum,
			engine, state, e); err != nil {
			return
		}
	}

	for i := 0; i < len(logTail.Commands); i++ {
		if err = consumeEntry(ctx, primarySeqnum,
			engine, state, logTail.Commands[i]); err != nil {
			return
		}
	}
	return nil
}

func genSyncLogTailReq(have, want timestamp.Timestamp, databaseId,
	tableId uint64) api.SyncLogTailReq {
	return api.SyncLogTailReq{
		CnHave: &have,
		CnWant: &want,
		Table: &api.TableID{
			DbId: databaseId,
			TbId: tableId,
		},
	}
}

func genLogTailReq(tn DNStore, req api.SyncLogTailReq) ([]txn.TxnRequest, error) {
	payload, err := types.Encode(&req)
	if err != nil {
		return nil, err
	}
	reqs := make([]txn.TxnRequest, len(tn.Shards))
	for i, info := range tn.Shards {
		reqs[i] = txn.TxnRequest{
			CNRequest: &txn.CNOpRequest{
				OpCode:  uint32(api.OpCode_OpGetLogTail),
				Payload: payload,
				Target: metadata.TNShard{
					TNShardRecord: metadata.TNShardRecord{
						ShardID: info.ShardID,
					},
					ReplicaID: info.ReplicaID,
					Address:   tn.GetTxnServiceAddress(),
				},
			},
			Options: &txn.TxnRequestOptions{
				RetryCodes: []int32{
					// tn shard not found
					int32(moerr.ErrTNShardNotFound),
				},
				RetryInterval: int64(time.Second),
			},
		}
	}
	return reqs, nil
}
