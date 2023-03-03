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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"time"
)

// updatePartitionOfPull the old method of log tail pull model.
func updatePartitionOfPull(
	primaryIdx int,
	tbl *txnTable,
	ctx context.Context,
	op client.TxnOperator,
	engine *Engine,
	partition *Partition,
	partitions Partitions,
	dn DNStore,
	req api.SyncLogTailReq,
) error {
	reqs, err := genLogTailReq(dn, req)
	if err != nil {
		return err
	}
	logTails, err := getLogTail(ctx, op, reqs)
	if err != nil {
		return err
	}

	state, doneMutate := partition.MutateState()

	for i := range logTails {
		if err := consumeLogTailOfPull(primaryIdx, tbl, ctx, engine, partition, partitions, state, logTails[i]); err != nil {
			logutil.Errorf("consume %d-%s logtail error: %v\n", tbl.tableId, tbl.tableName, err)
			return err
		}
	}

	doneMutate()

	return nil
}

func getLogTail(ctx context.Context, op client.TxnOperator, reqs []txn.TxnRequest) ([]*api.SyncLogTailResp, error) {
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
	primaryIdx int,
	tbl *txnTable,
	ctx context.Context,
	engine *Engine,
	partition *Partition,
	partitions Partitions,
	state *PartitionState,
	logTail *api.SyncLogTailResp,
) (err error) {
	var entries []*api.Entry

	if entries, err = logtail.LoadCheckpointEntries(
		ctx,
		logTail.CkpLocation,
		tbl.tableId,
		tbl.tableName,
		tbl.db.databaseId,
		tbl.db.databaseName,
		engine.fs); err != nil {
		return
	}
	for _, e := range entries {
		if err = consumeEntry(ctx, primaryIdx,
			engine, partition, state, e); err != nil {
			return
		}
	}

	for i := 0; i < len(logTail.Commands); i++ {
		if err = consumeEntry(ctx, primaryIdx,
			engine, partition, state, logTail.Commands[i]); err != nil {
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

func genLogTailReq(dn DNStore, req api.SyncLogTailReq) ([]txn.TxnRequest, error) {
	payload, err := types.Encode(req)
	if err != nil {
		return nil, err
	}
	reqs := make([]txn.TxnRequest, len(dn.Shards))
	for i, info := range dn.Shards {
		reqs[i] = txn.TxnRequest{
			CNRequest: &txn.CNOpRequest{
				OpCode:  uint32(api.OpCode_OpGetLogTail),
				Payload: payload,
				Target: metadata.DNShard{
					DNShardRecord: metadata.DNShardRecord{
						ShardID: info.ShardID,
					},
					ReplicaID: info.ReplicaID,
					Address:   dn.GetTxnServiceAddress(),
				},
			},
			Options: &txn.TxnRequestOptions{
				RetryCodes: []int32{
					// dn shard not found
					int32(moerr.ErrDNShardNotFound),
				},
				RetryInterval: int64(time.Second),
			},
		}
	}
	return reqs, nil
}
