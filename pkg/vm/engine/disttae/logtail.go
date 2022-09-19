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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

func updatePartition(ctx context.Context, op client.TxnOperator, mvcc MVCC, dn DNStore, req api.SyncLogTailReq) error {
	reqs, err := genLogTailReq(dn, req)
	if err != nil {
		return err
	}
	logTails, err := getLogTail(op, reqs)
	if err != nil {
		return err
	}
	for i := range logTails {
		if consumerLogTail(ctx, mvcc, logTails[i]); err != nil {
			return err
		}
	}
	return nil
}

func getLogTail(op client.TxnOperator, reqs []txn.TxnRequest) ([]*api.SyncLogTailResp, error) {
	ctx := context.TODO() // TODO
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

func consumerLogTail(ctx context.Context, mvcc MVCC, logTail *api.SyncLogTailResp) error {
	if err := consumerCheckPoint(logTail.CkpLocation); err != nil {
		return nil
	}
	for i := 0; i < len(logTail.Commands); i++ {
		if err := consumerEntry(ctx, mvcc, logTail.Commands[i]); err != nil {
			return err
		}
	}
	return nil
}

func consumerCheckPoint(ckpt string) error {
	// TODO
	return nil
}

func consumerEntry(ctx context.Context, mvcc MVCC, e *api.Entry) error {
	if e.EntryType == api.Entry_Insert {
		return mvcc.Insert(ctx, e.Bat)
	}
	return mvcc.Delete(ctx, e.Bat)
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
					Address:   dn.ServiceAddress,
				},
			},
			Options: &txn.TxnRequestOptions{
				RetryCodes: []txn.ErrorCode{
					// dn shard not found
					txn.ErrorCode_DNShardNotFound,
				},
				RetryInterval: int64(time.Second),
			},
		}
	}
	return reqs, nil
}
