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
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

func updatePartition(idx, primaryIdx int, tbl *table, ts timestamp.Timestamp,
	ctx context.Context, op client.TxnOperator, db *DB,
	mvcc MVCC, dn DNStore, req api.SyncLogTailReq) error {
	reqs, err := genLogTailReq(dn, req)
	if err != nil {
		return err
	}
	logTails, err := getLogTail(op, reqs)
	if err != nil {
		return err
	}
	for i := range logTails {
		if consumeLogTail(idx, primaryIdx, tbl, ts, ctx, db, mvcc, logTails[i]); err != nil {
			return err
		}
	}
	return nil
}

func getLogTail(op client.TxnOperator, reqs []txn.TxnRequest) ([]*api.SyncLogTailResp, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
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

func consumeLogTail(idx, primaryIdx int, tbl *table, ts timestamp.Timestamp,
	ctx context.Context, db *DB, mvcc MVCC, logTail *api.SyncLogTailResp) error {
	var entries []*api.Entry
	var err error
	if entries, err = consumerCheckPoint(logTail.CkpLocation, tbl, tbl.db.fs); err != nil {
		return err
	}
	for _, e := range entries {
		if err := consumeEntry(idx, primaryIdx, tbl, ts, ctx,
			db, mvcc, e); err != nil {
			return err
		}
	}
	for i := 0; i < len(logTail.Commands); i++ {
		if err := consumeEntry(idx, primaryIdx, tbl, ts, ctx,
			db, mvcc, logTail.Commands[i]); err != nil {
			return err
		}
	}
	return nil
}

func consumerCheckPoint(ckpt string, tbl *table, fs fileservice.FileService) ([]*api.Entry, error) {
	if ckpt == "" {
		return nil, nil
	}
	ckps := strings.Split(ckpt, ";")
	entries := make([]*api.Entry, 0)
	for _, ckp := range ckps {
		reader, err := blockio.NewCheckpointReader(fs, ckp)
		if err != nil {
			return nil, err
		}
		data := logtail.NewCheckpointData()
		defer data.Close()
		if err = data.ReadFrom(reader, common.DefaultAllocator); err != nil {
			return nil, err
		}
		ins, del, cnIns, err := data.GetTableData(tbl.tableId)
		if err != nil {
			return nil, err
		}
		tblName := tbl.tableName
		if tblName != catalog.MO_DATABASE && tblName != catalog.MO_COLUMNS && tblName != catalog.MO_TABLES {
			tblName = fmt.Sprintf("_%d_meta", tbl.tableId)
		}
		if ins != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Insert,
				TableId:      tbl.tableId,
				TableName:    tblName,
				DatabaseId:   tbl.db.databaseId,
				DatabaseName: tbl.db.databaseName,
				Bat:          ins,
			}
			entries = append(entries, entry)
		}
		if cnIns != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Insert,
				TableId:      tbl.tableId,
				TableName:    tblName,
				DatabaseId:   tbl.db.databaseId,
				DatabaseName: tbl.db.databaseName,
				Bat:          cnIns,
			}
			entries = append(entries, entry)
		}
		if del != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Delete,
				TableId:      tbl.tableId,
				TableName:    tblName,
				DatabaseId:   tbl.db.databaseId,
				DatabaseName: tbl.db.databaseName,
				Bat:          del,
			}
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func consumeEntry(idx, primaryIdx int, tbl *table, ts timestamp.Timestamp,
	ctx context.Context, db *DB, mvcc MVCC, e *api.Entry) error {
	if e.EntryType == api.Entry_Insert {
		if isMetaTable(e.TableName) {
			vec, err := vector.ProtoVectorToVector(e.Bat.Vecs[catalog.BLOCKMETA_ID_IDX+MO_PRIMARY_OFF])
			if err != nil {
				return err
			}
			timeVec, err := vector.ProtoVectorToVector(e.Bat.Vecs[catalog.BLOCKMETA_COMMITTS_IDX+MO_PRIMARY_OFF])
			if err != nil {
				return err
			}
			vs := vector.MustTCols[uint64](vec)
			timestamps := vector.MustTCols[types.TS](timeVec)
			for i, v := range vs {
				if err := tbl.parts[idx].DeleteByBlockID(ctx, timestamps[i].ToTimestamp(), v); err != nil {
					ve := e.Bat.Vecs[1]
					timeVec, err := vector.ProtoVectorToVector(ve)
					if err != nil {
						return err
					}
					timestamps := vector.MustTCols[types.TS](timeVec)
					if err := tbl.parts[idx].DeleteByBlockID(ctx, timestamps[i].ToTimestamp(), v); err != nil {
						return err
					}
				}
			}
			return db.getMetaPartitions(e.TableName)[idx].Insert(ctx, -1, e.Bat, false)
		}
		if primaryIdx >= 0 {
			return mvcc.Insert(ctx, MO_PRIMARY_OFF+primaryIdx, e.Bat, false)
		}
		return mvcc.Insert(ctx, primaryIdx, e.Bat, false)
	}
	if isMetaTable(e.TableName) {
		return db.getMetaPartitions(e.TableName)[idx].Delete(ctx, e.Bat)
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
