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

package memoryengine

import (
	"fmt"

	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

type OperationHandler interface {
	HandleOpenDatabase(
		meta txn.TxnMeta,
		req OpenDatabaseReq,
		resp *OpenDatabaseResp,
	) error

	HandleGetDatabases(
		meta txn.TxnMeta,
		req GetDatabasesReq,
		resp *GetDatabasesResp,
	) error

	HandleOpenRelation(
		meta txn.TxnMeta,
		req OpenRelationReq,
		resp *OpenRelationResp,
	) error

	HandleGetRelations(
		meta txn.TxnMeta,
		req GetRelationsReq,
		resp *GetRelationsResp,
	) error

	HandleGetPrimaryKeys(
		meta txn.TxnMeta,
		req GetPrimaryKeysReq,
		resp *GetPrimaryKeysResp,
	) error

	HandleGetTableDefs(
		meta txn.TxnMeta,
		req GetTableDefsReq,
		resp *GetTableDefsResp,
	) error

	HandleGetHiddenKeys(
		meta txn.TxnMeta,
		req GetHiddenKeysReq,
		resp *GetHiddenKeysResp,
	) error

	HandleNewTableIter(
		meta txn.TxnMeta,
		req NewTableIterReq,
		resp *NewTableIterResp,
	) error

	HandleRead(
		meta txn.TxnMeta,
		req ReadReq,
		resp *ReadResp,
	) error

	HandleCloseTableIter(
		meta txn.TxnMeta,
		req CloseTableIterReq,
		resp *CloseTableIterResp,
	) error

	HandleCreateDatabase(
		meta txn.TxnMeta,
		req CreateDatabaseReq,
		resp *CreateDatabaseResp,
	) error

	HandleDeleteDatabase(
		meta txn.TxnMeta,
		req DeleteDatabaseReq,
		resp *DeleteDatabaseResp,
	) error

	HandleCreateRelation(
		meta txn.TxnMeta,
		req CreateRelationReq,
		resp *CreateRelationResp,
	) error

	HandleDeleteRelation(
		meta txn.TxnMeta,
		req DeleteRelationReq,
		resp *DeleteRelationResp,
	) error

	HandleAddTableDef(
		meta txn.TxnMeta,
		req AddTableDefReq,
		resp *AddTableDefResp,
	) error

	HandleDelTableDef(
		meta txn.TxnMeta,
		req DelTableDefReq,
		resp *DelTableDefResp,
	) error

	HandleDelete(
		meta txn.TxnMeta,
		req DeleteReq,
		resp *DeleteResp,
	) error

	HandleTruncate(
		meta txn.TxnMeta,
		req TruncateReq,
		resp *TruncateResp,
	) error

	HandleUpdate(
		meta txn.TxnMeta,
		req UpdateReq,
		resp *UpdateResp,
	) error

	HandleWrite(
		meta txn.TxnMeta,
		req WriteReq,
		resp *WriteResp,
	) error

	HandleTableStats(
		meta txn.TxnMeta,
		req TableStatsReq,
		resp *TableStatsResp,
	) error

	HandleGetLogTail(
		meta txn.TxnMeta,
		req apipb.SyncLogTailReq,
		resp *apipb.SyncLogTailResp,
	) error

	HandlePreCommit(
		meta txn.TxnMeta,
		req apipb.PrecommitWriteCmd,
		resp *apipb.SyncLogTailResp,
	) error
}

type OperationHandlerProvider interface {
	GetOperationHandler() (OperationHandler, txn.TxnMeta)
}

func handle(
	handler OperationHandler,
	meta txn.TxnMeta,
	_ metadata.DNShard,
	op uint32,
	req any,
) (
	ret any,
	err error,
) {

	switch op {

	case OpCreateDatabase:
		var r CreateDatabaseResp
		err = handler.HandleCreateDatabase(meta, req.(CreateDatabaseReq), &r)
		ret = r

	case OpOpenDatabase:
		var r OpenDatabaseResp
		err = handler.HandleOpenDatabase(meta, req.(OpenDatabaseReq), &r)
		ret = r

	case OpGetDatabases:
		var r GetDatabasesResp
		err = handler.HandleGetDatabases(meta, req.(GetDatabasesReq), &r)
		ret = r

	case OpDeleteDatabase:
		var r DeleteDatabaseResp
		err = handler.HandleDeleteDatabase(meta, req.(DeleteDatabaseReq), &r)
		ret = r

	case OpCreateRelation:
		var r CreateRelationResp
		err = handler.HandleCreateRelation(meta, req.(CreateRelationReq), &r)
		ret = r

	case OpDeleteRelation:
		var r DeleteRelationResp
		err = handler.HandleDeleteRelation(meta, req.(DeleteRelationReq), &r)
		ret = r

	case OpOpenRelation:
		var r OpenRelationResp
		err = handler.HandleOpenRelation(meta, req.(OpenRelationReq), &r)
		ret = r

	case OpGetRelations:
		var r GetRelationsResp
		err = handler.HandleGetRelations(meta, req.(GetRelationsReq), &r)
		ret = r

	case OpAddTableDef:
		var r AddTableDefResp
		err = handler.HandleAddTableDef(meta, req.(AddTableDefReq), &r)
		ret = r

	case OpDelTableDef:
		var r DelTableDefResp
		err = handler.HandleDelTableDef(meta, req.(DelTableDefReq), &r)
		ret = r

	case OpDelete:
		var r DeleteResp
		err = handler.HandleDelete(meta, req.(DeleteReq), &r)
		ret = r

	case OpGetPrimaryKeys:
		var r GetPrimaryKeysResp
		err = handler.HandleGetPrimaryKeys(meta, req.(GetPrimaryKeysReq), &r)
		ret = r

	case OpGetTableDefs:
		var r GetTableDefsResp
		err = handler.HandleGetTableDefs(meta, req.(GetTableDefsReq), &r)
		ret = r

	case OpGetHiddenKeys:
		var r GetHiddenKeysResp
		err = handler.HandleGetHiddenKeys(meta, req.(GetHiddenKeysReq), &r)
		ret = r

	case OpTruncate:
		var r TruncateResp
		err = handler.HandleTruncate(meta, req.(TruncateReq), &r)
		ret = r

	case OpUpdate:
		var r UpdateResp
		err = handler.HandleUpdate(meta, req.(UpdateReq), &r)
		ret = r

	case OpWrite, OpPreCommit:
		var r WriteResp
		err = handler.HandleWrite(meta, req.(WriteReq), &r)
		ret = r

	case OpNewTableIter:
		var r NewTableIterResp
		err = handler.HandleNewTableIter(meta, req.(NewTableIterReq), &r)
		ret = r

	case OpRead:
		var r ReadResp
		err = handler.HandleRead(meta, req.(ReadReq), &r)
		ret = r

	case OpCloseTableIter:
		var r CloseTableIterResp
		err = handler.HandleCloseTableIter(meta, req.(CloseTableIterReq), &r)
		ret = r

	case OpTableStats:
		var r TableStatsResp
		err = handler.HandleTableStats(meta, req.(TableStatsReq), &r)
		ret = r

	case OpGetLogTail:
		var r apipb.SyncLogTailResp
		err = handler.HandleGetLogTail(meta, req.(apipb.SyncLogTailReq), &r)
		ret = r

	default:
		panic(fmt.Sprintf("unknown operation %v", op))
	}

	return
}
