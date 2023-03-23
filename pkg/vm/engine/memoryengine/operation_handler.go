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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

type OperationHandler interface {
	HandleOpenDatabase(
		ctx context.Context,
		meta txn.TxnMeta,
		req *OpenDatabaseReq,
		resp *OpenDatabaseResp,
	) error

	HandleGetDatabases(
		ctx context.Context,
		meta txn.TxnMeta,
		req *GetDatabasesReq,
		resp *GetDatabasesResp,
	) error

	HandleOpenRelation(
		ctx context.Context,
		meta txn.TxnMeta,
		req *OpenRelationReq,
		resp *OpenRelationResp,
	) error

	HandleGetRelations(
		ctx context.Context,
		meta txn.TxnMeta,
		req *GetRelationsReq,
		resp *GetRelationsResp,
	) error

	HandleGetPrimaryKeys(
		ctx context.Context,
		meta txn.TxnMeta,
		req *GetPrimaryKeysReq,
		resp *GetPrimaryKeysResp,
	) error

	HandleGetTableColumns(
		ctx context.Context,
		meta txn.TxnMeta,
		req *GetTableColumnsReq,
		resp *GetTableColumnsResp,
	) error

	HandleGetTableDefs(
		ctx context.Context,
		meta txn.TxnMeta,
		req *GetTableDefsReq,
		resp *GetTableDefsResp,
	) error

	HandleGetHiddenKeys(
		ctx context.Context,
		meta txn.TxnMeta,
		req *GetHiddenKeysReq,
		resp *GetHiddenKeysResp,
	) error

	HandleNewTableIter(
		ctx context.Context,
		meta txn.TxnMeta,
		req *NewTableIterReq,
		resp *NewTableIterResp,
	) error

	HandleRead(
		ctx context.Context,
		meta txn.TxnMeta,
		req *ReadReq,
		resp *ReadResp,
	) error

	HandleCloseTableIter(
		ctx context.Context,
		meta txn.TxnMeta,
		req *CloseTableIterReq,
		resp *CloseTableIterResp,
	) error

	HandleCreateDatabase(
		ctx context.Context,
		meta txn.TxnMeta,
		req *CreateDatabaseReq,
		resp *CreateDatabaseResp,
	) error

	HandleDeleteDatabase(
		ctx context.Context,
		meta txn.TxnMeta,
		req *DeleteDatabaseReq,
		resp *DeleteDatabaseResp,
	) error

	HandleCreateRelation(
		ctx context.Context,
		meta txn.TxnMeta,
		req *CreateRelationReq,
		resp *CreateRelationResp,
	) error

	HandleDeleteRelation(
		ctx context.Context,
		meta txn.TxnMeta,
		req *DeleteRelationReq,
		resp *DeleteRelationResp,
	) error

	HandleTruncateRelation(
		ctx context.Context,
		meta txn.TxnMeta,
		req *TruncateRelationReq,
		resp *TruncateRelationResp,
	) error

	HandleAddTableDef(
		ctx context.Context,
		meta txn.TxnMeta,
		req *AddTableDefReq,
		resp *AddTableDefResp,
	) error

	HandleDelTableDef(
		ctx context.Context,
		meta txn.TxnMeta,
		req *DelTableDefReq,
		resp *DelTableDefResp,
	) error

	HandleDelete(
		ctx context.Context,
		meta txn.TxnMeta,
		req *DeleteReq,
		resp *DeleteResp,
	) error

	HandleUpdate(
		ctx context.Context,
		meta txn.TxnMeta,
		req *UpdateReq,
		resp *UpdateResp,
	) error

	HandleWrite(
		ctx context.Context,
		meta txn.TxnMeta,
		req *WriteReq,
		resp *WriteResp,
	) error

	HandleTableStats(
		ctx context.Context,
		meta txn.TxnMeta,
		req *TableStatsReq,
		resp *TableStatsResp,
	) error
}

type OperationHandlerProvider interface {
	GetOperationHandler(shard Shard) (OperationHandler, txn.TxnMeta)
}

func handle(
	ctx context.Context,
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
		request := req.(CreateDatabaseReq)
		err = handler.HandleCreateDatabase(ctx, meta, &request, &r)
		ret = r

	case OpOpenDatabase:
		var r OpenDatabaseResp
		request := req.(OpenDatabaseReq)
		err = handler.HandleOpenDatabase(ctx, meta, &request, &r)
		ret = r

	case OpGetDatabases:
		var r GetDatabasesResp
		request := req.(GetDatabasesReq)
		err = handler.HandleGetDatabases(ctx, meta, &request, &r)
		ret = r

	case OpDeleteDatabase:
		var r DeleteDatabaseResp
		request := req.(DeleteDatabaseReq)
		err = handler.HandleDeleteDatabase(ctx, meta, &request, &r)
		ret = r

	case OpCreateRelation:
		var r CreateRelationResp
		request := req.(CreateRelationReq)
		err = handler.HandleCreateRelation(ctx, meta, &request, &r)
		ret = r

	case OpDeleteRelation:
		var r DeleteRelationResp
		request := req.(DeleteRelationReq)
		err = handler.HandleDeleteRelation(ctx, meta, &request, &r)
		ret = r

	case OpTruncateRelation:
		var r TruncateRelationResp
		request := req.(TruncateRelationReq)
		handler.HandleTruncateRelation(ctx, meta, &request, &r)
		ret = r

	case OpOpenRelation:
		var r OpenRelationResp
		request := req.(OpenRelationReq)
		err = handler.HandleOpenRelation(ctx, meta, &request, &r)
		ret = r

	case OpGetRelations:
		var r GetRelationsResp
		request := req.(GetRelationsReq)
		err = handler.HandleGetRelations(ctx, meta, &request, &r)
		ret = r

	case OpAddTableDef:
		var r AddTableDefResp
		request := req.(AddTableDefReq)
		err = handler.HandleAddTableDef(ctx, meta, &request, &r)
		ret = r

	case OpDelTableDef:
		var r DelTableDefResp
		request := req.(DelTableDefReq)
		err = handler.HandleDelTableDef(ctx, meta, &request, &r)
		ret = r

	case OpDelete:
		var r DeleteResp
		request := req.(DeleteReq)
		err = handler.HandleDelete(ctx, meta, &request, &r)
		ret = r

	case OpGetPrimaryKeys:
		var r GetPrimaryKeysResp
		request := req.(GetPrimaryKeysReq)
		err = handler.HandleGetPrimaryKeys(ctx, meta, &request, &r)
		ret = r

	case OpGetTableColumns:
		var r GetTableColumnsResp
		request := req.(GetTableColumnsReq)
		err = handler.HandleGetTableColumns(ctx, meta, &request, &r)
		ret = r

	case OpGetTableDefs:
		var r GetTableDefsResp
		request := req.(GetTableDefsReq)
		err = handler.HandleGetTableDefs(ctx, meta, &request, &r)
		ret = r

	case OpGetHiddenKeys:
		var r GetHiddenKeysResp
		request := req.(GetHiddenKeysReq)
		err = handler.HandleGetHiddenKeys(ctx, meta, &request, &r)
		ret = r

	case OpUpdate:
		var r UpdateResp
		request := req.(UpdateReq)
		err = handler.HandleUpdate(ctx, meta, &request, &r)
		ret = r

	case OpWrite, OpPreCommit:
		var r WriteResp
		request := req.(WriteReq)
		err = handler.HandleWrite(ctx, meta, &request, &r)
		ret = r

	case OpNewTableIter:
		var r NewTableIterResp
		request := req.(NewTableIterReq)
		err = handler.HandleNewTableIter(ctx, meta, &request, &r)
		ret = r

	case OpRead:
		var r ReadResp
		request := req.(ReadReq)
		err = handler.HandleRead(ctx, meta, &request, &r)
		ret = r

	case OpCloseTableIter:
		var r CloseTableIterResp
		request := req.(CloseTableIterReq)
		err = handler.HandleCloseTableIter(ctx, meta, &request, &r)
		ret = r

	case OpTableStats:
		var r TableStatsResp
		request := req.(TableStatsReq)
		err = handler.HandleTableStats(ctx, meta, &request, &r)
		ret = r

	default:
		panic(fmt.Sprintf("unknown operation %v", op))
	}

	return
}
