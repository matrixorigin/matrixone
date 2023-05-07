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
	"encoding"
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
	req encoding.BinaryMarshaler,
	resp encoding.BinaryUnmarshaler,
) (
	err error,
) {

	switch op {

	case OpCreateDatabase:
		response := resp.(*CreateDatabaseResp)
		request := req.(*CreateDatabaseReq)
		err = handler.HandleCreateDatabase(ctx, meta, request, response)

	case OpOpenDatabase:
		response := resp.(*OpenDatabaseResp)
		request := req.(*OpenDatabaseReq)
		err = handler.HandleOpenDatabase(ctx, meta, request, response)

	case OpGetDatabases:
		response := resp.(*GetDatabasesResp)
		request := req.(*GetDatabasesReq)
		err = handler.HandleGetDatabases(ctx, meta, request, response)

	case OpDeleteDatabase:
		response := resp.(*DeleteDatabaseResp)
		request := req.(*DeleteDatabaseReq)
		err = handler.HandleDeleteDatabase(ctx, meta, request, response)

	case OpCreateRelation:
		response := resp.(*CreateRelationResp)
		request := req.(*CreateRelationReq)
		err = handler.HandleCreateRelation(ctx, meta, request, response)

	case OpDeleteRelation:
		response := resp.(*DeleteRelationResp)
		request := req.(*DeleteRelationReq)
		err = handler.HandleDeleteRelation(ctx, meta, request, response)

	case OpTruncateRelation:
		response := resp.(*TruncateRelationResp)
		request := req.(*TruncateRelationReq)
		handler.HandleTruncateRelation(ctx, meta, request, response)

	case OpOpenRelation:
		response := resp.(*OpenRelationResp)
		request := req.(*OpenRelationReq)
		err = handler.HandleOpenRelation(ctx, meta, request, response)

	case OpGetRelations:
		response := resp.(*GetRelationsResp)
		request := req.(*GetRelationsReq)
		err = handler.HandleGetRelations(ctx, meta, request, response)

	case OpAddTableDef:
		response := resp.(*AddTableDefResp)
		request := req.(*AddTableDefReq)
		err = handler.HandleAddTableDef(ctx, meta, request, response)

	case OpDelTableDef:
		response := resp.(*DelTableDefResp)
		request := req.(*DelTableDefReq)
		err = handler.HandleDelTableDef(ctx, meta, request, response)

	case OpDelete:
		response := resp.(*DeleteResp)
		request := req.(*DeleteReq)
		err = handler.HandleDelete(ctx, meta, request, response)

	case OpGetPrimaryKeys:
		response := resp.(*GetPrimaryKeysResp)
		request := req.(*GetPrimaryKeysReq)
		err = handler.HandleGetPrimaryKeys(ctx, meta, request, response)

	case OpGetTableColumns:
		response := resp.(*GetTableColumnsResp)
		request := req.(*GetTableColumnsReq)
		err = handler.HandleGetTableColumns(ctx, meta, request, response)

	case OpGetTableDefs:
		response := resp.(*GetTableDefsResp)
		request := req.(*GetTableDefsReq)
		err = handler.HandleGetTableDefs(ctx, meta, request, response)

	case OpGetHiddenKeys:
		response := resp.(*GetHiddenKeysResp)
		request := req.(*GetHiddenKeysReq)
		err = handler.HandleGetHiddenKeys(ctx, meta, request, response)

	case OpUpdate:
		response := resp.(*UpdateResp)
		request := req.(*UpdateReq)
		err = handler.HandleUpdate(ctx, meta, request, response)

	case OpWrite, OpPreCommit:
		response := resp.(*WriteResp)
		request := req.(*WriteReq)
		err = handler.HandleWrite(ctx, meta, request, response)

	case OpNewTableIter:
		response := resp.(*NewTableIterResp)
		request := req.(*NewTableIterReq)
		err = handler.HandleNewTableIter(ctx, meta, request, response)

	case OpRead:
		response := resp.(*ReadResp)
		request := req.(*ReadReq)
		err = handler.HandleRead(ctx, meta, request, response)

	case OpCloseTableIter:
		response := resp.(*CloseTableIterResp)
		request := req.(*CloseTableIterReq)
		err = handler.HandleCloseTableIter(ctx, meta, request, response)

	case OpTableStats:
		response := resp.(*TableStatsResp)
		request := req.(*TableStatsReq)
		err = handler.HandleTableStats(ctx, meta, request, response)

	default:
		panic(fmt.Sprintf("unknown operation %v", op))
	}

	return
}
