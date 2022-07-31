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

package txnstorage

import (
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

type Handler interface {
	HandleOpenDatabase(
		meta txn.TxnMeta,
		req txnengine.OpenDatabaseReq,
		resp *txnengine.OpenDatabaseResp,
	) error

	HandleGetDatabases(
		meta txn.TxnMeta,
		req txnengine.GetDatabasesReq,
		resp *txnengine.GetDatabasesResp,
	) error

	HandleOpenRelation(
		meta txn.TxnMeta,
		req txnengine.OpenRelationReq,
		resp *txnengine.OpenRelationResp,
	) error

	HandleGetRelations(
		meta txn.TxnMeta,
		req txnengine.GetRelationsReq,
		resp *txnengine.GetRelationsResp,
	) error

	HandleGetPrimaryKeys(
		meta txn.TxnMeta,
		req txnengine.GetPrimaryKeysReq,
		resp *txnengine.GetPrimaryKeysResp,
	) error

	HandleGetTableDefs(
		meta txn.TxnMeta,
		req txnengine.GetTableDefsReq,
		resp *txnengine.GetTableDefsResp,
	) error

	HandleNewTableIter(
		meta txn.TxnMeta,
		req txnengine.NewTableIterReq,
		resp *txnengine.NewTableIterResp,
	) error

	HandleRead(
		meta txn.TxnMeta,
		req txnengine.ReadReq,
		resp *txnengine.ReadResp,
	) error

	HandleCloseTableIter(
		meta txn.TxnMeta,
		req txnengine.CloseTableIterReq,
		resp *txnengine.CloseTableIterResp,
	) error

	HandleCreateDatabase(
		meta txn.TxnMeta,
		req txnengine.CreateDatabaseReq,
		resp *txnengine.CreateDatabaseResp,
	) error

	HandleDeleteDatabase(
		meta txn.TxnMeta,
		req txnengine.DeleteDatabaseReq,
		resp *txnengine.DeleteDatabaseResp,
	) error

	HandleCreateRelation(
		meta txn.TxnMeta,
		req txnengine.CreateRelationReq,
		resp *txnengine.CreateRelationResp,
	) error

	HandleDeleteRelation(
		meta txn.TxnMeta,
		req txnengine.DeleteRelationReq,
		resp *txnengine.DeleteRelationResp,
	) error

	HandleAddTableDef(
		meta txn.TxnMeta,
		req txnengine.AddTableDefReq,
		resp *txnengine.AddTableDefResp,
	) error

	HandleDelTableDef(
		meta txn.TxnMeta,
		req txnengine.DelTableDefReq,
		resp *txnengine.DelTableDefResp,
	) error

	HandleDelete(
		meta txn.TxnMeta,
		req txnengine.DeleteReq,
		resp *txnengine.DeleteResp,
	) error

	HandleTruncate(
		meta txn.TxnMeta,
		req txnengine.TruncateReq,
		resp *txnengine.TruncateResp,
	) error

	HandleUpdate(
		meta txn.TxnMeta,
		req txnengine.UpdateReq,
		resp *txnengine.UpdateResp,
	) error

	HandleWrite(
		meta txn.TxnMeta,
		req txnengine.WriteReq,
		resp *txnengine.WriteResp,
	) error
}
