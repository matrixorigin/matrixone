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

package memorystorage

import (
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

type Handler interface {
	HandleCommit(
		meta txn.TxnMeta,
	) error

	HandleRollback(
		meta txn.TxnMeta,
	) error

	HandleCommitting(
		meta txn.TxnMeta,
	) error

	HandlePrepare(
		meta txn.TxnMeta,
	) (
		timestamp.Timestamp,
		error,
	)

	HandleStartRecovery(
		ch chan txn.TxnMeta,
	)

	HandleClose() error

	HandleDestroy() error

	HandleOpenDatabase(
		meta txn.TxnMeta,
		req memoryengine.OpenDatabaseReq,
		resp *memoryengine.OpenDatabaseResp,
	) error

	HandleGetDatabases(
		meta txn.TxnMeta,
		req memoryengine.GetDatabasesReq,
		resp *memoryengine.GetDatabasesResp,
	) error

	HandleOpenRelation(
		meta txn.TxnMeta,
		req memoryengine.OpenRelationReq,
		resp *memoryengine.OpenRelationResp,
	) error

	HandleGetRelations(
		meta txn.TxnMeta,
		req memoryengine.GetRelationsReq,
		resp *memoryengine.GetRelationsResp,
	) error

	HandleGetPrimaryKeys(
		meta txn.TxnMeta,
		req memoryengine.GetPrimaryKeysReq,
		resp *memoryengine.GetPrimaryKeysResp,
	) error

	HandleGetTableDefs(
		meta txn.TxnMeta,
		req memoryengine.GetTableDefsReq,
		resp *memoryengine.GetTableDefsResp,
	) error

	HandleGetHiddenKeys(
		meta txn.TxnMeta,
		req memoryengine.GetHiddenKeysReq,
		resp *memoryengine.GetHiddenKeysResp,
	) error

	HandleNewTableIter(
		meta txn.TxnMeta,
		req memoryengine.NewTableIterReq,
		resp *memoryengine.NewTableIterResp,
	) error

	HandleRead(
		meta txn.TxnMeta,
		req memoryengine.ReadReq,
		resp *memoryengine.ReadResp,
	) error

	HandleCloseTableIter(
		meta txn.TxnMeta,
		req memoryengine.CloseTableIterReq,
		resp *memoryengine.CloseTableIterResp,
	) error

	HandleCreateDatabase(
		meta txn.TxnMeta,
		req memoryengine.CreateDatabaseReq,
		resp *memoryengine.CreateDatabaseResp,
	) error

	HandleDeleteDatabase(
		meta txn.TxnMeta,
		req memoryengine.DeleteDatabaseReq,
		resp *memoryengine.DeleteDatabaseResp,
	) error

	HandleCreateRelation(
		meta txn.TxnMeta,
		req memoryengine.CreateRelationReq,
		resp *memoryengine.CreateRelationResp,
	) error

	HandleDeleteRelation(
		meta txn.TxnMeta,
		req memoryengine.DeleteRelationReq,
		resp *memoryengine.DeleteRelationResp,
	) error

	HandleAddTableDef(
		meta txn.TxnMeta,
		req memoryengine.AddTableDefReq,
		resp *memoryengine.AddTableDefResp,
	) error

	HandleDelTableDef(
		meta txn.TxnMeta,
		req memoryengine.DelTableDefReq,
		resp *memoryengine.DelTableDefResp,
	) error

	HandleDelete(
		meta txn.TxnMeta,
		req memoryengine.DeleteReq,
		resp *memoryengine.DeleteResp,
	) error

	HandleTruncate(
		meta txn.TxnMeta,
		req memoryengine.TruncateReq,
		resp *memoryengine.TruncateResp,
	) error

	HandleUpdate(
		meta txn.TxnMeta,
		req memoryengine.UpdateReq,
		resp *memoryengine.UpdateResp,
	) error

	HandleWrite(
		meta txn.TxnMeta,
		req memoryengine.WriteReq,
		resp *memoryengine.WriteResp,
	) error

	HandleTableStats(
		meta txn.TxnMeta,
		req memoryengine.TableStatsReq,
		resp *memoryengine.TableStatsResp,
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
