// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

func (tbl *txnTable) RegisterRebaseSnapshotDependency(ctx context.Context, physicalTime int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if physicalTime <= 0 || tbl.db.op.IsSnapOp() || catalog.IsSystemTable(tbl.tableId) {
		return moerr.NewInvalidInput(ctx, "invalid Rebase snapshot dependency")
	}
	created, err := tbl.isCreatedInTxn(ctx)
	if err != nil {
		return err
	}
	if created {
		return moerr.NewNotSupported(ctx, "Rebase dependency requires a previously committed table")
	}
	txn := tbl.getTxn()
	if len(txn.tnStores) != 1 || len(txn.tnStores[0].Shards) != 1 {
		return moerr.NewNotSupported(ctx, "Rebase dependency requires one storage shard")
	}
	req := api.NewValidateRebaseSnapshotReq(tbl.db.databaseId, tbl.tableId, physicalTime)
	payload, err := req.Marshal()
	if err != nil {
		return err
	}
	bat, err := catalog.GenTableAlterTuple([][]byte{payload}, txn.proc.Mp())
	if err != nil {
		return err
	}
	if _, err = txn.WriteBatch(REBASE_SNAPSHOT_DEPENDENCY, "", tbl.accountId, tbl.db.databaseId, tbl.tableId,
		tbl.db.databaseName, tbl.tableName, bat, txn.tnStores[0]); err != nil {
		bat.Clean(txn.proc.Mp())
		return err
	}
	return nil
}

func (tbl *txnTableDelegate) RegisterRebaseSnapshotDependency(ctx context.Context, physicalTime int64) error {
	if tbl.combined.is || tbl.shard.is || tbl.origin == nil {
		return moerr.NewNotSupported(ctx, "Rebase dependency does not support combined or sharded tables")
	}
	return tbl.origin.RegisterRebaseSnapshotDependency(ctx, physicalTime)
}
