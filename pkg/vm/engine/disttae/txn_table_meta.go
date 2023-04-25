// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func (tbl *txnTable) updateMeta(ctx context.Context, expr *plan.Expr) error {
	tbl.dnList = []int{0}
	_, created := tbl.db.txn.createMap.Load(genTableKey(ctx, tbl.tableName, tbl.db.databaseId))
	if !created && !tbl.updated {
		if tbl.db.txn.engine.UsePushModelOrNot() {
			if err := tbl.db.txn.engine.UpdateOfPush(ctx, tbl.db.databaseId, tbl.tableId, tbl.db.txn.meta.SnapshotTS); err != nil {
				return err
			}
			err := tbl.db.txn.engine.lazyLoad(ctx, tbl.db.databaseId, tbl.tableId, tbl)
			if err != nil {
				return err
			}
		} else {
			if err := tbl.db.txn.engine.UpdateOfPull(ctx, tbl.db.txn.dnStores[:1], tbl, tbl.db.txn.op, tbl.primaryIdx,
				tbl.db.databaseId, tbl.tableId, tbl.db.txn.meta.SnapshotTS); err != nil {
				return err
			}
		}
		columnLength := len(tbl.tableDef.Cols) - 1 //we use this data to fetch zonemap, but row_id has no zonemap
		metas, err := tbl.db.txn.getBlockMetas(ctx, tbl.db.databaseId, tbl.tableId, true, columnLength, false)
		if err != nil {
			return err
		}
		tbl.blockMetas = metas
		tbl.updated = true
	}
	return nil
}
