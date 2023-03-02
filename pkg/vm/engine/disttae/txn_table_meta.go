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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (tbl *txnTable) updateMeta(ctx context.Context, expr *plan.Expr) error {

	primaryKeys := make([]*engine.Attribute, 0, 1)
	if tbl.primaryIdx >= 0 {
		for _, def := range tbl.defs {
			attr, ok := def.(*engine.AttributeDef)
			if !ok {
				continue
			}
			if !attr.Attr.Primary {
				continue
			}
			primaryKeys = append(primaryKeys, &attr.Attr)
		}
	}

	dnList := needSyncDnStores(ctx, expr, tbl.tableDef, primaryKeys,
		tbl.db.txn.dnStores, tbl.db.txn.proc)
	switch tbl.tableId {
	case catalog.MO_DATABASE_ID,
		catalog.MO_TABLES_ID,
		catalog.MO_COLUMNS_ID:
		tbl.dnList = []int{0}
	default:
		tbl.dnList = dnList
	}

	_, created := tbl.db.txn.createMap.Load(genTableKey(ctx, tbl.tableName, tbl.db.databaseId))
	if !created && !tbl.updated {
		if tbl.db.txn.engine.UsePushModelOrNot() {
			if err := tbl.db.txn.engine.UpdateOfPush(ctx, tbl.db.databaseId, tbl.tableId, tbl.db.txn.meta.SnapshotTS); err != nil {
				return err
			}
		} else {
			if err := tbl.db.txn.engine.UpdateOfPull(ctx, tbl.db.txn.dnStores[:1], tbl, tbl.db.txn.op, tbl.primaryIdx,
				tbl.db.databaseId, tbl.tableId, tbl.db.txn.meta.SnapshotTS); err != nil {
				return err
			}
		}

		columnLength := len(tbl.tableDef.Cols) - 1 //we use this data to fetch zonemap, but row_id has no zonemap
		meta, err := tbl.db.txn.getTableMeta(ctx, tbl.db.databaseId, genMetaTableName(tbl.tableId), true, columnLength, false)
		if err != nil {
			return err
		}
		tbl.meta = meta
		tbl.updated = true
	}

	return nil
}
