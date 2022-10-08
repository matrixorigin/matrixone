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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ engine.Database = new(database)

func (db *database) Relations(ctx context.Context) ([]string, error) {
	return db.txn.getTableList(ctx, db.databaseId)
}

func (db *database) Relation(ctx context.Context, name string) (engine.Relation, error) {
	key := genTableKey(ctx, name, db.databaseId)
	if tbl, ok := db.txn.tableMap[key]; ok {
		return tbl, nil
	}
	id, defs, err := db.txn.getTableInfo(ctx, db.databaseId, name)
	if err != nil {
		return nil, err
	}
	meta, err := db.txn.getTableMeta(ctx, db.databaseId, genMetaTableName(id))
	if err != nil {
		return nil, err
	}
	parts := db.txn.db.getPartitions(db.databaseId, id)
	tbl := &table{
		db:         db,
		tableId:    id,
		tableName:  name,
		defs:       defs,
		meta:       meta,
		parts:      parts,
		insertExpr: genInsertExpr(defs, len(parts)),
		deleteExpr: genDeleteExpr(defs, len(parts)),
	}
	db.txn.tableMap[key] = tbl
	return tbl, nil
}

func (db *database) Delete(ctx context.Context, name string) error {
	key := genTableKey(ctx, name, db.databaseId)
	delete(db.txn.tableMap, key)
	id, err := db.txn.getTableId(ctx, db.databaseId, name)
	if err != nil {
		return err
	}
	bat, err := genDropTableTuple(id, db.databaseId, name, db.databaseName, db.txn.proc.GetMheap())
	if err != nil {
		return err
	}
	if err := db.txn.WriteBatch(DELETE, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		catalog.MO_CATALOG, catalog.MO_TABLES, bat, db.txn.dnStores[0]); err != nil {
		return err
	}
	return nil
}

func (db *database) Create(ctx context.Context, name string, defs []engine.TableDef) error {
	comment := getTableComment(defs)
	cols, err := genColumns(name, db.databaseName, db.databaseId, defs)
	if err != nil {
		return err
	}
	{
		sql := getSql(ctx)
		accountId, userId, roleId := getAccessInfo(ctx)
		bat, err := genCreateTableTuple(sql, accountId, userId, roleId, name,
			db.databaseId, db.databaseName, comment, db.txn.proc.GetMheap())
		if err != nil {
			return err
		}
		if err := db.txn.WriteBatch(INSERT, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, bat, db.txn.dnStores[0]); err != nil {
			return err
		}
	}
	for _, col := range cols {
		bat, err := genCreateColumnTuple(col, db.txn.proc.GetMheap())
		if err != nil {
			return err
		}
		if err := db.txn.WriteBatch(INSERT, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG, catalog.MO_COLUMNS, bat, db.txn.dnStores[0]); err != nil {
			return err
		}
	}
	return nil
}
