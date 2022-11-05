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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ engine.Database = new(database)

func (db *database) Relations(ctx context.Context) ([]string, error) {
	tables, err := db.txn.getTableList(ctx, db.databaseId)
	if err != nil {
		return nil, err
	}
	return tables, nil
}

func (db *database) Relation(ctx context.Context, name string) (engine.Relation, error) {
	key := genTableKey(ctx, name, db.databaseId)
	if tbl, ok := db.txn.tableMap.Load(key); ok {
		return tbl.(*table), nil
	}
	// for acceleration, and can work without these codes.
	if name == catalog.MO_DATABASE {
		id := uint64(catalog.MO_DATABASE_ID)
		defs := catalog.MoDatabaseTableDefs
		return db.openSysTable(key, id, name, defs), nil
	}
	if name == catalog.MO_TABLES {
		id := uint64(catalog.MO_TABLES_ID)
		defs := catalog.MoTablesTableDefs
		return db.openSysTable(key, id, name, defs), nil
	}
	if name == catalog.MO_COLUMNS {
		id := uint64(catalog.MO_COLUMNS_ID)
		defs := catalog.MoColumnsTableDefs
		return db.openSysTable(key, id, name, defs), nil

	}
	tbl, defs, err := db.txn.getTableInfo(ctx, db.databaseId, name)
	if err != nil {
		return nil, err
	}
	tbl.defs = defs
	tbl.tableDef = nil
	_, ok := db.txn.createTableMap[tbl.tableId]
	columnLength := len(tbl.getTableDef().Cols) - 1 //we use this data to fetch zonemap, but row_id has no zonemap
	meta, err := db.txn.getTableMeta(ctx, db.databaseId, genMetaTableName(tbl.tableId), !ok, columnLength)
	if err != nil {
		return nil, err
	}
	parts := db.txn.db.getPartitions(db.databaseId, tbl.tableId)
	tbl.db = db
	tbl.meta = meta
	tbl.parts = parts
	tbl.tableName = name
	tbl.insertExpr = genInsertExpr(defs, len(parts))
	db.txn.tableMap.Store(key, tbl)
	return tbl, nil
}

func (db *database) Delete(ctx context.Context, name string) error {
	key := genTableKey(ctx, name, db.databaseId)
	db.txn.tableMap.Delete(key)
	id, err := db.txn.getTableId(ctx, db.databaseId, name)
	if err != nil {
		return err
	}
	bat, err := genDropTableTuple(id, db.databaseId, name, db.databaseName, db.txn.proc.Mp())
	if err != nil {
		return err
	}
	if err := db.txn.WriteBatch(DELETE, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		catalog.MO_CATALOG, catalog.MO_TABLES, bat, db.txn.dnStores[0], -1); err != nil {
		return err
	}
	metaName := genMetaTableName(id)
	db.txn.deleteMetaTables = append(db.txn.deleteMetaTables, metaName)
	return nil
}

func (db *database) Truncate(ctx context.Context, name string) error {
	var tbl *table
	var oldId uint64

	newId, err := db.txn.allocateID(ctx)
	if err != nil {
		return err
	}
	key := genTableKey(ctx, name, db.databaseId)
	if v, ok := db.txn.tableMap.Load(key); ok {
		tbl = v.(*table)
	}

	if tbl != nil {
		oldId = tbl.tableId
		tbl.tableId = newId
	} else {
		if oldId, err = db.txn.getTableId(ctx, db.databaseId, name); err != nil {
			return err
		}
	}

	bat, err := genTruncateTableTuple(newId, db.databaseId,
		genMetaTableName(oldId)+name, db.databaseName, db.txn.proc.Mp())
	if err != nil {
		return err
	}
	for i := range db.txn.dnStores {
		if err := db.txn.WriteBatch(DELETE, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, bat, db.txn.dnStores[i], -1); err != nil {
			return err
		}
	}
	return nil
}

func (db *database) GetDatabaseId(ctx context.Context) string {
	return strconv.FormatUint(db.databaseId, 10)
}

func (db *database) Create(ctx context.Context, name string, defs []engine.TableDef) error {
	comment := getTableComment(defs)
	accountId, userId, roleId := getAccessInfo(ctx)
	tableId, err := db.txn.allocateID(ctx)
	if err != nil {
		return err
	}
	tbl := new(table)
	{
		for _, def := range defs { // copy from tae
			switch defVal := def.(type) {
			case *engine.PropertiesDef:
				for _, property := range defVal.Properties {
					switch strings.ToLower(property.Key) {
					case catalog.SystemRelAttr_Comment: // Watch priority over commentDef
						tbl.comment = property.Value
					case catalog.SystemRelAttr_Kind:
						tbl.relKind = property.Value
					case catalog.SystemRelAttr_CreateSQL:
						tbl.createSql = property.Value // I don't trust this information.
					default:
					}
				}
			case *engine.ViewDef:
				tbl.viewdef = defVal.View
			case *engine.PartitionDef:
				tbl.partition = defVal.Partition
			}
		}
	}
	cols, err := genColumns(accountId, name, db.databaseName, tableId, db.databaseId, defs)
	if err != nil {
		return err
	}
	{
		sql := getSql(ctx)
		bat, err := genCreateTableTuple(tbl, sql, accountId, userId, roleId, name,
			tableId, db.databaseId, db.databaseName, comment, db.txn.proc.Mp())
		if err != nil {
			return err
		}
		if err := db.txn.WriteBatch(INSERT, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, bat, db.txn.dnStores[0], -1); err != nil {
			return err
		}
	}
	for _, col := range cols {
		bat, err := genCreateColumnTuple(col, db.txn.proc.Mp())
		if err != nil {
			return err
		}
		if err := db.txn.WriteBatch(INSERT, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG, catalog.MO_COLUMNS, bat, db.txn.dnStores[0], -1); err != nil {
			return err
		}
	}
	db.txn.createTableMap[tableId] = 0
	return nil
}

func (db *database) openSysTable(key tableKey, id uint64, name string,
	defs []engine.TableDef) engine.Relation {
	parts := db.txn.db.getPartitions(db.databaseId, id)
	tbl := &table{
		db:        db,
		tableId:   id,
		tableName: name,
		defs:      defs,
		parts:     parts,
	}
	db.txn.tableMap.Store(key, tbl)
	return tbl
}
