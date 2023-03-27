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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
)

var _ engine.Database = new(txnDatabase)

func (db *txnDatabase) Relations(ctx context.Context) ([]string, error) {
	var rels []string

	db.txn.createMap.Range(func(k, _ any) bool {
		key := k.(tableKey)
		if key.databaseId == db.databaseId {
			rels = append(rels, key.name)
		}
		return true
	})
	tbls, _ := db.txn.engine.catalog.Tables(getAccountId(ctx), db.databaseId, db.txn.meta.SnapshotTS)
	rels = append(rels, tbls...)
	return rels, nil
}

func (db *txnDatabase) getTableNameById(ctx context.Context, id uint64) string {
	tblName := ""
	db.txn.createMap.Range(func(k, _ any) bool {
		key := k.(tableKey)
		if key.databaseId == db.databaseId && key.tableId == id {
			tblName = key.name
			return false
		}
		return true
	})

	if tblName == "" {
		tbls, tblIds := db.txn.engine.catalog.Tables(getAccountId(ctx), db.databaseId, db.txn.meta.SnapshotTS)
		for idx, tblId := range tblIds {
			if tblId == id {
				tblName = tbls[idx]
				break
			}
		}
	}
	return tblName
}

func (db *txnDatabase) getRelationById(ctx context.Context, id uint64) (string, engine.Relation, error) {
	tblName := db.getTableNameById(ctx, id)
	if tblName == "" {
		return "", nil, moerr.NewInternalError(ctx, "can not find table by id %d", id)
	}
	rel, err := db.Relation(ctx, tblName)
	return tblName, rel, err
}

func (db *txnDatabase) Relation(ctx context.Context, name string) (engine.Relation, error) {
	logDebugf(db.txn.meta, "txnDatabase.Relation table %s", name)
	if v, ok := db.txn.tableMap.Load(genTableKey(ctx, name, db.databaseId)); ok {
		return v.(*txnTable), nil
	}
	if v, ok := db.txn.createMap.Load(genTableKey(ctx, name, db.databaseId)); ok {
		return v.(*txnTable), nil
	}
	if db.databaseName == catalog.MO_CATALOG {
		switch name {
		case catalog.MO_DATABASE:
			id := uint64(catalog.MO_DATABASE_ID)
			defs := catalog.MoDatabaseTableDefs
			return db.openSysTable(genTableKey(ctx, name, db.databaseId), id, name, defs), nil
		case catalog.MO_TABLES:
			id := uint64(catalog.MO_TABLES_ID)
			defs := catalog.MoTablesTableDefs
			return db.openSysTable(genTableKey(ctx, name, db.databaseId), id, name, defs), nil
		case catalog.MO_COLUMNS:
			id := uint64(catalog.MO_COLUMNS_ID)
			defs := catalog.MoColumnsTableDefs
			return db.openSysTable(genTableKey(ctx, name, db.databaseId), id, name, defs), nil
		}
	}
	item := &cache.TableItem{
		Name:       name,
		DatabaseId: db.databaseId,
		AccountId:  getAccountId(ctx),
		Ts:         db.txn.meta.SnapshotTS,
	}
	if ok := db.txn.engine.catalog.GetTable(item); !ok {
		return nil, moerr.NewParseError(ctx, "table %q does not exist", name)
	}
	tbl := &txnTable{
		db:           db,
		tableId:      item.Id,
		tableName:    item.Name,
		defs:         item.Defs,
		tableDef:     item.TableDef,
		primaryIdx:   item.PrimaryIdx,
		clusterByIdx: item.ClusterByIdx,
		relKind:      item.Kind,
		viewdef:      item.ViewDef,
		comment:      item.Comment,
		partition:    item.Partition,
		createSql:    item.CreateSql,
		constraint:   item.Constraint,
		parts:        db.txn.engine.getPartitions(db.databaseId, item.Id).Snapshot(),
	}
	columnLength := len(item.TableDef.Cols) - 1 // we use this data to fetch zonemap, but row_id has no zonemap
	meta, err := db.txn.getTableMeta(ctx, db.databaseId, item.Id,
		true, columnLength, true)
	if err != nil {
		return nil, err
	}
	tbl.meta = meta
	tbl.updated = false
	db.txn.tableMap.Store(genTableKey(ctx, name, db.databaseId), tbl)
	return tbl, nil
}

func (db *txnDatabase) Delete(ctx context.Context, name string) error {
	var id uint64

	k := genTableKey(ctx, name, db.databaseId)
	if _, ok := db.txn.createMap.Load(k); ok {
		db.txn.createMap.Delete(k)
		return nil
	} else if v, ok := db.txn.tableMap.Load(k); ok {
		id = v.(*txnTable).tableId
		db.txn.tableMap.Delete(k)
	} else {
		item := &cache.TableItem{
			Name:       name,
			DatabaseId: db.databaseId,
			AccountId:  getAccountId(ctx),
			Ts:         db.txn.meta.SnapshotTS,
		}
		if ok := db.txn.engine.catalog.GetTable(item); !ok {
			return moerr.GetOkExpectedEOB()
		}
		id = item.Id
	}
	bat, err := genDropTableTuple(id, db.databaseId, name, db.databaseName, db.txn.proc.Mp())
	if err != nil {
		return err
	}

	for _, store := range db.txn.dnStores {
		if err := db.txn.WriteBatch(DELETE, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, bat, store, -1); err != nil {
			return err
		}
	}

	return nil
}

func (db *txnDatabase) Truncate(ctx context.Context, name string) (uint64, error) {
	var oldId uint64

	newId, err := db.txn.allocateID(ctx)
	if err != nil {
		return 0, err
	}
	k := genTableKey(ctx, name, db.databaseId)
	if v, ok := db.txn.createMap.Load(k); ok {
		oldId = v.(*txnTable).tableId
		v.(*txnTable).tableId = newId
	} else if v, ok := db.txn.tableMap.Load(k); ok {
		oldId = v.(*txnTable).tableId
	} else {
		item := &cache.TableItem{
			Name:       name,
			DatabaseId: db.databaseId,
			AccountId:  getAccountId(ctx),
			Ts:         db.txn.meta.SnapshotTS,
		}
		if ok := db.txn.engine.catalog.GetTable(item); !ok {
			return 0, moerr.GetOkExpectedEOB()
		}
		oldId = item.Id
	}
	bat, err := genTruncateTableTuple(newId, db.databaseId,
		genMetaTableName(oldId)+name, db.databaseName, db.txn.proc.Mp())
	if err != nil {
		return 0, err
	}
	for _, store := range db.txn.dnStores {
		if err := db.txn.WriteBatch(DELETE, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, bat, store, -1); err != nil {
			return 0, err
		}
	}
	return newId, nil
}

func (db *txnDatabase) GetDatabaseId(ctx context.Context) string {
	return strconv.FormatUint(db.databaseId, 10)
}

func (db *txnDatabase) GetCreateSql(ctx context.Context) string {
	return db.databaseCreateSql
}

func (db *txnDatabase) IsSubscription(ctx context.Context) bool {
	return db.databaseType == catalog.SystemDBTypeSubscription
}

func (db *txnDatabase) Create(ctx context.Context, name string, defs []engine.TableDef) error {
	accountId, userId, roleId := getAccessInfo(ctx)
	tableId, err := db.txn.allocateID(ctx)
	if err != nil {
		return err
	}
	tbl := new(txnTable)
	tbl.comment = getTableComment(defs)
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
			case *engine.ConstraintDef:
				tbl.constraint, err = defVal.MarshalBinary()
				if err != nil {
					return err
				}
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
			tableId, db.databaseId, db.databaseName, db.txn.proc.Mp())
		if err != nil {
			return err
		}
		for _, store := range db.txn.dnStores {
			if err := db.txn.WriteBatch(INSERT, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
				catalog.MO_CATALOG, catalog.MO_TABLES, bat, store, -1); err != nil {
				return err
			}
		}
	}
	tbl.primaryIdx = -1
	tbl.clusterByIdx = -1
	for i, col := range cols {
		bat, err := genCreateColumnTuple(col, db.txn.proc.Mp())
		if err != nil {
			return err
		}
		for _, store := range db.txn.dnStores {
			if err := db.txn.WriteBatch(INSERT, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID,
				catalog.MO_CATALOG, catalog.MO_COLUMNS, bat, store, -1); err != nil {
				return err
			}
		}
		if col.constraintType == catalog.SystemColPKConstraint {
			tbl.primaryIdx = i
		}
		if col.isClusterBy == 1 {
			tbl.clusterByIdx = i
		}
	}
	tbl.db = db
	tbl.defs = defs
	tbl.tableName = name
	tbl.tableId = tableId
	tbl.parts = db.txn.engine.getPartitions(db.databaseId, tableId).Snapshot()
	tbl.getTableDef()
	db.txn.createMap.Store(genTableKey(ctx, name, db.databaseId), tbl)
	return nil
}

func (db *txnDatabase) openSysTable(key tableKey, id uint64, name string,
	defs []engine.TableDef) engine.Relation {
	parts := db.txn.engine.getPartitions(db.databaseId, id).Snapshot()
	tbl := &txnTable{
		db:           db,
		tableId:      id,
		tableName:    name,
		defs:         defs,
		parts:        parts,
		primaryIdx:   -1,
		clusterByIdx: -1,
	}
	tbl.getTableDef()
	return tbl
}
