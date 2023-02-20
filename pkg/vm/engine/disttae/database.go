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

var _ engine.Database = new(database)

func (db *database) Relations(ctx context.Context) ([]string, error) {
	var rels []string

	db.txn.createMap.Range(func(k, _ any) bool {
		key := k.(tableKey)
		if key.databaseId == db.databaseId {
			rels = append(rels, key.name)
		}
		return true
	})
	tbls, _ := db.txn.catalog.Tables(getAccountId(ctx), db.databaseId, db.txn.meta.SnapshotTS)
	rels = append(rels, tbls...)
	return rels, nil
}

func (db *database) getTableNameById(ctx context.Context, id uint64) string {
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
		tbls, tblIds := db.txn.catalog.Tables(getAccountId(ctx), db.databaseId, db.txn.meta.SnapshotTS)
		for idx, tblId := range tblIds {
			if tblId == id {
				tblName = tbls[idx]
				break
			}
		}
	}
	return tblName
}

func (db *database) getRelationById(ctx context.Context, id uint64) (string, engine.Relation, error) {
	tblName := db.getTableNameById(ctx, id)
	if tblName == "" {
		return "", nil, moerr.NewInternalError(ctx, "can not find table by id %d", id)
	}
	rel, err := db.Relation(ctx, tblName)
	return tblName, rel, err
}

func (db *database) Relation(ctx context.Context, name string) (engine.Relation, error) {
	if v, ok := db.txn.tableMap.Load(genTableKey(ctx, name, db.databaseId)); ok {
		return v.(*table), nil
	}
	if v, ok := db.txn.createMap.Load(genTableKey(ctx, name, db.databaseId)); ok {
		return v.(*table), nil
	}
	if db.databaseName == "mo_catalog" {
		if name == catalog.MO_DATABASE {
			id := uint64(catalog.MO_DATABASE_ID)
			defs := catalog.MoDatabaseTableDefs
			return db.openSysTable(genTableKey(ctx, name, db.databaseId), id, name, defs), nil
		}
		if name == catalog.MO_TABLES {
			id := uint64(catalog.MO_TABLES_ID)
			defs := catalog.MoTablesTableDefs
			return db.openSysTable(genTableKey(ctx, name, db.databaseId), id, name, defs), nil
		}
		if name == catalog.MO_COLUMNS {
			id := uint64(catalog.MO_COLUMNS_ID)
			defs := catalog.MoColumnsTableDefs
			return db.openSysTable(genTableKey(ctx, name, db.databaseId), id, name, defs), nil

		}
	}
	key := &cache.TableItem{
		Name:       name,
		DatabaseId: db.databaseId,
		AccountId:  getAccountId(ctx),
		Ts:         db.txn.meta.SnapshotTS,
	}
	if ok := db.txn.catalog.GetTable(key); !ok {
		return nil, moerr.NewParseError(ctx, "table %q does not exist", name)
	}
	parts := db.txn.db.getPartitions(db.databaseId, key.Id)
	tbl := &table{
		db:           db,
		parts:        parts,
		tableId:      key.Id,
		tableName:    key.Name,
		defs:         key.Defs,
		tableDef:     key.TableDef,
		primaryIdx:   key.PrimaryIdx,
		clusterByIdx: key.ClusterByIdx,
		relKind:      key.Kind,
		viewdef:      key.ViewDef,
		comment:      key.Comment,
		partition:    key.Partition,
		createSql:    key.CreateSql,
		constraint:   key.Constraint,
	}
	columnLength := len(key.TableDef.Cols) - 1 //we use this data to fetch zonemap, but row_id has no zonemap
	meta, err := db.txn.getTableMeta(ctx, db.databaseId, genMetaTableName(key.Id),
		true, columnLength, true)
	if err != nil {
		return nil, err
	}
	tbl.meta = meta
	tbl.updated = false
	db.txn.tableMap.Store(genTableKey(ctx, name, db.databaseId), tbl)
	return tbl, nil
}

func (db *database) Delete(ctx context.Context, name string) error {
	var id uint64

	k := genTableKey(ctx, name, db.databaseId)
	if _, ok := db.txn.createMap.Load(k); ok {
		db.txn.createMap.Delete(k)
		return nil
	} else if v, ok := db.txn.tableMap.Load(k); ok {
		id = v.(*table).tableId
		db.txn.tableMap.Delete(k)
	} else {
		key := &cache.TableItem{
			Name:       name,
			DatabaseId: db.databaseId,
			AccountId:  getAccountId(ctx),
			Ts:         db.txn.meta.SnapshotTS,
		}
		if ok := db.txn.catalog.GetTable(key); !ok {
			return moerr.GetOkExpectedEOB()
		}
		id = key.Id
	}
	bat, err := genDropTableTuple(id, db.databaseId, name, db.databaseName, db.txn.proc.Mp())
	if err != nil {
		return err
	}
	if err := db.txn.WriteBatch(DELETE, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		catalog.MO_CATALOG, catalog.MO_TABLES, bat, db.txn.dnStores[0], -1); err != nil {
		return err
	}
	return nil
}

func (db *database) Truncate(ctx context.Context, name string) (uint64, error) {
	var oldId uint64

	newId, err := db.txn.allocateID(ctx)
	if err != nil {
		return 0, err
	}
	k := genTableKey(ctx, name, db.databaseId)
	if v, ok := db.txn.createMap.Load(k); ok {
		oldId = v.(*table).tableId
		v.(*table).tableId = newId
	} else if v, ok := db.txn.tableMap.Load(k); ok {
		oldId = v.(*table).tableId
	} else {
		key := &cache.TableItem{
			Name:       name,
			DatabaseId: db.databaseId,
			AccountId:  getAccountId(ctx),
			Ts:         db.txn.meta.SnapshotTS,
		}
		if ok := db.txn.catalog.GetTable(key); !ok {
			return 0, moerr.GetOkExpectedEOB()
		}
		oldId = key.Id
	}
	bat, err := genTruncateTableTuple(newId, db.databaseId,
		genMetaTableName(oldId)+name, db.databaseName, db.txn.proc.Mp())
	if err != nil {
		return 0, err
	}
	for i := range db.txn.dnStores {
		if err := db.txn.WriteBatch(DELETE, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, bat, db.txn.dnStores[i], -1); err != nil {
			return 0, err
		}
	}
	return newId, nil
}

func (db *database) GetDatabaseId(ctx context.Context) string {
	return strconv.FormatUint(db.databaseId, 10)
}

func (db *database) Create(ctx context.Context, name string, defs []engine.TableDef) error {
	accountId, userId, roleId := getAccessInfo(ctx)
	tableId, err := db.txn.allocateID(ctx)
	if err != nil {
		return err
	}
	tbl := new(table)
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
		if err := db.txn.WriteBatch(INSERT, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, bat, db.txn.dnStores[0], -1); err != nil {
			return err
		}
	}
	tbl.primaryIdx = -1
	tbl.clusterByIdx = -1
	for i, col := range cols {
		bat, err := genCreateColumnTuple(col, db.txn.proc.Mp())
		if err != nil {
			return err
		}
		if err := db.txn.WriteBatch(INSERT, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG, catalog.MO_COLUMNS, bat, db.txn.dnStores[0], -1); err != nil {
			return err
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
	tbl.parts = db.txn.db.getPartitions(db.databaseId, tableId)
	tbl.getTableDef()
	db.txn.createMap.Store(genTableKey(ctx, name, db.databaseId), tbl)
	return nil
}

func (db *database) openSysTable(key tableKey, id uint64, name string,
	defs []engine.TableDef) engine.Relation {
	parts := db.txn.db.getPartitions(db.databaseId, id)
	tbl := &table{
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
