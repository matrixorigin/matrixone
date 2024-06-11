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
	"runtime/debug"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	txn2 "github.com/matrixorigin/matrixone/pkg/pb/txn"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ engine.Database = new(txnDatabase)

func (db *txnDatabase) getTxn() *Transaction {
	return db.op.GetWorkspace().(*Transaction)
}

func (db *txnDatabase) getEng() *Engine {
	return db.op.GetWorkspace().(*Transaction).engine
}

func (db *txnDatabase) Relations(ctx context.Context) ([]string, error) {
	var rels []string
	//first get all delete tables
	deleteTables := make(map[string]any)
	db.getTxn().deletedTableMap.Range(func(k, _ any) bool {
		key := k.(tableKey)
		if key.databaseId == db.databaseId {
			deleteTables[key.name] = nil
		}
		return true
	})
	db.getTxn().createMap.Range(func(k, _ any) bool {
		key := k.(tableKey)
		if key.databaseId == db.databaseId {
			//if the table is deleted, do not save it.
			if _, exist := deleteTables[key.name]; !exist {
				rels = append(rels, key.name)
			}
		}
		return true
	})
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	var catache *cache.CatalogCache
	if !db.op.IsSnapOp() {
		catache = db.getTxn().engine.getLatestCatalogCache()
	} else {
		catache, err = db.getTxn().engine.getOrCreateSnapCatalogCache(
			ctx,
			types.TimestampToTS(db.op.SnapshotTS()))
		if err != nil {
			return nil, err
		}
	}
	tbls, _ := catache.Tables(
		accountId, db.databaseId, db.op.SnapshotTS())
	for _, tbl := range tbls {
		//if the table is deleted, do not save it.
		if _, exist := deleteTables[tbl]; !exist {
			rels = append(rels, tbl)
		}
	}
	return rels, nil
}

func (db *txnDatabase) getTableNameById(ctx context.Context, id uint64) (string, error) {
	tblName := ""
	//first check the tableID is deleted or not
	deleted := false
	db.getTxn().deletedTableMap.Range(func(k, v any) bool {
		key := k.(tableKey)
		val := v.(uint64)
		if key.databaseId == db.databaseId && val == id {
			deleted = true
			return false
		}
		return true
	})
	if deleted {
		return "", nil
	}
	db.getTxn().createMap.Range(func(k, v any) bool {
		key := k.(tableKey)
		val := v.(*txnTable)
		if key.databaseId == db.databaseId && val.tableId == id {
			tblName = key.name
			return false
		}
		return true
	})

	if tblName == "" {
		accountId, err := defines.GetAccountId(ctx)
		if err != nil {
			return "", err
		}
		var catache *cache.CatalogCache
		if !db.op.IsSnapOp() {
			catache = db.getTxn().engine.getLatestCatalogCache()
		} else {
			catache, err = db.getTxn().engine.getOrCreateSnapCatalogCache(
				ctx,
				types.TimestampToTS(db.op.SnapshotTS()))
			if err != nil {
				return "", err
			}
		}
		tbls, tblIds := catache.Tables(
			accountId, db.databaseId, db.op.SnapshotTS())
		for idx, tblId := range tblIds {
			if tblId == id {
				tblName = tbls[idx]
				break
			}
		}
	}
	return tblName, nil
}

func (db *txnDatabase) getRelationById(ctx context.Context, id uint64) (string, engine.Relation, error) {
	tblName, err := db.getTableNameById(ctx, id)
	if err != nil {
		return "", nil, err
	}
	if tblName == "" {
		return "", nil, nil
	}
	rel, _ := db.Relation(ctx, tblName, nil)
	return tblName, rel, nil
}

func (db *txnDatabase) Relation(ctx context.Context, name string, proc any) (engine.Relation, error) {
	logDebugf(db.op.Txn(), "txnDatabase.Relation table %s", name)
	txn := db.getTxn()
	if txn.op.Status() == txn2.TxnStatus_Aborted {
		return nil, moerr.NewTxnClosedNoCtx(txn.op.Txn().ID)
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	key := genTableKey(accountId, name, db.databaseId)
	// check the table is deleted or not
	if _, exist := db.getTxn().deletedTableMap.Load(key); exist {
		return nil, moerr.NewParseError(ctx, "table %q does not exist", name)
	}

	p := db.getTxn().proc
	if proc != nil {
		p = proc.(*process.Process)
	}

	rel := db.getTxn().getCachedTable(ctx, key)
	if rel != nil {
		rel.proc.Store(p)
		return rel, nil
	}

	// get relation from the txn created tables cache: created by this txn
	if v, ok := db.getTxn().createMap.Load(key); ok {
		v.(*txnTable).proc.Store(p)
		return v.(*txnTable), nil
	}

	// special tables
	if db.databaseName == catalog.MO_CATALOG {
		switch name {
		case catalog.MO_DATABASE:
			id := uint64(catalog.MO_DATABASE_ID)
			defs := catalog.MoDatabaseTableDefs
			return db.openSysTable(p, id, name, defs), nil
		case catalog.MO_TABLES:
			id := uint64(catalog.MO_TABLES_ID)
			defs := catalog.MoTablesTableDefs
			return db.openSysTable(p, id, name, defs), nil
		case catalog.MO_COLUMNS:
			id := uint64(catalog.MO_COLUMNS_ID)
			defs := catalog.MoColumnsTableDefs
			return db.openSysTable(p, id, name, defs), nil
		}
	}
	item := &cache.TableItem{
		Name:       name,
		DatabaseId: db.databaseId,
		AccountId:  accountId,
		Ts:         db.op.SnapshotTS(),
	}
	var catache *cache.CatalogCache
	if !db.op.IsSnapOp() {
		catache = db.getTxn().engine.getLatestCatalogCache()
	} else {
		catache, err = db.getTxn().engine.getOrCreateSnapCatalogCache(
			ctx,
			types.TimestampToTS(db.op.SnapshotTS()))
		if err != nil {
			return nil, err
		}
	}
	if ok := catache.GetTable(item); !ok {
		logutil.Debugf("txnDatabase.Relation table %q(acc %d db %d) does not exist",
			name,
			accountId,
			db.databaseId)
		if strings.Contains(name, "_copy_") {
			stackInfo := debug.Stack()
			logutil.Error(moerr.NewParseError(context.Background(), "table %q does not exists", name).Error(), zap.String("Stack Trace", string(stackInfo)))
		}
		return nil, moerr.NewParseError(ctx, "table %q does not exist", name)
	}

	tbl := &txnTable{
		db:            db,
		accountId:     item.AccountId,
		tableId:       item.Id,
		version:       item.Version,
		tableName:     item.Name,
		defs:          item.Defs,
		tableDef:      item.TableDef,
		primaryIdx:    item.PrimaryIdx,
		primarySeqnum: item.PrimarySeqnum,
		clusterByIdx:  item.ClusterByIdx,
		relKind:       item.Kind,
		viewdef:       item.ViewDef,
		comment:       item.Comment,
		partitioned:   item.Partitioned,
		partition:     item.Partition,
		createSql:     item.CreateSql,
		constraint:    item.Constraint,
		rowid:         item.Rowid,
		rowids:        item.Rowids,
		lastTS:        txn.op.SnapshotTS(),
	}
	tbl.proc.Store(p)

	db.getTxn().tableCache.tableMap.Store(key, tbl)
	return tbl, nil
}

func (db *txnDatabase) Delete(ctx context.Context, name string) error {
	var id uint64
	var rowid types.Rowid
	var rowids []types.Rowid
	if db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("delete table in snapshot transaction")
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	k := genTableKey(accountId, name, db.databaseId)
	if v, ok := db.getTxn().createMap.Load(k); ok {
		db.getTxn().createMap.Delete(k)
		table := v.(*txnTable)
		id = table.tableId
		rowid = table.rowid
		rowids = table.rowids
		/*
			Even if the created table in the createMap, there is an
			INSERT entry in the CN workspace. We need add a DELETE
			entry in the CN workspace to tell the TN to delete the
			table.
			CORNER CASE
			begin;
			create table t1;
			drop table t1;
			commit;
			If we do not add DELETE entry in workspace, there is
			a table t1 there after commit.
		*/
	} else if v, ok := db.getTxn().tableCache.tableMap.Load(k); ok {
		table := v.(*txnTable)
		id = table.tableId
		db.getTxn().tableCache.tableMap.Delete(k)
		rowid = table.rowid
		rowids = table.rowids
	} else {
		item := &cache.TableItem{
			Name:       name,
			DatabaseId: db.databaseId,
			AccountId:  accountId,
			Ts:         db.op.SnapshotTS(),
		}
		if ok := db.getTxn().engine.getLatestCatalogCache().GetTable(item); !ok {
			return moerr.GetOkExpectedEOB()
		}
		id = item.Id
		rowid = item.Rowid
		rowids = item.Rowids
	}
	bat, err := genDropTableTuple(rowid, id, db.databaseId,
		name, db.databaseName, db.getTxn().proc.Mp())
	if err != nil {
		return err
	}

	for _, store := range db.getTxn().tnStores {
		if err := db.getTxn().WriteBatch(
			DELETE, 0, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, bat, store, -1, false, false); err != nil {
			bat.Clean(db.getTxn().proc.Mp())
			return err
		}
	}

	//Add writeBatch(delete,mo_columns) to filter table in mo_columns.
	//Every row in writeBatch(delete,mo_columns) needs rowid
	for _, rid := range rowids {
		bat, err = genDropColumnTuple(rid, db.getTxn().proc.Mp())
		if err != nil {
			return err
		}
		for _, store := range db.getTxn().tnStores {
			if err = db.getTxn().WriteBatch(
				DELETE, 0, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID,
				catalog.MO_CATALOG, catalog.MO_COLUMNS, bat, store, -1, false, false); err != nil {
				bat.Clean(db.getTxn().proc.Mp())
				return err
			}
		}
	}

	db.getTxn().deletedTableMap.Store(k, id)
	return nil
}

func (db *txnDatabase) Truncate(ctx context.Context, name string) (uint64, error) {
	var oldId uint64
	var rowid types.Rowid
	var v any
	var ok bool
	if db.op.IsSnapOp() {
		return 0, moerr.NewInternalErrorNoCtx("truncate table in snapshot transaction")
	}
	newId, err := db.getTxn().allocateID(ctx)
	if err != nil {
		return 0, err
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return 0, err
	}
	k := genTableKey(accountId, name, db.databaseId)
	v, ok = db.getTxn().createMap.Load(k)
	if !ok {
		v, ok = db.getTxn().tableCache.tableMap.Load(k)
	}

	if ok {
		txnTable := v.(*txnTable)
		oldId = txnTable.tableId
		txnTable.reset(newId)
		rowid = txnTable.rowid
	} else {
		item := &cache.TableItem{
			Name:       name,
			DatabaseId: db.databaseId,
			AccountId:  accountId,
			Ts:         db.op.SnapshotTS(),
		}
		if ok := db.getTxn().engine.getLatestCatalogCache().GetTable(item); !ok {
			return 0, moerr.GetOkExpectedEOB()
		}
		oldId = item.Id
		rowid = item.Rowid
	}
	bat, err := genTruncateTableTuple(rowid, newId, db.databaseId,
		genMetaTableName(oldId)+name, db.databaseName, db.getTxn().proc.Mp())
	if err != nil {
		return 0, err
	}
	for _, store := range db.getTxn().tnStores {
		if err := db.getTxn().WriteBatch(DELETE, 0, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, bat, store, -1, false, true); err != nil {
			bat.Clean(db.getTxn().proc.Mp())
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
	if db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("create table in snapshot transaction")
	}
	accountId, userId, roleId, err := getAccessInfo(ctx)
	if err != nil {
		return err
	}
	tableId, err := db.getTxn().allocateID(ctx)
	if err != nil {
		return err
	}
	tbl := new(txnTable)
	tbl.accountId = accountId
	tbl.rowid = types.DecodeFixed[types.Rowid](types.EncodeSlice([]uint64{tableId}))
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
				tbl.partitioned = defVal.Partitioned
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
		bat, err := genCreateTableTuple(
			tbl, sql, accountId, userId, roleId, name,
			tableId, db.databaseId, db.databaseName,
			tbl.rowid, true, db.getTxn().proc.Mp())
		if err != nil {
			return err
		}
		for _, store := range db.getTxn().tnStores {
			if err := db.getTxn().WriteBatch(INSERT, 0, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
				catalog.MO_CATALOG, catalog.MO_TABLES, bat, store, -1, true, false); err != nil {
				bat.Clean(db.getTxn().proc.Mp())
				return err
			}
		}
	}
	tbl.primaryIdx = -1
	tbl.primarySeqnum = -1
	tbl.clusterByIdx = -1
	tbl.rowids = make([]types.Rowid, len(cols))
	for i, col := range cols {
		tbl.rowids[i] = db.getTxn().genRowId()
		bat, err := genCreateColumnTuple(col, tbl.rowids[i], true,
			db.getTxn().proc.Mp())
		if err != nil {
			return err
		}
		for _, store := range db.getTxn().tnStores {
			if err := db.getTxn().WriteBatch(
				INSERT, 0, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID,
				catalog.MO_CATALOG, catalog.MO_COLUMNS, bat, store, -1, true, false); err != nil {
				bat.Clean(db.getTxn().proc.Mp())
				return err
			}
		}
		if col.constraintType == catalog.SystemColPKConstraint {
			tbl.primaryIdx = i
			tbl.primarySeqnum = i
		}
		if col.isClusterBy == 1 {
			tbl.clusterByIdx = i
		}
	}
	tbl.db = db
	tbl.defs = defs
	tbl.tableName = name
	tbl.tableId = tableId
	tbl.GetTableDef(ctx)
	key := genTableKey(accountId, name, db.databaseId)
	db.getTxn().addCreateTable(key, tbl)
	//CORNER CASE
	//begin;
	//create table t1(a int);
	//drop table t1; //t1 is in deleteTableMap now.
	//select * from t1; //t1 does not exist.
	//create table t1(a int); //t1 does not exist. t1 can be created again.
	//	t1 needs be deleted from deleteTableMap
	db.getTxn().deletedTableMap.Delete(key)
	return nil
}

func (db *txnDatabase) openSysTable(p *process.Process, id uint64, name string,
	defs []engine.TableDef) engine.Relation {
	tbl := &txnTable{
		//AccountID for mo_tables, mo_database, mo_columns is always 0.
		accountId:     0,
		db:            db,
		tableId:       id,
		tableName:     name,
		defs:          defs,
		primaryIdx:    0,
		primarySeqnum: db.getEng().getLatestCatalogCache().GetTableById(db.databaseId, id).PrimarySeqnum,
		clusterByIdx:  -1,
	}
	switch name {
	case catalog.MO_DATABASE:
		tbl.constraint = catalog.MoDatabaseConstraint
	case catalog.MO_TABLES:
		tbl.constraint = catalog.MoTableConstraint
	case catalog.MO_COLUMNS:
		tbl.constraint = catalog.MoColumnConstraint
	}
	tbl.GetTableDef(context.TODO())
	tbl.proc.Store(p)
	return tbl
}
