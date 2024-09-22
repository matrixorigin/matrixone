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
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	txn2 "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

var _ engine.Database = new(txnDatabase)

func (db *txnDatabase) getTxn() *Transaction {
	return db.op.GetWorkspace().(*Transaction)
}

func (db *txnDatabase) getEng() *Engine {
	return db.op.GetWorkspace().(*Transaction).engine
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

func (db *txnDatabase) Relations(ctx context.Context) ([]string, error) {
	aid, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(catalog.MoTablesInDBQueryFormat, aid, db.databaseName)

	res, err := execReadSql(ctx, db.op, sql, true)
	if err != nil {
		return nil, err
	}

	defer res.Close()

	var rels []string
	for _, b := range res.Batches {
		for i, v := 0, b.Vecs[0]; i < v.Length(); i++ {
			rels = append(rels, v.GetStringAt(i))
		}
	}
	return rels, nil
}

func (db *txnDatabase) Relation(ctx context.Context, name string, proc any) (engine.Relation, error) {
	common.DoIfDebugEnabled(func() {
		logutil.Debug(
			"Transaction.Relation",
			zap.String("txn", db.op.Txn().DebugString()),
			zap.String("name", name),
		)
	})
	txn := db.getTxn()
	if txn.op.Status() == txn2.TxnStatus_Aborted {
		return nil, moerr.NewTxnClosedNoCtx(txn.op.Txn().ID)
	}

	p := txn.proc
	if proc != nil {
		p = proc.(*process.Process)
	}

	// special tables
	if db.databaseName == catalog.MO_CATALOG {
		switch name {
		case catalog.MO_DATABASE:
			id := uint64(catalog.MO_DATABASE_ID)
			defs := catalog.GetDefines(p.GetService()).MoDatabaseTableDefs
			return db.openSysTable(p, id, name, defs), nil
		case catalog.MO_TABLES:
			id := uint64(catalog.MO_TABLES_ID)
			defs := catalog.GetDefines(p.GetService()).MoTablesTableDefs
			return db.openSysTable(p, id, name, defs), nil
		case catalog.MO_COLUMNS:
			id := uint64(catalog.MO_COLUMNS_ID)
			defs := catalog.GetDefines(p.GetService()).MoColumnsTableDefs
			return db.openSysTable(p, id, name, defs), nil
		}
	}

	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	key := genTableKey(accountId, name, db.databaseId, db.databaseName)

	// check the table is deleted or not
	if txn.tableOps.existAndDeleted(key) {
		return nil, moerr.NewParseErrorf(ctx, "table %q does not exist", name)
	}

	// get relation from the txn created tables cache: created by this txn
	if v := txn.tableOps.existAndActive(key); v != nil {
		v.proc.Store(p)
		return v, nil
	}

	rel := txn.getCachedTable(ctx, key)
	if rel != nil {
		rel.origin.proc.Store(p)
		return rel, nil
	}

	item, err := db.getTableItem(
		ctx,
		accountId,
		name,
		txn.engine,
	)
	if err != nil {
		return nil, err
	}

	tbl, err := newTxnTable(
		db,
		item,
		p,
		shardservice.GetService(p.GetService()),
		txn.engine,
	)
	if err != nil {
		return nil, err
	}

	db.getTxn().tableCache.Store(key, tbl)
	return tbl, nil
}

func (db *txnDatabase) Delete(ctx context.Context, name string) error {
	_, err := db.deleteTable(ctx, name, false, false)
	return err
}

// deleteTable deletes a table.
//
// 1. forAlter being true means that the table is deleted due to alter table, as part of udpate.
// Drop table or Truncate table will set forAlter as false
//
// 2. useAlterNote means that the batch sent to TN will be just used
// to insert into or delete from mo_tables and mo_columns,
// rather than trigger an actual create or delete operation for table.
func (db *txnDatabase) deleteTable(ctx context.Context, name string, forAlter bool, useAlterNote bool) ([]engine.TableDef, error) {
	var id uint64
	var rowid types.Rowid
	var rowids []types.Rowid
	var colPKs [][]byte
	var defs []engine.TableDef
	var packer *types.Packer
	if db.op.IsSnapOp() {
		return nil, moerr.NewInternalErrorNoCtx("delete table in snapshot transaction")
	}

	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	txn := db.getTxn()
	putter := txn.engine.packerPool.Get(&packer)
	defer putter.Put()

	// 1. Get id and columns infortmation to prepate delete batch
	rel, err := db.Relation(ctx, name, nil)
	if err != nil {
		return nil, err
	}

	var toDelTbl *txnTable
	switch v := rel.(type) {
	case *txnTable:
		toDelTbl = v
	case *txnTableDelegate:
		toDelTbl = v.origin
	default:
		panic("unknown relation type")
	}

	defs = toDelTbl.defs
	id = toDelTbl.tableId
	colPKs = getColPks(accountId, db.databaseName, name, toDelTbl.tableDef.Cols, packer)

	// 1.1 table rowid
	sql := fmt.Sprintf(catalog.MoTablesRowidQueryFormat, accountId, db.databaseName, name)
	res, err := execReadSql(ctx, db.op, sql, true)
	if err != nil {
		return nil, err
	}
	if len(res.Batches) != 1 || res.Batches[0].Vecs[0].Length() != 1 {
		logutil.Error("FIND_TABLE deleteTableError",
			zap.String("bat", stringifySlice(res.Batches, func(a any) string {
				bat := a.(*batch.Batch)
				return common.MoBatchToString(bat, 10)
			})),
			zap.String("sql", sql),
			zap.Uint64("did", db.databaseId),
			zap.Uint64("tid", rel.GetTableID(ctx)),
			zap.String("workspace", db.getTxn().PPString()))
		panic("delete table failed: query failed")
	}
	rowid = vector.GetFixedAtNoTypeCheck[types.Rowid](res.Batches[0].Vecs[0], 0)

	// 1.2 table column rowids
	res, err = execReadSql(ctx, db.op, fmt.Sprintf(catalog.MoColumnsRowidsQueryFormat, accountId, db.databaseName, name, id), true)
	if err != nil {
		return nil, err
	}
	for _, b := range res.Batches {
		for i, v := 0, b.Vecs[0]; i < v.Length(); i++ {
			rowids = append(rowids, vector.GetFixedAtNoTypeCheck[types.Rowid](v, i))
		}
	}

	if len(rowids) != len(colPKs) {
		panic(fmt.Sprintf("delete table failed %v, %v", len(rowids), len(colPKs)))
	}

	{ // 2. delete the row from mo_tables

		bat, err := catalog.GenDropTableTuple(rowid, accountId, id, db.databaseId,
			name, db.databaseName, txn.proc.Mp(), packer)
		if err != nil {
			return nil, err
		}
		if bat = txn.deleteBatch(bat, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID); bat.RowCount() > 0 {
			// the deleted table is not created by this txn
			note := noteForDrop(id, name)
			if useAlterNote {
				note = noteForAlterDel(id, name)
			}
			if _, err := txn.WriteBatch(
				DELETE, note, catalog.System_Account, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
				catalog.MO_CATALOG, catalog.MO_TABLES, bat, txn.tnStores[0]); err != nil {
				bat.Clean(txn.proc.Mp())
				return nil, err
			}
		}
		if !forAlter {
			// An insert batch for mo_tables is cancelled, the dml on this table should be eliminated?
			// The answer for forAlter as true is NO, because later a table with the same tableId will be created.
			// The answer for forAlter as false is YES, because the table is really deleted, which is triggered by delete & truncate
			txn.Lock()
			txn.tablesInVain[id] = txn.statementID
			txn.Unlock()
		}
	}

	{ // 3. delete rows from mo_columns
		bat, err := catalog.GenDropColumnTuples(rowids, colPKs, txn.proc.Mp())
		if err != nil {
			return nil, err
		}

		if bat = txn.deleteBatch(bat, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID); bat.RowCount() > 0 {
			note := noteForDrop(id, name)
			if useAlterNote {
				note = noteForAlterDel(id, name)
			}
			if _, err = txn.WriteBatch(
				DELETE, note, catalog.System_Account, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID,
				catalog.MO_CATALOG, catalog.MO_COLUMNS, bat, txn.tnStores[0]); err != nil {
				bat.Clean(txn.proc.Mp())
				return nil, err
			}
		}
	}

	// 4. handle map cache
	key := genTableKey(accountId, name, db.databaseId, db.databaseName)
	txn.tableCache.Delete(key)
	txn.tableOps.addDeleteTable(key, txn.statementID, id)
	return defs, nil
}

func (db *txnDatabase) Truncate(ctx context.Context, name string) (uint64, error) {
	if db.op.IsSnapOp() {
		return 0, moerr.NewInternalErrorNoCtx("truncate table in snapshot transaction")
	}
	newId, err := db.getTxn().allocateID(ctx)
	if err != nil {
		return 0, err
	}

	defs, err := db.deleteTable(ctx, name, false, false)
	if err != nil {
		return 0, err
	}

	if err := db.createWithID(ctx, name, newId, defs, false); err != nil {
		return 0, err
	}

	return newId, nil
}

func (db *txnDatabase) Create(ctx context.Context, name string, defs []engine.TableDef) error {
	if db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("create table in snapshot transaction")
	}
	tableId, err := db.getTxn().allocateID(ctx)
	if err != nil {
		return err
	}
	return db.createWithID(ctx, name, tableId, defs, false)
}

func (db *txnDatabase) createWithID(
	ctx context.Context,
	name string, tableId uint64, defs []engine.TableDef, useAlterNote bool,
) error {
	if db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("create table in snapshot transaction")
	}
	accountId, userId, roleId, err := getAccessInfo(ctx)
	if err != nil {
		return err
	}
	txn := db.getTxn()
	m := txn.proc.Mp()

	// 1. inspect and **modify** defs, and construct columns
	cols, err := catalog.GenColumnsFromDefs(accountId, name, db.databaseName, tableId, db.databaseId, defs)
	if err != nil {
		return err
	}
	tbl := new(txnTable)
	tbl.eng = txn.engine

	{ // prepare table information
		// 2.1 prepare basic table information
		tbl.db = db
		tbl.tableName = name
		tbl.tableId = tableId
		tbl.accountId = accountId
		tbl.extraInfo = &api.SchemaExtra{}
		for _, def := range defs {
			switch defVal := def.(type) {
			case *engine.PropertiesDef:
				for _, property := range defVal.Properties {
					switch strings.ToLower(property.Key) {
					case catalog.SystemRelAttr_Comment:
						tbl.comment = property.Value
					case catalog.SystemRelAttr_Kind:
						tbl.relKind = property.Value
					case catalog.SystemRelAttr_CreateSQL:
						tbl.createSql = property.Value
					case catalog.PropSchemaExtra:
						tbl.extraInfo = api.MustUnmarshalTblExtra([]byte(property.Value))
					default:
					}
				}
			case *engine.ViewDef:
				tbl.viewdef = defVal.View
			case *engine.CommentDef:
				tbl.comment = defVal.Comment
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
		tbl.extraInfo.NextColSeqnum = uint32(len(cols) - 1 /*rowid doesn't occupy seqnum*/)
		if tbl.extraInfo.BlockMaxRows == 0 {
			tbl.extraInfo.BlockMaxRows = options.DefaultBlockMaxRows
		}
		if tbl.extraInfo.ObjectMaxBlocks == 0 {
			tbl.extraInfo.ObjectMaxBlocks = uint32(options.DefaultBlocksPerObject)
		}
		// 2.2 prepare columns related information
		tbl.primaryIdx = -1
		tbl.primarySeqnum = -1
		tbl.clusterByIdx = -1
		for i, col := range cols {
			if col.ConstraintType == catalog.SystemColPKConstraint {
				tbl.primaryIdx = i
				tbl.primarySeqnum = i
			}
			if col.IsClusterBy == 1 {
				tbl.clusterByIdx = i
			}
		}

		// 2.3 prepare holistic table def
		tbl.defs = defs
		tbl.GetTableDef(ctx) // generate tbl.tableDef
	}

	var packer *types.Packer
	put := db.getEng().packerPool.Get(&packer)
	defer put.Put()
	{ // 3. Write create table batch, update tbl.rowiod

		db := tbl.db
		arg := catalog.Table{
			AccountId:     accountId,
			UserId:        userId,
			RoleId:        roleId,
			DatabaseId:    db.databaseId,
			DatabaseName:  db.databaseName,
			TableName:     tbl.tableName,
			TableId:       tbl.tableId,
			Kind:          tbl.relKind,
			Comment:       tbl.comment,
			CreateSql:     tbl.createSql,
			Partitioned:   tbl.partitioned,
			PartitionInfo: tbl.partition,
			Viewdef:       tbl.viewdef,
			Constraint:    tbl.constraint,
			Version:       tbl.version,
			ExtraInfo:     api.MustMarshalTblExtra(tbl.extraInfo),
		}
		bat, err := catalog.GenCreateTableTuple(arg, m, packer)
		if err != nil {
			return err
		}
		note := noteForCreate(tbl.tableId, tbl.tableName)
		if useAlterNote {
			note = noteForAlterIns(tbl.tableId, tbl.tableName)
		}
		_, err = txn.WriteBatch(INSERT, note, catalog.System_Account, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, bat, txn.tnStores[0])
		if err != nil {
			bat.Clean(m)
			return err
		}
	}

	{ // 4. Write create column batch
		bat, err := catalog.GenCreateColumnTuples(cols, m, packer)
		if err != nil {
			return err
		}
		note := noteForCreate(tbl.tableId, tbl.tableName)
		if useAlterNote {
			note = noteForAlterIns(tbl.tableId, tbl.tableName)
		}
		_, err = txn.WriteBatch(
			INSERT, note, catalog.System_Account, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG, catalog.MO_COLUMNS, bat, txn.tnStores[0])
		if err != nil {
			bat.Clean(m)
			return err
		}
	}

	// 5. handle map cache
	key := genTableKey(accountId, name, db.databaseId, db.databaseName)
	txn.tableOps.addCreateTable(key, txn.statementID, tbl)
	return nil
}

func (db *txnDatabase) openSysTable(
	p *process.Process,
	id uint64,
	name string,
	defs []engine.TableDef,
) engine.Relation {
	item := &cache.TableItem{
		AccountId:  catalog.System_Account,
		DatabaseId: catalog.MO_CATALOG_ID,
		Name:       name,
		Ts:         db.op.SnapshotTS(),
	}
	// it is always safe to use latest cache to open system table
	found := db.getEng().GetLatestCatalogCache().GetTable(item)
	if !found {
		panic("can't find system table")
	}
	tbl := &txnTable{
		//AccountID for mo_tables, mo_database, mo_columns is always 0.
		accountId:     0,
		db:            db,
		tableId:       id,
		tableName:     name,
		defs:          defs,
		primaryIdx:    item.PrimaryIdx,
		primarySeqnum: item.PrimarySeqnum,
		clusterByIdx:  -1,
		eng:           db.getTxn().engine,
	}
	switch name {
	case catalog.MO_DATABASE:
		tbl.constraint = catalog.GetDefines(p.GetService()).MoDatabaseConstraint
	case catalog.MO_TABLES:
		tbl.constraint = catalog.GetDefines(p.GetService()).MoTableConstraint
	case catalog.MO_COLUMNS:
		tbl.constraint = catalog.GetDefines(p.GetService()).MoColumnConstraint
	}
	tbl.GetTableDef(p.Ctx)
	tbl.proc.Store(p)
	return tbl
}

func (db *txnDatabase) loadTableFromStorage(
	ctx context.Context,
	accountID uint32,
	name string,
) (tableitem *cache.TableItem, err error) {
	now := time.Now()
	defer func() {
		if time.Since(now) > time.Second {
			logutil.Info("FIND_TABLE slow loadTableFromStorage",
				zap.Duration("cost", time.Since(now)),
				zap.String("table", name),
				zap.Uint32("accountID", accountID),
				zap.String("database", db.databaseName),
				zap.Uint64("databaseId", db.databaseId),
				zap.Error(err))
		}
	}()
	var (
		ts    = types.TimestampToTS(db.op.SnapshotTS())
		tblid uint64
	)
	// query table
	{
		tblSql := fmt.Sprintf(catalog.MoTablesAllQueryFormat, accountID, db.databaseName, name)
		var res executor.Result
		res, err = execReadSql(ctx, db.op, tblSql, true)
		if err != nil {
			return
		}
		defer res.Close()
		if len(res.Batches) != 1 {
			return
		}
		if row := res.Batches[0].RowCount(); row != 1 {
			panic(fmt.Sprintf("FIND_TABLE loadTableFromStorage failed: table result row cnt: %v, sql : %s", row, tblSql))
		}
		bat := res.Batches[0]

		if err := fillTsVecForSysTableQueryBatch(bat, ts, res.Mp); err != nil {
			return nil, err
		}
		ids := vector.MustFixedColWithTypeCheck[uint64](bat.GetVector(catalog.MO_TABLES_REL_ID_IDX + cache.MO_OFF))
		tblid = ids[0]
		cache.ParseTablesBatchAnd(bat, func(ti *cache.TableItem) {
			tableitem = ti
		})
	}

	{
		// fresh columns
		colSql := fmt.Sprintf(catalog.MoColumnsAllQueryFormat, accountID, db.databaseName, name, tblid)
		var res executor.Result
		res, err = execReadSql(ctx, db.op, colSql, true)
		if err != nil {
			return
		}
		defer res.Close()
		if len(res.Batches) == 0 {
			err = moerr.NewParseErrorf(ctx, "FIND_TABLE columns of table %q does not exist, cnt: %v, sql:%v", name, len(res.Batches), colSql)
			return
		}
		bat := res.Batches[0]
		for _, b := range res.Batches[1:] {
			bat, err = bat.Append(ctx, res.Mp, b)
			if err != nil {
				return
			}
		}
		if err := fillTsVecForSysTableQueryBatch(bat, ts, res.Mp); err != nil {
			return nil, err
		}
		cache.ParseColumnsBatchAnd(bat, func(m map[cache.TableItemKey]cache.Columns) {
			if len(m) != 1 {
				panic(fmt.Sprintf("FIND_TABLE loadTableFromStorage failed: columns touch %d tables", len(m)))
			}
			for _, v := range m {
				cache.InitTableItemWithColumns(tableitem, v)
			}
		})
	}
	return tableitem, nil
}

func (db *txnDatabase) getTableItem(
	ctx context.Context,
	accountID uint32,
	name string,
	engine *Engine,
) (cache.TableItem, error) {
	item := cache.TableItem{
		Name:       name,
		DatabaseId: db.databaseId,
		AccountId:  accountID,
		Ts:         db.op.SnapshotTS(),
	}
	var err error
	c := engine.GetLatestCatalogCache()
	if ok := c.GetTable(&item); !ok {
		var tableitem *cache.TableItem
		if !c.CanServe(types.TimestampToTS(db.op.SnapshotTS())) {
			if tableitem, err = db.loadTableFromStorage(ctx, accountID, name); err != nil {
				return cache.TableItem{}, err
			}
		}
		if tableitem == nil {
			if strings.Contains(name, "_copy_") {
				stackInfo := debug.Stack()
				logutil.Error(moerr.NewParseErrorf(context.Background(), "table %q does not exists", name).Error(),
					zap.String("Stack Trace", string(stackInfo)))
			}
			return cache.TableItem{}, moerr.NewParseErrorf(ctx, "table %q does not exist", name)
		}
		return *tableitem, nil
	}
	return item, nil
}
