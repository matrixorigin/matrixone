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
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

func (db *txnDatabase) relation(ctx context.Context, name string, proc any) (engine.Relation, error) {
	common.DoIfDebugEnabled(func() {
		logutil.Debug(
			"Transaction.Relation",
			zap.String("txn", db.op.Txn().DebugString()),
			zap.String("name", name),
		)
	})
	txn := db.getTxn()
	if _, err := txnIsValid(txn.op); err != nil {
		return nil, err
	}

	p := txn.proc
	if proc != nil {
		p = proc.(*process.Process)
	}

	openSys := db.databaseId == catalog.MO_CATALOG_ID && catalog.IsSystemTableByName(name)
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	if openSys {
		accountId = 0
	}

	key := genTableKey(accountId, name, db.databaseId, db.databaseName)

	// check the table is deleted or not
	if !openSys && txn.tableOps.existAndDeleted(key) {
		logutil.Info("[relation] deleted in txn", zap.String("table", name))
		return nil, nil
	}

	// get relation from the txn created tables cache: created by this txn
	if !openSys {
		if v := txn.tableOps.existAndActive(key); v != nil {
			v.proc.Store(p)
			return v, nil
		}
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
	if item == nil {
		return nil, nil
	}

	tbl, err := newTxnTable(
		ctx,
		db,
		*item,
	)
	if err != nil {
		return nil, err
	}

	db.getTxn().tableCache.Store(key, tbl)
	return tbl, nil
}

func (db *txnDatabase) Relation(ctx context.Context, name string, proc any) (engine.Relation, error) {
	rel, err := db.relation(ctx, name, proc)
	if err != nil {
		return nil, err
	}
	if rel == nil {
		err = moerr.NewNoSuchTable(ctx, db.databaseName, name)
		return nil, err
	}
	return rel, nil
}

func (db *txnDatabase) RelationExists(ctx context.Context, name string, proc any) (bool, error) {
	rel, err := db.relation(ctx, name, proc)
	if err != nil {
		return false, err
	}
	return rel != nil, nil
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

	rmFault := func() {}

	if objectio.Debug19524Injected() {
		if rmFault, err = objectio.InjectLogRanges(
			ctx,
			catalog.MO_TABLES,
		); err != nil {
			return nil, err
		}
	}
	res, err := execReadSql(ctx, db.op, sql, true)
	rmFault()
	if err != nil {
		return nil, err
	}
	if len(res.Batches) != 1 || res.Batches[0].Vecs[0].Length() != 1 {
		logutil.Error(
			"FIND_TABLE deleteTableError",
			zap.String("bat", stringifySlice(res.Batches, func(a any) string {
				bat := a.(*batch.Batch)
				return common.MoBatchToString(bat, 10)
			})),
			zap.String("sql", sql),
			zap.String("txn", db.op.Txn().DebugString()),
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
		logutil.Error(
			"FIND_TABLE deleteTableError",
			zap.String("bat", stringifySlice(rowids, func(a any) string {
				r := a.(types.Rowid)
				return r.ShortStringEx()
			})),
			zap.String("txn", db.op.Txn().DebugString()),
			zap.Uint64("did", db.databaseId),
			zap.Uint64("tid", rel.GetTableID(ctx)),
			zap.String("workspace", db.getTxn().PPString()))
		panic(fmt.Sprintf("delete table %v-%v failed %v, %v", rel.GetTableID(ctx), rel.GetTableName(), len(rowids), len(colPKs)))
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

			// sync logical_id index table after mo_tables delete (only for non-alter operations)
			if !forAlter {
				if err := db.syncLogicalIdIndexDelete(ctx, toDelTbl.logicalId); err != nil {
					return nil, err
				}
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
	return 0, moerr.NewNYINoCtx("truncate table is not implemented")

}

func (db *txnDatabase) Create(ctx context.Context, name string, defs []engine.TableDef) error {
	if db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("create table in snapshot transaction")
	}
	txn := db.getTxn()

	var tableId uint64
	var err error
	value := ctx.Value(defines.TableIDKey{})
	if value != nil {
		tableId = value.(uint64)
	} else {
		tableId, err = txn.allocateID(ctx)
		if err != nil {
			return err
		}
	}
	txn.tableOps.addCreatedInTxn(tableId, txn.statementID)
	return db.createWithID(ctx, name, tableId, defs, false, nil)
}

func (db *txnDatabase) createWithID(
	ctx context.Context,
	name string,
	tableId uint64,
	defs []engine.TableDef,
	useAlterNote bool,
	extra *api.SchemaExtra,
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
		tbl.extraInfo = extra

		if tbl.extraInfo == nil {
			tbl.extraInfo = &api.SchemaExtra{}
		}

		for _, def := range defs {
			switch defVal := def.(type) {
			case *engine.PropertiesDef:
				for _, property := range defVal.Properties {
					if property.ValueFactory != nil {
						property.Value = property.ValueFactory()
					}

					switch strings.ToLower(property.Key) {
					case catalog.SystemRelAttr_Comment:
						tbl.comment = property.Value
					case catalog.SystemRelAttr_Kind:
						tbl.relKind = property.Value
					case catalog.SystemRelAttr_CreateSQL:
						tbl.createSql = property.Value
					case catalog.PropSchemaExtra:
						if extra == nil {
							tbl.extraInfo = api.MustUnmarshalTblExtra([]byte(property.Value))
						}
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
			case *engine.VersionDef:
				tbl.version = defVal.Version
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
	var logicalId uint64 = tbl.tableId
	// Check if this is an UPDATE operation (ALTER/TRUNCATE) by looking for LogicalIdKey in context
	// This is checked once and reused later
	logicalIdFromCtx := ctx.Value(defines.LogicalIdKey{})
	isUpdate := logicalIdFromCtx != nil && !strings.HasPrefix(name, catalog.IndexTableNamePrefix)
	if isUpdate {
		logicalId = logicalIdFromCtx.(uint64)
	}
	tbl.logicalId = logicalId
	{ // 3. Write create table batch, update tbl.rowiod

		db := tbl.db
		if features.IsPartition(tbl.extraInfo.FeatureFlag) {
			tbl.relKind = catalog.SystemPartitionRel
		}
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
			LogicalId:     logicalId,
		}
		bat, err := catalog.GenCreateTableTuple(arg, m, packer)
		if err != nil {
			return err
		}

		compositePk := bat.Vecs[catalog.MO_TABLES_CPKEY_IDX].GetBytesAt(0)

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

		if err = db.syncLogicalIdIndexInsert(ctx, logicalId, compositePk, isUpdate); err != nil {
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
) (*cache.TableItem, error) {
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
			logutil.Info("FIND_TABLE loadTableFromStorage", zap.String("table", name), zap.Uint32("accountID", accountID), zap.String("txn", db.op.Txn().DebugString()), zap.String("cacheTS", c.GetStartTS().ToString()))
			if tableitem, err = db.loadTableFromStorage(ctx, accountID, name); err != nil {
				return nil, err
			}
		}
		if tableitem == nil {
			return nil, nil
		}
		return tableitem, nil
	}
	return &item, nil
}

// syncLogicalIdIndexInsert synchronizes the logical_id index table for INSERT/UPDATE operations
func (db *txnDatabase) syncLogicalIdIndexInsert(
	ctx context.Context,
	logicalId uint64,
	compositePk []byte,
	isUpdate bool,
) error {
	txn := db.getTxn()
	m := txn.proc.Mp()

	// For UPDATE operations (ALTER/TRUNCATE), we need to delete the old record first
	if isUpdate {
		if err := db.syncLogicalIdIndexDelete(ctx, logicalId); err != nil {
			return err
		}
	}

	// Generate and execute INSERT batch for index table
	bat, err := catalog.GenLogicalIdIndexInsertBatch(logicalId, compositePk, m)
	if err != nil {
		return err
	}
	// Note: Do NOT clean bat here. WriteBatch stores the batch reference in txn.writes,
	// and the batch lifecycle is managed by the transaction (cleaned on commit/rollback).

	note := fmt.Sprintf("sync logical_id index insert %d", logicalId)
	_, err = txn.WriteBatch(
		INSERT, note, catalog.System_Account, catalog.MO_CATALOG_ID, catalog.MO_TABLES_LOGICAL_ID_INDEX_ID,
		catalog.MO_CATALOG, catalog.MO_TABLES_LOGICAL_ID_INDEX_TABLE_NAME, bat, txn.tnStores[0])
	if err != nil {
		bat.Clean(m)
	}
	return err
}

// syncLogicalIdIndexDelete synchronizes the logical_id index table for DELETE operations
func (db *txnDatabase) syncLogicalIdIndexDelete(
	ctx context.Context,
	logicalId uint64,
) error {
	txn := db.getTxn()
	m := txn.proc.Mp()

	// Query the rowid from index table
	sql := fmt.Sprintf(catalog.LogicalIdIndexRowidQueryFormat,
		catalog.MO_CATALOG, catalog.MO_TABLES_LOGICAL_ID_INDEX_TABLE_NAME,
		catalog.IndexTableIndexColName, logicalId)

	res, err := execReadSql(ctx, db.op, sql, true)
	if err != nil {
		logutil.Infof("LIDX-DEBUG syncLogicalIdIndexDelete query error: logicalId=%d, err=%v", logicalId, err)
		return err
	}
	defer res.Close()

	if len(res.Batches) == 0 || res.Batches[0].Vecs[0].Length() == 0 {
		return nil
	}

	rowid := vector.GetFixedAtNoTypeCheck[types.Rowid](res.Batches[0].Vecs[0], 0)

	// Generate DELETE batch for index table
	bat, err := catalog.GenLogicalIdIndexDeleteBatch(rowid, logicalId, m)
	if err != nil {
		return err
	}

	// Use deleteBatch to filter out rows that were created in this txn (same pattern as deleteTable)
	if bat = txn.deleteBatch(bat, catalog.MO_CATALOG_ID, catalog.MO_TABLES_LOGICAL_ID_INDEX_ID); bat.RowCount() > 0 {
		note := fmt.Sprintf("sync logical_id index delete %d", logicalId)
		if _, err = txn.WriteBatch(
			DELETE, note, catalog.System_Account, catalog.MO_CATALOG_ID, catalog.MO_TABLES_LOGICAL_ID_INDEX_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES_LOGICAL_ID_INDEX_TABLE_NAME, bat, txn.tnStores[0]); err != nil {
			bat.Clean(m)
			return err
		}
	}
	return nil
}
