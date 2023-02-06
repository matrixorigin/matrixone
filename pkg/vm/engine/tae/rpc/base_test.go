// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpc

import (
	"context"
	"fmt"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
)

const ModuleName = "TAEHANDLE"

type mockHandle struct {
	*Handle
	m *mpool.MPool
}

type CmdType int32

const (
	CmdPreCommitWrite CmdType = iota
	CmdPrepare
	CmdCommitting
	CmdCommit
	CmdRollback
)

type txnCommand struct {
	typ CmdType
	cmd any
}

func (h *mockHandle) HandleClose(ctx context.Context) error {
	err := h.Handle.HandleClose(ctx)
	return err
}

func (h *mockHandle) HandleCommit(ctx context.Context, meta *txn.TxnMeta) error {
	//2PC
	if len(meta.DNShards) > 1 && meta.CommitTS.IsEmpty() {
		meta.CommitTS = meta.PreparedTS.Next()
	}
	return h.Handle.HandleCommit(ctx, *meta)
}

func (h *mockHandle) HandleCommitting(ctx context.Context, meta *txn.TxnMeta) error {
	//meta.CommitTS = h.eng.GetTAE(context.TODO()).TxnMgr.TsAlloc.Alloc().ToTimestamp()
	if meta.PreparedTS.IsEmpty() {
		return moerr.NewInternalError(ctx, "PreparedTS is empty")
	}
	meta.CommitTS = meta.PreparedTS.Next()
	return h.Handle.HandleCommitting(ctx, *meta)
}

func (h *mockHandle) HandlePrepare(ctx context.Context, meta *txn.TxnMeta) error {
	pts, err := h.Handle.HandlePrepare(ctx, *meta)
	if err != nil {
		return err
	}
	meta.PreparedTS = pts
	return nil
}

func (h *mockHandle) HandleRollback(ctx context.Context, meta *txn.TxnMeta) error {
	return h.Handle.HandleRollback(ctx, *meta)
}

func (h *mockHandle) HandlePreCommit(
	ctx context.Context,
	meta *txn.TxnMeta,
	req api.PrecommitWriteCmd,
	resp *api.SyncLogTailResp) error {

	return h.Handle.HandlePreCommitWrite(ctx, *meta, req, resp)
}

func (h *mockHandle) handleCmds(
	ctx context.Context,
	txn *txn.TxnMeta,
	cmds []txnCommand) (err error) {
	for _, e := range cmds {
		switch e.typ {
		case CmdPreCommitWrite:
			cmd, ok := e.cmd.(api.PrecommitWriteCmd)
			if !ok {
				return moerr.NewInfo(ctx, "cmd is not PreCommitWriteCmd")
			}
			if err = h.Handle.HandlePreCommitWrite(ctx, *txn,
				cmd, new(api.SyncLogTailResp)); err != nil {
				return
			}
		case CmdPrepare:
			if err = h.HandlePrepare(ctx, txn); err != nil {
				return
			}
		case CmdCommitting:
			if err = h.HandleCommitting(ctx, txn); err != nil {
				return
			}
		case CmdCommit:
			if err = h.HandleCommit(ctx, txn); err != nil {
				return
			}
		case CmdRollback:
			if err = h.HandleRollback(ctx, txn); err != nil {
				return
			}
		default:
			panic(moerr.NewInfo(ctx, "Invalid CmdType"))
		}
	}
	return
}

func initDB(t *testing.T, opts *options.Options) *db.DB {
	dir := testutils.InitTestEnv(ModuleName, t)
	db, _ := db.Open(dir, opts)
	return db
}

func mockTAEHandle(t *testing.T, opts *options.Options) *mockHandle {
	tae := initDB(t, opts)
	mh := &mockHandle{
		m: mpool.MustNewZero(),
	}

	mh.Handle = &Handle{
		eng:          moengine.NewEngine(tae),
		jobScheduler: tasks.NewParallelJobScheduler(5),
	}
	mh.Handle.mu.txnCtxs = make(map[string]*txnContext)
	return mh
}

func mock1PCTxn(eng moengine.TxnEngine) *txn.TxnMeta {
	txnMeta := &txn.TxnMeta{}
	txnMeta.ID = eng.GetTAE(context.TODO()).TxnMgr.IdAlloc.Alloc()
	txnMeta.SnapshotTS = eng.GetTAE(context.TODO()).TxnMgr.TsAlloc.Alloc().ToTimestamp()
	return txnMeta
}

func mockDNShard(id uint64) metadata.DNShard {
	return metadata.DNShard{
		DNShardRecord: metadata.DNShardRecord{
			ShardID:    id,
			LogShardID: id,
		},
		ReplicaID: id,
		Address:   fmt.Sprintf("dn-%d", id),
	}
}

func mock2PCTxn(eng moengine.TxnEngine) *txn.TxnMeta {
	txnMeta := &txn.TxnMeta{}
	txnMeta.ID = eng.GetTAE(context.TODO()).TxnMgr.IdAlloc.Alloc()
	txnMeta.SnapshotTS = eng.GetTAE(context.TODO()).TxnMgr.TsAlloc.Alloc().ToTimestamp()
	txnMeta.DNShards = append(txnMeta.DNShards, mockDNShard(1))
	txnMeta.DNShards = append(txnMeta.DNShards, mockDNShard(2))
	return txnMeta
}

const (
	INSERT = iota
	DELETE
)

type AccessInfo struct {
	accountId uint32
	userId    uint32
	roleId    uint32
}

// Entry represents a delete/insert
type Entry struct {
	typ          int
	tableId      uint64
	databaseId   uint64
	tableName    string
	databaseName string
	//object name for s3 file
	fileName string
	// update or delete tuples
	bat *batch.Batch
}

type column struct {
	databaseId uint64
	accountId  uint32
	tableId    uint64
	// column name
	name            string
	tableName       string
	databaseName    string
	typ             []byte
	typLen          int32
	num             int32
	comment         string
	notNull         int8
	hasDef          int8
	defaultExpr     []byte
	constraintType  string
	isHidden        int8
	isAutoIncrement int8
	hasUpdate       int8
	updateExpr      []byte
	clusterBy       int8
}

func genColumns(accountId uint32, tableName, databaseName string,
	tableId, databaseId uint64, defs []engine.TableDef) ([]column, error) {
	{ // XXX Why not store PrimaryIndexDef and
		// then use PrimaryIndexDef for all primary key constraints.
		mp := make(map[string]int)
		for i, def := range defs {
			if attr, ok := def.(*engine.AttributeDef); ok {
				mp[attr.Attr.Name] = i
			}
		}
		for _, def := range defs {
			if constraintDef, ok := def.(*engine.ConstraintDef); ok {
				pkeyDef := constraintDef.GetPrimaryKeyDef()
				if pkeyDef != nil {
					pkeyColName := pkeyDef.Pkey.PkeyColName
					attr, _ := defs[mp[pkeyColName]].(*engine.AttributeDef)
					attr.Attr.Primary = true
				}
			}
		}
	}
	num := 0
	cols := make([]column, 0, len(defs))
	for _, def := range defs {
		attrDef, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		typ, err := types.Encode(&attrDef.Attr.Type)
		if err != nil {
			return nil, err
		}
		col := column{
			typ:          typ,
			typLen:       int32(len(typ)),
			accountId:    accountId,
			tableId:      tableId,
			databaseId:   databaseId,
			name:         attrDef.Attr.Name,
			tableName:    tableName,
			databaseName: databaseName,
			num:          int32(num),
			comment:      attrDef.Attr.Comment,
		}
		col.hasDef = 0
		if attrDef.Attr.Default != nil {
			defaultExpr, err := types.Encode(attrDef.Attr.Default)
			if err != nil {
				return nil, err
			}
			if len(defaultExpr) > 0 {
				col.hasDef = 1
				col.defaultExpr = defaultExpr
			}
		}
		if attrDef.Attr.OnUpdate != nil {
			expr, err := types.Encode(attrDef.Attr.OnUpdate)
			if err != nil {
				return nil, err
			}
			if len(expr) > 0 {
				col.hasUpdate = 1
				col.updateExpr = expr
			}
		}
		if attrDef.Attr.IsHidden {
			col.isHidden = 1
		}
		if attrDef.Attr.AutoIncrement {
			col.isAutoIncrement = 1
		}
		if attrDef.Attr.Primary {
			col.constraintType = catalog.SystemColPKConstraint
		} else {
			col.constraintType = catalog.SystemColNoConstraint
		}
		cols = append(cols, col)
		num++
	}
	return cols, nil
}

func makePBEntry(
	typ int,
	dbId,
	tableId uint64,
	dbName,
	tbName,
	file string,
	bat *batch.Batch) (pe *api.Entry, err error) {
	e := Entry{
		typ:          typ,
		databaseName: dbName,
		databaseId:   dbId,
		tableName:    tbName,
		tableId:      tableId,
		fileName:     file,
		bat:          bat,
	}
	return toPBEntry(e)
}

func getTableComment(defs []engine.TableDef) string {
	for _, def := range defs {
		if cdef, ok := def.(*engine.CommentDef); ok {
			return cdef.Comment
		}
	}
	return ""
}

func genCreateDatabaseTuple(
	sql string,
	accountId,
	userId,
	roleId uint32,
	name string,
	id uint64,
	m *mpool.MPool,
) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoDatabaseSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoDatabaseSchema...)
	{
		idx := catalog.MO_DATABASE_DAT_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // dat_id
		if err := bat.Vecs[idx].Append(uint64(id), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_DAT_NAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // datname
		if err := bat.Vecs[idx].Append([]byte(name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_DAT_CATALOG_NAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // dat_catalog_name
		if err := bat.Vecs[idx].Append([]byte(catalog.MO_CATALOG), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_CREATESQL_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx])            // dat_createsql
		if err := bat.Vecs[idx].Append([]byte(sql), false, m); err != nil { // TODO
			return nil, err
		}
		idx = catalog.MO_DATABASE_OWNER_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // owner
		if err := bat.Vecs[idx].Append(roleId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_CREATOR_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // creator
		if err := bat.Vecs[idx].Append(userId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_CREATED_TIME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // created_time
		if err := bat.Vecs[idx].Append(types.Timestamp(time.Now().Unix()), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_ACCOUNT_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // account_id
		if err := bat.Vecs[idx].Append(accountId, false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func genCreateColumnTuple(
	col column,
	m *mpool.MPool,
) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoColumnsSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
	bat.SetZs(1, m)
	{
		idx := catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_uniq_name
		if err := bat.Vecs[idx].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ACCOUNT_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // account_id
		if err := bat.Vecs[idx].Append(uint32(0), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_database_id
		if err := bat.Vecs[idx].Append(col.databaseId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_DATABASE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_database
		if err := bat.Vecs[idx].Append([]byte(col.databaseName), false, m); err != nil {
			return nil, err
		}

		idx = catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_relname_id
		if err := bat.Vecs[idx].Append(col.tableId, false, m); err != nil {
			return nil, err
		}

		idx = catalog.MO_COLUMNS_ATT_RELNAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_relname_id
		if err := bat.Vecs[idx].Append([]byte(col.tableName), false, m); err != nil {
			return nil, err
		}
		//idx = catalog.MO_COLUMNS_ATTNUM_IDX
		idx = catalog.MO_COLUMNS_ATTNAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // attname
		if err := bat.Vecs[idx].Append([]byte(col.name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTTYP_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_relname
		if err := bat.Vecs[idx].Append(col.typ, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTNUM_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // attnum
		if err := bat.Vecs[idx].Append(col.num, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_LENGTH_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_length
		if err := bat.Vecs[idx].Append(col.typLen, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTNOTNULL_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // attnotnul
		if err := bat.Vecs[idx].Append(col.notNull, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTHASDEF_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // atthasdef
		if err := bat.Vecs[idx].Append(col.hasDef, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_DEFAULT_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_default
		if err := bat.Vecs[idx].Append(col.defaultExpr, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTISDROPPED_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // attisdropped
		if err := bat.Vecs[idx].Append(int8(0), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_constraint_type
		if err := bat.Vecs[idx].Append([]byte(col.constraintType), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_IS_UNSIGNED_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_is_unsigned
		if err := bat.Vecs[idx].Append(int8(0), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_is_auto_increment
		if err := bat.Vecs[idx].Append(col.isAutoIncrement, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_COMMENT_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_comment
		if err := bat.Vecs[idx].Append([]byte(col.comment), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_IS_HIDDEN_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_is_hidden
		if err := bat.Vecs[idx].Append(col.isHidden, false, m); err != nil {
			return nil, err
		}

		idx = catalog.MO_COLUMNS_ATT_HAS_UPDATE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_has_update
		if err := bat.Vecs[idx].Append(col.hasUpdate, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_UPDATE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_update
		if err := bat.Vecs[idx].Append(col.updateExpr, false, m); err != nil {
			return nil, err
		}

		idx = catalog.MO_COLUMNS_ATT_IS_CLUSTERBY
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_is_clusterby
		if err := bat.Vecs[idx].Append(col.clusterBy, false, m); err != nil {
			return nil, err
		}

	}
	return bat, nil
}

func makeCreateDatabaseEntries(
	sql string,
	ac AccessInfo,
	name string,
	id uint64,
	m *mpool.MPool,
) ([]*api.Entry, error) {

	createDbBat, err := genCreateDatabaseTuple(
		sql,
		ac.accountId,
		ac.userId,
		ac.roleId,
		name,
		id,
		m,
	)
	if err != nil {
		return nil, err
	}
	createDbEntry, err := makePBEntry(
		INSERT,
		catalog.MO_CATALOG_ID,
		catalog.MO_DATABASE_ID,
		catalog.MO_CATALOG,
		catalog.MO_DATABASE,
		"",
		createDbBat,
	)
	if err != nil {
		return nil, err
	}
	var entries []*api.Entry
	entries = append(entries, createDbEntry)
	return entries, nil

}

func makeCreateTableEntries(
	sql string,
	ac AccessInfo,
	name string,
	tableId uint64,
	dbId uint64,
	dbName string,
	m *mpool.MPool,
	defs []engine.TableDef,
) ([]*api.Entry, error) {
	comment := getTableComment(defs)
	cols, err := genColumns(ac.accountId, name, dbName, tableId, dbId, defs)
	if err != nil {
		return nil, err
	}
	//var entries []*api.Entry
	entries := make([]*api.Entry, 0)
	{
		bat, err := genCreateTableTuple(
			sql, ac.accountId, ac.userId, ac.roleId,
			name, tableId, dbId, dbName, comment, m)
		if err != nil {
			return nil, err
		}
		createTbEntry, err := makePBEntry(INSERT,
			catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, "", bat)
		if err != nil {
			return nil, err
		}
		entries = append(entries, createTbEntry)
	}
	for _, col := range cols {
		bat, err := genCreateColumnTuple(col, m)
		if err != nil {
			return nil, err
		}
		createColumnEntry, err := makePBEntry(
			INSERT, catalog.MO_CATALOG_ID,
			catalog.MO_COLUMNS_ID, catalog.MO_CATALOG,
			catalog.MO_COLUMNS, "", bat)
		if err != nil {
			return nil, err
		}
		entries = append(entries, createColumnEntry)
	}
	return entries, nil

}

func genCreateTableTuple(
	sql string,
	accountId,
	userId,
	roleId uint32,
	name string,
	tableId uint64,
	databaseId uint64,
	databaseName string,
	comment string,
	m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoTablesSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoTablesSchema...)
	bat.SetZs(1, m)
	{
		idx := catalog.MO_TABLES_REL_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // rel_id
		if err := bat.Vecs[idx].Append(tableId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_NAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // relname
		if err := bat.Vecs[idx].Append([]byte(name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // reldatabase
		if err := bat.Vecs[idx].Append([]byte(databaseName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // reldatabase_id
		if err := bat.Vecs[idx].Append(databaseId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELPERSISTENCE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // relpersistence
		if err := bat.Vecs[idx].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELKIND_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // relkind
		if err := bat.Vecs[idx].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_COMMENT_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // rel_comment
		if err := bat.Vecs[idx].Append([]byte(comment), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_CREATESQL_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // rel_createsql
		if err := bat.Vecs[idx].Append([]byte(sql), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_CREATED_TIME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // created_time
		if err := bat.Vecs[idx].Append(types.Timestamp(time.Now().Unix()), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_CREATOR_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // creator
		if err := bat.Vecs[idx].Append(userId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_OWNER_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // owner
		if err := bat.Vecs[idx].Append(roleId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_ACCOUNT_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // account_id
		if err := bat.Vecs[idx].Append(accountId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_PARTITIONED_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // partition
		if err := bat.Vecs[idx].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_VIEWDEF_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // viewdef
		if err := bat.Vecs[idx].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_CONSTRAINT_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // constraint
		if err := bat.Vecs[idx].Append([]byte(""), false, m); err != nil {
			return nil, err
		}

	}
	return bat, nil
}

func toPBEntry(e Entry) (*api.Entry, error) {
	bat, err := toPBBatch(e.bat)
	if err != nil {
		return nil, err
	}
	typ := api.Entry_Insert
	if e.typ == DELETE {
		typ = api.Entry_Delete
	}
	return &api.Entry{
		Bat:          bat,
		EntryType:    typ,
		TableId:      e.tableId,
		DatabaseId:   e.databaseId,
		TableName:    e.tableName,
		DatabaseName: e.databaseName,
		FileName:     e.fileName,
	}, nil
}

func toPBBatch(bat *batch.Batch) (*api.Batch, error) {
	rbat := new(api.Batch)
	rbat.Attrs = bat.Attrs
	for _, vec := range bat.Vecs {
		pbVector, err := vector.VectorToProtoVector(vec)
		if err != nil {
			return nil, err
		}
		rbat.Vecs = append(rbat.Vecs, pbVector)
	}
	return rbat, nil
}

func toTAEBatchWithSharedMemory(schema *catalog2.Schema,
	bat *batch.Batch) *containers.Batch {
	allNullables := schema.AllNullables()
	taeBatch := containers.NewEmptyBatch()
	for i, vec := range bat.Vecs {
		v := containers.NewVectorWithSharedMemory(vec, allNullables[i])
		taeBatch.AddVector(bat.Attrs[i], v)
	}
	return taeBatch
}

//gen LogTail
