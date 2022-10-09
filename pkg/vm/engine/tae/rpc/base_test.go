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
	"github.com/matrixorigin/matrixone/pkg/catalog"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"testing"
	"time"
)

const ModuleName = "TAEHANDLE"

type mockHandle struct {
	*Handle
	m *mpool.MPool
}

func (h *mockHandle) HandleClose() error {
	err := h.Handle.HandleClose()
	//TODO::free h.m?
	return err
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
		eng: moengine.NewEngine(tae),
	}
	return mh
}

func mock1PCTxn(eng moengine.TxnEngine) txn.TxnMeta {
	txnMeta := txn.TxnMeta{}
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

func mock2PCTxn(eng moengine.TxnEngine) (txn.TxnMeta, error) {
	txnMeta := txn.TxnMeta{}
	txnMeta.ID = eng.GetTAE(context.TODO()).TxnMgr.IdAlloc.Alloc()
	txnMeta.SnapshotTS = eng.GetTAE(context.TODO()).TxnMgr.TsAlloc.Alloc().ToTimestamp()
	txnMeta.DNShards = append(txnMeta.DNShards, mockDNShard(1))
	txnMeta.DNShards = append(txnMeta.DNShards, mockDNShard(2))
	return txnMeta, nil
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
	// blockName for s3 file
	fileName string
	// blockId for s3 file
	blockId uint64
	// update or delete tuples
	bat *batch.Batch
}

type column struct {
	databaseId uint64
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
}

func genColumns(
	tableName,
	databaseName string,
	databaseId uint64,
	defs []engine.TableDef) ([]column, error) {
	num := 0
	cols := make([]column, 0, len(defs))
	for _, def := range defs {
		attrDef, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		typ, err := types.Encode(attrDef.Attr.Type)
		if err != nil {
			return nil, err
		}
		col := column{
			typ:          typ,
			typLen:       int32(len(typ)),
			databaseId:   databaseId,
			name:         attrDef.Attr.Name,
			tableName:    tableName,
			databaseName: databaseName,
			num:          int32(num),
			comment:      attrDef.Attr.Comment,
		}
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
	tbName string,
	bat *batch.Batch) (pe *api.Entry, err error) {
	e := Entry{
		typ:          typ,
		databaseName: dbName,
		databaseId:   dbId,
		tableName:    tbName,
		tableId:      tableId,
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
	m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoDatabaseSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoDatabaseSchema...)
	{
		idx := catalog.MO_DATABASE_DAT_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // dat_id
		if err := bat.Vecs[idx].Append(uint64(0), false, m); err != nil {
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

func genCreateColumnTuple(col column, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoColumnsSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
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
		//TODO::rel id get from col
		if err := bat.Vecs[idx].Append(uint64(0), false, m); err != nil {
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
	}
	return bat, nil
}

func makeCreateDatabaseEntries(
	sql string,
	ac AccessInfo,
	name string,
	m *mpool.MPool,
) ([]*api.Entry, error) {

	createDbBat, err := genCreateDatabaseTuple(
		sql,
		ac.accountId,
		ac.userId,
		ac.roleId,
		name,
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
	dbId uint64,
	dbName string,
	m *mpool.MPool,
	defs []engine.TableDef,
) ([]*api.Entry, error) {
	comment := getTableComment(defs)
	cols, err := genColumns(name, dbName, dbId, defs)
	if err != nil {
		return nil, err
	}
	//var entries []*api.Entry
	entries := make([]*api.Entry, 0)
	{
		bat, err := genCreateTableTuple(
			sql, ac.accountId, ac.userId, ac.roleId,
			name, dbId, dbName, comment, m)
		if err != nil {
			return nil, err
		}
		createTbEntry, err := makePBEntry(INSERT,
			catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, bat)
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
		createColumnEntry, err := makePBEntry(INSERT, catalog.MO_CATALOG_ID,
			catalog.MO_COLUMNS_ID, catalog.MO_CATALOG, catalog.MO_COLUMNS, bat)
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
	databaseId uint64,
	databaseName string,
	comment string,
	m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoTablesSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoTablesSchema...)
	{
		idx := catalog.MO_TABLES_REL_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // rel_id
		if err := bat.Vecs[idx].Append(uint64(0), false, m); err != nil {
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
		BlockId:      e.blockId,
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

//gen LogTail
