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
	"runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
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

func (h *mockHandle) HandleCommit(ctx context.Context, meta *txn.TxnMeta) (timestamp.Timestamp, error) {
	//2PC
	if len(meta.TNShards) > 1 && meta.CommitTS.IsEmpty() {
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
	req *api.PrecommitWriteCmd,
	resp *api.TNStringResponse) error {

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
				&cmd, new(api.TNStringResponse)); err != nil {
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
			if _, err = h.HandleCommit(ctx, txn); err != nil {
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

func initDB(ctx context.Context, t *testing.T, opts *options.Options) *db.DB {
	dir := testutils.InitTestEnv(ModuleName, t)
	db, _ := db.Open(ctx, dir, opts)
	return db
}

func mockTAEHandle(ctx context.Context, t *testing.T, opts *options.Options) *mockHandle {
	blockio.Start("")
	tae := initDB(ctx, t, opts)
	mh := &mockHandle{
		m: mpool.MustNewZero(),
	}

	mh.Handle = &Handle{
		db: tae,
	}
	mh.Handle.txnCtxs = common.NewMap[string, *txnContext](runtime.GOMAXPROCS(0))
	return mh
}

func mock1PCTxn(db *db.DB) *txn.TxnMeta {
	txnMeta := &txn.TxnMeta{}
	txnMeta.ID = db.TxnMgr.IdAlloc.Alloc()
	txnMeta.SnapshotTS = db.TxnMgr.Now().ToTimestamp()
	return txnMeta
}

func mockTNShard(id uint64) metadata.TNShard {
	return metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{
			ShardID:    id,
			LogShardID: id,
		},
		ReplicaID: id,
		Address:   fmt.Sprintf("dn-%d", id),
	}
}

func mock2PCTxn(db *db.DB) *txn.TxnMeta {
	txnMeta := &txn.TxnMeta{}
	txnMeta.ID = db.TxnMgr.IdAlloc.Alloc()
	txnMeta.SnapshotTS = db.TxnMgr.Now().ToTimestamp()
	txnMeta.TNShards = append(txnMeta.TNShards, mockTNShard(1))
	txnMeta.TNShards = append(txnMeta.TNShards, mockTNShard(2))
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

func makeCreateDatabaseEntries(
	sql string,
	ac AccessInfo,
	name string,
	id uint64,
	m *mpool.MPool,
) ([]*api.Entry, error) {

	packer := types.NewPacker()
	defer packer.Close()
	createDbBat, err := catalog.GenCreateDatabaseTuple(
		sql,
		ac.accountId,
		ac.userId,
		ac.roleId,
		name,
		id,
		"",
		m,
		packer,
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
	constraint []byte,
	m *mpool.MPool,
	defs []engine.TableDef,
) ([]*api.Entry, error) {
	cols, err := catalog.GenColumnsFromDefs(ac.accountId, name, dbName, tableId, dbId, defs)
	if err != nil {
		return nil, err
	}
	comment := ""
	for _, def := range defs {
		if cdef, ok := def.(*engine.CommentDef); ok {
			comment = cdef.Comment
			break
		}
	}
	packer := types.NewPacker()
	defer packer.Close()
	//var entries []*api.Entry
	entries := make([]*api.Entry, 0)
	{
		tbl := catalog.Table{
			AccountId:    ac.accountId,
			UserId:       ac.userId,
			RoleId:       ac.roleId,
			TableName:    name,
			TableId:      tableId,
			DatabaseId:   dbId,
			DatabaseName: dbName,
			Comment:      comment,
			CreateSql:    sql,
			Constraint:   constraint,
		}
		bat, err := catalog.GenCreateTableTuple(tbl, m, packer)
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
	{
		bat, err := catalog.GenCreateColumnTuples(cols, m, packer)
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
