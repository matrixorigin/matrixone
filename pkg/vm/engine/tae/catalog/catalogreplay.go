// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package catalog

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"slices"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const (
	Backup_Object_Offset uint16 = 1000
)

type ObjectListReplayer interface {
	Submit(context.Context, uint64, func()) error
}

//#region Replay WAL related

func (catalog *Catalog) ReplayCmd(
	txncmd txnif.TxnCmd,
	observer wal.ReplayObserver,
) {
	switch txncmd.GetType() {
	case txnbase.IOET_WALTxnCommand_Composed:
		cmds := txncmd.(*txnbase.ComposedCmd)
		for _, cmds := range cmds.Cmds {
			catalog.ReplayCmd(cmds, observer)
		}
	case IOET_WALTxnCommand_Database:
		cmd := txncmd.(*EntryCommand[*EmptyMVCCNode, *DBNode])
		catalog.onReplayUpdateDatabase(cmd, observer)
	case IOET_WALTxnCommand_Table:
		cmd := txncmd.(*EntryCommand[*TableMVCCNode, *TableNode])
		catalog.onReplayUpdateTable(cmd, observer)
	case IOET_WALTxnCommand_Object:
		cmd := txncmd.(*EntryCommand[*ObjectMVCCNode, *ObjectNode])
		catalog.onReplayUpdateObject(cmd, observer)
	// case IOET_WALTxnCommand_Block:
	// 	cmd := txncmd.(*EntryCommand[*MetadataMVCCNode, *BlockNode])
	// 	catalog.onReplayUpdateBlock(cmd, observer)
	// case IOET_WALTxnCommand_Segment:
	// 	// segment is deprecated
	// 	return
	default:
		panic("unsupport")
	}
}

func (catalog *Catalog) onReplayUpdateDatabase(cmd *EntryCommand[*EmptyMVCCNode, *DBNode], _ wal.ReplayObserver) {
	catalog.OnReplayDBID(cmd.ID.DbID)
	var err error
	un := cmd.mvccNode

	db, err := catalog.GetDatabaseByID(cmd.ID.DbID)
	if err != nil {
		catalog.Lock()
		defer catalog.Unlock()
		db = NewReplayDBEntry()
		db.ID = cmd.ID.DbID
		db.catalog = catalog
		db.DBNode = cmd.node
		db.InsertLocked(un)
		// After replaying checkpoint, all entries' commit-ts were forced to be endts of the last checkpoint,
		// so the start-ts of a txn from WAL can be smaller than the committs of the last checkpoint.
		// To prevent AddEntryLocked from issusing a w-w conflict error, set the skipDedup arg true
		err = catalog.AddEntryLocked(db, un.GetTxn(), true)
		if err != nil {
			panic(err)
		}
		cmd.applyCommitFn = func() {
			db.Lock()
			defer db.Unlock()
			err := cmd.mvccNode.ApplyCommit(cmd.mvccNode.Txn.GetID())
			if err != nil {
				panic(err)
			}
		}
		return
	}

	db.Lock()
	defer db.Unlock()
	cmd.applyCommitFn = func() {
		db.Lock()
		defer db.Unlock()
		err := cmd.mvccNode.ApplyCommit(cmd.mvccNode.Txn.GetID())
		if err != nil {
			panic(err)
		}
	}
	dbun := db.SearchNodeLocked(un)
	if dbun == nil {
		db.InsertLocked(un)
	} else {
		return
		// panic(fmt.Sprintf("logic err: duplicate node %v and %v", dbun.String(), un.String()))
	}
}

func (catalog *Catalog) onReplayUpdateTable(cmd *EntryCommand[*TableMVCCNode, *TableNode], _ wal.ReplayObserver) {
	catalog.OnReplayTableID(cmd.ID.TableID)
	db, err := catalog.GetDatabaseByID(cmd.ID.DbID)
	if err != nil {
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(cmd.ID.TableID)

	un := cmd.mvccNode
	if err != nil {
		db.Lock()
		defer db.Unlock()
		tbl = NewReplayTableEntry()
		tbl.ID = cmd.ID.TableID
		tbl.dataObjects.SetTableID(tbl.ID)
		tbl.tombstoneObjects.SetTableID(tbl.ID)
		tbl.db = db
		tbl.tableData = catalog.MakeTableFactory()(tbl)
		tbl.TableNode = cmd.node
		tbl.TableNode.schema.Store(un.BaseNode.Schema)
		tbl.InsertLocked(un)
		err = db.AddEntryLocked(tbl, un.GetTxn(), true)
		if err != nil {
			logutil.Warn(catalog.SimplePPString(common.PPL3))
			panic(err)
		}
		cmd.applyCommitFn = func() {
			tbl.Lock()
			defer tbl.Unlock()
			err := cmd.mvccNode.ApplyCommit(cmd.mvccNode.Txn.GetID())
			if err != nil {
				panic(err)
			}
		}
		return
	}

	tbl.Lock()
	defer tbl.Unlock()
	cmd.applyCommitFn = func() {
		tbl.Lock()
		defer tbl.Unlock()
		err := cmd.mvccNode.ApplyCommit(cmd.mvccNode.Txn.GetID())
		if err != nil {
			panic(err)
		}
	}
	tblun := tbl.SearchNodeLocked(un)
	if tblun == nil {
		tbl.InsertLocked(un) //TODO isvalid
		if tbl.isColumnChangedInSchemaLocked() {
			tbl.FreezeAppend()
		}
		schema := un.BaseNode.Schema
		tbl.TableNode.schema.Store(schema)
		// alter table rename
		if schema.Extra.OldName != "" && un.DeletedAt.IsEmpty() {
			// WAL replays each table's final name independently. Another table in
			// the same committed swap may still have its old name, so skip admission.
			if err := tbl.db.replayRenameTable(
				schema.Extra.OldName, schema.Name, tbl.ID, schema.AcInfo.TenantID, nil, true,
			); err != nil {
				logutil.Warn(schema.String())
				panic(err)
			}
		}
	}
}

func (catalog *Catalog) onReplayUpdateObject(
	cmd *EntryCommand[*ObjectMVCCNode, *ObjectNode],
	_ wal.ReplayObserver,
) {
	catalog.OnReplayObjectID(cmd.node.SortHint)

	db, err := catalog.GetDatabaseByID(cmd.ID.DbID)
	if err != nil {
		// a db is dropped before checkpoint end
		// and its tables are flushed after the checkpoint end,
		// it is normal to for WAL to miss the db
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			return
		}
		panic(err)
	}
	rel, err := db.GetTableEntryByID(cmd.ID.TableID)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			return
		}
		panic(err)
	}
	var obj *ObjectEntry
	if cmd.mvccNode.CreatedAt.Equal(&txnif.UncommitTS) {
		rel.Lock()
		defer rel.Unlock()
		obj = NewReplayObjectEntry()
		obj.table = rel
		obj.ObjectNode = *cmd.node
		obj.SortHint = catalog.NextObject()
		obj.EntryMVCCNode = cmd.mvccNode.EntryMVCCNode
		obj.CreateNode = cmd.mvccNode.TxnMVCCNode
		cmd.mvccNode.CommitSideEffect = func(id string, ts types.TS) {
			obj.CreateNode.ApplyCommit(id)
			rel.UpdateReplayEntryTs(obj, ts)
		}
		obj.ObjectMVCCNode = *cmd.mvccNode.BaseNode
		obj.ObjectState = ObjectState_Create_PrepareCommit
		rel.AddEntryLocked(obj)
	}
	if cmd.mvccNode.DeletedAt.Equal(&txnif.UncommitTS) {
		cobj, err := rel.GetObjectByID(cmd.ID.ObjectID(), cmd.node.IsTombstone)
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) && cmd.mvccNode.BaseNode.ObjectStats.GetAppendable() {
			rel.Lock()
			cobj, err = rel.GetObjectByID(cmd.ID.ObjectID(), cmd.node.IsTombstone)
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				var factory ObjectDataFactory
				if catalog.DataFactory != nil {
					factory = catalog.DataFactory.MakeObjectFactory()
				}
				cobj = NewCommittedObjectEntry(
					rel,
					cmd.mvccNode.CreatedAt,
					cmd.mvccNode.BaseNode.ObjectStats,
					factory,
					cmd.node.IsTombstone,
				)
				cobj.ObjectNode = *cmd.node
				rel.AddEntryLocked(cobj)
				err = nil
			}
			rel.Unlock()
		}
		if err != nil {
			panic(fmt.Sprintf("obj %v not existed, table:\n%v", cmd.ID.String(), rel.StringWithLevel(3)))
		}
		obj = cobj.Clone()
		updatedCreate := cobj.Clone()
		updatedCreate.nextVersion = obj
		obj.prevVersion = updatedCreate
		obj.EntryMVCCNode = cmd.mvccNode.EntryMVCCNode
		obj.DeleteNode = cmd.mvccNode.TxnMVCCNode
		obj.ObjectMVCCNode = *cmd.mvccNode.BaseNode
		cmd.mvccNode.CommitSideEffect = func(id string, ts types.TS) {
			obj.DeleteNode.ApplyCommit(id)
			rel.UpdateReplayEntryTs(obj, ts)
		}
		obj.ObjectState = ObjectState_Delete_PrepareCommit
		rel.AddEntryLocked(obj)
	}

	cmd.applyCommitFn = func() {
		err := obj.ApplyCommit(cmd.mvccNode.Txn.GetID())
		if err != nil {
			panic(err)
		}
	}

	if obj.objData == nil {
		obj.objData = catalog.MakeObjectFactory()(obj)
	} else {
		deleteAt := obj.GetDeleteAt()
		if !obj.IsAppendable() || (obj.IsAppendable() && !deleteAt.IsEmpty()) {
			obj.objData.TryUpgrade()
		}
	}
}

//#endregion

//#region Replay Checkpoint related

type replayTableKey struct {
	id   uint64
	name string
}

type replayTable struct {
	db     *DBEntry
	id     uint64
	schema *Schema
	// Missing sequence metadata must not silently renumber evolved columns.
	nonOrdinalSeq bool
	missingSeq    bool
}

func replayVector(bat *containers.Batch, name string) *vector.Vector {
	idx, ok := bat.Nameidx[name]
	if !ok {
		return nil
	}
	return bat.Vecs[idx]
}

func replayFixed[T any](bat *containers.Batch, name string, row int) T {
	v := replayVector(bat, name)
	if v == nil || v.IsNull(uint64(row)) {
		var zero T
		return zero
	}
	return vector.GetFixedAtNoTypeCheck[T](v, row)
}

func replayString(bat *containers.Batch, name string, row int) string {
	v := replayVector(bat, name)
	if v == nil || v.IsNull(uint64(row)) {
		return ""
	}
	return v.GetStringAt(row)
}

// replayBytes returns a view that must be copied before the scanner advances.
func replayBytes(bat *containers.Batch, name string, row int) []byte {
	v := replayVector(bat, name)
	if v == nil || v.IsNull(uint64(row)) {
		return nil
	}
	return v.GetBytesAt(row)
}

func requireReplayFields(bat *containers.Batch, row int, names ...string) error {
	for _, name := range names {
		v := replayVector(bat, name)
		if v == nil || v.IsNull(uint64(row)) {
			return moerr.NewInternalErrorNoCtxf("missing required catalog field %s", name)
		}
	}
	return nil
}

// The synthetic timestamp identifies catalog nodes reconstructed from a checkpoint.
var checkpointCatalogTS = types.BuildTS(42424242, 0)

// ReplayFromSysTableObjects rebuilds the catalog without retaining raw system
// table batches. Only owned schemas and their lookup by table identity remain
// live while columns are read. Replay tasks do not borrow scanner buffers.
func (catalog *Catalog) ReplayFromSysTableObjects(
	ctx context.Context,
	readTxn txnif.AsyncTxn,
	scan func(context.Context, *TableEntry, txnif.AsyncTxn, func(*containers.Batch, int) error) error,
	replayer ObjectListReplayer,
) error {
	db, err := catalog.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		return err
	}
	dbTbl, err := db.GetTableEntryByID(pkgcatalog.MO_DATABASE_ID)
	if err != nil {
		return err
	}
	tableTbl, err := db.GetTableEntryByID(pkgcatalog.MO_TABLES_ID)
	if err != nil {
		return err
	}
	columnTbl, err := db.GetTableEntryByID(pkgcatalog.MO_COLUMNS_ID)
	if err != nil {
		return err
	}

	// Keep checkpoint-created entries before WAL operations that may drop them.
	txnNode := &txnbase.TxnMVCCNode{Start: checkpointCatalogTS, Prepare: checkpointCatalogTS, End: checkpointCatalogTS}
	if err = scan(ctx, dbTbl, readTxn, func(bat *containers.Batch, row int) error {
		if err := requireReplayFields(bat, row, pkgcatalog.SystemDBAttr_ID, pkgcatalog.SystemDBAttr_Name); err != nil {
			return err
		}
		catalog.onReplayCreateDB(
			replayFixed[uint64](bat, pkgcatalog.SystemDBAttr_ID, row),
			replayString(bat, pkgcatalog.SystemDBAttr_Name, row), txnNode,
			replayFixed[uint32](bat, pkgcatalog.SystemDBAttr_AccID, row),
			replayFixed[uint32](bat, pkgcatalog.SystemDBAttr_Creator, row),
			replayFixed[uint32](bat, pkgcatalog.SystemDBAttr_Owner, row),
			replayFixed[types.Timestamp](bat, pkgcatalog.SystemDBAttr_CreateAt, row),
			replayString(bat, pkgcatalog.SystemDBAttr_CreateSQL, row),
			replayString(bat, pkgcatalog.SystemDBAttr_Type, row),
		)
		return nil
	}); err != nil {
		return err
	}

	var tables []*replayTable
	byKey := make(map[replayTableKey][]*replayTable)
	if err = scan(ctx, tableTbl, readTxn, func(bat *containers.Batch, row int) error {
		if err := requireReplayFields(bat, row, pkgcatalog.SystemRelAttr_ID, pkgcatalog.SystemRelAttr_DBID, pkgcatalog.SystemRelAttr_Name); err != nil {
			return err
		}
		tableDB, err := catalog.GetDatabaseByID(replayFixed[uint64](bat, pkgcatalog.SystemRelAttr_DBID, row))
		if err != nil {
			return err
		}
		table := &replayTable{
			db:     tableDB,
			id:     replayFixed[uint64](bat, pkgcatalog.SystemRelAttr_ID, row),
			schema: NewEmptySchema(replayString(bat, pkgcatalog.SystemRelAttr_Name, row)),
		}
		schema := table.schema
		schema.Comment = replayString(bat, pkgcatalog.SystemRelAttr_Comment, row)
		schema.Version = replayFixed[uint32](bat, pkgcatalog.SystemRelAttr_Version, row)
		schema.CatalogVersion = replayFixed[uint32](bat, pkgcatalog.SystemRelAttr_CatalogVersion, row)
		schema.Partitioned = replayFixed[int8](bat, pkgcatalog.SystemRelAttr_Partitioned, row)
		schema.Partition = replayString(bat, pkgcatalog.SystemRelAttr_Partition, row)
		schema.Relkind = replayString(bat, pkgcatalog.SystemRelAttr_Kind, row)
		schema.Createsql = replayString(bat, pkgcatalog.SystemRelAttr_CreateSQL, row)
		schema.View = replayString(bat, pkgcatalog.SystemRelAttr_ViewDef, row)
		schema.Constraint = bytes.Clone(replayBytes(bat, pkgcatalog.SystemRelAttr_Constraint, row))
		schema.AcInfo.RoleID = replayFixed[uint32](bat, pkgcatalog.SystemRelAttr_Owner, row)
		schema.AcInfo.UserID = replayFixed[uint32](bat, pkgcatalog.SystemRelAttr_Creator, row)
		schema.AcInfo.CreateAt = replayFixed[types.Timestamp](bat, pkgcatalog.SystemRelAttr_CreateAt, row)
		schema.AcInfo.TenantID = replayFixed[uint32](bat, pkgcatalog.SystemRelAttr_AccID, row)
		// Unmarshal while the source block is alive; the schema owns the result.
		if err := schema.restoreExtra(replayBytes(bat, pkgcatalog.SystemRelAttr_ExtraInfo, row)); err != nil {
			return err
		}
		if schema.Extra.BlockMaxRows == 0 {
			schema.Extra.BlockMaxRows = objectio.BlockMaxRows
		}
		if schema.Extra.ObjectMaxBlocks == 0 {
			schema.Extra.ObjectMaxBlocks = uint32(options.DefaultBlocksPerObject)
		}
		tables = append(tables, table)
		key := replayTableKey{id: table.id, name: schema.Name}
		byKey[key] = append(byKey[key], table)
		return nil
	}); err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}
	if err = scan(ctx, columnTbl, readTxn, func(bat *containers.Batch, row int) error {
		if err := requireReplayFields(bat, row, pkgcatalog.SystemColAttr_RelID, pkgcatalog.SystemColAttr_RelName); err != nil {
			return err
		}
		key := replayTableKey{
			id:   replayFixed[uint64](bat, pkgcatalog.SystemColAttr_RelID, row),
			name: replayString(bat, pkgcatalog.SystemColAttr_RelName, row),
		}
		matches := byKey[key]
		if len(matches) == 0 {
			return nil
		}
		if err := requireReplayFields(bat, row, pkgcatalog.SystemColAttr_Name, pkgcatalog.SystemColAttr_Type, pkgcatalog.SystemColAttr_Num); err != nil {
			return err
		}
		for _, table := range matches {
			if err := table.schema.readReplayColumn(bat, row); err != nil {
				return err
			}
			def := table.schema.ColDefs[len(table.schema.ColDefs)-1]
			seqVec := replayVector(bat, pkgcatalog.SystemColAttr_Seqnum)
			if !def.PhyAddr {
				if seqVec == nil || seqVec.IsNull(uint64(row)) {
					table.missingSeq = true
				} else {
					table.nonOrdinalSeq = table.nonOrdinalSeq || int(def.SeqNum) != def.Idx
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// The old replay sorts by table ID before submission. Keep that ordering,
	// and replay multiple visible versions of an ID in version order.
	slices.SortStableFunc(tables, func(a, b *replayTable) int {
		if order := cmp.Compare(a.id, b.id); order != 0 {
			return order
		}
		return cmp.Compare(a.schema.Version, b.schema.Version)
	})
	for _, table := range tables {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		nextSeq := table.schema.Extra.NextColSeqnum
		if nextSeq > uint32(objectio.SEQNUM_UPPER) {
			return moerr.NewInternalErrorNoCtxf("catalog replay table %d has an invalid column sequence high water mark", table.id)
		}
		if nextSeq == 0 && table.nonOrdinalSeq {
			return moerr.NewInternalErrorNoCtxf("catalog replay table %d is missing column sequence metadata", table.id)
		}
		if nextSeq > 0 {
			if table.missingSeq {
				return moerr.NewInternalErrorNoCtxf("catalog replay table %d has a column without a sequence number", table.id)
			}
			for _, def := range table.schema.ColDefs {
				if !def.PhyAddr && uint32(def.SeqNum) >= nextSeq {
					return moerr.NewInternalErrorNoCtxf("catalog replay table %d has a column sequence beyond its high water mark", table.id)
				}
			}
		}
		slices.SortStableFunc(table.schema.ColDefs, func(a, b *ColDef) int {
			return cmp.Compare(a.Idx, b.Idx)
		})
		if err := table.schema.Finalize(true); err != nil {
			return err
		}
		if table.schema.PhyAddrKey == nil || table.schema.PhyAddrKey.Type.Oid != types.T_Rowid {
			return moerr.NewInternalErrorNoCtxf("catalog replay table %d has no valid physical row ID column", table.id)
		}
		if len(table.schema.SeqnumMap) != len(table.schema.ColDefs) {
			return moerr.NewInternalErrorNoCtxf("catalog replay table %d has duplicate column sequence numbers", table.id)
		}
	}
	for _, table := range tables {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		if err := replayer.Submit(ctx, table.db.ID, func() {
			catalog.onReplayCreateTable(table.db, table.id, table.schema, txnNode)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (catalog *Catalog) onReplayCreateDB(
	dbid uint64, name string, txnNode *txnbase.TxnMVCCNode,
	tenantID, userID, roleID uint32, createAt types.Timestamp, createSql, datType string) {
	catalog.OnReplayDBID(dbid)
	db, _ := catalog.GetDatabaseByID(dbid)
	if db != nil {
		dbCreatedAt := db.GetCreatedAtLocked()
		if !dbCreatedAt.Equal(&txnNode.End) {
			panic(moerr.NewInternalErrorNoCtxf("logic err expect %s, get %s",
				txnNode.End.ToString(), dbCreatedAt.ToString()))
		}
		return
	}
	db = NewReplayDBEntry()
	db.catalog = catalog
	db.ID = dbid
	db.DBNode = &DBNode{
		acInfo: accessInfo{
			TenantID: tenantID,
			UserID:   userID,
			RoleID:   roleID,
			CreateAt: createAt,
		},
		createSql: createSql,
		datType:   datType,
		name:      name,
	}
	_ = catalog.AddEntryLocked(db, nil, true)
	un := &MVCCNode[*EmptyMVCCNode]{
		EntryMVCCNode: EntryMVCCNode{
			CreatedAt: txnNode.End,
		},
		TxnMVCCNode: *txnNode,
	}
	db.InsertLocked(un)
}

func (catalog *Catalog) onReplayCreateTable(db *DBEntry, tid uint64, schema *Schema, txnNode *txnbase.TxnMVCCNode) {
	catalog.OnReplayTableID(tid)
	tbl, _ := db.GetTableEntryByID(tid)
	if tbl != nil {
		tblCreatedAt := tbl.GetCreatedAtLocked()
		if tblCreatedAt.GT(&txnNode.End) {
			panic(moerr.NewInternalErrorNoCtxf("logic err expect %s, get %s", txnNode.End.ToString(), tblCreatedAt.ToString()))
		}
		// alter table
		un := &MVCCNode[*TableMVCCNode]{
			EntryMVCCNode: EntryMVCCNode{
				CreatedAt: tblCreatedAt,
			},
			TxnMVCCNode: *txnNode,
			BaseNode: &TableMVCCNode{
				Schema:          schema,
				TombstoneSchema: GetTombstoneSchema(schema),
			},
		}
		tbl.InsertLocked(un)
		if tbl.isColumnChangedInSchema() {
			tbl.FreezeAppend()
		}
		tbl.TableNode.schema.Store(schema)
		if schema.Extra.OldName != "" {
			logutil.Infof("replay rename %v from %v -> %v", tid, schema.Extra.OldName, schema.Name)
			err := tbl.db.replayRenameTable(schema.Extra.OldName, schema.Name, tbl.ID, schema.AcInfo.TenantID, un.GetTxn(), true)
			if err != nil {
				logutil.Warn(schema.String())
				panic(err)
			}
		}

		return
	}
	tbl = NewReplayTableEntry()
	tbl.TableNode = &TableNode{}
	tbl.TableNode.schema.Store(schema)
	tbl.db = db
	tbl.ID = tid
	tbl.dataObjects.SetTableID(tbl.ID)
	tbl.tombstoneObjects.SetTableID(tbl.ID)
	tbl.tableData = catalog.MakeTableFactory()(tbl)
	_ = db.AddEntryLocked(tbl, nil, true)
	un := &MVCCNode[*TableMVCCNode]{
		EntryMVCCNode: EntryMVCCNode{
			CreatedAt: txnNode.End,
		},
		TxnMVCCNode: *txnNode,
		BaseNode: &TableMVCCNode{
			Schema:          schema,
			TombstoneSchema: GetTombstoneSchema(schema),
		},
	}
	tbl.InsertLocked(un)
}
func (catalog *Catalog) OnReplayObjectBatch_V2(
	dbid, tid uint64,
	objectType int8,
	stats objectio.ObjectStats,
	create, delete types.TS,
) {
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		// As replaying only the catalog view at the end time of lastest checkpoint
		// it is normal fot deleted db or table to be missed
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			return
		}
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	rel, err := db.GetTableEntryByID(tid)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			return
		}
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	objID := stats.ObjectName().ObjectId()
	var isTombstone bool
	switch objectType {
	case ioutil.ObjectType_Data:
		isTombstone = false
	case ioutil.ObjectType_Tombstone:
		isTombstone = true
	default:
		panic(fmt.Sprintf("invalid object type %d", objectType))
	}
	obj, err := rel.GetObjectByID(objID, isTombstone)
	if err != nil && !moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
		panic(err)
	}
	if obj == nil {
		obj = &ObjectEntry{
			table: rel,
			ObjectNode: ObjectNode{
				SortHint:    catalog.NextObject(),
				IsTombstone: isTombstone,
				forcePNode:  true, // any object replayed from checkpoint is forced to be created
			},
			EntryMVCCNode: EntryMVCCNode{
				CreatedAt: create,
			},
			ObjectMVCCNode: ObjectMVCCNode{
				ObjectStats: stats,
			},
			CreateNode: txnbase.TxnMVCCNode{
				Start:   create.Prev(),
				Prepare: create,
				End:     create,
			},
			ObjectState: ObjectState_Create_ApplyCommit,
		}
		rel.AddEntryLocked(obj)
		if !delete.IsEmpty() {
			updatedCreate := obj.Clone()
			dropped := obj.Clone()
			dropped.DeletedAt = delete
			dropped.DeleteNode = txnbase.TxnMVCCNode{
				Start:   delete.Prev(),
				Prepare: delete,
				End:     delete,
			}
			dropped.prevVersion = updatedCreate
			updatedCreate.nextVersion = dropped
			dropped.ObjectState = ObjectState_Delete_ApplyCommit
			rel.AddEntryLocked(dropped)
			obj = updatedCreate
		}
	} else {
		if obj.DeletedAt.IsEmpty() && !delete.IsEmpty() {
			updatedCreate := obj.Clone()
			dropped := obj.Clone()
			dropped.DeletedAt = delete
			dropped.DeleteNode = txnbase.TxnMVCCNode{
				Start:   delete.Prev(),
				Prepare: delete,
				End:     delete,
			}
			dropped.prevVersion = updatedCreate
			updatedCreate.nextVersion = dropped
			dropped.ObjectState = ObjectState_Delete_ApplyCommit
			rel.AddEntryLocked(dropped)
			obj = updatedCreate
		}
	}
	if obj.objData == nil {
		obj.objData = catalog.MakeObjectFactory()(obj)
	} else {
		obj.objData.UpdateMeta(obj)
		deleteAt := obj.GetDeleteAt()
		if !obj.IsAppendable() || (obj.IsAppendable() && !deleteAt.IsEmpty()) {
			obj.objData.TryUpgrade()
		}
	}
	// A first-seen delete record creates both versions before initializing data.
	// Both versions must share the same object, as in onReplayCheckpointObject.
	if next := obj.GetNextVersion(); next != nil {
		next.objData = obj.objData
	}
}
func replayCheckpointDeleteObject(
	rel *TableEntry,
	obj *ObjectEntry,
	createTS, deleteTS types.TS,
) *ObjectEntry {
	if obj.IsDEntry() {
		return obj.prevVersion
	}
	if obj.HasDCounterpart() {
		return obj
	}

	updatedCreate := obj.Clone()
	updatedCreate.DeletedAt = types.TS{}
	updatedCreate.DeleteNode = txnbase.TxnMVCCNode{}
	updatedCreate.CreateNode = txnbase.NewTxnMVCCNodeWithTS(createTS)
	updatedCreate.ObjectState = ObjectState_Create_ApplyCommit
	updatedCreate.prevVersion = nil
	updatedCreate.nextVersion = nil

	deleteEntry := updatedCreate.Clone()
	deleteEntry.DeletedAt = deleteTS
	deleteEntry.DeleteNode = txnbase.NewTxnMVCCNodeWithTS(deleteTS)
	deleteEntry.ObjectState = ObjectState_Delete_ApplyCommit
	updatedCreate.nextVersion = deleteEntry
	deleteEntry.prevVersion = updatedCreate

	rel.AddEntryLocked(deleteEntry)
	return updatedCreate
}

func (catalog *Catalog) onReplayCheckpointObject(
	dbid, tbid uint64,
	objid *types.Objectid,
	createTS, deleteTS types.TS,
	start, prepare, end types.TS,
	objNode *ObjectMVCCNode,
	isTombstone bool,
) {
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		// As replaying only the catalog view at the end time of lastest checkpoint
		// it is normal fot deleted db or table to be missed
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			return
		}
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	rel, err := db.GetTableEntryByID(tbid)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			return
		}
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	newObject := func() *ObjectEntry {
		object := NewReplayObjectEntry()
		object.table = rel
		object.ObjectNode = ObjectNode{
			SortHint:    catalog.NextObject(),
			IsTombstone: isTombstone,
		}
		object.EntryMVCCNode = EntryMVCCNode{
			CreatedAt: createTS,
			DeletedAt: deleteTS,
		}
		object.ObjectMVCCNode = *objNode
		object.CreateNode = txnbase.TxnMVCCNode{
			Start:   start,
			Prepare: prepare,
			End:     end,
		}
		object.ObjectState = ObjectState_Create_ApplyCommit
		object.forcePNode = true // any object replayed from checkpoint is forced to be created
		return object
	}
	var obj *ObjectEntry
	if createTS.Equal(&end) {
		obj = newObject()
		rel.AddEntryLocked(obj)
	}
	if deleteTS.Equal(&end) {
		obj, err = rel.GetObjectByID(objid, isTombstone)
		if err != nil {
			panic(fmt.Sprintf("obj %v(%v %v), [%v %v %v %v %v] not existed, table:\n%v", objid.String(),
				createTS.ToString(), deleteTS.ToString(), isTombstone, objNode.String(),
				start.ToString(), prepare.ToString(), end.ToString(), rel.StringWithLevel(3)))
		}
		updatedCreate := obj.Clone()
		deleteNode := obj.Clone()
		updatedCreate.nextVersion = deleteNode
		deleteNode.prevVersion = updatedCreate
		deleteNode.EntryMVCCNode = EntryMVCCNode{
			CreatedAt: createTS,
			DeletedAt: deleteTS,
		}
		deleteNode.ObjectMVCCNode = *objNode
		deleteNode.DeleteNode = txnbase.TxnMVCCNode{
			Start:   start,
			Prepare: prepare,
			End:     end,
		}
		deleteNode.ObjectState = ObjectState_Delete_ApplyCommit
		rel.AddEntryLocked(deleteNode)
		obj = updatedCreate
	}
	if !createTS.Equal(&end) && !deleteTS.Equal(&end) {
		// In back up, aobj is replaced with naobj and its DeleteAt is removed.
		// Before back up, txnNode.End equals DeleteAt of naobj.
		// After back up, DeleteAt is empty.
		if objid.Offset() == Backup_Object_Offset && deleteTS.IsEmpty() {
			obj = newObject()
			rel.AddEntryLocked(obj)
			_, sarg, _ := fault.TriggerFault("back up UT")
			if sarg == "" {
				obj.CreateNode = txnbase.NewTxnMVCCNodeWithTS(obj.CreatedAt)
			}
			logutil.Warnf("obj %v, tbl %v-%d create %v, delete %v, end %v",
				objid.String(), rel.fullName, rel.ID, createTS.ToString(),
				deleteTS.ToString(), end.ToString())
		} else {
			if !deleteTS.IsEmpty() {
				logutil.Warnf("obj %v, tbl %v-%d create %v, delete %v, end %v",
					objid.String(), rel.fullName, rel.ID, createTS.ToString(),
					deleteTS.ToString(), end.ToString())
				obj, _ = rel.GetObjectByID(objid, isTombstone)
				if obj == nil {
					obj = newObject()
					obj.DeletedAt = types.TS{}
					obj.DeleteNode = txnbase.TxnMVCCNode{}
					obj.CreateNode = txnbase.NewTxnMVCCNodeWithTS(createTS)
					rel.AddEntryLocked(obj)
				}
				obj = replayCheckpointDeleteObject(rel, obj, createTS, deleteTS)
			}
		}
	}
	if obj == nil {
		obj, err = rel.GetObjectByID(objid, isTombstone)
		if err != nil {
			panic(fmt.Sprintf("obj %v(%v %v), [%v %v %v %v %v] not existed, table:\n%v", objid.String(),
				createTS.ToString(), deleteTS.ToString(), isTombstone, objNode.String(),
				start.ToString(), prepare.ToString(), end.ToString(), rel.StringWithLevel(3)))
		}
	}
	if obj.objData == nil {
		obj.objData = catalog.MakeObjectFactory()(obj)
	} else {
		obj.objData.UpdateMeta(obj)
		deleteAt := obj.GetDeleteAt()
		if !obj.IsAppendable() || (obj.IsAppendable() && !deleteAt.IsEmpty()) {
			obj.objData.TryUpgrade()
		}
	}
	if next := obj.GetNextVersion(); next != nil {
		next.objData = obj.objData
	}
}

func (catalog *Catalog) ReplayTableRows() {
	processor := new(LoopProcessor)
	processor.TableFn = func(tbl *TableEntry) error {
		if tbl.db.name == pkgcatalog.MO_CATALOG {
			return nil
		}
		rows := uint64(0)
		reader := txnbase.MockTxnReaderWithNow()
		it := tbl.MakeDataVisibleObjectIt(reader)
		defer it.Release()
		for it.Next() {
			rows += it.Item().GetObjectData().GetRowsOnReplay()
		}
		it = tbl.MakeTombstoneVisibleObjectIt(reader)
		defer it.Release()
		for it.Next() {
			rows -= it.Item().GetObjectData().GetRowsOnReplay()
		}
		tbl.rows.Store(rows)
		return nil
	}
	err := catalog.RecurLoop(processor)
	if err != nil {
		panic(err)
	}
}

//#endregion
