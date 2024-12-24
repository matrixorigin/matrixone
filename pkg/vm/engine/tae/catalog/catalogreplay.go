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
	"context"
	"fmt"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

const (
	Backup_Object_Offset uint16 = 1000
)

type ObjectListReplayer interface {
	Submit(uint64, func())
}

//#region Replay WAL related

func (catalog *Catalog) ReplayCmd(
	txncmd txnif.TxnCmd,
	dataFactory DataFactory,
	observer wal.ReplayObserver) {
	switch txncmd.GetType() {
	case txnbase.IOET_WALTxnCommand_Composed:
		cmds := txncmd.(*txnbase.ComposedCmd)
		for _, cmds := range cmds.Cmds {
			catalog.ReplayCmd(cmds, dataFactory, observer)
		}
	case IOET_WALTxnCommand_Database:
		cmd := txncmd.(*EntryCommand[*EmptyMVCCNode, *DBNode])
		catalog.onReplayUpdateDatabase(cmd, observer)
	case IOET_WALTxnCommand_Table:
		cmd := txncmd.(*EntryCommand[*TableMVCCNode, *TableNode])
		catalog.onReplayUpdateTable(cmd, dataFactory, observer)
	case IOET_WALTxnCommand_Object:
		cmd := txncmd.(*EntryCommand[*ObjectMVCCNode, *ObjectNode])
		catalog.onReplayUpdateObject(cmd, dataFactory, observer)
	// case IOET_WALTxnCommand_Block:
	// 	cmd := txncmd.(*EntryCommand[*MetadataMVCCNode, *BlockNode])
	// 	catalog.onReplayUpdateBlock(cmd, dataFactory, observer)
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
		return
	}

	dbun := db.SearchNodeLocked(un)
	if dbun == nil {
		db.InsertLocked(un)
	} else {
		return
		// panic(fmt.Sprintf("logic err: duplicate node %v and %v", dbun.String(), un.String()))
	}
}

func (catalog *Catalog) onReplayUpdateTable(cmd *EntryCommand[*TableMVCCNode, *TableNode], dataFactory DataFactory, _ wal.ReplayObserver) {
	catalog.OnReplayTableID(cmd.ID.TableID)
	// prepareTS := cmd.GetTs()
	// if prepareTS.LessEq(catalog.GetCheckpointed().MaxTS) {
	// 	if observer != nil {
	// 		observer.OnStaleIndex(idx)
	// 	}
	// 	return
	// }
	db, err := catalog.GetDatabaseByID(cmd.ID.DbID)
	if err != nil {
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(cmd.ID.TableID)

	un := cmd.mvccNode
	if err != nil {
		tbl = NewReplayTableEntry()
		tbl.ID = cmd.ID.TableID
		tbl.db = db
		tbl.tableData = dataFactory.MakeTableFactory()(tbl)
		tbl.TableNode = cmd.node
		tbl.TableNode.schema.Store(un.BaseNode.Schema)
		tbl.InsertLocked(un)
		err = db.AddEntryLocked(tbl, un.GetTxn(), true)
		if err != nil {
			logutil.Warn(catalog.SimplePPString(common.PPL3))
			panic(err)
		}
		return
	}
	tblun := tbl.SearchNodeLocked(un)
	if tblun == nil {
		tbl.InsertLocked(un) //TODO isvalid
		if tbl.isColumnChangedInSchema() {
			tbl.FreezeAppend()
		}
		schema := un.BaseNode.Schema
		tbl.TableNode.schema.Store(schema)
		// alter table rename
		if schema.Extra.OldName != "" && un.DeletedAt.IsEmpty() {
			err := tbl.db.RenameTableInTxn(schema.Extra.OldName, schema.Name, tbl.ID, schema.AcInfo.TenantID, un.GetTxn(), true)
			if err != nil {
				logutil.Warn(schema.String())
				panic(err)
			}
		}
	}
}

func (catalog *Catalog) onReplayUpdateObject(
	cmd *EntryCommand[*ObjectMVCCNode, *ObjectNode],
	dataFactory DataFactory,
	_ wal.ReplayObserver) {
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
		obj = NewReplayObjectEntry()
		obj.table = rel
		obj.ObjectNode = *cmd.node
		obj.SortHint = catalog.NextObject()
		obj.EntryMVCCNode = *cmd.mvccNode.EntryMVCCNode
		obj.CreateNode = *cmd.mvccNode.TxnMVCCNode
		cmd.mvccNode.TxnMVCCNode = &obj.CreateNode
		cmd.mvccNode.EntryMVCCNode = &obj.EntryMVCCNode
		cmd.mvccNode.CommitSideEffect = func(ts types.TS) {
			rel.UpdateReplayEntryTs(obj, ts)
		}
		obj.ObjectMVCCNode = *cmd.mvccNode.BaseNode
		obj.ObjectState = ObjectState_Create_ApplyCommit
		rel.AddEntryLocked(obj)
	}
	if cmd.mvccNode.DeletedAt.Equal(&txnif.UncommitTS) {
		cobj, err := rel.GetObjectByID(cmd.ID.ObjectID(), cmd.node.IsTombstone)
		if err != nil {
			panic(fmt.Sprintf("obj %v not existed, table:\n%v", cmd.ID.String(), rel.StringWithLevel(3)))
		}
		obj = cobj.Clone()
		obj.prevVersion = cobj
		cobj.nextVersion = obj
		obj.EntryMVCCNode = *cmd.mvccNode.EntryMVCCNode
		obj.DeleteNode = *cmd.mvccNode.TxnMVCCNode
		obj.ObjectMVCCNode = *cmd.mvccNode.BaseNode
		cmd.mvccNode.TxnMVCCNode = &obj.DeleteNode
		cmd.mvccNode.EntryMVCCNode = &obj.EntryMVCCNode
		cmd.mvccNode.CommitSideEffect = func(ts types.TS) {
			rel.UpdateReplayEntryTs(obj, ts)
		}
		obj.ObjectState = ObjectState_Delete_ApplyCommit
		rel.AddEntryLocked(obj)
	}

	if obj.objData == nil {
		obj.objData = dataFactory.MakeObjectFactory()(obj)
	} else {
		deleteAt := obj.GetDeleteAt()
		if !obj.IsAppendable() || (obj.IsAppendable() && !deleteAt.IsEmpty()) {
			obj.objData.TryUpgrade()
		}
	}
}

//#endregion

//#region Replay Checkpoint related

func (catalog *Catalog) RelayFromSysTableObjects(
	ctx context.Context,
	readTxn txnif.AsyncTxn,
	dataFactory DataFactory,
	readFunc func(context.Context, *TableEntry, txnif.AsyncTxn) *containers.Batch,
	sortFunc func([]containers.Vector, int) error,
) {
	db, err := catalog.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		panic(err)
	}
	dbTbl, err := db.GetTableEntryByID(pkgcatalog.MO_DATABASE_ID)
	if err != nil {
		panic(err)
	}
	tableTbl, err := db.GetTableEntryByID(pkgcatalog.MO_TABLES_ID)
	if err != nil {
		panic(err)
	}
	columnTbl, err := db.GetTableEntryByID(pkgcatalog.MO_COLUMNS_ID)
	if err != nil {
		panic(err)
	}

	////  Note: do not use ckp-end as txnNode
	// Running
	// ------+------+---------+----+-----------+-> time
	//       |      |         |    |           |
	//  create-db-s |         |  ckp-end     drop-db-c
	//         create-db-c  drop-db-s
	//
	// Replay
	// -----------------------+----+-----------+-> time
	//                        |    |           |
	//                  drop-db-s  |          drop-db-c
	//                             ckp-end
	//                             create-db-s
	//                             create-db-c
	// create-db entry was replayed from checkpoint and drop-db entry was replayed from WAL
	// If ckp-end was used, the create-db and drop-db are disordered, leading to ExpectedDup error

	panguEpoch := types.BuildTS(42424242, 0)
	txnNode := &txnbase.TxnMVCCNode{
		Start:   panguEpoch,
		Prepare: panguEpoch,
		End:     panguEpoch,
	}

	// replay database catalog
	if dbBatch := readFunc(ctx, dbTbl, readTxn); dbBatch != nil {
		defer dbBatch.Close()
		catalog.ReplayMODatabase(ctx, txnNode, dbBatch)
	}

	// replay table catalog
	if tableBatch := readFunc(ctx, tableTbl, readTxn); tableBatch != nil {
		if err := sortFunc(tableBatch.Vecs, pkgcatalog.MO_TABLES_REL_ID_IDX); err != nil {
			panic(err)
		}
		defer tableBatch.Close()
		columnBatch := readFunc(ctx, columnTbl, readTxn)
		if err := sortFunc(columnBatch.Vecs, pkgcatalog.MO_COLUMNS_ATT_RELNAME_ID_IDX); err != nil {
			panic(err)
		}
		defer columnBatch.Close()
		catalog.ReplayMOTables(ctx, txnNode, dataFactory, tableBatch, columnBatch)
	}
	// logutil.Info(catalog.SimplePPString(common.PPL3))
}

func (catalog *Catalog) ReplayMODatabase(ctx context.Context, txnNode *txnbase.TxnMVCCNode, bat *containers.Batch) {
	for i := 0; i < bat.Length(); i++ {
		dbid := bat.GetVectorByName(pkgcatalog.SystemDBAttr_ID).Get(i).(uint64)
		name := string(bat.GetVectorByName(pkgcatalog.SystemDBAttr_Name).Get(i).([]byte))
		tenantID := bat.GetVectorByName(pkgcatalog.SystemDBAttr_AccID).Get(i).(uint32)
		userID := bat.GetVectorByName(pkgcatalog.SystemDBAttr_Creator).Get(i).(uint32)
		roleID := bat.GetVectorByName(pkgcatalog.SystemDBAttr_Owner).Get(i).(uint32)
		createAt := bat.GetVectorByName(pkgcatalog.SystemDBAttr_CreateAt).Get(i).(types.Timestamp)
		createSql := string(bat.GetVectorByName(pkgcatalog.SystemDBAttr_CreateSQL).Get(i).([]byte))
		datType := string(bat.GetVectorByName(pkgcatalog.SystemDBAttr_Type).Get(i).([]byte))
		catalog.onReplayCreateDB(dbid, name, txnNode, tenantID, userID, roleID, createAt, createSql, datType)
	}
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
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
	}
	db.InsertLocked(un)
}

func (catalog *Catalog) ReplayMOTables(ctx context.Context, txnNode *txnbase.TxnMVCCNode, dataF DataFactory, tblBat, colBat *containers.Batch) {
	schemaOffset := 0
	for i := 0; i < tblBat.Length(); i++ {
		tid := tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_ID).Get(i).(uint64)
		dbid := tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_DBID).Get(i).(uint64)
		name := string(tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Name).Get(i).([]byte))
		schema := NewEmptySchema(name)
		schemaOffset = schema.ReadFromBatch(colBat, schemaOffset, tid)
		schema.Comment = string(tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Comment).Get(i).([]byte))
		schema.Version = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Version).Get(i).(uint32)
		schema.CatalogVersion = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_CatalogVersion).Get(i).(uint32)
		schema.Partitioned = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Partitioned).Get(i).(int8)
		schema.Partition = string(tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Partition).Get(i).([]byte))
		schema.Relkind = string(tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Kind).Get(i).([]byte))
		schema.Createsql = string(tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_CreateSQL).Get(i).([]byte))
		schema.View = string(tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_ViewDef).Get(i).([]byte))
		schema.Constraint = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Constraint).Get(i).([]byte)
		schema.AcInfo = accessInfo{}
		schema.AcInfo.RoleID = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Owner).Get(i).(uint32)
		schema.AcInfo.UserID = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Creator).Get(i).(uint32)
		schema.AcInfo.CreateAt = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_CreateAt).Get(i).(types.Timestamp)
		schema.AcInfo.TenantID = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_AccID).Get(i).(uint32)
		extra := tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_ExtraInfo).Get(i).([]byte)
		schema.MustRestoreExtra(extra)
		if err := schema.Finalize(true); err != nil {
			panic(err)
		}
		catalog.onReplayCreateTable(dbid, tid, schema, txnNode, dataF)
	}
}

func (catalog *Catalog) onReplayCreateTable(dbid, tid uint64, schema *Schema, txnNode *txnbase.TxnMVCCNode, dataFactory DataFactory) {
	catalog.OnReplayTableID(tid)
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		panic(err)
	}
	tbl, _ := db.GetTableEntryByID(tid)
	if tbl != nil {
		tblCreatedAt := tbl.GetCreatedAtLocked()
		if tblCreatedAt.GT(&txnNode.End) {
			panic(moerr.NewInternalErrorNoCtxf("logic err expect %s, get %s", txnNode.End.ToString(), tblCreatedAt.ToString()))
		}
		// alter table
		un := &MVCCNode[*TableMVCCNode]{
			EntryMVCCNode: &EntryMVCCNode{
				CreatedAt: tblCreatedAt,
			},
			TxnMVCCNode: txnNode,
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
			err := tbl.db.RenameTableInTxn(schema.Extra.OldName, schema.Name, tbl.ID, schema.AcInfo.TenantID, un.GetTxn(), true)
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
	tbl.tableData = dataFactory.MakeTableFactory()(tbl)
	_ = db.AddEntryLocked(tbl, nil, true)
	un := &MVCCNode[*TableMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
		BaseNode: &TableMVCCNode{
			Schema:          schema,
			TombstoneSchema: GetTombstoneSchema(schema),
		},
	}
	tbl.InsertLocked(un)
}

func (catalog *Catalog) OnReplayObjectBatch(replayer ObjectListReplayer, objectInfo *containers.Batch, isTombstone bool, dataFactory DataFactory, forSys bool) {
	tids := vector.MustFixedColNoTypeCheck[uint64](objectInfo.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	dbids := vector.MustFixedColNoTypeCheck[uint64](objectInfo.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector())
	commitTSs := vector.MustFixedColNoTypeCheck[types.TS](objectInfo.GetVectorByName(txnbase.SnapshotAttr_CommitTS).GetDownstreamVector())
	prepareTSs := vector.MustFixedColNoTypeCheck[types.TS](objectInfo.GetVectorByName(txnbase.SnapshotAttr_PrepareTS).GetDownstreamVector())
	startTSs := vector.MustFixedColNoTypeCheck[types.TS](objectInfo.GetVectorByName(txnbase.SnapshotAttr_StartTS).GetDownstreamVector())
	createTSs := vector.MustFixedColNoTypeCheck[types.TS](objectInfo.GetVectorByName(EntryNode_CreateAt).GetDownstreamVector())
	deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](objectInfo.GetVectorByName(EntryNode_DeleteAt).GetDownstreamVector())
	for i, tid := range tids {
		if forSys != pkgcatalog.IsSystemTable(tid) {
			continue
		}
		replayFn := func() {
			dbid := dbids[i]
			objectNode := ReadObjectInfoTuple(objectInfo, i)
			sid := objectNode.ObjectName().ObjectId()
			catalog.onReplayCheckpointObject(
				dbid, tid, sid, createTSs[i], deleteTSs[i], startTSs[i], prepareTSs[i], commitTSs[i], objectNode, isTombstone, dataFactory)
		}
		replayer.Submit(tid, replayFn)
	}
}

func (catalog *Catalog) onReplayCheckpointObject(
	dbid, tbid uint64,
	objid *types.Objectid,
	createTS, deleteTS types.TS,
	start, prepare, end types.TS,
	objNode *ObjectMVCCNode,
	isTombstone bool,
	dataFactory DataFactory,
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
		deleteNode := obj.Clone()
		obj.nextVersion = deleteNode
		deleteNode.prevVersion = obj
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
				obj.CreateNode = *txnbase.NewTxnMVCCNodeWithTS(obj.CreatedAt)
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
					rel.AddEntryLocked(obj)
				}
				obj.CreateNode = *txnbase.NewTxnMVCCNodeWithTS(createTS)
				obj.DeleteNode = *txnbase.NewTxnMVCCNodeWithTS(deleteTS)
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
		obj.objData = dataFactory.MakeObjectFactory()(obj)
	} else {
		deleteAt := obj.GetDeleteAt()
		if !obj.IsAppendable() || (obj.IsAppendable() && !deleteAt.IsEmpty()) {
			obj.objData.TryUpgrade()
		}
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
