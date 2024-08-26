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
	"fmt"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"

	"go.uber.org/zap"
)

const (
	Backup_Object_Offset uint16 = 1000
)

//#region Replay related

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

func (catalog *Catalog) onReplayUpdateDatabase(cmd *EntryCommand[*EmptyMVCCNode, *DBNode], observer wal.ReplayObserver) {
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
		err = catalog.AddEntryLocked(db, un.GetTxn(), false)
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

func (catalog *Catalog) OnReplayDatabaseBatch(ins, insTxn, del, delTxn *containers.Batch) {
	for i := 0; i < ins.Length(); i++ {
		dbid := ins.GetVectorByName(pkgcatalog.SystemDBAttr_ID).Get(i).(uint64)
		name := string(ins.GetVectorByName(pkgcatalog.SystemDBAttr_Name).Get(i).([]byte))
		txnNode := txnbase.ReadTuple(insTxn, i)
		tenantID := ins.GetVectorByName(pkgcatalog.SystemDBAttr_AccID).Get(i).(uint32)
		userID := ins.GetVectorByName(pkgcatalog.SystemDBAttr_Creator).Get(i).(uint32)
		roleID := ins.GetVectorByName(pkgcatalog.SystemDBAttr_Owner).Get(i).(uint32)
		createAt := ins.GetVectorByName(pkgcatalog.SystemDBAttr_CreateAt).Get(i).(types.Timestamp)
		createSql := string(ins.GetVectorByName(pkgcatalog.SystemDBAttr_CreateSQL).Get(i).([]byte))
		datType := string(ins.GetVectorByName(pkgcatalog.SystemDBAttr_Type).Get(i).([]byte))
		catalog.onReplayCreateDB(dbid, name, txnNode, tenantID, userID, roleID, createAt, createSql, datType)
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		txnNode := txnbase.ReadTuple(delTxn, i)
		catalog.onReplayDeleteDB(dbid, txnNode)
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
func (catalog *Catalog) onReplayDeleteDB(dbid uint64, txnNode *txnbase.TxnMVCCNode) {
	catalog.OnReplayDBID(dbid)
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Info("delete %d", zap.Uint64("dbid", dbid), zap.String("catalog pp", catalog.SimplePPString(common.PPL3)))
		panic(err)
	}
	dbDeleteAt := db.GetDeleteAtLocked()
	if !dbDeleteAt.IsEmpty() {
		if !dbDeleteAt.Equal(&txnNode.End) {
			panic(moerr.NewInternalErrorNoCtxf("logic err expect %s, get %s", txnNode.End.ToString(), dbDeleteAt.ToString()))
		}
		return
	}
	prev := db.MVCCChain.GetLatestNodeLocked()
	un := &MVCCNode[*EmptyMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: db.GetCreatedAtLocked(),
			DeletedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
		BaseNode:    prev.BaseNode.CloneAll(),
	}
	db.InsertLocked(un)
}
func (catalog *Catalog) onReplayUpdateTable(cmd *EntryCommand[*TableMVCCNode, *TableNode], dataFactory DataFactory, observer wal.ReplayObserver) {
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

func (catalog *Catalog) OnReplayTableBatch(ins, insTxn, insCol, del, delTxn *containers.Batch, dataFactory DataFactory) {
	schemaOffset := 0
	for i := 0; i < ins.Length(); i++ {
		tid := ins.GetVectorByName(pkgcatalog.SystemRelAttr_ID).Get(i).(uint64)
		dbid := ins.GetVectorByName(pkgcatalog.SystemRelAttr_DBID).Get(i).(uint64)
		name := string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Name).Get(i).([]byte))
		schema := NewEmptySchema(name)
		schemaOffset = schema.ReadFromBatch(insCol, schemaOffset, tid)
		schema.Comment = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Comment).Get(i).([]byte))
		schema.Version = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Version).Get(i).(uint32)
		schema.CatalogVersion = ins.GetVectorByName(pkgcatalog.SystemRelAttr_CatalogVersion).Get(i).(uint32)
		schema.Partitioned = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Partitioned).Get(i).(int8)
		schema.Partition = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Partition).Get(i).([]byte))
		schema.Relkind = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Kind).Get(i).([]byte))
		schema.Createsql = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_CreateSQL).Get(i).([]byte))
		schema.View = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_ViewDef).Get(i).([]byte))
		schema.Constraint = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Constraint).Get(i).([]byte)
		schema.AcInfo = accessInfo{}
		schema.AcInfo.RoleID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Owner).Get(i).(uint32)
		schema.AcInfo.UserID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Creator).Get(i).(uint32)
		schema.AcInfo.CreateAt = ins.GetVectorByName(pkgcatalog.SystemRelAttr_CreateAt).Get(i).(types.Timestamp)
		schema.AcInfo.TenantID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_AccID).Get(i).(uint32)
		schema.BlockMaxRows = insTxn.GetVectorByName(SnapshotAttr_BlockMaxRow).Get(i).(uint32)
		schema.ObjectMaxBlocks = insTxn.GetVectorByName(SnapshotAttr_ObjectMaxBlock).Get(i).(uint16)
		extra := insTxn.GetVectorByName(SnapshotAttr_SchemaExtra).Get(i).([]byte)
		schema.MustRestoreExtra(extra)
		schema.Finalize(true)
		txnNode := txnbase.ReadTuple(insTxn, i)
		catalog.onReplayCreateTable(dbid, tid, schema, txnNode, dataFactory)
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		txnNode := txnbase.ReadTuple(delTxn, i)
		catalog.onReplayDeleteTable(dbid, tid, txnNode)
	}
}

func (catalog *Catalog) onReplayCreateTable(dbid, tid uint64, schema *Schema, txnNode *txnbase.TxnMVCCNode, dataFactory DataFactory) {
	catalog.OnReplayTableID(tid)
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	tbl, _ := db.GetTableEntryByID(tid)
	if tbl != nil {
		tblCreatedAt := tbl.GetCreatedAtLocked()
		if tblCreatedAt.Greater(&txnNode.End) {
			panic(moerr.NewInternalErrorNoCtxf("logic err expect %s, get %s", txnNode.End.ToString(), tblCreatedAt.ToString()))
		}
		// alter table
		un := &MVCCNode[*TableMVCCNode]{
			EntryMVCCNode: &EntryMVCCNode{
				CreatedAt: tblCreatedAt,
			},
			TxnMVCCNode: txnNode,
			BaseNode: &TableMVCCNode{
				Schema: schema,
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
			Schema: schema,
		},
	}
	tbl.InsertLocked(un)
}
func (catalog *Catalog) onReplayDeleteTable(dbid, tid uint64, txnNode *txnbase.TxnMVCCNode) {
	catalog.OnReplayTableID(tid)
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(tid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	tableDeleteAt := tbl.GetDeleteAtLocked()
	if !tableDeleteAt.IsEmpty() {
		if !tableDeleteAt.Equal(&txnNode.End) {
			panic(moerr.NewInternalErrorNoCtxf("logic err expect %s, get %s", txnNode.End.ToString(), tableDeleteAt.ToString()))
		}
		return
	}
	prev := tbl.MVCCChain.GetLatestCommittedNodeLocked()
	un := &MVCCNode[*TableMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: prev.CreatedAt,
			DeletedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
		BaseNode:    prev.BaseNode.CloneAll(),
	}
	tbl.InsertLocked(un)

}
func (catalog *Catalog) onReplayUpdateObject(
	cmd *EntryCommand[*ObjectMVCCNode, *ObjectNode],
	dataFactory DataFactory,
	observer wal.ReplayObserver) {
	catalog.OnReplayObjectID(cmd.node.SortHint)

	db, err := catalog.GetDatabaseByID(cmd.ID.DbID)
	if err != nil {
		panic(err)
	}
	rel, err := db.GetTableEntryByID(cmd.ID.TableID)
	if err != nil {
		logutil.Debugf("tbl %d-%d", cmd.ID.DbID, cmd.ID.TableID)
		logutil.Info(catalog.SimplePPString(3))
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
		obj.ObjectMVCCNode = *cmd.mvccNode.BaseNode
		obj.ObjectState = ObjectState_Create_ApplyCommit
		rel.AddEntryLocked(obj)
	}
	if cmd.mvccNode.DeletedAt.Equal(&txnif.UncommitTS) {
		obj, err = rel.GetObjectByID(cmd.ID.ObjectID(), cmd.node.IsTombstone)
		if err != nil {
			panic(fmt.Sprintf("obj %v not existed, table:\n%v", cmd.ID.String(), rel.StringWithLevel(3)))
		}
		obj.EntryMVCCNode = *cmd.mvccNode.EntryMVCCNode
		obj.DeleteNode = *cmd.mvccNode.TxnMVCCNode
		obj.ObjectMVCCNode = *cmd.mvccNode.BaseNode
		cmd.mvccNode.TxnMVCCNode = &obj.DeleteNode
		cmd.mvccNode.EntryMVCCNode = &obj.EntryMVCCNode
		obj.ObjectState = ObjectState_Delete_ApplyCommit
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

func (catalog *Catalog) OnReplayObjectBatch(objectInfo *containers.Batch, isTombstone bool, dataFactory DataFactory) {
	dbidVec := objectInfo.GetVectorByName(SnapshotAttr_DBID)
	for i := 0; i < dbidVec.Length(); i++ {
		dbid := objectInfo.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := objectInfo.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		objectNode := ReadObjectInfoTuple(objectInfo, i)
		sid := objectNode.ObjectName().ObjectId()
		txnNode := txnbase.ReadTuple(objectInfo, i)
		entryNode := ReadEntryNodeTuple(objectInfo, i)
		state := objectInfo.GetVectorByName(ObjectAttr_State).Get(i).(bool)
		entryState := ES_Appendable
		if !state {
			entryState = ES_NotAppendable
		}
		catalog.onReplayCheckpointObject(dbid, tid, sid, objectNode, entryNode, txnNode, entryState, isTombstone, dataFactory)
	}
}

func (catalog *Catalog) onReplayCheckpointObject(
	dbid, tbid uint64,
	objid *types.Objectid,
	objNode *ObjectMVCCNode,
	entryNode *EntryMVCCNode,
	txnNode *txnbase.TxnMVCCNode,
	state EntryState,
	isTombstone bool,
	dataFactory DataFactory,
) {
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	rel, err := db.GetTableEntryByID(tbid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	newObject := func() *ObjectEntry {
		object := NewReplayObjectEntry()
		object.table = rel
		object.ObjectNode = ObjectNode{
			state:       state,
			sorted:      state == ES_NotAppendable,
			SortHint:    catalog.NextObject(),
			IsTombstone: isTombstone,
		}
		object.EntryMVCCNode = *entryNode
		object.ObjectMVCCNode = *objNode
		object.CreateNode = *txnNode
		object.ObjectState = ObjectState_Create_ApplyCommit
		return object
	}
	var obj *ObjectEntry
	if entryNode.CreatedAt.Equal(&txnNode.End) {
		obj = newObject()
		rel.AddEntryLocked(obj)
	}
	if entryNode.DeletedAt.Equal(&txnNode.End) {
		obj, err = rel.GetObjectByID(objid, isTombstone)
		if err != nil {
			panic(fmt.Sprintf("obj %v(%v), [%v %v %v] not existed, table:\n%v", objid.String(),
				entryNode.String(), isTombstone, objNode.String(),
				txnNode.String(), rel.StringWithLevel(3)))
		}
		obj.EntryMVCCNode = *entryNode
		obj.ObjectMVCCNode = *objNode
		obj.DeleteNode = *txnNode
		obj.ObjectState = ObjectState_Delete_ApplyCommit
	}
	if !entryNode.CreatedAt.Equal(&txnNode.End) && !entryNode.DeletedAt.Equal(&txnNode.End) {
		// In back up, aobj is replaced with naobj and its DeleteAt is removed.
		// Before back up, txnNode.End equals DeleteAt of naobj.
		// After back up, DeleteAt is empty.
		if objid.Offset() == Backup_Object_Offset && entryNode.DeletedAt.IsEmpty() {
			obj = newObject()
			rel.AddEntryLocked(obj)
			logutil.Warnf("obj %v, tbl %v-%d delete %v, create %v, end %v",
				objid.String(), rel.fullName, rel.ID, entryNode.CreatedAt.ToString(),
				entryNode.DeletedAt.ToString(), txnNode.End.ToString())
		} else {
			if !entryNode.DeletedAt.IsEmpty() {
				panic(fmt.Sprintf("logic error: obj %v, tbl %v-%d create %v, delete %v, end %v",
					objid.String(), rel.fullName, rel.ID, entryNode.CreatedAt.ToString(),
					entryNode.DeletedAt.ToString(), txnNode.End.ToString()))
			}
		}
	}
	if obj == nil {
		obj, err = rel.GetObjectByID(objid, isTombstone)
		if err != nil {
			panic(fmt.Sprintf("obj %v(%v), [%v %v %v] not existed, table:\n%v", objid.String(),
				entryNode.String(), isTombstone, objNode.String(),
				txnNode.String(), rel.StringWithLevel(3)))
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
	rows := uint64(0)
	tableProcessor := new(LoopProcessor)
	tableProcessor.ObjectFn = func(be *ObjectEntry) error {
		if !be.IsActive() {
			return nil
		}
		rows += be.GetObjectData().GetRowsOnReplay()
		return nil
	}
	tableProcessor.TombstoneFn = func(be *ObjectEntry) error {
		if !be.IsActive() {
			return nil
		}
		rows -= be.GetObjectData().GetRowsOnReplay()
		return nil
	}
	processor := new(LoopProcessor)
	processor.TableFn = func(tbl *TableEntry) error {
		if tbl.db.name == pkgcatalog.MO_CATALOG {
			return nil
		}
		rows = 0
		err := tbl.RecurLoop(tableProcessor)
		if err != nil {
			panic(err)
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
