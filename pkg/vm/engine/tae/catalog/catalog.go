// Copyright 2021 Matrix Origin
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

package catalog

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	// "time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

// +--------+---------+----------+----------+------------+
// |   ID   |  Name   | CreateAt | DeleteAt | CommitInfo |
// +--------+---------+----------+----------+------------+
// |(uint64)|(varchar)| (uint64) | (uint64) |  (varchar) |
// +--------+---------+----------+----------+------------+
const (
	SnapshotAttr_SegID           = "segment_id"
	SnapshotAttr_TID             = "table_id"
	SnapshotAttr_DBID            = "db_id"
	SegmentAttr_ID               = "id"
	SegmentAttr_CreateAt         = "create_at"
	SegmentAttr_State            = "state"
	SnapshotAttr_BlockMaxRow     = "block_max_row"
	SnapshotAttr_SegmentMaxBlock = "segment_max_block"
)

type DataFactory interface {
	MakeTableFactory() TableDataFactory
	MakeSegmentFactory() SegmentDataFactory
	MakeBlockFactory() BlockDataFactory
}

func rowIDToU64(rowID types.Rowid) uint64 {
	return types.DecodeUint64(rowID[:8])
}

type Catalog struct {
	*IDAlloctor
	*sync.RWMutex

	scheduler tasks.TaskScheduler

	entries   map[uint64]*common.GenericDLNode[*DBEntry]
	nameNodes map[string]*nodeList[*DBEntry]
	link      *common.GenericSortedDList[*DBEntry]

	nodesMu sync.RWMutex

	tableCnt  atomic.Int32
	columnCnt atomic.Int32
}

func genDBFullName(tenantID uint32, name string) string {
	if name == pkgcatalog.MO_CATALOG {
		tenantID = 0
	}
	return fmt.Sprintf("%d-%s", tenantID, name)
}

func compareDBFn(a, b *DBEntry) int {
	return a.DBBaseEntry.DoCompre(b.DBBaseEntry)
}

func MockCatalog(scheduler tasks.TaskScheduler) *Catalog {
	catalog := &Catalog{
		RWMutex:    new(sync.RWMutex),
		IDAlloctor: NewIDAllocator(),
		entries:    make(map[uint64]*common.GenericDLNode[*DBEntry]),
		nameNodes:  make(map[string]*nodeList[*DBEntry]),
		link:       common.NewGenericSortedDList(compareDBFn),
		scheduler:  scheduler,
	}
	catalog.InitSystemDB()
	return catalog
}

func NewEmptyCatalog() *Catalog {
	return &Catalog{
		RWMutex:    new(sync.RWMutex),
		IDAlloctor: NewIDAllocator(),
		entries:    make(map[uint64]*common.GenericDLNode[*DBEntry]),
		nameNodes:  make(map[string]*nodeList[*DBEntry]),
		link:       common.NewGenericSortedDList(compareDBFn),
		scheduler:  nil,
	}
}

func OpenCatalog(scheduler tasks.TaskScheduler, dataFactory DataFactory) (*Catalog, error) {
	catalog := &Catalog{
		RWMutex:    new(sync.RWMutex),
		IDAlloctor: NewIDAllocator(),
		entries:    make(map[uint64]*common.GenericDLNode[*DBEntry]),
		nameNodes:  make(map[string]*nodeList[*DBEntry]),
		link:       common.NewGenericSortedDList(compareDBFn),
		scheduler:  scheduler,
	}
	catalog.InitSystemDB()
	return catalog, nil
}

func (catalog *Catalog) InitSystemDB() {
	sysDB := NewSystemDBEntry(catalog)
	dbTables := NewSystemTableEntry(sysDB, pkgcatalog.MO_DATABASE_ID, SystemDBSchema)
	tableTables := NewSystemTableEntry(sysDB, pkgcatalog.MO_TABLES_ID, SystemTableSchema)
	columnTables := NewSystemTableEntry(sysDB, pkgcatalog.MO_COLUMNS_ID, SystemColumnSchema)
	err := sysDB.AddEntryLocked(dbTables, nil, false)
	if err != nil {
		panic(err)
	}
	if err = sysDB.AddEntryLocked(tableTables, nil, false); err != nil {
		panic(err)
	}
	if err = sysDB.AddEntryLocked(columnTables, nil, false); err != nil {
		panic(err)
	}
	if err = catalog.AddEntryLocked(sysDB, nil, false); err != nil {
		panic(err)
	}
}

func (catalog *Catalog) ReplayCmd(
	txncmd txnif.TxnCmd,
	dataFactory DataFactory,
	idxCtx *wal.Index,
	observer wal.ReplayObserver) {
	switch txncmd.GetType() {
	case txnbase.CmdComposed:
		cmds := txncmd.(*txnbase.ComposedCmd)
		idxCtx.Size = cmds.CmdSize
		for i, cmds := range cmds.Cmds {
			idx := idxCtx.Clone()
			idx.CSN = uint32(i)
			catalog.ReplayCmd(cmds, dataFactory, idx, observer)
		}
	case CmdUpdateDatabase:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayUpdateDatabase(cmd, idxCtx, observer)
	case CmdUpdateTable:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayUpdateTable(cmd, dataFactory, idxCtx, observer)
	case CmdUpdateSegment:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayUpdateSegment(cmd, dataFactory, idxCtx, observer)
	case CmdUpdateBlock:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayUpdateBlock(cmd, dataFactory, idxCtx, observer)
	default:
		panic("unsupport")
	}
}

func (catalog *Catalog) onReplayUpdateDatabase(cmd *EntryCommand, idx *wal.Index, observer wal.ReplayObserver) {
	catalog.OnReplayDBID(cmd.DB.ID)
	// prepareTS := cmd.GetTs()
	// if prepareTS.LessEq(catalog.GetCheckpointed().MaxTS) {
	// 	if observer != nil {
	// 		observer.OnStaleIndex(idx)
	// 	}
	// 	return
	// }
	var err error
	un := cmd.entry.GetLatestNodeLocked().(*DBMVCCNode)
	un.SetLogIndex(idx)
	if un.Is1PC() {
		if err := un.ApplyCommit(nil); err != nil {
			panic(err)
		}
	}

	db, err := catalog.GetDatabaseByID(cmd.entry.GetID())
	if err != nil {
		cmd.DB.RWMutex = new(sync.RWMutex)
		cmd.DB.catalog = catalog
		cmd.entry.GetLatestNodeLocked().SetLogIndex(idx)
		err = catalog.AddEntryLocked(cmd.DB, un.GetTxn(), false)
		if err != nil {
			panic(err)
		}
		return
	}

	dbun := db.SearchNode(un)
	if dbun == nil {
		db.Insert(un)
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
		catalog.onReplayCreateDB(dbid, name, txnNode, tenantID, userID, roleID, createAt)
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		txnNode := txnbase.ReadTuple(delTxn, i)
		catalog.onReplayDeleteDB(dbid, txnNode)
	}
}

func (catalog *Catalog) onReplayCreateDB(dbid uint64, name string, txnNode *txnbase.TxnMVCCNode, tenantID, userID, roleID uint32, createAt types.Timestamp) {
	catalog.OnReplayDBID(dbid)
	db, _ := catalog.GetDatabaseByID(dbid)
	if db != nil {
		dbCreatedAt := db.GetCreatedAt()
		if !dbCreatedAt.Equal(txnNode.End) {
			panic(moerr.NewInternalError("logic err expect %s, get %s", txnNode.End.ToString(), dbCreatedAt.ToString()))
		}
		return
	}
	db = NewReplayDBEntry()
	db.catalog = catalog
	db.ID = dbid
	db.name = name
	db.acInfo = accessInfo{
		TenantID: tenantID,
		UserID:   userID,
		RoleID:   roleID,
		CreateAt: createAt,
	}
	_ = catalog.AddEntryLocked(db, nil, true)
	un := &DBMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
	}
	db.Insert(un)
}
func (catalog *Catalog) onReplayDeleteDB(dbid uint64, txnNode *txnbase.TxnMVCCNode) {
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Infof("delete %d", dbid)
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	dbDeleteAt := db.GetDeleteAt()
	if !dbDeleteAt.IsEmpty() {
		if !dbDeleteAt.Equal(txnNode.End) {
			panic(moerr.NewInternalError("logic err expect %s, get %s", txnNode.End.ToString(), dbDeleteAt.ToString()))
		}
		return
	}
	un := &DBMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: db.GetCreatedAt(),
			DeletedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
	}
	db.Insert(un)
}
func (catalog *Catalog) onReplayUpdateTable(cmd *EntryCommand, dataFactory DataFactory, idx *wal.Index, observer wal.ReplayObserver) {
	catalog.OnReplayTableID(cmd.Table.ID)
	// prepareTS := cmd.GetTs()
	// if prepareTS.LessEq(catalog.GetCheckpointed().MaxTS) {
	// 	if observer != nil {
	// 		observer.OnStaleIndex(idx)
	// 	}
	// 	return
	// }
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(cmd.Table.ID)

	un := cmd.entry.GetLatestNodeLocked().(*TableMVCCNode)
	un.SetLogIndex(idx)
	if un.Is1PC() {
		if err := un.ApplyCommit(nil); err != nil {
			panic(err)
		}
	}

	if err != nil {
		cmd.Table.db = db
		cmd.Table.tableData = dataFactory.MakeTableFactory()(cmd.Table)
		err = db.AddEntryLocked(cmd.Table, un.GetTxn(), false)
		if err != nil {
			logutil.Warn(catalog.SimplePPString(common.PPL3))
			panic(err)
		}
		return
	}
	tblun := tbl.SearchNode(un)
	if tblun == nil {
		tbl.Insert(un) //TODO isvalid
		// } else {
		// 	panic(fmt.Sprintf("duplicate node %v and %v", tblun, un))
	}

}

func (catalog *Catalog) OnReplayTableBatch(ins, insTxn, insCol, del, delTxn *containers.Batch, dataFactory DataFactory) {
	schemaOffset := 0
	for i := 0; i < ins.Length(); i++ {
		tid := ins.GetVectorByName(pkgcatalog.SystemRelAttr_ID).Get(i).(uint64)
		dbid := ins.GetVectorByName(pkgcatalog.SystemRelAttr_DBID).Get(i).(uint64)
		name := string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Name).Get(i).([]byte))
		schema := NewEmptySchema(name)
		schemaOffset = schema.ReadFromBatch(insCol, schemaOffset)
		schema.Comment = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Comment).Get(i).([]byte))
		schema.Partition = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Partition).Get(i).([]byte))
		schema.Relkind = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Kind).Get(i).([]byte))
		schema.Createsql = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_CreateSQL).Get(i).([]byte))
		schema.View = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_ViewDef).Get(i).([]byte))
		schema.AcInfo = accessInfo{}
		schema.AcInfo.RoleID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Owner).Get(i).(uint32)
		schema.AcInfo.UserID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Creator).Get(i).(uint32)
		schema.AcInfo.CreateAt = ins.GetVectorByName(pkgcatalog.SystemRelAttr_CreateAt).Get(i).(types.Timestamp)
		schema.AcInfo.TenantID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_AccID).Get(i).(uint32)
		schema.BlockMaxRows = insTxn.GetVectorByName(SnapshotAttr_BlockMaxRow).Get(i).(uint32)
		schema.SegmentMaxBlocks = insTxn.GetVectorByName(SnapshotAttr_SegmentMaxBlock).Get(i).(uint16)
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
		tblCreatedAt := tbl.GetCreatedAt()
		if !tblCreatedAt.Equal(txnNode.End) {
			panic(moerr.NewInternalError("logic err expect %s, get %s", txnNode.End.ToString(), tblCreatedAt.ToString()))
		}
		return
	}
	tbl = NewReplayTableEntry()
	tbl.schema = schema
	tbl.db = db
	tbl.ID = tid
	tbl.tableData = dataFactory.MakeTableFactory()(tbl)
	_ = db.AddEntryLocked(tbl, nil, true)
	un := &TableMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
	}
	tbl.Insert(un)
}
func (catalog *Catalog) onReplayDeleteTable(dbid, tid uint64, txnNode *txnbase.TxnMVCCNode) {
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
	tableDeleteAt := tbl.GetDeleteAt()
	if !tableDeleteAt.IsEmpty() {
		if !tableDeleteAt.Equal(txnNode.End) {
			panic(moerr.NewInternalError("logic err expect %s, get %s", txnNode.End.ToString(), tableDeleteAt.ToString()))
		}
		return
	}
	prev := tbl.MVCCChain.GetLatestCommittedNode().(*TableMVCCNode)
	un := &TableMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: prev.CreatedAt,
			DeletedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
	}
	tbl.Insert(un)

}
func (catalog *Catalog) onReplayUpdateSegment(
	cmd *EntryCommand,
	dataFactory DataFactory,
	idx *wal.Index,
	observer wal.ReplayObserver) {
	catalog.OnReplaySegmentID(cmd.Segment.ID)
	// prepareTS := cmd.GetTs()
	// if prepareTS.LessEq(catalog.GetCheckpointed().MaxTS) {
	// 	if observer != nil {
	// 		observer.OnStaleIndex(idx)
	// 	}
	// 	return
	// }

	un := cmd.entry.GetLatestNodeLocked().(*MetadataMVCCNode)
	un.SetLogIndex(idx)
	if un.Is1PC() {
		if err := un.ApplyCommit(nil); err != nil {
			panic(err)
		}
	}
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		logutil.Infof("tbl %d-%d", cmd.DBID, cmd.TableID)
		logutil.Info(catalog.SimplePPString(3))
		panic(err)
	}
	seg, err := tbl.GetSegmentByID(cmd.Segment.ID)
	if err != nil {
		cmd.Segment.table = tbl
		cmd.Segment.RWMutex = new(sync.RWMutex)
		cmd.Segment.segData = dataFactory.MakeSegmentFactory()(cmd.Segment)
		tbl.AddEntryLocked(cmd.Segment)
	} else {
		node := seg.SearchNode(un)
		if node == nil {
			seg.Insert(un)
		} else {
			node.Update(un)
		}
	}
}

func (catalog *Catalog) OnReplaySegmentBatch(ins, insTxn, del, delTxn *containers.Batch, dataFactory DataFactory) {
	idVec := ins.GetVectorByName(SegmentAttr_ID)
	for i := 0; i < idVec.Length(); i++ {
		dbid := insTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := insTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		appendable := ins.GetVectorByName(SegmentAttr_State).Get(i).(bool)
		state := ES_NotAppendable
		if appendable {
			state = ES_Appendable
		}
		sid := ins.GetVectorByName(SegmentAttr_ID).Get(i).(uint64)
		txnNode := txnbase.ReadTuple(insTxn, i)
		catalog.onReplayCreateSegment(dbid, tid, sid, state, txnNode, dataFactory)
	}
	idVec = delTxn.GetVectorByName(SnapshotAttr_DBID)
	for i := 0; i < idVec.Length(); i++ {
		dbid := delTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		sid := del.GetVectorByName(AttrRowID).Get(i).(types.Rowid)
		txnNode := txnbase.ReadTuple(delTxn, i)
		catalog.onReplayDeleteSegment(dbid, tid, rowIDToU64(sid), txnNode)
	}
}
func (catalog *Catalog) onReplayCreateSegment(dbid, tbid, segid uint64, state EntryState, txnNode *txnbase.TxnMVCCNode, dataFactory DataFactory) {
	catalog.OnReplaySegmentID(segid)
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
	seg, _ := rel.GetSegmentByID(segid)
	if seg != nil {
		segCreatedAt := seg.GetCreatedAt()
		if !segCreatedAt.Equal(txnNode.End) {
			panic(moerr.NewInternalError("logic err expect %s, get %s", txnNode.End.ToString(), segCreatedAt.ToString()))
		}
		return
	}
	seg = NewReplaySegmentEntry()
	seg.table = rel
	seg.ID = segid
	seg.state = state
	seg.segData = dataFactory.MakeSegmentFactory()(seg)
	rel.AddEntryLocked(seg)
	un := &MetadataMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
	}
	seg.Insert(un)
}
func (catalog *Catalog) onReplayDeleteSegment(dbid, tbid, segid uint64, txnNode *txnbase.TxnMVCCNode) {
	catalog.OnReplaySegmentID(segid)
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
	seg, err := rel.GetSegmentByID(segid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	segDeleteAt := seg.GetDeleteAt()
	if !segDeleteAt.IsEmpty() {
		if !segDeleteAt.Equal(txnNode.End) {
			panic(moerr.NewInternalError("logic err expect %s, get %s", txnNode.End.ToString(), segDeleteAt.ToString()))
		}
		return
	}
	prevUn := seg.MVCCChain.GetLatestNodeLocked().(*MetadataMVCCNode)
	un := &MetadataMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: prevUn.CreatedAt,
			DeletedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
	}
	seg.Insert(un)
}
func (catalog *Catalog) onReplayUpdateBlock(cmd *EntryCommand,
	dataFactory DataFactory,
	idx *wal.Index,
	observer wal.ReplayObserver) {
	catalog.OnReplayBlockID(cmd.Block.ID)
	prepareTS := cmd.GetTs()
	// if prepareTS.LessEq(catalog.GetCheckpointed().MaxTS) {
	// 	if observer != nil {
	// 		observer.OnStaleIndex(idx)
	// 	}
	// 	return
	// }
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		panic(err)
	}
	seg, err := tbl.GetSegmentByID(cmd.SegmentID)
	if err != nil {
		panic(err)
	}
	blk, err := seg.GetBlockEntryByID(cmd.Block.ID)
	un := cmd.entry.GetLatestNodeLocked().(*MetadataMVCCNode)
	un.SetLogIndex(idx)
	if un.Is1PC() {
		if err := un.ApplyCommit(nil); err != nil {
			panic(err)
		}
	}
	if err == nil {
		blkun := blk.SearchNode(un)
		if blkun != nil {
			blkun.Update(un)
		} else {
			blk.Insert(un)
		}
		return
	}
	cmd.Block.RWMutex = new(sync.RWMutex)
	cmd.Block.segment = seg
	cmd.Block.blkData = dataFactory.MakeBlockFactory()(cmd.Block)
	if observer != nil {
		observer.OnTimeStamp(prepareTS)
	}
	seg.AddEntryLocked(cmd.Block)
}

func (catalog *Catalog) OnReplayBlockBatch(ins, insTxn, del, delTxn *containers.Batch, dataFactory DataFactory) {
	for i := 0; i < ins.Length(); i++ {
		dbid := insTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := insTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		sid := insTxn.GetVectorByName(SnapshotAttr_SegID).Get(i).(uint64)
		appendable := ins.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Get(i).(bool)
		state := ES_NotAppendable
		if appendable {
			state = ES_Appendable
		}
		blkID := ins.GetVectorByName(pkgcatalog.BlockMeta_ID).Get(i).(uint64)
		metaLoc := string(ins.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte))
		deltaLoc := string(ins.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Get(i).([]byte))
		txnNode := txnbase.ReadTuple(insTxn, i)
		catalog.onReplayCreateBlock(dbid, tid, sid, blkID, state, metaLoc, deltaLoc, txnNode, dataFactory)
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		sid := delTxn.GetVectorByName(SnapshotAttr_SegID).Get(i).(uint64)
		blkID := del.GetVectorByName(AttrRowID).Get(i).(types.Rowid)
		un := txnbase.ReadTuple(delTxn, i)
		metaLoc := string(delTxn.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte))
		deltaLoc := string(delTxn.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Get(i).([]byte))
		catalog.onReplayDeleteBlock(dbid, tid, sid, rowIDToU64(blkID), metaLoc, deltaLoc, un)
	}
}
func (catalog *Catalog) onReplayCreateBlock(
	dbid, tid, segid, blkid uint64,
	state EntryState,
	metaloc, deltaloc string,
	txnNode *txnbase.TxnMVCCNode,
	dataFactory DataFactory) {
	catalog.OnReplayBlockID(blkid)
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	rel, err := db.GetTableEntryByID(tid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	seg, err := rel.GetSegmentByID(segid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	blk, _ := seg.GetBlockEntryByID(blkid)
	var un *MetadataMVCCNode
	if blk == nil {
		blk = NewReplayBlockEntry()
		blk.segment = seg
		blk.ID = blkid
		blk.state = state
		blk.blkData = dataFactory.MakeBlockFactory()(blk)
		seg.AddEntryLocked(blk)
		un = &MetadataMVCCNode{
			EntryMVCCNode: &EntryMVCCNode{
				CreatedAt: txnNode.End,
			},
			TxnMVCCNode: txnNode,
			MetaLoc:     metaloc,
			DeltaLoc:    deltaloc,
		}
	} else {
		prevUn := blk.MVCCChain.GetLatestNodeLocked().(*MetadataMVCCNode)
		un = &MetadataMVCCNode{
			EntryMVCCNode: &EntryMVCCNode{
				CreatedAt: prevUn.CreatedAt,
			},
			TxnMVCCNode: txnNode,
			MetaLoc:     metaloc,
			DeltaLoc:    deltaloc,
		}
		node := blk.MVCCChain.SearchNode(un).(*MetadataMVCCNode)
		if node != nil {
			if !node.CreatedAt.Equal(un.CreatedAt) {
				panic(moerr.NewInternalError("logic err expect %s, get %s", node.CreatedAt.ToString(), un.CreatedAt.ToString()))
			}
			if node.MetaLoc != un.MetaLoc {
				panic(moerr.NewInternalError("logic err expect %s, get %s", node.MetaLoc, un.MetaLoc))
			}
			if node.DeltaLoc != un.DeltaLoc {
				panic(moerr.NewInternalError("logic err expect %s, get %s", node.DeltaLoc, un.DeltaLoc))
			}
			return
		}
	}
	blk.Insert(un)
}
func (catalog *Catalog) onReplayDeleteBlock(dbid, tid, segid, blkid uint64, metaloc, deltaloc string, txnNode *txnbase.TxnMVCCNode) {
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	rel, err := db.GetTableEntryByID(tid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	seg, err := rel.GetSegmentByID(segid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	blk, err := seg.GetBlockEntryByID(blkid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	blkDeleteAt := blk.GetDeleteAt()
	if !blkDeleteAt.IsEmpty() {
		if !blkDeleteAt.Equal(txnNode.End) {
			panic(moerr.NewInternalError("logic err expect %s, get %s", txnNode.End.ToString(), blkDeleteAt.ToString()))
		}
		return
	}
	prevUn := blk.MVCCChain.GetLatestNodeLocked().(*MetadataMVCCNode)
	un := &MetadataMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: prevUn.CreatedAt,
			DeletedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
		MetaLoc:     metaloc,
		DeltaLoc:    deltaloc,
	}
	blk.Insert(un)
}
func (catalog *Catalog) ReplayTableRows() {
	rows := uint64(0)
	tableProcessor := new(LoopProcessor)
	tableProcessor.BlockFn = func(be *BlockEntry) error {
		if !be.IsActive() {
			return nil
		}
		rows += be.GetBlockData().GetRowsOnReplay()
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
func (catalog *Catalog) Close() error {
	// if catalog.store != nil {
	// 	catalog.store.Close()
	// }
	return nil
}

func (catalog *Catalog) CoarseDBCnt() int {
	catalog.RLock()
	defer catalog.RUnlock()
	return len(catalog.entries)
}

func (catalog *Catalog) CoarseTableCnt() int {
	return int(catalog.tableCnt.Load())
}

func (catalog *Catalog) CoarseColumnCnt() int {
	return int(catalog.columnCnt.Load())
}

func (catalog *Catalog) AddTableCnt(cnt int) {
	if catalog.tableCnt.Add(int32(cnt)) < 0 {
		panic("logic error")
	}
}

func (catalog *Catalog) AddColumnCnt(cnt int) {
	if catalog.columnCnt.Add(int32(cnt)) < 0 {
		panic("logic error")
	}
}

func (catalog *Catalog) GetItemNodeByIDLocked(id uint64) *common.GenericDLNode[*DBEntry] {
	return catalog.entries[id]
}

func (catalog *Catalog) GetScheduler() tasks.TaskScheduler { return catalog.scheduler }
func (catalog *Catalog) GetDatabaseByID(id uint64) (db *DBEntry, err error) {
	catalog.RLock()
	defer catalog.RUnlock()
	node := catalog.entries[id]
	if node == nil {
		err = moerr.GetOkExpectedEOB()
		return
	}
	db = node.GetPayload()
	return
}

func (catalog *Catalog) AddEntryLocked(database *DBEntry, txn txnif.TxnReader, skipDedup bool) error {
	nn := catalog.nameNodes[database.GetFullName()]
	if nn == nil {
		n := catalog.link.Insert(database)
		catalog.entries[database.GetID()] = n

		nn := newNodeList(catalog.GetItemNodeByIDLocked,
			dbVisibilityFn[*DBEntry],
			&catalog.nodesMu,
			database.name)
		catalog.nameNodes[database.GetFullName()] = nn

		nn.CreateNode(database.GetID())
	} else {
		node := nn.GetNode()
		if !skipDedup {
			record := node.GetPayload()
			err := record.PrepareAdd(txn)
			if err != nil {
				return err
			}
		}
		n := catalog.link.Insert(database)
		catalog.entries[database.GetID()] = n
		nn.CreateNode(database.GetID())
	}
	return nil
}

func (catalog *Catalog) MakeDBIt(reverse bool) *common.GenericSortedDListIt[*DBEntry] {
	catalog.RLock()
	defer catalog.RUnlock()
	return common.NewGenericSortedDListIt(catalog.RWMutex, catalog.link, reverse)
}

func (catalog *Catalog) SimplePPString(level common.PPLevel) string {
	return catalog.PPString(level, 0, "")
}

func (catalog *Catalog) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	cnt := 0
	it := catalog.MakeDBIt(true)
	for it.Valid() {
		cnt++
		entry := it.Get().GetPayload()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(entry.PPString(level, depth+1, ""))
		it.Next()
	}

	var w2 bytes.Buffer
	_, _ = w2.WriteString(fmt.Sprintf("CATALOG[CNT=%d]", cnt))
	_, _ = w2.WriteString(w.String())
	return w2.String()
}

func (catalog *Catalog) RemoveEntry(database *DBEntry) error {
	if database.IsSystemDB() {
		logutil.Warnf("system db cannot be removed")
		return moerr.NewTAEError("not permitted")
	}
	logutil.Info("[Catalog]", common.OperationField("remove"),
		common.OperandField(database.String()))
	catalog.Lock()
	defer catalog.Unlock()
	if n, ok := catalog.entries[database.GetID()]; !ok {
		return moerr.NewBadDB(database.GetName())
	} else {
		nn := catalog.nameNodes[database.GetFullName()]
		nn.DeleteNode(database.GetID())
		catalog.link.Delete(n)
		if nn.Length() == 0 {
			delete(catalog.nameNodes, database.GetFullName())
		}
		delete(catalog.entries, database.GetID())
	}
	return nil
}

func (catalog *Catalog) txnGetNodeByName(
	tenantID uint32,
	name string,
	ts types.TS) (*common.GenericDLNode[*DBEntry], error) {
	catalog.RLock()
	defer catalog.RUnlock()
	fullName := genDBFullName(tenantID, name)
	node := catalog.nameNodes[fullName]
	if node == nil {
		return nil, moerr.NewBadDB(name)
	}
	return node.TxnGetNodeLocked(ts)
}

func (catalog *Catalog) GetDBEntryByName(
	tenantID uint32,
	name string,
	ts types.TS) (db *DBEntry, err error) {
	n, err := catalog.txnGetNodeByName(tenantID, name, ts)
	if err != nil {
		return
	}
	db = n.GetPayload()
	return
}

func (catalog *Catalog) TxnGetDBEntryByName(name string, txn txnif.AsyncTxn) (*DBEntry, error) {
	n, err := catalog.txnGetNodeByName(txn.GetTenantID(), name, txn.GetStartTS())
	if err != nil {
		return nil, err
	}
	return n.GetPayload(), nil
}

func (catalog *Catalog) TxnGetDBEntryByID(id uint64, txn txnif.AsyncTxn) (*DBEntry, error) {
	dbEntry, err := catalog.GetDatabaseByID(id)
	if err != nil {
		return nil, err
	}
	visiable, dropped := dbEntry.GetVisibility(txn.GetStartTS())
	if !visiable || dropped {
		return nil, moerr.GetOkExpectedEOB()
	}
	return dbEntry, nil
}

func (catalog *Catalog) DropDBEntry(
	name string,
	txn txnif.AsyncTxn) (newEntry bool, deleted *DBEntry, err error) {
	if name == pkgcatalog.MO_CATALOG {
		err = moerr.NewTAEError("not permitted")
		return
	}
	dn, err := catalog.txnGetNodeByName(txn.GetTenantID(), name, txn.GetStartTS())
	if err != nil {
		return
	}
	entry := dn.GetPayload()
	entry.Lock()
	defer entry.Unlock()
	if newEntry, err = entry.DropEntryLocked(txn); err == nil {
		deleted = entry
	}
	return
}

func (catalog *Catalog) DropDBEntryByID(id uint64, txn txnif.AsyncTxn) (newEntry bool, deleted *DBEntry, err error) {
	if id == pkgcatalog.MO_CATALOG_ID {
		err = moerr.NewTAEError("not permitted")
		return
	}
	entry, err := catalog.GetDatabaseByID(id)
	if err != nil {
		return
	}
	entry.Lock()
	defer entry.Unlock()
	if newEntry, err = entry.DropEntryLocked(txn); err == nil {
		deleted = entry
	}
	return
}

func (catalog *Catalog) CreateDBEntry(name, createSql string, txn txnif.AsyncTxn) (*DBEntry, error) {
	var err error
	catalog.Lock()
	defer catalog.Unlock()
	entry := NewDBEntry(catalog, name, createSql, txn)
	err = catalog.AddEntryLocked(entry, txn, false)

	return entry, err
}

func (catalog *Catalog) CreateDBEntryWithID(name, createSql string, id uint64, txn txnif.AsyncTxn) (*DBEntry, error) {
	var err error
	catalog.Lock()
	defer catalog.Unlock()
	if _, exist := catalog.entries[id]; exist {
		return nil, moerr.NewDuplicate()
	}
	entry := NewDBEntryWithID(catalog, name, createSql, id, txn)
	err = catalog.AddEntryLocked(entry, txn, false)

	return entry, err
}

func (catalog *Catalog) CreateDBEntryByTS(name string, ts types.TS) (*DBEntry, error) {
	entry := NewDBEntryByTS(catalog, name, ts)
	err := catalog.AddEntryLocked(entry, nil, false)
	return entry, err
}

func (catalog *Catalog) RecurLoop(processor Processor) (err error) {
	dbIt := catalog.MakeDBIt(true)
	for dbIt.Valid() {
		dbEntry := dbIt.Get().GetPayload()
		if err = processor.OnDatabase(dbEntry); err != nil {
			// XXX: Performance problem.   Error should not be used
			// to handle normal return code.
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				err = nil
				dbIt.Next()
				continue
			}
			break
		}
		if err = dbEntry.RecurLoop(processor); err != nil {
			return
		}
		dbIt.Next()
	}
	if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
		err = nil
	}
	return err
}
