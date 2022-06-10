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
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

// +--------+---------+----------+----------+------------+
// |   ID   |  Name   | CreateAt | DeleteAt | CommitInfo |
// +--------+---------+----------+----------+------------+
// |(uint64)|(varchar)| (uint64) | (uint64) |  (varchar) |
// +--------+---------+----------+----------+------------+

type DataFactory interface {
	MakeTableFactory() TableDataFactory
	MakeSegmentFactory() SegmentDataFactory
	MakeBlockFactory(segFile file.Segment) BlockDataFactory
}

type Catalog struct {
	*IDAlloctor
	*sync.RWMutex
	store store.Store

	scheduler   tasks.TaskScheduler
	ckpmu       sync.RWMutex
	checkpoints []*Checkpoint

	entries   map[uint64]*common.DLNode
	nameNodes map[string]*nodeList
	link      *common.Link

	nodesMu sync.RWMutex

	tableCnt  int32
	columnCnt int32
}

func MockCatalog(dir, name string, cfg *store.StoreCfg, scheduler tasks.TaskScheduler) *Catalog {
	var driver store.Store
	var err error
	driver, err = store.NewBaseStore(dir, name, cfg)
	if err != nil {
		panic(err)
	}
	catalog := &Catalog{
		RWMutex:     new(sync.RWMutex),
		IDAlloctor:  NewIDAllocator(),
		store:       driver,
		entries:     make(map[uint64]*common.DLNode),
		nameNodes:   make(map[string]*nodeList),
		link:        new(common.Link),
		checkpoints: make([]*Checkpoint, 0),
		scheduler:   scheduler,
	}
	catalog.InitSystemDB()
	return catalog
}

func OpenCatalog(dir, name string, cfg *store.StoreCfg, scheduler tasks.TaskScheduler, dataFactory DataFactory) (*Catalog, error) {
	driver, err := store.NewBaseStore(dir, name, cfg)
	if err != nil {
		panic(err)
	}
	catalog := &Catalog{
		RWMutex:     new(sync.RWMutex),
		IDAlloctor:  NewIDAllocator(),
		store:       driver,
		entries:     make(map[uint64]*common.DLNode),
		nameNodes:   make(map[string]*nodeList),
		link:        new(common.Link),
		checkpoints: make([]*Checkpoint, 0),
		scheduler:   scheduler,
	}
	catalog.InitSystemDB()
	replayer := NewReplayer(dataFactory, catalog)
	err = catalog.store.Replay(replayer.ReplayerHandle)
	return catalog, err
}

func (catalog *Catalog) InitSystemDB() {
	sysDB := NewSystemDBEntry(catalog)
	dbTables := NewSystemTableEntry(sysDB, SystemTable_DB_ID, SystemDBSchema)
	tableTables := NewSystemTableEntry(sysDB, SystemTable_Table_ID, SystemTableSchema)
	columnTables := NewSystemTableEntry(sysDB, SystemTable_Columns_ID, SystemColumnSchema)
	err := sysDB.AddEntryLocked(dbTables)
	if err != nil {
		panic(err)
	}
	if err = sysDB.AddEntryLocked(tableTables); err != nil {
		panic(err)
	}
	if err = sysDB.AddEntryLocked(columnTables); err != nil {
		panic(err)
	}
	if err = catalog.AddEntryLocked(sysDB); err != nil {
		panic(err)
	}
}

func (catalog *Catalog) GetStore() store.Store { return catalog.store }

func (catalog *Catalog) ReplayCmd(txncmd txnif.TxnCmd, datafactory DataFactory, idxCtx *wal.Index, observer wal.ReplayObserver, cache *bytes.Buffer) {
	switch txncmd.GetType() {
	case txnbase.CmdComposed:
		cmds := txncmd.(*txnbase.ComposedCmd)
		idxCtx.Size = cmds.CmdSize
		for i, cmds := range cmds.Cmds {
			idx := idxCtx.Clone()
			idx.CSN = uint32(i)
			catalog.ReplayCmd(cmds, datafactory, idx, observer, cache)
		}
	case CmdLogBlock:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayBlock(cmd, datafactory)
	case CmdLogSegment:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplaySegment(cmd, datafactory, cache)
	case CmdLogTable:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayTable(cmd, datafactory)
	case CmdLogDatabase:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayDatabase(cmd)
	case CmdCreateDatabase:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayCreateDatabase(cmd, idxCtx, observer)
	case CmdCreateTable:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayCreateTable(cmd, datafactory, idxCtx, observer)
	case CmdCreateSegment:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayCreateSegment(cmd, datafactory, idxCtx, observer, cache)
	case CmdCreateBlock:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayCreateBlock(cmd, datafactory, idxCtx, observer)
	case CmdDropTable:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayDropTable(cmd, idxCtx, observer)
	case CmdDropDatabase:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayDropDatabase(cmd, idxCtx, observer)
	case CmdDropSegment:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayDropSegment(cmd, idxCtx, observer)
	case CmdDropBlock:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayDropBlock(cmd, idxCtx, observer)
	default:
		panic("unsupport")
	}
}

func (catalog *Catalog) onReplayCreateDatabase(cmd *EntryCommand, idx *wal.Index, observer wal.ReplayObserver) {
	if cmd.entry.CreateAt <= catalog.GetCheckpointed().MaxTS {
		if observer != nil {
			observer.OnStaleIndex(idx)
		}
		return
	}
	var err error
	db, err := catalog.GetDatabaseByID(cmd.entry.ID)
	if err == nil {
		db.LogIndex = cmd.entry.LogIndex
		return
	}
	err = catalog.AddEntryLocked(cmd.DB)
	cmd.DB.catalog = catalog
	cmd.DB.LogIndex = idx
	catalog.OnReplayDBID(cmd.DB.ID)
	if observer != nil {
		observer.OnTimeStamp(cmd.entry.CreateAt)
	}
	if err != nil && err != ErrDuplicate {
		panic(err)
	}
}

func (catalog *Catalog) onReplayDropDatabase(cmd *EntryCommand, idx *wal.Index, observer wal.ReplayObserver) {
	if cmd.entry.DeleteAt <= catalog.GetCheckpointed().MaxTS {
		if observer != nil {
			observer.OnStaleIndex(idx)
		}
		return
	}
	var err error
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		panic(err)
	}
	err = db.ApplyDeleteCmd(cmd.entry.DeleteAt, idx)
	if observer != nil {
		observer.OnTimeStamp(cmd.entry.DeleteAt)
	}
	if err != nil {
		panic(err)
	}
}

func (catalog *Catalog) onReplayDatabase(cmd *EntryCommand) {
	var err error
	cmd.DB.catalog = catalog
	if cmd.DB.CurrOp == OpCreate {
		catalog.OnReplayDBID(cmd.DB.ID)
		err = catalog.AddEntryLocked(cmd.DB)
		if err != nil {
			panic(err)
		}
	} else {
		var db *DBEntry
		db, err = catalog.GetDatabaseByID(cmd.DB.ID)
		if err == nil {
			cmd.DB.entries = db.entries
			cmd.DB.link = db.link
			cmd.DB.nameNodes = db.nameNodes
			if err = catalog.RemoveEntry(db); err != nil {
				panic(err)
			}
		}
		err = catalog.AddEntryLocked(cmd.DB)
		if err != nil {
			panic(err)
		}
	}
}

func (catalog *Catalog) onReplayCreateTable(cmd *EntryCommand, datafactory DataFactory, idx *wal.Index, observer wal.ReplayObserver) {
	if cmd.entry.CreateAt <= catalog.GetCheckpointed().MaxTS {
		if observer != nil {
			observer.OnStaleIndex(idx)
		}
		return
	}
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(cmd.Table.ID)
	if err == nil {
		tbl.LogIndex = cmd.Table.LogIndex
		return
	}
	cmd.Table.db = db
	cmd.Table.tableData = datafactory.MakeTableFactory()(cmd.Table)
	catalog.OnReplayTableID(cmd.Table.ID)
	cmd.Table.LogIndex = idx
	err = db.AddEntryLocked(cmd.Table)
	if observer != nil {
		observer.OnTimeStamp(cmd.entry.CreateAt)
	}
	if err != nil && err != ErrDuplicate {
		panic(err)
	}
}

func (catalog *Catalog) onReplayDropTable(cmd *EntryCommand, idx *wal.Index, observer wal.ReplayObserver) {
	if cmd.entry.DeleteAt <= catalog.GetCheckpointed().MaxTS {
		if observer != nil {
			observer.OnStaleIndex(idx)
		}
		return
	}
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		panic(err)
	}
	err = tbl.ApplyDeleteCmd(cmd.entry.DeleteAt, idx)
	if observer != nil {
		observer.OnTimeStamp(cmd.entry.DeleteAt)
	}
	if err != nil {
		panic(err)
	}
}

func (catalog *Catalog) onReplayTable(cmd *EntryCommand, datafactory DataFactory) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		panic(err)
	}
	cmd.Table.db = db
	if cmd.Table.CurrOp == OpCreate {
		catalog.OnReplayTableID(cmd.Table.ID)
		cmd.Table.tableData = datafactory.MakeTableFactory()(cmd.Table)
		err = db.AddEntryLocked(cmd.Table)
	} else {
		rel, _ := db.GetTableEntryByID(cmd.Table.ID)
		if rel != nil {
			cmd.Table.entries = rel.entries
			cmd.Table.link = rel.link
			if err = db.RemoveEntry(rel); err != nil {
				panic(err)
			}
		}
		err = db.AddEntryLocked(cmd.Table)
	}
	if err != nil {
		panic(err)
	}
}

func (catalog *Catalog) onReplayCreateSegment(cmd *EntryCommand, datafactory DataFactory, idx *wal.Index, observer wal.ReplayObserver, cache *bytes.Buffer) {
	if cmd.entry.CreateAt <= catalog.GetCheckpointed().MaxTS {
		if observer != nil {
			observer.OnStaleIndex(idx)
		}
		return
	}
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		panic(err)
	}
	seg, err := tbl.GetSegmentByID(cmd.Segment.ID)
	if err == nil {
		seg.LogIndex = cmd.entry.LogIndex
		return
	}
	cmd.Segment.table = tbl
	cmd.Segment.RWMutex = new(sync.RWMutex)
	cmd.Segment.CurrOp = OpCreate
	cmd.Segment.link = new(common.Link)
	cmd.Segment.entries = make(map[uint64]*common.DLNode)
	cmd.Segment.segData = datafactory.MakeSegmentFactory()(cmd.Segment)
	cmd.Segment.ReplayFile(cache)
	catalog.OnReplaySegmentID(cmd.Segment.ID)
	tbl.AddEntryLocked(cmd.Segment)
	cmd.Segment.LogIndex = idx
	if observer != nil {
		observer.OnTimeStamp(cmd.Segment.CreateAt)
	}
}

func (catalog *Catalog) onReplayDropSegment(cmd *EntryCommand, idx *wal.Index, observer wal.ReplayObserver) {
	if cmd.entry.DeleteAt <= catalog.GetCheckpointed().MaxTS {
		if observer != nil {
			observer.OnStaleIndex(idx)
		}
		return
	}
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		panic(err)
	}
	seg, err := tbl.GetSegmentByID(cmd.entry.ID)
	if err != nil {
		panic(err)
	}
	err = seg.ApplyDeleteCmd(cmd.entry.DeleteAt, idx)
	if observer != nil {
		observer.OnTimeStamp(cmd.entry.DeleteAt)
	}
	if err != nil {
		panic(err)
	}
}

func (catalog *Catalog) onReplaySegment(cmd *EntryCommand, datafactory DataFactory, cache *bytes.Buffer) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		panic(err)
	}
	rel, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		panic(err)
	}
	cmd.Segment.table = rel
	if cmd.Segment.CurrOp == OpCreate {
		cmd.Segment.segData = datafactory.MakeSegmentFactory()(cmd.Segment)
		catalog.OnReplaySegmentID(cmd.Segment.ID)
		rel.AddEntryLocked(cmd.Segment)
		cmd.Segment.ReplayFile(cache)
	} else {
		seg, _ := rel.GetSegmentByID(cmd.Segment.ID)
		if seg != nil {
			cmd.Segment.entries = seg.entries
			if err = rel.deleteEntryLocked(seg); err != nil {
				panic(err)
			}
		}
		rel.AddEntryLocked(cmd.Segment)
	}
}

func (catalog *Catalog) onReplayCreateBlock(cmd *EntryCommand, datafactory DataFactory, idx *wal.Index, observer wal.ReplayObserver) {
	if cmd.entry.CreateAt <= catalog.GetCheckpointed().MaxTS {
		if observer != nil {
			observer.OnStaleIndex(idx)
		}
		return
	}
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
	if err == nil {
		blk.LogIndex = cmd.entry.LogIndex
		return
	}
	cmd.Block.RWMutex = new(sync.RWMutex)
	cmd.Block.CurrOp = OpCreate
	cmd.Block.segment = seg
	cmd.Block.blkData = datafactory.MakeBlockFactory(seg.segData.GetSegmentFile())(cmd.Block)
	ts := cmd.Block.blkData.GetMaxCheckpointTS()
	if observer != nil {
		observer.OnTimeStamp(ts)
	}
	// cmd.Block.blkData.ReplayData()
	catalog.OnReplayBlockID(cmd.Block.ID)
	cmd.Block.LogIndex = idx
	seg.AddEntryLocked(cmd.Block)
	if observer != nil {
		observer.OnTimeStamp(cmd.Block.CreateAt)
	}
}

func (catalog *Catalog) onReplayDropBlock(cmd *EntryCommand, idx *wal.Index, observer wal.ReplayObserver) {
	if cmd.entry.DeleteAt <= catalog.GetCheckpointed().MaxTS {
		if observer != nil {
			observer.OnStaleIndex(idx)
		}
		return
	}
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
	blk, err := seg.GetBlockEntryByID(cmd.entry.ID)
	if err != nil {
		panic(err)
	}
	err = blk.ApplyDeleteCmd(cmd.entry.DeleteAt, idx)
	if observer != nil {
		observer.OnTimeStamp(cmd.entry.DeleteAt)
	}
	if err != nil {
		panic(err)
	}
}

func (catalog *Catalog) onReplayBlock(cmd *EntryCommand, datafactory DataFactory) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		panic(err)
	}
	rel, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		panic(err)
	}
	seg, err := rel.GetSegmentByID(cmd.SegmentID)
	if err != nil {
		panic(err)
	}
	cmd.Block.segment = seg
	if cmd.Block.CurrOp == OpCreate {
		cmd.Block.blkData = datafactory.MakeBlockFactory(seg.segData.GetSegmentFile())(cmd.Block)
		// cmd.Block.blkData.ReplayData()
		catalog.OnReplayBlockID(cmd.Block.ID)
		seg.AddEntryLocked(cmd.Block)
	} else {
		blk, _ := seg.GetBlockEntryByID(cmd.Block.ID)
		if blk != nil {
			if err = seg.deleteEntryLocked(blk); err != nil {
				panic(err)
			}
		}
		seg.AddEntryLocked(cmd.Block)
	}
}
func (catalog *Catalog) ReplayTableRows() {
	rows := uint64(0)
	tableProcessor := new(LoopProcessor)
	tableProcessor.BlockFn = func(be *BlockEntry) error {
		if be.IsDroppedCommitted() {
			return nil
		}
		rows += be.GetBlockData().GetRowsOnReplay()
		return nil
	}
	processor := new(LoopProcessor)
	processor.TableFn = func(tbl *TableEntry) error {
		if tbl.db.name == "mo_catalog" {
			return nil
		}
		rows = 0
		err := tbl.RecurLoop(tableProcessor)
		if err != nil {
			panic(err)
		}
		tbl.rows = rows
		return nil
	}
	err := catalog.RecurLoop(processor)
	if err != nil {
		panic(err)
	}
}
func (catalog *Catalog) Close() error {
	if catalog.store != nil {
		catalog.store.Close()
	}
	return nil
}

func (catalog *Catalog) CoarseDBCnt() int {
	catalog.RLock()
	defer catalog.RUnlock()
	return len(catalog.entries)
}

func (catalog *Catalog) CoarseTableCnt() int {
	return int(atomic.LoadInt32(&catalog.tableCnt))
}

func (catalog *Catalog) CoarseColumnCnt() int {
	return int(atomic.LoadInt32(&catalog.columnCnt))
}

func (catalog *Catalog) AddTableCnt(cnt int) {
	n := atomic.AddInt32(&catalog.tableCnt, int32(cnt))
	if n < 0 {
		panic("logic error")
	}
}

func (catalog *Catalog) AddColumnCnt(cnt int) {
	n := atomic.AddInt32(&catalog.columnCnt, int32(cnt))
	if n < 0 {
		panic("logic error")
	}
}

func (catalog *Catalog) GetScheduler() tasks.TaskScheduler { return catalog.scheduler }
func (catalog *Catalog) GetDatabaseByID(id uint64) (db *DBEntry, err error) {
	catalog.RLock()
	defer catalog.RUnlock()
	node := catalog.entries[id]
	if node == nil {
		err = ErrNotFound
		return
	}
	db = node.GetPayload().(*DBEntry)
	return
}

func (catalog *Catalog) AddEntryLocked(database *DBEntry) error {
	nn := catalog.nameNodes[database.name]
	if nn == nil {
		n := catalog.link.Insert(database)
		catalog.entries[database.GetID()] = n

		nn := newNodeList(catalog, &catalog.nodesMu, database.name)
		catalog.nameNodes[database.name] = nn

		nn.CreateNode(database.GetID())
	} else {
		node := nn.GetDBNode()
		record := node.GetPayload().(*DBEntry)
		record.RLock()
		err := record.PrepareWrite(database.GetTxn(), record.RWMutex)
		if err != nil {
			record.RUnlock()
			return err
		}
		if record.HasActiveTxn() {
			if !record.IsDroppedUncommitted() {
				record.RUnlock()
				return ErrDuplicate
			}
		} else if !record.HasDropped() {
			record.RUnlock()
			return ErrDuplicate
		}

		record.RUnlock()
		n := catalog.link.Insert(database)
		catalog.entries[database.GetID()] = n
		nn.CreateNode(database.GetID())
	}
	return nil
}

func (catalog *Catalog) MakeDBIt(reverse bool) *common.LinkIt {
	catalog.RLock()
	defer catalog.RUnlock()
	return common.NewLinkIt(catalog.RWMutex, catalog.link, reverse)
}

func (catalog *Catalog) SimplePPString(level common.PPLevel) string {
	return catalog.PPString(level, 0, "")
}

func (catalog *Catalog) PPString(level common.PPLevel, depth int, prefix string) string {
	cnt := 0
	var body string
	it := catalog.MakeDBIt(true)
	for it.Valid() {
		cnt++
		table := it.Get().GetPayload().(*DBEntry)
		if len(body) == 0 {
			body = table.PPString(level, depth+1, "")
		} else {
			body = fmt.Sprintf("%s\n%s", body, table.PPString(level, depth+1, ""))
		}
		it.Next()
	}

	var ckp *Checkpoint
	catalog.ckpmu.RLock()
	if len(catalog.checkpoints) > 0 {
		ckp = catalog.checkpoints[len(catalog.checkpoints)-1]
	}
	catalog.ckpmu.RUnlock()

	head := fmt.Sprintf("CATALOG[CNT=%d][%s]", cnt, ckp.String())

	if len(body) == 0 {
		return head
	}
	return fmt.Sprintf("%s\n%s", head, body)
}

func (catalog *Catalog) RemoveEntry(database *DBEntry) error {
	if database.IsSystemDB() {
		logutil.Warnf("system db cannot be removed")
		return ErrNotPermitted
	}
	catalog.Lock()
	defer catalog.Unlock()
	if n, ok := catalog.entries[database.GetID()]; !ok {
		return ErrNotFound
	} else {
		nn := catalog.nameNodes[database.name]
		nn.DeleteNode(database.GetID())
		catalog.link.Delete(n)
		if nn.Length() == 0 {
			delete(catalog.nameNodes, database.name)
		}
		delete(catalog.entries, database.GetID())
	}
	return nil
}

func (catalog *Catalog) txnGetNodeByNameLocked(name string, txnCtx txnif.AsyncTxn) (*common.DLNode, error) {
	node := catalog.nameNodes[name]
	if node == nil {
		return nil, ErrNotFound
	}
	return node.TxnGetDBNodeLocked(txnCtx)
}

func (catalog *Catalog) GetDBEntry(name string, txnCtx txnif.AsyncTxn) (*DBEntry, error) {
	catalog.RLock()
	n, err := catalog.txnGetNodeByNameLocked(name, txnCtx)
	catalog.RUnlock()
	if err != nil {
		return nil, err
	}
	return n.GetPayload().(*DBEntry), nil
}

func (catalog *Catalog) DropDBEntry(name string, txnCtx txnif.AsyncTxn) (deleted *DBEntry, err error) {
	if name == SystemDBName {
		err = ErrNotPermitted
		return
	}
	catalog.Lock()
	defer catalog.Unlock()
	dn, err := catalog.txnGetNodeByNameLocked(name, txnCtx)
	if err != nil {
		return
	}
	entry := dn.GetPayload().(*DBEntry)
	entry.Lock()
	defer entry.Unlock()
	err = entry.DropEntryLocked(txnCtx)
	if err == nil {
		deleted = entry
	}
	return
}

func (catalog *Catalog) CreateDBEntry(name string, txnCtx txnif.AsyncTxn) (*DBEntry, error) {
	var err error
	catalog.Lock()
	entry := NewDBEntry(catalog, name, txnCtx)
	err = catalog.AddEntryLocked(entry)
	catalog.Unlock()

	return entry, err
}

func (catalog *Catalog) RecurLoop(processor Processor) (err error) {
	dbIt := catalog.MakeDBIt(true)
	for dbIt.Valid() {
		dbEntry := dbIt.Get().GetPayload().(*DBEntry)
		if err = processor.OnDatabase(dbEntry); err != nil {
			if err == ErrStopCurrRecur {
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
	if err == ErrStopCurrRecur {
		err = nil
	}
	return err
}

func (catalog *Catalog) PrepareCheckpoint(startTs, endTs uint64) *CheckpointEntry {
	ckpEntry := NewCheckpointEntry(startTs, endTs)
	processor := new(LoopProcessor)
	processor.BlockFn = func(block *BlockEntry) (err error) {
		entry := block.BaseEntry
		CheckpointOp(ckpEntry, entry, block, startTs, endTs)
		return
	}
	processor.SegmentFn = func(segment *SegmentEntry) (err error) {
		entry := segment.BaseEntry
		CheckpointOp(ckpEntry, entry, segment, startTs, endTs)
		return
	}
	processor.TableFn = func(table *TableEntry) (err error) {
		if table.IsVirtual() {
			err = ErrStopCurrRecur
			return
		}
		entry := table.BaseEntry
		CheckpointOp(ckpEntry, entry, table, startTs, endTs)
		return
	}
	processor.DatabaseFn = func(database *DBEntry) (err error) {
		if database.IsSystemDB() {
			// No need to checkpoint system db entry
			return
		}
		entry := database.BaseEntry
		CheckpointOp(ckpEntry, entry, database, startTs, endTs)
		return
	}
	if err := catalog.RecurLoop(processor); err != nil {
		panic(err)
	}
	return ckpEntry
}

func (catalog *Catalog) GetCheckpointed() *Checkpoint {
	catalog.ckpmu.RLock()
	defer catalog.ckpmu.RUnlock()
	if len(catalog.checkpoints) == 0 {
		return EmptyCheckpoint
	}
	return catalog.checkpoints[len(catalog.checkpoints)-1]
}

func (catalog *Catalog) CheckpointClosure(maxTs uint64) tasks.FuncT {
	return func() error {
		return catalog.Checkpoint(maxTs)
	}
}

func (catalog *Catalog) Checkpoint(maxTs uint64) (err error) {
	var minTs uint64
	catalog.ckpmu.RLock()
	if len(catalog.checkpoints) != 0 {
		lastMax := catalog.checkpoints[len(catalog.checkpoints)-1].MaxTS
		if maxTs < lastMax {
			err = ErrCheckpoint
		}
		if maxTs == lastMax {
			catalog.ckpmu.RUnlock()
			return
		}
		minTs = lastMax + 1
	}
	catalog.ckpmu.RUnlock()
	now := time.Now()
	entry := catalog.PrepareCheckpoint(minTs, maxTs)
	logutil.Infof("PrepareCheckpoint: %s", time.Since(now))
	if len(entry.LogIndexes) == 0 {
		return
	}
	now = time.Now()
	logEntry, err := entry.MakeLogEntry()
	if err != nil {
		return
	}
	logutil.Infof("MakeLogEntry: %s", time.Since(now))
	now = time.Now()
	defer logEntry.Free()
	checkpoint := new(Checkpoint)
	checkpoint.MaxTS = maxTs
	checkpoint.LSN = entry.MaxIndex.LSN
	checkpoint.CommitId, err = catalog.store.AppendEntry(0, logEntry)
	if err != nil {
		panic(err)
	}
	if err = logEntry.WaitDone(); err != nil {
		panic(err)
	}
	logutil.Infof("SaveCheckpointed: %s", time.Since(now))
	// for _, index := range entry.LogIndexes {
	// 	logutil.Infof("Ckp0Index %s", index.String())
	// }
	now = time.Now()
	if err = catalog.scheduler.Checkpoint(entry.LogIndexes); err != nil {
		logutil.Warnf("Schedule checkpoint log indexes: %v", err)
		return
	}
	logutil.Infof("CheckpointWal: %s", time.Since(now))
	catalog.ckpmu.Lock()
	catalog.checkpoints = append(catalog.checkpoints, checkpoint)
	catalog.ckpmu.Unlock()
	logutil.Infof("Max LogIndex: %s", entry.MaxIndex.String())
	return
}
