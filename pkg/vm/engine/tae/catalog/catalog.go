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
	"fmt"
	"sync"
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

func OpenCatalog(dir, name string, cfg *store.StoreCfg, scheduler tasks.TaskScheduler, dataFactory DataFactory) (uint64, *Catalog, error) {
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
	return replayer.ts, catalog, err
}

func (catalog *Catalog) InitSystemDB() {
	sysDB := NewSystemDBEntry(catalog)
	dbTables := NewSystemTableEntry(sysDB, SystemTable_DB_ID, SystemDBSchema)
	tableTables := NewSystemTableEntry(sysDB, SystemTable_Table_ID, SystemTableSchema)
	columnTables := NewSystemTableEntry(sysDB, SystemTable_Columns_ID, SystemColumnSchema)
	err := sysDB.addEntryLocked(dbTables)
	if err != nil {
		panic(err)
	}
	if err = sysDB.addEntryLocked(tableTables); err != nil {
		panic(err)
	}
	if err = sysDB.addEntryLocked(columnTables); err != nil {
		panic(err)
	}
	if err = catalog.addEntryLocked(sysDB); err != nil {
		panic(err)
	}
}

func (catalog *Catalog) GetStore() store.Store { return catalog.store }

func (catalog *Catalog) ReplayCmd(txncmd txnif.TxnCmd, datafactory DataFactory, idx *wal.Index) (ts uint64, err error) {
	switch txncmd.GetType() {
	case txnbase.CmdComposed:
		cmds := txncmd.(*txnbase.ComposedCmd)
		size := uint32(len(cmds.Cmds))
		for i, cmds := range cmds.Cmds {
			idx2 := &wal.Index{LSN: idx.LSN, Size: size, CSN: uint32(i)}
			if ts, err = catalog.ReplayCmd(cmds, datafactory, idx2); err != nil {
				break
			}
		}
	case CmdLogBlock:
		cmd := txncmd.(*EntryCommand)
		ts, err = catalog.onReplayBlock(cmd, datafactory)
	case CmdLogSegment:
		cmd := txncmd.(*EntryCommand)
		ts, err = catalog.onReplaySegment(cmd, datafactory)
	case CmdLogTable:
		cmd := txncmd.(*EntryCommand)
		ts, err = catalog.onReplayTable(cmd, datafactory)
	case CmdLogDatabase:
		cmd := txncmd.(*EntryCommand)
		ts, err = catalog.onReplayDatabase(cmd)
	case CmdCreateDatabase:
		cmd := txncmd.(*EntryCommand)
		ts, err = catalog.onReplayCreateDatabase(cmd, idx)
	case CmdCreateTable:
		cmd := txncmd.(*EntryCommand)
		ts, err = catalog.onReplayCreateTable(cmd, datafactory, idx)
	case CmdCreateSegment:
		cmd := txncmd.(*EntryCommand)
		ts, err = catalog.onReplayCreateSegment(cmd, datafactory, idx)
	case CmdCreateBlock:
		cmd := txncmd.(*EntryCommand)
		ts, err = catalog.onReplayCreateBlock(cmd, datafactory, idx)
	case CmdDropTable:
		cmd := txncmd.(*EntryCommand)
		ts, err = catalog.onReplayDropTable(cmd, idx)
	case CmdDropDatabase:
		cmd := txncmd.(*EntryCommand)
		ts, err = catalog.onReplayDropDatabase(cmd, idx)
	case CmdDropSegment:
		cmd := txncmd.(*EntryCommand)
		ts, err = catalog.onReplayDropSegment(cmd, idx)
	case CmdDropBlock:
		cmd := txncmd.(*EntryCommand)
		ts, err = catalog.onReplayDropBlock(cmd, idx)
	default:
		panic("unsupport")
	}
	return
}

func (catalog *Catalog) onReplayCreateDatabase(cmd *EntryCommand, idx *wal.Index) (ts uint64, err error) {
	entry := NewDBEntry(catalog, cmd.DB.name, nil)
	entry.CreateAt = cmd.entry.CreateAt
	err = catalog.addEntryLocked(entry)
	entry.LogIndex = idx
	catalog.OnReplayDBID(entry.ID)
	ts = cmd.entry.CreateAt
	return
}

func (catalog *Catalog) onReplayDropDatabase(cmd *EntryCommand, idx *wal.Index) (ts uint64, err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return 0, err
	}
	err = db.OnReplayDrop(cmd.entry.DeleteAt)
	db.LogIndex = idx
	ts = cmd.entry.DeleteAt
	return
}

func (catalog *Catalog) onReplayDatabase(cmd *EntryCommand) (ts uint64, err error) {
	cmd.DB.catalog = catalog
	if cmd.DB.CurrOp == OpCreate {
		catalog.OnReplayDBID(cmd.DB.ID)
		return cmd.DB.CreateAt, catalog.addEntryLocked(cmd.DB)
	} else {
		db, err := catalog.GetDatabaseByID(cmd.DB.ID)
		if err == nil {
			if err = catalog.RemoveEntry(db); err != nil {
				return 0, err
			}
		}
		err = catalog.addEntryLocked(cmd.DB)
		return cmd.DB.DeleteAt, err
	}
}

func (catalog *Catalog) onReplayCreateTable(cmd *EntryCommand, datafactory DataFactory, idx *wal.Index) (ts uint64, err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return 0, err
	}
	cmd.Table.db = db
	cmd.Table.tableData = datafactory.MakeTableFactory()(cmd.Table)
	catalog.OnReplayTableID(cmd.Table.ID)
	cmd.Table.LogIndex = idx
	return cmd.Table.CreateAt, db.addEntryLocked(cmd.Table)
}

func (catalog *Catalog) onReplayDropTable(cmd *EntryCommand, idx *wal.Index) (ts uint64, err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return 0, err
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		return 0, err
	}
	err = tbl.OnReplayDrop(cmd.entry.DeleteAt)
	tbl.LogIndex = idx
	ts = cmd.entry.DeleteAt
	return
}

func (catalog *Catalog) onReplayTable(cmd *EntryCommand, datafactory DataFactory) (ts uint64, err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return
	}
	cmd.Table.db = db
	if cmd.Table.CurrOp == OpCreate {
		catalog.OnReplayTableID(cmd.Table.ID)
		cmd.Table.tableData = datafactory.MakeTableFactory()(cmd.Table)
		return cmd.Table.CreateAt, db.addEntryLocked(cmd.Table)
	} else {
		rel, _ := db.GetTableEntryByID(cmd.Table.ID)
		if rel != nil {
			if err = db.RemoveEntry(rel); err != nil {
				return
			}
		}
		return cmd.Table.DeleteAt, db.addEntryLocked(cmd.Table)
	}
}

func (catalog *Catalog) onReplayCreateSegment(cmd *EntryCommand, datafactory DataFactory, idx *wal.Index) (ts uint64, err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return 0, err
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		return 0, err
	}
	cmd.Segment.table = tbl
	cmd.Segment.RWMutex = new(sync.RWMutex)
	cmd.Segment.CurrOp = OpCreate
	cmd.Segment.link = new(common.Link)
	cmd.Segment.entries = make(map[uint64]*common.DLNode)
	cmd.Segment.segData = datafactory.MakeSegmentFactory()(cmd.Segment)
	catalog.OnReplaySegmentID(cmd.Segment.ID)
	tbl.addEntryLocked(cmd.Segment)
	cmd.Segment.LogIndex = idx
	ts = cmd.Segment.CreateAt
	return
}

func (catalog *Catalog) onReplayDropSegment(cmd *EntryCommand, idx *wal.Index) (ts uint64, err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return 0, err
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		return 0, err
	}
	seg, err := tbl.GetSegmentByID(cmd.entry.ID)
	if err != nil {
		return 0, err
	}
	err = seg.OnReplayDrop(cmd.entry.DeleteAt)
	seg.LogIndex = idx
	ts = cmd.entry.DeleteAt
	return
}

func (catalog *Catalog) onReplaySegment(cmd *EntryCommand, datafactory DataFactory) (ts uint64, err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return
	}
	rel, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		return
	}
	cmd.Segment.table = rel
	if cmd.Segment.CurrOp == OpCreate {
		cmd.Segment.segData = datafactory.MakeSegmentFactory()(cmd.Segment)
		catalog.OnReplaySegmentID(cmd.Segment.ID)
		rel.addEntryLocked(cmd.Segment)
		ts = cmd.Segment.CreateAt
	} else {
		seg, _ := rel.GetSegmentByID(cmd.Segment.ID)
		if seg != nil {
			if err = rel.deleteEntryLocked(seg); err != nil {
				return
			}
		}
		rel.addEntryLocked(cmd.Segment)
		ts = cmd.Segment.DeleteAt
	}
	return ts, nil
}

func (catalog *Catalog) onReplayCreateBlock(cmd *EntryCommand, datafactory DataFactory, idx *wal.Index) (ts uint64, err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return 0, err
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		return 0, err
	}
	seg, err := tbl.GetSegmentByID(cmd.SegmentID)
	if err != nil {
		return 0, err
	}
	cmd.Block.RWMutex = new(sync.RWMutex)
	cmd.Block.CurrOp = OpCreate
	cmd.Block.segment = seg
	cmd.Block.blkData = datafactory.MakeBlockFactory(seg.segData.GetSegmentFile())(cmd.Block)
	catalog.OnReplayBlockID(cmd.Block.ID)
	cmd.Block.LogIndex = idx
	seg.addEntryLocked(cmd.Block)
	ts = cmd.Block.CreateAt
	return
}

func (catalog *Catalog) onReplayDropBlock(cmd *EntryCommand, idx *wal.Index) (ts uint64, err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return 0, err
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		return 0, err
	}
	seg, err := tbl.GetSegmentByID(cmd.SegmentID)
	if err != nil {
		return 0, err
	}
	blk, err := seg.GetBlockEntryByID(cmd.entry.ID)
	if err != nil {
		return 0, err
	}
	err = blk.OnReplayDrop(cmd.entry.DeleteAt)
	blk.LogIndex = idx
	ts = cmd.entry.DeleteAt
	return
}

func (catalog *Catalog) onReplayBlock(cmd *EntryCommand, datafactory DataFactory) (ts uint64, err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return
	}
	rel, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		return
	}
	seg, err := rel.GetSegmentByID(cmd.SegmentID)
	if err != nil {
		return
	}
	cmd.Block.segment = seg
	if cmd.Block.CurrOp == OpCreate {
		cmd.Block.blkData = datafactory.MakeBlockFactory(seg.segData.GetSegmentFile())(cmd.Block)
		catalog.OnReplayBlockID(cmd.Block.ID)
		seg.addEntryLocked(cmd.Block)
		ts = cmd.Block.CreateAt
	} else {
		blk, _ := seg.GetBlockEntryByID(cmd.Block.ID)
		if blk != nil {
			if err = seg.deleteEntryLocked(blk); err != nil {
				return
			}
		}
		seg.addEntryLocked(cmd.Block)
		ts = cmd.Block.DeleteAt
	}
	return ts, nil
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

func (catalog *Catalog) addEntryLocked(database *DBEntry) error {
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
	}
	return nil
}

func (catalog *Catalog) txnGetNodeByNameLocked(name string, txnCtx txnif.AsyncTxn) *common.DLNode {
	node := catalog.nameNodes[name]
	if node == nil {
		return nil
	}
	return node.TxnGetDBNodeLocked(txnCtx)
}

func (catalog *Catalog) GetDBEntry(name string, txnCtx txnif.AsyncTxn) (*DBEntry, error) {
	catalog.RLock()
	n := catalog.txnGetNodeByNameLocked(name, txnCtx)
	catalog.RUnlock()
	if n == nil {
		return nil, ErrNotFound
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
	dn := catalog.txnGetNodeByNameLocked(name, txnCtx)
	if dn == nil {
		err = ErrNotFound
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
	err = catalog.addEntryLocked(entry)
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
		entry := table.BaseEntry
		CheckpointOp(ckpEntry, entry, table, startTs, endTs)
		return
	}
	processor.DatabaseFn = func(database *DBEntry) (err error) {
		if database.IsSystemDB() {
			err = ErrStopCurrRecur
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
