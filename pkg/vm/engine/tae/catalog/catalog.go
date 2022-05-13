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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

// +--------+---------+----------+----------+------------+
// |   ID   |  Name   | CreateAt | DeleteAt | CommitInfo |
// +--------+---------+----------+----------+------------+
// |(uint64)|(varchar)| (uint64) | (uint64) |  (varchar) |
// +--------+---------+----------+----------+------------+

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
	// catalog.StateMachine.Start()
	return catalog
}

func OpenCatalog(dir, name string, cfg *store.StoreCfg, scheduler tasks.TaskScheduler) (*Catalog, error) {
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
	err = catalog.store.Replay(catalog.OnRelay)
	return catalog, err
}
func (catalog *Catalog) GetStore() store.Store { return catalog.store }
func (catalog *Catalog) ReplayCmd(txncmd txnif.TxnCmd) (err error) {
	switch txncmd.GetType() {
	case txnbase.CmdComposed:
		cmds := txncmd.(*txnbase.ComposedCmd)
		for _, cmds := range cmds.Cmds {
			catalog.ReplayCmd(cmds)
		}
	case CmdLogBlock:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayBlock(cmd)
	case CmdLogSegment:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplaySegment(cmd)
	case CmdLogTable:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayTable(cmd)
	case CmdLogDatabase:
		cmd := txncmd.(*EntryCommand)
		catalog.onReplayDatabase(cmd)
	case CmdCreateDatabase:
		cmd := txncmd.(*EntryCommand)
		err = catalog.onReplayCreateDatabase(cmd)
	case CmdCreateTable:
		cmd := txncmd.(*EntryCommand)
		err = catalog.onReplayCreateTable(cmd)
	case CmdCreateSegment:
		cmd := txncmd.(*EntryCommand)
		err = catalog.onReplayCreateSegment(cmd)
	case CmdCreateBlock:
		cmd := txncmd.(*EntryCommand)
		err = catalog.onReplayCreateBlock(cmd)
	case CmdDropTable:
		cmd := txncmd.(*EntryCommand)
		err = catalog.onReplayDropTable(cmd)
	case CmdDropDatabase:
		cmd := txncmd.(*EntryCommand)
		err = catalog.onReplayDropDatabase(cmd)
	case CmdDropSegment:
		cmd := txncmd.(*EntryCommand)
		err = catalog.onReplayDropSegment(cmd)
	case CmdDropBlock:
		cmd := txncmd.(*EntryCommand)
		err = catalog.onReplayDropBlock(cmd)
	default:
		// panic("unsupport")
	}
	return
}

func (catalog *Catalog) onReplayCreateDatabase(cmd *EntryCommand) (err error) {
	entry := NewDBEntry(catalog, cmd.DB.name, nil)
	entry.CreateAt = cmd.entry.CreateAt
	err = catalog.addEntryLocked(entry)
	return
}

func (catalog *Catalog) onReplayDropDatabase(cmd *EntryCommand) (err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return err
	}
	db.CurrOp = OpSoftDelete
	db.DeleteAt = cmd.entry.DeleteAt
	return
}
func (catalog *Catalog) onReplayDatabase(cmd *EntryCommand) (err error) {
	cmd.DB.catalog = catalog
	if cmd.DB.CurrOp == OpCreate {
		return catalog.addEntryLocked(cmd.DB)
	} else {
		db, _ := catalog.GetDatabaseByID(cmd.DB.ID)
		if db != nil {
			catalog.RemoveEntry(db)
		}
		return catalog.addEntryLocked(cmd.DB)
	}
}

func (catalog *Catalog) onReplayCreateTable(cmd *EntryCommand) (err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return err
	}
	meta, err := db.CreateTableEntry(cmd.Table.schema, nil, nil)
	if err != nil {
		return err
	}
	meta.CreateAt = cmd.entry.CreateAt
	return
}

func (catalog *Catalog) onReplayDropTable(cmd *EntryCommand) (err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return err
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		return err
	}
	tbl.CurrOp = OpSoftDelete
	tbl.DeleteAt = cmd.entry.DeleteAt
	return
}
func (catalog *Catalog) onReplayTable(cmd *EntryCommand) (err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return
	}
	cmd.Table.db = db
	if cmd.Table.CurrOp == OpCreate {
		return db.addEntryLocked(cmd.Table)
	} else {
		rel, _ := db.GetTableEntryByID(cmd.Table.ID)
		if rel != nil {
			db.RemoveEntry(rel)
		}
		return db.addEntryLocked(cmd.Table)
	}
}

func (catalog *Catalog) onReplayCreateSegment(cmd *EntryCommand) (err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return err
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		return err
	}
	cmd.Segment.table = tbl
	cmd.Segment.RWMutex = new(sync.RWMutex)
	cmd.Segment.CurrOp = OpCreate
	cmd.Segment.link = new(common.Link)
	cmd.Segment.entries = make(map[uint64]*common.DLNode)
	tbl.addEntryLocked(cmd.Segment)
	return
}
func (catalog *Catalog) onReplayDropSegment(cmd *EntryCommand) (err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return err
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		return err
	}
	seg, err := tbl.GetSegmentByID(cmd.entry.ID)
	if err != nil {
		return err
	}
	seg.CurrOp = OpSoftDelete
	seg.DeleteAt = cmd.entry.DeleteAt
	return
}
func (catalog *Catalog) onReplaySegment(cmd *EntryCommand) (err error) {
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
		rel.addEntryLocked(cmd.Segment)
	} else {
		seg, _ := rel.GetSegmentByID(cmd.Segment.ID)
		if seg != nil {
			rel.deleteEntryLocked(seg)
		}
		rel.addEntryLocked(cmd.Segment)
	}
	return nil
}

func (catalog *Catalog) onReplayCreateBlock(cmd *EntryCommand) (err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return err
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		return err
	}
	seg, err := tbl.GetSegmentByID(cmd.SegmentID)
	if err != nil {
		return err
	}
	cmd.Block.RWMutex = new(sync.RWMutex)
	cmd.Block.CurrOp = OpCreate
	cmd.Block.segment = seg
	cmd.Block.state = seg.state
	seg.addEntryLocked(cmd.Block)
	return
}
func (catalog *Catalog) onReplayDropBlock(cmd *EntryCommand) (err error) {
	db, err := catalog.GetDatabaseByID(cmd.DBID)
	if err != nil {
		return err
	}
	tbl, err := db.GetTableEntryByID(cmd.TableID)
	if err != nil {
		return err
	}
	seg, err := tbl.GetSegmentByID(cmd.SegmentID)
	if err != nil {
		return err
	}
	blk, err := seg.GetBlockEntryByID(cmd.entry.ID)
	if err != nil {
		return err
	}
	blk.CurrOp = OpSoftDelete
	blk.DeleteAt = cmd.entry.DeleteAt
	return
}
func (catalog *Catalog) onReplayBlock(cmd *EntryCommand) (err error) {
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
		seg.addEntryLocked(cmd.Block)
	} else {
		blk, _ := seg.GetBlockEntryByID(cmd.Block.ID)
		if blk != nil {
			seg.deleteEntryLocked(blk)
		}
		seg.addEntryLocked(cmd.Block)
	}
	return nil
}

func (catalog *Catalog) OnRelay(group uint32, commitId uint64, payload []byte, typ uint16, info interface{}) (err error) {
	if typ != ETCatalogCheckpoint {
		return
	}
	e := NewEmptyCheckpointEntry()
	e.Unarshal(payload)
	checkpoint := new(Checkpoint)
	checkpoint.LSN = commitId
	checkpoint.MaxTS = e.MaxTS
	for _, cmd := range e.Entries {
		catalog.ReplayCmd(cmd)
	}
	if len(catalog.checkpoints) == 0 {
		catalog.checkpoints = append(catalog.checkpoints, checkpoint)
	} else {
		catalog.checkpoints[0] = checkpoint
	}
	return
}

func (catalog *Catalog) Close() error {
	if catalog.store != nil {
		catalog.store.Close()
	}
	return nil
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
			logutil.Info(record.String())
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
	catalog.Lock()
	defer catalog.Unlock()
	if n, ok := catalog.entries[database.GetID()]; !ok {
		return ErrNotFound
	} else {
		nn := catalog.nameNodes[database.name]
		nn.DeleteNode(database.GetID())
		catalog.link.Delete(n)
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
		entry := database.BaseEntry
		CheckpointOp(ckpEntry, entry, database, startTs, endTs)
		return
	}
	catalog.RecurLoop(processor)
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
	checkpoint.LSN, err = catalog.store.AppendEntry(0, logEntry)
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
