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

package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	// worker "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker"
	// workerBase "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker/base"
)

var (
	DefaultCheckpointDelta    = uint64(10000)
	DefaultCheckpointInterval = time.Minute
)

var (
	ErrResourceStale       = errors.New("aoe: resource is stale")
	ErrDuplicate           = errors.New("aoe: duplicate")
	ErrDatabaseNotFound    = errors.New("aoe: db not found")
	ErrTableNotFound       = errors.New("aoe: table not found")
	ErrSegmentNotFound     = errors.New("aoe: segment not found")
	ErrBlockNotFound       = errors.New("aoe: block not found")
	ErrInvalidSchema       = errors.New("aoe: invalid schema")
	ErrInconsistentShardId = errors.New("aoe: InconsistentShardIdErr")
	ErrCannotHardDelete    = errors.New("aoe: cannot hard delete now")
	ErrCommitStale         = errors.New("aoe: commit stale info")
	ErrIdempotence         = errors.New("aoe: idempotence error")
	ErrDupIndex            = errors.New("aoe: dup index")
	ErrIndexNotFound       = errors.New("aoe: index not found")
	ErrReplayFailed        = errors.New("aoe: replay failed")
)

type CatalogCfg struct {
	Dir                 string `json:"-"`
	BlockMaxRows        uint64 `toml:"block-max-rows"`
	SegmentMaxBlocks    uint64 `toml:"segment-max-blocks"`
	RotationFileMaxSize int    `toml:"rotation-file-max-size"`
}

type Catalog struct {
	sm.ClosedState  `json:"-"`
	sm.StateMachine `json:"-"`
	*sync.RWMutex   `json:"-"`
	Sequence        `json:"-"`
	pipeline        *commitPipeline      `json:"-"`
	Store           logstore.AwareStore  `json:"-"`
	IndexWal        Wal                  `json:"-"`
	Cfg             *CatalogCfg          `json:"-"`
	nodesMu         sync.RWMutex         `json:"-"`
	commitMu        sync.RWMutex         `json:"-"`
	nameNodes       map[string]*nodeList `json:"-"`
	// checkpointer    workerBase.IHeartbeater

	Databases map[uint64]*Database `json:"dbs"`
}

func OpenCatalog(mu *sync.RWMutex, cfg *CatalogCfg) (*Catalog, error) {
	replayer := newCatalogReplayer()
	return replayer.RebuildCatalog(mu, cfg)
}

func OpenCatalogWithDriver(mu *sync.RWMutex, cfg *CatalogCfg, store logstore.AwareStore, indexWal wal.ShardAwareWal) (*Catalog, error) {
	replayer := newCatalogReplayer()
	return replayer.RebuildCatalogWithDriver(mu, cfg, store, indexWal)
}

func NewCatalogWithDriver(mu *sync.RWMutex, cfg *CatalogCfg, store logstore.AwareStore, indexWal wal.ShardAwareWal) *Catalog {
	catalog := &Catalog{
		RWMutex:   mu,
		Cfg:       cfg,
		Databases: make(map[uint64]*Database),
		nameNodes: make(map[string]*nodeList),
		Store:     store,
		IndexWal:  indexWal,
	}

	wg := new(sync.WaitGroup)
	rQueue := sm.NewSafeQueue(100000, 100, nil)
	ckpQueue := sm.NewSafeQueue(100000, 10, catalog.onCheckpoint)
	catalog.StateMachine = sm.NewStateMachine(wg, catalog, rQueue, ckpQueue)
	catalog.pipeline = newCommitPipeline(catalog)
	// catalog.checkpointer = worker.NewHeartBeater(DefaultCheckpointInterval, &catalogCheckpointer{
	// 	catalog: catalog,
	// })
	return catalog
}

// Unused
// type catalogCheckpointer struct {
// 	catalog *Catalog
// }

// Unused
// func (c *catalogCheckpointer) OnExec() {
// 	previousCheckpointId := c.catalog.GetCheckpointId()
// 	commitId := c.catalog.Store.GetSyncedId()
// 	if commitId < previousCheckpointId+DefaultCheckpointDelta {
// 		return
// 	}
// 	c.catalog.Checkpoint()
// }
// func (c *catalogCheckpointer) OnStopped() {}

func NewCatalog(mu *sync.RWMutex, cfg *CatalogCfg) *Catalog {
	if cfg.RotationFileMaxSize <= 0 {
		logutil.Warnf("Set rotation max size to default size: %s", common.ToH(uint64(logstore.DefaultVersionFileSize)))
		cfg.RotationFileMaxSize = logstore.DefaultVersionFileSize
	}
	catalog := &Catalog{
		RWMutex:   mu,
		Cfg:       cfg,
		Databases: make(map[uint64]*Database),
		nameNodes: make(map[string]*nodeList),
	}

	rotationCfg := &logstore.RotationCfg{}
	rotationCfg.RotateChecker = &logstore.MaxSizeRotationChecker{
		MaxSize: cfg.RotationFileMaxSize,
	}
	store, err := logstore.NewBatchStore(common.MakeMetaDir(cfg.Dir), "store", rotationCfg)
	if err != nil {
		panic(err)
	}
	catalog.Store = store
	wg := new(sync.WaitGroup)
	rQueue := sm.NewSafeQueue(100000, 100, nil)
	ckpQueue := sm.NewSafeQueue(100000, 10, catalog.onCheckpoint)
	catalog.StateMachine = sm.NewStateMachine(wg, catalog, rQueue, ckpQueue)
	catalog.pipeline = newCommitPipeline(catalog)
	// catalog.checkpointer = worker.NewHeartBeater(DefaultCheckpointInterval, &catalogCheckpointer{
	// 	catalog: catalog,
	// })
	return catalog
}

func (catalog *Catalog) unregisterDatabaseLocked(db *Database) error {
	node := catalog.nameNodes[db.Name]
	if node == nil {
		return ErrDatabaseNotFound
	}
	_, empty := node.DeleteNode(db.Id)
	if empty {
		delete(catalog.nameNodes, db.Name)
	}
	delete(catalog.Databases, db.Id)
	return nil
}

func (catalog *Catalog) Compact(dbListener DatabaseListener, tblListener TableListener) {
	dbs := make([]*Database, 0, 4)
	hardDeletes := make([]*Database, 0, 4)

	catalog.RLock()
	for _, db := range catalog.Databases {
		dbs = append(dbs, db)
	}
	catalog.RUnlock()
	for _, db := range dbs {
		db.Compact(dbListener, tblListener)
		db.RLock()
		if db.IsHardDeletedLocked() && db.HasCommittedLocked() {
			hardDeletes = append(hardDeletes, db)
		}
		db.RUnlock()
	}
	if len(hardDeletes) > 0 {
		catalog.Lock()
		for _, db := range hardDeletes {
			if err := catalog.unregisterDatabaseLocked(db); err != nil {
				panic(err)
			}
		}
		catalog.Unlock()
	}
	for _, db := range hardDeletes {
		db.Release()
		if dbListener != nil {
			dbListener.OnDatabaseCompacted(db)
		}
	}
}

func (catalog *Catalog) DebugCheckReplayedState() {
	if catalog.pipeline == nil {
		panic("pipeline is missing")
	}
	if catalog.nameNodes == nil {
		panic("nameNodes is missing")
	}
	for _, db := range catalog.Databases {
		db.DebugCheckReplayedState()
	}
}

func (catalog *Catalog) Start() {
	// catalog.checkpointer.Start()
	catalog.Store.Start()
	catalog.StateMachine.Start()
}

func (catalog *Catalog) Close() error {
	// catalog.checkpointer.Stop()
	catalog.Stop()
	if catalog.IndexWal != nil {
		catalog.IndexWal.Close()
	} else {
		catalog.Store.Close()
	}
	logutil.Infof("[AOE] Safe synced id %d", catalog.GetSafeCommitId())
	logutil.Infof("[AOE] Safe checkpointed id %d", catalog.GetCheckpointId())
	return nil
}

func (catalog *Catalog) CommitSplit(replace *dbReplaceLogEntry) error {
	ctx := new(commitSplitCtx)
	ctx.replace = replace
	return catalog.onCommitRequest(ctx, true)
}

func (catalog *Catalog) prepareCommitSplit(ctx *commitSplitCtx) (LogEntry, error) {
	logEntry := catalog.prepareCommitEntry(ctx.replace, ETSplitDatabase, nil)
	return logEntry, nil
}

func (catalog *Catalog) StartTxn(index *LogIndex) *TxnCtx {
	return NewTxn(catalog, index)
}

func (catalog *Catalog) CommitTxn(txn *TxnCtx) error {
	return catalog.onCommitRequest(txn, true)
}

func (catalog *Catalog) prepareCommitTxn(txn *TxnCtx) (LogEntry, error) {
	logEntry := catalog.prepareCommitEntry(txn.store, ETTransaction, nil)
	return logEntry, nil
}

func (catalog *Catalog) AbortTxn(txn *TxnCtx) error {
	// TODO
	return nil
}

func (catalog *Catalog) SimpleGetDatabase(id uint64) (*Database, error) {
	catalog.RLock()
	defer catalog.RUnlock()
	db := catalog.Databases[id]
	if db == nil {
		return nil, ErrDatabaseNotFound
	}
	return db, nil
}

func (catalog *Catalog) SimpleGetTableByName(dbName, tableName string) (*Table, error) {
	database, err := catalog.SimpleGetDatabaseByName(dbName)
	if err != nil {
		return nil, err
	}
	table := database.SimpleGetTableByName(tableName)
	if table == nil {
		return nil, ErrTableNotFound
	}
	return table, nil
}

func (catalog *Catalog) GetTableByNameAndLogIndex(dbName, tableName string, index *LogIndex) (*Table, error) {
	database, err := catalog.SimpleGetDatabaseByName(dbName)
	if err != nil {
		return nil, err
	}
	return database.GetTableByNameAndLogIndex(tableName, index)
}

func (catalog *Catalog) SimpleReplaceDatabase(view *databaseLogEntry, replaced *Database, tranId uint64) error {
	ctx := new(replaceDatabaseCtx)
	ctx.tranId = tranId
	ctx.inTran = true
	ctx.view = view
	ctx.replaced = replaced
	return catalog.onCommitRequest(ctx, true)
}

func (catalog *Catalog) prepareReplaceDatabase(ctx *replaceDatabaseCtx) (LogEntry, error) {
	var err error
	entry := newDbReplaceLogEntry()
	catalog.RLock()
	logIndex := new(LogIndex)
	logIndex.Id = shard.SimpleIndexId(ctx.view.LogRange.Range.Right)
	logIndex.ShardId = ctx.view.LogRange.ShardId
	if ctx.replaced != nil {
		db := ctx.replaced
		rCtx := new(addReplaceCommitCtx)
		rCtx.tranId = ctx.tranId
		rCtx.inTran = true
		rCtx.database = db
		rCtx.exIndex = logIndex
		if _, err = db.prepareReplace(rCtx); err != nil {
			panic(err)
		}
		if !rCtx.discard {
			entry.Replaced = &databaseLogEntry{
				BaseEntry: db.BaseEntry,
				Id:        db.Id,
			}
		}
		entry.Index = ctx.replaced.GetCommit().GetIndex()
	}
	catalog.RUnlock()

	nCtx := new(addDatabaseCtx)
	nCtx.tranId = ctx.tranId
	nCtx.inTran = true
	nCtx.database = ctx.view.Database
	if _, err = catalog.prepareAddDatabase(nCtx); err != nil {
		panic(err)
	}

	entry.AddReplacer(ctx.view.Database)
	logEntry := catalog.prepareCommitEntry(entry, ETReplaceDatabase, nil)
	return logEntry, err
}

func (catalog *Catalog) prepareAddDatabase(ctx *addDatabaseCtx) (LogEntry, error) {
	var err error
	catalog.Lock()
	if err = catalog.onNewDatabase(ctx.database); err != nil {
		catalog.Unlock()
		return nil, err
	}
	catalog.Unlock()
	if ctx.inTran {
		return nil, nil
	}
	panic("todo")
}

func (catalog *Catalog) LoopLocked(processor Processor) error {
	var err error
	for _, database := range catalog.Databases {
		if err = processor.OnDatabase(database); err != nil {
			return err
		}
	}
	return err
}

func (catalog *Catalog) RecurLoopLocked(processor Processor) error {
	var err error
	for _, database := range catalog.Databases {
		if err = processor.OnDatabase(database); err != nil {
			return err
		}
		database.RLock()
		defer database.RUnlock()
		if err = database.RecurLoopLocked(processor); err != nil {
			return err
		}
	}
	return err
}

func (catalog *Catalog) CommitLogEntry(entry logstore.Entry, commitId uint64, sync bool) error {
	var err error
	entry.SetAuxilaryInfo(commitId)
	if err = catalog.Store.AppendEntry(entry); err != nil {
		return err
	}
	if sync {
		err = catalog.Store.Sync()
	}
	return err
}

func (catalog *Catalog) onCommitRequest(ctx interface{}, doCommit bool) error {
	entry, err := catalog.pipeline.prepare(ctx)
	if err != nil {
		return err
	}
	if !doCommit {
		return nil
	}
	err = catalog.pipeline.commit(entry)
	return err
}

func (catalog *Catalog) prepareCommitLog(entry IEntry, logEntry LogEntry) {
	catalog.commitMu.Lock()
	defer catalog.commitMu.Unlock()
	commitId := catalog.NextCommitId()
	entry.Lock()
	entry.CommitLocked(commitId)
	entry.Unlock()
	SetCommitIdToLogEntry(commitId, logEntry)
	logEntry.SetAuxilaryInfo(commitId)
	if err := catalog.Store.AppendEntry(logEntry); err != nil {
		panic(err)
	}
}

func (catalog *Catalog) prepareCommitEntry(entry IEntry, eType LogEntryType, locker sync.Locker) LogEntry {
	catalog.commitMu.Lock()
	defer catalog.commitMu.Unlock()
	commitId := catalog.NextCommitId()
	var logEntry LogEntry
	if locker == nil {
		entry.Lock()
		entry.CommitLocked(commitId)
		logEntry = entry.ToLogEntry(eType)
		entry.Unlock()
	} else {
		entry.CommitLocked(commitId)
		logEntry = entry.ToLogEntry(eType)
		locker.Unlock()
		defer locker.Lock()
	}
	SetCommitIdToLogEntry(commitId, logEntry)
	logEntry.SetAuxilaryInfo(commitId)
	if err := catalog.Store.AppendEntry(logEntry); err != nil {
		panic(err)
	}
	return logEntry
}

func (catalog *Catalog) GetSafeCommitId() uint64 {
	return catalog.Store.GetSyncedId()
}

func (catalog *Catalog) GetCheckpointId() uint64 {
	return catalog.Store.GetCheckpointId()
}

func (catalog *Catalog) onCheckpoint(items ...interface{}) {
	catalog.Store.TryCompact()
}

func (catalog *Catalog) onReplayTableEntry(entry *tableCheckpoint) error {
	if entry.NeedReplay {
		err := catalog.onReplayTableCheckpoint(&entry.LogEntry)
		if err != nil {
			panic(ErrReplayFailed)
		}
	}
	for _, segmentEntry := range entry.Segments {
		if segmentEntry.NeedReplay {
			err := catalog.onReplaySegmentCheckpoint(
				&segmentEntry.LogEntry)
			if err != nil {
				panic(ErrReplayFailed)
			}
		}
		for _, blockEntry := range segmentEntry.Blocks {
			err := catalog.onReplayBlockCheckpoint(blockEntry)
			if err != nil {
				panic(ErrReplayFailed)
			}
		}
	}
	return nil
}

func (catalog *Catalog) onReplayCheckpoint(entry *catalogLogEntry) error {
	catalogProcessor := new(LoopProcessor)

	var currentDatabaseEntry *databaseCheckpoint

	catalogProcessor.DatabaseFn = func(db *Database) error {
		databaseEntry, ok := entry.Databases[db.Id]

		//delete the database if its checkpoint entry doesn't exist
		if !ok {
			// db.CommitInfo.Op = OpHardDelete
			// db.CommitInfo.CommitId = entry.Range.Right
			err := catalog.unregisterDatabaseLocked(db)
			if err != nil {
				panic(ErrReplayFailed)
			}
			return nil
		}

		//if replay the database
		if databaseEntry.NeedReplay {
			err := catalog.onReplayDatabaseCheckpoint(
				&databaseEntry.LogEntry)
			if err != nil {
				panic(ErrReplayFailed)
			}
		}

		//replay the tables in the database
		currentDatabaseEntry = databaseEntry
		err := db.LoopLocked(catalogProcessor)
		if err != nil {
			panic(ErrReplayFailed)
		}
		for _, tableEntry := range databaseEntry.Tables {
			err := catalog.onReplayTableEntry(tableEntry)
			if err != nil {
				panic(ErrReplayFailed)
			}
		}
		delete(entry.Databases, db.Id)
		return nil
	}

	catalogProcessor.TableFn = func(tb *Table) error {
		tableEntry, ok := currentDatabaseEntry.Tables[tb.Id]
		if !ok {
			tb.CommitInfo.Op = OpHardDelete
			tb.CommitInfo.CommitId = entry.Range.Right
			return nil
		}
		err := catalog.onReplayTableEntry(tableEntry)
		if err != nil {
			panic(ErrReplayFailed)
		}
		delete(currentDatabaseEntry.Tables, tb.Id)
		return nil
	}
	err := catalog.LoopLocked(catalogProcessor)
	if err != nil {
		panic(ErrReplayFailed)
	}

	for _, databaseEntry := range entry.Databases {
		if databaseEntry.NeedReplay {
			err := catalog.onReplayDatabaseCheckpoint(
				&databaseEntry.LogEntry)
			if err != nil {
				panic(ErrReplayFailed)
			}
		}
		for _, tableEntry := range databaseEntry.Tables {
			err := catalog.onReplayTableEntry(tableEntry)
			if err != nil {
				panic(ErrReplayFailed)
			}
		}
	}
	return nil
}

func (catalog *Catalog) Checkpoint() (logstore.AsyncEntry, error) {
	catalog.RLock()
	entry := catalog.ToLogEntry(logstore.ETCheckpoint)
	catalog.RUnlock()
	if err := catalog.Store.Checkpoint(entry); err != nil {
		panic(err)
	}
	ret, err := catalog.EnqueueCheckpoint(entry)
	if err != nil {
		return nil, err
	}
	return ret.(logstore.AsyncEntry), nil
}

func (catalog *Catalog) PString(level PPLevel, depth int) string {
	catalog.RLock()
	defer catalog.RUnlock()
	ident := strings.Repeat("  ", depth)
	ident2 := " " + ident
	s := fmt.Sprintf("Catalog | Cnt=%d {", len(catalog.Databases))
	for _, db := range catalog.Databases {
		s = fmt.Sprintf("%s\n%s%s", s, ident2, db.PString(level, depth+1))
	}
	if len(catalog.Databases) == 0 {
		s = fmt.Sprintf("%s}", s)
	} else {
		s = fmt.Sprintf("%s\n%s}", s, ident)
	}
	return s
}

func (catalog *Catalog) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, catalog)
}

func (entry *catalogLogEntry) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, entry)
}

func (catalog *Catalog) ToCatalogLogEntry() *catalogLogEntry {
	catalogCkp := &catalogLogEntry{}
	if catalog.IndexWal != nil {
		catalogCkp.SafeIds = catalog.IndexWal.GetAllShardCheckpointId()
	}
	catalogCkp.Databases = map[uint64]*databaseCheckpoint{}
	previousCheckpointId := catalog.GetCheckpointId()
	commitId := catalog.Store.GetSyncedId()

	processor := new(LoopProcessor)

	catalogCkp.Range = &common.Range{
		Left:  previousCheckpointId + 1,
		Right: commitId,
	}

	check := func(info *CommitInfo) bool {
		return catalogCkp.Range.ClosedIn(info.CommitId)
	}

	processor.DatabaseFn = func(db *Database) error {
		databaseCkp := &databaseCheckpoint{}
		databaseCkp.Tables = make(map[uint64]*tableCheckpoint)
		db.RLock()
		info := db.CommitInfo.ChooseCommitInfo(check)
		if info != nil {
			databaseCkp.NeedReplay = true
			databaseCkp.LogEntry = db.ToDatabaseLogEntry(info)
		}
		db.RUnlock()
		catalogCkp.Databases[db.Id] = databaseCkp
		return nil
	}

	processor.TableFn = func(tb *Table) error {
		tableCkp := &tableCheckpoint{}
		tableCkp.Segments = make([]*segmentCheckpoint, 0)
		tb.RLock()
		info := tb.CommitInfo.ChooseCommitInfo(check)
		if info != nil {
			tableCkp.NeedReplay = true
			tableCkp.LogEntry = tb.ToTableLogEntry(info)
		}
		tb.RUnlock()
		catalogCkp.Databases[tb.Database.Id].
			Tables[tb.Id] = tableCkp
		return nil
	}

	processor.SegmentFn = func(seg *Segment) error {
		segmentCkp := &segmentCheckpoint{}
		segmentCkp.Blocks = make([]*blockLogEntry, 0)
		seg.RLock()
		info := seg.CommitInfo.ChooseCommitInfo(check)
		if info != nil {
			segmentCkp.NeedReplay = true
			segmentCkp.LogEntry = *seg.toLogEntry(info)
		}
		seg.RUnlock()
		tableCkp := catalogCkp.Databases[seg.Table.Database.Id].
			Tables[seg.Table.Id]
		tableCkp.Segments = append(tableCkp.Segments, segmentCkp)
		return nil
	}

	processor.BlockFn = func(blk *Block) error {
		blk.RLock()
		info := blk.CommitInfo.ChooseCommitInfo(check)
		if info != nil {
			blkEntry := blk.toLogEntry(info)
			segId := blk.Segment.Id
			segIdx := blk.Segment.Table.IdIndex[segId]
			segmentCkp := catalogCkp.
				Databases[blk.Segment.Table.Database.Id].
				Tables[blk.Segment.Table.Id].
				Segments[segIdx]
			segmentCkp.Blocks = append(
				segmentCkp.Blocks, blkEntry)
		}
		blk.RUnlock()
		return nil
	}

	err := catalog.RecurLoopLocked(processor)
	if err != nil {
		panic(ErrReplayFailed)
	}

	return catalogCkp
}

func (entry *catalogLogEntry) Marshal() ([]byte, error) {

	return json.Marshal(entry)
}

func (catalog *Catalog) CommitLocked(uint64) {}

func (catalog *Catalog) ToLogEntry(eType LogEntryType) LogEntry {
	switch eType {
	case logstore.ETCheckpoint:
		break
	default:
		panic("not supported")
	}
	entry := catalog.ToCatalogLogEntry()
	checkpointRange := entry.Range
	buf, err := entry.Marshal()
	if err != nil {
		panic(err)
	}
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.SetAuxilaryInfo(checkpointRange)
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}

func (catalog *Catalog) SimpleGetDatabaseNames() (names []string) {
	catalog.RLock()
	defer catalog.RUnlock()
	for _, db := range catalog.Databases {
		if db.IsDeleted() {
			continue
		}
		names = append(names, db.Name)
	}
	return
}

func (catalog *Catalog) SimpleCreateDatabase(name string, index *LogIndex) (*Database, error) {
	tranId := catalog.NextUncommitId()
	ctx := new(createDatabaseCtx)
	ctx.tranId = tranId
	ctx.name = name
	ctx.exIndex = index
	err := catalog.onCommitRequest(ctx, true)
	return ctx.database, err
}

func (catalog *Catalog) CreateDatabaseInTxn(txn *TxnCtx, name string) (*Database, error) {
	ctx := new(createDatabaseCtx)
	ctx.tranId = txn.tranId
	ctx.name = name
	ctx.exIndex = txn.index
	ctx.inTran = true
	ctx.txn = txn
	err := catalog.onCommitRequest(ctx, false)
	return ctx.database, err
}

func (catalog *Catalog) prepareCreateDatabase(ctx *createDatabaseCtx) (LogEntry, error) {
	var err error
	db := NewDatabase(catalog, ctx.name, ctx.tranId, ctx.exIndex)
	catalog.Lock()
	if err = catalog.onNewDatabase(db); err != nil {
		catalog.Unlock()
		return nil, err
	}
	catalog.Unlock()
	ctx.database = db
	if ctx.inTran {
		ctx.txn.AddEntry(db, ETCreateDatabase)
		return nil, nil
	}
	if ctx.exIndex == nil {
		ctx.exIndex = &LogIndex{
			ShardId: db.Id,
			Id:      shard.SimpleIndexId(0),
		}
	}
	entry := db.ToLogEntry(ETCreateDatabase)
	catalog.prepareCommitLog(db, entry)
	return entry, err
}

func (catalog *Catalog) SimpleDropDatabaseByName(name string, index *LogIndex) error {
	db, err := catalog.SimpleGetDatabaseByName(name)
	if err != nil {
		return err
	}
	tranId := catalog.NextUncommitId()
	ctx := new(dropDatabaseCtx)
	ctx.tranId = tranId
	ctx.exIndex = index
	ctx.database = db
	return catalog.onCommitRequest(ctx, true)
}

func (catalog *Catalog) DropDatabaseByNameInTxn(txn *TxnCtx, name string) error {
	database, err := catalog.GetDatabaseByNameInTxn(txn, name)
	if err != nil {
		return err
	}
	ctx := new(dropDatabaseCtx)
	ctx.tranId = txn.tranId
	ctx.exIndex = txn.index
	ctx.database = database
	ctx.inTran = true
	ctx.txn = txn
	return catalog.onCommitRequest(ctx, false)
}

func (catalog *Catalog) GetDatabaseByNameInTxn(txn *TxnCtx, name string) (*Database, error) {
	catalog.RLock()
	defer catalog.RUnlock()
	nn := catalog.nameNodes[name]
	if nn == nil {
		return nil, ErrDatabaseNotFound
	}
	db := nn.GetDatabase()
	if db.IsDeletedInTxnLocked(txn) {
		return nil, ErrDatabaseNotFound
	}
	return db, nil
}

func (catalog *Catalog) GetDatabaseByName(name string) (*Database, error) {
	catalog.RLock()
	defer catalog.RUnlock()
	nn := catalog.nameNodes[name]
	if nn == nil {
		return nil, ErrDatabaseNotFound
	}
	db := nn.GetDatabase()
	return db, nil
}

func (catalog *Catalog) SimpleGetDatabaseByName(name string) (*Database, error) {
	catalog.RLock()
	defer catalog.RUnlock()
	nn := catalog.nameNodes[name]
	if nn == nil {
		return nil, ErrDatabaseNotFound
	}
	db := nn.GetDatabase()
	if db.IsDeletedLocked() || !db.HasCommittedLocked() {
		return nil, ErrDatabaseNotFound
	}
	return db, nil
}

func (catalog *Catalog) SimpleHardDeleteDatabase(id uint64) error {
	catalog.Lock()
	db := catalog.Databases[id]
	if db == nil {
		catalog.Unlock()
		return ErrDatabaseNotFound
	}
	catalog.Unlock()
	return db.SimpleHardDelete()
}

func (catalog *Catalog) SplitCheck(size, index uint64, dbName string) (coarseSize uint64, coarseCount uint64, keys [][]byte, ctx []byte, err error) {
	var db *Database
	db, err = catalog.SimpleGetDatabaseByName(dbName)
	if err != nil {
		return
	}
	return db.SplitCheck(size, index)
}

// Unused
// func (catalog *Catalog) execSplit(rename RenameTableFactory, spec *ShardSplitSpec, tranId uint64, index *LogIndex, dbSpecs []*DBSpec) error {
// 	ctx := new(splitDBCtx)
// 	ctx.spec = spec
// 	ctx.renameTable = rename
// 	ctx.tranId = tranId
// 	ctx.exIndex = index
// 	ctx.dbSpecs = dbSpecs
// 	return catalog.onCommitRequest(ctx, true)
// }

func (catalog *Catalog) prepareSplit(ctx *splitDBCtx) (LogEntry, error) {
	entry := newDbReplaceLogEntry()
	db := ctx.spec.db
	dbs := make(map[uint64]*Database)
	for _, spec := range ctx.dbSpecs {
		nDB := NewDatabase(catalog, spec.Name, ctx.tranId, nil)
		spec.ShardId = nDB.CommitInfo.GetShardId()
		dbs[spec.ShardId] = nDB
		entry.AddReplacer(nDB)
	}
	for _, spec := range ctx.spec.Specs {
		table := ctx.spec.splitted[spec.Index]
		table.Splite(catalog, ctx.tranId, spec, ctx.renameTable, dbs)
	}
	rCtx := new(addReplaceCommitCtx)
	rCtx.tranId = ctx.tranId
	rCtx.inTran = true
	rCtx.database = db
	rCtx.exIndex = ctx.exIndex
	if _, err := db.prepareReplace(rCtx); err != nil {
		panic(err)
	}
	entry.Replaced = &databaseLogEntry{
		BaseEntry: db.BaseEntry,
		Id:        db.Id,
	}

	for _, nDB := range dbs {
		nCtx := new(addDatabaseCtx)
		nCtx.tranId = ctx.tranId
		nCtx.inTran = true
		nCtx.database = nDB
		if _, err := catalog.prepareAddDatabase(nCtx); err != nil {
			panic(err)
		}
	}

	logEntry := db.Catalog.prepareCommitEntry(entry, ETSplitDatabase, nil)
	return logEntry, nil
}

func (catalog *Catalog) onNewDatabase(db *Database) error {
	nn := catalog.nameNodes[db.Name]
	if nn != nil {
		e := nn.GetDatabase()
		if !e.IsDeletedLocked() {
			return ErrDuplicate
		}
		catalog.Databases[db.Id] = db
		nn.CreateNode(db.Id)
	} else {
		catalog.Databases[db.Id] = db

		nn := newNodeList(catalog, &catalog.nodesMu, db.Name)
		catalog.nameNodes[db.Name] = nn

		nn.CreateNode(db.Id)
	}
	return nil
}

func (catalog *Catalog) onReplayNewDatabase(db *Database) error {
	nn := catalog.nameNodes[db.Name]
	if nn != nil {
		e := nn.GetDatabase()
		if !e.IsDeletedLocked() {
			if db.IsDeletedLocked() {
				catalog.Databases[db.Id] = db
				nn.DeleteNode(e.Id)
				nn.CreateNode(db.Id)
				nn.CreateNode(e.Id)
				return nil
			}
			return ErrDuplicate
		}
		catalog.Databases[db.Id] = db
		nn.CreateNode(db.Id)
	} else {
		catalog.Databases[db.Id] = db

		nn := newNodeList(catalog, &catalog.nodesMu, db.Name)
		catalog.nameNodes[db.Name] = nn

		nn.CreateNode(db.Id)
	}
	return nil
}

func (catalog *Catalog) onReplayCreateTable(entry *tableLogEntry) error {
	db := catalog.Databases[entry.DatabaseId]
	tbl := NewEmptyTableEntry(db)
	tbl.BaseEntry = entry.Table.BaseEntry
	tbl.Schema = entry.Table.Schema
	return db.onNewTable(tbl)
}

func (catalog *Catalog) onReplayTableOperation(entry *tableLogEntry) error {
	db := catalog.Databases[entry.DatabaseId]
	tbl := db.TableSet[entry.Id]
	return tbl.onCommit(entry.CommitInfo)
}

func (catalog *Catalog) onReplayTableCheckpoint(entry *tableLogEntry) error {
	db := catalog.Databases[entry.DatabaseId]
	tbl, ok := db.TableSet[entry.Table.Id]
	if ok {
		return tbl.onCommit(entry.Table.CommitInfo)
	}
	tbl = NewEmptyTableEntry(db)
	tbl.BaseEntry = entry.Table.BaseEntry
	tbl.Schema = entry.Table.Schema
	catalog.TryUpdateTableId(tbl.Id)
	return db.onReplayNewTable(tbl)
}

func (catalog *Catalog) onReplayCreateSegment(entry *segmentLogEntry) error {
	db := catalog.Databases[entry.DatabaseId]
	tbl := db.TableSet[entry.TableId]
	seg := newCommittedSegmentEntry(tbl, entry.BaseEntry)
	tbl.onNewSegment(seg)
	return nil
}

func (catalog *Catalog) onReplayUpgradeSegment(entry *segmentLogEntry) error {
	db := catalog.Databases[entry.DatabaseId]
	tbl := db.TableSet[entry.TableId]
	pos := tbl.IdIndex[entry.Id]
	seg := tbl.SegmentSet[pos]
	return seg.onCommit(entry.CommitInfo)
}

func (catalog *Catalog) onReplaySegmentCheckpoint(entry *segmentLogEntry) error {
	db := catalog.Databases[entry.DatabaseId]
	tbl := db.TableSet[entry.TableId]
	pos, ok := tbl.IdIndex[entry.Id]
	if ok {
		seg := tbl.SegmentSet[pos]
		return seg.onCommit(entry.CommitInfo)
	}
	seg := newCommittedSegmentEntry(tbl, entry.BaseEntry)
	catalog.TryUpdateSegmentId(seg.Id)
	tbl.onNewSegment(seg)
	return nil
}

func (catalog *Catalog) onReplayCreateBlock(entry *blockLogEntry) error {
	db := catalog.Databases[entry.DatabaseId]
	tbl := db.TableSet[entry.TableId]
	segpos := tbl.IdIndex[entry.SegmentId]
	seg := tbl.SegmentSet[segpos]
	blk := newCommittedBlockEntry(seg, entry.BaseEntry)
	seg.onNewBlock(blk)
	return nil
}

func (catalog *Catalog) onReplayUpgradeBlock(entry *blockLogEntry) error {
	db := catalog.Databases[entry.DatabaseId]
	tbl := db.TableSet[entry.TableId]
	segpos := tbl.IdIndex[entry.SegmentId]
	seg := tbl.SegmentSet[segpos]
	blkpos := seg.IdIndex[entry.Id]
	blk := seg.BlockSet[blkpos]
	blk.IndiceMemo = nil
	return blk.onCommit(entry.CommitInfo)
}

func (catalog *Catalog) onReplayBlockCheckpoint(entry *blockLogEntry) error {
	db := catalog.Databases[entry.DatabaseId]
	tbl := db.TableSet[entry.TableId]
	segpos := tbl.IdIndex[entry.SegmentId]
	seg := tbl.SegmentSet[segpos]
	blkpos, ok := seg.IdIndex[entry.Id]
	if ok {
		blk := seg.BlockSet[blkpos]
		blk.IndiceMemo = nil
		return blk.onCommit(entry.CommitInfo)
	}
	blk := newCommittedBlockEntry(seg, entry.BaseEntry)
	catalog.TryUpdateBlockId(blk.Id)
	seg.onNewBlock(blk)
	return nil
}

func (catalog *Catalog) onReplayCreateDatabase(entry *Database) error {
	db := NewEmptyDatabase(catalog)
	db.BaseEntry = entry.BaseEntry
	db.Name = entry.Name
	db.ShardWal = wal.NewWalShard(db.BaseEntry.GetShardId(), catalog.IndexWal)
	catalog.TryUpdateDatabaseId(db.Id)
	return catalog.onNewDatabase(db)
}

func (catalog *Catalog) onReplaySoftDeleteDatabase(entry *databaseLogEntry) error {
	db := catalog.Databases[entry.BaseEntry.Id]
	return db.onCommit(entry.CommitInfo)
}

func (catalog *Catalog) onReplayHardDeleteDatabase(entry *databaseLogEntry) error {
	db := catalog.Databases[entry.BaseEntry.Id]
	return db.onCommit(entry.CommitInfo)
}

func (catalog *Catalog) onReplayDatabaseCheckpoint(entry *databaseLogEntry) error {
	db, ok := catalog.Databases[entry.BaseEntry.Id]
	if ok {
		return db.onCommit(entry.CommitInfo)
	}
	catalog.TryUpdateDatabaseId(entry.BaseEntry.Id)
	db = NewEmptyDatabase(catalog)
	db.Name = entry.Database.Name
	db.BaseEntry.Id = entry.BaseEntry.Id
	db.CommitInfo.TranId = entry.CommitInfo.TranId
	db.CommitInfo.LogIndex = entry.CommitInfo.LogIndex
	db.BaseEntry = entry.BaseEntry
	db.Name = entry.Database.Name
	db.ShardWal = wal.NewWalShard(
		db.BaseEntry.GetShardId(), catalog.IndexWal)
	catalog.TryUpdateDatabaseId(db.Id)
	err := catalog.onReplayNewDatabase(db)
	if err != nil {
		panic(ErrReplayFailed)
	}
	return nil
}

func (catalog *Catalog) onReplayReplaceDatabase(entry *dbReplaceLogEntry, isSplit bool) error {
	var err error
	if entry.Replaced != nil {
		replaced := catalog.Databases[entry.Replaced.Id]
		err := replaced.onCommit(entry.Replaced.CommitInfo)
		if err != nil {
			return err
		}
	}
	idx := entry.Index
	for _, replacer := range entry.Replacer {
		catalog.TryUpdateDatabaseId(replacer.Id)
		if err = catalog.onNewDatabase(replacer); err != nil {
			break
		}
		replacer.Catalog = catalog
		if err = replacer.rebuild(false, true); err != nil {
			break
		}
		err := replacer.InitWal(idx)
		if err != nil {
			return err
		}
	}
	return err
}

func MockCatalogAndWal(dir string, blkRows, segBlks uint64) (*Catalog, Wal) {
	driver, _ := logstore.NewBatchStore(dir, "driver", nil)
	indexWal := shard.NewManagerWithDriver(driver, false, wal.BrokerRole)
	return MockCatalog(dir, blkRows, segBlks, driver, indexWal), indexWal
}

func MockCatalog(dir string, blkRows, segBlks uint64, driver logstore.AwareStore, indexWal wal.ShardAwareWal) *Catalog {
	cfg := new(CatalogCfg)
	cfg.Dir = dir
	cfg.BlockMaxRows = blkRows
	cfg.SegmentMaxBlocks = segBlks
	var (
		catalog *Catalog
		err     error
	)
	if driver != nil {
		catalog, err = OpenCatalogWithDriver(new(sync.RWMutex), cfg, driver, indexWal)
	} else {
		catalog, err = OpenCatalog(new(sync.RWMutex), cfg)
	}
	if err != nil {
		panic(err)
	}
	catalog.Start()
	catalog.Compact(nil, nil)
	return catalog
}
