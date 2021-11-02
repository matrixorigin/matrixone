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
	"sort"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"

	"github.com/google/btree"
)

var (
	DefaultCheckpointDelta = uint64(10000)
)

var (
	DuplicateErr       = errors.New("aoe: duplicate")
	ShardNotFoundErr   = errors.New("aoe: shard not found")
	TableNotFoundErr   = errors.New("aoe: table not found")
	SegmentNotFoundErr = errors.New("aoe: segment not found")
	BlockNotFoundErr   = errors.New("aoe: block not found")
	InvalidSchemaErr   = errors.New("aoe: invalid schema")
)

type CatalogCfg struct {
	Dir                 string `json:"-"`
	BlockMaxRows        uint64 `toml:"block-max-rows"`
	SegmentMaxBlocks    uint64 `toml:"segment-max-blocks"`
	RotationFileMaxSize int    `toml:"rotation-file-max-size"`
}

type catalogLogEntry struct {
	Range    *common.Range `json:"range"`
	Catalog  *Catalog      `json:"catalog"`
	LogRange *LogRange     `json:"logrange"`
}

func newCatalogLogEntry(id uint64) *catalogLogEntry {
	e := &catalogLogEntry{
		Catalog: &Catalog{
			RWMutex:  &sync.RWMutex{},
			TableSet: make(map[uint64]*Table),
		},
		Range: new(common.Range),
	}
	e.Range.Right = id
	return e
}

func newShardSnapshotLogEntry(shardId, index uint64) *catalogLogEntry {
	logRange := new(LogRange)
	logRange.ShardId = shardId
	logRange.Range.Right = index
	e := &catalogLogEntry{
		Catalog: &Catalog{
			RWMutex:  &sync.RWMutex{},
			TableSet: make(map[uint64]*Table),
		},
		LogRange: logRange,
	}
	return e
}

func (e *catalogLogEntry) CheckpointId() uint64 {
	return e.Range.Right
}

func (e *catalogLogEntry) Marshal() ([]byte, error) {
	buf, err := json.Marshal(e)
	return buf, err
}

func (e *catalogLogEntry) Unmarshal(buf []byte) error {
	if err := json.Unmarshal(buf, e); err != nil {
		return err
	}
	e.Catalog.RWMutex = new(sync.RWMutex)
	return nil
}

func (e *catalogLogEntry) ToLogEntry(eType LogEntryType) LogEntry {
	switch eType {
	case logstore.ETCheckpoint:
	case ETShardSnapshot:
		break
	default:
		panic("not supported")
	}
	buf, _ := e.Marshal()
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	logEntry.SetAuxilaryInfo(&e.Range)
	return logEntry
}

type Catalog struct {
	sm.ClosedState  `json:"-"`
	sm.StateMachine `json:"-"`
	*sync.RWMutex   `json:"-"`
	Sequence        `json:"-"`
	pipeline        *commitPipeline        `json:"-"`
	Store           logstore.AwareStore    `json:"-"`
	IndexWal        wal.ShardWal           `json:"-"`
	Cfg             *CatalogCfg            `json:"-"`
	nameIndex       *btree.BTree           `json:"-"`
	nodesMu         sync.RWMutex           `json:"-"`
	commitMu        sync.RWMutex           `json:"-"`
	nameNodes       map[string]*tableNode  `json:"-"`
	shardMu         sync.RWMutex           `json:"-"`
	shardsStats     map[uint64]*shardStats `json:"-"`

	tableListener   TableListener   `json:"-"`
	segmentListener SegmentListener `json:"-"`
	blockListener   BlockListener   `json:"-"`

	TableSet map[uint64]*Table `json:"tables"`
}

func OpenCatalog(mu *sync.RWMutex, cfg *CatalogCfg) (*Catalog, error) {
	replayer := newCatalogReplayer()
	return replayer.RebuildCatalog(mu, cfg)
}

func OpenCatalogWithDriver(mu *sync.RWMutex, cfg *CatalogCfg, store logstore.AwareStore, indexWal wal.ShardWal) (*Catalog, error) {
	replayer := newCatalogReplayer()
	return replayer.RebuildCatalogWithDriver(mu, cfg, store, indexWal)
}

func NewCatalogWithDriver(mu *sync.RWMutex, cfg *CatalogCfg, store logstore.AwareStore, indexWal wal.ShardWal) *Catalog {
	catalog := &Catalog{
		RWMutex:     mu,
		Cfg:         cfg,
		TableSet:    make(map[uint64]*Table),
		nameNodes:   make(map[string]*tableNode),
		nameIndex:   btree.New(2),
		Store:       store,
		IndexWal:    indexWal,
		shardsStats: make(map[uint64]*shardStats),
	}
	blockListener := new(BaseBlockListener)
	blockListener.BlockUpgradedFn = catalog.onBlockUpgraded
	segmentListener := new(BaseSegmentListener)
	segmentListener.SegmentUpgradedFn = catalog.onSegmentUpgraded
	tableListener := new(BaseTableListener)
	tableListener.HardDeletedFn = catalog.onTableHardDelete
	catalog.tableListener = tableListener
	catalog.blockListener = blockListener
	catalog.segmentListener = segmentListener

	wg := new(sync.WaitGroup)
	rQueue := sm.NewSafeQueue(100000, 100, nil)
	ckpQueue := sm.NewSafeQueue(100000, 10, catalog.onCheckpoint)
	catalog.StateMachine = sm.NewStateMachine(wg, catalog, rQueue, ckpQueue)
	catalog.pipeline = newCommitPipeline(catalog)
	return catalog
}

func NewCatalog(mu *sync.RWMutex, cfg *CatalogCfg) *Catalog {
	if cfg.RotationFileMaxSize <= 0 {
		logutil.Warnf("Set rotation max size to default size: %s", common.ToH(uint64(logstore.DefaultVersionFileSize)))
		cfg.RotationFileMaxSize = logstore.DefaultVersionFileSize
	}
	catalog := &Catalog{
		RWMutex:     mu,
		Cfg:         cfg,
		TableSet:    make(map[uint64]*Table),
		nameNodes:   make(map[string]*tableNode),
		nameIndex:   btree.New(2),
		shardsStats: make(map[uint64]*shardStats),
	}
	blockListener := new(BaseBlockListener)
	blockListener.BlockUpgradedFn = catalog.onBlockUpgraded
	segmentListener := new(BaseSegmentListener)
	segmentListener.SegmentUpgradedFn = catalog.onSegmentUpgraded
	tableListener := new(BaseTableListener)
	tableListener.HardDeletedFn = catalog.onTableHardDelete
	catalog.tableListener = tableListener
	catalog.blockListener = blockListener
	catalog.segmentListener = segmentListener

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
	return catalog
}

func (catalog *Catalog) Start() {
	catalog.Store.Start()
	catalog.StateMachine.Start()
}

func (catalog *Catalog) rebuild(tables map[uint64]*Table, r *common.Range) error {
	catalog.Sequence.nextCommitId = r.Right
	sorted := make([]*Table, len(tables))
	idx := 0
	for _, table := range tables {
		sorted[idx] = table
		idx++
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Id < sorted[j].Id
	})
	for _, table := range sorted {
		table.rebuild(catalog)
		if err := catalog.onNewTable(table); err != nil {
			return err
		}
	}
	if len(sorted) > 0 {
		catalog.Sequence.nextTableId = sorted[len(sorted)-1].Id
	}
	return nil
}

func (catalog *Catalog) onTableHardDelete(table *Table) {
	catalog.UpdateShardStats(table.GetShardId(), -table.GetCoarseSize(), -table.GetCoarseCount())
}

func (catalog *Catalog) onBlockUpgraded(block *Block) {
	catalog.UpdateShardStats(block.GetShardId(), block.GetCoarseSize(), block.GetCoarseCount())
}

func (catalog *Catalog) onSegmentUpgraded(segment *Segment, prev *CommitInfo) {
	catalog.UpdateShardStats(segment.GetShardId(), segment.GetCoarseSize()-prev.GetSize(), 0)
}

func (catalog *Catalog) UpdateShardStats(shardId uint64, size int64, count int64) {
	catalog.shardMu.RLock()
	stats := catalog.shardsStats[shardId]
	catalog.shardMu.RUnlock()
	stats.addCount(count)
	stats.addSize(size)
	// logutil.Infof("%s", stats.String())
}

func (catalog *Catalog) SimpleGetTableAppliedIdByName(name string) (uint64, bool) {
	table := catalog.SimpleGetTableByName(name)
	if table == nil {
		return uint64(0), false
	}
	return table.GetAppliedIndex(nil)
}

func (catalog *Catalog) SimpleGetTablesByPrefix(prefix string) (tbls []*Table) {
	catalog.RLock()
	upperBound := []byte(prefix)
	upperBound = append(upperBound, byte(255))
	lowerN := newTableNode(catalog, prefix)
	upperN := newTableNode(catalog, string(upperBound))

	catalog.nameIndex.AscendRange(
		lowerN, upperN,
		func(item btree.Item) bool {
			t := item.(*tableNode).GetEntry()
			if t.IsDeleted() {
				return false
			}
			tbls = append(tbls, t)
			return true
		})
	catalog.RUnlock()
	return tbls
}

func (catalog *Catalog) ShardView(shardId, index uint64) *catalogLogEntry {
	filter := new(Filter)
	subFilter := newCommitFilter()
	subFilter.AddChecker(createShardChecker(shardId))
	subFilter.AddChecker(createIndexRangeChecker(index))
	filter.blockFilter = subFilter
	filter.segmentFilter = subFilter
	filter.tableFilter = newCommitFilter()
	filter.tableFilter.AddChecker(createShardChecker(shardId))
	filter.tableFilter.AddChecker(createIndexChecker(index))
	stopper := createDeleteAndIndexStopper(index)
	filter.tableFilter.AddStopper(stopper)
	view := newShardSnapshotLogEntry(shardId, index)
	catalog.fillView(filter, view.Catalog)
	return view
}

func (catalog *Catalog) LatestView() *catalogLogEntry {
	commitId := catalog.Store.GetSyncedId()
	filter := new(Filter)
	filter.tableFilter = newCommitFilter()
	filter.tableFilter.AddChecker(createCommitIdChecker(commitId))
	filter.segmentFilter = filter.tableFilter
	filter.blockFilter = filter.tableFilter
	view := newCatalogLogEntry(commitId)
	catalog.fillView(filter, view.Catalog)
	return view
}

func (catalog *Catalog) fillView(filter *Filter, view *Catalog) {
	entries := make(map[uint64]*Table)
	catalog.RLock()
	for id, entry := range catalog.TableSet {
		entries[id] = entry
	}
	catalog.RUnlock()
	for eid, entry := range entries {
		committed := entry.fillView(filter)
		if committed != nil {
			view.TableSet[eid] = committed
		}
	}
}

func (catalog *Catalog) RecurLoop(processor LoopProcessor) error {
	var err error
	for _, table := range catalog.TableSet {
		if err = processor.OnTable(table); err != nil {
			return err
		}
		table.RLock()
		defer table.RUnlock()
		if err = table.RecurLoopLocked(processor); err != nil {
			return err
		}
	}
	return err
}

func (catalog *Catalog) Compact() {
	tables := make([]*Table, 0, 2)
	nodes := make([]*tableNode, 0, 2)
	catalog.RLock()
	for _, table := range catalog.TableSet {
		if table.IsHardDeleted() {
			tables = append(tables, table)
			nodes = append(nodes, catalog.nameNodes[table.Schema.Name])
		}
	}
	catalog.RUnlock()
	if len(tables) == 0 {
		return
	}

	names := make([]string, 0)
	for i, table := range tables {
		node := nodes[i]
		_, empty := node.DeleteNode(table.Id)
		if empty {
			names = append(names, node.name)
		}
	}
	catalog.Lock()
	for _, table := range tables {
		delete(catalog.TableSet, table.Id)
	}
	catalog.Unlock()
	if len(names) > 0 {
		catalog.TryDeleteEmptyNameNodes(names...)
	}
}

func (catalog *Catalog) TryDeleteEmptyNameNodes(names ...string) (deleted []*tableNode) {
	catalog.Lock()
	defer catalog.Unlock()
	for _, name := range names {
		node := catalog.nameNodes[name]
		if node == nil {
			continue
		}
		if node.Length() != 0 {
			continue
		}
		delete(catalog.nameNodes, name)
		catalog.nameIndex.Delete(node)
	}
	return
}

func (catalog *Catalog) Close() error {
	catalog.Stop()
	catalog.Store.Close()
	logutil.Infof("[AOE] Safe synced id %d", catalog.GetSafeCommitId())
	logutil.Infof("[AOE] Safe checkpointed id %d", catalog.GetCheckpointId())
	return nil
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

func (catalog *Catalog) SimpleGetTableIds() []uint64 {
	catalog.RLock()
	defer catalog.RUnlock()
	ids := make([]uint64, len(catalog.TableSet))
	pos := 0
	for _, tbl := range catalog.TableSet {
		ids[pos] = tbl.Id
		pos++
	}
	return ids
}

func (catalog *Catalog) SimpleGetTableNames() []string {
	catalog.RLock()
	defer catalog.RUnlock()
	names := make([]string, len(catalog.TableSet))
	pos := 0
	for _, tbl := range catalog.TableSet {
		names[pos] = tbl.Schema.Name
		pos++
	}
	return names
}

func (catalog *Catalog) HardDeleteTable(id uint64) error {
	catalog.Lock()
	table := catalog.TableSet[id]
	if table == nil {
		catalog.Unlock()
		return TableNotFoundErr
	}
	catalog.Unlock()

	return table.HardDelete()
}

func (catalog *Catalog) SimpleDropTableByName(name string, exIndex *LogIndex) error {
	ctx := newDropTableCtx(name, exIndex)
	err := catalog.prepareDropTable(ctx)
	if err != nil {
		return err
	}
	return catalog.onCommitRequest(ctx)
}

func (catalog *Catalog) prepareDropTable(ctx *dropTableCtx) error {
	catalog.Lock()
	nn := catalog.nameNodes[ctx.name]
	if nn == nil {
		catalog.Unlock()
		return TableNotFoundErr
	}
	table := nn.GetEntry()
	catalog.Unlock()
	table.RLock()
	defer table.RUnlock()
	if table.IsDeletedLocked() {
		return TableNotFoundErr
	}
	ctx.table = table
	return nil
}

func (catalog *Catalog) SimpleCreateTable(schema *Schema, exIndex *LogIndex) (*Table, error) {
	if !schema.Valid() {
		return nil, InvalidSchemaErr
	}
	ctx := newCreateTableCtx(schema, exIndex)
	err := catalog.onCommitRequest(ctx)
	return ctx.table, err
}

func (catalog *Catalog) prepareCreateTable(ctx *createTableCtx) (LogEntry, error) {
	var err error
	entry := NewTableEntry(catalog, ctx.schema, catalog.NextUncommitId(), ctx.exIndex)
	logEntry := entry.ToLogEntry(ETCreateTable)
	catalog.commitMu.Lock()
	defer catalog.commitMu.Unlock()
	catalog.Lock()
	if err = catalog.onNewTable(entry); err != nil {
		catalog.Unlock()
		return nil, err
	}
	catalog.Unlock()
	ctx.table = entry
	if ctx.inTran {
		return nil, nil
	}
	catalog.prepareCommitLog(entry, logEntry)
	return logEntry, err
}

func (catalog *Catalog) prepareAddTable(ctx *addTableCtx) (LogEntry, error) {
	var err error
	catalog.Lock()
	if err = catalog.onNewTable(ctx.table); err != nil {
		catalog.Unlock()
		return nil, err
	}
	catalog.Unlock()
	if ctx.inTran {
		return nil, nil
	}
	panic("todo")
}

func (catalog *Catalog) SimpleReplaceShard(view *catalogLogEntry) error {
	ctx := newReplaceShardCtx(view)
	err := catalog.onCommitRequest(ctx)
	return err
}

func (catalog *Catalog) prepareReplaceShard(ctx *replaceShardCtx) (LogEntry, error) {
	var err error
	entry := newShardLogEntry()
	catalog.RLock()
	logIndex := new(LogIndex)
	logIndex.Id = shard.SimpleIndexId(ctx.view.LogRange.Range.Right)
	logIndex.ShardId = ctx.view.LogRange.ShardId
	for _, table := range catalog.TableSet {
		if table.GetShardId() != ctx.view.LogRange.ShardId {
			continue
		}
		rCtx := newReplaceTableCtx(table, logIndex, true)
		if _, err = table.prepareReplace(rCtx); err != nil {
			panic(err)
		}
		if !rCtx.discard {
			entry.addReplaced(table)
		}
	}
	catalog.RUnlock()
	for _, table := range ctx.view.Catalog.TableSet {
		nCtx := newAddTableCtx(table, true)
		if _, err = catalog.prepareAddTable(nCtx); err != nil {
			panic(err)
		}
		entry.addReplacer(table)
	}
	logEntry := catalog.prepareCommitEntry(entry, ETShardSnapshot, nil)
	return logEntry, err
}

func (catalog *Catalog) onCommitRequest(ctx interface{}) error {
	entry, err := catalog.pipeline.prepare(ctx)
	if err != nil {
		return err
	}
	err = catalog.pipeline.commit(entry)
	return err
}

func (catalog *Catalog) prepareCommitLog(entry IEntry, logEntry LogEntry) {
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

func (catalog *Catalog) SimpleGetTableByName(name string) *Table {
	catalog.RLock()
	defer catalog.RUnlock()
	return catalog.GetTableByName(name, MinUncommitId)
}

func (catalog *Catalog) GetTableByName(name string, tranId uint64) *Table {
	nn := catalog.nameNodes[name]
	if nn == nil {
		return nil
	}

	next := nn.GetNext()
	for next != nil {
		entry := next.(*nameNode).GetEntry()
		if entry.CanUse(tranId) && !entry.IsDeleted() {
			return entry
		}
		next = next.GetNext()
	}
	return nil
}

func (catalog *Catalog) SimpleGetTable(id uint64) *Table {
	catalog.RLock()
	defer catalog.RUnlock()
	return catalog.GetTable(id)
}

func (catalog *Catalog) GetTable(id uint64) *Table {
	return catalog.TableSet[id]
}

func (catalog *Catalog) ForLoopTables(h func(*Table) error) error {
	tables := make([]*Table, 0, 10)
	catalog.RLock()
	for _, tbl := range catalog.TableSet {
		tables = append(tables, tbl)
	}
	catalog.RUnlock()
	for _, tbl := range tables {
		if err := h(tbl); err != nil {
			return err
		}
	}
	return nil
}

func (catalog *Catalog) SimpleGetSegment(tableId, segmentId uint64) (*Segment, error) {
	table := catalog.SimpleGetTable(tableId)
	if table == nil {
		return nil, TableNotFoundErr
	}
	segment := table.SimpleGetSegment(segmentId)
	if segment == nil {
		return nil, SegmentNotFoundErr
	}
	return segment, nil
}

func (catalog *Catalog) SimpleGetBlock(tableId, segmentId, blockId uint64) (*Block, error) {
	segment, err := catalog.SimpleGetSegment(tableId, segmentId)
	if err != nil {
		return nil, err
	}
	block := segment.SimpleGetBlock(blockId)
	if block == nil {
		return nil, BlockNotFoundErr
	}
	return block, nil
}

func (catalog *Catalog) onCheckpoint(items ...interface{}) {
	if err := catalog.doCheckPoint(); err != nil {
		panic(err)
	}
	for _, item := range items {
		item.(logstore.AsyncEntry).DoneWithErr(nil)
	}
}

func (catalog *Catalog) Checkpoint() (logstore.AsyncEntry, error) {
	entry := logstore.NewAsyncBaseEntry()
	ret, err := catalog.EnqueueCheckpoint(entry)
	if err != nil {
		return nil, err
	}
	return ret.(logstore.AsyncEntry), nil
}

func (catalog *Catalog) doCheckPoint() error {
	var err error
	view := catalog.LatestView()
	entry := view.ToLogEntry(logstore.ETCheckpoint)
	if err = catalog.Store.Checkpoint(entry); err != nil {
		return err
	}
	err = entry.WaitDone()
	entry.Free()
	return err
}

func (catalog *Catalog) PString(level PPLevel) string {
	catalog.RLock()
	s := fmt.Sprintf("<Catalog>(Cnt=%d){", len(catalog.TableSet))
	for _, table := range catalog.TableSet {
		s = fmt.Sprintf("%s\n%s", s, table.PString(level))
	}
	if len(catalog.TableSet) == 0 {
		s = fmt.Sprintf("%s}", s)
	} else {
		s = fmt.Sprintf("%s\n}", s)
	}
	catalog.RUnlock()
	return s
}

func (catalog *Catalog) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, catalog)
}

func (catalog *Catalog) Marshal() ([]byte, error) {
	return json.Marshal(catalog)
}

func (catalog *Catalog) CommitLocked(uint64) {}

func (catalog *Catalog) ToLogEntry(eType LogEntryType) LogEntry {
	switch eType {
	case logstore.ETCheckpoint:
		break
	default:
		panic("not supported")
	}
	buf, _ := catalog.Marshal()
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}

func (catalog *Catalog) tryCreateShardStats(shardId uint64) {
	catalog.shardMu.RLock()
	stats := catalog.shardsStats[shardId]
	if stats != nil {
		catalog.shardMu.RUnlock()
		return
	}
	catalog.shardMu.RUnlock()
	catalog.shardMu.Lock()
	defer catalog.shardMu.Unlock()
	stats = catalog.shardsStats[shardId]
	if stats != nil {
		return
	}
	catalog.shardsStats[shardId] = newShardStats(shardId)
}

func (catalog *Catalog) CleanupShard(shardId uint64) {
	catalog.shardMu.Lock()
	defer catalog.shardMu.Unlock()
	delete(catalog.shardsStats, shardId)
}

// func (catalog *Catalog) SplitCheck(size uint64, shardId uint64) (coarseSize uint64, coarseCount uint64, keys [][]byte, specs []byte, err error) {
// 	catalog.shardMu.RLock()
// 	stats := catalog.shardsStats[shardId]
// 	catalog.shardMu.RUnlock()
// 	coarseSize, coarseCount = uint64(stats.getSize()), stats.getCount()
// 	if coarseSize < size {
// 		return
// 	}
// 	parts = make([][]byte, coarseSize/size+1)

// 	return
// }

func (catalog *Catalog) onNewTable(entry *Table) error {
	shardId := entry.GetShardId()
	catalog.tryCreateShardStats(shardId)
	nn := catalog.nameNodes[entry.Schema.Name]
	if nn != nil {
		e := nn.GetEntry()
		if !e.IsDeletedLocked() {
			return DuplicateErr
		}
		catalog.TableSet[entry.Id] = entry
		nn.CreateNode(entry.Id)
	} else {
		catalog.TableSet[entry.Id] = entry

		nn := newTableNode(catalog, entry.Schema.Name)
		catalog.nameNodes[entry.Schema.Name] = nn
		catalog.nameIndex.ReplaceOrInsert(nn)

		nn.CreateNode(entry.Id)
	}
	return nil
}

func (catalog *Catalog) onReplayCreateTable(entry *Table) error {
	tbl := NewEmptyTableEntry(catalog)
	tbl.BaseEntry = entry.BaseEntry
	tbl.Schema = entry.Schema
	return catalog.onNewTable(tbl)
}

func (catalog *Catalog) onReplaySoftDeleteTable(entry *tableLogEntry) error {
	tbl := catalog.TableSet[entry.Id]
	tbl.onNewCommit(entry.CommitInfo)
	return nil
}

func (catalog *Catalog) onReplayHardDeleteTable(entry *tableLogEntry) error {
	tbl := catalog.TableSet[entry.Id]
	tbl.onNewCommit(entry.CommitInfo)
	return nil
}

func (catalog *Catalog) onReplayCreateSegment(entry *segmentLogEntry) error {
	tbl := catalog.TableSet[entry.TableId]
	seg := newCommittedSegmentEntry(catalog, tbl, entry.BaseEntry)
	tbl.onNewSegment(seg)
	return nil
}

func (catalog *Catalog) onReplayUpgradeSegment(entry *segmentLogEntry) error {
	tbl := catalog.TableSet[entry.TableId]
	pos := tbl.IdIndex[entry.Id]
	seg := tbl.SegmentSet[pos]
	seg.onNewCommit(entry.CommitInfo)
	return nil
}

func (catalog *Catalog) onReplayCreateBlock(entry *blockLogEntry) error {
	tbl := catalog.TableSet[entry.TableId]
	segpos := tbl.IdIndex[entry.SegmentId]
	seg := tbl.SegmentSet[segpos]
	blk := newCommittedBlockEntry(seg, entry.BaseEntry)
	seg.onNewBlock(blk)
	return nil
}

func (catalog *Catalog) onReplayUpgradeBlock(entry *blockLogEntry) error {
	tbl := catalog.TableSet[entry.TableId]
	segpos := tbl.IdIndex[entry.SegmentId]
	seg := tbl.SegmentSet[segpos]
	blkpos := seg.IdIndex[entry.Id]
	blk := seg.BlockSet[blkpos]
	blk.IndiceMemo = nil
	blk.onNewCommit(entry.CommitInfo)
	return nil
}

func (catalog *Catalog) onReplayShardSnapshot(entry *shardLogEntry) error {
	for _, replaced := range entry.Replaced {
		table := catalog.TableSet[replaced.Id]
		table.onNewCommit(replaced.CommitInfo)
	}
	for _, replacer := range entry.Replacer {
		catalog.onNewTable(replacer)
	}
	return nil
}

func MockCatalog(dir string, blkRows, segBlks uint64) *Catalog {
	cfg := new(CatalogCfg)
	cfg.Dir = dir
	cfg.BlockMaxRows = blkRows
	cfg.SegmentMaxBlocks = segBlks
	catalog, err := OpenCatalog(new(sync.RWMutex), cfg)
	if err != nil {
		panic(err)
	}
	catalog.Start()
	return catalog
}
