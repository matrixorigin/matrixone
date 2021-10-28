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

	"github.com/google/btree"
)

var (
	DefaultCheckpointDelta = uint64(10000)
)

var (
	DuplicateErr       = errors.New("aoe: duplicate")
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
	Range   common.Range
	Catalog *Catalog
}

func newCatalogLogEntry(id uint64) *catalogLogEntry {
	e := &catalogLogEntry{
		Catalog: &Catalog{
			RWMutex:  &sync.RWMutex{},
			TableSet: make(map[uint64]*Table),
		},
	}
	e.Range.Right = id
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
	return json.Unmarshal(buf, e)
}

func (e *catalogLogEntry) ToLogEntry(eType LogEntryType) LogEntry {
	switch eType {
	case logstore.ETCheckpoint:
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
	pipeline        *commitPipeline       `json:"-"`
	Store           logstore.AwareStore   `json:"-"`
	IndexWal        wal.ShardWal          `json:"-"`
	Cfg             *CatalogCfg           `json:"-"`
	nameIndex       *btree.BTree          `json:"-"`
	nodesMu         sync.RWMutex          `json:"-"`
	commitMu        sync.RWMutex          `json:"-"`
	nameNodes       map[string]*tableNode `json:"-"`
	shardNodes      map[uint64]*shardNode `json:"-"`

	TableSet map[uint64]*Table
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
		RWMutex:    mu,
		Cfg:        cfg,
		TableSet:   make(map[uint64]*Table),
		nameNodes:  make(map[string]*tableNode),
		shardNodes: make(map[uint64]*shardNode),
		nameIndex:  btree.New(2),
		Store:      store,
		IndexWal:   indexWal,
	}
	wg := new(sync.WaitGroup)
	rQueue := sm.NewSafeQueue(100000, 100, nil)
	ckpQueue := sm.NewSafeQueue(100000, 10, catalog.onCheckpoint)
	// rQueue := sm.NewWaitableQueue(100000, 100, catalog, wg, nil, nil, nil)
	// ckpQueue := sm.NewWaitableQueue(100000, 10, catalog, wg, nil, nil, catalog.onCheckpoint)
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
		RWMutex:    mu,
		Cfg:        cfg,
		TableSet:   make(map[uint64]*Table),
		nameNodes:  make(map[string]*tableNode),
		shardNodes: make(map[uint64]*shardNode),
		nameIndex:  btree.New(2),
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
	// rQueue := sm.NewWaitableQueue(100000, 100, catalog, wg, nil, nil, nil)
	// ckpQueue := sm.NewWaitableQueue(100000, 10, catalog, wg, nil, nil, catalog.onCheckpoint)
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

func (catalog *Catalog) LatestView() *catalogLogEntry {
	commitId := catalog.Store.GetSyncedId()
	return catalog.CommittedView(commitId)
}

func (catalog *Catalog) CommittedView(id uint64) *catalogLogEntry {
	ss := newCatalogLogEntry(id)
	entries := make(map[uint64]*Table)
	catalog.RLock()
	for id, entry := range catalog.TableSet {
		entries[id] = entry
	}
	catalog.RUnlock()
	for eid, entry := range entries {
		committed := entry.CommittedView(id)
		if committed != nil {
			ss.Catalog.TableSet[eid] = committed
		}
	}
	return ss
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
	catalog.prepareCommitLog(entry, logEntry)
	ctx.table = entry
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
	if level >= PPL1 {
		for _, node := range catalog.shardNodes {
			s = fmt.Sprintf("%s\n%s", s, node.PString(level))
		}
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

func (catalog *Catalog) addIntoShard(table *Table) {
	shardId := table.GetShardId()
	n := catalog.shardNodes[shardId]
	if n == nil {
		n = newShardNode(catalog, shardId)
		n.CreateNode()
		catalog.shardNodes[shardId] = n
	}
	n.GetGroup().Add(table.Id)
}

func (catalog *Catalog) onNewTable(entry *Table) error {
	nn := catalog.nameNodes[entry.Schema.Name]
	if nn != nil {
		e := nn.GetEntry()
		if !e.IsSoftDeletedLocked() {
			return DuplicateErr
		}
		catalog.TableSet[entry.Id] = entry
		nn.CreateNode(entry.Id)
		catalog.addIntoShard(entry)
	} else {
		catalog.TableSet[entry.Id] = entry

		nn := newTableNode(catalog, entry.Schema.Name)
		catalog.nameNodes[entry.Schema.Name] = nn
		catalog.nameIndex.ReplaceOrInsert(nn)

		nn.CreateNode(entry.Id)
		catalog.addIntoShard(entry)
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
