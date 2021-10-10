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
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/logstore"
	worker "matrixone/pkg/vm/engine/aoe/storage/worker"
	wb "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"sort"
	"sync"

	"github.com/google/btree"
)

type SyncerCfg = logstore.SyncerCfg

var (
	DuplicateErr       = errors.New("aoe: duplicate")
	TableNotFoundErr   = errors.New("aoe: table not found")
	SegmentNotFoundErr = errors.New("aoe: segment not found")
	BlockNotFoundErr   = errors.New("aoe: block not found")
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
	logEntry := logstore.GetEmptyEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	logEntry.SetAuxilaryInfo(&e.Range)
	return logEntry
}

type Catalog struct {
	*sync.RWMutex `json:"-"`
	Sequence      `json:"-"`
	Store         logstore.AwareStore   `json:"-"`
	Cfg           *CatalogCfg           `json:"-"`
	NameNodes     map[string]*tableNode `json:"-"`
	NameIndex     *btree.BTree          `json:"-"`
	nodesMu       sync.RWMutex          `json:"-"`
	archiver      wb.IOpWorker          `json:"-"`

	TableSet map[uint64]*Table
}

func OpenCatalog(mu *sync.RWMutex, cfg *CatalogCfg, syncerCfg *SyncerCfg) (*Catalog, error) {
	replayer := newCatalogReplayer()
	return replayer.RebuildCatalog(mu, cfg, syncerCfg)
}

func NewCatalogWithBatchStore(mu *sync.RWMutex, cfg *CatalogCfg) *Catalog {
	if cfg.RotationFileMaxSize <= 0 {
		logutil.Warnf("Set rotation max size to default size: %d", logstore.DefaultVersionFileSize)
		cfg.RotationFileMaxSize = logstore.DefaultVersionFileSize
	}
	catalog := &Catalog{
		RWMutex:   mu,
		Cfg:       cfg,
		TableSet:  make(map[uint64]*Table),
		NameNodes: make(map[string]*tableNode),
		NameIndex: btree.New(2),
	}
	rotationCfg := &logstore.RotationCfg{}
	rotationCfg.RotateChecker = &logstore.MaxSizeRotationChecker{
		MaxSize: cfg.RotationFileMaxSize,
	}
	store, err := logstore.NewBatchStore(cfg.Dir, "bstore", rotationCfg)
	if err != nil {
		panic(err)
	}
	catalog.Store = store
	return catalog
}

func NewCatalog(mu *sync.RWMutex, cfg *CatalogCfg, syncerCfg *SyncerCfg) *Catalog {
	if cfg.RotationFileMaxSize <= 0 {
		logutil.Warnf("Set rotation max size to default size: %d", logstore.DefaultVersionFileSize)
		cfg.RotationFileMaxSize = logstore.DefaultVersionFileSize
	}
	catalog := &Catalog{
		RWMutex:   mu,
		Cfg:       cfg,
		TableSet:  make(map[uint64]*Table),
		NameNodes: make(map[string]*tableNode),
		NameIndex: btree.New(2),
	}
	if syncerCfg == nil {
		syncerCfg = &SyncerCfg{
			Interval: logstore.DefaultHBInterval,
		}
	} else if syncerCfg.Interval == 0 {
		syncerCfg.Interval = logstore.DefaultHBInterval
	}
	factory := &hbHandleFactory{
		catalog: catalog,
	}
	syncerCfg.Factory = factory.builder
	rotationCfg := &logstore.RotationCfg{}
	rotationCfg.RotateChecker = &logstore.MaxSizeRotationChecker{
		MaxSize: cfg.RotationFileMaxSize,
	}
	store, err := logstore.NewSyncAwareStore(common.MakeMetaDir(cfg.Dir), "store", rotationCfg, syncerCfg)
	if err != nil {
		panic(err)
	}
	catalog.Store = store
	catalog.archiver = worker.NewOpWorker("archiver")
	catalog.archiver.Start()
	return catalog
}

func (catalog *Catalog) StartSyncer() {
	catalog.Store.Start()
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

	catalog.NameIndex.AscendRange(
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
	catalog.Store.Close()
	if catalog.archiver != nil {
		catalog.archiver.Stop()
	}
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
	table.HardDelete()
	return nil
}

func (catalog *Catalog) SimpleDropTableByName(name string, exIndex *ExternalIndex) error {
	catalog.Lock()
	defer catalog.Unlock()
	return catalog.DropTableByName(name, catalog.NextUncommitId(), true, exIndex)
}

// Guarded by lock
func (catalog *Catalog) DropTableByName(name string, tranId uint64, autoCommit bool, exIndex *ExternalIndex) error {
	nn := catalog.NameNodes[name]
	if nn == nil {
		return TableNotFoundErr
	}
	entry := nn.GetEntry()
	catalog.Unlock()
	defer catalog.Lock()
	entry.RLock()
	if entry.IsSoftDeletedLocked() {
		entry.RUnlock()
		return TableNotFoundErr
	}
	entry.RUnlock()
	entry.SoftDelete(tranId, exIndex, autoCommit)
	return nil
}

func (catalog *Catalog) SimpleCreateTable(schema *Schema, exIndex *ExternalIndex) (*Table, error) {
	catalog.Lock()
	defer catalog.Unlock()
	return catalog.CreateTable(schema, catalog.NextUncommitId(), true, exIndex)
}

func (catalog *Catalog) CreateTable(schema *Schema, tranId uint64, autoCommit bool, exIndex *ExternalIndex) (*Table, error) {
	entry := NewTableEntry(catalog, schema, tranId, exIndex)
	if err := catalog.onNewTable(entry); err != nil {
		return nil, err
	}
	if !autoCommit {
		return entry, nil
	}
	catalog.commitLocked(entry, ETCreateTable, nil)
	return entry, nil
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
	nn := catalog.NameNodes[name]
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

func (catalog *Catalog) Commit(entry IEntry, eType LogEntryType, rwlocker *sync.RWMutex) {
	catalog.Lock()
	defer catalog.Unlock()
	catalog.commitLocked(entry, eType, rwlocker)
}

func (catalog *Catalog) commitLocked(entry IEntry, eType LogEntryType, rwlocker *sync.RWMutex) {
	commitId := catalog.NextCommitId()
	var lentry LogEntry
	if rwlocker == nil {
		entry.Lock()
		entry.CommitLocked(commitId)
		lentry = entry.ToLogEntry(eType)
		entry.Unlock()
	} else {
		entry.CommitLocked(commitId)
		lentry = entry.ToLogEntry(eType)
		rwlocker.Unlock()
		defer rwlocker.Lock()
	}
	err := catalog.CommitLogEntry(lentry, commitId, IsSyncDDLEntryType(eType))
	if err != nil {
		panic(err)
	}
	lentry.Free()
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

func (catalog *Catalog) Checkpoint() error {
	view := catalog.LatestView()
	err := catalog.Store.Checkpoint(view.ToLogEntry(logstore.ETCheckpoint))
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
	logEntry := logstore.GetEmptyEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}

func (catalog *Catalog) onNewTable(entry *Table) error {
	nn := catalog.NameNodes[entry.Schema.Name]
	if nn != nil {
		e := nn.GetEntry()
		if !e.IsSoftDeletedLocked() {
			return DuplicateErr
		}
		catalog.TableSet[entry.Id] = entry
		nn.CreateNode(entry.Id)
	} else {
		catalog.TableSet[entry.Id] = entry

		nn := newTableNode(catalog, entry.Schema.Name)
		catalog.NameNodes[entry.Schema.Name] = nn
		catalog.NameIndex.ReplaceOrInsert(nn)

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
	blk := newCommittedBlockEntry(seg, &entry.BaseEntry)
	seg.onNewBlock(blk)
	return nil
}

func (catalog *Catalog) onReplayUpgradeBlock(entry *blockLogEntry) error {
	tbl := catalog.TableSet[entry.TableId]
	segpos := tbl.IdIndex[entry.SegmentId]
	seg := tbl.SegmentSet[segpos]
	blkpos := seg.IdIndex[entry.Id]
	blk := seg.BlockSet[blkpos]
	blk.onNewCommit(entry.CommitInfo)
	return nil
}

func MockCatalog(dir string, blkRows, segBlks uint64) *Catalog {
	cfg := new(CatalogCfg)
	cfg.Dir = dir
	cfg.BlockMaxRows = blkRows
	cfg.SegmentMaxBlocks = segBlks
	catalog, err := OpenCatalog(new(sync.RWMutex), cfg, nil)
	if err != nil {
		panic(err)
	}
	catalog.StartSyncer()
	return catalog
}
