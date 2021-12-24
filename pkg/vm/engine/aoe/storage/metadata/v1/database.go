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

package metadata

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
)

type coarseStats struct {
	size, count int64
}

func (stats *coarseStats) AddCount(count int64) {
	atomic.AddInt64(&stats.count, count)
}

func (stats *coarseStats) GetCount() int64 {
	return atomic.LoadInt64(&stats.count)
}

func (stats *coarseStats) AddSize(size int64) {
	atomic.AddInt64(&stats.size, size)
}

func (stats *coarseStats) GetSize() int64 {
	return atomic.LoadInt64(&stats.size)
}

type Database struct {
	IdempotentChecker `json:"-"`
	*ShardWal         `json:"-"`
	*coarseStats      `json:"-"`
	*BaseEntry
	nameNodes map[string]*nodeList `json:"-"`
	Catalog   *Catalog             `json:"-"`
	Name      string               `json:"name"`

	TableSet map[uint64]*Table `json:"tables"`

	tableListener   TableListener   `json:"-"`
	segmentListener SegmentListener `json:"-"`
	blockListener   BlockListener   `json:"-"`
}

func NewDatabase(catalog *Catalog, name string, tranId uint64, exIndex *LogIndex) *Database {
	id := catalog.NextDatabaseId()
	if exIndex == nil {
		exIndex = &LogIndex{
			ShardId: id,
			Id:      shard.SimpleIndexId(0),
		}
	}
	db := &Database{
		coarseStats: new(coarseStats),
		ShardWal:    wal.NewWalShard(exIndex.ShardId, catalog.IndexWal),
		BaseEntry: &BaseEntry{
			Id: id,
			CommitInfo: &CommitInfo{
				TranId:   tranId,
				CommitId: tranId,
				SSLLNode: *common.NewSSLLNode(),
				Op:       OpCreate,
				LogIndex: exIndex,
			},
		},
		Name:      name,
		Catalog:   catalog,
		TableSet:  make(map[uint64]*Table),
		nameNodes: make(map[string]*nodeList),
	}
	db.initListeners()
	return db
}

func NewEmptyDatabase(catalog *Catalog) *Database {
	db := &Database{
		coarseStats: new(coarseStats),
		BaseEntry: &BaseEntry{
			CommitInfo: &CommitInfo{
				SSLLNode: *common.NewSSLLNode(),
			},
		},
		Catalog:   catalog,
		TableSet:  make(map[uint64]*Table),
		nameNodes: make(map[string]*nodeList),
	}
	db.initListeners()
	return db
}

func (db *Database) Repr() string {
	return fmt.Sprintf("DB-%d<\"%s\",S-%d>", db.Id, db.Name, db.CommitInfo.GetShardId())
}

func (db *Database) FindTableCommitByIndex(name string, index *LogIndex) *CommitInfo {
	db.RLock()
	defer db.RUnlock()
	return db.FindTableCommitByIndexLocked(name, index)
}

func (db *Database) FindTableCommitByIndexLocked(name string, index *LogIndex) *CommitInfo {
	node := db.nameNodes[name]
	if node == nil {
		return nil
	}
	var found *CommitInfo
	fn := func(nn *nameNode) bool {
		table := nn.GetTable()
		found = table.FindCommitByIndexLocked(index)
		if found != nil {
			return false
		}
		return true
	}
	node.ForEachNodes(fn)
	return found
}

func (db *Database) DebugCheckReplayedState() {
	if db.Catalog == nil {
		panic("catalog is missing")
	}
	if db.ShardWal == nil {
		panic("wal is missing")
	}
	if db.coarseStats == nil {
		panic("stats is missing")
	}
	if db.nameNodes == nil {
		panic("name nodes are missing")
	}
	if db.blockListener == nil || db.segmentListener == nil || db.tableListener == nil {
		panic("listener is missing")
	}
	if db.Catalog.TryUpdateCommitId(db.GetCommit().CommitId) {
		panic("sequence error")
	}
	if db.Catalog.TryUpdateDatabaseId(db.Id) {
		panic("sequence error")
	}
	var maxDDLIndex *LogIndex
	var maxIndex *LogIndex

	for _, table := range db.TableSet {
		table.DebugCheckReplayedState()
		table.InitIdempotentIndex(table.MaxLogIndex())
		if maxDDLIndex == nil {
			maxDDLIndex = table.CommitInfo.LogIndex
		} else if maxDDLIndex.Compare(table.CommitInfo.LogIndex) < 0 {
			maxDDLIndex = table.CommitInfo.LogIndex
		}
		if maxIndex == nil {
			maxIndex = table.GetIdempotentIndex()
		} else if maxIndex.Compare(table.GetIdempotentIndex()) < 0 {
			maxIndex = table.GetIdempotentIndex()
		}
	}
	db.InitIdempotentIndex(maxDDLIndex)
	db.InitMaxIndex(maxIndex)
}

func (db *Database) initListeners() {
	blockListener := new(BaseBlockListener)
	blockListener.BlockUpgradedFn = db.onBlockUpgraded
	segmentListener := new(BaseSegmentListener)
	segmentListener.SegmentUpgradedFn = db.onSegmentUpgraded
	tableListener := new(BaseTableListener)
	tableListener.HardDeletedFn = db.onTableHardDelete
	db.tableListener = tableListener
	db.blockListener = blockListener
	db.segmentListener = segmentListener
}

func (db *Database) onTableHardDelete(table *Table) {
	db.AddSize(-table.GetCoarseSize())
	db.AddCount(-table.GetCoarseCount())
}

func (db *Database) onBlockUpgraded(block *Block) {
	db.AddSize(block.GetCoarseSize())
	db.AddCount(block.GetCoarseCount())
}

func (db *Database) onSegmentUpgraded(segment *Segment, prev *CommitInfo) {
	db.AddSize(segment.GetCoarseSize() - segment.GetUnsortedSize())
}

func (db *Database) Release() {
	logutil.Infof("%s | Compacted", db.Repr())
	// PXU TODO
}

func (db *Database) CanHardDeleteLocked() bool {
	if !db.HasCommittedLocked() || !db.IsDeletedLocked() || db.IsHardDeletedLocked() {
		return false
	}
	if db.Catalog.IndexWal == nil {
		return true
	}
	return db.GetCheckpointId() == db.CommitInfo.GetIndex()
}

func (db *Database) GetShardId() uint64 {
	if db.ShardWal == nil {
		return db.BaseEntry.GetShardId()
	}
	return db.ShardWal.GetShardId()
}

func (db *Database) View(index uint64) *databaseLogEntry {
	shardId := db.GetShardId()
	filter := new(Filter)
	subFilter := newCommitFilter()
	subFilter.AddChecker(createIndexRangeChecker(index))
	filter.blockFilter = subFilter
	filter.segmentFilter = subFilter
	filter.tableFilter = newCommitFilter()
	filter.tableFilter.AddChecker(createIndexChecker(index))
	stopper := createDeleteAndIndexStopper(index)
	filter.tableFilter.AddStopper(stopper)
	filter.dbFilter = filter.tableFilter
	view := newDatabaseLogEntry(shardId, index)
	view.Database.Name = db.Name
	view.Id = shardId
	db.fillView(filter, view.Database)
	return view
}

func (db *Database) LatestView() *databaseLogEntry {
	commitId := db.Catalog.Store.GetSyncedId()
	filter := new(Filter)
	filter.tableFilter = newCommitFilter()
	filter.tableFilter.AddChecker(createCommitIdChecker(commitId))
	filter.segmentFilter = filter.tableFilter
	filter.blockFilter = filter.tableFilter
	view := newDatabaseLogEntry(db.GetShardId(), commitId)
	view.Database.Name = db.Name
	db.fillView(filter, view.Database)
	return view
}

func (db *Database) fillView(filter *Filter, view *Database) {
	baseEntry := db.UseCommitted(filter.dbFilter)
	if baseEntry == nil {
		return
	}
	view.BaseEntry = baseEntry
	entries := make(map[uint64]*Table)
	db.RLock()
	for id, entry := range db.TableSet {
		entries[id] = entry
	}
	db.RUnlock()
	for eid, entry := range entries {
		committed := entry.fillView(filter)
		if committed != nil {
			view.TableSet[eid] = committed
		}
	}
}

func (db *Database) prepareReplace(ctx *addReplaceCommitCtx) (LogEntry, error) {
	cInfo := &CommitInfo{
		Op:       OpReplaced,
		LogIndex: ctx.exIndex,
		SSLLNode: *common.NewSSLLNode(),
	}
	if ctx.inTran {
		cInfo.CommitId = ctx.tranId
		cInfo.TranId = ctx.tranId
	} else {
		cInfo.CommitId = db.Catalog.NextUncommitId()
		cInfo.TranId = ctx.tranId
	}
	db.Lock()
	defer db.Unlock()
	if db.IsHardDeletedLocked() {
		ctx.discard = true
		return nil, nil
	}
	if err := db.onCommit(cInfo); err != nil {
		return nil, err
	}
	if ctx.inTran {
		return nil, nil
	}
	logEntry := db.Catalog.prepareCommitEntry(db, ETDatabaseReplaced, db)
	return logEntry, nil
}

func (db *Database) LoopLocked(processor Processor) error {
	var err error
	for _, table := range db.TableSet {
		if err = processor.OnTable(table); err != nil {
			return err
		}
	}
	return err
}

func (db *Database) RecurLoopLocked(processor Processor) error {
	var err error
	for _, table := range db.TableSet {
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

func (db *Database) SimpleGetTableIds() []uint64 {
	db.RLock()
	defer db.RUnlock()
	ids := make([]uint64, len(db.TableSet))
	pos := 0
	for _, tbl := range db.TableSet {
		ids[pos] = tbl.Id
		pos++
	}
	return ids
}

func (db *Database) SimpleGetTableNames() []string {
	db.RLock()
	defer db.RUnlock()
	names := make([]string, len(db.TableSet))
	pos := 0
	for _, tbl := range db.TableSet {
		names[pos] = tbl.Schema.Name
		pos++
	}
	return names
}

func (db *Database) SimpleHardDeleteTable(id uint64) error {
	db.Lock()
	table := db.TableSet[id]
	if table == nil {
		db.Unlock()
		return TableNotFoundErr
	}
	db.Unlock()

	return table.HardDelete()
}

func (db *Database) SimpleHardDelete() error {
	tranId := db.Catalog.NextUncommitId()
	ctx := new(deleteDatabaseCtx)
	ctx.tranId = tranId
	ctx.database = db
	err := db.Catalog.onCommitRequest(ctx, true)
	return err
}

func (db *Database) prepareHardDelete(ctx *deleteDatabaseCtx) (LogEntry, error) {
	cInfo := &CommitInfo{
		CommitId: ctx.tranId,
		TranId:   ctx.tranId,
		Op:       OpHardDelete,
		SSLLNode: *common.NewSSLLNode(),
	}
	db.Lock()
	defer db.Unlock()
	if !db.CanHardDeleteLocked() {
		return nil, CannotHardDeleteErr
	}
	cInfo.LogIndex = db.CommitInfo.LogIndex
	if err := db.onCommit(cInfo); err != nil {
		return nil, err
	}
	logEntry := db.Catalog.prepareCommitEntry(db, ETHardDeleteDatabase, db)
	return logEntry, nil
}

func (db *Database) SimpleSoftDelete(index *LogIndex) error {
	tranId := db.Catalog.NextUncommitId()
	ctx := new(dropDatabaseCtx)
	ctx.tranId = tranId
	ctx.database = db
	ctx.exIndex = index
	return db.Catalog.onCommitRequest(ctx, true)
}

func (db *Database) SoftDeleteInTxn(txn *TxnCtx) error {
	ctx := new(dropDatabaseCtx)
	ctx.tranId = txn.tranId
	ctx.exIndex = txn.index
	ctx.database = db
	ctx.inTran = true
	ctx.txn = txn
	return db.Catalog.onCommitRequest(ctx, false)
}

func (db *Database) prepareSoftDelete(ctx *dropDatabaseCtx) (LogEntry, error) {
	cInfo := &CommitInfo{
		TranId:   ctx.tranId,
		CommitId: ctx.tranId,
		LogIndex: ctx.exIndex,
		Op:       OpSoftDelete,
		SSLLNode: *common.NewSSLLNode(),
	}
	db.Lock()
	if db.IsDeletedLocked() {
		db.Unlock()
		return nil, TableNotFoundErr
	}
	err := db.onCommit(cInfo)
	db.Unlock()
	if err != nil {
		return nil, err
	}
	if ctx.inTran {
		ctx.txn.AddEntry(db, ETSoftDeleteDatabase)
		return nil, nil
	}
	logEntry := db.Catalog.prepareCommitEntry(db, ETSoftDeleteDatabase, nil)
	return logEntry, nil
}

func (db *Database) SimpleDropTableByName(name string, exIndex *LogIndex) error {
	table := db.SimpleGetTableByName(name)
	if table == nil {
		return TableNotFoundErr
	}
	tranId := db.Catalog.NextUncommitId()
	ctx := new(dropTableCtx)
	ctx.tranId = tranId
	ctx.exIndex = exIndex
	ctx.table = table
	return db.Catalog.onCommitRequest(ctx, true)
}

func (db *Database) DropTableByNameInTxn(txn *TxnCtx, name string) (*Table, error) {
	table := db.GetTableByNameInTxn(txn, name)
	if table == nil {
		return nil, TableNotFoundErr
	}
	ctx := new(dropTableCtx)
	ctx.tranId = txn.tranId
	ctx.exIndex = txn.index
	ctx.table = table
	ctx.txn = txn
	ctx.inTran = true
	return table, db.Catalog.onCommitRequest(ctx, false)
}

func (db *Database) GetTableByNameInTxn(txn *TxnCtx, name string) *Table {
	db.RLock()
	defer db.RUnlock()
	return db.GetTableByNameLocked(name, txn.tranId)
}

func (db *Database) SimpleGetTableByName(name string) *Table {
	db.RLock()
	defer db.RUnlock()
	if db.IsDeletedLocked() {
		return nil
	}
	return db.GetTableByNameLocked(name, MinUncommitId)
}

func (db *Database) GetTableByNameLocked(name string, tranId uint64) *Table {
	nn := db.nameNodes[name]
	if nn == nil {
		return nil
	}

	next := nn.GetNext()
	for next != nil {
		entry := next.(*nameNode).GetTable()
		if entry.CanUseTxn(tranId) && !entry.IsDeleted() {
			return entry
		}
		next = next.GetNext()
	}
	return nil
}

func (db *Database) GetTableByNameAndLogIndex(name string, index *LogIndex) (*Table, error) {
	db.RLock()
	defer db.RUnlock()
	nn := db.nameNodes[name]
	if nn == nil {
		return nil, TableNotFoundErr
	}
	var err error
	var table *Table
	nn.ForEachNodesLocked(func(n *nameNode) bool {
		entry := n.GetTable()
		createIdx := entry.FirstCommit().LogIndex
		if createIdx == nil || createIdx.Compare(index) <= 0 {
			table = entry
			return false
		}
		return true
	})
	if table == nil {
		err = TableNotFoundErr
	}
	return table, err
}

func (db *Database) SimpleGetTable(id uint64) *Table {
	db.RLock()
	defer db.RUnlock()
	return db.GetTable(id)
}

func (db *Database) GetTable(id uint64) *Table {
	return db.TableSet[id]
}

func (db *Database) ForLoopTables(h func(*Table) error) error {
	tables := make([]*Table, 0, 10)
	db.RLock()
	for _, tbl := range db.TableSet {
		tables = append(tables, tbl)
	}
	db.RUnlock()
	for _, tbl := range tables {
		if err := h(tbl); err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) SimpleGetSegment(tableId, segmentId uint64) (*Segment, error) {
	table := db.SimpleGetTable(tableId)
	if table == nil {
		return nil, TableNotFoundErr
	}
	segment := table.SimpleGetSegment(segmentId)
	if segment == nil {
		return nil, SegmentNotFoundErr
	}
	return segment, nil
}

func (db *Database) SimpleGetBlock(tableId, segmentId, blockId uint64) (*Block, error) {
	segment, err := db.SimpleGetSegment(tableId, segmentId)
	if err != nil {
		return nil, err
	}
	block := segment.SimpleGetBlock(blockId)
	if block == nil {
		return nil, BlockNotFoundErr
	}
	return block, nil
}

func (db *Database) SimpleCreateTable(schema *Schema, indice *IndexSchema, exIndex *LogIndex) (*Table, error) {
	if !schema.Valid() {
		return nil, InvalidSchemaErr
	}
	tranId := db.Catalog.NextUncommitId()
	ctx := new(createTableCtx)
	ctx.tranId = tranId
	ctx.exIndex = exIndex
	ctx.schema = schema
	ctx.indice = indice
	ctx.database = db
	err := db.Catalog.onCommitRequest(ctx, true)
	return ctx.table, err
}

func (db *Database) CreateTableInTxn(txn *TxnCtx, schema *Schema, indice *IndexSchema) (*Table, error) {
	if !schema.Valid() {
		return nil, InvalidSchemaErr
	}
	ctx := new(createTableCtx)
	ctx.tranId = txn.tranId
	ctx.exIndex = txn.index
	ctx.schema = schema
	ctx.indice = indice
	ctx.database = db
	ctx.txn = txn
	ctx.inTran = true
	err := db.Catalog.onCommitRequest(ctx, false)
	return ctx.table, err
}

func (db *Database) prepareCreateTable(ctx *createTableCtx) (LogEntry, error) {
	var err error
	entry := NewTableEntry(db, ctx.schema, ctx.indice, ctx.tranId, ctx.exIndex)
	db.Lock()
	if db.IsSoftDeletedLocked() {
		db.Unlock()
		return nil, DatabaseNotFoundErr
	}
	// if ctx.exIndex != nil {
	// 	if found := db.FindTableLogIndexLocked(ctx.schema.Name, ctx.exIndex); found {
	// 		db.Unlock()
	// 		return nil, CommitStaleErr
	// 	}
	// }
	if err = db.onNewTable(entry); err != nil {
		db.Unlock()
		return nil, err
	}
	db.Unlock()
	ctx.table = entry
	if ctx.inTran {
		ctx.txn.AddEntry(entry, ETCreateTable)
		return nil, nil
	}
	logEntry := entry.ToLogEntry(ETCreateTable)
	db.Catalog.prepareCommitLog(entry, logEntry)
	return logEntry, err
}

func (db *Database) prepareAddTable(ctx *addTableCtx) (LogEntry, error) {
	var err error
	db.Lock()
	if err = db.onNewTable(ctx.table); err != nil {
		db.Unlock()
		return nil, err
	}
	db.Unlock()
	if ctx.inTran {
		return nil, nil
	}
	panic("todo")
}

func (db *Database) SplitCheck(size, index uint64) (coarseSize uint64, coarseCount uint64, keys [][]byte, ctx []byte, err error) {
	coarseSize, coarseCount = uint64(db.GetSize()), uint64(db.GetCount())
	if coarseSize < size {
		return
	}

	partSize := int64(size) / 2
	if coarseSize/size < 2 {
		partSize = int64(coarseSize) / 2
	}

	view := db.View(index)
	totalSize := int64(0)

	shardSpec := NewShardSplitSpec(db.Name, index)
	activeSize := int64(0)
	currGroup := uint32(0)

	for _, table := range view.Database.TableSet {
		spec := NewTableSplitSpec(table.GetCommit().LogIndex)
		rangeSpec := new(TableRangeSpec)
		rangeSpec.Group = currGroup
		rangeSpec.Range.Left = uint64(0)
		tableSize := table.GetCoarseSize()
		totalSize += tableSize
		if len(table.SegmentSet) <= 1 {
			activeSize += tableSize
			rangeSpec.CoarseSize += tableSize
			spec.AddSpec(rangeSpec)
			if activeSize >= partSize {
				currGroup++
				activeSize = int64(0)
			}
		} else {
			for i, segment := range table.SegmentSet {
				activeSize += segment.GetCoarseSize()
				rangeSpec.CoarseSize += segment.GetCoarseSize()
				rangeSpec.Range.Right = uint64(i+1)*table.Schema.BlockMaxRows*table.Schema.SegmentMaxBlocks - 1
				if activeSize >= partSize {
					currGroup++
					activeSize = int64(0)
					spec.AddSpec(rangeSpec)
					last := rangeSpec
					rangeSpec = new(TableRangeSpec)
					rangeSpec.Group = currGroup
					rangeSpec.Range.Left = last.Range.Right + uint64(1)
				}
			}
			if rangeSpec.Range.Right != uint64(0) {
				spec.AddSpec(rangeSpec)
			}
		}
		lastRange := spec.LastSpec()
		if lastRange != nil {
			lastRange.Range.Right = math.MaxUint64
		}
		shardSpec.AddSpec(spec)
	}
	if totalSize < int64(size) {
		return
	}

	keys = make([][]byte, currGroup+1)
	for i, _ := range keys {
		keys[i] = []byte("1")
	}
	// logutil.Infof(shardSpec.String())
	ctx, err = shardSpec.Marshal()
	return
}

func (db *Database) Marshal() ([]byte, error) {
	return json.Marshal(db)
}

func (db *Database) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, db)
}

func (db *Database) String() string {
	buf, _ := db.Marshal()
	return string(buf)
}

// Not safe
// Usually it is used during creating a table. We need to commit the new table entry
// to the store.
func (db *Database) ToLogEntry(eType LogEntryType) LogEntry {
	var buf []byte
	switch eType {
	case ETCreateDatabase:
		buf, _ = db.Marshal()
	case ETSoftDeleteDatabase:
		if !db.IsSoftDeletedLocked() {
			panic("logic error")
		}
		entry := databaseLogEntry{
			BaseEntry: db.BaseEntry,
		}
		buf, _ = entry.Marshal()
	case ETHardDeleteDatabase:
		if !db.IsHardDeletedLocked() {
			panic("logic error")
		}
		entry := databaseLogEntry{
			BaseEntry: db.BaseEntry,
		}
		buf, _ = entry.Marshal()
	case ETDatabaseReplaced:
		if !db.IsHardDeletedLocked() {
			panic("logic error")
		}
		entry := databaseLogEntry{
			BaseEntry: db.BaseEntry,
		}
		buf, _ = entry.Marshal()
	default:
		panic(fmt.Sprintf("not supported: %d", eType))
	}
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}

func (db *Database) PString(level PPLevel, depth int) string {
	db.RLock()
	defer db.RUnlock()
	ident := strings.Repeat("  ", depth)
	ident2 := " " + ident
	s := fmt.Sprintf("%s | %s | Cnt=%d {", db.Repr(), db.BaseEntry.PString(level), len(db.TableSet))
	for _, table := range db.TableSet {
		s = fmt.Sprintf("%s\n%s%s", s, ident2, table.PString(level, depth+1))
	}
	if len(db.TableSet) == 0 {
		s = fmt.Sprintf("%s}", s)
	} else {
		s = fmt.Sprintf("%s\n%s}", s, ident)
	}
	return s
}

func (db *Database) Compact(dbListener DatabaseListener, tblListener TableListener) {
	tables := make([]*Table, 0, 2)
	nodes := make([]*nodeList, 0, 2)
	safeId := db.GetCheckpointId()
	db.RLock()
	// If database is hard delete, it will be handled during catalog compact cycle
	// if db.IsDeletedLocked() {
	// 	hardDeleted := db.IsHardDeletedLocked()
	// 	canHardDelete := db.CanHardDeleteLocked()
	// 	db.RUnlock()
	// 	if hardDeleted || !canHardDelete {
	// 		return
	// 	}
	// 	if err := db.SimpleHardDelete(); err != nil {
	// 		panic(err)
	// 	}
	// 	logutil.Infof("%s | HardDeleted", db.Repr())
	// 	return
	// }
	if db.IsHardDeletedLocked() && db.HasCommittedLocked() {
		db.RUnlock()
		return
	}
	for _, table := range db.TableSet {
		table.RLock()
		if table.IsHardDeletedLocked() && table.HasCommittedLocked() && (!db.WalEnabled() || table.CommitInfo.GetIndex() <= safeId) {
			tables = append(tables, table)
			nodes = append(nodes, db.nameNodes[table.Schema.Name])
		}
		table.RUnlock()
	}
	db.RUnlock()
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
	db.Lock()
	for _, table := range tables {
		logutil.Infof("%s | Compacted", table.Repr(false))
		delete(db.TableSet, table.Id)
		if tblListener != nil {
			tblListener.OnTableCompacted(table)
		}
	}
	db.Unlock()
	if len(names) > 0 {
		db.compactNameNodes(names...)
	}
}

func (db *Database) compactNameNodes(names ...string) (deleted []*nodeList) {
	db.Lock()
	defer db.Unlock()
	for _, name := range names {
		node := db.nameNodes[name]
		if node == nil {
			continue
		}
		if node.Length() != 0 {
			continue
		}
		delete(db.nameNodes, name)
	}
	return
}

func (db *Database) onNewTable(entry *Table) error {
	nn := db.nameNodes[entry.Schema.Name]
	if nn != nil {
		e := nn.GetTable()
		// Conflict checks all committed and uncommitted entries.
		if !e.IsDeleted() {
			return DuplicateErr
		}
		db.TableSet[entry.Id] = entry
		nn.CreateNode(entry.Id)
	} else {
		db.TableSet[entry.Id] = entry

		nn := newNodeList(db, &db.Catalog.nodesMu, entry.Schema.Name)
		db.nameNodes[entry.Schema.Name] = nn

		nn.CreateNode(entry.Id)
	}
	return nil
}

func (db *Database) rebuildStats() {
	for _, table := range db.TableSet {
		db.AddSize(table.GetCoarseSize())
		db.AddCount(table.GetCoarseCount())
	}
}

func (db *Database) rebuild(stats, replay bool) error {
	db.initListeners()
	db.ShardWal = wal.NewWalShard(db.BaseEntry.GetShardId(), db.Catalog.IndexWal)
	db.coarseStats = new(coarseStats)
	sorted := make([]*Table, len(db.TableSet))
	idx := 0
	for _, table := range db.TableSet {
		sorted[idx] = table
		idx++
	}
	db.TableSet = make(map[uint64]*Table)
	db.nameNodes = make(map[string]*nodeList)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Id < sorted[j].Id
	})
	for _, table := range sorted {
		if err := db.onNewTable(table); err != nil {
			return err
		}
		if replay {
			db.Catalog.TryUpdateTableId(table.Id)
		}
		table.rebuild(db, replay)
		if stats {
			db.AddSize(table.GetCoarseSize())
			db.AddCount(table.GetCoarseCount())
		}
	}
	return nil
}
