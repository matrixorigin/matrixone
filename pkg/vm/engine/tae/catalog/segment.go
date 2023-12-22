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
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type SegmentEntry struct {
	ID   objectio.Segmentid
	Stat SegStat
	*BaseEntryImpl[*MetadataMVCCNode]
	table   *TableEntry
	entries map[types.Blockid]*common.GenericDLNode[*BlockEntry]
	//link.head and tail is nil when new a segmentEntry object.
	link *common.GenericSortedDList[*BlockEntry]
	*SegmentNode
}

type SegStat struct {
	// min max etc. later
	sync.RWMutex
	loaded         bool
	originSize     int
	compSize       int
	sortKeyZonemap index.ZM
	rows           int
	remainingRows  int
}

func (s *SegStat) loadObjectInfo(blk *BlockEntry) error {
	schema := blk.GetSchema()
	loc := blk.GetMetaLoc()

	if len(loc) == 0 {
		return nil
	}

	s.Lock()
	defer s.Unlock()

	if s.loaded {
		return nil
	}

	objMeta, err := objectio.FastLoadObjectMeta(context.Background(), &loc, false, blk.blkData.GetFs().Service)
	if err != nil {
		return err
	}

	meta := objMeta.MustDataMeta()

	for _, col := range schema.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		colmata := meta.MustGetColumn(uint16(col.SeqNum))
		s.originSize += int(colmata.Location().OriginSize())
		s.compSize += int(colmata.Location().Length())
	}

	if schema.HasSortKey() {
		col := schema.GetSingleSortKey()
		s.sortKeyZonemap = meta.MustGetColumn(col.SeqNum).ZoneMap().Clone()
	}

	s.loaded = true

	return nil
}

func (s *SegStat) GetLoaded() bool {
	s.RLock()
	defer s.RUnlock()
	return s.loaded
}

func (s *SegStat) GetSortKeyZonemap() index.ZM {
	s.RLock()
	defer s.RUnlock()
	return s.sortKeyZonemap.Clone()
}

func (s *SegStat) SetRows(rows int) {
	s.Lock()
	defer s.Unlock()
	s.rows = rows
}

func (s *SegStat) SetRemainingRows(rows int) {
	s.Lock()
	defer s.Unlock()
	s.remainingRows = rows
}

func (s *SegStat) GetRemainingRows() int {
	s.RLock()
	defer s.RUnlock()
	return s.remainingRows
}

func (s *SegStat) GetRows() int {
	s.RLock()
	defer s.RUnlock()
	return s.rows
}

func (s *SegStat) GetOriginSize() int {
	s.RLock()
	defer s.RUnlock()
	return s.originSize
}

func (s *SegStat) SetOriginSize(size int) {
	s.Lock()
	defer s.Unlock()
	s.originSize = size
}

func (s *SegStat) GetCompSize() int {
	s.RLock()
	defer s.RUnlock()
	return s.compSize
}

func (s *SegStat) String(composeSortKey bool) string {
	zonemapStr := "nil"
	if s.sortKeyZonemap != nil {
		if composeSortKey {
			zonemapStr = s.sortKeyZonemap.StringForCompose()
		} else {
			zonemapStr = s.sortKeyZonemap.String()
		}
	}
	return fmt.Sprintf("loaded:%t, oSize:%s, cSize %v,  rows:%d, remainingRows:%d, zm: %s",
		s.loaded, common.HumanReadableBytes(s.originSize), common.HumanReadableBytes(s.compSize), s.rows, s.remainingRows, zonemapStr,
	)
}

func NewSegmentEntry(table *TableEntry, id *objectio.Segmentid, txn txnif.AsyncTxn, state EntryState) *SegmentEntry {
	e := &SegmentEntry{
		ID: *id,
		BaseEntryImpl: NewBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} }),
		table:   table,
		link:    common.NewGenericSortedDList((*BlockEntry).Less),
		entries: make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
		SegmentNode: &SegmentNode{
			state:    state,
			SortHint: table.GetDB().catalog.NextSegment(),
		},
	}
	e.CreateWithTxn(txn, &MetadataMVCCNode{})
	return e
}

func NewReplaySegmentEntry() *SegmentEntry {
	e := &SegmentEntry{
		BaseEntryImpl: NewReplayBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} }),
		link:    common.NewGenericSortedDList((*BlockEntry).Less),
		entries: make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
	}
	return e
}

func NewStandaloneSegment(table *TableEntry, ts types.TS) *SegmentEntry {
	e := &SegmentEntry{
		ID: *objectio.NewSegmentid(),
		BaseEntryImpl: NewBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} }),
		table:   table,
		link:    common.NewGenericSortedDList((*BlockEntry).Less),
		entries: make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
		SegmentNode: &SegmentNode{
			state:   ES_Appendable,
			IsLocal: true,
		},
	}
	e.CreateWithTS(ts, &MetadataMVCCNode{})
	return e
}

func NewSysSegmentEntry(table *TableEntry, id types.Uuid) *SegmentEntry {
	e := &SegmentEntry{
		BaseEntryImpl: NewBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} }),
		table:   table,
		link:    common.NewGenericSortedDList((*BlockEntry).Less),
		entries: make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
		SegmentNode: &SegmentNode{
			state: ES_Appendable,
		},
	}
	e.CreateWithTS(types.SystemDBTS, &MetadataMVCCNode{})
	var bid types.Blockid
	schema := table.GetLastestSchema()
	if schema.Name == SystemTableSchema.Name {
		bid = SystemBlock_Table_ID
	} else if schema.Name == SystemDBSchema.Name {
		bid = SystemBlock_DB_ID
	} else if schema.Name == SystemColumnSchema.Name {
		bid = SystemBlock_Columns_ID
	} else {
		panic("not supported")
	}
	e.ID = *bid.Segment()
	block := NewSysBlockEntry(e, bid)
	e.AddEntryLocked(block)
	return e
}

func (entry *SegmentEntry) GetFirstBlkEntry() *BlockEntry {
	entry.RLock()
	defer entry.RUnlock()

	// head may be nil
	head := entry.link.GetHead()
	if head == nil {
		return nil
	}

	return head.GetPayload()
}

func (entry *SegmentEntry) Less(b *SegmentEntry) int {
	if entry.SortHint < b.SortHint {
		return -1
	} else if entry.SortHint > b.SortHint {
		return 1
	}
	return 0
}

// LoadObjectInfo is called only in merge scanner goroutine, no need to hold lock
func (entry *SegmentEntry) LoadObjectInfo() error {
	name := entry.GetTable().GetLastestSchema().Name
	defer func() {
		// after loading object info, original size is still 0, we have to estimate it by experience
		rows := entry.Stat.GetRows()
		if name == motrace.RawLogTbl && entry.Stat.GetOriginSize() == 0 && rows != 0 {
			factor := 1 + rows/1600
			entry.Stat.SetOriginSize((1 << 20) * factor)
		}
	}()

	if entry.Stat.GetLoaded() {
		return nil
	}

	// special case for raw log table.
	if name == motrace.RawLogTbl &&
		len(entry.table.entries) > int(common.RuntimeNotLoadMoreThan.Load()) {
		return nil
	}

	blk := entry.GetFirstBlkEntry()
	if blk == nil {
		return nil
	}

	return entry.Stat.loadObjectInfo(blk)
}

func (entry *SegmentEntry) GetBlockEntryByID(id *objectio.Blockid) (blk *BlockEntry, err error) {
	entry.RLock()
	defer entry.RUnlock()
	return entry.GetBlockEntryByIDLocked(id)
}

// XXX API like this, why do we need the error?   Isn't blk is nil enough?
func (entry *SegmentEntry) GetBlockEntryByIDLocked(id *objectio.Blockid) (blk *BlockEntry, err error) {
	node := entry.entries[*id]
	if node == nil {
		err = moerr.GetOkExpectedEOB()
		return
	}
	blk = node.GetPayload()
	return
}

func (entry *SegmentEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := IOET_WALTxnCommand_Segment
	entry.RLock()
	defer entry.RUnlock()
	return newSegmentCmd(id, cmdType, entry), nil
}

func (entry *SegmentEntry) Set1PC() {
	entry.GetLatestNodeLocked().Set1PC()
}
func (entry *SegmentEntry) Is1PC() bool {
	return entry.GetLatestNodeLocked().Is1PC()
}
func (entry *SegmentEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.StringWithLevel(level)))
	if level == common.PPL0 {
		return w.String()
	}
	it := entry.MakeBlockIt(true)
	for it.Valid() {
		block := it.Get().GetPayload()
		block.RLock()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(block.PPString(level, depth+1, prefix))
		block.RUnlock()
		it.Next()
	}
	return w.String()
}

func (entry *SegmentEntry) StringLocked() string {
	return entry.StringWithLevelLocked(common.PPL1)
}

func (entry *SegmentEntry) Repr() string {
	id := entry.AsCommonID()
	return fmt.Sprintf("[%s%s]SEG[%s]", entry.state.Repr(), entry.SegmentNode.String(), id.String())
}

func (entry *SegmentEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
}

func (entry *SegmentEntry) StringWithLevel(level common.PPLevel) string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringWithLevelLocked(level)
}

func (entry *SegmentEntry) StringWithLevelLocked(level common.PPLevel) string {
	if level <= common.PPL1 {
		return fmt.Sprintf("[%s-%s]SEG[%s][C@%s,D@%s]",
			entry.state.Repr(), entry.SegmentNode.String(), entry.ID.ToString(), entry.GetCreatedAtLocked().ToString(), entry.GetDeleteAt().ToString())
	}
	return fmt.Sprintf("[%s-%s]SEG[%s]%s", entry.state.Repr(), entry.SegmentNode.String(), entry.ID.ToString(), entry.BaseEntryImpl.StringLocked())
}

func (entry *SegmentEntry) BlockCnt() int {
	return len(entry.entries)
}

func (entry *SegmentEntry) IsAppendable() bool {
	return entry.state == ES_Appendable
}

func (entry *SegmentEntry) SetSorted() {
	// modifing segment interface to supporte a borned sorted seg is verbose
	// use Lock instead, the contention won't be intense
	entry.Lock()
	defer entry.Unlock()
	entry.sorted = true
}

func (entry *SegmentEntry) IsSorted() bool {
	entry.RLock()
	defer entry.RUnlock()
	return entry.sorted
}

func (entry *SegmentEntry) IsSortedLocked() bool {
	return entry.sorted
}

func (entry *SegmentEntry) GetTable() *TableEntry {
	return entry.table
}

func (entry *SegmentEntry) GetAppendableBlockCnt() int {
	cnt := 0
	it := entry.MakeBlockIt(true)
	for it.Valid() {
		if it.Get().GetPayload().IsAppendable() {
			cnt++
		}
		it.Next()
	}
	return cnt
}

// GetNonAppendableBlockCnt Non-appendable segment only can contain non-appendable blocks;
// Appendable segment can contain both of appendable blocks and non-appendable blocks
func (entry *SegmentEntry) GetNonAppendableBlockCnt() int {
	cnt := 0
	it := entry.MakeBlockIt(true)
	for it.Valid() {
		if !it.Get().GetPayload().IsAppendable() {
			cnt++
		}
		it.Next()
	}
	return cnt
}

func (entry *SegmentEntry) GetAppendableBlock() (blk *BlockEntry) {
	it := entry.MakeBlockIt(false)
	for it.Valid() {
		itBlk := it.Get().GetPayload()
		if itBlk.IsAppendable() {
			blk = itBlk
			break
		}
		it.Next()
	}
	return
}
func (entry *SegmentEntry) LastAppendableBlock() (blk *BlockEntry) {
	it := entry.MakeBlockIt(false)
	for it.Valid() {
		itBlk := it.Get().GetPayload()
		dropped := itBlk.HasDropCommitted()
		if itBlk.IsAppendable() && !dropped {
			blk = itBlk
			break
		}
		it.Next()
	}
	return
}

func (entry *SegmentEntry) GetNextObjectIndex() uint16 {
	entry.RLock()
	defer entry.RUnlock()
	return entry.nextObjectIdx
}

func (entry *SegmentEntry) CreateBlock(
	txn txnif.AsyncTxn,
	state EntryState,
	dataFactory BlockDataFactory,
	opts *objectio.CreateBlockOpt) (created *BlockEntry, err error) {
	entry.Lock()
	defer entry.Unlock()
	var id *objectio.Blockid
	if opts != nil && opts.Id != nil {
		id = objectio.NewBlockid(&entry.ID, opts.Id.Filen, opts.Id.Blkn)
		if entry.nextObjectIdx <= opts.Id.Filen {
			entry.nextObjectIdx = opts.Id.Filen + 1
		}
	} else {
		id = objectio.NewBlockid(&entry.ID, entry.nextObjectIdx, 0)
		entry.nextObjectIdx += 1
	}
	if entry.nextObjectIdx == math.MaxUint16 {
		panic("bad logic: full object offset")
	}
	if _, ok := entry.entries[*id]; ok {
		panic(fmt.Sprintf("duplicate bad block id: %s", id.String()))
	}
	if opts != nil && opts.Loc != nil {
		created = NewBlockEntryWithMeta(entry, id, txn, state, dataFactory, opts.Loc.Metaloc, opts.Loc.Deltaloc)
	} else {
		created = NewBlockEntry(entry, id, txn, state, dataFactory)
	}
	entry.AddEntryLocked(created)
	return
}

func (entry *SegmentEntry) DropBlockEntry(id *objectio.Blockid, txn txnif.AsyncTxn) (deleted *BlockEntry, err error) {
	blk, err := entry.GetBlockEntryByID(id)
	if err != nil {
		return
	}
	blk.Lock()
	defer blk.Unlock()
	needWait, waitTxn := blk.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		blk.Unlock()
		waitTxn.GetTxnState(true)
		blk.Lock()
	}
	var isNewNode bool
	isNewNode, err = blk.DropEntryLocked(txn)
	if err == nil && isNewNode {
		deleted = blk
	}
	return
}

func (entry *SegmentEntry) MakeBlockIt(reverse bool) *common.GenericSortedDListIt[*BlockEntry] {
	entry.RLock()
	defer entry.RUnlock()
	return common.NewGenericSortedDListIt(entry.RWMutex, entry.link, reverse)
}

func (entry *SegmentEntry) AddEntryLocked(block *BlockEntry) {
	n := entry.link.Insert(block)
	entry.entries[block.ID] = n
}

func (entry *SegmentEntry) ReplayAddEntryLocked(block *BlockEntry) {
	// bump object idx during replaying.
	objn, _ := block.ID.Offsets()
	entry.replayNextObjectIdx(objn)
	entry.AddEntryLocked(block)
}

func (entry *SegmentEntry) replayNextObjectIdx(objn uint16) {
	if objn >= entry.nextObjectIdx {
		entry.nextObjectIdx = objn + 1
	}
}

func (entry *SegmentEntry) AsCommonID() *common.ID {
	id := &common.ID{
		DbID:    entry.GetTable().GetDB().ID,
		TableID: entry.GetTable().ID,
	}
	id.SetSegmentID(&entry.ID)
	return id
}

func (entry *SegmentEntry) GetCatalog() *Catalog { return entry.table.db.catalog }

func (entry *SegmentEntry) deleteEntryLocked(block *BlockEntry) error {
	if n, ok := entry.entries[block.ID]; !ok {
		return moerr.GetOkExpectedEOB()
	} else {
		entry.link.Delete(n)
		delete(entry.entries, block.ID)
	}
	// block.blkData.Close()
	// block.blkData = nil
	return nil
}

func (entry *SegmentEntry) RemoveEntry(block *BlockEntry) (err error) {
	logutil.Debug("[Catalog]", common.OperationField("remove"),
		common.OperandField(block.String()))
	entry.Lock()
	defer entry.Unlock()
	return entry.deleteEntryLocked(block)
}

func (entry *SegmentEntry) PrepareRollback() (err error) {
	var isEmpty bool
	if isEmpty, err = entry.BaseEntryImpl.PrepareRollback(); err != nil {
		return
	}
	if isEmpty {
		if err = entry.GetTable().RemoveEntry(entry); err != nil {
			return
		}
	}
	return
}

func (entry *SegmentEntry) CollectBlockEntries(commitFilter func(be *BaseEntryImpl[*MetadataMVCCNode]) bool, blockFilter func(be *BlockEntry) bool) []*BlockEntry {
	blks := make([]*BlockEntry, 0)
	blkIt := entry.MakeBlockIt(true)
	for blkIt.Valid() {
		blk := blkIt.Get().GetPayload()
		blk.RLock()
		if commitFilter != nil && blockFilter != nil {
			if commitFilter(blk.BaseEntryImpl) && blockFilter(blk) {
				blks = append(blks, blk)
			}
		} else if blockFilter != nil {
			if blockFilter(blk) {
				blks = append(blks, blk)
			}
		} else if commitFilter != nil {
			if commitFilter(blk.BaseEntryImpl) {
				blks = append(blks, blk)
			}
		}
		blk.RUnlock()
		blkIt.Next()
	}
	return blks
}

// IsActive is coarse API: no consistency check
func (entry *SegmentEntry) IsActive() bool {
	table := entry.GetTable()
	if !table.IsActive() {
		return false
	}
	return !entry.HasDropCommitted()
}

func (entry *SegmentEntry) TreeMaxDropCommitEntry() BaseEntry {
	table := entry.GetTable()
	db := table.GetDB()
	if db.HasDropCommittedLocked() {
		return db.BaseEntryImpl
	}
	if table.HasDropCommittedLocked() {
		return table.BaseEntryImpl
	}
	if entry.HasDropCommittedLocked() {
		return entry.BaseEntryImpl
	}
	return nil
}

// GetTerminationTS is coarse API: no consistency check
func (entry *SegmentEntry) GetTerminationTS() (ts types.TS, terminated bool) {
	tableEntry := entry.GetTable()
	dbEntry := tableEntry.GetDB()

	dbEntry.RLock()
	terminated, ts = dbEntry.TryGetTerminatedTS(true)
	if terminated {
		dbEntry.RUnlock()
		return
	}
	dbEntry.RUnlock()

	tableEntry.RLock()
	terminated, ts = tableEntry.TryGetTerminatedTS(true)
	tableEntry.RUnlock()
	return
}
