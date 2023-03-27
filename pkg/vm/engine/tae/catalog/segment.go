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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type SegmentDataFactory = func(meta *SegmentEntry) data.Segment

func compareSegmentFn(a, b *SegmentEntry) int {
	if a.SortHint < b.SortHint {
		return -1
	} else if a.SortHint > b.SortHint {
		return 1
	}
	return 0
}

type SegmentEntry struct {
	*MetaBaseEntry
	table    *TableEntry
	ID       types.Uuid
	IsLocal  bool   // this segment is hold by a localsegment
	SortHint uint64 // sort segment by create time, make iteration on segment determined
	// used in appendable segment, bump this if creating a new block, and
	// the block will be eventually flushed to a s3 file.
	// for non-appendable segment, this field makes no sense, because if we
	// decide to create a new non-appendable segment, its content is all set.
	nextObjectIdx uint16
	sorted        bool // deprecated

	entries map[types.Blockid]*common.GenericDLNode[*BlockEntry]
	//link.head and tail is nil when new a segmentEntry object.
	link  *common.GenericSortedDList[*BlockEntry]
	state EntryState

	segData data.Segment
}

func NewSegmentEntry(table *TableEntry, id types.Uuid, txn txnif.AsyncTxn, state EntryState, dataFactory SegmentDataFactory) *SegmentEntry {
	e := &SegmentEntry{
		MetaBaseEntry: NewMetaBaseEntry(),
		ID:            id,
		SortHint:      table.GetDB().catalog.NextSegment(),
		table:         table,
		link:          common.NewGenericSortedDList(compareBlockFn),
		entries:       make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
		state:         state,
	}
	e.CreateWithTxn(txn)
	if dataFactory != nil {
		e.segData = dataFactory(e)
	}
	return e
}

func NewReplaySegmentEntry() *SegmentEntry {
	e := &SegmentEntry{
		MetaBaseEntry: NewReplayMetaBaseEntry(),
		link:          common.NewGenericSortedDList(compareBlockFn),
		entries:       make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
	}
	return e
}

func NewStandaloneSegment(table *TableEntry, ts types.TS) *SegmentEntry {
	e := &SegmentEntry{
		MetaBaseEntry: NewMetaBaseEntry(),
		ID:            common.NewSegmentid(),
		IsLocal:       true,
		table:         table,
		link:          common.NewGenericSortedDList(compareBlockFn),
		entries:       make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
		state:         ES_Appendable,
	}
	e.CreateWithTS(ts)
	return e
}

func NewSysSegmentEntry(table *TableEntry, id types.Uuid) *SegmentEntry {
	e := &SegmentEntry{
		MetaBaseEntry: NewMetaBaseEntry(),
		ID:            id,
		table:         table,
		link:          common.NewGenericSortedDList(compareBlockFn),
		entries:       make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
		state:         ES_Appendable,
	}
	e.CreateWithTS(types.SystemDBTS)
	var bid types.Blockid
	if table.schema.Name == SystemTableSchema.Name {
		bid = SystemBlock_Table_ID
	} else if table.schema.Name == SystemDBSchema.Name {
		bid = SystemBlock_DB_ID
	} else if table.schema.Name == SystemColumnSchema.Name {
		bid = SystemBlock_Columns_ID
	} else {
		panic("not supported")
	}
	block := NewSysBlockEntry(e, bid)
	e.AddEntryLocked(block)
	return e
}

func (entry *SegmentEntry) WriteAddonInfo(w io.Writer) (n int64, err error) {
	entry.RLock()
	defer entry.RUnlock()
	if err = binary.Write(w, binary.BigEndian, entry.SortHint); err != nil {
		return
	}
	n += 8
	if err = binary.Write(w, binary.BigEndian, entry.IsLocal); err != nil {
		return
	}
	n += 1
	return
}

func (entry *SegmentEntry) ReadAddonInfo(r io.Reader) (n int64, err error) {
	//
	// called during replay, no need to lock
	//
	if err = binary.Read(r, binary.BigEndian, &entry.SortHint); err != nil {
		return
	}
	n += 8
	if err = binary.Read(r, binary.BigEndian, &entry.IsLocal); err != nil {
		return
	}
	n += 1
	return
}

func (entry *SegmentEntry) AddonInfoStringLocked() string {
	sorted := "US"
	if entry.sorted {
		sorted = "S"
	}
	return fmt.Sprintf("%s/%d/%d", sorted, entry.SortHint, entry.nextObjectIdx)
}

func (entry *SegmentEntry) GetBlockEntryByID(id types.Blockid) (blk *BlockEntry, err error) {
	entry.RLock()
	defer entry.RUnlock()
	return entry.GetBlockEntryByIDLocked(id)
}

// XXX API like this, why do we need the error?   Isn't blk is nil enough?
func (entry *SegmentEntry) GetBlockEntryByIDLocked(id types.Blockid) (blk *BlockEntry, err error) {
	node := entry.entries[id]
	if node == nil {
		err = moerr.GetOkExpectedEOB()
		return
	}
	blk = node.GetPayload()
	return
}

func (entry *SegmentEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := CmdUpdateSegment
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
	return fmt.Sprintf("[%s%s]SEG[%s]", entry.state.Repr(), entry.AddonInfoStringLocked(), id.String())
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
			entry.state.Repr(),
			entry.AddonInfoStringLocked(),
			entry.ID.ToString(),
			entry.GetCreatedAt().ToString(),
			entry.GetDeleteAt().ToString())
	}
	return fmt.Sprintf("[%s-%s]SEG[%s]%s",
		entry.state.Repr(),
		entry.AddonInfoStringLocked(),
		entry.ID.ToString(),
		entry.MetaBaseEntry.StringLocked())
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

func (entry *SegmentEntry) CreateBlock(txn txnif.AsyncTxn, state EntryState, dataFactory BlockDataFactory, opts *common.CreateBlockOpt) (created *BlockEntry, err error) {
	entry.Lock()
	defer entry.Unlock()
	var id types.Blockid
	if opts != nil && opts.Id != nil {
		id = common.NewBlockid(&entry.ID, opts.Id.Filen, opts.Id.Blkn)
		if entry.nextObjectIdx <= opts.Id.Filen {
			entry.nextObjectIdx = opts.Id.Filen + 1
		}
	} else {
		id = common.NewBlockid(&entry.ID, entry.nextObjectIdx, 0)
		entry.nextObjectIdx += 1
	}
	if _, ok := entry.entries[id]; ok {
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

func (entry *SegmentEntry) DropBlockEntry(id types.Blockid, txn txnif.AsyncTxn) (deleted *BlockEntry, err error) {
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
	if objn >= entry.nextObjectIdx {
		entry.nextObjectIdx = objn + 1
	}
	entry.AddEntryLocked(block)
}

func (entry *SegmentEntry) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   entry.GetTable().GetID(),
		SegmentID: entry.ID,
	}
}

func (entry *SegmentEntry) GetCatalog() *Catalog { return entry.table.db.catalog }

func (entry *SegmentEntry) InitData(factory DataFactory) {
	if factory == nil {
		return
	}
	dataFactory := factory.MakeSegmentFactory()
	entry.segData = dataFactory(entry)
}
func (entry *SegmentEntry) GetSegmentData() data.Segment { return entry.segData }

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
	if isEmpty, err = entry.MetaBaseEntry.PrepareRollback(); err != nil {
		return
	}
	if isEmpty {
		if err = entry.GetTable().RemoveEntry(entry); err != nil {
			return
		}
	}
	return
}

func (entry *SegmentEntry) GetScheduler() tasks.TaskScheduler {
	return entry.GetTable().GetCatalog().GetScheduler()
}

func (entry *SegmentEntry) CollectBlockEntries(commitFilter func(be *MetaBaseEntry) bool, blockFilter func(be *BlockEntry) bool) []*BlockEntry {
	blks := make([]*BlockEntry, 0)
	blkIt := entry.MakeBlockIt(true)
	for blkIt.Valid() {
		blk := blkIt.Get().GetPayload()
		blk.RLock()
		if commitFilter != nil && blockFilter != nil {
			if commitFilter(blk.MetaBaseEntry) && blockFilter(blk) {
				blks = append(blks, blk)
			}
		} else if blockFilter != nil {
			if blockFilter(blk) {
				blks = append(blks, blk)
			}
		} else if commitFilter != nil {
			if commitFilter(blk.MetaBaseEntry) {
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
		return db.DBBaseEntry
	}
	if table.HasDropCommittedLocked() {
		return table.TableBaseEntry
	}
	if entry.HasDropCommittedLocked() {
		return entry.MetaBaseEntry
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
