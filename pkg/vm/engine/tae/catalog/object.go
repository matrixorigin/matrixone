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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type ObjectEntry struct {
	ID   types.Objectid
	Stat ObjStat
	*BaseEntryImpl[*ObjectMVCCNode]
	table   *TableEntry
	entries map[types.Blockid]*common.GenericDLNode[*BlockEntry]
	//link.head and tail is nil when new a ObjectEntry object.
	link *common.GenericSortedDList[*BlockEntry]
	*ObjectNode
}

type ObjStat struct {
	// min max etc. later
	entry         *ObjectEntry
	remainingRows int
}

func (s *ObjStat) GetLoaded() bool {
	return s.GetRows() != 0
}

func (s *ObjStat) GetSortKeyZonemap() index.ZM {
	res := s.entry.GetObjectStats()
	return res.SortKeyZoneMap()
}

func (s *ObjStat) SetRows(rows int) {}

func (s *ObjStat) SetRemainingRows(rows int) {
	s.remainingRows = rows
}

func (s *ObjStat) GetRemainingRows() int {
	return s.remainingRows
}

func (s *ObjStat) GetRows() int {
	res := s.entry.GetObjectStats()
	return int(res.Rows())
}

func (s *ObjStat) GetOriginSize() int {
	res := s.entry.GetObjectStats()
	return int(res.OriginSize())
}

func (s *ObjStat) GetCompSize() int {
	res := s.entry.GetObjectStats()
	return int(res.Size())
}

func (s *ObjStat) String(composeSortKey bool) string {
	zonemapStr := "nil"
	if z := s.GetSortKeyZonemap(); z != nil {
		if composeSortKey {
			zonemapStr = z.StringForCompose()
		} else {
			zonemapStr = z.String()
		}
	}
	return fmt.Sprintf(
		"loaded:%t, oSize:%s, cSzie:%s rows:%d, remainingRows:%d, zm: %s",
		s.GetLoaded(),
		common.HumanReadableBytes(s.GetOriginSize()),
		common.HumanReadableBytes(s.GetCompSize()),
		s.GetRows(),
		s.remainingRows,
		zonemapStr,
	)
}

func NewObjectEntry(table *TableEntry, id *objectio.ObjectId, txn txnif.AsyncTxn, state EntryState) *ObjectEntry {
	e := &ObjectEntry{
		ID: *id,
		BaseEntryImpl: NewBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{*objectio.NewObjectStats()} }),
		table:   table,
		link:    common.NewGenericSortedDList((*BlockEntry).Less),
		entries: make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
		ObjectNode: &ObjectNode{
			state:    state,
			SortHint: table.GetDB().catalog.NextObject(),
		},
	}
	e.CreateWithTxn(txn, NewObjectInfoWithObjectID(id))
	e.Stat.entry = e
	return e
}

func NewObjectEntryByMetaLocation(table *TableEntry, id *objectio.ObjectId, start, end types.TS, state EntryState, metalocation objectio.Location) *ObjectEntry {
	e := &ObjectEntry{
		ID: *id,
		BaseEntryImpl: NewBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{*objectio.NewObjectStats()} }),
		table:   table,
		link:    common.NewGenericSortedDList((*BlockEntry).Less),
		entries: make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
		ObjectNode: &ObjectNode{
			state:    state,
			sorted:   state == ES_NotAppendable,
			SortHint: table.GetDB().catalog.NextObject(),
		},
	}
	e.CreateWithStartAndEnd(start, end, NewObjectInfoWithMetaLocation(metalocation, id))
	e.Stat.entry = e
	return e
}

func NewReplayObjectEntry() *ObjectEntry {
	e := &ObjectEntry{
		BaseEntryImpl: NewReplayBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{*objectio.NewObjectStats()} }),
		link:    common.NewGenericSortedDList((*BlockEntry).Less),
		entries: make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
	}
	e.Stat.entry = e
	return e
}

func NewStandaloneObject(table *TableEntry, ts types.TS) *ObjectEntry {
	e := &ObjectEntry{
		ID: *objectio.NewObjectid(),
		BaseEntryImpl: NewBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{*objectio.NewObjectStats()} }),
		table:   table,
		link:    common.NewGenericSortedDList((*BlockEntry).Less),
		entries: make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
		ObjectNode: &ObjectNode{
			state:   ES_Appendable,
			IsLocal: true,
		},
	}
	e.CreateWithTS(ts, &ObjectMVCCNode{*objectio.NewObjectStats()})
	e.Stat.entry = e
	return e
}

func NewSysObjectEntry(table *TableEntry, id types.Uuid) *ObjectEntry {
	e := &ObjectEntry{
		BaseEntryImpl: NewBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{*objectio.NewObjectStats()} }),
		table:   table,
		link:    common.NewGenericSortedDList((*BlockEntry).Less),
		entries: make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
		ObjectNode: &ObjectNode{
			state: ES_Appendable,
		},
	}
	e.CreateWithTS(types.SystemDBTS, &ObjectMVCCNode{*objectio.NewObjectStats()})
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
	e.ID = *bid.Object()
	block := NewSysBlockEntry(e, bid)
	e.AddEntryLocked(block)
	e.Stat.entry = e
	return e
}

func (entry *ObjectEntry) GetObjectStats() (stats objectio.ObjectStats) {
	entry.RLock()
	defer entry.RUnlock()
	entry.LoopChain(func(node *MVCCNode[*ObjectMVCCNode]) bool {
		if !node.BaseNode.IsEmpty() {
			stats = node.BaseNode.ObjectStats
			return false
		}
		return true
	})
	return
}

func (entry *ObjectEntry) CheckAndLoad() error {
	s := entry.GetObjectStats()
	if s.Rows() == 0 {
		ins := time.Now()
		defer func() {
			v2.GetObjectStatsDurationHistogram.Observe(time.Since(ins).Seconds())
		}()
		_, err := entry.LoadObjectInfoForLastNode()
		// logutil.Infof("yyyyyy loaded %v %v", common.ShortObjId(entry.ID), err)
		return err
	}
	return nil
}

func (entry *ObjectEntry) NeedPrefetchObjectMetaForObjectInfo(nodes []*MVCCNode[*ObjectMVCCNode]) (needPrefetch bool, blk *BlockEntry) {
	lastNode := nodes[0]
	for _, n := range nodes {
		if n.Start.Greater(lastNode.Start) {
			lastNode = n
		}
	}
	if !lastNode.BaseNode.IsEmpty() {
		return
	}
	blk = entry.GetFirstBlkEntry()
	// for gc in test
	if blk == nil {
		return false, nil
	}
	blk.RLock()
	node := blk.SearchNode(&MVCCNode[*MetadataMVCCNode]{
		TxnMVCCNode: &txnbase.TxnMVCCNode{Start: lastNode.Start},
	})
	blk.RUnlock()
	// in some unit tests, node doesn't exist
	if node == nil || node.BaseNode.MetaLoc == nil || node.BaseNode.MetaLoc.IsEmpty() {
		return
	}

	needPrefetch = true
	return
}

func (entry *ObjectEntry) SetObjectStatsForPreviousNode(nodes []*MVCCNode[*ObjectMVCCNode]) {
	if entry.IsAppendable() || len(nodes) <= 1 {
		return
	}
	lastNode := nodes[0]
	for _, n := range nodes {
		if n.Start.Greater(lastNode.Start) {
			lastNode = n
		}
	}
	stat := lastNode.BaseNode.ObjectStats
	if lastNode.BaseNode.IsEmpty() {
		panic(fmt.Sprintf("logic error: last node is empty, object %v", entry.PPString(3, 0, "")))
	}
	entry.Lock()
	for _, n := range nodes {
		if n.BaseNode.IsEmpty() {
			n.BaseNode.ObjectStats = *stat.Clone()
		}
	}
	entry.Unlock()
}
func (entry *ObjectEntry) LoadObjectInfoWithTxnTS(startTS types.TS) (objectio.ObjectStats, error) {
	stats := *objectio.NewObjectStats()
	blk := entry.GetFirstBlkEntry()
	// for gc in test
	if blk == nil {
		return stats, nil
	}
	blk.RLock()
	node := blk.SearchNode(&MVCCNode[*MetadataMVCCNode]{
		TxnMVCCNode: &txnbase.TxnMVCCNode{Start: startTS},
	})
	if node.BaseNode.MetaLoc == nil || node.BaseNode.MetaLoc.IsEmpty() {
		blk.RUnlock()
		objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectNameWithObjectID(&entry.ID))
		return stats, nil
	}
	blk.RUnlock()

	entry.RLock()
	entry.LoopChain(func(n *MVCCNode[*ObjectMVCCNode]) bool {
		if !n.BaseNode.IsEmpty() {
			stats = *n.BaseNode.ObjectStats.Clone()
			return false
		}
		return true
	})
	entry.RUnlock()
	if stats.Rows() != 0 {
		return stats, nil
	}
	metaLoc := node.BaseNode.MetaLoc

	objMeta, err := objectio.FastLoadObjectMeta(context.Background(), &metaLoc, false, blk.blkData.GetFs().Service)
	if err != nil {
		return *objectio.NewObjectStats(), err
	}
	objectio.SetObjectStatsObjectName(&stats, metaLoc.Name())
	objectio.SetObjectStatsExtent(&stats, metaLoc.Extent())
	objectDataMeta := objMeta.MustDataMeta()
	objectio.SetObjectStatsRowCnt(&stats, objectDataMeta.BlockHeader().Rows())
	objectio.SetObjectStatsBlkCnt(&stats, objectDataMeta.BlockCount())
	objectio.SetObjectStatsSize(&stats, metaLoc.Extent().End()+objectio.FooterSize)
	schema := entry.table.schema.Load()
	originSize := uint32(0)
	for _, col := range schema.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		colmata := objectDataMeta.MustGetColumn(uint16(col.SeqNum))
		originSize += colmata.Location().OriginSize()
	}
	objectio.SetObjectStatsOriginSize(&stats, originSize)
	if schema.HasSortKey() {
		col := schema.GetSingleSortKey()
		objectio.SetObjectStatsSortKeyZoneMap(&stats, objectDataMeta.MustGetColumn(col.SeqNum).ZoneMap())
	}
	return stats, nil
}

func (entry *ObjectEntry) LoadObjectInfoForLastNode() (stats objectio.ObjectStats, err error) {
	entry.RLock()
	startTS := entry.GetLatestCommittedNode().Start
	entry.RUnlock()

	stats, err = entry.LoadObjectInfoWithTxnTS(startTS)
	if err == nil {
		entry.Lock()
		entry.GetLatestNodeLocked().BaseNode.ObjectStats = stats
		entry.Unlock()
	}
	return stats, nil
}

// for test
func (entry *ObjectEntry) GetInMemoryObjectInfo() *ObjectMVCCNode {
	return entry.BaseEntryImpl.GetLatestCommittedNode().BaseNode
}
func (entry *ObjectEntry) GetFirstBlkEntry() *BlockEntry {
	entry.RLock()
	defer entry.RUnlock()

	// head may be nil
	head := entry.link.GetHead()
	if head == nil {
		return nil
	}

	return head.GetPayload()
}

func (entry *ObjectEntry) Less(b *ObjectEntry) int {
	if entry.SortHint < b.SortHint {
		return -1
	} else if entry.SortHint > b.SortHint {
		return 1
	}
	return 0
}

func (entry *ObjectEntry) GetBlockEntryByID(id *objectio.Blockid) (blk *BlockEntry, err error) {
	entry.RLock()
	defer entry.RUnlock()
	return entry.GetBlockEntryByIDLocked(id)
}

func (entry *ObjectEntry) UpdateObjectInfo(txn txnif.TxnReader, stats *objectio.ObjectStats) (isNewNode bool, err error) {
	entry.Lock()
	defer entry.Unlock()
	needWait, txnToWait := entry.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		entry.Unlock()
		txnToWait.GetTxnState(true)
		entry.Lock()
	}
	err = entry.CheckConflict(txn)
	if err != nil {
		return
	}
	baseNode := NewObjectInfoWithObjectStats(stats)
	var node *MVCCNode[*ObjectMVCCNode]
	isNewNode, node = entry.getOrSetUpdateNode(txn)
	node.BaseNode.Update(baseNode)
	return
}

// XXX API like this, why do we need the error?   Isn't blk is nil enough?
func (entry *ObjectEntry) GetBlockEntryByIDLocked(id *objectio.Blockid) (blk *BlockEntry, err error) {
	node := entry.entries[*id]
	if node == nil {
		err = moerr.GetOkExpectedEOB()
		return
	}
	blk = node.GetPayload()
	return
}

func (entry *ObjectEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := IOET_WALTxnCommand_Object
	entry.RLock()
	defer entry.RUnlock()
	return newObjectCmd(id, cmdType, entry), nil
}

func (entry *ObjectEntry) Set1PC() {
	entry.GetLatestNodeLocked().Set1PC()
}
func (entry *ObjectEntry) Is1PC() bool {
	return entry.GetLatestNodeLocked().Is1PC()
}
func (entry *ObjectEntry) PPString(level common.PPLevel, depth int, prefix string) string {
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

func (entry *ObjectEntry) StringLocked() string {
	return entry.StringWithLevelLocked(common.PPL1)
}

func (entry *ObjectEntry) Repr() string {
	id := entry.AsCommonID()
	return fmt.Sprintf("[%s%s]OBJ[%s]", entry.state.Repr(), entry.ObjectNode.String(), id.String())
}

func (entry *ObjectEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
}

func (entry *ObjectEntry) StringWithLevel(level common.PPLevel) string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringWithLevelLocked(level)
}

func (entry *ObjectEntry) StringWithLevelLocked(level common.PPLevel) string {
	if level <= common.PPL1 {
		return fmt.Sprintf("[%s-%s]OBJ[%s][C@%s,D@%s]",
			entry.state.Repr(), entry.ObjectNode.String(), entry.ID.String(), entry.GetCreatedAtLocked().ToString(), entry.GetDeleteAt().ToString())
	}
	return fmt.Sprintf("[%s-%s]OBJ[%s]%s", entry.state.Repr(), entry.ObjectNode.String(), entry.ID.String(), entry.BaseEntryImpl.StringLocked())
}

func (entry *ObjectEntry) BlockCnt() int {
	return len(entry.entries)
}

func (entry *ObjectEntry) IsAppendable() bool {
	return entry.state == ES_Appendable
}

func (entry *ObjectEntry) SetSorted() {
	// modifing Object interface to supporte a borned sorted obj is verbose
	// use Lock instead, the contention won't be intense
	entry.Lock()
	defer entry.Unlock()
	entry.sorted = true
}

func (entry *ObjectEntry) IsSorted() bool {
	entry.RLock()
	defer entry.RUnlock()
	return entry.sorted
}

func (entry *ObjectEntry) GetTable() *TableEntry {
	return entry.table
}

func (entry *ObjectEntry) GetAppendableBlockCnt() int {
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

// GetNonAppendableBlockCnt Non-appendable Object only can contain non-appendable blocks;
// Appendable Object can contain both of appendable blocks and non-appendable blocks
func (entry *ObjectEntry) GetNonAppendableBlockCnt() int {
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

func (entry *ObjectEntry) GetAppendableBlock() (blk *BlockEntry) {
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
func (entry *ObjectEntry) LastAppendableBlock() (blk *BlockEntry) {
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

func (entry *ObjectEntry) GetNextObjectIndex() uint16 {
	entry.RLock()
	defer entry.RUnlock()
	return entry.nextObjectIdx
}

func (entry *ObjectEntry) CreateBlock(
	txn txnif.AsyncTxn,
	state EntryState,
	dataFactory BlockDataFactory,
	opts *objectio.CreateBlockOpt) (created *BlockEntry, err error) {
	entry.Lock()
	defer entry.Unlock()
	var id *objectio.Blockid
	if entry.IsAppendable() && len(entry.entries) >= 1 {
		panic("Logic error. Appendable Object has as most one block.")
	}
	if opts != nil && opts.Id != nil {
		id = objectio.NewBlockidWithObjectID(&entry.ID, opts.Id.Blkn)
		if entry.nextObjectIdx <= opts.Id.Filen {
			entry.nextObjectIdx = opts.Id.Filen + 1
		}
	} else {
		id = objectio.NewBlockidWithObjectID(&entry.ID, 0)
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

func (entry *ObjectEntry) DropBlockEntry(id *objectio.Blockid, txn txnif.AsyncTxn) (deleted *BlockEntry, err error) {
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

func (entry *ObjectEntry) MakeBlockIt(reverse bool) *common.GenericSortedDListIt[*BlockEntry] {
	entry.RLock()
	defer entry.RUnlock()
	return common.NewGenericSortedDListIt(entry.RWMutex, entry.link, reverse)
}

func (entry *ObjectEntry) AddEntryLocked(block *BlockEntry) {
	n := entry.link.Insert(block)
	entry.entries[block.ID] = n
}

func (entry *ObjectEntry) ReplayAddEntryLocked(block *BlockEntry) {
	// bump object idx during replaying.
	objn, _ := block.ID.Offsets()
	entry.replayNextObjectIdx(objn)
	entry.AddEntryLocked(block)
}

func (entry *ObjectEntry) replayNextObjectIdx(objn uint16) {
	if objn >= entry.nextObjectIdx {
		entry.nextObjectIdx = objn + 1
	}
}

func (entry *ObjectEntry) AsCommonID() *common.ID {
	id := &common.ID{
		DbID:    entry.GetTable().GetDB().ID,
		TableID: entry.GetTable().ID,
	}
	id.SetObjectID(&entry.ID)
	return id
}

func (entry *ObjectEntry) GetCatalog() *Catalog { return entry.table.db.catalog }

func (entry *ObjectEntry) deleteEntryLocked(block *BlockEntry) error {
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

func (entry *ObjectEntry) RemoveEntry(block *BlockEntry) (err error) {
	logutil.Debug("[Catalog]", common.OperationField("remove"),
		common.OperandField(block.String()))
	entry.Lock()
	defer entry.Unlock()
	return entry.deleteEntryLocked(block)
}

func (entry *ObjectEntry) PrepareRollback() (err error) {
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

func (entry *ObjectEntry) CollectBlockEntries(commitFilter func(be *BaseEntryImpl[*MetadataMVCCNode]) bool, blockFilter func(be *BlockEntry) bool) []*BlockEntry {
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
func (entry *ObjectEntry) IsActive() bool {
	table := entry.GetTable()
	if !table.IsActive() {
		return false
	}
	return !entry.HasDropCommitted()
}

func (entry *ObjectEntry) TreeMaxDropCommitEntry() BaseEntry {
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
func (entry *ObjectEntry) GetTerminationTS() (ts types.TS, terminated bool) {
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

func MockObjEntryWithTbl(tbl *TableEntry, size uint64) *ObjectEntry {
	stats := objectio.NewObjectStats()
	objectio.SetObjectStatsSize(stats, uint32(size))
	// to make sure pass the stats empty check
	objectio.SetObjectStatsRowCnt(stats, uint32(1))

	e := &ObjectEntry{
		BaseEntryImpl: NewBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{*objectio.NewObjectStats()} }),
		table:      tbl,
		link:       common.NewGenericSortedDList((*BlockEntry).Less),
		entries:    make(map[types.Blockid]*common.GenericDLNode[*BlockEntry]),
		ObjectNode: &ObjectNode{},
	}
	e.CreateWithTS(types.BuildTS(time.Now().UnixNano(), 0), &ObjectMVCCNode{*stats})
	e.Stat.entry = e
	return e
}
