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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type ObjectDataFactory = func(meta *ObjectEntry) data.Object
type TombstoneFactory = func(meta *ObjectEntry) data.Tombstone
type ObjectEntry struct {
	ID     types.Objectid
	blkCnt int
	*BaseEntryImpl[*ObjectMVCCNode]
	table *TableEntry
	*ObjectNode
	objData data.Object
}

func (entry *ObjectEntry) GetLoaded() bool {
	stats := entry.GetObjectStats()
	return stats.Rows() != 0
}

func (entry *ObjectEntry) GetSortKeyZonemap() index.ZM {
	stats := entry.GetObjectStats()
	return stats.SortKeyZoneMap()
}

func (entry *ObjectEntry) SetRemainingRows(rows int) {
	entry.remainingRows.Append(rows)
}

func (entry *ObjectEntry) GetRemainingRows() int {
	return entry.remainingRows.V()
}

func (entry *ObjectEntry) GetRows() int {
	stats := entry.GetObjectStats()
	return int(stats.Rows())
}

func (entry *ObjectEntry) GetOriginSize() int {
	stats := entry.GetObjectStats()
	return int(stats.OriginSize())
}

func (entry *ObjectEntry) GetCompSize() int {
	stats := entry.GetObjectStats()
	return int(stats.Size())
}

func (entry *ObjectEntry) StatsString(zonemapKind common.ZonemapPrintKind) string {
	zonemapStr := "nil"
	if z := entry.GetSortKeyZonemap(); z != nil {
		switch zonemapKind {
		case common.ZonemapPrintKindNormal:
			zonemapStr = z.String()
		case common.ZonemapPrintKindCompose:
			zonemapStr = z.StringForCompose()
		case common.ZonemapPrintKindHex:
			zonemapStr = z.StringForHex()
		}
	}
	return fmt.Sprintf(
		"loaded:%t, oSize:%s, cSzie:%s rows:%d, remainingRows:%d, zm: %s",
		entry.GetLoaded(),
		common.HumanReadableBytes(entry.GetOriginSize()),
		common.HumanReadableBytes(entry.GetCompSize()),
		entry.GetRows(),
		entry.remainingRows.V(),
		zonemapStr,
	)
}

func (entry *ObjectEntry) InMemoryDeletesExisted() bool {
	entry.RLock()
	defer entry.RUnlock()
	return entry.InMemoryDeletesExistedLocked()
}

func (entry *ObjectEntry) InMemoryDeletesExistedLocked() bool {
	tombstone := entry.GetTable().TryGetTombstone(entry.ID)
	if tombstone != nil {
		return tombstone.InMemoryDeletesExistedLocked()
	}
	return false
}
func NewObjectEntry(
	table *TableEntry,
	id *objectio.ObjectId,
	txn txnif.AsyncTxn,
	state EntryState,
	dataFactory ObjectDataFactory,
) *ObjectEntry {
	e := &ObjectEntry{
		ID: *id,
		BaseEntryImpl: NewBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{*objectio.NewObjectStats()} }),
		table: table,
		ObjectNode: &ObjectNode{
			state:    state,
			SortHint: table.GetDB().catalog.NextObject(),
		},
	}
	e.CreateWithTxn(txn, NewObjectInfoWithObjectID(id))
	if dataFactory != nil {
		e.objData = dataFactory(e)
	}
	return e
}

func NewObjectEntryByMetaLocation(
	table *TableEntry,
	id *objectio.ObjectId,
	start, end types.TS,
	state EntryState,
	metalocation objectio.Location,
	dataFactory ObjectDataFactory,
) *ObjectEntry {
	e := &ObjectEntry{
		ID: *id,
		BaseEntryImpl: NewBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{*objectio.NewObjectStats()} }),
		table: table,
		ObjectNode: &ObjectNode{
			state:    state,
			sorted:   state == ES_NotAppendable,
			SortHint: table.GetDB().catalog.NextObject(),
		},
	}
	e.CreateWithStartAndEnd(start, end, NewObjectInfoWithMetaLocation(metalocation, id))
	if dataFactory != nil {
		e.objData = dataFactory(e)
	}
	return e
}

func NewReplayObjectEntry() *ObjectEntry {
	e := &ObjectEntry{
		BaseEntryImpl: NewReplayBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{*objectio.NewObjectStats()} }),
	}
	return e
}

func NewStandaloneObject(table *TableEntry, ts types.TS) *ObjectEntry {
	e := &ObjectEntry{
		ID: *objectio.NewObjectid(),
		BaseEntryImpl: NewBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{*objectio.NewObjectStats()} }),
		table: table,
		ObjectNode: &ObjectNode{
			state:   ES_Appendable,
			IsLocal: true,
		},
	}
	e.CreateWithTS(ts, &ObjectMVCCNode{*objectio.NewObjectStats()})
	return e
}

func NewSysObjectEntry(table *TableEntry, id types.Uuid) *ObjectEntry {
	e := &ObjectEntry{
		BaseEntryImpl: NewBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{*objectio.NewObjectStats()} }),
		table: table,
		ObjectNode: &ObjectNode{
			state: ES_Appendable,
		},
	}
	e.CreateWithTS(types.SystemDBTS, &ObjectMVCCNode{*objectio.NewObjectStats()})
	var bid types.Blockid
	schema := table.GetLastestSchemaLocked()
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
	return e
}

func (entry *ObjectEntry) GetLocation() objectio.Location {
	entry.RLock()
	defer entry.RUnlock()
	node := entry.GetLatestNodeLocked()
	location := node.BaseNode.ObjectStats.ObjectLocation()
	return location
}
func (entry *ObjectEntry) InitData(factory DataFactory) {
	if factory == nil {
		return
	}
	dataFactory := factory.MakeObjectFactory()
	entry.objData = dataFactory(entry)
}
func (entry *ObjectEntry) HasPersistedData() bool {
	return entry.ObjectPersisted()
}
func (entry *ObjectEntry) GetObjectData() data.Object { return entry.objData }
func (entry *ObjectEntry) GetObjectStats() (stats objectio.ObjectStats) {
	entry.RLock()
	defer entry.RUnlock()
	entry.LoopChainLocked(func(node *MVCCNode[*ObjectMVCCNode]) bool {
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

func (entry *ObjectEntry) NeedPrefetchObjectMetaForObjectInfo(nodes []*MVCCNode[*ObjectMVCCNode]) (needPrefetch bool) {
	lastNode := nodes[0]
	for _, n := range nodes {
		if n.Start.Greater(&lastNode.Start) {
			lastNode = n
		}
	}
	if !lastNode.BaseNode.IsEmpty() {
		return
	}
	if entry.nodeHasPersistedData(lastNode) {
		needPrefetch = true
	}

	return
}
func (entry *ObjectEntry) nodeHasPersistedData(node *MVCCNode[*ObjectMVCCNode]) bool {
	if !entry.IsAppendable() {
		return true
	}
	return node.HasDropCommitted()
}
func (entry *ObjectEntry) SetObjectStatsForPreviousNode(nodes []*MVCCNode[*ObjectMVCCNode]) {
	if entry.IsAppendable() || len(nodes) <= 1 {
		return
	}
	lastNode := nodes[0]
	for _, n := range nodes {
		if n.Start.Greater(&lastNode.Start) {
			lastNode = n
		}
	}
	stat := lastNode.BaseNode.ObjectStats
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

	entry.RLock()
	entry.LoopChainLocked(func(n *MVCCNode[*ObjectMVCCNode]) bool {
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
	metaLoc := entry.GetLatestCommittedNodeLocked().BaseNode.ObjectStats.ObjectLocation()

	objMeta, err := objectio.FastLoadObjectMeta(context.Background(), &metaLoc, false, entry.objData.GetFs().Service)
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
	startTS := entry.GetLatestCommittedNodeLocked().Start
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
	return entry.BaseEntryImpl.GetLatestCommittedNodeLocked().BaseNode
}

func (entry *ObjectEntry) Less(b *ObjectEntry) int {
	if entry.SortHint < b.SortHint {
		return -1
	} else if entry.SortHint > b.SortHint {
		return 1
	}
	return 0
}

func (entry *ObjectEntry) UpdateObjectInfo(txn txnif.TxnReader, stats *objectio.ObjectStats) (isNewNode bool, err error) {
	entry.Lock()
	defer entry.Unlock()
	needWait, txnToWait := entry.NeedWaitCommittingLocked(txn.GetStartTS())
	if needWait {
		entry.Unlock()
		txnToWait.GetTxnState(true)
		entry.Lock()
	}
	err = entry.CheckConflictLocked(txn)
	if err != nil {
		return
	}
	baseNode := NewObjectInfoWithObjectStats(stats)
	var node *MVCCNode[*ObjectMVCCNode]
	isNewNode, node = entry.getOrSetUpdateNodeLocked(txn)
	node.BaseNode.Update(baseNode)
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
	return w.String()
}
func (entry *ObjectEntry) PPStringLocked(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.StringWithLevelLocked(level)))
	if level == common.PPL0 {
		return w.String()
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
			entry.state.Repr(), entry.ObjectNode.String(), entry.ID.String(), entry.GetCreatedAtLocked().ToString(), entry.GetDeleteAtLocked().ToString())
	}
	return fmt.Sprintf("[%s-%s]OBJ[%s]%s", entry.state.Repr(), entry.ObjectNode.String(), entry.ID.String(), entry.BaseEntryImpl.StringLocked())
}

func (entry *ObjectEntry) BlockCnt() int {
	if entry.IsAppendable() {
		return 1
	}
	cnt := entry.getBlockCntFromStats()
	if cnt != 0 {
		return int(cnt)
	}
	return entry.blkCnt
}

func (entry *ObjectEntry) getBlockCntFromStats() (blkCnt uint32) {
	entry.RLock()
	defer entry.RUnlock()
	node := entry.GetLatestNodeLocked()
	if node == nil {
		return
	}
	if node.BaseNode.IsEmpty() {
		return
	}
	return node.BaseNode.ObjectStats.BlkCnt()
}

func (entry *ObjectEntry) tryUpdateBlockCnt(cnt int) {
	if entry.blkCnt < cnt {
		entry.blkCnt = cnt
	}
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

// GetNonAppendableBlockCnt Non-appendable Object only can contain non-appendable blocks;
// Appendable Object can contain both of appendable blocks and non-appendable blocks
func (entry *ObjectEntry) GetNonAppendableBlockCnt() int {
	return entry.blkCnt
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
	terminated, ts = dbEntry.TryGetTerminatedTSLocked(true)
	if terminated {
		dbEntry.RUnlock()
		return
	}
	dbEntry.RUnlock()

	terminated, ts = tableEntry.TryGetTerminatedTS(true)
	return
}

func (entry *ObjectEntry) GetSchema() *Schema {
	return entry.table.GetLastestSchema()
}
func (entry *ObjectEntry) GetSchemaLocked() *Schema {
	return entry.table.GetLastestSchemaLocked()
}

// PrepareCompact is performance insensitive
// a block can be compacted:
// 1. no uncommited node
// 2. at least one committed node
func (entry *ObjectEntry) PrepareCompact() bool {
	entry.RLock()
	defer entry.RUnlock()
	return entry.PrepareCompactLocked()
}

func (entry *ObjectEntry) PrepareCompactLocked() bool {
	if entry.HasUncommittedNodeLocked() {
		return false
	}
	if !entry.HasCommittedNodeLocked() {
		return false
	}
	return true
}

// for old flushed objects, stats may be empty
func (entry *ObjectEntry) ObjectPersisted() bool {
	entry.RLock()
	defer entry.RUnlock()
	if entry.IsEmptyLocked() {
		return false
	}
	if entry.IsAppendable() {
		lastNode := entry.GetLatestNodeLocked()
		return lastNode.HasDropIntent()
	} else {
		return true
	}
}

// PXU TODO: I can't understand this code
// aobj has persisted data after it is dropped
// obj always has persisted data
func (entry *ObjectEntry) HasCommittedPersistedData() bool {
	entry.RLock()
	defer entry.RUnlock()
	if entry.IsAppendable() {
		lastNode := entry.GetLatestNodeLocked()
		return lastNode.HasDropCommitted()
	} else {
		return entry.HasCommittedNodeLocked()
	}
}
func (entry *ObjectEntry) MustGetObjectStats() (objectio.ObjectStats, error) {
	entry.RLock()
	baseNode := entry.GetLatestNodeLocked().BaseNode
	entry.RUnlock()
	if baseNode.IsEmpty() {
		return entry.LoadObjectInfoForLastNode()
	}
	return baseNode.ObjectStats, nil
}

func (entry *ObjectEntry) GetPKZoneMap(
	ctx context.Context,
	fs fileservice.FileService,
) (zm index.ZM, err error) {
	stats, err := entry.MustGetObjectStats()
	if err != nil {
		return
	}
	return stats.SortKeyZoneMap(), nil
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
		ObjectNode: &ObjectNode{},
	}
	e.CreateWithTS(types.BuildTS(time.Now().UnixNano(), 0), &ObjectMVCCNode{*stats})
	return e
}
