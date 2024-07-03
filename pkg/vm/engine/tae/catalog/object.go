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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type ObjectDataFactory = func(meta *ObjectEntry) data.Object
type TombstoneFactory = func(meta *ObjectEntry) data.Tombstone
type ObjectEntry struct {
	list   *ObjectList
	ID     types.Objectid
	blkCnt int

	CreateNode *MVCCNode[*ObjectMVCCNode]
	DeleteNode *MVCCNode[*ObjectMVCCNode]

	table *TableEntry
	ObjectNode
	objData     data.Object
	ObjectState uint8

	HasPrintedPrepareComapct bool
}

func NewObjectMVCCNode() *MVCCNode[*ObjectMVCCNode] {
	return &MVCCNode[*ObjectMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{},
		BaseNode:      &ObjectMVCCNode{},
		TxnMVCCNode:   &txnbase.TxnMVCCNode{},
	}
}

func (entry *ObjectEntry) ClonePreparedInRange(start, end types.TS) []*MVCCNode[*ObjectMVCCNode] {
	var ret []*MVCCNode[*ObjectMVCCNode]
	if entry.CreateNode != nil {
		in, _ := entry.CreateNode.PreparedIn(start, end)
		if in {
			ret = make([]*MVCCNode[*ObjectMVCCNode], 0)
			ret = append(ret, entry.CreateNode)
		}
	}
	if entry.DeleteNode != nil {
		in, _ := entry.DeleteNode.PreparedIn(start, end)
		if in {
			ret = make([]*MVCCNode[*ObjectMVCCNode], 0)
			ret = append(ret, entry.DeleteNode)
		}
	}
	return ret
}
func (entry *ObjectEntry) GetDeleteAt() types.TS {
	if entry.DeleteNode == nil {
		return types.TS{}
	}
	return entry.DeleteNode.DeletedAt
}
func (entry *ObjectEntry) GetCreatedAt() types.TS {
	if entry.CreateNode == nil {
		panic("logic err")
	}
	return entry.CreateNode.CreatedAt
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
func (entry *ObjectEntry) GetLastMVCCNode() *MVCCNode[*ObjectMVCCNode] {
	if entry.DeleteNode != nil {
		return entry.DeleteNode
	}
	return entry.CreateNode
}
func (entry *ObjectEntry) Clone() *ObjectEntry {
	obj := &ObjectEntry{
		list:       entry.list,
		ID:         entry.ID,
		blkCnt:     entry.blkCnt,
		CreateNode: entry.CreateNode.CloneAll(),
		DeleteNode: entry.DeleteNode.CloneAll(),
		table:      entry.table,
		ObjectNode: ObjectNode{
			state:         entry.state,
			IsLocal:       entry.IsLocal,
			SortHint:      entry.SortHint,
			sorted:        entry.sorted,
			remainingRows: entry.remainingRows,
		},
		objData:                  entry.objData,
		ObjectState:              entry.ObjectState,
		HasPrintedPrepareComapct: entry.HasPrintedPrepareComapct,
	}
	return obj
}
func (entry *ObjectEntry) GetDropEntry(txn txnif.TxnReader) (dropped *ObjectEntry, isNewNode bool) {
	dropped = entry.Clone()
	dropped.ObjectState = ObjectState_Delete_Active
	dropped.DeleteNode = dropped.CreateNode.CloneData()
	dropped.DeleteNode.DeletedAt = txnif.UncommitTS
	dropped.DeleteNode.Txn = txn
	if entry.CreateNode.Txn != nil && txn.GetID() == entry.CreateNode.Txn.GetID() {
		return
	}
	isNewNode = true
	return
}
func (entry *ObjectEntry) GetUpdateEntry(txn txnif.TxnReader, stats *objectio.ObjectStats) (dropped *ObjectEntry, isNewNode bool) {
	dropped = entry.Clone()
	node := dropped.GetLastMVCCNode()
	node.BaseNode.ObjectStats = *stats
	if node.Txn != nil && txn.GetID() == node.Txn.GetID() {
		return
	}
	isNewNode = true
	node.Txn = txn
	return
}

func (entry *ObjectEntry) GetSortedEntry() (sorted *ObjectEntry) {
	sorted = entry.Clone()
	sorted.sorted = true
	return
}
func (entry *ObjectEntry) DeleteBefore(ts types.TS) bool {
	deleteTS := entry.GetDeleteAt()
	if deleteTS.IsEmpty() {
		return false
	}
	return deleteTS.Less(&ts)
}
func (entry *ObjectEntry) GetLatestNode() *ObjectEntry {
	return entry.list.Copy().GetLastestNode(&entry.ID)
}
func (entry *ObjectEntry) ApplyCommit(tid string) error {
	entry.list.Lock()
	defer entry.list.Unlock()
	lastNode := entry.list.GetLastestNode(&entry.ID)
	if lastNode == nil {
		panic("logic error")
	}
	if lastNode.ID != entry.ID {
		panic("logic error")
	}
	var newNode *ObjectEntry
	switch lastNode.ObjectState {
	case ObjectState_Create_PrepareCommit:
		newNode = entry.Clone()
		newNode.ObjectState = ObjectState_Create_ApplyCommit
	case ObjectState_Delete_PrepareCommit:
		newNode = entry.Clone()
		newNode.ObjectState = ObjectState_Create_ApplyCommit
	default:
		panic(fmt.Sprintf("invalid object state %v", lastNode.ObjectState))
	}
	err := newNode.GetLastMVCCNode().ApplyCommit(tid)
	if err != nil {
		return err
	}
	entry.list.Insert(newNode)
	entry.list.Delete(lastNode)
	return nil
}
func (entry *ObjectEntry) ApplyRollback() error { panic("not support") }
func (entry *ObjectEntry) PrepareCommit() error {
	entry.list.Lock()
	defer entry.list.Unlock()
	lastNode := entry.list.GetLastestNode(&entry.ID)
	if lastNode == nil {
		panic("logic error")
	}
	var newNode *ObjectEntry
	switch lastNode.ObjectState {
	case ObjectState_Create_Active:
		newNode = entry.Clone()
		newNode.ObjectState = ObjectState_Create_PrepareCommit
	case ObjectState_Delete_Active:
		newNode = entry.Clone()
		newNode.ObjectState = ObjectState_Delete_PrepareCommit
	default:
		panic(fmt.Sprintf("invalid object state %v", lastNode.ObjectState))
	}
	err := newNode.GetLastMVCCNode().PrepareCommit()
	if err != nil {
		return err
	}
	entry.list.Insert(newNode)
	entry.list.Delete(lastNode)
	return nil
}
func (entry *ObjectEntry) IsDeletesFlushedBefore(ts types.TS) bool {
	tombstone := entry.GetTable().TryGetTombstone(entry.ID)
	if tombstone == nil {
		return true
	}
	persistedTS := tombstone.GetDeltaCommitedTS()
	return persistedTS.Less(&ts)
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
	tombstone := entry.GetTable().TryGetTombstone(entry.ID)
	if tombstone != nil {
		return tombstone.InMemoryDeletesExisted()
	}
	return false
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
		ID:    *id,
		list:  table.link,
		table: table,
		ObjectNode: ObjectNode{
			state:    state,
			SortHint: table.GetDB().catalog.NextObject(),
		},
		CreateNode: &MVCCNode[*ObjectMVCCNode]{
			EntryMVCCNode: &EntryMVCCNode{
				CreatedAt: txnif.UncommitTS,
			},
			TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
			BaseNode:    NewObjectInfoWithObjectID(id),
		},
		ObjectState: ObjectState_Create_Active,
	}
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
		ID:    *id,
		table: table,
		ObjectNode: ObjectNode{
			state:    state,
			sorted:   state == ES_NotAppendable,
			SortHint: table.GetDB().catalog.NextObject(),
		},
		CreateNode: &MVCCNode[*ObjectMVCCNode]{
			EntryMVCCNode: &EntryMVCCNode{
				CreatedAt: end,
			},
			TxnMVCCNode: txnbase.NewTxnMVCCNodeWithStartEnd(start, end),
			BaseNode:    NewObjectInfoWithObjectID(id),
		},
		ObjectState: ObjectState_Create_ApplyCommit,
	}
	if dataFactory != nil {
		e.objData = dataFactory(e)
	}
	return e
}

func NewReplayObjectEntry() *ObjectEntry {
	e := &ObjectEntry{}
	return e
}

func NewStandaloneObject(table *TableEntry, ts types.TS) *ObjectEntry {
	e := &ObjectEntry{
		ID:    *objectio.NewObjectid(),
		list:  table.link,
		table: table,
		ObjectNode: ObjectNode{
			state:   ES_Appendable,
			IsLocal: true,
		},
		CreateNode: &MVCCNode[*ObjectMVCCNode]{
			EntryMVCCNode: &EntryMVCCNode{
				CreatedAt: ts,
			},
			TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTS(ts),
			BaseNode:    &ObjectMVCCNode{*objectio.NewObjectStats()},
		},
		ObjectState: ObjectState_Create_ApplyCommit,
	}
	return e
}

func NewSysObjectEntry(table *TableEntry, id types.Uuid) *ObjectEntry {
	e := &ObjectEntry{
		table: table,
		list:  table.link,
		ObjectNode: ObjectNode{
			state: ES_Appendable,
		},
		CreateNode: &MVCCNode[*ObjectMVCCNode]{
			EntryMVCCNode: &EntryMVCCNode{
				CreatedAt: types.SystemDBTS,
			},
			TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTS(types.SystemDBTS),
			BaseNode:    &ObjectMVCCNode{*objectio.NewObjectStats()},
		},
		ObjectState: ObjectState_Create_ApplyCommit,
	}
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
	node := entry.GetLatestNode()
	location := node.GetLastMVCCNode().BaseNode.ObjectStats.ObjectLocation()
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
	node := entry.GetLatestNode()
	stats = node.GetLastMVCCNode().BaseNode.ObjectStats
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
	panic("not support")
}

func (entry *ObjectEntry) LoadObjectInfoWithTxnTS(startTS types.TS) (objectio.ObjectStats, error) {
	panic("not support")
}

func (entry *ObjectEntry) LoadObjectInfoForLastNode() (stats objectio.ObjectStats, err error) {
	panic("not support")
}

func (entry *ObjectEntry) Less(b *ObjectEntry) bool {
	cmp := bytes.Compare(entry.ID[:], b.ID[:])
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}
	return entry.ObjectState < b.ObjectState
}

func (entry *ObjectEntry) UpdateObjectInfo(txn txnif.TxnReader, stats *objectio.ObjectStats) (isNewNode bool, err error) {
	return entry.list.UpdateObjectInfo(&entry.ID, txn, stats)
}

func (entry *ObjectEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := IOET_WALTxnCommand_Object
	return newObjectCmd(id, cmdType, entry), nil
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
	return entry.StringLocked()
}

func (entry *ObjectEntry) StringWithLevel(level common.PPLevel) string {
	return entry.StringWithLevelLocked(level)
}

func (entry *ObjectEntry) StringWithLevelLocked(level common.PPLevel) string {
	if level <= common.PPL1 {
		return fmt.Sprintf("[%s-%s]OBJ[%s][C@%s,D@%s]",
			entry.state.Repr(), entry.ObjectNode.String(), entry.ID.String(), entry.GetCreatedAt().ToString(), entry.GetDeleteAt().ToString())
	}
	s := fmt.Sprintf("[%s-%s]OBJ[%s]%s%s", entry.state.Repr(), entry.ObjectNode.String(), entry.ID.String(), entry.CreateNode.String())
	if entry.DeleteNode != nil {
		s = fmt.Sprintf("%s -> %s", entry.DeleteNode.String())
	}
	return s
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
	node := entry.GetLatestNode()
	if node == nil {
		return
	}
	if node.GetLastMVCCNode().BaseNode.IsEmpty() {
		return
	}
	return node.GetLastMVCCNode().BaseNode.ObjectStats.BlkCnt()
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
	entry.list.SetSorted(&entry.ID)
}

func (entry *ObjectEntry) IsSorted() bool {
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
func (entry *ObjectEntry) IsCommitted() bool { return entry.GetLastMVCCNode().IsCommitted() }
func (entry *ObjectEntry) GetLatestCommittedNode() *MVCCNode[*ObjectMVCCNode] {
	if entry.DeleteNode != nil && entry.DeleteNode.IsCommitted() {
		return entry.DeleteNode
	}
	if entry.CreateNode.IsCommitted() {
		return entry.CreateNode
	}
	return nil
}
func (entry *ObjectEntry) GetCatalog() *Catalog { return entry.table.db.catalog }

func (entry *ObjectEntry) PrepareRollback() (err error) {
	entry.list.Lock()
	defer entry.list.Unlock()
	lastNode := entry.list.GetLastestNode(&entry.ID)
	if lastNode == nil {
		panic("logic error")
	}
	switch lastNode.ObjectState {
	case ObjectState_Create_Active:
	case ObjectState_Delete_Active:
	default:
		panic(fmt.Sprintf("invalid object state %v", lastNode.ObjectState))
	}
	entry.list.Delete(lastNode)
	return
}

func (entry *ObjectEntry) HasDropCommitted() bool { return entry.GetLastMVCCNode().HasDropCommitted() }

// IsActive is coarse API: no consistency check
func (entry *ObjectEntry) IsActive() bool {
	table := entry.GetTable()
	if !table.IsActive() {
		return false
	}
	return !entry.HasDropCommitted()
}

func (entry *ObjectEntry) TreeMaxDropCommitEntry() (BaseEntry, *ObjectEntry) {
	table := entry.GetTable()
	db := table.GetDB()
	if db.HasDropCommittedLocked() {
		return db.BaseEntryImpl, nil
	}
	if table.HasDropCommittedLocked() {
		return table.BaseEntryImpl, nil
	}
	if entry.HasDropCommitted() {
		return nil, entry
	}
	return nil, nil
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
// Note: Soft deleted nobjects might have in memory deletes to be flushed.
func (entry *ObjectEntry) PrepareCompact() bool {
	return entry.PrepareCompactLocked()
}

func (entry *ObjectEntry) PrepareCompactLocked() bool {
	return entry.GetLatestNode().IsCommitted()
}

func (entry *ObjectEntry) HasDropIntent() bool { return entry.GetLastMVCCNode().HasDropIntent() }

// for old flushed objects, stats may be empty
func (entry *ObjectEntry) ObjectPersisted() bool {
	if entry.IsAppendable() {
		lastNode := entry.GetLatestNode()
		return lastNode.HasDropIntent()
	} else {
		return true
	}
}

// PXU TODO: I can't understand this code
// aobj has persisted data after it is dropped
// obj always has persisted data
func (entry *ObjectEntry) HasCommittedPersistedData() bool {
	lastNode := entry.GetLatestNode()
	if lastNode == nil {
		return false
	}
	if entry.IsAppendable() {
		return lastNode.HasDropCommitted()
	} else {
		return entry.IsCommitted()
	}
}
func (entry *ObjectEntry) MustGetObjectStats() (objectio.ObjectStats, error) {
	return entry.GetObjectStats(), nil
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

func (entry *ObjectEntry) CheckPrintPrepareCompact() bool {
	return entry.CheckPrintPrepareCompactLocked()
}

func (entry *ObjectEntry) CheckPrintPrepareCompactLocked() bool {
	lastNode := entry.GetLatestNode()
	startTS := lastNode.GetLastMVCCNode().GetStart()
	return startTS.Physical() < time.Now().UTC().UnixNano()-(time.Minute*30).Nanoseconds()
}

func (entry *ObjectEntry) PrintPrepareCompactDebugLog() {
	if entry.HasPrintedPrepareComapct {
		return
	}
	entry.HasPrintedPrepareComapct = true
	s := fmt.Sprintf("prepare compact failed, obj %v", entry.PPString(3, 0, ""))
	lastNode := entry.GetLatestNode().GetLastMVCCNode()
	startTS := lastNode.GetStart()
	if lastNode.Txn != nil {
		s = fmt.Sprintf("%s txn is %x.", s, lastNode.Txn.GetID())
	}
	it := entry.GetTable().MakeObjectIt(false)
	for it.Next() {
		obj := it.Item()
		if obj.CreateNode.Start.Equal(&startTS) || (obj.DeleteNode != nil && obj.DeleteNode.Start.Equal(&startTS)) {
			s = fmt.Sprintf("%s %v.", s, obj.PPString(3, 0, ""))
		}
	}
	logutil.Infof(s)
}

func MockObjEntryWithTbl(tbl *TableEntry, size uint64) *ObjectEntry {
	stats := objectio.NewObjectStats()
	objectio.SetObjectStatsSize(stats, uint32(size))
	// to make sure pass the stats empty check
	objectio.SetObjectStatsRowCnt(stats, uint32(1))
	ts := types.BuildTS(time.Now().UnixNano(), 0)
	e := &ObjectEntry{
		list:       tbl.link,
		table:      tbl,
		ObjectNode: ObjectNode{},
		CreateNode: &MVCCNode[*ObjectMVCCNode]{
			EntryMVCCNode: &EntryMVCCNode{
				CreatedAt: ts,
			},
			TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTS(ts),
			BaseNode:    &ObjectMVCCNode{*stats},
		},
		ObjectState: ObjectState_Create_ApplyCommit,
	}
	return e
}
