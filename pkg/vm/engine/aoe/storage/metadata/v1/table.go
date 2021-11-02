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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
)

type tableLogEntry struct {
	*BaseEntry
	Prev    *Table
	Catalog *Catalog `json:"-"`
}

func (e *tableLogEntry) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *tableLogEntry) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, e)
}

func (e *tableLogEntry) ToEntry() *Table {
	e.BaseEntry.CommitInfo.SetNext(e.Prev.CommitInfo)
	e.Prev.BaseEntry = e.BaseEntry
	return e.Prev
}

// func createTableHandle(r io.Reader, meta *LogEntryMeta) (LogEntry, int64, error) {
// 	entry := Table{}
// 	logEntry
// 	// entry.Unmarshal()

// }

type Table struct {
	*BaseEntry
	Schema     *Schema        `json:"schema"`
	SegmentSet []*Segment     `json:"segments"`
	IdIndex    map[uint64]int `json:"-"`
	Catalog    *Catalog       `json:"-"`
	FlushTS    int64          `json:"-"`
}

func NewTableEntry(catalog *Catalog, schema *Schema, tranId uint64, exIndex *LogIndex) *Table {
	schema.BlockMaxRows = catalog.Cfg.BlockMaxRows
	schema.SegmentMaxBlocks = catalog.Cfg.SegmentMaxBlocks
	e := &Table{
		BaseEntry: &BaseEntry{
			Id: catalog.NextTableId(),
			CommitInfo: &CommitInfo{
				TranId:   tranId,
				CommitId: tranId,
				SSLLNode: *common.NewSSLLNode(),
				Op:       OpCreate,
				LogIndex: exIndex,
			},
		},
		Schema:     schema,
		Catalog:    catalog,
		SegmentSet: make([]*Segment, 0),
		IdIndex:    make(map[uint64]int),
	}
	return e
}

func NewEmptyTableEntry(catalog *Catalog) *Table {
	e := &Table{
		BaseEntry: &BaseEntry{
			CommitInfo: &CommitInfo{
				SSLLNode: *common.NewSSLLNode(),
			},
		},
		SegmentSet: make([]*Segment, 0),
		IdIndex:    make(map[uint64]int),
		Catalog:    catalog,
	}
	return e
}

func (e *Table) UpdateFlushTS() {
	now := time.Now().UnixMicro()
	atomic.StoreInt64(&e.FlushTS, now)
}

func (e *Table) GetFlushTS() int64 {
	return atomic.LoadInt64(&e.FlushTS)
}

func (e *Table) prepareReplace(ctx *replaceTableCtx) (LogEntry, error) {
	cInfo := &CommitInfo{
		CommitId: e.Catalog.NextUncommitId(),
		Op:       OpReplaced,
		LogIndex: ctx.exIndex,
		SSLLNode: *common.NewSSLLNode(),
	}
	e.Lock()
	defer e.Unlock()
	if e.IsHardDeletedLocked() {
		ctx.discard = true
		return nil, nil
	}
	e.onNewCommit(cInfo)
	if ctx.inTran {
		return nil, nil
	}
	logEntry := e.Catalog.prepareCommitEntry(e, ETShardUpgradeReplaced, e)
	return logEntry, nil
}

// Threadsafe
// It is used to take a snapshot of table base on a commit id. It goes through
// the version chain to find a "safe" commit version and create a view base on
// that version.
// v2(commitId=7) -> v1(commitId=4) -> v0(commitId=2)
//      |                 |                  |
//      |                 |                   -------- fillView [0,2]
//      |                  --------------------------- fillView [4,6]
//       --------------------------------------------- fillView [7,+oo)
func (e *Table) fillView(filter *Filter) *Table {
	// TODO: if baseEntry op is drop, should introduce an index to
	// indicate weather to return nil
	baseEntry := e.UseCommitted(filter.tableFilter)
	if baseEntry == nil {
		return nil
	}
	view := &Table{
		Schema:     e.Schema,
		BaseEntry:  baseEntry,
		SegmentSet: make([]*Segment, 0),
	}
	e.RLock()
	segs := make([]*Segment, 0, len(e.SegmentSet))
	for _, seg := range e.SegmentSet {
		segs = append(segs, seg)
	}
	e.RUnlock()
	for _, seg := range segs {
		segView := seg.fillView(filter)
		if segView == nil {
			continue
		}
		view.SegmentSet = append(view.SegmentSet, segView)
	}
	return view
}

// Not threadsafe, and not needed
// Only used during data replay by the catalog replayer
func (e *Table) rebuild(catalog *Catalog) {
	e.Catalog = catalog
	e.IdIndex = make(map[uint64]int)
	for i, seg := range e.SegmentSet {
		catalog.Sequence.TryUpdateSegmentId(seg.Id)
		seg.rebuild(e)
		e.IdIndex[seg.Id] = i
	}
}

// Threadsafe
// It should be applied on a table that was previously soft-deleted
// It is always driven by engine internal scheduler. It means all the
// table related data resources were deleted. A hard-deleted table will
// be deleted from catalog later
func (e *Table) HardDelete() error {
	ctx := newDeleteTableCtx(e)
	return e.Catalog.onCommitRequest(ctx)
}

func (e *Table) prepareHardDelete(ctx *deleteTableCtx) (LogEntry, error) {
	cInfo := &CommitInfo{
		CommitId: e.Catalog.NextUncommitId(),
		Op:       OpHardDelete,
		SSLLNode: *common.NewSSLLNode(),
	}
	e.Catalog.commitMu.Lock()
	defer e.Catalog.commitMu.Unlock()
	e.Lock()
	defer e.Unlock()
	if e.IsHardDeletedLocked() {
		logutil.Warnf("HardDelete %d but already hard deleted", e.Id)
		return nil, TableNotFoundErr
	}
	if !e.IsSoftDeletedLocked() && !e.IsReplacedLocked() {
		panic("logic error: Cannot hard delete entry that not soft deleted or replaced")
	}
	cInfo.LogIndex = e.CommitInfo.LogIndex
	e.onNewCommit(cInfo)
	logEntry := e.Catalog.prepareCommitEntry(e, ETHardDeleteTable, e)
	return logEntry, nil
}

// Simple* wrappes simple usage of wrapped operation
// It is driven by external command. The engine then schedules a GC task to hard delete
// related resources.
func (e *Table) SimpleSoftDelete(exIndex *LogIndex) error {
	ctx := newDropTableCtx(e.Schema.Name, exIndex)
	ctx.table = e
	return e.Catalog.onCommitRequest(ctx)
}

func (e *Table) prepareSoftDelete(ctx *dropTableCtx) (LogEntry, error) {
	commitId := e.Catalog.NextUncommitId()
	cInfo := &CommitInfo{
		TranId:   commitId,
		CommitId: commitId,
		LogIndex: ctx.exIndex,
		Op:       OpSoftDelete,
		SSLLNode: *common.NewSSLLNode(),
	}
	e.Catalog.commitMu.Lock()
	defer e.Catalog.commitMu.Unlock()
	e.Lock()
	defer e.Unlock()
	if e.IsSoftDeletedLocked() {
		return nil, TableNotFoundErr
	}
	e.onNewCommit(cInfo)
	logEntry := e.Catalog.prepareCommitEntry(e, ETSoftDeleteTable, e)
	return logEntry, nil
}

// Not safe
func (e *Table) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// Not safe
func (e *Table) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, e)
}

// Not safe
func (e *Table) String() string {
	buf, _ := e.Marshal()
	return string(buf)
}

// Not safe
// Usually it is used during creating a table. We need to commit the new table entry
// to the store.
func (e *Table) ToLogEntry(eType LogEntryType) LogEntry {
	var buf []byte
	switch eType {
	case ETCreateTable:
		buf, _ = e.Marshal()
	case ETSoftDeleteTable:
		if !e.IsSoftDeletedLocked() {
			panic("logic error")
		}
		entry := tableLogEntry{
			BaseEntry: e.BaseEntry,
		}
		buf, _ = entry.Marshal()
	case ETHardDeleteTable:
		if !e.IsHardDeletedLocked() {
			panic("logic error")
		}
		entry := tableLogEntry{
			BaseEntry: e.BaseEntry,
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

// Safe
func (e *Table) SimpleGetCurrSegment() *Segment {
	e.RLock()
	if len(e.SegmentSet) == 0 {
		e.RUnlock()
		return nil
	}
	seg := e.SegmentSet[len(e.SegmentSet)-1]
	e.RUnlock()
	return seg
}

// Not safe and no need
// Only used during data replay
// TODO: Only compatible with v1. Remove later
func (e *Table) GetReplayIndex() *LogIndex {
	for i := len(e.SegmentSet) - 1; i >= 0; i-- {
		seg := e.SegmentSet[i]
		idx := seg.GetReplayIndex()
		if idx != nil {
			return idx
		}
	}
	return nil
}

func (e *Table) RecurLoopLocked(processor LoopProcessor) error {
	var err error
	for _, segment := range e.SegmentSet {
		if err = processor.OnSegment(segment); err != nil {
			return err
		}
		segment.RLock()
		defer segment.RUnlock()
		for _, block := range segment.BlockSet {
			if err = processor.OnBlock(block); err != nil {
				return err
			}
		}
	}
	return err
}

// Safe
// TODO: Only compatible with v1. Remove later
func (e *Table) GetAppliedIndex(rwmtx *sync.RWMutex) (uint64, bool) {
	if rwmtx == nil {
		e.RLock()
		defer e.RUnlock()
	}
	if e.IsDeletedLocked() {
		return e.BaseEntry.GetAppliedIndex()
	}
	var (
		id uint64
		ok bool
	)
	for i := len(e.SegmentSet) - 1; i >= 0; i-- {
		seg := e.SegmentSet[i]
		id, ok = seg.GetAppliedIndex(nil)
		if ok {
			break
		}
	}
	if !ok {
		return e.BaseEntry.GetAppliedIndex()
	}
	return id, ok
}

// Not safe. One writer, multi-readers
func (e *Table) SimpleCreateBlock() (*Block, *Segment) {
	var prevSeg *Segment
	currSeg := e.SimpleGetCurrSegment()
	if currSeg == nil || currSeg.HasMaxBlocks() {
		prevSeg = currSeg
		currSeg = e.SimpleCreateSegment()
	}
	blk := currSeg.SimpleCreateBlock()
	return blk, prevSeg
}

func (e *Table) getFirstInfullSegment(from *Segment) (*Segment, *Segment) {
	if len(e.SegmentSet) == 0 {
		return nil, nil
	}
	var curr, next *Segment
	for i := len(e.SegmentSet) - 1; i >= 0; i-- {
		seg := e.SegmentSet[i]
		if seg.Appendable() && from.LE(seg) {
			curr, next = seg, curr
		} else {
			break
		}
	}
	return curr, next
}

// Not safe. One writer, multi-readers
func (e *Table) SimpleGetOrCreateNextBlock(from *Block) *Block {
	var fromSeg *Segment
	if from != nil {
		fromSeg = from.Segment
	}
	curr, next := e.getFirstInfullSegment(fromSeg)
	// logutil.Infof("%s, %s", curr.PString(PPL0), fromSeg.PString(PPL1))
	if curr == nil {
		curr = e.SimpleCreateSegment()
	}
	blk := curr.SimpleGetOrCreateNextBlock(from)
	if blk != nil {
		return blk
	}
	if next == nil {
		next = e.SimpleCreateSegment()
	}
	return next.SimpleGetOrCreateNextBlock(nil)
}

func (e *Table) SimpleCreateSegment() *Segment {
	ctx := newCreateSegmentCtx(e)
	if err := e.Catalog.onCommitRequest(ctx); err != nil {
		return nil
	}
	return ctx.segment
}

// Safe
func (e *Table) SimpleGetSegmentIds() []uint64 {
	e.RLock()
	defer e.RUnlock()
	arrLen := len(e.SegmentSet)
	ret := make([]uint64, arrLen)
	for i, seg := range e.SegmentSet {
		ret[i] = seg.Id
	}
	return ret
}

// Safe
func (e *Table) SimpleGetSegmentCount() int {
	e.RLock()
	defer e.RUnlock()
	return len(e.SegmentSet)
}

func (e *Table) prepareCreateSegment(ctx *createSegmentCtx) (LogEntry, error) {
	se := newSegmentEntry(e.Catalog, e, e.Catalog.NextUncommitId(), ctx.exIndex)
	logEntry := se.ToLogEntry(ETCreateSegment)
	e.Catalog.commitMu.Lock()
	defer e.Catalog.commitMu.Unlock()
	e.Lock()
	e.onNewSegment(se)
	e.Unlock()
	e.Catalog.prepareCommitLog(se, logEntry)
	ctx.segment = se
	return logEntry, nil
}

func (e *Table) onNewSegment(entry *Segment) {
	e.IdIndex[entry.Id] = len(e.SegmentSet)
	e.SegmentSet = append(e.SegmentSet, entry)
}

// Safe
func (e *Table) SimpleGetBlock(segId, blkId uint64) (*Block, error) {
	seg := e.SimpleGetSegment(segId)
	if seg == nil {
		return nil, SegmentNotFoundErr
	}
	blk := seg.SimpleGetBlock(blkId)
	if blk == nil {
		return nil, BlockNotFoundErr
	}
	return blk, nil
}

// Safe
func (e *Table) SimpleGetSegment(id uint64) *Segment {
	e.RLock()
	defer e.RUnlock()
	return e.GetSegment(id, MinUncommitId)
}

func (e *Table) Splite(specs []*TableRangeSpec, nameFactory TableNameFactory, catalog *Catalog) []*Table {
	tranId := catalog.NextUncommitId()
	tables := make([]*Table, len(specs))
	for i, spec := range specs {
		logIndex := LogIndex{
			ShardId: spec.ShardId,
			Id:      shard.SimpleIndexId(uint64(0)),
		}
		info := e.CommitInfo
		info.TranId = tranId
		info.CommitId = tranId
		info.LogIndex = &logIndex
		baseEntry := &BaseEntry{
			Id:         catalog.NextTableId(),
			CommitInfo: info,
		}
		schema := *e.Schema
		schema.Name = nameFactory.Rename(schema.Name, spec.ShardId)
		tables[i] = &Table{
			Schema:     &schema,
			BaseEntry:  baseEntry,
			SegmentSet: make([]*Segment, 0),
		}
	}
	idx := 0
	spec := specs[idx]
	for i, segment := range e.SegmentSet {
		minRow := uint64(i) * e.Schema.BlockMaxRows * e.Schema.SegmentMaxBlocks
		if spec.Range.LT(minRow) {
			idx++
			spec = specs[idx]
		}
		table := tables[idx]
		segment.Id = catalog.NextSegmentId()
		segment.CommitInfo.TranId = tranId
		segment.CommitInfo.CommitId = tranId
		segment.CommitInfo.LogIndex = table.CommitInfo.LogIndex
		for _, block := range segment.BlockSet {
			block.Id = catalog.NextBlockId()
			block.CommitInfo.TranId = tranId
			block.CommitInfo.CommitId = tranId
			block.CommitInfo.LogIndex = table.CommitInfo.LogIndex
		}
	}
	return tables
}

func (e *Table) GetSegment(id, tranId uint64) *Segment {
	pos, ok := e.IdIndex[id]
	if !ok {
		return nil
	}
	entry := e.SegmentSet[pos]
	return entry
}

// Not safe
func (e *Table) PString(level PPLevel) string {
	s := fmt.Sprintf("<Table[%s]>(%s)(Cnt=%d)", e.Schema.Name, e.BaseEntry.PString(level), len(e.SegmentSet))
	if level > PPL0 && len(e.SegmentSet) > 0 {
		s = fmt.Sprintf("%s{", s)
		for _, seg := range e.SegmentSet {
			s = fmt.Sprintf("%s\n%s", s, seg.PString(level))
		}
		s = fmt.Sprintf("%s\n}", s)
	}
	return s
}

func MockTable(catalog *Catalog, schema *Schema, blkCnt uint64, idx *LogIndex) *Table {
	if schema == nil {
		schema = MockSchema(2)
	}
	if idx == nil {
		idx = &LogIndex{
			Id: shard.SimpleIndexId(common.NextGlobalSeqNum()),
		}
	}
	tbl, err := catalog.SimpleCreateTable(schema, idx)
	if err != nil {
		panic(err)
	}

	var activeSeg *Segment
	for i := uint64(0); i < blkCnt; i++ {
		if activeSeg == nil {
			activeSeg = tbl.SimpleCreateSegment()
		}
		activeSeg.SimpleCreateBlock()
		if len(activeSeg.BlockSet) == int(tbl.Schema.SegmentMaxBlocks) {
			activeSeg = nil
		}
	}
	return tbl
}
