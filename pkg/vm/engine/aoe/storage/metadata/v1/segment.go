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
	"strings"

	"github.com/jiangxinmeng1/logstore/pkg/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

var (
	UpgradeInfullSegmentErr = errors.New("aoe: upgrade infull segment")
	UpgradeNotNeededErr     = errors.New("aoe: already upgraded")
)

type segmentLogEntry struct {
	*BaseEntry
	DatabaseId uint64
	TableId    uint64
	Catalog    *Catalog `json:"-"`
}

func (e *segmentLogEntry) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *segmentLogEntry) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, e)
}

type Segment struct {
	*BaseEntry
	Table    *Table         `json:"-"`
	IdIndex  map[uint64]int `json:"-"`
	BlockSet []*Block       `json:"blocks"`
}

func newSegmentEntry(table *Table, tranId uint64, exIndex *LogIndex) *Segment {
	e := &Segment{
		Table:    table,
		BlockSet: make([]*Block, 0),
		IdIndex:  make(map[uint64]int),
		BaseEntry: &BaseEntry{
			Id: table.Database.Catalog.NextSegmentId(),
			CommitInfo: &CommitInfo{
				CommitId: tranId,
				TranId:   tranId,
				SSLLNode: *common.NewSSLLNode(),
				Op:       OpCreate,
				LogIndex: exIndex,
			},
		},
	}
	return e
}

func newCommittedSegmentEntry(table *Table, base *BaseEntry) *Segment {
	e := &Segment{
		Table:     table,
		BlockSet:  make([]*Block, 0),
		IdIndex:   make(map[uint64]int),
		BaseEntry: base,
	}
	return e
}

func (e *Segment) DebugCheckReplayedState() {
	if e.Table == nil {
		panic("table is missing")
	}
	if e.IdIndex == nil {
		panic("id index is missing")
	}
	if e.Table.Database.Catalog.TryUpdateCommitId(e.GetCommit().CommitId) {
		panic("sequence error")
	}
	if e.Table.Database.Catalog.TryUpdateSegmentId(e.Id) {
		panic("sequence error")
	}
	for _, blk := range e.BlockSet {
		blk.DebugCheckReplayedState()
	}
}

func (e *Segment) LE(o *Segment) bool {
	if e == nil {
		return true
	}
	return e.Id <= o.Id
}

func (e *Segment) rebuild(table *Table, replay bool) {
	e.Table = table
	e.IdIndex = make(map[uint64]int)
	for i, blk := range e.BlockSet {
		if replay {
			e.Table.Database.Catalog.Sequence.TryUpdateBlockId(blk.Id)
		}
		blk.rebuild(e)
		e.IdIndex[blk.Id] = i
	}
}

// Safe
func (e *Segment) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   e.Table.Id,
		SegmentID: e.Id,
	}
}

// Safe
func (e *Segment) fillView(filter *Filter) *Segment {
	baseEntry := e.UseCommitted(filter.segmentFilter)
	if baseEntry == nil {
		return nil
	}
	view := &Segment{
		BaseEntry: baseEntry,
		BlockSet:  make([]*Block, 0),
	}
	e.RLock()
	blks := make([]*Block, 0, len(e.BlockSet))
	for _, blk := range e.BlockSet {
		blks = append(blks, blk)
	}
	e.RUnlock()
	for _, blk := range blks {
		blkView := blk.fillView(filter)
		if blkView == nil {
			continue
		}
		view.BlockSet = append(view.BlockSet, blkView)
	}
	if len(view.BlockSet) == 0 {
		return nil
	}
	return view
}

func (e *Segment) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *Segment) toLogEntry(info *CommitInfo) *segmentLogEntry {
	if info == nil {
		info = e.CommitInfo
	}
	return &segmentLogEntry{
		BaseEntry: &BaseEntry{
			Id: e.Id,
			CommitInfo: info.Clone()},
		TableId:    e.Table.Id,
		DatabaseId: e.Table.Database.Id,
	}
}

func (e *Segment) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, e)
}

// Not safe
func (e *Segment) PString(level PPLevel, depth int) string {
	if e == nil {
		return "null segment"
	}
	ident := strings.Repeat("  ", depth)
	ident2 := " " + ident
	e.RLock()
	defer e.RUnlock()
	s := fmt.Sprintf("<Segment[%d] %s", e.Id, e.BaseEntry.PString(level))
	cnt := 0
	if level > PPL0 {
		for _, blk := range e.BlockSet {
			cnt++
			s = fmt.Sprintf("%s\n%s%s", s, ident2, blk.PString(level))
		}
	}
	if cnt == 0 {
		s = fmt.Sprintf("%s[Size=%d]>", s, e.GetCoarseSizeLocked())
	} else {
		s = fmt.Sprintf("%s\n%s[Size=%d]\n>", s, ident, e.GetCoarseSizeLocked())
	}
	return s
}

// Not safe
func (e *Segment) String() string {
	buf, _ := e.Marshal()
	return string(buf)
}

// Not safe
func (e *Segment) ToLogEntry(eType LogEntryType) LogEntry {
	switch eType {
	case ETCreateSegment:
		break
	case ETUpgradeSegment:
		break
	case ETDropSegment:
		if !e.IsSoftDeletedLocked() {
			panic("logic error")
		}
		break
	default:
		panic("not supported")
	}
	ent := e.toLogEntry(nil)
	buf, _ := ent.Marshal()
	logEntry := entry.GetBase()
	logEntry.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}

// Safe
func (e *Segment) SimpleCreateBlock() *Block {
	tranId := e.Table.Database.Catalog.NextUncommitId()
	ctx := newCreateBlockCtx(e, tranId)
	if err := e.Table.Database.Catalog.onCommitRequest(ctx, true); err != nil {
		return nil
	}
	return ctx.block
}

func (e *Segment) AppendableLocked() bool {
	if e.HasMaxBlocks() {
		return !e.BlockSet[len(e.BlockSet)-1].IsFull()
	}
	return true
}

func (e *Segment) prepareCreateBlock(ctx *createBlockCtx) (LogEntry, error) {
	be := newBlockEntry(e, ctx.tranId, ctx.exIndex)
	logEntry := be.ToLogEntry(ETCreateBlock)
	e.Lock()
	e.onNewBlock(be)
	e.Unlock()
	e.Table.Database.Catalog.prepareCommitLog(be, logEntry)
	ctx.block = be
	return logEntry, nil
}

func (e *Segment) MaxLogIndex() *LogIndex {
	e.RLock()
	defer e.RUnlock()
	var index *LogIndex
	for i := len(e.BlockSet) - 1; i >= 0; i-- {
		index = e.BlockSet[i].LatestLogIndexLocked()
		if index != nil {
			break
		}
	}
	return index
}

func (e *Segment) onNewBlock(entry *Block) {
	idx := len(e.BlockSet)
	e.IdIndex[entry.Id] = idx
	entry.Idx = uint32(idx)
	e.BlockSet = append(e.BlockSet, entry)
}

// Safe
func (e *Segment) SimpleUpgrade(size int64, exIndice []*LogIndex) error {
	stale := e.GetCommit()
	tranId := e.Table.Database.Catalog.NextUncommitId()
	ctx := newUpgradeSegmentCtx(e, size, exIndice, tranId)
	err := e.Table.Database.Catalog.onCommitRequest(ctx, true)
	if err != nil {
		return err
	}
	e.Table.Database.segmentListener.OnSegmentUpgraded(e, stale)
	return err
}

// Not safe
func (e *Segment) FirstInFullBlock() *Block {
	if len(e.BlockSet) == 0 {
		return nil
	}
	var found *Block
	for i := len(e.BlockSet) - 1; i >= 0; i-- {
		if !e.BlockSet[i].IsFullLocked() {
			found = e.BlockSet[i]
		} else {
			break
		}
	}
	return found
}

func (e *Segment) IsUpgradable() bool {
	e.RLock()
	defer e.RUnlock()
	return e.IsUpgradableLocked()
}

func (e *Segment) IsUpgradableLocked() bool {
	if e.IsSortedLocked() {
		return false
	}
	if len(e.BlockSet) != int(e.Table.Schema.SegmentMaxBlocks) {
		return false
	}
	for _, block := range e.BlockSet {
		if !block.IsFull() {
			return false
		}
	}
	return true
}

// Not safe
func (e *Segment) HasMaxBlocks() bool {
	return e.IsSortedLocked() || len(e.BlockSet) == int(e.Table.Schema.SegmentMaxBlocks)
}

func (e *Segment) GetCoarseCountLocked() int64 {
	if e.IsSortedLocked() {
		return int64(e.Table.Schema.SegmentMaxBlocks * e.Table.Schema.BlockMaxRows)
	}
	count := int64(0)
	for _, block := range e.BlockSet {
		count += block.GetCoarseCount()
	}
	return count
}

func (e *Segment) GetCoarseCount() int64 {
	e.RLock()
	defer e.RUnlock()
	return e.GetCoarseCountLocked()
}

func (e *Segment) GetCoarseSize() int64 {
	e.RLock()
	defer e.RUnlock()
	return e.GetCoarseSizeLocked()
}

func (e *Segment) GetUnsortedSize() int64 {
	e.RLock()
	defer e.RUnlock()
	return e.GetUnsortedSizeLocked()
}

func (e *Segment) GetUnsortedSizeLocked() int64 {
	size := int64(0)
	for _, block := range e.BlockSet {
		size += block.GetCoarseSize()
	}
	return size
}

func (e *Segment) GetCoarseSizeLocked() int64 {
	if e.IsSortedLocked() {
		return e.CommitInfo.Size
	}
	return e.GetUnsortedSizeLocked()
}

func (e *Segment) prepareUpgrade(ctx *upgradeSegmentCtx) (LogEntry, error) {
	e.RLock()
	if !e.HasMaxBlocks() {
		e.RUnlock()
		return nil, UpgradeInfullSegmentErr
	}
	if e.IsSortedLocked() {
		return nil, UpgradeNotNeededErr
	}
	for _, blk := range e.BlockSet {
		if !blk.IsFullLocked() {
			return nil, UpgradeInfullSegmentErr
		}
	}
	e.RUnlock()
	e.Lock()
	defer e.Unlock()
	var newOp OpT
	switch e.CommitInfo.Op {
	case OpCreate:
		newOp = OpUpgradeSorted
	default:
		return nil, UpgradeNotNeededErr
	}
	cInfo := &CommitInfo{
		TranId:   ctx.tranId,
		CommitId: ctx.tranId,
		Op:       newOp,
		Size:     ctx.size,
	}
	if ctx.exIndice != nil {
		cInfo.LogIndex = ctx.exIndice[0]
	}
	if err := e.onCommit(cInfo); err != nil {
		return nil, err
	}
	logEntry := e.Table.Database.Catalog.prepareCommitEntry(e, ETUpgradeSegment, e)
	return logEntry, nil
}

func (e *Segment) DryUpgrade(size int64) {
	e.CommitInfo.Op = OpUpgradeSorted
	e.CommitInfo.Size = size
}

// Not safe
// One writer, multi-readers
func (e *Segment) SimpleGetOrCreateNextBlock(from *Block) *Block {
	e.RLock()
	if len(e.BlockSet) == 0 {
		e.RUnlock()
		return e.SimpleCreateBlock()
	}
	var ret *Block
	for i := len(e.BlockSet) - 1; i >= 0; i-- {
		blk := e.BlockSet[i]
		if !blk.IsFull() && from.Less(blk) {
			ret = blk
		} else {
			break
		}
	}
	if ret != nil || e.HasMaxBlocks() {
		e.RUnlock()
		return ret
	}
	e.RUnlock()
	return e.SimpleCreateBlock()
}

// Safe
func (e *Segment) SimpleGetBlock(id uint64) *Block {
	e.RLock()
	defer e.RUnlock()
	return e.GetBlock(id, MinUncommitId)
}

func (e *Segment) GetBlock(id, tranId uint64) *Block {
	pos, ok := e.IdIndex[id]
	if !ok {
		return nil
	}
	entry := e.BlockSet[pos]
	return entry
}

func (e *Segment) GetRowCount() uint64 {
	e.RLock()
	defer e.RUnlock()
	return e.GetRowCountLocked()
}

func (e *Segment) GetRowCountLocked() uint64 {
	if e.CommitInfo.Op >= OpUpgradeClose {
		return e.Table.Schema.BlockMaxRows * e.Table.Schema.SegmentMaxBlocks
	}
	var ret uint64
	e.RLock()
	for _, blk := range e.BlockSet {
		blk.RLock()
		ret += blk.GetCountLocked()
		blk.RUnlock()
	}
	e.RUnlock()
	return ret
}
