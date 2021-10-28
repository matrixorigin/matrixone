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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
)

var (
	UpgradeInfullSegmentErr = errors.New("aoe: upgrade infull segment")
	UpgradeNotNeededErr     = errors.New("aoe: already upgraded")
)

type segmentLogEntry struct {
	*BaseEntry
	TableId uint64
	Catalog *Catalog `json:"-"`
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
	Catalog  *Catalog       `json:"-"`
	IdIndex  map[uint64]int `json:"-"`
	BlockSet []*Block
}

func newSegmentEntry(catalog *Catalog, table *Table, tranId uint64, exIndex *LogIndex) *Segment {
	e := &Segment{
		Catalog:  catalog,
		Table:    table,
		BlockSet: make([]*Block, 0),
		IdIndex:  make(map[uint64]int),
		BaseEntry: &BaseEntry{
			Id: table.Catalog.NextSegmentId(),
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

func newCommittedSegmentEntry(catalog *Catalog, table *Table, base *BaseEntry) *Segment {
	e := &Segment{
		Catalog:   catalog,
		Table:     table,
		BlockSet:  make([]*Block, 0),
		IdIndex:   make(map[uint64]int),
		BaseEntry: base,
	}
	return e
}

func (e *Segment) LE(o *Segment) bool {
	if e == nil {
		return true
	}
	return e.Id <= o.Id
}

func (e *Segment) rebuild(table *Table) {
	e.Catalog = table.Catalog
	e.Table = table
	e.IdIndex = make(map[uint64]int)
	for i, blk := range e.BlockSet {
		e.Catalog.Sequence.TryUpdateBlockId(blk.Id)
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
	return view
}

func (e *Segment) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *Segment) toLogEntry() *segmentLogEntry {
	return &segmentLogEntry{
		BaseEntry: e.BaseEntry,
		TableId:   e.Table.Id,
	}
}

func (e *Segment) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, e)
}

// Not safe
func (e *Segment) PString(level PPLevel) string {
	if e == nil {
		return "null segment"
	}
	s := fmt.Sprintf("<Segment %s", e.BaseEntry.PString(level))
	cnt := 0
	if level > PPL0 {
		for _, blk := range e.BlockSet {
			cnt++
			s = fmt.Sprintf("%s\n%s", s, blk.PString(level))
		}
	}
	if cnt == 0 {
		s = fmt.Sprintf("%s>", s)
	} else {
		s = fmt.Sprintf("%s\n>", s)
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
	entry := e.toLogEntry()
	buf, _ := entry.Marshal()
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}

// Safe
func (e *Segment) SimpleCreateBlock() *Block {
	ctx := newCreateBlockCtx(e)
	if err := e.Table.Catalog.onCommitRequest(ctx); err != nil {
		return nil
	}
	return ctx.block
}

// Safe
func (e *Segment) Appendable() bool {
	e.RLock()
	defer e.RUnlock()
	if e.HasMaxBlocks() {
		return !e.BlockSet[len(e.BlockSet)-1].IsFull()
	}
	return true
}

func (e *Segment) prepareCreateBlock(ctx *createBlockCtx) (LogEntry, error) {
	tranId := e.Catalog.NextUncommitId()
	be := newBlockEntry(e, tranId, ctx.exIndex)
	logEntry := be.ToLogEntry(ETCreateBlock)
	e.Lock()
	e.onNewBlock(be)
	e.Unlock()
	e.Table.Catalog.commitMu.Lock()
	defer e.Table.Catalog.commitMu.Unlock()
	e.Table.Catalog.prepareCommitLog(be, logEntry)
	ctx.block = be
	return logEntry, nil
}

// Safe
func (e *Segment) GetAppliedIndex(rwmtx *sync.RWMutex) (uint64, bool) {
	if rwmtx == nil {
		e.RLock()
		defer e.RUnlock()
	}
	if e.IsSorted() {
		return e.BaseEntry.GetAppliedIndex()
	}
	return e.calcAppliedIndex()
}

// Not safe
func (e *Segment) GetReplayIndex() *LogIndex {
	for i := len(e.BlockSet) - 1; i >= 0; i-- {
		blk := e.BlockSet[i]
		if blk.CommitInfo.LogIndex != nil && (blk.Count > 0 || blk.IsFull()) {
			return blk.CommitInfo.LogIndex
		}
	}
	return nil
}

func (e *Segment) calcAppliedIndex() (id uint64, ok bool) {
	for i := len(e.BlockSet) - 1; i >= 0; i-- {
		blk := e.BlockSet[i]
		id, ok = blk.GetAppliedIndex(nil)
		if ok {
			break
		}
	}
	return id, ok
}

func (e *Segment) onNewBlock(entry *Block) {
	e.IdIndex[entry.Id] = len(e.BlockSet)
	e.BlockSet = append(e.BlockSet, entry)
}

// Safe
func (e *Segment) SimpleUpgrade(exIndice []*LogIndex) error {
	ctx := newUpgradeSegmentCtx(e, exIndice)
	return e.Table.Catalog.onCommitRequest(ctx)
}

// Not safe
func (e *Segment) FirstInFullBlock() *Block {
	if len(e.BlockSet) == 0 {
		return nil
	}
	var found *Block
	for i := len(e.BlockSet) - 1; i >= 0; i-- {
		if !e.BlockSet[i].IsFull() {
			found = e.BlockSet[i]
		} else {
			break
		}
	}
	return found
}

// Not safe
func (e *Segment) HasMaxBlocks() bool {
	return e.IsSorted() || len(e.BlockSet) == int(e.Table.Schema.SegmentMaxBlocks)
}

func (e *Segment) prepareUpgrade(ctx *upgradeSegmentCtx) (LogEntry, error) {
	tranId := e.Table.Catalog.NextUncommitId()
	e.RLock()
	if !e.HasMaxBlocks() {
		e.RUnlock()
		return nil, UpgradeInfullSegmentErr
	}
	if e.IsSorted() {
		return nil, UpgradeNotNeededErr
	}
	for _, blk := range e.BlockSet {
		if !blk.IsFull() {
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
		TranId:   tranId,
		CommitId: tranId,
		Op:       newOp,
	}
	if ctx.exIndice == nil {
		id, ok := e.calcAppliedIndex()
		if ok {
			cInfo.AppliedIndex = &LogIndex{
				Id: shard.SimpleIndexId(id),
			}
		}
	} else {
		cInfo.LogIndex = ctx.exIndice[0]
		if len(ctx.exIndice) > 1 {
			cInfo.PrevIndex = ctx.exIndice[1]
		}
	}
	e.onNewCommit(cInfo)
	e.Table.Catalog.commitMu.Lock()
	defer e.Table.Catalog.commitMu.Unlock()
	logEntry := e.Table.Catalog.prepareCommitEntry(e, ETUpgradeSegment, e)
	return logEntry, nil
}

// Not safe
// One writer, multi-readers
func (e *Segment) SimpleGetOrCreateNextBlock(from *Block) *Block {
	if len(e.BlockSet) == 0 {
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
		return ret
	}
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
