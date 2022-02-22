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
	UpgradeInfullBlockErr = errors.New("aoe: upgrade infull block")
)

type blockLogEntry struct {
	*BaseEntry
	Catalog    *Catalog `json:"-"`
	DatabaseId uint64
	TableId    uint64
	SegmentId  uint64
}

func (e *blockLogEntry) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *blockLogEntry) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, e)
}

type IndiceMemo struct {
	mu     *sync.Mutex
	indice *shard.SliceIndice
}

func NewIndiceMemo(e *Block) *IndiceMemo {
	i := new(IndiceMemo)
	i.mu = new(sync.Mutex)
	i.indice = e.CreateSnippet()
	return i
}

func (memo *IndiceMemo) Append(index *shard.SliceIndex) {
	if memo == nil {
		return
	}
	memo.mu.Lock()
	memo.indice.Append(index)
	memo.mu.Unlock()
}

func (memo *IndiceMemo) Fetch(block *Block) *shard.SliceIndice {
	if memo == nil {
		return nil
	}
	memo.mu.Lock()
	defer memo.mu.Unlock()
	indice := memo.indice
	memo.indice = block.CreateSnippet()
	return indice
}

type Block struct {
	*BaseEntry
	Segment     *Segment    `json:"-"`
	IndiceMemo  *IndiceMemo `json:"-"`
	Idx         uint32      `json:"idx"`
	Count       uint64      `json:"count"`
	SegmentedId uint64      `json:"segmentedid"`
}

func newBlockEntry(segment *Segment, tranId uint64, exIndex *LogIndex) *Block {
	e := &Block{
		Segment: segment,
		BaseEntry: &BaseEntry{
			Id: segment.Table.Database.Catalog.NextBlockId(),
			CommitInfo: &CommitInfo{
				CommitId: tranId,
				TranId:   tranId,
				SSLLNode: *common.NewSSLLNode(),
				Op:       OpCreate,
				LogIndex: exIndex,
			},
		},
	}
	indice := e.CreateSnippet()
	if indice != nil {
		e.IndiceMemo = new(IndiceMemo)
		e.IndiceMemo.indice = indice
		e.IndiceMemo.mu = new(sync.Mutex)
	}
	return e
}

func newCommittedBlockEntry(segment *Segment, base *BaseEntry) *Block {
	e := &Block{
		Segment:   segment,
		BaseEntry: base,
	}
	indice := e.CreateSnippet()
	if indice != nil {
		e.IndiceMemo = new(IndiceMemo)
		e.IndiceMemo.indice = indice
		e.IndiceMemo.mu = new(sync.Mutex)
	}
	return e
}

func (e *Block) DebugCheckReplayedState() {
	if e.Segment == nil {
		panic("segment is missing")
	}
	if e.Segment.Table.Database.Catalog.TryUpdateCommitId(e.GetCommit().CommitId) {
		panic("sequence error")
	}
	if e.Segment.Table.Database.Catalog.TryUpdateBlockId(e.Id) {
		panic("sequence error")
	}
}

func (e *Block) CreateSnippet() *shard.SliceIndice {
	tableLogIndex := e.Segment.Table.GetCommit().LogIndex
	if tableLogIndex == nil {
		return nil
	}
	return shard.NewBatchIndice(tableLogIndex.ShardId)
}

func (e *Block) ConsumeSnippet(reset bool) *shard.SliceIndice {
	e.RLock()
	indice := e.IndiceMemo.Fetch(e)
	e.RUnlock()
	if reset {
		e.Lock()
		defer e.Unlock()
		e.IndiceMemo = nil
	}
	return indice
}

func (e *Block) View() (view *Block) {
	e.RLock()
	view = &Block{
		BaseEntry:   &BaseEntry{Id: e.Id, CommitInfo: e.CommitInfo.Clone()},
		Segment:     e.Segment,
		Count:       e.Count,
		SegmentedId: e.SegmentedId,
	}
	e.RUnlock()
	return
}

// Safe
func (e *Block) Less(o *Block) bool {
	if e == nil {
		return true
	}
	return e.Id < o.Id
}

func (e *Block) rebuild(segment *Segment) {
	e.Segment = segment
	if e.IsFullLocked() {
		return
	}
	indice := e.CreateSnippet()
	if indice != nil {
		e.IndiceMemo = new(IndiceMemo)
		e.IndiceMemo.indice = indice
		e.IndiceMemo.mu = new(sync.Mutex)
	}
}

func (e *Block) DescId() common.ID {
	return common.ID{
		TableID:   e.Segment.Table.Id,
		SegmentID: e.Segment.Id,
		BlockID:   uint64(e.Idx),
	}
}

// Safe
func (e *Block) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   e.Segment.Table.Id,
		SegmentID: e.Segment.Id,
		BlockID:   e.Id,
	}
}

// Not safe
func (e *Block) HasMaxRowsLocked() bool {
	return e.Count == e.Segment.Table.Schema.BlockMaxRows
}

// Not safe
func (e *Block) SetIndexLocked(index *shard.SliceIndex) error {
	idx := index.Clone()
	err := e.CommitInfo.SetIndex(*idx.Index)
	if err != nil {
		return err
	}
	e.IndiceMemo.Append(idx)
	return err
}

func (e *Block) GetCountLocked() uint64 {
	if e.IsFullLocked() {
		return e.Segment.Table.Schema.BlockMaxRows
	}
	return e.Count
}

func (e *Block) GetCoarseCountLocked() int64 {
	if e.IsFullLocked() {
		return int64(e.Segment.Table.Schema.BlockMaxRows)
	}
	return 0
}

func (e *Block) GetCoarseCount() int64 {
	e.RLock()
	defer e.RUnlock()
	return e.GetCoarseCountLocked()
}

func (e *Block) AddCountLocked(n uint64) (uint64, error) {
	if e.Count+n > e.Segment.Table.Schema.BlockMaxRows {
		return 0, errors.New("overflow")
	}
	e.Count += n
	return e.Count, nil
}

// TODO: remove it. Should not needed
func (e *Block) SetCount(count uint64) error {
	if count > e.Segment.Table.Schema.BlockMaxRows {
		return errors.New("SetCount exceeds max limit")
	}
	if count < e.Count {
		return errors.New("SetCount cannot set smaller count")
	}
	e.Count = count
	return nil
}

// Safe
func (e *Block) fillView(filter *Filter) *Block {
	e.RLock()
	defer e.RUnlock()
	baseEntry := e.UseCommittedLocked(filter.blockFilter)
	if baseEntry == nil {
		return nil
	}
	view := *e
	view.BaseEntry = baseEntry
	return &view
}

func (e *Block) SetSize(size int64) {
	e.Lock()
	defer e.Unlock()
	e.CommitInfo.SetSize(size)
}

// Safe
func (e *Block) SimpleUpgrade(exIndice []*LogIndex) error {
	tranId := e.Segment.Table.Database.Catalog.NextUncommitId()
	ctx := newUpgradeBlockCtx(e, exIndice, tranId)
	err := e.Segment.Table.Database.Catalog.onCommitRequest(ctx, true)
	if err != nil {
		return err
	}
	e.Segment.Table.Database.blockListener.OnBlockUpgraded(e)
	return err
}

func (e *Block) prepareUpgrade(ctx *upgradeBlockCtx) (LogEntry, error) {
	e.Lock()
	defer e.Unlock()
	if e.GetCountLocked() != e.Segment.Table.Schema.BlockMaxRows {
		return nil, UpgradeInfullBlockErr
	}
	var newOp OpT
	switch e.CommitInfo.Op {
	case OpCreate:
		newOp = OpUpgradeFull
	default:
		return nil, UpgradeNotNeededErr
	}
	cInfo := &CommitInfo{
		TranId:   ctx.tranId,
		CommitId: ctx.tranId,
		Op:       newOp,
		Size:     e.CommitInfo.GetSize(),
	}
	if ctx.exIndice != nil {
		cInfo.LogIndex = ctx.exIndice[0]
	} else {
		cInfo.LogRange = e.CommitInfo.LogRange
		cInfo.LogIndex = e.CommitInfo.LogIndex
	}
	if err := e.onCommit(cInfo); err != nil {
		return nil, err
	}
	logEntry := e.Segment.Table.Database.Catalog.prepareCommitEntry(e, ETUpgradeBlock, e)
	return logEntry, nil
}

func (e *Block) toLogEntry(info *CommitInfo) *blockLogEntry {
	if info == nil {
		info = e.CommitInfo
	}
	return &blockLogEntry{
		BaseEntry: &BaseEntry{
			Id: e.Id,
			CommitInfo: info.Clone()},
		Catalog:    e.Segment.Table.Database.Catalog,
		DatabaseId: e.Segment.Table.Database.Id,
		TableId:    e.Segment.Table.Id,
		SegmentId:  e.Segment.Id,
	}
}

func (e *Block) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *Block) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, e)
}

func (e *Block) GetCoarseSizeLocked() int64 {
	return e.CommitInfo.GetSize()
}

func (e *Block) GetCoarseSize() int64 {
	e.RLock()
	defer e.RUnlock()
	return e.GetCoarseSizeLocked()
}

// Not safe
func (e *Block) PString(level PPLevel) string {
	e.RLock()
	defer e.RUnlock()
	s := fmt.Sprintf("<%d. Block[%d] %s>[Size=%d]", e.Idx, e.Id, e.BaseEntry.PString(level), e.GetCoarseSizeLocked())
	return s
}

// Not safe
func (e *Block) String() string {
	buf, _ := e.Marshal()
	return string(buf)
}

// Not safe
func (e *Block) ToLogEntry(eType LogEntryType) LogEntry {
	switch eType {
	case ETCreateBlock:
		break
	case ETUpgradeBlock:
		break
	case ETDropBlock:
		if !e.IsSoftDeletedLocked() {
			panic("logic error")
		}
		break
	default:
		panic("not supported")
	}
	entry := e.toLogEntry(nil)
	buf, _ := entry.Marshal()
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}
