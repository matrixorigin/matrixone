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
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
)

var (
	UpgradeInfullBlockErr = errors.New("aoe: upgrade infull block")
)

type blockLogEntry struct {
	*BaseEntry
	Catalog   *Catalog `json:"-"`
	TableId   uint64
	SegmentId uint64
}

func (e *blockLogEntry) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *blockLogEntry) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, e)
}

func (e *blockLogEntry) ToEntry() *Block {
	entry := &Block{
		BaseEntry: e.BaseEntry,
	}
	table := e.Catalog.TableSet[e.TableId]
	entry.Segment = table.GetSegment(e.SegmentId, MinUncommitId)
	return entry
}

type IndiceMemo struct {
	mu      *sync.Mutex
	snippet *shard.Snippet
}

func NewIndiceMemo(e *Block) *IndiceMemo {
	i := new(IndiceMemo)
	i.mu = new(sync.Mutex)
	i.snippet = e.CreateSnippet()
	return i
}

func (memo *IndiceMemo) Append(index *LogIndex) {
	if memo == nil {
		return
	}
	memo.mu.Lock()
	memo.snippet.Append(index)
	memo.mu.Unlock()
}

func (memo *IndiceMemo) Fetch(block *Block) *shard.Snippet {
	if memo == nil {
		return nil
	}
	memo.mu.Lock()
	defer memo.mu.Unlock()
	snippet := memo.snippet
	memo.snippet = block.CreateSnippet()
	return snippet
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
			Id: segment.Table.Catalog.NextBlockId(),
			CommitInfo: &CommitInfo{
				CommitId: tranId,
				TranId:   tranId,
				SSLLNode: *common.NewSSLLNode(),
				Op:       OpCreate,
				LogIndex: exIndex,
			},
		},
	}
	snippet := e.CreateSnippet()
	if snippet != nil {
		e.IndiceMemo = new(IndiceMemo)
		e.IndiceMemo.snippet = snippet
		e.IndiceMemo.mu = new(sync.Mutex)
	}
	return e
}

func newCommittedBlockEntry(segment *Segment, base *BaseEntry) *Block {
	e := &Block{
		Segment:   segment,
		BaseEntry: base,
	}
	snippet := e.CreateSnippet()
	if snippet != nil {
		e.IndiceMemo = new(IndiceMemo)
		e.IndiceMemo.snippet = snippet
		e.IndiceMemo.mu = new(sync.Mutex)
	}
	return e
}

func (e *Block) CreateSnippet() *shard.Snippet {
	tableLogIndex := e.Segment.Table.GetCommit().LogIndex
	if tableLogIndex == nil {
		return nil
	}
	return shard.NewSnippet(tableLogIndex.ShardId, e.Id, uint32(0))
}

func (e *Block) ConsumeSnippet(reset bool) *shard.Snippet {
	e.RLock()
	snippet := e.IndiceMemo.Fetch(e)
	e.RUnlock()
	if reset {
		e.Lock()
		defer e.Unlock()
		e.IndiceMemo = nil
	}
	return snippet
}

func (e *Block) View() (view *Block) {
	e.RLock()
	view = &Block{
		BaseEntry:   &BaseEntry{Id: e.Id, CommitInfo: e.CommitInfo},
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
// One writer, multi-readers
func (e *Block) SetSegmentedId(id uint64) error {
	atomic.StoreUint64(&e.SegmentedId, id)
	return nil
}

// Safe
func (e *Block) GetAppliedIndex(rwmtx *sync.RWMutex) (uint64, bool) {
	if rwmtx == nil {
		e.RLock()
		defer e.RUnlock()
	}
	if !e.IsFullLocked() {
		id := atomic.LoadUint64(&e.SegmentedId)
		if id == 0 {
			return id, false
		}
		return id, true
	}
	return e.BaseEntry.GetAppliedIndex()
}

// Not safe
func (e *Block) HasMaxRowsLocked() bool {
	return e.Count == e.Segment.Table.Schema.BlockMaxRows
}

// Not safe
func (e *Block) SetIndexLocked(idx LogIndex) error {
	err := e.CommitInfo.SetIndex(idx)
	if err != nil {
		return err
	}
	e.IndiceMemo.Append(&idx)
	return err
}

func (e *Block) GetCountLocked() uint64 {
	if e.IsFullLocked() {
		return e.Segment.Table.Schema.BlockMaxRows
	}
	return atomic.LoadUint64(&e.Count)
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
	curCnt := e.GetCountLocked()
	if curCnt+n > e.Segment.Table.Schema.BlockMaxRows {
		return 0, errors.New(fmt.Sprintf("block row count %d > block max rows %d", curCnt+n, e.Segment.Table.Schema.BlockMaxRows))
	}
	for !atomic.CompareAndSwapUint64(&e.Count, curCnt, curCnt+n) {
		runtime.Gosched()
		curCnt = e.GetCountLocked()
		if curCnt+n > e.Segment.Table.Schema.BlockMaxRows {
			return 0, errors.New(fmt.Sprintf("block row count %d > block max rows %d", curCnt+n, e.Segment.Table.Schema.BlockMaxRows))
		}
	}
	return curCnt + n, nil
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
	baseEntry := e.UseCommitted(filter.blockFilter)
	if baseEntry == nil {
		return nil
	}
	view := *e
	view.BaseEntry = baseEntry
	return &view
}

// Safe
func (e *Block) SimpleUpgrade(exIndice []*LogIndex) error {
	tranId := e.Segment.Table.Catalog.NextUncommitId()
	ctx := newUpgradeBlockCtx(e, exIndice, tranId)
	err := e.Segment.Table.Catalog.onCommitRequest(ctx)
	if err != nil {
		return err
	}
	e.Segment.Table.Catalog.blockListener.OnBlockUpgraded(e)
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
		if len(ctx.exIndice) > 1 {
			cInfo.PrevIndex = ctx.exIndice[1]
		}
	} else {
		cInfo.LogRange = e.CommitInfo.LogRange
		cInfo.LogIndex = e.CommitInfo.LogIndex
		id, ok := e.BaseEntry.GetAppliedIndex()
		if ok {
			cInfo.AppliedIndex = &LogIndex{
				Id: shard.SimpleIndexId(id),
			}
		}
	}
	e.onNewCommit(cInfo)
	logEntry := e.Segment.Catalog.prepareCommitEntry(e, ETUpgradeBlock, e)
	return logEntry, nil
}

func (e *Block) toLogEntry() *blockLogEntry {
	return &blockLogEntry{
		BaseEntry: e.BaseEntry,
		Catalog:   e.Segment.Catalog,
		TableId:   e.Segment.Table.Id,
		SegmentId: e.Segment.Id,
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
	s := fmt.Sprintf("<%d. Block %s>[Size=%d]", e.Idx, e.BaseEntry.PString(level), e.GetCoarseSizeLocked())
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
	entry := e.toLogEntry()
	buf, _ := entry.Marshal()
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}
