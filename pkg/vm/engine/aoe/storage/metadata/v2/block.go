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
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/logstore"
	"runtime"
	"sync"
	"sync/atomic"
)

var (
	UpgradeInfullBlockErr = errors.New("aoe: upgrade infull block")
)

type blockLogEntry struct {
	BaseEntry
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

type Block struct {
	BaseEntry
	Segment     *Segment `json:"-"`
	Count       uint64
	SegmentedId uint64
}

func newBlockEntry(segment *Segment, tranId uint64, exIndex *ExternalIndex) *Block {
	e := &Block{
		Segment: segment,
		BaseEntry: BaseEntry{
			Id: segment.Table.Catalog.NextBlockId(),
			CommitInfo: &CommitInfo{
				CommitId:      tranId,
				TranId:        tranId,
				SSLLNode:      *common.NewSSLLNode(),
				Op:            OpCreate,
				ExternalIndex: exIndex,
			},
		},
	}
	return e
}

func newCommittedBlockEntry(segment *Segment, base *BaseEntry) *Block {
	e := &Block{
		Segment:   segment,
		BaseEntry: *base,
	}
	return e
}

func (e *Block) View() (view *Block) {
	e.RLock()
	view = &Block{
		BaseEntry:   BaseEntry{Id: e.Id, CommitInfo: e.CommitInfo},
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
	if !e.IsFull() {
		id := atomic.LoadUint64(&e.SegmentedId)
		if id == 0 {
			return id, false
		}
		return id, true
	}
	return e.BaseEntry.GetAppliedIndex()
}

// Not safe
func (e *Block) HasMaxRows() bool {
	return e.Count == e.Segment.Table.Schema.BlockMaxRows
}

// Not safe
func (e *Block) SetIndex(idx LogIndex) error {
	return e.CommitInfo.SetIndex(idx)
}

// Not safe
// TODO: should be safe
func (e *Block) GetCount() uint64 {
	if e.IsFull() {
		return e.Segment.Table.Schema.BlockMaxRows
	}
	return atomic.LoadUint64(&e.Count)
}

// Not safe
// TODO: should be safe
func (e *Block) AddCount(n uint64) (uint64, error) {
	curCnt := e.GetCount()
	if curCnt+n > e.Segment.Table.Schema.BlockMaxRows {
		return 0, errors.New(fmt.Sprintf("block row count %d > block max rows %d", curCnt+n, e.Segment.Table.Schema.BlockMaxRows))
	}
	for !atomic.CompareAndSwapUint64(&e.Count, curCnt, curCnt+n) {
		runtime.Gosched()
		curCnt = e.GetCount()
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
func (e *Block) CommittedView(id uint64) *Block {
	baseEntry := e.UseCommitted(id)
	if baseEntry == nil {
		return nil
	}
	return &Block{
		BaseEntry: *baseEntry,
	}
}

// Safe
func (e *Block) SimpleUpgrade(exIndice []*ExternalIndex) error {
	return e.Upgrade(e.Segment.Table.Catalog.NextUncommitId(), exIndice, true)
}

func (e *Block) Upgrade(tranId uint64, exIndice []*ExternalIndex, autoCommit bool) error {
	if e.GetCount() != e.Segment.Table.Schema.BlockMaxRows {
		return UpgradeInfullBlockErr
	}
	e.Lock()
	defer e.Unlock()
	var newOp OpT
	switch e.CommitInfo.Op {
	case OpCreate:
		newOp = OpUpgradeFull
	default:
		return UpgradeNotNeededErr
	}
	cInfo := &CommitInfo{
		TranId:   tranId,
		CommitId: tranId,
		Op:       newOp,
	}
	if exIndice != nil {
		cInfo.ExternalIndex = exIndice[0]
		if len(exIndice) > 1 {
			cInfo.PrevIndex = exIndice[1]
		}
	} else {
		cInfo.ExternalIndex = e.CommitInfo.ExternalIndex
		id, ok := e.BaseEntry.GetAppliedIndex()
		if ok {
			cInfo.AppliedIndex = &ExternalIndex{
				Id: SimpleBatchId(id),
			}
		}
	}
	e.onNewCommit(cInfo)
	if !autoCommit {
		return nil
	}
	e.Segment.Catalog.Commit(e, ETUpgradeBlock, &e.RWMutex)
	return nil
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

// Not safe
func (e *Block) PString(level PPLevel) string {
	s := fmt.Sprintf("<Block %s>", e.BaseEntry.PString(level))
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
	logEntry := logstore.GetEmptyEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}
