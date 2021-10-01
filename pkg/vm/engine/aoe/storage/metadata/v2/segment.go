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
	"sync"
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

// func (e *segmentLogEntry) ToEntry() *Segment {
// 	entry := &Segment{
// 		BaseEntry: e.BaseEntry,
// 	}
// 	entry.Table = e.Catalog.TableSet[e.TableId]
// 	return entry
// }

type Segment struct {
	BaseEntry
	Table    *Table         `json:"-"`
	Catalog  *Catalog       `json:"-"`
	IdIndex  map[uint64]int `json:"-"`
	BlockSet []*Block
}

func newSegmentEntry(catalog *Catalog, table *Table, tranId uint64, exIndex *ExternalIndex) *Segment {
	e := &Segment{
		Catalog:  catalog,
		Table:    table,
		BlockSet: make([]*Block, 0),
		IdIndex:  make(map[uint64]int),
		BaseEntry: BaseEntry{
			Id: table.Catalog.NextSegmentId(),
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

func newCommittedSegmentEntry(catalog *Catalog, table *Table, base *BaseEntry) *Segment {
	e := &Segment{
		Catalog:   catalog,
		Table:     table,
		BlockSet:  make([]*Block, 0),
		IdIndex:   make(map[uint64]int),
		BaseEntry: *base,
	}
	return e
}

func (e *Segment) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   e.Table.Id,
		SegmentID: e.Id,
	}
}

func (e *Segment) CommittedView(id uint64) *Segment {
	baseEntry := e.UseCommitted(id)
	if baseEntry == nil {
		return nil
	}
	view := &Segment{
		BaseEntry: *baseEntry,
		BlockSet:  make([]*Block, 0),
	}
	e.RLock()
	blks := make([]*Block, 0, len(e.BlockSet))
	for _, blk := range e.BlockSet {
		blks = append(blks, blk)
	}
	e.RUnlock()
	for _, blk := range blks {
		blkView := blk.CommittedView(id)
		if blkView == nil {
			continue
		}
		view.BlockSet = append(view.BlockSet, blk)
	}
	return view
}

func (e *Segment) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *Segment) toLogEntry() *segmentLogEntry {
	return &segmentLogEntry{
		BaseEntry: &e.BaseEntry,
		TableId:   e.Table.Id,
	}
}

func (e *Segment) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, e)
}

func (e *Segment) PString(level PPLevel) string {
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

func (e *Segment) String() string {
	buf, _ := e.Marshal()
	return string(buf)
}

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
	logEntry := logstore.NewBaseEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}

func (e *Segment) SimpleCreateBlock(exIndex *ExternalIndex) *Block {
	return e.CreateBlock(e.Table.Catalog.NextUncommitId(), exIndex, true)
}

func (e *Segment) CreateBlock(tranId uint64, exIndex *ExternalIndex, autoCommit bool) *Block {
	be := newBlockEntry(e, tranId, exIndex)
	e.Lock()
	e.onNewBlock(be)
	e.Unlock()
	if !autoCommit {
		return be
	}
	e.Catalog.Commit(be, ETCreateBlock, nil)
	return be
}

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

func (e *Segment) SimpleUpgrade(exIndice []*ExternalIndex) error {
	return e.Upgrade(e.Table.Catalog.NextUncommitId(), exIndice, true)
}

func (e *Segment) HasMaxBlocks() bool {
	return e.IsSorted() || len(e.BlockSet) == int(e.Table.Schema.SegmentMaxBlocks)
}

func (e *Segment) Upgrade(tranId uint64, exIndice []*ExternalIndex, autoCommit bool) error {
	e.RLock()
	if !e.HasMaxBlocks() {
		e.RUnlock()
		return UpgradeInfullSegmentErr
	}
	if e.IsSorted() {
		return UpgradeNotNeededErr
	}
	for _, blk := range e.BlockSet {
		if !blk.IsFull() {
			return UpgradeInfullSegmentErr
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
		return UpgradeNotNeededErr
	}
	cInfo := &CommitInfo{
		TranId:   tranId,
		CommitId: tranId,
		Op:       newOp,
	}
	if exIndice == nil {
		id, ok := e.calcAppliedIndex()
		if ok {
			cInfo.AppliedIndex = &ExternalIndex{
				Id: SimpleBatchId(id),
			}
		}
	} else {
		cInfo.ExternalIndex = exIndice[0]
		if len(exIndice) > 1 {
			cInfo.PrevIndex = exIndice[1]
		}
	}
	e.onNewCommit(cInfo)
	if !autoCommit {
		return nil
	}
	e.Table.Catalog.Commit(e, ETUpgradeSegment, &e.RWMutex)
	return nil
}

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
