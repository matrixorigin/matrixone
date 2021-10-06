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
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/logstore"
	"sync"
)

type tableLogEntry struct {
	BaseEntry
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
	BaseEntry
	Schema     *Schema
	SegmentSet []*Segment
	IdIndex    map[uint64]int `json:"-"`
	Catalog    *Catalog       `json:"-"`
}

func NewTableEntry(catalog *Catalog, schema *Schema, tranId uint64, exIndex *ExternalIndex) *Table {
	schema.BlockMaxRows = catalog.Cfg.BlockMaxRows
	schema.SegmentMaxBlocks = catalog.Cfg.SegmentMaxBlocks
	e := &Table{
		BaseEntry: BaseEntry{
			Id: catalog.NextTableId(),
			CommitInfo: &CommitInfo{
				TranId:        tranId,
				CommitId:      tranId,
				SSLLNode:      *common.NewSSLLNode(),
				Op:            OpCreate,
				ExternalIndex: exIndex,
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
		BaseEntry: BaseEntry{
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

func (e *Table) CommittedView(id uint64) *Table {
	// TODO: if baseEntry op is drop, should introduce an index to
	// indicate weather to return nil
	baseEntry := e.UseCommitted(id)
	if baseEntry == nil {
		return nil
	}
	view := &Table{
		Schema:     e.Schema,
		BaseEntry:  *baseEntry,
		SegmentSet: make([]*Segment, 0),
	}
	e.RLock()
	segs := make([]*Segment, 0, len(e.SegmentSet))
	for _, seg := range e.SegmentSet {
		segs = append(segs, seg)
	}
	e.RUnlock()
	for _, seg := range segs {
		segView := seg.CommittedView(id)
		if segView == nil {
			continue
		}
		view.SegmentSet = append(view.SegmentSet, segView)
	}
	return view
}

func (e *Table) rebuild(catalog *Catalog) {
	e.Catalog = catalog
	e.IdIndex = make(map[uint64]int)
	for i, seg := range e.SegmentSet {
		catalog.Sequence.TryUpdateSegmentId(seg.Id)
		seg.rebuild(e)
		e.IdIndex[seg.Id] = i
	}
}

func (e *Table) HardDelete() {
	cInfo := &CommitInfo{
		CommitId: e.Catalog.NextUncommitId(),
		Op:       OpHardDelete,
		SSLLNode: *common.NewSSLLNode(),
	}
	e.Lock()
	defer e.Unlock()
	if e.IsHardDeletedLocked() {
		logutil.Warnf("HardDelete %d but already hard deleted", e.Id)
		return
	}
	if !e.IsSoftDeletedLocked() {
		panic("logic error: Cannot hard delete entry that not soft deleted")
	}
	e.onNewCommit(cInfo)
	e.Catalog.Commit(e, ETHardDeleteTable, &e.RWMutex)
}

func (e *Table) SimpleSoftDelete(exIndex *ExternalIndex) {
	e.SoftDelete(e.Catalog.NextUncommitId(), exIndex, true)
}

func (e *Table) SoftDelete(tranId uint64, exIndex *ExternalIndex, autoCommit bool) {
	cInfo := &CommitInfo{
		TranId:        tranId,
		CommitId:      tranId,
		ExternalIndex: exIndex,
		Op:            OpSoftDelete,
		SSLLNode:      *common.NewSSLLNode(),
	}
	e.Lock()
	defer e.Unlock()
	if e.IsSoftDeletedLocked() {
		return
	}
	e.onNewCommit(cInfo)

	if !autoCommit {
		return
	}
	e.Catalog.Commit(e, ETSoftDeleteTable, &e.RWMutex)
}

func (e *Table) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *Table) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, e)
}

func (e *Table) String() string {
	buf, _ := e.Marshal()
	return string(buf)
}

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
		panic("not supported")
	}
	logEntry := logstore.NewBaseEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}

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

// Note: Only support one producer
func (e *Table) SimpleCreateBlock(exIndex *ExternalIndex) (*Block, *Segment) {
	var prevSeg *Segment
	currSeg := e.SimpleGetCurrSegment()
	if currSeg == nil || currSeg.HasMaxBlocks() {
		prevSeg = currSeg
		currSeg = e.SimpleCreateSegment(exIndex)
	}
	blk := currSeg.SimpleCreateBlock(exIndex)
	return blk, prevSeg
}

func (e *Table) SimpleCreateSegment(exIndex *ExternalIndex) *Segment {
	return e.CreateSegment(e.Catalog.NextUncommitId(), exIndex, true)
}

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

func (e *Table) SimpleGetSegmentCount() int {
	e.RLock()
	defer e.RUnlock()
	return len(e.SegmentSet)
}

func (e *Table) CreateSegment(tranId uint64, exIndex *ExternalIndex, autoCommit bool) *Segment {
	se := newSegmentEntry(e.Catalog, e, tranId, exIndex)
	e.Lock()
	e.onNewSegment(se)
	e.Unlock()
	if !autoCommit {
		return se
	}
	e.Catalog.Commit(se, ETCreateSegment, nil)
	return se
}

func (e *Table) onNewSegment(entry *Segment) {
	e.IdIndex[entry.Id] = len(e.SegmentSet)
	e.SegmentSet = append(e.SegmentSet, entry)
}

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

func (e *Table) SimpleGetSegment(id uint64) *Segment {
	e.RLock()
	defer e.RUnlock()
	return e.GetSegment(id, MinUncommitId)
}

func (e *Table) GetSegment(id, tranId uint64) *Segment {
	pos, ok := e.IdIndex[id]
	if !ok {
		return nil
	}
	entry := e.SegmentSet[pos]
	return entry
}

func (e *Table) PString(level PPLevel) string {
	s := fmt.Sprintf("<Table>(%s)(Cnt=%d)", e.BaseEntry.PString(level), len(e.SegmentSet))
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
			Id: SimpleBatchId(common.NextGlobalSeqNum()),
		}
	}
	tbl, err := catalog.SimpleCreateTable(schema, idx)
	if err != nil {
		panic(err)
	}

	var activeSeg *Segment
	for i := uint64(0); i < blkCnt; i++ {
		if activeSeg == nil {
			activeSeg = tbl.SimpleCreateSegment(nil)
		}
		activeSeg.SimpleCreateBlock(nil)
		if len(activeSeg.BlockSet) == int(tbl.Schema.SegmentMaxBlocks) {
			activeSeg = nil
		}
	}
	return tbl
}
