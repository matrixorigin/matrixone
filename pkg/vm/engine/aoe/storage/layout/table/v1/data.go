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

package table

import (
	"errors"
	"fmt"
	"matrixone/pkg/container/types"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync"
	"sync/atomic"
)

var (
	NotExistErr = errors.New("not exist error")
)

func newTableData(host *Tables, meta *md.Table) *tableData {
	data := &tableData{
		meta:        meta,
		host:        host,
		indexHolder: index.NewTableHolder(host.IndexBufMgr, meta.ID),
	}
	if mutFactory != nil {
		data.blkFactory = newAltBlockFactory(mutFactory, data)
	}
	data.tree.segments = make([]iface.ISegment, 0)
	data.tree.helper = make(map[uint64]int)
	data.tree.ids = make([]uint64, 0)
	data.OnZeroCB = data.close
	data.Ref()
	return data
}

type tableData struct {
	common.RefHelper
	tree struct {
		sync.RWMutex
		segments   []iface.ISegment
		ids        []uint64
		helper     map[uint64]int
		segmentCnt uint32
		rowCount   uint64
	}
	host        *Tables
	meta        *md.Table
	indexHolder *index.TableHolder
	blkFactory  iface.IBlockFactory
}

func (td *tableData) close() {
	td.indexHolder.Unref()
	for _, segment := range td.tree.segments {
		segment.Unref()
	}
	// log.Infof("table %d noref", td.meta.ID)
}

func (td *tableData) Ref() {
	td.RefHelper.Ref()
}

func (td *tableData) Unref() {
	td.RefHelper.Unref()
}

func (td *tableData) GetIndexHolder() *index.TableHolder {
	return td.indexHolder
}

func (td *tableData) WeakRefRoot() iface.ISegment {
	if atomic.LoadUint32(&td.tree.segmentCnt) == 0 {
		return nil
	}
	td.tree.RLock()
	root := td.tree.segments[0]
	td.tree.RUnlock()
	return root
}

func (td *tableData) StongRefRoot() iface.ISegment {
	if atomic.LoadUint32(&td.tree.segmentCnt) == 0 {
		return nil
	}
	td.tree.RLock()
	root := td.tree.segments[0]
	td.tree.RUnlock()
	root.Ref()
	return root
}

func (td *tableData) GetID() uint64 {
	return td.meta.ID
}

func (td *tableData) GetName() string {
	return td.meta.Schema.Name
}

func (td *tableData) GetColTypes() []types.Type {
	return td.meta.Schema.Types()
}

func (td *tableData) GetColTypeSize(idx int) uint64 {
	return uint64(td.meta.Schema.ColDefs[idx].Type.Size)
}

func (td *tableData) GetMTBufMgr() bmgrif.IBufferManager {
	return td.host.MTBufMgr
}

func (td *TableData) GetBlockFactory() iface.IBlockFactory {
	return td.blkFactory
}

func (td *tableData) GetSSTBufMgr() bmgrif.IBufferManager {
	return td.host.SSTBufMgr
}

func (td *tableData) GetFsManager() base.IManager {
	return td.host.FsMgr
}

func (td *tableData) GetSegmentCount() uint32 {
	return atomic.LoadUint32(&td.tree.segmentCnt)
}

func (td *tableData) GetMeta() *md.Table {
	return td.meta
}

func (td *tableData) String() string {
	td.tree.RLock()
	defer td.tree.RUnlock()
	s := fmt.Sprintf("<Table[%d]>(SegCnt=%d)(Refs=%d)", td.meta.ID, td.tree.segmentCnt, td.RefCount())
	for _, seg := range td.tree.segments {
		s = fmt.Sprintf("%s\n\t%s", s, seg.String())
	}

	return s
}

func (td *tableData) WeakRefSegment(id uint64) iface.ISegment {
	td.tree.RLock()
	defer td.tree.RUnlock()
	idx, ok := td.tree.helper[id]
	if !ok {
		return nil
	}
	return td.tree.segments[idx]
}

func (td *tableData) StrongRefSegment(id uint64) iface.ISegment {
	td.tree.RLock()
	defer td.tree.RUnlock()
	idx, ok := td.tree.helper[id]
	if !ok {
		return nil
	}
	seg := td.tree.segments[idx]
	seg.Ref()
	return seg
}

func (td *tableData) WeakRefBlock(segId, blkId uint64) iface.IBlock {
	seg := td.WeakRefSegment(segId)
	if seg == nil {
		return nil
	}
	return seg.WeakRefBlock(blkId)
}

func (td *tableData) StrongRefBlock(segId, blkId uint64) iface.IBlock {
	seg := td.WeakRefSegment(segId)
	if seg == nil {
		return nil
	}
	return seg.StrongRefBlock(blkId)
}

func (td *tableData) RegisterSegment(meta *md.Segment) (seg iface.ISegment, err error) {
	seg, err = newSegment(td, meta)
	if err != nil {
		panic(err)
	}
	td.tree.Lock()
	defer td.tree.Unlock()
	_, ok := td.tree.helper[meta.ID]
	if ok {
		return nil, errors.New("Duplicate seg")
	}

	if len(td.tree.segments) != 0 {
		seg.Ref()
		td.tree.segments[len(td.tree.segments)-1].SetNext(seg)
	}

	td.tree.segments = append(td.tree.segments, seg)
	td.tree.ids = append(td.tree.ids, seg.GetMeta().ID)
	td.tree.helper[meta.ID] = int(td.tree.segmentCnt)
	atomic.AddUint32(&td.tree.segmentCnt, uint32(1))
	seg.Ref()
	return seg, err
}

func (td *tableData) Size(attr string) uint64 {
	size := uint64(0)
	segCnt := atomic.LoadUint32(&td.tree.segmentCnt)
	var seg iface.ISegment
	for i := 0; i < int(segCnt); i++ {
		td.tree.RLock()
		seg = td.tree.segments[i]
		td.tree.RUnlock()
		size += seg.Size(attr)
	}
	return size
}

func (td *tableData) GetSegmentedIndex() (id uint64, ok bool) {
	ts := td.meta.Info.GetCheckpointTime()
	id, ok = td.meta.CreatedIndex, true
	if td.meta.IsDeleted(ts) {
		return td.meta.DeletedIndex, true
	}

	segCnt := atomic.LoadUint32(&td.tree.segmentCnt)
	for i := int(segCnt) - 1; i >= 0; i-- {
		td.tree.RLock()
		seg := td.tree.segments[i]
		td.tree.RUnlock()
		id, ok := seg.GetSegmentedIndex()
		if ok {
			return id, ok
		}
	}
	return id, ok
}

func (td *tableData) GetReplayIndex() *md.LogIndex {
	return td.meta.ReplayIndex
}

func (td *tableData) SegmentIds() []uint64 {
	ids := make([]uint64, 0, atomic.LoadUint32(&td.tree.segmentCnt))
	td.tree.RLock()
	for _, seg := range td.tree.segments {
		ids = append(ids, seg.GetMeta().ID)
	}
	td.tree.RUnlock()
	return ids
}

func (td *tableData) GetRowCount() uint64 {
	return atomic.LoadUint64(&td.tree.rowCount)
}

func (td *tableData) AddRows(rows uint64) uint64 {
	return atomic.AddUint64(&td.tree.rowCount, rows)
}

func (td *tableData) initReplayCtx() {
	if td.tree.segmentCnt == 0 {
		return
	}
	var ctx *md.LogIndex
	for segIdx := int(td.tree.segmentCnt) - 1; segIdx >= 0; segIdx-- {
		seg := td.tree.segments[segIdx]
		if ctx = seg.GetReplayIndex(); ctx != nil {
			break
		}
	}
	td.meta.ReplayIndex = ctx
}

func (td *tableData) InitReplay() {
	td.initRowCount()
	td.initReplayCtx()
}

func (td *tableData) initRowCount() {
	for _, seg := range td.tree.segments {
		td.tree.rowCount += seg.GetRowCount()
	}
}

func (td *tableData) RegisterBlock(meta *md.Block) (blk iface.IBlock, err error) {
	td.tree.RLock()
	defer td.tree.RUnlock()
	idx, ok := td.tree.helper[meta.Segment.ID]
	if !ok {
		return nil, errors.New(fmt.Sprintf("seg %d not found", meta.Segment.ID))
	}
	seg := td.tree.segments[idx]
	blk, err = seg.RegisterBlock(meta)
	return blk, err
}

func (td *tableData) UpgradeBlock(meta *md.Block) (blk iface.IBlock, err error) {
	idx, ok := td.tree.helper[meta.Segment.ID]
	if !ok {
		return nil, errors.New("seg not found")
	}
	seg := td.tree.segments[idx]
	return seg.UpgradeBlock(meta)
}

func (td *tableData) UpgradeSegment(id uint64) (seg iface.ISegment, err error) {
	idx, ok := td.tree.helper[id]
	if !ok {
		panic("logic error")
	}
	old := td.tree.segments[idx]
	if old.GetType() != base.UNSORTED_SEG {
		panic(fmt.Sprintf("old segment %d type is %d", id, old.GetType()))
	}
	if old.GetMeta().ID != id {
		panic("logic error")
	}
	meta, err := td.meta.ReferenceSegment(id)
	if err != nil {
		return nil, err
	}
	upgradeSeg, err := old.CloneWithUpgrade(td, meta)
	if err != nil {
		panic(err)
	}

	var oldNext iface.ISegment
	if idx != len(td.tree.segments)-1 {
		oldNext = old.GetNext()
	}
	upgradeSeg.SetNext(oldNext)

	td.tree.Lock()
	defer td.tree.Unlock()
	td.tree.segments[idx] = upgradeSeg
	if idx > 0 {
		upgradeSeg.Ref()
		td.tree.segments[idx-1].SetNext(upgradeSeg)
	}
	// old.SetNext(nil)
	upgradeSeg.Ref()
	old.Unref()
	return upgradeSeg, nil
}

func MockSegments(meta *md.Table, tblData iface.ITableData) []uint64 {
	segs := make([]uint64, 0)
	for _, segMeta := range meta.Segments {
		seg, err := tblData.RegisterSegment(segMeta)
		if err != nil {
			panic(err)
		}
		for _, blkMeta := range segMeta.Blocks {
			blk, err := seg.RegisterBlock(blkMeta)
			if err != nil {
				panic(err)
			}
			blk.Unref()
		}
		segs = append(segs, seg.GetMeta().ID)
		seg.Unref()
	}

	return segs
}

type Tables struct {
	Mu        *sync.RWMutex
	Data      map[uint64]iface.ITableData
	ids       map[uint64]bool
	Tombstone map[uint64]iface.ITableData

	FsMgr base.IManager

	MTBufMgr, SSTBufMgr, IndexBufMgr bmgrif.IBufferManager
}

func NewTables(mu *sync.RWMutex, fsMgr base.IManager, mtBufMgr, sstBufMgr, indexBufMgr bmgrif.IBufferManager) *Tables {
	return &Tables{
		Mu:          mu,
		Data:        make(map[uint64]iface.ITableData),
		ids:         make(map[uint64]bool),
		Tombstone:   make(map[uint64]iface.ITableData),
		MTBufMgr:    mtBufMgr,
		SSTBufMgr:   sstBufMgr,
		IndexBufMgr: indexBufMgr,
		FsMgr:       fsMgr,
	}
}

func (ts *Tables) String() string {
	ts.Mu.RLock()
	defer ts.Mu.RUnlock()
	s := fmt.Sprintf("<Tables>[Cnt=%d]", len(ts.Data))
	for _, td := range ts.Data {
		s = fmt.Sprintf("%s\n%s", s, td.String())
	}
	return s
}

func (ts *Tables) TableIds() (ids map[uint64]bool) {
	return ts.ids
}

func (ts *Tables) DropTable(tid uint64) (tbl iface.ITableData, err error) {
	ts.Mu.Lock()
	tbl, err = ts.DropTableNoLock(tid)
	ts.Mu.Unlock()
	return tbl, err
}

func (ts *Tables) DropTableNoLock(tid uint64) (tbl iface.ITableData, err error) {
	tbl, ok := ts.Data[tid]
	if !ok {
		// return errors.New(fmt.Sprintf("Specified table %d not found", tid))
		return tbl, NotExistErr
	}
	// ts.Tombstone[tid] = tbl
	delete(ts.ids, tid)
	delete(ts.Data, tid)
	return tbl, nil
}

func (ts *Tables) GetTableNoLock(tid uint64) (tbl iface.ITableData, err error) {
	tbl, ok := ts.Data[tid]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Specified table %d not found", tid))
	}
	return tbl, err
}

func (ts *Tables) WeakRefTable(tid uint64) (tbl iface.ITableData, err error) {
	ts.Mu.RLock()
	tbl, err = ts.GetTableNoLock(tid)
	ts.Mu.RUnlock()
	return tbl, err
}

func (ts *Tables) StrongRefTable(tid uint64) (tbl iface.ITableData, err error) {
	ts.Mu.RLock()
	tbl, err = ts.GetTableNoLock(tid)
	if tbl != nil {
		tbl.Ref()
	}
	ts.Mu.RUnlock()
	return tbl, err
}

func (ts *Tables) RegisterTable(meta *md.Table) (iface.ITableData, error) {
	tbl := newTableData(ts, meta)
	ts.Mu.Lock()
	defer ts.Mu.Unlock()
	if err := ts.CreateTableNoLock(tbl); err != nil {
		tbl.Unref()
		return nil, err
	}
	return tbl, nil
}

func (ts *Tables) CreateTableNoLock(tbl iface.ITableData) (err error) {
	_, ok := ts.Data[tbl.GetID()]
	if ok {
		return errors.New(fmt.Sprintf("Dup table %d found", tbl.GetID()))
	}
	ts.ids[tbl.GetID()] = true
	ts.Data[tbl.GetID()] = tbl
	return nil
}

func (ts *Tables) Replay(fsMgr base.IManager, indexBufMgr, mtBufMgr, sstBufMgr bmgrif.IBufferManager, info *md.MetaInfo) error {
	for _, meta := range info.Tables {
		tbl, err := ts.RegisterTable(meta)
		if err != nil {
			return err
		}
		activeSeg := meta.GetActiveSegment()
		for _, segMeta := range meta.Segments {
			if activeSeg != nil && activeSeg.ID < segMeta.ID {
				break
			}

			seg, err := tbl.RegisterSegment(segMeta)
			if err != nil {
				panic(err)
			}
			defer seg.Unref()

			activeBlk := segMeta.GetActiveBlk()
			for _, blkMeta := range segMeta.Blocks {
				if activeBlk != nil && activeBlk.ID <= blkMeta.ID {
					break
				}
				blk, err := seg.RegisterBlock(blkMeta)
				if err != nil {
					panic(err)
				}
				defer blk.Unref()
			}
		}
		tbl.InitReplay()
	}
	return nil
}
