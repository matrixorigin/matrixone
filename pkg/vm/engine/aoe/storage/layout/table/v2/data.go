package table

import (
	"errors"
	"fmt"
	"matrixone/pkg/container/types"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"sync"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

func NewTableData(fsMgr base.IManager, indexBufMgr, mtBufMgr, sstBufMgr bmgrif.IBufferManager, meta *md.Table) iface.ITableData {
	data := &TableData{
		MTBufMgr:    mtBufMgr,
		SSTBufMgr:   sstBufMgr,
		Meta:        meta,
		FsMgr:       fsMgr,
		IndexHolder: index.NewTableHolder(indexBufMgr, meta.ID),
	}
	data.tree.Segments = make([]iface.ISegment, 0)
	data.tree.Helper = make(map[uint64]int)
	data.tree.Ids = make([]uint64, 0)
	data.OnZeroCB = data.close
	data.Ref()
	return data
}

type TableData struct {
	common.RefHelper
	tree struct {
		sync.RWMutex
		Segments   []iface.ISegment
		Ids        []uint64
		Helper     map[uint64]int
		SegmentCnt uint32
	}
	MTBufMgr    bmgrif.IBufferManager
	SSTBufMgr   bmgrif.IBufferManager
	Meta        *md.Table
	IndexHolder *index.TableHolder
	FsMgr       base.IManager
}

func (td *TableData) close() {
	for _, segment := range td.tree.Segments {
		segment.Unref()
	}
}

func (td *TableData) GetIndexHolder() *index.TableHolder {
	return td.IndexHolder
}

func (td *TableData) WeakRefRoot() iface.ISegment {
	if atomic.LoadUint32(&td.tree.SegmentCnt) == 0 {
		return nil
	}
	td.tree.RLock()
	root := td.tree.Segments[0]
	td.tree.RUnlock()
	return root
}

func (td *TableData) StongRefRoot() iface.ISegment {
	if atomic.LoadUint32(&td.tree.SegmentCnt) == 0 {
		return nil
	}
	td.tree.RLock()
	root := td.tree.Segments[0]
	td.tree.RUnlock()
	root.Ref()
	return root
}

func (td *TableData) GetID() uint64 {
	return td.Meta.ID
}

func (td *TableData) GetName() string {
	return td.Meta.Schema.Name
}

func (td *TableData) GetColTypes() []types.Type {
	return td.Meta.Schema.Types()
}

func (td *TableData) GetColTypeSize(idx int) uint64 {
	return uint64(td.Meta.Schema.ColDefs[idx].Type.Size)
}

func (td *TableData) GetMTBufMgr() bmgrif.IBufferManager {
	return td.MTBufMgr
}

func (td *TableData) GetSSTBufMgr() bmgrif.IBufferManager {
	return td.SSTBufMgr
}

func (td *TableData) GetFsManager() base.IManager {
	return td.FsMgr
}

func (td *TableData) GetSegmentCount() uint32 {
	return atomic.LoadUint32(&td.tree.SegmentCnt)
}

func (td *TableData) String() string {
	td.tree.RLock()
	defer td.tree.RUnlock()
	s := fmt.Sprintf("<Table[%d]>(SegCnt=%d)(Refs=%d)", td.Meta.ID, td.tree.SegmentCnt, td.RefCount())
	for _, seg := range td.tree.Segments {
		s = fmt.Sprintf("%s\n\t%s", s, seg.String())
	}

	return s
}

func (td *TableData) WeakRefSegment(id uint64) iface.ISegment {
	td.tree.RLock()
	defer td.tree.RUnlock()
	idx, ok := td.tree.Helper[id]
	if !ok {
		return nil
	}
	return td.tree.Segments[idx]
}

func (td *TableData) StrongRefSegment(id uint64) iface.ISegment {
	td.tree.RLock()
	defer td.tree.RUnlock()
	idx, ok := td.tree.Helper[id]
	if !ok {
		return nil
	}
	seg := td.tree.Segments[idx]
	seg.Ref()
	return seg
}

func (td *TableData) WeakRefBlock(segId, blkId uint64) iface.IBlock {
	seg := td.WeakRefSegment(segId)
	if seg == nil {
		return nil
	}
	return seg.WeakRefBlock(blkId)
}

func (td *TableData) StrongRefBlock(segId, blkId uint64) iface.IBlock {
	seg := td.WeakRefSegment(segId)
	if seg == nil {
		return nil
	}
	return seg.StrongRefBlock(blkId)
}

func (td *TableData) RegisterSegment(meta *md.Segment) (seg iface.ISegment, err error) {
	seg, err = NewSegment(td, meta)
	if err != nil {
		panic(err)
	}
	td.tree.Lock()
	defer td.tree.Unlock()
	_, ok := td.tree.Helper[meta.ID]
	if ok {
		return nil, errors.New("Duplicate seg")
	}

	if len(td.tree.Segments) != 0 {
		seg.Ref()
		td.tree.Segments[len(td.tree.Segments)-1].SetNext(seg)
	}

	td.tree.Segments = append(td.tree.Segments, seg)
	td.tree.Ids = append(td.tree.Ids, seg.GetMeta().ID)
	td.tree.Helper[meta.ID] = int(td.tree.SegmentCnt)
	atomic.AddUint32(&td.tree.SegmentCnt, uint32(1))
	seg.Ref()
	return seg, err
}

func (td *TableData) SegmentIds() []uint64 {
	ids := make([]uint64, 0, atomic.LoadUint32(&td.tree.SegmentCnt))
	td.tree.RLock()
	for _, seg := range td.tree.Segments {
		ids = append(ids, seg.GetMeta().ID)
	}
	td.tree.RUnlock()
	return ids
}

func (td *TableData) RegisterBlock(meta *md.Block) (blk iface.IBlock, err error) {
	td.tree.RLock()
	defer td.tree.RUnlock()
	idx, ok := td.tree.Helper[meta.Segment.ID]
	if !ok {
		return nil, errors.New(fmt.Sprintf("seg %d not found", meta.Segment.ID))
	}
	seg := td.tree.Segments[idx]
	blk, err = seg.RegisterBlock(meta)
	return blk, err
}

func (td *TableData) UpgradeBlock(meta *md.Block) (blk iface.IBlock, err error) {
	idx, ok := td.tree.Helper[meta.Segment.ID]
	if !ok {
		return nil, errors.New("seg not found")
	}
	seg := td.tree.Segments[idx]
	return seg.UpgradeBlock(meta)
}

func (td *TableData) UpgradeSegment(id uint64) (seg iface.ISegment, err error) {
	idx, ok := td.tree.Helper[id]
	if !ok {
		panic("logic error")
	}
	old := td.tree.Segments[idx]
	if old.GetType() != base.UNSORTED_SEG {
		panic("logic error")
	}
	if old.GetMeta().ID != id {
		panic("logic error")
	}
	meta, err := td.Meta.ReferenceSegment(id)
	if err != nil {
		return nil, err
	}
	upgradeSeg, err := old.CloneWithUpgrade(td, meta)
	if err != nil {
		panic(err)
	}

	var oldNext iface.ISegment
	if idx != len(td.tree.Segments)-1 {
		oldNext = old.GetNext()
	}
	upgradeSeg.SetNext(oldNext)

	td.tree.Lock()
	defer td.tree.Unlock()
	td.tree.Segments[idx] = upgradeSeg
	if idx > 0 {
		upgradeSeg.Ref()
		td.tree.Segments[idx-1].SetNext(upgradeSeg)
	}
	// old.SetNext(nil)
	old.Unref()
	upgradeSeg.Ref()
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
	sync.RWMutex
	Data      map[uint64]iface.ITableData
	Ids       map[uint64]bool
	Tombstone map[uint64]iface.ITableData
}

func NewTables() *Tables {
	return &Tables{
		Data:      make(map[uint64]iface.ITableData),
		Ids:       make(map[uint64]bool),
		Tombstone: make(map[uint64]iface.ITableData),
	}
}

func (ts *Tables) TableIds() (ids map[uint64]bool) {
	return ts.Ids
}

func (ts *Tables) DropTable(tid uint64) (err error) {
	ts.Lock()
	err = ts.DropTableNoLock(tid)
	ts.Unlock()
	return err
}

func (ts *Tables) DropTableNoLock(tid uint64) (err error) {
	tbl, ok := ts.Data[tid]
	if !ok {
		return errors.New(fmt.Sprintf("Specified table %d not found", tid))
	}
	ts.Tombstone[tid] = tbl
	delete(ts.Ids, tid)
	delete(ts.Data, tid)
	return nil
}

func (ts *Tables) GetTableNoLock(tid uint64) (tbl iface.ITableData, err error) {
	tbl, ok := ts.Data[tid]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Specified table %d not found", tid))
	}
	return tbl, err
}

func (ts *Tables) WeakRefTable(tid uint64) (tbl iface.ITableData, err error) {
	ts.RLock()
	tbl, err = ts.GetTableNoLock(tid)
	ts.RUnlock()
	return tbl, err
}

func (ts *Tables) StrongRefTable(tid uint64) (tbl iface.ITableData, err error) {
	ts.RLock()
	tbl, err = ts.GetTableNoLock(tid)
	if tbl != nil {
		tbl.Ref()
	}
	ts.RUnlock()
	return tbl, err
}

func (ts *Tables) CreateTable(tbl iface.ITableData) (err error) {
	ts.Lock()
	err = ts.CreateTableNoLock(tbl)
	ts.Unlock()
	return err
}

func (ts *Tables) CreateTableNoLock(tbl iface.ITableData) (err error) {
	_, ok := ts.Data[tbl.GetID()]
	if ok {
		return errors.New(fmt.Sprintf("Dup table %d found", tbl.GetID()))
	}
	ts.Ids[tbl.GetID()] = true
	ts.Data[tbl.GetID()] = tbl
	return nil
}

func (ts *Tables) Replay(fsMgr base.IManager, indexBufMgr, mtBufMgr, sstBufMgr bmgrif.IBufferManager, info *md.MetaInfo) error {
	for _, meta := range info.Tables {
		tbl := NewTableData(fsMgr, indexBufMgr, mtBufMgr, sstBufMgr, meta)
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
		if err := ts.CreateTable(tbl); err != nil {
			return err
		}
	}
	return nil
}
