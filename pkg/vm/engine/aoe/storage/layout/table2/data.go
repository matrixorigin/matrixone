package table

import (
	"errors"
	"fmt"
	"matrixone/pkg/container/types"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table2/iface"
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
	data.OnZeroCB = data.close
	return data
}

type TableData struct {
	common.RefHelper
	tree struct {
		sync.RWMutex
		Segments   []iface.ISegment
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
	s := fmt.Sprintf("<Table[%d]>(SegCnt=%d)", td.Meta.ID, td.tree.SegmentCnt)
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
	td.tree.Segments = append(td.tree.Segments, seg)
	td.tree.Helper[meta.ID] = int(td.tree.SegmentCnt)
	atomic.AddUint32(&td.tree.SegmentCnt, uint32(1))
	seg.Ref()
	return seg, err
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
	seg = td.tree.Segments[idx]
	if seg.GetType() != base.UNSORTED_SEG {
		panic("logic error")
	}
	if seg.GetMeta().ID != id {
		panic("logic error")
	}
	meta, err := td.Meta.ReferenceSegment(id)
	if err != nil {
		return nil, err
	}
	upgradeSeg, err := seg.CloneWithUpgrade(td, meta)
	if err != nil {
		panic(err)
	}
	td.tree.Lock()
	defer td.tree.Unlock()
	td.tree.Segments[idx] = upgradeSeg
	seg.Unref()
	upgradeSeg.Ref()
	return upgradeSeg, nil
}
