package col

import (
	"errors"
	"fmt"
	"io"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	it "matrixone/pkg/vm/engine/aoe/storage/iterator"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"sync"
	"sync/atomic"
	"time"
	// log "github.com/sirupsen/logrus"
)

type IColumnSegment interface {
	it.IResources
	io.Closer
	sync.Locker
	GetNext() IColumnSegment
	SetNext(next IColumnSegment)
	GetID() common.ID
	GetBlockIDs() []common.ID
	GetBlockRoot() IColumnBlock
	GetPartRoot() IColumnPart
	GetRowCount() uint64
	String() string
	ToString(verbose bool) string
	Append(blk IColumnBlock)
	GetColIdx() int
	GetSegmentType() base.SegmentType
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	GetIndexHolder() *index.SegmentHolder
	CloneWithUpgrade(*md.Segment, *index.TableHolder) IColumnSegment
	UpgradeBlock(*md.Block) (IColumnBlock, error)
	GetBlock(id common.ID) IColumnBlock
	InitScanCursor(cursor *ScanCursor) error
	RegisterBlock(*md.Block) (blk IColumnBlock, err error)
	Ref() IColumnSegment
	UnRef()
	GetRefs() int64
	GetMeta() *md.Segment
	GetFsManager() base.IManager
	EvalFilter(*index.FilterCtx) error
}

type ColumnSegment struct {
	sync.RWMutex
	it.BaseResources
	Refs        int64
	ID          common.ID
	ColIdx      int
	Next        IColumnSegment
	Blocks      []IColumnBlock
	IDMap       map[common.ID]int
	Type        base.SegmentType
	MTBufMgr    bmgrif.IBufferManager
	SSTBufMgr   bmgrif.IBufferManager
	Meta        *md.Segment
	FsMgr       base.IManager
	IndexHolder *index.SegmentHolder
}

func NewColumnSegment(tblHolder *index.TableHolder, fsMgr base.IManager, mtBufMgr, sstBufMgr bmgrif.IBufferManager, colIdx int, meta *md.Segment) IColumnSegment {
	segType := base.UNSORTED_SEG
	indexSegType := base.UNSORTED_SEG
	if meta.DataState == md.SORTED {
		segType = base.SORTED_SEG
		indexSegType = base.SORTED_SEG
	}
	seg := &ColumnSegment{
		ID:        *meta.AsCommonID(),
		ColIdx:    colIdx,
		Refs:      0,
		IDMap:     make(map[common.ID]int, 0),
		Type:      segType,
		Meta:      meta,
		MTBufMgr:  mtBufMgr,
		SSTBufMgr: sstBufMgr,
		FsMgr:     fsMgr,
	}
	seg.Impl = seg
	seg.IndexHolder = tblHolder.GetSegment(seg.ID.SegmentID)
	var err error
	if seg.IndexHolder == nil {
		segHolder := tblHolder.RegisterSegment(seg.ID.AsSegmentID(), indexSegType, nil)
		seg.IndexHolder = segHolder
		id := seg.ID.AsSegmentID()
		segFile := fsMgr.GetUnsortedFile(id)
		if segType == base.UNSORTED_SEG {
			if segFile == nil {
				segFile, err = fsMgr.RegisterUnsortedFiles(seg.GetID())
				if err != nil {
					panic(err)
				}
			}
			seg.IndexHolder.Init(segFile)
		} else {
			if segFile != nil {
				fsMgr.UpgradeFile(seg.GetID())
			} else {
				segFile = fsMgr.GetSortedFile(seg.GetID())
				if segFile == nil {
					segFile, err = fsMgr.RegisterSortedFiles(seg.GetID())
					if err != nil {
						panic(err)
					}
				}
			}
			seg.IndexHolder.Init(segFile)
		}
	}

	return seg.Ref()
}

func (seg *ColumnSegment) EvalFilter(ctx *index.FilterCtx) error {
	return seg.IndexHolder.EvalFilter(seg.ColIdx, ctx)
}

func (seg *ColumnSegment) HandleResources(handle it.HandleT) error {
	var err error
	for _, blk := range seg.Blocks {
		err = handle(blk)
		if err != nil {
			return err
		}
	}
	return err
}

func (seg *ColumnSegment) GetIndexHolder() *index.SegmentHolder {
	return seg.IndexHolder
}

func (seg *ColumnSegment) GetFsManager() base.IManager {
	return seg.FsMgr
}

func (seg *ColumnSegment) GetMeta() *md.Segment {
	return seg.Meta
}

func (seg *ColumnSegment) GetMTBufMgr() bmgrif.IBufferManager {
	return seg.MTBufMgr
}

func (seg *ColumnSegment) GetSSTBufMgr() bmgrif.IBufferManager {
	return seg.SSTBufMgr
}

func (seg *ColumnSegment) Ref() IColumnSegment {
	atomic.AddInt64(&seg.Refs, int64(1))
	// log.Infof("Ref ColSegment %s to refs=%d %p", seg.ID.SegmentString(), newRef, seg)
	return seg
}

func (seg *ColumnSegment) UnRef() {
	newRef := atomic.AddInt64(&seg.Refs, int64(-1))
	// log.Infof("UnRef ColSegment %s to refs=%d %p", seg.ID.SegmentString(), newRef, seg)
	if newRef == 0 {
		seg.Close()
	} else if newRef < 0 {
		panic("logic error")
	}
}

func (seg *ColumnSegment) GetRefs() int64 {
	return atomic.LoadInt64(&seg.Refs)
}

func (seg *ColumnSegment) GetColIdx() int {
	return seg.ColIdx
}

func (seg *ColumnSegment) GetSegmentType() base.SegmentType {
	seg.RLock()
	defer seg.RUnlock()
	return seg.Type
}

func (seg *ColumnSegment) GetBlock(id common.ID) IColumnBlock {
	seg.RLock()
	defer seg.RUnlock()
	idx, ok := seg.IDMap[id]
	if !ok {
		return nil
	}
	return seg.Blocks[idx].Ref()
}

func (seg *ColumnSegment) UpgradeBlock(newMeta *md.Block) (IColumnBlock, error) {
	if seg.Type != base.UNSORTED_SEG {
		panic("logic error")
	}
	id := newMeta.AsCommonID()
	if !seg.ID.IsSameSegment(*id) {
		panic("logic error")
	}
	idx, ok := seg.IDMap[*id]
	if !ok {
		panic(fmt.Sprintf("logic error: blk %s not found in seg %s", id.BlockString(), seg.ID.SegmentString()))
	}
	old := seg.Blocks[idx]
	upgradeBlk := old.CloneWithUpgrade(seg.Ref(), newMeta)
	if upgradeBlk == nil {
		return nil, errors.New(fmt.Sprintf("Cannot upgrade blk: %s", id.BlockString()))
	}
	var old_next IColumnBlock
	if idx != len(seg.Blocks)-1 {
		old_next = old.GetNext()
	}
	upgradeBlk.SetNext(old_next)
	seg.Lock()
	defer seg.Unlock()
	seg.Blocks[idx] = upgradeBlk
	if idx > 0 {
		seg.Blocks[idx-1].SetNext(upgradeBlk.Ref())
	}
	old.UnRef()
	return upgradeBlk.Ref(), nil
}

func (seg *ColumnSegment) CloneWithUpgrade(meta *md.Segment, indexTblHolder *index.TableHolder) IColumnSegment {
	if seg.Type != base.UNSORTED_SEG {
		panic("logic error")
	}
	// TODO: temp commit it, will be enabled later
	// if meta.DataState != md.SORTED {
	// 	panic("logic error")
	// }
	cloned := &ColumnSegment{
		ID:        seg.ID,
		IDMap:     seg.IDMap,
		Next:      seg.Next,
		Type:      base.SORTED_SEG,
		MTBufMgr:  seg.MTBufMgr,
		SSTBufMgr: seg.SSTBufMgr,
		Meta:      meta,
		FsMgr:     seg.GetFsManager(),
	}
	cloned.Ref()
	segIndexHolder := indexTblHolder.GetSegment(seg.ID.SegmentID)
	if segIndexHolder == nil {
		panic("logic error")
	}
	if segIndexHolder.Type == base.UNSORTED_SEG {
		segIndexHolder = indexTblHolder.UpgradeSegment(seg.ID.SegmentID, base.SORTED_SEG)
		id := seg.ID.AsSegmentID()
		segFile := cloned.FsMgr.GetSortedFile(id)
		if segFile == nil {
			segFile = cloned.FsMgr.UpgradeFile(id)
			if segFile == nil {
				panic("logic error")
			}
		}
		seg.IndexHolder.Init(segFile)
	}
	cloned.IndexHolder = segIndexHolder
	var prev IColumnBlock
	for _, blk := range seg.Blocks {
		newBlkMeta, err := meta.ReferenceBlock(blk.GetID().BlockID)
		if err != nil {
			panic(err)
		}
		cur := blk.CloneWithUpgrade(cloned.Ref(), newBlkMeta)
		cloned.Blocks = append(cloned.Blocks, cur)
		if prev != nil {
			prev.SetNext(cur.Ref())
		}
		prev = cur
	}
	return cloned
}

func (seg *ColumnSegment) GetRowCount() uint64 {
	return atomic.LoadUint64(&seg.Meta.Count)
}

func (seg *ColumnSegment) SetNext(next IColumnSegment) {
	seg.Lock()
	if seg.Next != nil {
		seg.Next.UnRef()
	}
	seg.Next = next
	seg.Unlock()
}

func (seg *ColumnSegment) GetNext() IColumnSegment {
	seg.RLock()
	if seg.Next != nil {
		seg.Next.Ref()
	}
	r := seg.Next
	seg.RUnlock()
	return r
}

func (seg *ColumnSegment) Close() error {
	// log.Infof("Close segment %s refs=%d", seg.ID.SegmentString(), seg.Refs)
	for _, blk := range seg.Blocks {
		blk.UnRef()
	}
	seg.Blocks = nil
	return nil
}

func (seg *ColumnSegment) RegisterBlock(blkMeta *md.Block) (blk IColumnBlock, err error) {
	blk = NewStdColumnBlock(seg, blkMeta)
	part := NewColumnPart(seg.FsMgr, seg.MTBufMgr, blk.Ref(), blk.GetID(),
		blkMeta.Segment.Info.Conf.BlockMaxRows*uint64(seg.Meta.Schema.ColDefs[seg.ColIdx].Type.Size))
	for part == nil {
		part = NewColumnPart(seg.FsMgr, seg.MTBufMgr, blk.Ref(), blk.GetID(),
			blkMeta.Segment.Info.Conf.BlockMaxRows*uint64(seg.Meta.Schema.ColDefs[seg.ColIdx].Type.Size))
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
	seg.Append(blk.Ref())
	// TODO: StrColumnBlock
	return blk, err
}

func (seg *ColumnSegment) GetID() common.ID {
	return seg.ID
}

func (seg *ColumnSegment) Append(blk IColumnBlock) {
	if !seg.ID.IsSameSegment(blk.GetID()) {
		blk.UnRef()
		panic("logic error")
	}
	seg.Lock()
	defer seg.Unlock()
	if len(seg.Blocks) > 0 {
		seg.Blocks[len(seg.Blocks)-1].SetNext(blk.Ref())
	}
	seg.Blocks = append(seg.Blocks, blk)
	seg.IDMap[blk.GetID()] = len(seg.Blocks) - 1
}

func (seg *ColumnSegment) GetBlockRoot() IColumnBlock {
	seg.RLock()
	defer seg.RUnlock()
	if len(seg.Blocks) == 0 {
		return nil
	}
	return seg.Blocks[0].Ref()
}

func (seg *ColumnSegment) GetPartRoot() IColumnPart {
	seg.RLock()
	defer seg.RUnlock()
	if len(seg.Blocks) == 0 {
		return nil
	}
	return seg.Blocks[0].GetPartRoot()
}

func (seg *ColumnSegment) InitScanCursor(cursor *ScanCursor) error {
	seg.RLock()
	if len(seg.Blocks) == 0 {
		return nil
	}
	blk := seg.Blocks[0]
	seg.RUnlock()
	return blk.InitScanCursor(cursor)
}

func (seg *ColumnSegment) GetBlockIDs() []common.ID {
	seg.RLock()
	defer seg.RUnlock()
	var ids []common.ID
	for _, blk := range seg.Blocks {
		ids = append(ids, blk.GetID())
	}
	return ids
}

func (seg *ColumnSegment) String() string {
	return seg.ToString(true)
}

func (seg *ColumnSegment) ToString(verbose bool) string {
	if verbose {
		s := fmt.Sprintf("Seg(%v)(%d)[HasNext:%v]", seg.ID.String(), seg.Type, seg.Next != nil)
		for _, blk := range seg.Blocks {
			s = fmt.Sprintf("%s\n\t%s", s, blk.String())
		}
		return s
	}
	return fmt.Sprintf("(%v, %v)", seg.ID, seg.Type)
}
