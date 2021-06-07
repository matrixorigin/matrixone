package col

import (
	"errors"
	"fmt"
	"io"
	"matrixone/pkg/container/types"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"runtime"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type SegmentType uint8

const (
	UNSORTED_SEG SegmentType = iota
	SORTED_SEG
)

type IColumnSegment interface {
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
	GetSegmentType() SegmentType
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	CloneWithUpgrade() IColumnSegment
	UpgradeBlock(id common.ID) (IColumnBlock, error)
	GetBlock(id common.ID) IColumnBlock
	InitScanCursor(cursor *ScanCursor) error
	RegisterBlock(id common.ID, maxRows uint64) (blk IColumnBlock, err error)
}

type ColumnSegment struct {
	sync.RWMutex
	ID        common.ID
	Next      IColumnSegment
	Blocks    []IColumnBlock
	RowCount  uint64
	IDMap     map[common.ID]int
	Idx       int
	Type      SegmentType
	ColType   types.Type
	MTBufMgr  bmgrif.IBufferManager
	SSTBufMgr bmgrif.IBufferManager
}

func NewColumnSegment(mtBufMgr, sstBufMgr bmgrif.IBufferManager, id common.ID, colIdx int, colType types.Type, segType SegmentType) IColumnSegment {
	seg := &ColumnSegment{
		ID:        id,
		IDMap:     make(map[common.ID]int, 0),
		Idx:       colIdx,
		Type:      segType,
		ColType:   colType,
		MTBufMgr:  mtBufMgr,
		SSTBufMgr: sstBufMgr,
	}
	runtime.SetFinalizer(seg, func(o IColumnSegment) {
		id := o.GetID()
		o.SetNext(nil)
		log.Infof("[GC]: ColumnSegment %s [%d]", id.SegmentString(), o.GetSegmentType())
		o.Close()
	})
	return seg
}

func (seg *ColumnSegment) GetMTBufMgr() bmgrif.IBufferManager {
	return seg.MTBufMgr
}

func (seg *ColumnSegment) GetSSTBufMgr() bmgrif.IBufferManager {
	return seg.SSTBufMgr
}

func (seg *ColumnSegment) GetColIdx() int {
	return seg.Idx
}

func (seg *ColumnSegment) GetSegmentType() SegmentType {
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
	return seg.Blocks[idx]
}

func (seg *ColumnSegment) UpgradeBlock(id common.ID) (IColumnBlock, error) {
	if seg.Type != UNSORTED_SEG {
		panic("logic error")
	}
	if !seg.ID.IsSameSegment(id) {
		panic("logic error")
	}
	idx, ok := seg.IDMap[id]
	if !ok {
		panic("logic error")
	}
	old := seg.Blocks[idx]
	upgradeBlk := old.CloneWithUpgrade(seg)
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
		seg.Blocks[idx-1].SetNext(upgradeBlk)
	}
	return upgradeBlk, nil
}

func (seg *ColumnSegment) CloneWithUpgrade() IColumnSegment {
	if seg.Type != UNSORTED_SEG {
		panic("logic error")
	}
	cloned := &ColumnSegment{
		ID:        seg.ID,
		IDMap:     seg.IDMap,
		RowCount:  seg.RowCount,
		Next:      seg.Next,
		Type:      SORTED_SEG,
		ColType:   seg.ColType,
		MTBufMgr:  seg.MTBufMgr,
		SSTBufMgr: seg.SSTBufMgr,
	}
	var prev IColumnBlock
	for _, blk := range seg.Blocks {
		cur := blk.CloneWithUpgrade(cloned)
		cloned.Blocks = append(cloned.Blocks, cur)
		if prev != nil {
			prev.SetNext(cur)
		}
		prev = cur
	}
	runtime.SetFinalizer(cloned, func(o IColumnSegment) {
		o.SetNext(nil)
		// id := o.GetID()
		// log.Infof("[GC]: ColumnSegment %s [%d]", id.SegmentString(), o.GetSegmentType())
	})
	return cloned
}

func (seg *ColumnSegment) GetRowCount() uint64 {
	seg.RLock()
	defer seg.RUnlock()
	return seg.RowCount
}

func (seg *ColumnSegment) SetNext(next IColumnSegment) {
	seg.Lock()
	defer seg.Unlock()
	seg.Next = next
}

func (seg *ColumnSegment) GetNext() IColumnSegment {
	seg.RLock()
	defer seg.RUnlock()
	return seg.Next
}

func (seg *ColumnSegment) Close() error {
	return nil
}

func (seg *ColumnSegment) RegisterBlock(id common.ID, maxRows uint64) (blk IColumnBlock, err error) {
	blk = NewStdColumnBlock(seg, id, TRANSIENT_BLK)
	part := NewColumnPart(seg.MTBufMgr, blk, id, maxRows, uint64(seg.ColType.Size))
	for part == nil {
		part = NewColumnPart(seg.MTBufMgr, blk, id, maxRows, uint64(seg.ColType.Size))
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
	// TODO: StrColumnBlock
	return blk, err
}

func (seg *ColumnSegment) GetID() common.ID {
	return seg.ID
}

func (seg *ColumnSegment) Append(blk IColumnBlock) {
	if !seg.ID.IsSameSegment(blk.GetID()) {
		panic("logic error")
	}
	seg.Lock()
	defer seg.Unlock()
	if len(seg.Blocks) > 0 {
		seg.Blocks[len(seg.Blocks)-1].SetNext(blk)
	}
	seg.Blocks = append(seg.Blocks, blk)
	seg.IDMap[blk.GetID()] = len(seg.Blocks) - 1
	seg.RowCount += blk.GetRowCount()
}

func (seg *ColumnSegment) GetBlockRoot() IColumnBlock {
	seg.RLock()
	defer seg.RUnlock()
	if len(seg.Blocks) == 0 {
		return nil
	}
	return seg.Blocks[0]
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
		return fmt.Sprintf("Seg(%v)(%d)[HasNext:%v]", seg.ID, seg.Type, seg.Next != nil)
	}
	return fmt.Sprintf("(%v, %v)", seg.ID, seg.Type)
}
