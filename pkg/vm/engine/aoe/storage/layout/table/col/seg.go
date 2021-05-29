package col

import (
	"errors"
	"fmt"
	"io"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	mock "matrixone/pkg/vm/engine/aoe/storage/mock/type"
	"runtime"
	"sync"

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
	GetID() layout.ID
	GetBlockIDs() []layout.ID
	GetBlockRoot() IColumnBlock
	GetPartRoot() IColumnPart
	GetRowCount() uint64
	String() string
	ToString(verbose bool) string
	Append(blk IColumnBlock)
	GetColIdx() int
	GetSegmentType() SegmentType
	CloneWithUpgrade() IColumnSegment
	UpgradeBlock(id layout.ID) (IColumnBlock, error)
	GetBlock(id layout.ID) IColumnBlock
	InitScanCursor(cursor *ScanCursor) error
	RegisterBlock(bufMgr bmgrif.IBufferManager, id layout.ID, maxRows uint64) (blk IColumnBlock, err error)
}

type ColumnSegment struct {
	sync.RWMutex
	ID       layout.ID
	Next     IColumnSegment
	Blocks   []IColumnBlock
	RowCount uint64
	IDMap    map[layout.ID]int
	Idx      int
	Type     SegmentType
	ColType  mock.ColType
}

func NewColumnSegment(id layout.ID, colIdx int, colType mock.ColType, segType SegmentType) IColumnSegment {
	seg := &ColumnSegment{
		ID:      id,
		IDMap:   make(map[layout.ID]int, 0),
		Idx:     colIdx,
		Type:    segType,
		ColType: colType,
	}
	runtime.SetFinalizer(seg, func(o IColumnSegment) {
		id := o.GetID()
		o.SetNext(nil)
		log.Infof("[GC]: ColumnSegment %s [%d]", id.SegmentString(), o.GetSegmentType())
		o.Close()
	})
	return seg
}

func (seg *ColumnSegment) GetColIdx() int {
	return seg.Idx
}

func (seg *ColumnSegment) GetSegmentType() SegmentType {
	seg.RLock()
	defer seg.RUnlock()
	return seg.Type
}

func (seg *ColumnSegment) GetBlock(id layout.ID) IColumnBlock {
	seg.RLock()
	defer seg.RUnlock()
	idx, ok := seg.IDMap[id]
	if !ok {
		return nil
	}
	return seg.Blocks[idx]
}

func (seg *ColumnSegment) UpgradeBlock(id layout.ID) (IColumnBlock, error) {
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
		ID:       seg.ID,
		IDMap:    seg.IDMap,
		RowCount: seg.RowCount,
		Next:     seg.Next,
		Type:     SORTED_SEG,
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

func (seg *ColumnSegment) RegisterBlock(bufMgr bmgrif.IBufferManager, id layout.ID, maxRows uint64) (blk IColumnBlock, err error) {
	blk = NewStdColumnBlock(seg, id, TRANSIENT_BLK)
	_ = NewColumnPart(bufMgr, blk, id, maxRows, uint64(seg.ColType.Size()))
	// TODO: StrColumnBlock
	return blk, err
}

func (seg *ColumnSegment) GetID() layout.ID {
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

func (seg *ColumnSegment) GetBlockIDs() []layout.ID {
	seg.RLock()
	defer seg.RUnlock()
	var ids []layout.ID
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
