package col

import (
	"errors"
	"fmt"
	"matrixone/pkg/container/types"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
)

type IColumnData interface {
	String() string
	ToString(depth uint64) string
	InitScanCursor(cursor *ScanCursor) error
	Append(seg IColumnSegment) error
	DropSegment(id layout.ID) (seg IColumnSegment, err error)
	// AppendBlock(blk IColumnBlock) error
	// AppendPart(part IColumnPart) error
	UpgradeBlock(blkID layout.ID) IColumnBlock
	UpgradeSegment(segID layout.ID) IColumnSegment
	SegmentCount() uint64
	GetSegmentRoot() IColumnSegment
	GetSegmentTail() IColumnSegment
	GetColIdx() int
	RegisterSegment(id layout.ID) (seg IColumnSegment, err error)
	RegisterBlock(bufMgr bmgrif.IBufferManager, id layout.ID, maxRows uint64) (blk IColumnBlock, err error)
}

type ColumnData struct {
	Type     types.Type
	Idx      int
	RowCount uint64
	SegTree  ISegmentTree
}

func NewColumnData(col_type types.Type, col_idx int) IColumnData {
	data := &ColumnData{
		Type:    col_type,
		Idx:     col_idx,
		SegTree: NewSegmentTree(),
	}
	return data
}

func (cdata *ColumnData) GetColIdx() int {
	return cdata.Idx
}

func (cdata *ColumnData) GetSegmentRoot() IColumnSegment {
	return cdata.SegTree.GetRoot()
}

func (cdata *ColumnData) GetSegmentTail() IColumnSegment {
	return cdata.SegTree.GetTail()
}

func (cdata *ColumnData) DropSegment(id layout.ID) (seg IColumnSegment, err error) {
	return cdata.SegTree.DropSegment(id)
}

func (cdata *ColumnData) SegmentCount() uint64 {
	return cdata.SegTree.Depth()
}

func (cdata *ColumnData) Append(seg IColumnSegment) error {
	if seg.GetColIdx() != cdata.Idx {
		panic("logic error")
	}
	return cdata.SegTree.Append(seg)
}

func (cdata *ColumnData) RegisterSegment(id layout.ID) (seg IColumnSegment, err error) {
	seg = NewColumnSegment(id, cdata.Idx, cdata.Type, UNSORTED_SEG)
	err = cdata.Append(seg)
	return seg, err
}

func (cdata *ColumnData) RegisterBlock(bufMgr bmgrif.IBufferManager, id layout.ID, maxRows uint64) (blk IColumnBlock, err error) {
	seg := cdata.GetSegmentTail()
	if seg == nil {
		err = errors.New(fmt.Sprintf("cannot register blk: %s", id.BlockString()))
		return blk, err
	}
	return seg.RegisterBlock(bufMgr, id, maxRows)
}

// func (cdata *ColumnData) AppendBlock(blk IColumnBlock) error {
// 	tail_seg := cdata.SegTree.GetTail()
// 	id := blk.GetID()
// 	if tail_seg == nil || !id.IsSameSegment(tail_seg.GetID()) {
// 		err := cdata.Append(blk.GetSegment())
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func (cdata *ColumnData) AppendPart(part IColumnPart) error {
// 	err := cdata.AppendBlock(part.GetBlock())
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func (cdata *ColumnData) UpgradeBlock(blkID layout.ID) IColumnBlock {
	return cdata.SegTree.UpgradeBlock(blkID)
}

func (cdata *ColumnData) UpgradeSegment(segID layout.ID) IColumnSegment {
	return cdata.SegTree.UpgradeSegment(segID)
}

func (cdata *ColumnData) InitScanCursor(cursor *ScanCursor) error {
	err := cursor.Close()
	if err != nil {
		return err
	}
	root := cdata.SegTree.GetRoot()
	if root == nil {
		return nil
	}
	blk := root.GetBlockRoot()
	if blk == nil {
		return nil
	}
	cursor.Current = blk.GetPartRoot()
	return nil
}

func (cdata *ColumnData) String() string {
	return fmt.Sprintf("CData(%d,%d,%d)[SegCnt=%d]", cdata.Type, cdata.Idx, cdata.RowCount, cdata.SegmentCount())
}

func (cdata *ColumnData) ToString(depth uint64) string {
	s := fmt.Sprintf("CData(%d,%d,%d)[SegCnt=%d]", cdata.Type, cdata.Idx, cdata.RowCount, cdata.SegmentCount())

	return fmt.Sprintf("%s\n%s", s, cdata.SegTree.ToString(depth))
}
