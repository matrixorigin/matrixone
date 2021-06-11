package col

import (
	"fmt"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

type StdColumnBlock struct {
	ColumnBlock
	Part IColumnPart
}

func NewStdColumnBlock(seg IColumnSegment, meta *md.Block) IColumnBlock {
	var blkType BlockType
	var segFile ldio.ISegmentFile
	fsMgr := seg.GetFsManager()
	if meta.DataState < md.FULL {
		blkType = TRANSIENT_BLK
	} else if seg.GetSegmentType() == UNSORTED_SEG {
		blkType = PERSISTENT_BLK
		segFile = fsMgr.GetUnsortedFile(seg.GetID())
		if segFile == nil {
			_, err := fsMgr.RegisterUnsortedFiles(seg.GetID())
			if err != nil {
				panic(err)
			}
		}
	} else {
		blkType = PERSISTENT_SORTED_BLK
		segFile = fsMgr.GetUnsortedFile(seg.GetID())
		if segFile != nil {
			fsMgr.UpgradeFile(seg.GetID())
		} else {
			_, err := fsMgr.RegisterSortedFiles(seg.GetID())
			if err != nil {
				panic(err)
			}
		}
	}
	blk := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			ID:     *meta.AsCommonID(),
			Type:   blkType,
			ColIdx: seg.GetColIdx(),
			Meta:   meta,
		},
	}
	seg.Append(blk.Ref())
	return blk.Ref()
}

func (blk *StdColumnBlock) CloneWithUpgrade(seg IColumnSegment, newMeta *md.Block) IColumnBlock {
	if blk.Type == PERSISTENT_SORTED_BLK {
		panic("logic error")
	}
	if newMeta.DataState != md.FULL {
		panic(fmt.Sprintf("logic error: blk %s DataState=%d", newMeta.AsCommonID().BlockString(), newMeta.DataState))
	}
	fsMgr := seg.GetFsManager()
	var newType BlockType
	switch blk.Type {
	case TRANSIENT_BLK:
		newType = PERSISTENT_BLK
		segFile := fsMgr.GetUnsortedFile(seg.GetID())
		if segFile == nil {
			_, err := fsMgr.RegisterUnsortedFiles(seg.GetID())
			if err != nil {
				panic(err)
			}
		}
	case PERSISTENT_BLK:
		newType = PERSISTENT_SORTED_BLK
	case MOCK_BLK:
		newType = MOCK_PERSISTENT_BLK
	case MOCK_PERSISTENT_BLK:
		newType = MOCK_PERSISTENT_SORTED_BLK
	}
	cloned := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			ID:     blk.ID,
			Type:   newType,
			ColIdx: seg.GetColIdx(),
			Meta:   newMeta,
		},
	}
	cloned.Ref()
	blk.RLock()
	part := blk.Part.CloneWithUpgrade(cloned.Ref(), seg.GetSSTBufMgr(), seg.GetFsManager())
	blk.RUnlock()
	if part == nil {
		panic("logic error")
	}
	cloned.Part = part
	seg.UnRef()
	return cloned
}

func (blk *StdColumnBlock) Ref() IColumnBlock {
	atomic.AddInt64(&blk.Refs, int64(1))
	// newRef := atomic.AddInt64(&blk.Refs, int64(1))
	// log.Infof("StdColumnBlock %s Ref to %d, %p", blk.ID.BlockString(), newRef, blk)
	return blk
}

func (blk *StdColumnBlock) UnRef() {
	newRef := atomic.AddInt64(&blk.Refs, int64(-1))
	// log.Infof("StdColumnBlock %s Unref to %d, %p", blk.ID.BlockString(), newRef, blk)
	if newRef == 0 {
		blk.Close()
	}
}

func (blk *StdColumnBlock) GetPartRoot() IColumnPart {
	blk.RLock()
	p := blk.Part
	blk.RUnlock()
	return p
}

func (blk *StdColumnBlock) Append(part IColumnPart) {
	blk.Lock()
	defer blk.Unlock()
	if !blk.ID.IsSameBlock(part.GetID()) || blk.Part != nil {
		panic("logic error")
	}
	blk.Part = part
}

func (blk *StdColumnBlock) Close() error {
	// log.Infof("Close StdBlk %s Refs=%d, %p", blk.ID.BlockString(), blk.Refs, blk)
	if blk.Next != nil {
		blk.Next.UnRef()
		blk.Next = nil
	}
	if blk.Part != nil {
		blk.Part.Close()
	}
	blk.Part = nil
	return nil
}

func (blk *StdColumnBlock) InitScanCursor(cursor *ScanCursor) error {
	blk.RLock()
	defer blk.RUnlock()
	if blk.Part != nil {
		cursor.Current = blk.Part
		// return blk.Part.InitScanCursor(cursor)
	}
	return nil
}

func (blk *StdColumnBlock) String() string {
	partID := blk.Part.GetNodeID()
	s := fmt.Sprintf("Std[%s](T=%s)[Part=%s](Refs=%d)", blk.ID.BlockString(), blk.Type.String(), partID.String(), blk.GetRefs())
	return s
}
