package col

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"sync/atomic"
)

type StdColumnBlock struct {
	ColumnBlock
	Part IColumnPart
}

func NewStdColumnBlock(seg IColumnSegment, meta *md.Block) IColumnBlock {
	var (
		blkType base.BlockType
		segFile ldio.ISegmentFile
	)
	fsMgr := seg.GetFsManager()
	indexHolder := seg.GetIndexHolder().GetBlock(meta.ID)

	if meta.DataState < md.FULL {
		blkType = base.TRANSIENT_BLK
		if indexHolder == nil {
			indexHolder = index.NewBlockHolder(meta.ID, blkType)
			seg.GetIndexHolder().AddBlock(indexHolder)
		}
	} else if seg.GetSegmentType() == base.UNSORTED_SEG {
		blkType = base.PERSISTENT_BLK
		segFile = fsMgr.GetUnsortedFile(seg.GetID())
		if segFile == nil {
			_, err := fsMgr.RegisterUnsortedFiles(seg.GetID())
			if err != nil {
				panic(err)
			}
		}
		if indexHolder == nil {
			indexHolder = index.NewBlockHolder(meta.ID, blkType)
			seg.GetIndexHolder().AddBlock(indexHolder)
		}
	} else {
		blkType = base.PERSISTENT_SORTED_BLK
		segFile = fsMgr.GetUnsortedFile(seg.GetID())
		if segFile != nil {
			fsMgr.UpgradeFile(seg.GetID())
		} else {
			_, err := fsMgr.RegisterSortedFiles(seg.GetID())
			if err != nil {
				panic(err)
			}
		}
		if indexHolder == nil {
			indexHolder = index.NewBlockHolder(meta.ID, blkType)
			seg.GetIndexHolder().AddBlock(indexHolder)
		}
	}
	blk := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			ID:          *meta.AsCommonID(),
			Type:        blkType,
			ColIdx:      seg.GetColIdx(),
			Meta:        meta,
			IndexHolder: indexHolder,
		},
	}
	// seg.Append(blk.Ref())
	return blk.Ref()
}

func (blk *StdColumnBlock) CloneWithUpgrade(seg IColumnSegment, newMeta *md.Block) IColumnBlock {
	if blk.Type == base.PERSISTENT_SORTED_BLK {
		panic("logic error")
	}
	if newMeta.DataState != md.FULL {
		panic(fmt.Sprintf("logic error: blk %s DataState=%d", newMeta.AsCommonID().BlockString(), newMeta.DataState))
	}
	fsMgr := seg.GetFsManager()
	indexHolder := seg.GetIndexHolder().GetBlock(newMeta.ID)
	var newType base.BlockType
	switch blk.Type {
	case base.TRANSIENT_BLK:
		newType = base.PERSISTENT_BLK
		segFile := fsMgr.GetUnsortedFile(seg.GetID())
		if segFile == nil {
			_, err := fsMgr.RegisterUnsortedFiles(seg.GetID())
			if err != nil {
				panic(err)
			}
		}
		if indexHolder == nil {
			indexHolder = index.NewBlockHolder(newMeta.ID, newType)
			seg.GetIndexHolder().AddBlock(indexHolder)
		} else if indexHolder.Type < newType {
			indexHolder = seg.GetIndexHolder().UpgradeBlock(newMeta.ID, newType)
		}
	case base.PERSISTENT_BLK:
		newType = base.PERSISTENT_SORTED_BLK
		segFile := fsMgr.GetUnsortedFile(seg.GetID())
		if segFile != nil {
			fsMgr.UpgradeFile(seg.GetID())
		} else {
			segFile = fsMgr.GetSortedFile(seg.GetID())
		}
		if segFile == nil {
			panic("logic error")
		}
		if indexHolder == nil {
			indexHolder = index.NewBlockHolder(newMeta.ID, newType)
			seg.GetIndexHolder().AddBlock(indexHolder)
		} else if indexHolder.Type < newType {
			indexHolder = seg.GetIndexHolder().UpgradeBlock(newMeta.ID, newType)
		}
	default:
		panic("logic error")
	}
	cloned := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			ID:          blk.ID,
			Type:        newType,
			ColIdx:      seg.GetColIdx(),
			Meta:        newMeta,
			IndexHolder: indexHolder,
		},
	}
	cloned.Ref()
	blk.RLock()
	part := blk.Part.CloneWithUpgrade(cloned.Ref(), seg.GetSSTBufMgr(), seg.GetFsManager())
	blk.RUnlock()
	if part == nil {
		log.Errorf("logic error")
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
	s := fmt.Sprintf("<Std[%s](T=%s)[Part=%d](Refs=%d)(Size=%d)>", blk.Meta.String(), blk.Type.String(), partID, blk.GetRefs(), blk.Meta.Count)
	return s
}
