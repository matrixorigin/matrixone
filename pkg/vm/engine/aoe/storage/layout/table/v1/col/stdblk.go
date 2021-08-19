package col

import (
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/logutil"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync/atomic"
)

type StdColumnBlock struct {
	ColumnBlock
	Part IColumnPart
}

func NewStdColumnBlock(seg IColumnSegment, meta *md.Block) IColumnBlock {
	var (
		blkType base.BlockType
		segFile base.ISegmentFile
		err     error
	)
	fsMgr := seg.GetFsManager()
	indexHolder := seg.GetIndexHolder().StrongRefBlock(meta.ID)
	newIndexHolder := false

	if meta.DataState < md.FULL {
		blkType = base.TRANSIENT_BLK
		if indexHolder == nil {
			indexHolder = seg.GetIndexHolder().RegisterBlock(meta.AsCommonID().AsBlockID(), blkType, nil)
			newIndexHolder = true
		}
	} else if seg.GetSegmentType() == base.UNSORTED_SEG {
		blkType = base.PERSISTENT_BLK
		segFile = fsMgr.GetUnsortedFile(seg.GetID())
		if segFile == nil {
			segFile, err = fsMgr.RegisterUnsortedFiles(seg.GetID())
			if err != nil {
				panic(err)
			}
		}
		if indexHolder == nil {
			indexHolder = seg.GetIndexHolder().RegisterBlock(meta.AsCommonID().AsBlockID(), blkType, nil)
			newIndexHolder = true
		}
	} else {
		blkType = base.PERSISTENT_SORTED_BLK
		segFile = fsMgr.GetUnsortedFile(seg.GetID())
		if segFile != nil {
			fsMgr.UpgradeFile(seg.GetID())
		} else {
			segFile = fsMgr.GetSortedFile(seg.GetID())
			if segFile == nil {
				_, err := fsMgr.RegisterSortedFiles(seg.GetID())
				if err != nil {
					panic(err)
				}
			}
		}
		if indexHolder == nil {
			indexHolder = seg.GetIndexHolder().RegisterBlock(meta.AsCommonID().AsBlockID(), blkType, nil)
			newIndexHolder = true
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

	if newIndexHolder && segFile != nil {
		indexHolder.Init(segFile)
	}
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
	indexHolder := seg.GetIndexHolder().StrongRefBlock(newMeta.ID)
	newIndexHolder := false
	var err error
	var newType base.BlockType
	var segFile base.ISegmentFile
	switch blk.Type {
	case base.TRANSIENT_BLK:
		newType = base.PERSISTENT_BLK
		segFile = fsMgr.GetUnsortedFile(seg.GetID())
		if segFile == nil {
			segFile, err = fsMgr.RegisterUnsortedFiles(seg.GetID())
			if err != nil {
				panic(err)
			}
		}
		if indexHolder == nil {
			indexHolder = seg.GetIndexHolder().RegisterBlock(newMeta.AsCommonID().AsBlockID(), newType, nil)
			newIndexHolder = true
		} else if indexHolder.Type < newType {
			indexHolder = seg.GetIndexHolder().UpgradeBlock(newMeta.ID, newType)
			newIndexHolder = true
		}
	case base.PERSISTENT_BLK:
		newType = base.PERSISTENT_SORTED_BLK
		segFile = fsMgr.GetUnsortedFile(seg.GetID())
		if segFile != nil {
			segFile = fsMgr.UpgradeFile(seg.GetID())
		} else {
			segFile = fsMgr.GetSortedFile(seg.GetID())
		}
		if segFile == nil {
			panic("logic error")
		}
		if indexHolder == nil {
			indexHolder = seg.GetIndexHolder().RegisterBlock(newMeta.AsCommonID().AsBlockID(), newType, nil)
			newIndexHolder = true
		} else if indexHolder.Type < newType {
			indexHolder = seg.GetIndexHolder().UpgradeBlock(newMeta.ID, newType)
			newIndexHolder = true
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
		logutil.Error("logic error")
		panic("logic error")
	}
	if newIndexHolder {
		indexHolder.Init(segFile)
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
	if blk.IndexHolder != nil {
		blk.IndexHolder.Unref()
		blk.IndexHolder = nil
	}
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
	s := fmt.Sprintf("<Std[%s](T=%s)(Refs=%d)(Size=%d)>", blk.Meta.String(), blk.Type.String(), blk.GetRefs(), blk.Meta.Count)
	return s
}
