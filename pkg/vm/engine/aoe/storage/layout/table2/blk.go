package table

import (
	"fmt"
	// log "github.com/sirupsen/logrus"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table2/col"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"sync"
)

type Block struct {
	common.RefHelper
	data struct {
		sync.RWMutex
		Columns []col.IColumnBlock
		Helper  map[string]int
		Next    iface.IBlock
	}
	Meta        *md.Block
	MTBufMgr    bmgrif.IBufferManager
	SSTBufMgr   bmgrif.IBufferManager
	IndexHolder *index.BlockHolder
	FsMgr       base.IManager
	SegmentFile base.ISegmentFile
	Type        base.BlockType
}

func NewBlock(host iface.ISegment, meta *md.Block) (iface.IBlock, error) {
	blk := &Block{
		Meta:      meta,
		MTBufMgr:  host.GetMTBufMgr(),
		SSTBufMgr: host.GetSSTBufMgr(),
		FsMgr:     host.GetFsManager(),
	}

	blk.data.Columns = make([]col.IColumnBlock, 0)
	blk.data.Helper = make(map[string]int)
	blk.OnZeroCB = blk.noRefCB

	var blkType base.BlockType
	if meta.DataState < md.FULL {
		blkType = base.TRANSIENT_BLK
	} else if host.GetType() == base.UNSORTED_SEG {
		blkType = base.PERSISTENT_BLK
	} else {
		blkType = base.PERSISTENT_SORTED_BLK
	}
	indexHolder := host.GetIndexHolder().RegisterBlock(meta.AsCommonID().AsBlockID(), blkType, nil)

	blk.Ref()
	err := blk.initColumns()
	if err != nil {
		return nil, err
	}

	blk.SegmentFile = host.GetSegmentFile()
	blk.IndexHolder = indexHolder
	blk.Type = blkType
	return blk, nil
}

func (blk *Block) initColumns() error {
	for idx, colDef := range blk.Meta.Segment.Schema.ColDefs {
		blk.Ref()
		colBlk := col.NewStdColumnBlock(blk, idx)
		blk.data.Helper[colDef.Name] = len(blk.data.Columns)
		blk.data.Columns = append(blk.data.Columns, colBlk)
	}
	return nil
}

func (blk *Block) GetType() base.BlockType {
	return blk.Type
}

func (blk *Block) noRefCB() {
	for _, colBlk := range blk.data.Columns {
		// log.Infof("destroy blk %d, col %d, refs %d", blk.Meta.ID, idx, colBlk.RefCount())
		colBlk.Unref()
	}
	if blk.data.Next != nil {
		blk.data.Next.Unref()
		blk.data.Next = nil
	}
	// log.Infof("destroy blk %d", blk.Meta.ID)
}

func (blk *Block) GetMTBufMgr() bmgrif.IBufferManager {
	return blk.MTBufMgr
}

func (blk *Block) GetSSTBufMgr() bmgrif.IBufferManager {
	return blk.SSTBufMgr
}

func (blk *Block) GetFsManager() base.IManager {
	return blk.FsMgr
}

func (blk *Block) GetIndexHolder() *index.BlockHolder {
	return blk.IndexHolder
}

func (blk *Block) GetMeta() *md.Block {
	return blk.Meta
}

func (blk *Block) CloneWithUpgrade(host iface.ISegment, meta *md.Block) (iface.IBlock, error) {
	if blk.Type == base.PERSISTENT_SORTED_BLK {
		panic("logic error")
	}
	if meta.DataState != md.FULL {
		panic("logic error")
	}

	blkId := meta.AsCommonID().AsBlockID()
	var (
		newType base.BlockType
	)
	indexHolder := host.GetIndexHolder().GetBlock(meta.ID)
	newIndexHolder := false

	switch blk.Type {
	case base.TRANSIENT_BLK:
		newType = base.PERSISTENT_BLK
		if indexHolder == nil {
			indexHolder = host.GetIndexHolder().RegisterBlock(blkId, newType, nil)
			newIndexHolder = true
		} else if indexHolder.Type < newType {
			indexHolder = host.GetIndexHolder().UpgradeBlock(meta.ID, newType)
			newIndexHolder = true
		}
	case base.PERSISTENT_BLK:
		newType = base.PERSISTENT_SORTED_BLK
		if indexHolder == nil {
			indexHolder = host.GetIndexHolder().RegisterBlock(blkId, newType, nil)
			newIndexHolder = true
		} else if indexHolder.Type < newType {
			indexHolder = host.GetIndexHolder().UpgradeBlock(meta.ID, newType)
			newIndexHolder = true
		}
	default:
		panic("logic error")
	}
	cloned := &Block{
		Meta:        meta,
		MTBufMgr:    blk.MTBufMgr,
		SSTBufMgr:   blk.SSTBufMgr,
		FsMgr:       blk.FsMgr,
		IndexHolder: indexHolder,
		Type:        newType,
		SegmentFile: host.GetSegmentFile(),
	}
	cloned.data.Columns = make([]col.IColumnBlock, 0)
	cloned.data.Helper = make(map[string]int)
	if newIndexHolder {
		indexHolder.Init(cloned.SegmentFile)
	}
	blk.cloneWithUpgradeColumns(cloned)
	cloned.OnZeroCB = cloned.noRefCB
	cloned.Ref()
	host.Unref()
	return cloned, nil
}

func (blk *Block) cloneWithUpgradeColumns(cloned *Block) {
	for name, idx := range blk.data.Helper {
		colBlk := blk.data.Columns[idx]
		cloned.Ref()
		clonedCol := colBlk.CloneWithUpgrade(cloned)
		cloned.data.Helper[name] = len(cloned.data.Columns)
		cloned.data.Columns = append(cloned.data.Columns, clonedCol)
	}
}

func (blk *Block) GetSegmentFile() base.ISegmentFile {
	return blk.SegmentFile
}

func (blk *Block) String() string {
	s := fmt.Sprintf("<Blk[%d]>(ColBlk=%d)(Refs=%d)", blk.Meta.ID, len(blk.data.Columns), blk.RefCount())
	for _, colBlk := range blk.data.Columns {
		s = fmt.Sprintf("%s/n\t%s", s, colBlk.String())
	}
	return s
}

func (blk *Block) GetBlockHandle() iface.IBlockHandle {
	h := new(BlockHandle)
	h.Columns = make(map[int]iface.IColBlockHandle, len(blk.data.Columns))
	blk.Ref()
	h.Host = blk
	for idx, colBlk := range blk.data.Columns {
		h.Columns[idx] = colBlk.GetBlockHandle()
	}
	return h
}

func (blk *Block) SetNext(next iface.IBlock) {
	blk.data.Lock()
	defer blk.data.Unlock()
	if blk.data.Next != nil {
		blk.data.Next.Unref()
	}
	blk.data.Next = next
}

func (blk *Block) GetNext() iface.IBlock {
	blk.data.RLock()
	if blk.data.Next != nil {
		blk.data.Next.Ref()
	}
	r := blk.data.Next
	blk.data.RUnlock()
	return r
}
