package table

import (
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/index"
	// "matrixone/pkg/vm/engine/aoe/storage/layout/table2/col"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"sync"
)

type IColumnBlock interface {
	common.IRef
}

type Block struct {
	common.RefHelper
	data struct {
		sync.RWMutex
		Columns []IColumnBlock
		Helper  map[string]int
	}
	Meta        *md.Block
	MTBufMgr    bmgrif.IBufferManager
	SSTBufMgr   bmgrif.IBufferManager
	IndexHolder *index.BlockHolder
	FsMgr       base.IManager
	Type        base.BlockType
}

func NewBlock(host iface.ISegment, meta *md.Block) (iface.IBlock, error) {
	blk := &Block{
		Meta:      meta,
		MTBufMgr:  host.GetMTBufMgr(),
		SSTBufMgr: host.GetSSTBufMgr(),
		FsMgr:     host.GetFsManager(),
	}

	blk.data.Columns = make([]IColumnBlock, 0)
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

	err := blk.initColumns()
	if err != nil {
		return nil, err
	}

	blk.IndexHolder = indexHolder
	blk.Type = blkType
	blk.Ref()
	return blk, nil
}

func (blk *Block) initColumns() error {
	return nil
}

func (blk *Block) GetType() base.BlockType {
	return blk.Type
}

func (blk *Block) noRefCB() {
	for _, colBlk := range blk.data.Columns {
		colBlk.Unref()
	}
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
