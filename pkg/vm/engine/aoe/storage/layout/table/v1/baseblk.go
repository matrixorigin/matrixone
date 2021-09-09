package table

import (
	"fmt"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type sllnode = common.SLLNode

type baseBlock struct {
	sllnode
	meta        *metadata.Block
	host        iface.ISegment
	typ         base.BlockType
	indexholder *index.BlockHolder
}

func newBaseBlock(host iface.ISegment, meta *metadata.Block) *baseBlock {
	blk := &baseBlock{
		host:    host,
		meta:    meta,
		sllnode: *common.NewSLLNode(nil),
	}
	if meta.DataState < metadata.FULL {
		blk.typ = base.TRANSIENT_BLK
	} else if host.GetType() == base.UNSORTED_SEG {
		blk.typ = base.PERSISTENT_BLK
	} else {
		blk.typ = base.PERSISTENT_SORTED_BLK
	}
	return blk
}

func (blk *baseBlock) GetMeta() *metadata.Block { return blk.meta }
func (blk *baseBlock) GetType() base.BlockType  { return blk.typ }
func (blk *baseBlock) GetRowCount() uint64      { return blk.meta.GetCount() }
func (blk *baseBlock) IsMutable() bool {
	if blk.typ >= base.PERSISTENT_BLK {
		return false
	}
	return true
}

func (blk *baseBlock) WeakRefSegment() iface.ISegment      { return blk.host }
func (blk *baseBlock) GetMTBufMgr() bmgrif.IBufferManager  { return blk.host.GetMTBufMgr() }
func (blk *baseBlock) GetSSTBufMgr() bmgrif.IBufferManager { return blk.host.GetSSTBufMgr() }
func (blk *baseBlock) GetFsManager() base.IManager         { return blk.host.GetFsManager() }
func (blk *baseBlock) GetSegmentFile() base.ISegmentFile   { return blk.host.GetSegmentFile() }

func (blk *baseBlock) GetIndexHolder() *index.BlockHolder {
	blk.indexholder.Ref()
	return blk.indexholder
}

func (blk *baseBlock) SetNext(next iface.IBlock) {
	blk.sllnode.SetNextNode(next)
}

func (blk *baseBlock) GetNext() iface.IBlock {
	next := blk.sllnode.GetNextNode()
	if next == nil {
		return nil
	}
	return next.(iface.IBlock)
}

func (blk *baseBlock) release() {
	if blk.indexholder != nil {
		blk.indexholder.Unref()
	}
	blk.sllnode.ReleaseNextNode()
}

func (blk *baseBlock) preUpgradeAssert() {
	if blk.typ == base.PERSISTENT_SORTED_BLK {
		panic("logic error")
	}
	if blk.meta.DataState != md.FULL {
		panic(fmt.Sprintf("blk data state is %d", blk.meta.DataState))
	}
}

func (blk *baseBlock) upgrade(host iface.ISegment, meta *metadata.Block) (*baseBlock, error) {
	blk.preUpgradeAssert()

	id := meta.AsCommonID().AsBlockID()
	upgraded := &baseBlock{
		meta:    meta,
		host:    host,
		sllnode: *common.NewSLLNode(nil),
	}

	upgraded.indexholder = host.GetIndexHolder().StrongRefBlock(meta.ID)
	switch blk.typ {
	case base.TRANSIENT_BLK:
		upgraded.typ = base.PERSISTENT_BLK
		if upgraded.indexholder == nil {
			upgraded.indexholder = host.GetIndexHolder().RegisterBlock(id, upgraded.typ, nil)
		} else if upgraded.indexholder.Type < upgraded.typ {
			upgraded.indexholder.Unref()
			upgraded.indexholder = host.GetIndexHolder().UpgradeBlock(meta.ID, upgraded.typ)
		}
	case base.PERSISTENT_BLK:
		upgraded.typ = base.PERSISTENT_SORTED_BLK
		if upgraded.indexholder == nil {
			upgraded.indexholder = host.GetIndexHolder().RegisterBlock(id, upgraded.typ, nil)
		} else if upgraded.indexholder.Type < upgraded.typ {
			upgraded.indexholder.Unref()
			upgraded.indexholder = host.GetIndexHolder().UpgradeBlock(meta.ID, upgraded.typ)
		}
	default:
		panic("logic error")
	}
	return upgraded, nil
}
