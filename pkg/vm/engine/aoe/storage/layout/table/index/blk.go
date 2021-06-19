package index

import (
	"fmt"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync"
)

type BlockHolder struct {
	RefHelper
	ID common.ID
	sync.RWMutex
	Indexes     []*Node
	Type        base.BlockType
	BufMgr      mgrif.IBufferManager
	Inited      bool
	PostCloseCB PostCloseCB
}

func newBlockHolder(bufMgr mgrif.IBufferManager, id common.ID, t base.BlockType, cb PostCloseCB) *BlockHolder {
	holder := &BlockHolder{
		ID:          id,
		Type:        t,
		BufMgr:      bufMgr,
		Inited:      false,
		PostCloseCB: cb,
	}
	holder.Indexes = make([]*Node, 0)
	holder.OnZeroCB = holder.close
	holder.Ref()
	return holder
}

func (holder *BlockHolder) Init(segFile base.ISegmentFile) {
	if holder.Inited {
		panic("logic error")
	}
	indexesMeta := segFile.GetBlockIndexesMeta(holder.ID)
	if indexesMeta == nil {
		return
	}
	for _, meta := range indexesMeta.Data {
		vf := segFile.MakeVirtualBlkIndexFile(&holder.ID, meta)
		node := newNode(holder.BufMgr, vf, ZoneMapIndexConstructor, meta.Ptr.Len, meta.Cols, nil)
		holder.Indexes = append(holder.Indexes, node)
	}
	holder.Inited = true
}

func (holder *BlockHolder) close() {
	for _, index := range holder.Indexes {
		index.Unref()
	}
	if holder.PostCloseCB != nil {
		holder.PostCloseCB(holder)
	}
}

func (holder *BlockHolder) Any() bool {
	return len(holder.Indexes) > 0
}

func (holder *BlockHolder) IndexCount() int {
	return len(holder.Indexes)
}

func (holder *BlockHolder) stringNoLock() string {
	s := fmt.Sprintf("<IndexBlkHolder[%s]>[Ty=%d](Cnt=%d)", holder.ID.BlockString(), holder.Type, len(holder.Indexes))
	return s
}
