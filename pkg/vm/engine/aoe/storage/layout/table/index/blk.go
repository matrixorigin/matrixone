package index

import (
	"fmt"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync"
)

type BlockHolder struct {
	ID common.ID
	sync.RWMutex
	Indexes []*Node
	Type    base.BlockType
	BufMgr  mgrif.IBufferManager
}

func NewBlockHolder(bufMgr mgrif.IBufferManager, id common.ID, t base.BlockType) *BlockHolder {
	holder := &BlockHolder{
		ID:     id,
		Type:   t,
		BufMgr: bufMgr,
	}
	holder.Indexes = make([]*Node, 0)
	return holder
}

func (holder *BlockHolder) init(segFile base.ISegmentFile) {
	indexesMeta := segFile.GetBlockIndexesMeta(holder.ID)
	if indexesMeta == nil {
		return
	}
	// for _, meta := range indexesMeta.Data {
	// 	vf := segFile.MakeVirtualBlkIndexFile(&holder.ID, meta)
	// 	node := NewNode(meta.Cols, vf, ZoneMapIndexConstructor, meta.Ptr.Len)
	// }
}

func (holder *BlockHolder) Any() bool {
	return len(holder.Indexes) > 0
}

func (holder *BlockHolder) addNode(bufMgr mgrif.IBufferManager, vf mgrif.IVFile) {
	// NewNode(meta.Cols, bufMgr,
}

func (holder *BlockHolder) stringNoLock() string {
	s := fmt.Sprintf("<IndexBlkHolder[%s]>[Ty=%d]", holder.ID.BlockString(), holder.Type)
	return s
}
