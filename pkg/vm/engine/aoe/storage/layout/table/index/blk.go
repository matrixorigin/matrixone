package index

import (
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync"
)

type BlockHolder struct {
	ID uint64
	sync.RWMutex
	Indexes []*Node
	Type    base.BlockType
}

func NewBlockHolder(id uint64, t base.BlockType) *BlockHolder {
	holder := &BlockHolder{
		ID:   id,
		Type: t,
	}
	holder.Indexes = make([]*Node, 0)
	return holder
}

func (holder *BlockHolder) stringNoLock() string {
	s := fmt.Sprintf("<IndexBlkHolder[%d]>[Ty=%d]", holder.ID, holder.Type)
	return s
}
