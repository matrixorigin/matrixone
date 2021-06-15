package index

import (
	// "matrixone/pkg/vm/engine/aoe/storage/common"
	"sync"
)

type BlockHolder struct {
	ID uint64
	sync.RWMutex
	Indexes map[string]Index
}

func NewBlockHolder(id uint64) *BlockHolder {
	holder := &BlockHolder{
		ID: id,
	}
	holder.Indexes = make(map[string]Index)
	return holder
}
