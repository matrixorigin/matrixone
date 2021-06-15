package index

import (
	"sync"
)

type BlockType uint8

const (
	TransientBlk BlockType = iota
	PersistentBlk
	SortedPersistentBlk
)

type BlockHolder struct {
	ID uint64
	sync.RWMutex
	Indexes map[string]Index
	Type    BlockType
}

func NewBlockHolder(id uint64, t BlockType) *BlockHolder {
	holder := &BlockHolder{
		ID:   id,
		Type: t,
	}
	holder.Indexes = make(map[string]Index)
	return holder
}
