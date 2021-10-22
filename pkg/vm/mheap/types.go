package mheap

import (
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
)

/*
type Mheap interface {
	Size() int64
	HostSize() int64

	Free([]byte)
	Alloc(int64) ([]byte, error)
}
*/

type Mheap struct {
	Gm *guest.Mmu
	Mp *mempool.Mempool
}
