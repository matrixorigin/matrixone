package mheap

import (
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
)

type Mheap struct {
	Gm *guest.Mmu
	Mp *mempool.Mempool
}
