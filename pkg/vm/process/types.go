package process

import (
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/mmu/guest"
)

/*
type Process interface {
	Size() int64
	Free(int64)
	Alloc(int64) error
}
*/

type Register struct {
	Ax interface{}
}

type Process struct {
	Reg Register
	Gm  *guest.Mmu
	Mp  *mempool.Mempool
}
