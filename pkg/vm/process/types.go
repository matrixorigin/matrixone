package process

import (
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/mmu/guest"
)

/*
type Process interface {
	Size() int64
	HostSize() int64

	Free([]byte)
	Alloc(int64) ([]byte, error)
}
*/

type Register struct {
	Ax interface{}
	Ts []interface{}
	Cs []chan interface{}
}

type Process struct {
	Reg   Register
	Gm    *guest.Mmu
	Mp    *mempool.Mempool
	Refer map[string]uint64
}
