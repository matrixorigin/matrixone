package process

import (
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
	"sync"
)

/*
type Process interface {
	Size() int64
	HostSize() int64

	Free([]byte)
	Alloc(int64) ([]byte, error)
}
*/

type WaitRegister struct {
	Wg *sync.WaitGroup
	Ch chan interface{}
}

type Register struct {
	Ax interface{}
	Ts []interface{}
	Ws []*WaitRegister
}

type Limitation struct {
	Size      int64 // memory threshold
	BatchSize int64 // limit size for batch
}

type Process struct {
	Reg   Register
	Lim   Limitation
	Gm    *guest.Mmu
	Mp    *mempool.Mempool
	Refer map[string]uint64
}
