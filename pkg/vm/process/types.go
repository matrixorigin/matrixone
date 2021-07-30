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
	Size          int64 // memory threshold
	BatchRows     int64 // max rows for batch
	BatchSize     int64 // max size for batch
	PartitionRows int64 // max rows for partition
}

type Process struct {
	Id    string // query id
	Reg   Register
	Lim   Limitation
	Gm    *guest.Mmu
	Mp    *mempool.Mempool
	Refer map[string]uint64
	Epoch uint64
}
