package fulltext

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// 24 bits low bits - offset in partition (16MB)
// high bits - partition id
var LOWER_BIT_MASK = uint64(0xffffff)
var LOWER_BIT_SHIFT = uint64(24)

func GetPartitionOffset(addr uint64) uint64 {
	return (addr & LOWER_BIT_MASK)
}

func GetPartitionId(addr uint64) uint64 {
	return (addr >> LOWER_BIT_SHIFT)
}

func GetPartitionAddr(partid uint64, offset uint64) uint64 {
	return (partid << LOWER_BIT_SHIFT) | offset
}

type Partition struct {
	cxt         context.Context
	id          uint64
	used        uint64
	capacity    uint64
	refcnt      uint64
	spilled     bool
	spill_fpath string
	dsize       uint64 // fixed data size of docvec []uint8
	cpos        uint64
	data        []byte
	full        bool
}

func NewPartition(cxt context.Context, id uint64, capacity uint64, dsize uint64) (*Partition, error) {
	if capacity > uint64(LOWER_BIT_MASK) || capacity < 0 {
		return nil, moerr.NewInternalError(cxt, "request capacity is larger than 16MB (24 bits)")
	}
	p := Partition{cxt: cxt, id: id, capacity: capacity, dsize: dsize, data: make([]byte, capacity)}
	return &p, nil
}

func (part *Partition) NewItem() (addr uint64, b []byte, err error) {
	if part.cpos+part.dsize > part.capacity {
		return 0, nil, moerr.NewInternalError(part.cxt, "Partition NewItem out of bound")
	}

	b = unsafe.Slice(&part.data[part.cpos], part.dsize)
	addr = GetPartitionAddr(part.id, part.cpos)
	part.cpos += part.dsize
	part.used += part.dsize
	part.refcnt++
	if part.cpos+part.dsize > part.capacity {
		part.full = true
	}
	return addr, b, nil
}

func (part *Partition) GetItem(offset uint64) ([]byte, error) {
	if part.spilled {
		return nil, moerr.NewInternalError(part.cxt, "GetItem: Spill not supported yet")
	}
	return unsafe.Slice(&part.data[offset], part.dsize), nil
}

func (part *Partition) FreeItem(offfset uint64) (uint64, error) {
	if part.refcnt == 0 {
		return 0, moerr.NewInternalError(part.cxt, "FreeItem: refcnt = 0, double free")
	}
	part.refcnt--

	ret := uint64(0)
	if part.refcnt == 0 {
		// no more reference delete the data
		ret = part.capacity
		part.data = nil
		part.capacity = 0
		part.cpos = 0
	}
	return ret, nil
}

type FixedBytePool struct {
	capacity      uint64
	partition_cap uint64
	dsize         uint64
	partitions    []*Partition
	cxt           context.Context
}

func NewFixedBytePool(context context.Context, dsize uint64, partition_cap uint64) *FixedBytePool {
	if partition_cap == 0 {
		partition_cap = LOWER_BIT_MASK
	}

	pool := FixedBytePool{dsize: dsize, cxt: context, partition_cap: partition_cap}
	pool.partitions = make([]*Partition, 0, 32)
	return &pool
}

func (pool *FixedBytePool) NewItem() (addr uint64, b []byte, err error) {
	// find a free partition
	for _, p := range pool.partitions {
		if !p.full {
			return p.NewItem()
		}
	}

	// partition not found and create new partition
	id := uint64(len(pool.partitions))
	part, err := NewPartition(pool.cxt, id, pool.partition_cap, pool.dsize)
	if err != nil {
		return 0, nil, err
	}

	pool.partitions = append(pool.partitions, part)
	pool.capacity += part.capacity
	return part.NewItem()
}

func (pool *FixedBytePool) GetItem(addr uint64) ([]byte, error) {
	id := GetPartitionId(addr)
	offset := GetPartitionOffset(addr)

	if id >= uint64(len(pool.partitions)) {
		return nil, moerr.NewInternalError(pool.cxt, "GetItem: id out of bound")
	}

	p := pool.partitions[id]
	return p.GetItem(offset)
}

func (pool *FixedBytePool) FreeItem(addr uint64) error {
	id := GetPartitionId(addr)
	offset := GetPartitionOffset(addr)
	if id >= uint64(len(pool.partitions)) {
		return moerr.NewInternalError(pool.cxt, "FreeItem: id out of bound")
	}

	p := pool.partitions[id]
	freesize, err := p.FreeItem(offset)
	if err != nil {
		return err
	}
	pool.capacity -= freesize
	return nil
}

func (pool *FixedBytePool) String() string {
	return fmt.Sprintf("FixedBytePool: capacity %d, part_cap %d, npart %d, dsize %d\n",
		pool.capacity, pool.partition_cap, len(pool.partitions), pool.dsize)
}
