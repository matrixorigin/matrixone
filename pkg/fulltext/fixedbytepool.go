package fulltext

import (
	"context"
	"fmt"
	"os"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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
	mp          *mpool.MPool
	cxt         context.Context
	id          uint64
	nitem       uint64
	used        uint64
	capacity    uint64
	refcnt      uint64
	spilled     bool
	spill_fpath string
	dsize       uint64 // fixed data size of docvec []uint8
	cpos        uint64
	data        []byte
	full        bool
	last_update time.Time
}

func NewPartition(mp *mpool.MPool, cxt context.Context, id uint64, capacity uint64, dsize uint64) (*Partition, error) {
	if capacity > uint64(LOWER_BIT_MASK) || capacity < 0 {
		return nil, moerr.NewInternalError(cxt, "request capacity is larger than 16MB (24 bits)")
	}
	p := Partition{mp: mp, cxt: cxt, id: id, dsize: dsize}
	err := p.alloc(capacity)
	if err != nil {
		return nil, err
	}

	return &p, nil
}

func (part *Partition) alloc(capacity uint64) (err error) {
	part.data, err = part.mp.Alloc(int(capacity), false)
	if err != nil {
		return err
	}
	part.capacity = capacity
	return nil
}

func (part *Partition) Close() {
	if part.data != nil {
		part.mp.Free(part.data)
		part.data = nil
	}
	part.capacity = 0
	part.refcnt = 0

	//delete the temp file
	if part.spilled {
		os.Remove(part.spill_fpath)
	}
}

func (part *Partition) NewItem() (addr uint64, b []byte, err error) {
	if part.cpos+part.dsize > part.capacity {
		return 0, nil, moerr.NewInternalError(part.cxt, "Partition NewItem out of bound")
	}

	if part.spilled {
		err := part.Unspill()
		if err != nil {
			return 0, nil, err
		}
	}

	b = unsafe.Slice(&part.data[part.cpos], part.dsize)
	addr = GetPartitionAddr(part.id, part.cpos)
	part.cpos += part.dsize
	part.used += part.dsize
	part.refcnt++
	part.nitem++
	part.last_update = time.Now()
	if part.cpos+part.dsize > part.capacity {
		part.full = true
	}
	return addr, b, nil
}

func (part *Partition) GetItem(offset uint64) ([]byte, error) {
	part.last_update = time.Now()
	if part.spilled {
		err := part.Unspill()
		if err != nil {
			return nil, err
		}
	}
	return unsafe.Slice(&part.data[offset], part.dsize), nil
}

// FreeItem simply reduce reference count by one and free the data when refcnt == 0
func (part *Partition) FreeItem(offfset uint64) (uint64, error) {
	if part.refcnt == 0 {
		return 0, moerr.NewInternalError(part.cxt, "FreeItem: refcnt = 0, double free")
	}

	part.refcnt--
	ret := uint64(0)
	if part.refcnt == 0 {
		// no more reference delete the data
		ret = part.capacity
		if part.data != nil {
			part.mp.Free(part.data)
			part.data = nil
		}
		part.capacity = 0
		part.cpos = 0
	}
	return ret, nil
}

func (part *Partition) Spill() error {

	if part.data == nil {
		return nil
	}

	f, err := os.CreateTemp("", "fulltext")
	if err != nil {
		return err
	}

	defer f.Close()

	part.spill_fpath = f.Name()
	if _, err := f.Write(part.data); err != nil {
		return err
	}

	part.spilled = true
	part.mp.Free(part.data)
	part.data = nil
	return nil
}

func (part *Partition) Unspill() error {
	if !part.spilled {
		return moerr.NewInternalError(part.cxt, "Unspill: partition is not spilled")
	}

	fpath := part.spill_fpath
	f, err := os.Open(fpath)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		os.Remove(fpath)
	}()

	// alloc memory with capacity
	capacity := part.capacity
	err = part.alloc(capacity)
	if err != nil {
		return err
	}

	n, err := f.Read(part.data)
	if err != nil {
		return err
	}
	if uint64(n) != capacity {
		return moerr.NewInternalError(part.cxt, "Spill file size not match with capacity")
	}

	part.spilled = false
	part.spill_fpath = ""
	return nil
}

type FixedBytePool struct {
	mp            *mpool.MPool
	capacity      uint64
	partition_cap uint64
	dsize         uint64
	partitions    []*Partition
	cxt           context.Context
}

func NewFixedBytePool(mp *mpool.MPool, context context.Context, dsize uint64, partition_cap uint64) *FixedBytePool {
	if partition_cap == 0 {
		partition_cap = LOWER_BIT_MASK
	}

	pool := FixedBytePool{mp: mp, dsize: dsize, cxt: context, partition_cap: partition_cap}
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
	part, err := NewPartition(pool.mp, pool.cxt, id, pool.partition_cap, pool.dsize)
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

func (pool *FixedBytePool) Close() {
	for i, p := range pool.partitions {
		p.Close()
		pool.partitions[i] = nil
	}
}
