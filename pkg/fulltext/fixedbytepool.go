package fulltext

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

/*
  FixedBytePool is the memory pool for store a fixed size byte slice.
  data will be stored in multiple partitions.  Each partition has the maximum capacity.
  When data grow exceed the partition limit, new partition will be created.

  Data can be accessed by address which is uint64 with high 40 bits (partition ID) and low 24 bits (offset in partition).

  There is a memory limit of the pool.  If memory in use exceed the limit, partitions will be spilled out to disk.

  After hash build/aggregate, all data will reside in partitions.  You can use FixedBytePoolIterator to tranverse all data
  in partitions.  You don't need to tranverse the data with hashtable which you are using for aggregate.  It is a sequential
  access and should be faster than hashtable.

  Assumption:
  - During hash build, you won't do any delete.
  - Iterator will free up the memory once it finish traverse a partition with partition.Close().  If not doing this, we will
    have OOM due to keep all partitions in memory.  When partition was spilled into disk, iterator will read the file into
    memory.
*/

// 24 bits low bits - offset in partition (16MB)
// high bits - partition id
var LOWER_BIT_MASK = uint64(0xffffff)
var LOWER_BIT_SHIFT = uint64(24)

// Least recently use
type Lru struct {
	id          uint64
	last_update time.Time
}

// Partition which is able to spill/unspill.  Data must be fixed size when init
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

// FixedBytePool
type FixedBytePool struct {
	mp            *mpool.MPool
	cxt           context.Context
	partitions    []*Partition
	capacity      uint64
	partition_cap uint64
	dsize         uint64
	mem_in_use    uint64
	mem_limit     uint64
	spill_size    uint64
}

// FixedBytePoolIterator to tranverse the data in the pool
type FixedBytePoolIterator struct {
	pool   *FixedBytePool
	idx    int
	offset uint64
}

// get offset from address
func GetPartitionOffset(addr uint64) uint64 {
	return (addr & LOWER_BIT_MASK)
}

// get partition ID from address
func GetPartitionId(addr uint64) uint64 {
	return (addr >> LOWER_BIT_SHIFT)
}

// convert partition id and offset into address
func GetPartitionAddr(partid uint64, offset uint64) uint64 {
	return (partid << LOWER_BIT_SHIFT) | offset
}

// New Partition with capacity, fixed data size
func NewPartition(mp *mpool.MPool, cxt context.Context, id uint64, capacity uint64, dsize uint64) (*Partition, error) {
	if capacity > uint64(LOWER_BIT_MASK) || capacity < 0 {
		return nil, moerr.NewInternalError(cxt, "request capacity is larger than 16MB (24 bits)")
	}
	p := Partition{mp: mp, cxt: cxt, id: id, dsize: dsize}
	err := p.alloc(capacity)
	if err != nil {
		return nil, err
	}

	p.last_update = time.Now()
	return &p, nil
}

func (part *Partition) Id() uint64 {
	return part.id
}

// memory allocation with mpool.MPool
func (part *Partition) alloc(capacity uint64) (err error) {
	part.data, err = part.mp.Alloc(int(capacity), false)
	if err != nil {
		return err
	}
	part.capacity = capacity
	return nil
}

// Close the partition
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
		part.spilled = false
		part.spill_fpath = ""
	}
}

// NewItem will return the []byte and address and set full is true when partition is full for next item
func (part *Partition) NewItem() (addr uint64, b []byte, err error) {
	if part.cpos+part.dsize > part.capacity {
		return 0, nil, moerr.NewInternalError(part.cxt, "Partition NewItem out of bound")
	}

	if part.spilled {
		return 0, nil, moerr.NewInternalError(part.cxt, "NewItem: partition is spillled")
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

// GetItem with offset
func (part *Partition) GetItem(offset uint64) ([]byte, error) {
	part.last_update = time.Now()
	if part.spilled {
		return nil, moerr.NewInternalError(part.cxt, "GetItem: partition is spillled")
	}
	if offset+part.dsize > part.used {
		return nil, moerr.NewInternalError(part.cxt, "GetItem: offset out of bound")
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

// Spill to spill the memory into disk and free up memory
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

// read the spill file to memory and remove the temp file
// the state of the partition should be the same with spill/unspill so that
// partition can still perform NewItem() and GetItem() after unspill()
func (part *Partition) Unspill() error {
	if !part.spilled {
		return nil
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

// check partition is already spilled
func (part *Partition) Spilled() bool {
	return part.spilled
}

// check last update of the partition.  Spill always choose LRU partitions
func (part *Partition) LastUpdate() time.Time {
	return part.last_update
}

// FixedBytePool
func NewFixedBytePool(mp *mpool.MPool, context context.Context, dsize uint64, partition_cap uint64, mem_limit uint64) *FixedBytePool {
	if partition_cap == 0 {
		partition_cap = LOWER_BIT_MASK
	}

	if mem_limit == 0 {
		mem_limit = uint64(1024 * 1024 * 1024) // 1G
	}

	pool := FixedBytePool{mp: mp, dsize: dsize, cxt: context, partition_cap: partition_cap, mem_limit: mem_limit, spill_size: 2}
	pool.partitions = make([]*Partition, 0, 32)
	return &pool
}

// NewItem will find a available partition for NewItem.  Usually the last item of partition slice
// If memory in use exceed the memory limit, spill
// If no avaiable partition, create a new partition
func (pool *FixedBytePool) NewItem() (addr uint64, b []byte, err error) {
	// find last partition to new item
	np := len(pool.partitions)
	if np > 0 {
		p := pool.partitions[np-1]
		if !p.full {
			return p.NewItem()
		}
	}

	if pool.mem_in_use+pool.partition_cap > pool.mem_limit {
		// spill
		pool.Spill()
	}

	// partition not found and create new partition
	id := uint64(len(pool.partitions))
	part, err := NewPartition(pool.mp, pool.cxt, id, pool.partition_cap, pool.dsize)
	if err != nil {
		return 0, nil, err
	}

	pool.partitions = append(pool.partitions, part)
	pool.capacity += part.capacity
	pool.mem_in_use += part.capacity

	return part.NewItem()
}

// Getitem return item with partitions.  If requested partition was spilled, unspill()
func (pool *FixedBytePool) GetItem(addr uint64) ([]byte, error) {
	id := GetPartitionId(addr)
	offset := GetPartitionOffset(addr)

	if id >= uint64(len(pool.partitions)) {
		return nil, moerr.NewInternalError(pool.cxt, "GetItem: id out of bound")
	}

	p := pool.partitions[id]

	if p.Spilled() {
		err := p.Unspill()
		if err != nil {
			return nil, err
		}
		pool.mem_in_use += pool.partition_cap
	}

	return p.GetItem(offset)
}

// FreeItem call partition.FreeItem
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
	pool.mem_in_use -= freesize
	return nil
}

func (pool *FixedBytePool) String() string {
	return fmt.Sprintf("FixedBytePool: capacity %d, part_cap %d, npart %d, dsize %d\n",
		pool.capacity, pool.partition_cap, len(pool.partitions), pool.dsize)
}

// Close the pool and cleanup memory and temp files
func (pool *FixedBytePool) Close() {
	for i, p := range pool.partitions {
		if p != nil {
			p.Close()
			pool.partitions[i] = nil
		}
	}
}

// spill will find LRU partitions to spill and will double the number of partitions to spill for the next time
func (pool *FixedBytePool) Spill() error {

	// find unspilled partitions
	lru := make([]Lru, 0, len(pool.partitions))

	for _, p := range pool.partitions {
		if p.Spilled() {
			continue
		}
		lru = append(lru, Lru{id: p.Id(), last_update: p.LastUpdate()})
	}

	if len(lru) == 0 {
		return nil
	}

	sort.Slice(lru, func(i, j int) bool {
		return lru[i].last_update.Before(lru[j].last_update)
	})

	fmt.Printf("sorted %v\n", lru)

	// double the spill size every time
	nspill := pool.spill_size
	if nspill > uint64(len(lru)) {
		nspill = uint64(len(lru))
	}
	pool.spill_size *= 2

	// concurrent spill partitions
	var wg sync.WaitGroup
	var errs error
	for i := 0; i < int(nspill); i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			err := pool.partitions[lru[i].id].Spill()
			if err != nil {
				errs = errors.Join(errs, err)
			}
		}()
	}

	wg.Wait()

	if errs != nil {
		return errs
	}

	pool.mem_in_use -= uint64(nspill) * pool.partition_cap

	fmt.Printf("%d spilled, mem in use %d\n", nspill, pool.mem_in_use)
	return nil
}

// Iterator
func NewFixedBytePoolIterator(p *FixedBytePool) *FixedBytePoolIterator {
	return &FixedBytePoolIterator{pool: p}
}

// Get next []byte from the pool.  If no more data, return nil []byte and nil error
func (it *FixedBytePoolIterator) Next() ([]byte, error) {
	for {
		if it.idx >= len(it.pool.partitions) {
			break
		}

		p := it.pool.partitions[it.idx]
		if p.Spilled() {
			err := p.Unspill()
			if err != nil {
				return nil, err
			}
		}

		if it.offset >= p.used {
			// next partition
			it.idx++
			it.offset = 0

			// close partition
			p.Close()
			continue
		}

		b, err := p.GetItem(it.offset)
		if err != nil {
			return nil, err
		}

		it.offset += it.pool.dsize
		return b, nil
	}

	return nil, nil
}
