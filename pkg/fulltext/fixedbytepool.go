// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fulltext

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

/*
  FixedBytePool is the memory pool for store a fixed size byte slice.
  data will be stored in multiple partitions.  Each partition has the maximum capacity.
  When data grow exceed the partition limit, new partition will be created.

  Data can be accessed by address which is uint64 with high 40 bits (partition ID) and low 24 bits (offset in partition).

  There is a memory limit of the pool.  If memory in use exceed the limit, partitions will be spilled out to disk.


  Assumption:
  - During hash build, you won't do any delete.
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
	proc        *process.Process
	id          uint64    // id of the partition
	nitem       uint64    // number of item in partition
	used        uint64    // number of byte used in partition
	capacity    uint64    // total capacity of the partition (fixed when init)
	refcnt      uint64    // reference counter
	spilled     bool      // partition spilled or not
	spill_fpath string    // filepath of the spill file
	dsize       uint64    // fixed data size of docvec []uint8
	cpos        uint64    // current position for next item
	data        []byte    // data in []byte
	full        bool      // is partition full. If true, no more new item
	last_update time.Time // last update time
}

// FixedBytePool
type FixedBytePool struct {
	proc          *process.Process
	partitions    []*Partition // list of partitions
	capacity      uint64       // total capacity of all partitions
	partition_cap uint64       // max capacity of partition (fixed when init)
	dsize         uint64       // data size (fixed when init)
	mem_in_use    uint64       // memory in use
	mem_limit     uint64       // memory limit to check with mem_in_use to see spill or not
	spill_size    uint64       // total number of spilled partitions for the next round, start from 2 and double each time with max 16.
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
func NewPartition(proc *process.Process, id uint64, capacity uint64, dsize uint64) (*Partition, error) {
	if capacity > uint64(LOWER_BIT_MASK) {
		return nil, moerr.NewInternalError(proc.Ctx, "request capacity is larger than 16MB (24 bits)")
	}
	p := Partition{proc: proc, id: id, dsize: dsize}
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
	part.data, err = part.proc.Mp().Alloc(int(capacity), false)
	if err != nil {
		return err
	}
	part.capacity = capacity
	return nil
}

// Close the partition
func (part *Partition) Close() {
	if part.data != nil {
		part.proc.Mp().Free(part.data)
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
		return 0, nil, moerr.NewInternalError(part.proc.Ctx, "Partition NewItem out of bound")
	}

	if part.spilled {
		return 0, nil, moerr.NewInternalError(part.proc.Ctx, "NewItem: partition is spillled")
	}

	b = util.UnsafeToBytesWithLength(&part.data[part.cpos], int(part.dsize))
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
		return nil, moerr.NewInternalError(part.proc.Ctx, "GetItem: partition is spillled")
	}
	if offset+part.dsize > part.used {
		return nil, moerr.NewInternalError(part.proc.Ctx, "GetItem: offset out of bound")
	}

	return util.UnsafeToBytesWithLength(&part.data[offset], int(part.dsize)), nil
}

// FreeItem simply reduce reference count by one and free the data when refcnt == 0
func (part *Partition) FreeItem(offfset uint64) (uint64, error) {
	if part.refcnt == 0 {
		return 0, moerr.NewInternalError(part.proc.Ctx, "FreeItem: refcnt = 0, double free")
	}

	part.refcnt--
	ret := uint64(0)
	if part.refcnt == 0 {
		// no more reference delete the data
		ret = part.capacity
		if part.data != nil {
			part.proc.Mp().Free(part.data)
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

	if _, err := f.Write(part.data); err != nil {
		return err
	}

	part.spilled = true
	part.spill_fpath = f.Name()
	part.proc.Mp().Free(part.data)
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
		part.spilled = false
		part.spill_fpath = ""
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
		return moerr.NewInternalError(part.proc.Ctx, "Spill file size not match with capacity")
	}

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
func NewFixedBytePool(proc *process.Process, dsize uint64, partition_cap uint64, mem_limit uint64) *FixedBytePool {
	if partition_cap == 0 {
		partition_cap = LOWER_BIT_MASK
	}

	if mem_limit == 0 {
		mem_limit = uint64(1024 * 1024 * 1024) // 1G
	}

	pool := FixedBytePool{proc: proc, dsize: dsize, partition_cap: partition_cap, mem_limit: mem_limit, spill_size: 2}
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
		err := pool.Spill()
		if err != nil {
			return 0, nil, err
		}
	}

	// partition not found and create new partition
	id := uint64(len(pool.partitions))
	part, err := NewPartition(pool.proc, id, pool.partition_cap, pool.dsize)
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
		return nil, moerr.NewInternalError(pool.proc.Ctx, "GetItem: id out of bound")
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
		return moerr.NewInternalError(pool.proc.Ctx, "FreeItem: id out of bound")
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

	//fmt.Printf("sorted %v\n", lru)

	// double the spill size every time
	nspill := pool.spill_size
	if nspill > uint64(len(lru)) {
		nspill = uint64(len(lru))
	}
	// max 16 spill size
	if pool.spill_size < 16 {
		pool.spill_size *= 2
	}

	// concurrent spill partitions
	var wg sync.WaitGroup
	errchan := make(chan error, nspill)
	for i := 0; i < int(nspill); i++ {
		wg.Add(1)

		go func(tid int) {
			defer wg.Done()
			err := pool.partitions[lru[tid].id].Spill()
			if err != nil {
				errchan <- err
			}
		}(i)
	}

	wg.Wait()

	if len(errchan) > 0 {
		return <-errchan
	}

	pool.mem_in_use -= uint64(nspill) * pool.partition_cap

	//fmt.Printf("%d spilled, mem in use %d\n", nspill, pool.mem_in_use)
	return nil
}

// Iterator
//
// After hash build/aggregate, all data will reside in partitions.  You can use FixedBytePoolIterator to tranverse all data
// in partitions. We don't provide you the hash key here. just the values
//
// Iterator will free up the memory once it finish traverse a partition with partition.Close().  If not doing this, we will
// have OOM due to keep all partitions in memory.  When partition was spilled into disk, iterator will read the file into
// memory.
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
