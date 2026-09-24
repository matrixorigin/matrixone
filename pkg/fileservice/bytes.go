// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// Bytes is a reference-counted byte buffer, optionally backed by an
// allocator. Misuse is detected and panics: use after the last Release,
// releasing more times than retained, and retaining an already-released
// object are all programming errors.
type Bytes struct {
	bytes       []byte
	deallocator malloc.Deallocator
	owner       *fscache.DataOwner
	reservation cacheDataReservation
	// doNotReuse preserves malloc.DoNotReuse across the MemCache's internal
	// pending-allocation pool. Such callers require Release to return the
	// allocation to the underlying allocator immediately.
	doNotReuse bool
	// cacheAdmissionOwner identifies a cache that deliberately allocated this
	// as a transient read buffer because it could not reserve cache capacity.
	cacheAdmissionOwner *fscache.DataOwner
	refs                atomic.Int32
}

// cacheDataReservation accounts for a buffer that has been allocated for a
// cache but is not yet retained by the FIFO. The buffer owns the reservation
// until it is committed by cache insertion or released with the allocation.
type cacheDataReservation interface {
	commit()
	release()
}

func NewBytes(data []byte) *Bytes {
	b := &Bytes{
		bytes: data,
	}
	b.refs.Store(1)
	return b
}

func (b *Bytes) Size() int64 {
	if b.refs.Load() <= 0 {
		panic("Bytes.Size: use after free")
	}
	return int64(len(b.bytes))
}

func (b *Bytes) Capacity() int64 {
	if b.refs.Load() <= 0 {
		panic("Bytes.Capacity: use after free")
	}
	return int64(cap(b.bytes))
}

func (b *Bytes) Bytes() []byte {
	if b.refs.Load() <= 0 {
		panic("Bytes.Bytes: use after free")
	}
	return b.bytes
}

func (b *Bytes) Slice(length int) fscache.Data {
	if b.refs.Load() <= 0 {
		panic("Bytes.Slice: use after free")
	}
	b.bytes = b.bytes[:length]
	return b
}

// Retain increments the reference count. It refuses to resurrect a released
// object: incrementing is only possible while the observed count is positive,
// so a Release that wins the 1 -> 0 transition is final.
func (b *Bytes) Retain() {
	for {
		n := b.refs.Load()
		if n <= 0 {
			panic("Bytes.Retain: use after free")
		}
		if b.refs.CompareAndSwap(n, n+1) {
			return
		}
	}
}

// Release decrements the reference count. The caller that drops the count to
// zero deallocates; further Release or Retain calls panic.
func (b *Bytes) Release() {
	n := b.refs.Add(-1)
	if n == 0 {
		// Last reference: no other goroutine may legally touch b anymore
		// (Retain from zero panics), so plain writes are safe here.
		if reservation, ok := b.reservation.(recyclableCacheDataReservation); ok &&
			!b.doNotReuse && reservation.recycle(b) {
			return
		}
		b.releaseAllocation()
	} else if n < 0 {
		panic("Bytes.Release: double free")
	}
}

// releaseAllocation returns the native allocation and drops any pending cache
// reservation. It is called on the ordinary final Release path and when a
// MemCache drains an idle pending allocation at an explicit reclaim boundary.
// The latter owns a Bytes with refs already at zero.
func (b *Bytes) releaseAllocation() {
	b.bytes = nil
	if b.deallocator != nil {
		b.deallocator.Deallocate()
		b.deallocator = nil
	}
	if b.reservation != nil {
		b.reservation.release()
		b.reservation = nil
	}
}

var _ fscache.DataOwnership = (*Bytes)(nil)
var _ fscache.DataCacheReservation = (*Bytes)(nil)
var _ fscache.DataCacheAdmission = (*Bytes)(nil)

// CommitCacheReservation transfers pending capacity accounting to the FIFO
// after this buffer has been retained by a cache.
func (b *Bytes) CommitCacheReservation() {
	if b.reservation != nil {
		b.reservation.commit()
	}
}

// CacheAdmissionAllowed reports whether this data can enter the destination
// cache. A transient buffer must remain owned by the read and never turn into
// an unaccounted cache allocation.
func (b *Bytes) CacheAdmissionAllowed(owner *fscache.DataOwner) bool {
	return b.cacheAdmissionOwner != owner
}

func (b *Bytes) CacheDataOwner() *fscache.DataOwner {
	return b.owner
}

func (b *Bytes) RehomeCacheData(copyData func([]byte) fscache.Data) fscache.Data {
	return copyData(b.Bytes())
}

type bytesAllocator struct {
	allocator malloc.Allocator
	owner     *fscache.DataOwner
}

var _ CacheDataAllocator = new(bytesAllocator)

func newBytesAllocator(allocator malloc.Allocator) *bytesAllocator {
	return &bytesAllocator{
		allocator: allocator,
		owner:     new(fscache.DataOwner),
	}
}

func (b *bytesAllocator) allocateCacheData(size int, hints malloc.Hints) fscache.Data {
	return b.allocateCacheBytes(size, hints)
}

func (b *bytesAllocator) allocateCacheBytes(size int, hints malloc.Hints) *Bytes {
	bytes, err := b.tryAllocateCacheBytes(size, hints)
	if err != nil {
		panic(err)
	}
	return bytes
}

func (b *bytesAllocator) tryAllocateCacheBytes(size int, hints malloc.Hints) (*Bytes, error) {
	slice, dec, err := b.allocator.Allocate(uint64(size), hints)
	if err != nil {
		return nil, err
	}
	bytes := &Bytes{
		bytes:       slice,
		deallocator: dec,
		owner:       b.owner,
		doNotReuse:  hints&malloc.DoNotReuse != 0,
	}
	bytes.refs.Store(1)
	return bytes, nil
}

func (b *bytesAllocator) AllocateCacheData(ctx context.Context, size int) fscache.Data {
	return b.allocateCacheData(size, malloc.NoHints)
}

func (b *bytesAllocator) AllocateCacheDataWithHint(ctx context.Context, size int, hints malloc.Hints) fscache.Data {
	return b.allocateCacheData(size, hints)
}

func (b *bytesAllocator) CopyToCacheData(ctx context.Context, data []byte) fscache.Data {
	ret := b.allocateCacheData(len(data), malloc.NoClear)
	copy(ret.Bytes(), data)
	return ret
}

func (b *bytesAllocator) BackingSize(size int) int {
	backingSize, err := malloc.BackingSize(b.allocator, uint64(size))
	if err != nil {
		panic(err)
	}
	if uint64(int(backingSize)) != backingSize {
		panic("cache backing size overflows int")
	}
	return int(backingSize)
}
