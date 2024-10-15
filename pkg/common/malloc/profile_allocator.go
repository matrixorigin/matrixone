// Copyright 2024 Matrix Origin
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

package malloc

import (
	"runtime"
	"sync/atomic"

	"github.com/google/pprof/profile"
)

type HeapSampleValues struct {
	Objects struct {
		Allocated   ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
		Deallocated ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
	}
	Bytes struct {
		Allocated   ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
		Deallocated ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
	}
}

var _ SampleValues[*HeapSampleValues] = new(HeapSampleValues)

func (h *HeapSampleValues) Init() {
	h.Objects.Allocated = *NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0))
	h.Objects.Deallocated = *NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0))
	h.Bytes.Allocated = *NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0))
	h.Bytes.Deallocated = *NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0))
}

func (h *HeapSampleValues) DefaultSampleType() string {
	return "inuse_bytes"
}

func (h *HeapSampleValues) SampleTypes() []*profile.ValueType {
	return []*profile.ValueType{
		{
			Type: "allocated_objects",
			Unit: "object",
		},
		{
			Type: "allocated_bytes",
			Unit: "bytes",
		},
		{
			Type: "inuse_objects",
			Unit: "object",
		},
		{
			Type: "inuse_bytes",
			Unit: "bytes",
		},
	}
}

func (h *HeapSampleValues) Values() []int64 {
	objectsAllocated := h.Objects.Allocated.Load()
	bytesAllocated := h.Bytes.Allocated.Load()
	objectsDeallocated := h.Objects.Deallocated.Load()
	bytesDeallocated := h.Bytes.Deallocated.Load()
	return []int64{
		int64(objectsAllocated),
		int64(bytesAllocated),
		int64(objectsAllocated - objectsDeallocated),
		int64(bytesAllocated - bytesDeallocated),
	}
}

func (h *HeapSampleValues) Merge(merges []*HeapSampleValues) *HeapSampleValues {
	objectsAllocated := h.Objects.Allocated.Load()
	bytesAllocated := h.Bytes.Allocated.Load()
	objectsDeallocated := h.Objects.Deallocated.Load()
	bytesDeallocated := h.Bytes.Deallocated.Load()
	for _, merge := range merges {
		objectsAllocated += merge.Objects.Allocated.Load()
		bytesAllocated += merge.Bytes.Allocated.Load()
		objectsDeallocated += merge.Objects.Deallocated.Load()
		bytesDeallocated += merge.Bytes.Deallocated.Load()
	}

	ret := new(HeapSampleValues)
	ret.Init()
	ret.Objects.Allocated.Add(objectsAllocated)
	ret.Objects.Deallocated.Add(objectsDeallocated)
	ret.Bytes.Allocated.Add(bytesAllocated)
	ret.Bytes.Deallocated.Add(bytesDeallocated)
	return ret
}

type ProfileAllocator[U Allocator] struct {
	upstream        U
	profiler        *Profiler[HeapSampleValues, *HeapSampleValues]
	fraction        uint32
	deallocatorPool *ClosureDeallocatorPool[profileDeallocateArgs, *profileDeallocateArgs]
}

func NewProfileAllocator[U Allocator](
	upstream U,
	profiler *Profiler[HeapSampleValues, *HeapSampleValues],
	fraction uint32,
) *ProfileAllocator[U] {
	return &ProfileAllocator[U]{
		upstream: upstream,
		profiler: profiler,
		fraction: fraction,

		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *profileDeallocateArgs) {
				args.values.Bytes.Deallocated.Add(args.size)
				args.values.Objects.Deallocated.Add(1)
			},
		),
	}
}

type profileDeallocateArgs struct {
	values *HeapSampleValues
	size   uint64
}

func (profileDeallocateArgs) As(Trait) bool {
	return false
}

var _ Allocator = new(ProfileAllocator[Allocator])

func (p *ProfileAllocator[U]) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	ptr, dec, err := p.upstream.Allocate(size, hints)
	if err != nil {
		return nil, nil, err
	}
	const skip = 1 // p.Allocate
	values := p.profiler.Sample(skip, p.fraction)
	values.Bytes.Allocated.Add(size)
	values.Objects.Allocated.Add(1)
	return ptr, ChainDeallocator(
		dec,
		p.deallocatorPool.Get(profileDeallocateArgs{
			values: values,
			size:   size,
		}),
	), nil
}
