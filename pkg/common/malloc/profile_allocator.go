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
	AllocatedObjects ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	AllocatedBytes   ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	InuseObjects     ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	InuseBytes       ShardedCounter[int64, atomic.Int64, *atomic.Int64]
}

var _ SampleValues = new(HeapSampleValues)

func (h *HeapSampleValues) Init() {
	h.AllocatedObjects = *NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))
	h.AllocatedBytes = *NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))
	h.InuseObjects = *NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))
	h.InuseBytes = *NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))
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
	return []int64{
		h.AllocatedObjects.Load(),
		h.AllocatedBytes.Load(),
		h.InuseObjects.Load(),
		h.InuseBytes.Load(),
	}
}

type ProfileAllocator struct {
	upstream        Allocator
	profiler        *Profiler[HeapSampleValues, *HeapSampleValues]
	fraction        uint32
	deallocatorPool *ClosureDeallocatorPool[profileDeallocateArgs, *profileDeallocateArgs]
}

func NewProfileAllocator(
	upstream Allocator,
	profiler *Profiler[HeapSampleValues, *HeapSampleValues],
	fraction uint32,
) *ProfileAllocator {
	return &ProfileAllocator{
		upstream: upstream,
		profiler: profiler,
		fraction: fraction,

		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *profileDeallocateArgs) {
				args.values.InuseBytes.Add(int64(-args.size))
				args.values.InuseObjects.Add(-1)
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

var _ Allocator = new(ProfileAllocator)

func (p *ProfileAllocator) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	ptr, dec, err := p.upstream.Allocate(size, hints)
	if err != nil {
		return nil, nil, err
	}
	const skip = 1 // p.Allocate
	values := p.profiler.Sample(skip, p.fraction)
	values.AllocatedBytes.Add(int64(size))
	values.AllocatedObjects.Add(1)
	values.InuseBytes.Add(int64(size))
	values.InuseObjects.Add(int64(1))
	return ptr, ChainDeallocator(
		dec,
		p.deallocatorPool.Get(profileDeallocateArgs{
			values: values,
			size:   size,
		}),
	), nil
}
