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
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/google/pprof/profile"
)

type HeapSampleValues struct {
	AllocatedObjects atomic.Int64
	AllocatedBytes   atomic.Int64
	InuseObjects     atomic.Int64
	InuseBytes       atomic.Int64
}

var _ SampleValues = new(HeapSampleValues)

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
	profileFraction uint32
	funcPool        sync.Pool
}

func NewProfileAllocator(
	upstream Allocator,
	profiler *Profiler[HeapSampleValues, *HeapSampleValues],
	profileRate uint32,
) *ProfileAllocator {
	ret := &ProfileAllocator{
		upstream:        upstream,
		profiler:        profiler,
		profileFraction: profileRate,
	}

	ret.funcPool = sync.Pool{
		New: func() any {
			argumented := new(argumentedFuncDeallocator[profileDeallocateArgs])
			argumented.fn = func(_ unsafe.Pointer, args profileDeallocateArgs) {
				args.values.InuseBytes.Add(int64(-args.size))
				args.values.InuseObjects.Add(-1)
				ret.funcPool.Put(argumented)
			}
			return argumented
		},
	}

	return ret
}

type profileDeallocateArgs struct {
	values *HeapSampleValues
	size   uint64
}

var _ Allocator = new(ProfileAllocator)

func (p *ProfileAllocator) Allocate(size uint64) (unsafe.Pointer, Deallocator) {
	ptr, dec := p.upstream.Allocate(size)
	values := p.profiler.Sample(1, p.profileFraction)
	values.AllocatedBytes.Add(int64(size))
	values.AllocatedObjects.Add(1)
	values.InuseBytes.Add(int64(size))
	values.InuseObjects.Add(int64(1))
	fn := p.funcPool.Get().(*argumentedFuncDeallocator[profileDeallocateArgs])
	fn.SetArgument(profileDeallocateArgs{
		values: values,
		size:   size,
	})
	return ptr, ChainDeallocator(dec, fn)
}
