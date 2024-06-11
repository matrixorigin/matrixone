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
	"unsafe"

	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

type MetricsAllocator struct {
	upstream Allocator
	funcPool sync.Pool
}

func NewMetricsAllocator(upstream Allocator) *MetricsAllocator {
	ret := &MetricsAllocator{
		upstream: upstream,
	}
	ret.funcPool = sync.Pool{
		New: func() any {
			argumented := new(argumentedFuncDeallocator[uint64])
			argumented.fn = func(_ unsafe.Pointer, size uint64) {
				metric.MallocCounterFreeBytes.Add(float64(size))
				ret.funcPool.Put(argumented)
			}
			return argumented
		},
	}
	return ret
}

type AllocateInfo struct {
	Deallocator Deallocator
	Size        uint64
}

var _ Allocator = new(MetricsAllocator)

func (m *MetricsAllocator) Allocate(size uint64) (unsafe.Pointer, Deallocator) {
	metric.MallocCounterAllocateBytes.Add(float64(size))
	ptr, dec := m.upstream.Allocate(size)
	fn := m.funcPool.Get().(*argumentedFuncDeallocator[uint64])
	fn.SetArgument(size)
	return ptr, ChainDeallocator(dec, fn)
}
