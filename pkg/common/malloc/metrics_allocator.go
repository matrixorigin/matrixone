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
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

type MetricsAllocator struct {
	upstream Allocator
	metrics  *Metrics
	funcPool sync.Pool
}

type Metrics struct {
	AllocateBytesDelta atomic.Uint64
	FreeBytesDelta     atomic.Uint64
}

func NewMetricsAllocator(upstream Allocator, metrics *Metrics) *MetricsAllocator {
	ret := &MetricsAllocator{
		upstream: upstream,
		metrics:  metrics,
	}
	ret.funcPool = sync.Pool{
		New: func() any {
			argumented := new(argumentedFuncDeallocator[uint64])
			argumented.fn = func(_ unsafe.Pointer, size uint64) {
				ret.metrics.FreeBytesDelta.Add(size)
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
	m.metrics.AllocateBytesDelta.Add(size)
	ptr, dec := m.upstream.Allocate(size)
	fn := m.funcPool.Get().(*argumentedFuncDeallocator[uint64])
	fn.SetArgument(size)
	return ptr, ChainDeallocator(dec, fn)
}

func (m *Metrics) startExport() {
	var sumAllocateBytes, sumFreeBytes, lastSumAllocateBytes uint64
	for range time.NewTicker(time.Second).C {
		allocateBytes := m.AllocateBytesDelta.Swap(0)
		freeBytes := m.FreeBytesDelta.Swap(0)

		sumAllocateBytes += allocateBytes
		sumFreeBytes += freeBytes
		if sumAllocateBytes-lastSumAllocateBytes > (1 << 30) {
			logutil.Debug("malloc stats",
				zap.Any("allocate", sumAllocateBytes),
				zap.Any("free", sumFreeBytes),
			)
			lastSumAllocateBytes = sumAllocateBytes
		}

		metric.MallocCounterAllocateBytes.Add(float64(allocateBytes))
		metric.MallocCounterFreeBytes.Add(float64(freeBytes))
	}
}
