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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type MetricsAllocator[U Allocator] struct {
	upstream        U
	deallocatorPool *ClosureDeallocatorPool[metricsDeallocatorArgs, *metricsDeallocatorArgs]

	allocateBytesCounter   prometheus.Counter
	inuseBytesGauge        prometheus.Gauge
	allocateObjectsCounter prometheus.Counter
	inuseObjectsGauge      prometheus.Gauge

	allocateBytes   ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
	inuseBytes      ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	allocateObjects ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
	inuseObjects    ShardedCounter[int64, atomic.Int64, *atomic.Int64]

	updating atomic.Bool
}

type metricsDeallocatorArgs struct {
	size uint64
}

func (metricsDeallocatorArgs) As(Trait) bool {
	return false
}

func NewMetricsAllocator[U Allocator](
	upstream U,
	allocateBytesCounter prometheus.Counter,
	inuseBytesGauge prometheus.Gauge,
	allocateObjectsCounter prometheus.Counter,
	inuseObjectsGauge prometheus.Gauge,
) *MetricsAllocator[U] {

	var ret *MetricsAllocator[U]

	ret = &MetricsAllocator[U]{
		upstream:               upstream,
		allocateBytesCounter:   allocateBytesCounter,
		inuseBytesGauge:        inuseBytesGauge,
		allocateObjectsCounter: allocateObjectsCounter,
		inuseObjectsGauge:      inuseObjectsGauge,

		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *metricsDeallocatorArgs) {
				ret.inuseBytes.Add(-int64(args.size))
				ret.inuseObjects.Add(-1)
				ret.triggerUpdate()
			},
		),
	}

	ret.allocateBytes = *NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0))
	ret.inuseBytes = *NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))
	ret.allocateObjects = *NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0))
	ret.inuseObjects = *NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))

	return ret
}

type AllocateInfo struct {
	Deallocator Deallocator
	Size        uint64
}

var _ Allocator = new(MetricsAllocator[Allocator])

func (m *MetricsAllocator[U]) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	ptr, dec, err := m.upstream.Allocate(size, hints)
	if err != nil {
		return nil, nil, err
	}
	m.allocateBytes.Add(size)
	m.inuseBytes.Add(int64(size))
	m.allocateObjects.Add(1)
	m.inuseObjects.Add(1)
	m.triggerUpdate()

	return ptr, ChainDeallocator(
		dec,
		m.deallocatorPool.Get(metricsDeallocatorArgs{
			size: size,
		}),
	), nil
}

func (m *MetricsAllocator[U]) triggerUpdate() {
	if m.updating.CompareAndSwap(false, true) {
		time.AfterFunc(time.Second, func() {

			if m.allocateBytesCounter != nil {
				var n uint64
				m.allocateBytes.Each(func(v *atomic.Uint64) {
					n += v.Swap(0)
				})
				m.allocateBytesCounter.Add(float64(n))
			}

			if m.inuseBytesGauge != nil {
				var n int64
				m.inuseBytes.Each(func(v *atomic.Int64) {
					n += v.Swap(0)
				})
				m.inuseBytesGauge.Add(float64(n))
			}

			if m.allocateObjectsCounter != nil {
				var n uint64
				m.allocateObjects.Each(func(v *atomic.Uint64) {
					n += v.Swap(0)
				})
				m.allocateObjectsCounter.Add(float64(n))
			}

			if m.inuseObjectsGauge != nil {
				var n int64
				m.inuseObjects.Each(func(v *atomic.Int64) {
					n += v.Swap(0)
				})
				m.inuseObjectsGauge.Add(float64(n))
			}

			m.updating.Store(false)
		})
	}
}
