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
	// absoluteInuseGauge aggregates in-use bytes across allocators that share
	// the same metric label. Nil disables reporting.
	absoluteInuseGauge prometheus.Gauge

	allocateBytes   ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
	inuseBytes      ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	allocateObjects ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
	inuseObjects    ShardedCounter[int64, atomic.Int64, *atomic.Int64]

	updating atomic.Bool
	// dirty records metric mutations which arrive while a refresh is publishing.
	// It closes the interval between draining the sharded counters and clearing
	// updating, where an update could otherwise be left unscheduled forever.
	dirty atomic.Bool
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
	absoluteInuseGauge prometheus.Gauge,
) *MetricsAllocator[U] {

	var ret *MetricsAllocator[U]

	ret = &MetricsAllocator[U]{
		upstream:               upstream,
		allocateBytesCounter:   allocateBytesCounter,
		inuseBytesGauge:        inuseBytesGauge,
		allocateObjectsCounter: allocateObjectsCounter,
		inuseObjectsGauge:      inuseObjectsGauge,
		absoluteInuseGauge:     absoluteInuseGauge,

		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *metricsDeallocatorArgs) {
				ret.inuseBytes.Add(-int64(args.size))
				ret.inuseObjects.Add(-1)
				ret.recordUpdate()
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
	backingSize := uint64(cap(ptr))
	m.allocateBytes.Add(backingSize)
	m.inuseBytes.Add(int64(backingSize))
	m.allocateObjects.Add(1)
	m.inuseObjects.Add(1)
	m.recordUpdate()

	return ptr, ChainDeallocator(
		dec,
		m.deallocatorPool.Get(metricsDeallocatorArgs{
			size: backingSize,
		}),
	), nil
}

const metricsAllocatorUpdateWindow = time.Millisecond * 100

func (m *MetricsAllocator[U]) recordUpdate() {
	m.dirty.Store(true)
	m.triggerUpdate()
}

func (m *MetricsAllocator[U]) triggerUpdate() {
	if m.updating.CompareAndSwap(false, true) {
		time.AfterFunc(metricsAllocatorUpdateWindow, m.refreshMetrics)
	}
}

func (m *MetricsAllocator[U]) refreshMetrics() {
	// Mutations before this store are included in this refresh. Mutations after
	// it either set dirty for the rearm below or schedule their own refresh.
	m.dirty.Store(false)

	if m.allocateBytesCounter != nil {
		var n uint64
		m.allocateBytes.Each(func(v *atomic.Uint64) {
			n += v.Swap(0)
		})
		m.allocateBytesCounter.Add(float64(n))
	}

	if m.inuseBytesGauge != nil || m.absoluteInuseGauge != nil {
		var n int64
		m.inuseBytes.Each(func(v *atomic.Int64) {
			n += v.Swap(0)
		})
		if m.inuseBytesGauge != nil {
			m.inuseBytesGauge.Add(float64(n))
		}
		if m.absoluteInuseGauge != nil {
			m.absoluteInuseGauge.Add(float64(n))
		}
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
	if m.dirty.Load() {
		m.triggerUpdate()
	}
}
