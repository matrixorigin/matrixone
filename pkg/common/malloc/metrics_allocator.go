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
	"github.com/prometheus/client_golang/prometheus"
)

type MetricsAllocator struct {
	upstream        Allocator
	deallocatorPool *ClosureDeallocatorPool[metricsDeallocatorArgs, *metricsDeallocatorArgs]

	allocateBytesCounter   prometheus.Counter
	inuseBytesGauge        prometheus.Gauge
	allocateObjectsCounter prometheus.Counter
	inuseObjectsGauge      prometheus.Gauge
}

type metricsDeallocatorArgs struct {
	size uint64
}

func (metricsDeallocatorArgs) As(Trait) bool {
	return false
}

func NewMetricsAllocator(
	upstream Allocator,
	allocateBytesCounter prometheus.Counter,
	inuseBytesGauge prometheus.Gauge,
	allocateObjectsCounter prometheus.Counter,
	inuseObjectsGauge prometheus.Gauge,
) *MetricsAllocator {
	return &MetricsAllocator{
		upstream:               upstream,
		allocateBytesCounter:   allocateBytesCounter,
		inuseBytesGauge:        inuseBytesGauge,
		allocateObjectsCounter: allocateObjectsCounter,
		inuseObjectsGauge:      inuseObjectsGauge,

		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *metricsDeallocatorArgs) {
				if inuseBytesGauge != nil {
					inuseBytesGauge.Add(-float64(args.size))
				}
				if inuseObjectsGauge != nil {
					inuseObjectsGauge.Add(-1)
				}
			},
		),
	}
}

type AllocateInfo struct {
	Deallocator Deallocator
	Size        uint64
}

var _ Allocator = new(MetricsAllocator)

func (m *MetricsAllocator) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	ptr, dec, err := m.upstream.Allocate(size, hints)
	if err != nil {
		return nil, nil, err
	}
	if m.allocateBytesCounter != nil {
		m.allocateBytesCounter.Add(float64(size))
	}
	if m.inuseBytesGauge != nil {
		m.inuseBytesGauge.Add(float64(size))
	}
	if m.allocateObjectsCounter != nil {
		m.allocateObjectsCounter.Add(1)
	}
	if m.inuseObjectsGauge != nil {
		m.inuseObjectsGauge.Add(1)
	}

	return ptr, ChainDeallocator(
		dec,
		m.deallocatorPool.Get(metricsDeallocatorArgs{
			size: size,
		}),
	), nil
}
