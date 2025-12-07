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

type SimpleCAllocator struct {
	ca *CAllocator

	allocateBytesCounter   prometheus.Counter
	inuseBytesGauge        prometheus.Gauge
	allocateObjectsCounter prometheus.Counter
	inuseObjectsGauge      prometheus.Gauge

	// it is not clear if these shared counters are overengineering.
	allocateBytes   *ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
	inuseBytes      *ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	allocateObjects *ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
	inuseObjects    *ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	updating        atomic.Bool
}

func NewSimpleCAllocator(
	allocateBytesCounter prometheus.Counter,
	inuseBytesGauge prometheus.Gauge,
	allocateObjectsCounter prometheus.Counter,
	inuseObjectsGauge prometheus.Gauge,
) *SimpleCAllocator {
	sca := &SimpleCAllocator{
		ca:                     NewCAllocator(),
		allocateBytesCounter:   allocateBytesCounter,
		inuseBytesGauge:        inuseBytesGauge,
		allocateObjectsCounter: allocateObjectsCounter,
		inuseObjectsGauge:      inuseObjectsGauge,
		allocateBytes:          NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0)),
		inuseBytes:             NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0)),
		allocateObjects:        NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0)),
		inuseObjects:           NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0)),
	}
	return sca
}

func (sca *SimpleCAllocator) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	ptr, dec, err := sca.ca.Allocate(size, hints)
	if err != nil {
		return nil, nil, err
	}
	sca.allocateBytes.Add(size)
	sca.inuseBytes.Add(int64(size))
	sca.allocateObjects.Add(1)
	sca.inuseObjects.Add(1)
	sca.triggerUpdate()
	return ptr, dec, nil
}

func (sca *SimpleCAllocator) Deallocate(slice []byte, size uint64) {
	sca.inuseBytes.Add(-int64(size))
	sca.inuseObjects.Add(-1)
	sca.triggerUpdate()
	sca.ca.Deallocate(slice)
}

func (sca *SimpleCAllocator) triggerUpdate() {
	const simpleCAllocatorUpdateWindow = time.Second
	if sca.updating.CompareAndSwap(false, true) {
		time.AfterFunc(simpleCAllocatorUpdateWindow, func() {

			if sca.allocateBytesCounter != nil {
				var n uint64
				sca.allocateBytes.Each(func(v *atomic.Uint64) {
					n += v.Swap(0)
				})
				sca.allocateBytesCounter.Add(float64(n))
			}

			if sca.inuseBytesGauge != nil {
				var n int64
				sca.inuseBytes.Each(func(v *atomic.Int64) {
					n += v.Swap(0)
				})
				sca.inuseBytesGauge.Add(float64(n))
			}

			if sca.allocateObjectsCounter != nil {
				var n uint64
				sca.allocateObjects.Each(func(v *atomic.Uint64) {
					n += v.Swap(0)
				})
				sca.allocateObjectsCounter.Add(float64(n))
			}

			if sca.inuseObjectsGauge != nil {
				var n int64
				sca.inuseObjects.Each(func(v *atomic.Int64) {
					n += v.Swap(0)
				})
				sca.inuseObjectsGauge.Add(float64(n))
			}

			sca.updating.Store(false)
		})
	}
}
