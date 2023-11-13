// Copyright 2023 Matrix Origin
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

package v2

import "github.com/prometheus/client_golang/prometheus"

var (
	memMPoolAllocatedSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "mem",
			Name:      "mpool_allocated_size",
			Help:      "Size of mpool have allocated.",
		}, []string{"type"})

	MemTAEDefaultAllocatorGauge    = memMPoolAllocatedSizeGauge.WithLabelValues("tae_default")
	MemTAEMutableAllocatorGauge    = memMPoolAllocatedSizeGauge.WithLabelValues("tae_mutable")
	MemTAESmallAllocatorGauge      = memMPoolAllocatedSizeGauge.WithLabelValues("tae_small")
	MemTAEVectorPoolSmallGauge     = memMPoolAllocatedSizeGauge.WithLabelValues("vectorpool_default")
	MemTAEVectorPoolTransientGauge = memMPoolAllocatedSizeGauge.WithLabelValues("vectorpool_transient")
)

var (
	MemTotalCrossPoolFreeCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "mem",
			Name:      "cross_pool_free_total",
			Help:      "Total number of cross pool free",
		})
)
