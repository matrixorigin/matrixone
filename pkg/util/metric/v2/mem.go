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

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	memMPoolAllocatedSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "mem",
			Name:      "mpool_allocated_size",
			Help:      "Size of mpool have allocated.",
		}, []string{"type"})

	MemTAEDefaultAllocatorGauge           = memMPoolAllocatedSizeGauge.WithLabelValues("tae_default")
	MemTAEMutableAllocatorGauge           = memMPoolAllocatedSizeGauge.WithLabelValues("tae_mutable")
	MemTAESmallAllocatorGauge             = memMPoolAllocatedSizeGauge.WithLabelValues("tae_small")
	MemTAEVectorPoolDefaultAllocatorGauge = memMPoolAllocatedSizeGauge.WithLabelValues("vectorpool_default")
	MemTAELogtailAllocatorGauge           = memMPoolAllocatedSizeGauge.WithLabelValues("tae_logtail")
	MemTAECheckpointAllocatorGauge        = memMPoolAllocatedSizeGauge.WithLabelValues("tae_checkpoint")
	MemTAEMergeAllocatorGauge             = memMPoolAllocatedSizeGauge.WithLabelValues("tae_merge")
	MemTAEWorkSpaceAllocatorGauge         = memMPoolAllocatedSizeGauge.WithLabelValues("tae_workspace")
	MemTAEDebugAllocatorGauge             = memMPoolAllocatedSizeGauge.WithLabelValues("tae_debug")
	MemGlobalStatsAllocatedGauge          = memMPoolAllocatedSizeGauge.WithLabelValues("global_stats_allocated")

	memMPoolHighWaterMarkGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "mem",
			Name:      "mpool_high_water_mark_size",
			Help:      "Size of high water mark mp have ever reached",
		}, []string{"type"})

	MemTAEDefaultHighWaterMarkGauge           = memMPoolHighWaterMarkGauge.WithLabelValues("tae_default_high_water_mark")
	MemTAEMutableHighWaterMarkGauge           = memMPoolHighWaterMarkGauge.WithLabelValues("tae_mutable_high_water_mark")
	MemTAESmallHighWaterMarkGauge             = memMPoolHighWaterMarkGauge.WithLabelValues("tae_small_high_water_mark")
	MemTAEVectorPoolDefaultHighWaterMarkGauge = memMPoolHighWaterMarkGauge.WithLabelValues("vectorpool_default_high_water_mark")
	MemTAELogtailHighWaterMarkGauge           = memMPoolHighWaterMarkGauge.WithLabelValues("tae_logtail_high_water_mark")
	MemTAECheckpointHighWaterMarkGauge        = memMPoolHighWaterMarkGauge.WithLabelValues("tae_checkpoint_high_water_mark")
	MemTAEMergeHighWaterMarkGauge             = memMPoolHighWaterMarkGauge.WithLabelValues("tae_merge_high_water_mark")
	MemTAEWorkSpaceHighWaterMarkGauge         = memMPoolHighWaterMarkGauge.WithLabelValues("tae_workspace_high_water_mark")
	MemTAEDebugHighWaterMarkGauge             = memMPoolHighWaterMarkGauge.WithLabelValues("tae_debug_high_water_mark")
	MemGlobalStatsHighWaterMarkGauge          = memMPoolHighWaterMarkGauge.WithLabelValues("global_stats_allocated_high_water_mark")
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

var (
	mallocCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "mem",
			Name:      "malloc_counter",
			Help:      "malloc counter",
		},
		[]string{"type"},
	)

	mallocGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "mem",
			Name:      "malloc_gauge",
			Help:      "malloc gauge",
		},
		[]string{"type"},
	)

	// all
	MallocCounterAllocateBytes   = mallocCounter.WithLabelValues("allocate")
	MallocCounterAllocateObjects = mallocCounter.WithLabelValues("allocate-objects")
	MallocGaugeInuseBytes        = mallocGauge.WithLabelValues("inuse")
	MallocGaugeInuseObjects      = mallocGauge.WithLabelValues("inuse-objects")

	// memory cache
	MallocCounterMemoryCacheAllocateBytes   = mallocCounter.WithLabelValues("memory-cache-allocate")
	MallocCounterMemoryCacheAllocateObjects = mallocCounter.WithLabelValues("memory-cache-allocate-objects")
	MallocGaugeMemoryCacheInuseBytes        = mallocGauge.WithLabelValues("memory-cache-inuse")
	MallocGaugeMemoryCacheInuseObjects      = mallocGauge.WithLabelValues("memory-cache-inuse-objects")

	// io
	MallocCounterIOAllocateBytes   = mallocCounter.WithLabelValues("io-allocate")
	MallocCounterIOAllocateObjects = mallocCounter.WithLabelValues("io-allocate-objects")
	MallocGaugeIOInuseBytes        = mallocGauge.WithLabelValues("io-inuse")
	MallocGaugeIOInuseObjects      = mallocGauge.WithLabelValues("io-inuse-objects")

	// bytes
	MallocCounterBytesAllocateBytes   = mallocCounter.WithLabelValues("bytes-allocate")
	MallocCounterBytesAllocateObjects = mallocCounter.WithLabelValues("bytes-allocate-objects")
	MallocGaugeBytesInuseBytes        = mallocGauge.WithLabelValues("bytes-inuse")
	MallocGaugeBytesInuseObjects      = mallocGauge.WithLabelValues("bytes-inuse-objects")

	// session
	MallocCounterSessionAllocateBytes   = mallocCounter.WithLabelValues("session-allocate")
	MallocCounterSessionAllocateObjects = mallocCounter.WithLabelValues("session-allocate-objects")
	MallocGaugeSessionInuseBytes        = mallocGauge.WithLabelValues("session-inuse")
	MallocGaugeSessionInuseObjects      = mallocGauge.WithLabelValues("session-inuse-objects")
)
