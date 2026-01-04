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

// Package dashboard provides Grafana dashboard creation utilities for MatrixOne metrics.
// This file specifically handles memory-related metrics visualization, including mpool allocators,
// malloc statistics, and off-heap memory usage tracking.
package dashboard

import (
	"context"
	"fmt"
	"strings"

	"github.com/K-Phoen/grabana/dashboard"
	"github.com/K-Phoen/grabana/row"
)

// initMemDashboard initializes the Memory Metrics dashboard in Grafana.
// This dashboard displays comprehensive memory usage statistics including:
//   - Mpool allocator metrics: current allocated size and high water marks for various TAE allocators
//   - Malloc statistics: allocation counters and in-use gauges for different components
//   - Off-heap memory usage: current off-heap memory consumption by component
//
// The dashboard is organized into rows, each containing multiple graphs for different memory metrics.
func (c *DashboardCreator) initMemDashboard() error {
	folder, err := c.createFolder(c.folderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Memory Metrics",
		c.withRowOptions(
			c.initMpoolAllocatorRow(),
			c.initMallocRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

// initMpoolAllocatorRow creates a dashboard row displaying mpool (memory pool) allocator metrics.
// Mpool is MatrixOne's custom memory allocator that manages memory allocation for the TAE (Transaction-Aware Engine) storage engine.
//
// This row displays metrics for multiple allocator types:
//   - tae_default: Default allocator for general TAE operations
//   - tae_mutable: Allocator for mutable memory operations in TAE
//   - tae_small: Allocator optimized for small memory allocations
//   - vectorpool_default: Allocator for vector pool operations
//   - tae_logtail: Allocator for logtail operations
//   - tae_checkpoint: Allocator for checkpoint operations
//   - tae_merge: Allocator for merge operations
//   - tae_workspace: Allocator for workspace operations
//   - tae_debug: Allocator for debug operations
//   - global_stats_allocated: Global statistics for allocated memory
//
// For each allocator, two metrics are displayed:
//   1. mo_mem_mpool_allocated_size: Current allocated memory size in bytes (gauge)
//      - Represents the real-time memory consumption by the allocator
//      - Only tracks off-heap memory allocations (allocations with offHeap=true)
//      - Metric name: mo_mem_mpool_allocated_size{type="<allocator_name>"}
//
//   2. mo_mem_mpool_high_water_mark_size: Peak memory usage ever reached (gauge)
//      - Tracks the maximum memory usage since the system started
//      - Useful for understanding peak memory requirements and capacity planning
//      - Metric name: mo_mem_mpool_high_water_mark_size{type="<allocator_name>_high_water_mark"}
//
// Additionally, this row includes:
//   - mo_mem_cross_pool_free_total: Counter for cross-pool memory free operations
//     - Increments when memory allocated from one pool is freed in a different pool
//     - Indicates potential memory management issues or cross-pool memory leaks
//     - Metric name: mo_mem_cross_pool_free_total
//     - Displayed as rate of increase over the dashboard interval
func (c *DashboardCreator) initMpoolAllocatorRow() dashboard.Option {
	options := make([]row.Option, 0)
	// List of allocator names to monitor. Each allocator serves a specific purpose:
	// - tae_default: General-purpose allocator for TAE operations
	// - tae_mutable: Allocator for mutable memory structures
	// - tae_small: Optimized for small allocations to reduce fragmentation
	// - vectorpool_default: Allocator for vector pool operations
	// - tae_logtail: Allocator for logtail processing
	// - tae_checkpoint: Allocator for checkpoint operations
	// - tae_merge: Allocator for merge operations during compaction
	// - tae_workspace: Allocator for temporary workspace operations
	// - tae_debug: Allocator for debug and diagnostic operations
	// - global_stats_allocated: Global aggregated statistics across all allocators
	names := []string{
		"tae_default", "tae_mutable", "tae_small",
		"vectorpool_default", "tae_logtail",
		"tae_checkpoint", "tae_merge", "tae_workspace",
		"tae_debug", "global_stats_allocated",
	}

	// Create a graph for each allocator showing both current allocation and high water mark
	for idx := 0; idx < len(names); idx++ {
		options = append(options, c.withMultiGraph(
			strings.ToTitle(strings.Replace(names[idx], "_", " ", 10)),
			3, // Graph width: 3 columns out of 12
			[]string{
				// Metric: mo_mem_mpool_allocated_size
				// Description: Current allocated memory size in bytes for this allocator
				// Type: Gauge (can go up or down)
				// Note: Only tracks off-heap allocations (offHeap=true)
				// Usage: Monitor real-time memory consumption per allocator
				`sum(` + c.getMetricWithFilter("mo_mem_mpool_allocated_size", fmt.Sprintf(`type="%s"`, names[idx])) + `)`,
				// Metric: mo_mem_mpool_high_water_mark_size
				// Description: Maximum memory size ever allocated by this allocator since startup
				// Type: Gauge (monotonically increasing until restart)
				// Usage: Understand peak memory requirements and plan capacity
				`sum(` + c.getMetricWithFilter("mo_mem_mpool_high_water_mark_size", fmt.Sprintf(`type="%s_high_water_mark"`, names[idx])) + `)`,
			},
			[]string{
				names[idx],
				names[idx] + "_high_water_mark",
			}))
	}

	// Add cross-pool free counter graph
	// This metric tracks memory that was allocated from one pool but freed in a different pool.
	// High values may indicate memory management issues or cross-pool memory leaks.
	options = append(options, c.withGraph(
		"Cross Pool Free Counter",
		6, // Graph width: 6 columns out of 12
		// Metric: mo_mem_cross_pool_free_total
		// Description: Total count of cross-pool memory free operations
		// Type: Counter (monotonically increasing)
		// Calculation: increase() function calculates the rate of cross-pool frees per interval
		// Usage: Monitor for memory management anomalies and potential leaks
		`increase(`+c.getMetricWithFilter("mo_mem_cross_pool_free_total", "")+`[$interval])`,
		""))

	return dashboard.Row(
		"TAE Mpool Allocator",
		options...,
	)
}

// initMallocRow creates a dashboard row displaying malloc-related memory statistics.
// This row tracks memory allocations using C malloc/calloc/realloc functions, which are used
// for off-heap memory management in MatrixOne.
//
// The row displays metrics for different components:
//   - all: Aggregate statistics across all components
//   - memory-cache: Memory cache component allocations
//   - io: I/O operations memory allocations
//   - session: Session-related memory allocations
//   - hashmap: Hash map data structure allocations
//   - mpool: Memory pool allocator allocations
//
// For each component, four metrics are displayed:
//   1. allocate bytes: Total bytes allocated (counter)
//   2. inuse bytes: Currently in-use bytes (gauge)
//   3. allocate objects: Total number of objects allocated (counter)
//   4. inuse objects: Currently in-use objects (gauge)
//
// Additionally, the row includes an off-heap inuse graph showing current off-heap memory
// consumption broken down by component type.
func (c *DashboardCreator) initMallocRow() dashboard.Option {
	// makeGraph creates a multi-graph panel for a specific component prefix
	// prefix: Component name prefix (e.g., "memory-cache-", "io-", etc.)
	//         Empty string "" represents aggregate statistics across all components
	makeGraph := func(prefix string) row.Option {
		name := prefix
		if name == "" {
			name = "all"
		}
		return c.withMultiGraph(
			name,
			4, // Graph width: 4 columns out of 12
			[]string{
				// Metric: mo_mem_malloc_counter{type="<prefix>allocate"}
				// Description: Total bytes allocated via malloc/calloc/realloc for this component
				// Type: Counter (monotonically increasing)
				// Usage: Track cumulative memory allocation over time
				`sum(` + c.getMetricWithFilter("mo_mem_malloc_counter", `type="`+prefix+`allocate"`) + `)`,
				// Metric: mo_mem_malloc_gauge{type="<prefix>inuse"}
				// Description: Current bytes in use (allocated but not yet freed) for this component
				// Type: Gauge (can go up or down)
				// Usage: Monitor real-time memory consumption
				`sum(` + c.getMetricWithFilter("mo_mem_malloc_gauge", `type="`+prefix+`inuse"`) + `)`,
				// Metric: mo_mem_malloc_counter{type="<prefix>allocate-objects"}
				// Description: Total number of memory objects allocated for this component
				// Type: Counter (monotonically increasing)
				// Usage: Track allocation frequency and object count trends
				`sum(` + c.getMetricWithFilter("mo_mem_malloc_counter", `type="`+prefix+`allocate-objects"`) + `)`,
				// Metric: mo_mem_malloc_gauge{type="<prefix>inuse-objects"}
				// Description: Current number of memory objects in use for this component
				// Type: Gauge (can go up or down)
				// Usage: Monitor active object count
				`sum(` + c.getMetricWithFilter("mo_mem_malloc_gauge", `type="`+prefix+`inuse-objects"`) + `)`,
			},
			[]string{
				prefix + "allocate bytes",
				prefix + "inuse bytes",
				prefix + "allocate objects",
				prefix + "inuse objects",
			},
		)
	}
	return dashboard.Row(
		"malloc",
		// Aggregate statistics across all components
		makeGraph(""),
		// Memory cache component statistics
		makeGraph("memory-cache-"),
		// I/O operations component statistics
		makeGraph("io-"),
		// Session-related component statistics
		makeGraph("session-"),
		// Hash map component statistics
		makeGraph("hashmap-"),
		// Memory pool component statistics
		makeGraph("mpool-"),
		// Off-heap memory usage breakdown by component
		// This graph shows the absolute current off-heap memory consumption for each component
		// and the total across all components. Off-heap memory is allocated via C malloc/calloc/realloc
		// and is not managed by Go's garbage collector.
		c.withMultiGraph(
			"offheap inuse (absolute, by component)",
			6, // Graph width: 6 columns out of 12
			[]string{
				// Metric: mo_mem_offheap_inuse_bytes{type="mpool"}
				// Description: Current off-heap bytes in use by mpool component
				// Type: Gauge
				// Usage: Monitor mpool off-heap memory consumption
				`sum(` + c.getMetricWithFilter("mo_mem_offheap_inuse_bytes", `type="mpool"`) + `)`,
				// Metric: mo_mem_offheap_inuse_bytes{type="memory-cache"}
				// Description: Current off-heap bytes in use by memory-cache component
				// Type: Gauge
				// Usage: Monitor memory cache off-heap consumption
				`sum(` + c.getMetricWithFilter("mo_mem_offheap_inuse_bytes", `type="memory-cache"`) + `)`,
				// Metric: mo_mem_offheap_inuse_bytes{type="io"}
				// Description: Current off-heap bytes in use by I/O component
				// Type: Gauge
				// Usage: Monitor I/O operations off-heap memory usage
				`sum(` + c.getMetricWithFilter("mo_mem_offheap_inuse_bytes", `type="io"`) + `)`,
				// Metric: mo_mem_offheap_inuse_bytes{type="session"}
				// Description: Current off-heap bytes in use by session component
				// Type: Gauge
				// Usage: Monitor session-related off-heap memory consumption
				`sum(` + c.getMetricWithFilter("mo_mem_offheap_inuse_bytes", `type="session"`) + `)`,
				// Metric: mo_mem_offheap_inuse_bytes{type="hashmap"}
				// Description: Current off-heap bytes in use by hashmap component
				// Type: Gauge
				// Usage: Monitor hash map data structures off-heap memory usage
				`sum(` + c.getMetricWithFilter("mo_mem_offheap_inuse_bytes", `type="hashmap"`) + `)`,
				// Metric: mo_mem_offheap_inuse_bytes (no type label)
				// Description: Total current off-heap bytes in use across all components
				// Type: Gauge
				// Usage: Monitor total off-heap memory consumption system-wide
				`sum(` + c.getMetricWithFilter("mo_mem_offheap_inuse_bytes", ``) + `)`,
			},
			[]string{
				"mpool",
				"memory-cache",
				"io",
				"session",
				"hashmap",
				"total",
			}),
	)
}
