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

package dashboard

import (
	"context"

	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/dashboard"
)

func (c *DashboardCreator) initRuntimeDashboard() error {
	folder, err := c.createFolder(moFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Go Runtime Metrics",
		c.withRowOptions(
			c.initMemoryRow(),
			c.initGCRow(),
			c.initGoroutineRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initGCRow() dashboard.Option {
	return dashboard.Row(
		"Go GC Status",
		c.getHistogramWithExtraBy(
			"STW duration",
			c.getMetricWithFilter(`go_gc_pauses_seconds_bucket`, ``),
			[]float64{0.8, 0.90, 0.95, 0.99},
			12,
			c.by,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initGoroutineRow() dashboard.Option {
	return dashboard.Row(
		"Goroutine Status",
		c.withGraph(
			"Goroutine count",
			6,
			`sum(`+c.getMetricWithFilter("go_goroutines", "")+`) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),

		c.getHistogramWithExtraBy(
			"Schedule latency duration",
			c.getMetricWithFilter(`go_sched_latencies_seconds_bucket`, ``),
			[]float64{0.8, 0.90, 0.95, 0.99},
			6,
			c.by,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initMemoryRow() dashboard.Option {
	return dashboard.Row(
		"Memory Status",
		c.withGraph(
			"Live Objects",
			3,
			`sum(`+c.getMetricWithFilter("go_gc_heap_objects_objects", "")+`) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),

		c.withGraph(
			"Free and ready to return system",
			3,
			`sum(`+c.getMetricWithFilter("go_memory_classes_heap_free_bytes", "")+`) by (`+c.by+`)`,
			"{{ "+c.by+" }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"Dead objects and not marked free live objects",
			3,
			`sum(`+c.getMetricWithFilter("go_memory_classes_heap_objects_bytes", "")+`) by (`+c.by+`)`,
			"{{ "+c.by+" }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"Released to system",
			3,
			`sum(`+c.getMetricWithFilter("go_memory_classes_heap_released_bytes", "")+`) by (`+c.by+`)`,
			"{{ "+c.by+" }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"Heap Allocation Bytes/s",
			3,
			`sum(rate(`+c.getMetricWithFilter("go_gc_heap_allocs_bytes_total", "")+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"Heap Free Bytes/s",
			3,
			`sum(rate(`+c.getMetricWithFilter("go_gc_heap_frees_bytes_total", "")+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"Heap Allocation Object/s",
			3,
			`sum(rate(`+c.getMetricWithFilter("go_gc_heap_allocs_objects_total", "")+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),

		c.withGraph(
			"Heap Free Object/s",
			3,
			`sum(rate(`+c.getMetricWithFilter("go_gc_heap_frees_objects_total", "")+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),

		c.getHistogram(
			"Allocation bytes size",
			c.getMetricWithFilter(`go_gc_heap_allocs_by_size_bytes_bucket`, ``),
			[]float64{0.8, 0.90, 0.95, 0.99},
			12,
			axis.Unit("bytes"),
			axis.Min(0)),
	)
}
