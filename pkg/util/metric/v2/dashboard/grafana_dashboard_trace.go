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

func (c *DashboardCreator) initTraceDashboard() error {
	folder, err := c.createFolder(moFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Trace Metrics",
		c.withRowOptions(
			c.initTraceDurationRow(),
			c.initCUStatusRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initTraceDurationRow() dashboard.Option {
	return dashboard.Row(
		"Trace Collector duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_trace_collector_duration_seconds_bucket`, `type="collect"`),
				c.getMetricWithFilter(`mo_trace_collector_duration_seconds_bucket`, `type="generate_awake"`),
				c.getMetricWithFilter(`mo_trace_collector_duration_seconds_bucket`, `type="generate_awake_discard"`),
				c.getMetricWithFilter(`mo_trace_collector_duration_seconds_bucket`, `type="generate_delay"`),
				c.getMetricWithFilter(`mo_trace_collector_duration_seconds_bucket`, `type="generate"`),
				c.getMetricWithFilter(`mo_trace_collector_duration_seconds_bucket`, `type="generate_discard"`),
				c.getMetricWithFilter(`mo_trace_collector_duration_seconds_bucket`, `type="export"`),
			},
			[]string{
				"collect",
				"generate_awake",
				"generate_awake_discard",
				"generate_delay",
				"generate",
				"generate_discard",
				"export",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initCUStatusRow() dashboard.Option {
	return dashboard.Row(
		"CU Status",
		c.withMultiGraph(
			"Negative CU status",
			4,
			[]string{
				`sum(delta(` + c.getMetricWithFilter("mo_trace_negative_cu_total", "") + `[$interval])) by (type)`,
			},
			[]string{"{{ type }}"}),
	)
}
