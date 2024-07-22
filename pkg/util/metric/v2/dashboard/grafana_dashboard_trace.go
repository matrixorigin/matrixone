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
			//c.initTraceDurationRow(),
			c.initTraceCollectorOverviewRow(),
			c.initTraceMoLoggerExportDataRow(),
			c.initCronTaskRow(),
			c.initCUStatusRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initTraceMoLoggerExportDataRow() dashboard.Option {

	// export data bytes
	panels := c.getMultiHistogram(
		[]string{
			c.getMetricWithFilter(`mo_trace_mologger_export_data_bytes_bucket`, `type="sql"`),
			c.getMetricWithFilter(`mo_trace_mologger_export_data_bytes_bucket`, `type="csv"`),
		},
		[]string{
			"sql",
			"csv",
		},
		[]float64{0.50, 0.99},
		[]float32{3, 3},
		axis.Unit("bytes"),
		axis.Min(0),
	)

	// export files count
	panels = append(panels, c.withMultiGraph(
		"files",
		3,
		[]string{
			`sum(delta(` + c.getMetricWithFilter("mo_trace_mologger_export_data_bytes_count", "") + `[$interval])) by (type)`,
		},
		[]string{
			"{{type}}",
		}),
	)

	// ETLMerge files count
	panels = append(panels, c.withMultiGraph(
		"ETLMerge files",
		3,
		[]string{
			`sum(delta(` + c.getMetricWithFilter("mo_trace_etl_merge_total", "") + `[$interval]))`,
			`sum(delta(` + c.getMetricWithFilter("mo_trace_etl_merge_total", `type="success"`) + `[$interval]))`,
			`sum(delta(` + c.getMetricWithFilter("mo_trace_etl_merge_total", `type="exist"`) + `[$interval]))`,
			`sum(delta(` + c.getMetricWithFilter("mo_trace_etl_merge_total", `type="open_failed"`) + `[$interval]))`,
			`sum(delta(` + c.getMetricWithFilter("mo_trace_etl_merge_total", `type="read_failed"`) + `[$interval]))`,
			`sum(delta(` + c.getMetricWithFilter("mo_trace_etl_merge_total", `type="parse_failed"`) + `[$interval]))`,
			`sum(delta(` + c.getMetricWithFilter("mo_trace_etl_merge_total", `type="write_failed"`) + `[$interval]))`,
			`sum(delta(` + c.getMetricWithFilter("mo_trace_etl_merge_total", `type="delete_failed"`) + `[$interval]))`,
		},
		[]string{
			"total",
			"success",
			"exist",
			"open",
			"read",
			"parse",
			"write",
			"delete",
		}),
	)

	return dashboard.Row(
		"MOLogger Export",
		panels...,
	)
}

func (c *DashboardCreator) initTraceCollectorOverviewRow() dashboard.Option {

	panelP99Cost := c.getMultiHistogram(
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
		[]float64{0.99},
		[]float32{3},
		axis.Unit("s"),
		axis.Min(0))

	return dashboard.Row(
		"Collector Overview",

		c.withMultiGraph(
			"rate (avg) - each component",
			3,
			[]string{
				`avg(rate(` + c.getMetricWithFilter("mo_trace_collector_duration_seconds_count", `type="collect"`) + `[$interval])) by (type, matrixorigin_io_component)`,
			},
			[]string{
				"",
			}),

		c.withMultiGraph(
			"rate (sum)",
			3,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_trace_collector_duration_seconds_count", `type="collect"`) + `[$interval]))`,
			},
			[]string{
				"collect",
			}),

		c.withMultiGraph(
			"rate (sum) - no collect",
			3,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_trace_collector_duration_seconds_count", `type="generate_awake"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_trace_collector_duration_seconds_count", `type="generate_awake_discard"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_trace_collector_duration_seconds_count", `type="generate_delay"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_trace_collector_duration_seconds_count", `type="generate"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_trace_collector_duration_seconds_count", `type="generate_discard"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_trace_collector_duration_seconds_count", `type="export"`) + `[$interval]))`,
			},
			[]string{
				"generate_awake",
				"generate_awake_discard",
				"generate_delay",
				"generate",
				"generate_discard",
				"export",
			}),

		panelP99Cost[0],

		c.withMultiGraph(
			"Discard Count",
			12,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_trace_collector_discard_total", `type="statement_info"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_trace_collector_discard_total", `type="rawlog"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_trace_collector_discard_total", `type="metric"`) + `[$interval]))`,
			},
			[]string{
				"statement_info",
				"rawlog",
				"metric",
			}),
	)
}

func (c *DashboardCreator) initCUStatusRow() dashboard.Option {
	return dashboard.Row(
		"CU Status",
		c.withMultiGraph(
			"Negative CU status",
			6,
			[]string{
				`sum(delta(` + c.getMetricWithFilter("mo_trace_negative_cu_total", "") + `[$interval])) by (type)`,
			},
			[]string{"{{ type }}"}),
	)
}

func (c *DashboardCreator) initCronTaskRow() dashboard.Option {
	return dashboard.Row(
		"CronTask StorageUsage",
		c.withMultiGraph(
			"Check Count",
			3,
			[]string{
				`sum(delta(` + c.getMetricWithFilter("mo_trace_check_storage_usage_total", `type="all"`) + `[$interval:1m])) by (type)`,
				`sum(delta(` + c.getMetricWithFilter("mo_trace_check_storage_usage_total", `type="new"`) + `[$interval:1m])) by (type)`,
			},
			[]string{
				"check_all",
				"check_new",
			}),
		c.withMultiGraph(
			"New Account",
			3,
			[]string{
				`sum(delta(` + c.getMetricWithFilter("mo_trace_check_storage_usage_total", `type="inc"`) + `[$interval:1m])) by (type)`,
			},
			[]string{
				"new_inc",
			}),
	)
}
