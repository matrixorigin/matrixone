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
	"fmt"

	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/dashboard"
	"github.com/K-Phoen/grabana/row"
	"github.com/K-Phoen/grabana/timeseries"
	tsaxis "github.com/K-Phoen/grabana/timeseries/axis"
)

func (c *DashboardCreator) initTaskDashboard() error {
	folder, err := c.createFolder(c.folderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Task Metrics",
		c.withRowOptions(
			c.initTaskFlushTableTailRow(),
			c.initTaskMergeRow(),
			c.initTaskMergeTransferPageRow(),
			c.initCommitTimeRow(),
			c.initTaskCheckpointRow(),
			c.initTaskSelectivityRow(),
			c.initTaskStorageUsageRow(),
		)...)

	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initTaskMergeTransferPageRow() dashboard.Option {
	return dashboard.Row(
		"Task Merge Transfer Page",
		c.withGraph(
			"Transfer Page size",
			12,
			`sum(`+c.getMetricWithFilter("mo_task_merge_transfer_page_size", ``)+`)`+`* 28 * 1.3`,
			"{{ "+c.by+" }}", axis.Unit("decbytes"),
		),
		c.getTimeSeries(
			"Transfer page row count",
			[]string{fmt.Sprintf(
				"sum by (%s) (increase(%s[$interval]))",
				c.by,
				c.getMetricWithFilter(`mo_task_transfer_page_row_sum`, ""),
			)},
			[]string{"count"},
			timeseries.Span(3),
		),
		c.getTimeSeries(
			"Transfer page hit count",
			[]string{fmt.Sprintf(
				"sum by (%s) (increase(%s[$interval]))",
				c.by,
				c.getMetricWithFilter(`mo_task_transfer_page_hit_count_sum`, `type="total"`),
			)},
			[]string{"count"},
			timeseries.Span(3),
		),
		c.getPercentHist(
			"Transfer run ttl duration",
			c.getMetricWithFilter(`mo_task_transfer_duration_bucket`, `type="table_run_ttl_duration"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(3),
		),
		c.getPercentHist(
			"Transfer duration since born",
			c.getMetricWithFilter(`mo_task_transfer_duration_bucket`, `type="page_since_born_duration"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(3),
		),
		c.getPercentHist(
			"Transfer memory latency",
			c.getMetricWithFilter(`mo_task_transfer_short_duration_bucket`, `type="mem_latency"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(3),
		),
		c.getPercentHist(
			"Transfer disk latency",
			c.getMetricWithFilter(`mo_task_transfer_duration_bucket`, `type="disk_latency"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(3),
		),
		c.getPercentHist(
			"Transfer page write latency in flush",
			c.getMetricWithFilter(`mo_task_transfer_duration_bucket`, `type="page_flush_latency"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(3),
		),
		c.getPercentHist(
			"Transfer page write latency in merge",
			c.getMetricWithFilter(`mo_task_transfer_duration_bucket`, `type="page_merge_latency"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(3),
		),
	)
}

func (c *DashboardCreator) initCommitTimeRow() dashboard.Option {
	return dashboard.Row(
		"Commit Time",
		c.getPercentHist(
			"Flush Commit Time",
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="commit_table_tail"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(6),
		),
		c.getPercentHist(
			"Merge Commit Time",
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="commit_merge_objects"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(6),
		),
	)
}

func (c *DashboardCreator) initTaskFlushTableTailRow() dashboard.Option {
	return dashboard.Row(
		"Flush Table Tail",
		c.getTimeSeries(
			"Flush table tail Count",
			[]string{fmt.Sprintf(
				"sum by (%s) (increase(%s[$interval]))",
				c.by,
				c.getMetricWithFilter(`mo_task_short_duration_seconds_count`, `type="flush_table_tail"`),
			)},
			[]string{"Count"},
			timeseries.Span(3),
		),
		c.getPercentHist(
			"Flush table tail Duration",
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="flush_table_tail"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(3),
		),
		c.getPercentHist(
			"Flush table deletes count",
			c.getMetricWithFilter(`mo_task_hist_total_bucket`, `type="flush_deletes_count"`),
			[]float64{0.5, 0.7, 0.8, 0.9},
			timeseries.Axis(tsaxis.Unit("")),
			timeseries.Span(3),
			SpanNulls(true),
		),

		c.getPercentHist(
			"Flush table deletes file size",
			c.getMetricWithFilter(`mo_task_hist_bytes_bucket`, `type="flush_deletes_size"`),
			[]float64{0.5, 0.7, 0.8, 0.9},
			timeseries.Axis(tsaxis.Unit("decbytes")),
			timeseries.Span(3),
			SpanNulls(true),
		),
	)
}

func (c *DashboardCreator) initTaskMergeRow() dashboard.Option {
	return dashboard.Row(
		"Merge",
		c.getTimeSeries(
			"Merge Count",
			[]string{
				fmt.Sprintf(
					"sum by (%s) (increase(%s[$interval]))",
					c.by,
					c.getMetricWithFilter(`mo_task_scheduled_by_total`, `type="merge"`)),

				fmt.Sprintf(
					"sum by (%s) (increase(%s[$interval]))",
					c.by,
					c.getMetricWithFilter(`mo_task_scheduled_by_total`, `type="merge",nodetype="cn"`)),
			},
			[]string{
				"Schedule Count",
				"CN Schedule Count",
			},
		),
		c.getTimeSeries(
			"Merge Batch Size",
			[]string{fmt.Sprintf(
				"sum by (%s) (increase(%s[$interval]))",
				c.by,
				c.getMetricWithFilter(`mo_task_execute_results_total`, `type="merged_size"`))},
			[]string{"Size"},
			timeseries.Axis(tsaxis.Unit("decbytes")),
		),
	)
}

func (c *DashboardCreator) initTaskCheckpointRow() dashboard.Option {
	return dashboard.Row(
		"Checkpoint",
		c.getPercentHist(
			"Checkpoint Entry Pending",
			c.getMetricWithFilter(`mo_task_long_duration_seconds_bucket`, `type="ckp_entry_pending"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(12),
		),
	)
}

func (c *DashboardCreator) initTaskStorageUsageRow() dashboard.Option {
	rows := c.getMultiHistogram(
		[]string{
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="gckp_collect_usage"`),
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="ickp_collect_usage"`),
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="handle_usage_request"`),
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="show_accounts_get_table_stats"`),
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="show_accounts_get_storage_usage"`),
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="show_accounts_total_duration"`),
		},
		[]string{
			"gckp_collect_usage",
			"ickp_collect_usage",
			"handle_usage_request",
			"show_accounts_get_table_stats",
			"show_accounts_get_storage_usage",
			"show_accounts_total_duration",
		},
		[]float64{0.50, 0.8, 0.90, 0.99},
		[]float32{3, 3, 3, 3},
		axis.Unit("s"),
		axis.Min(0))

	rows = append(rows, c.withGraph(
		"tn storage usage cache mem used",
		12,
		`sum(`+c.getMetricWithFilter("mo_task_storage_usage_cache_size", ``)+`)`,
		"cache mem used",
		axis.Unit("mb")))

	return dashboard.Row(
		"Storage Usage Overview",
		rows...,
	)

}

func (c *DashboardCreator) initTaskSelectivityRow() dashboard.Option {

	hitRateFunc := func(title, metricType string) row.Option {
		return c.getTimeSeries(
			title,
			[]string{
				fmt.Sprintf(
					"sum(%s) by (%s) / on(%s) sum(%s) by (%s)",
					c.getMetricWithFilter(`mo_task_selectivity`, `type="`+metricType+`_hit"`), c.by, c.by,
					c.getMetricWithFilter(`mo_task_selectivity`, `type="`+metricType+`_total"`), c.by),
			},
			[]string{fmt.Sprintf("filterout-{{ %s }}", c.by)},
			timeseries.Span(4),
		)
	}
	counterRateFunc := func(title, metricType string) row.Option {
		return c.getTimeSeries(
			title,
			[]string{
				fmt.Sprintf(
					"sum(rate(%s[$interval])) by (%s)",
					c.getMetricWithFilter(`mo_task_selectivity`, `type="`+metricType+`_total"`), c.by),
			},
			[]string{fmt.Sprintf("req-{{ %s }}", c.by)},
			timeseries.Span(4),
		)
	}
	return dashboard.Row(
		"Read Selectivity",
		hitRateFunc("Read filter rate", "readfilter"),
		hitRateFunc("Block range filter rate", "block"),
		hitRateFunc("Column update filter rate", "column"),
		counterRateFunc("Read filter request", "readfilter"),
		counterRateFunc("Block range request", "block"),
		counterRateFunc("Column update request", "column"),
		c.getPercentHist(
			"Iterate deletes rows count per block",
			c.getMetricWithFilter(`mo_task_hist_total_bucket`, `type="load_mem_deletes_per_block"`),
			[]float64{0.5, 0.7, 0.8, 0.9},
			timeseries.Axis(tsaxis.Unit("")),
			timeseries.Span(4),
			SpanNulls(true),
		),
	)
}
