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

	"github.com/K-Phoen/grabana/dashboard"
	"github.com/K-Phoen/grabana/row"
	"github.com/K-Phoen/grabana/timeseries"
	tsaxis "github.com/K-Phoen/grabana/timeseries/axis"
)

func (c *DashboardCreator) initTaskDashboard() error {
	folder, err := c.createFolder(moFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Task Metrics",
		c.withRowOptions(
			c.initTaskFlushTableTailRow(),
			c.initTaskMergeRow(),
			c.initTaskCheckpointRow(),
			c.initTaskSelectivityRow(),
			c.initTaskMergeTransferPageRow(),
		)...)

	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initTaskMergeTransferPageRow() dashboard.Option {
	return dashboard.Row(
		"Task Merge Transfer Page Length",
		c.withGraph(
			"Transfer Page Length",
			12,
			`sum(`+c.getMetricWithFilter("mo_task_merge_transfer_page_size", ``)+`)`,
			"{{ "+c.by+" }}"),
	)
}

func (c *DashboardCreator) initTaskFlushTableTailRow() dashboard.Option {
	return dashboard.Row(
		"Flush Table Tail",
		c.getTimeSeries(
			"Flush table tail Count",
			[]string{fmt.Sprintf(
				"increase(%s[$interval])",
				c.getMetricWithFilter(`mo_task_short_duration_seconds_count`, `type="flush_table_tail"`),
			)},
			[]string{"Count"},
		),
		c.getPercentHist(
			"Flush table tail Duration",
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="flush_table_tail"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
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
					"increase(%s[$interval])",
					c.getMetricWithFilter(`mo_task_execute_results_total`, `type="merged_block"`)),

				fmt.Sprintf(
					"increase(%s[$interval])",
					c.getMetricWithFilter(`mo_task_scheduled_by_total`, `type="merge"`)),
			},
			[]string{
				"Block Count",
				"Schedule Count",
			},
		),
		c.getTimeSeries(
			"Merge Batch Size",
			[]string{fmt.Sprintf(
				"increase(%s[$interval])",
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
			timeseries.Span(3),
		),
		c.getPercentHist(
			"ICheckpoint Collecting Duration",
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="ickp_collect_uage"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(3),
		),
		c.getTimeSeries(
			"Checkpoint Load Object Count",
			[]string{
				fmt.Sprintf(
					"increase(%s[$interval])",
					c.getMetricWithFilter(`mo_task_execute_results_total`, `type="gckp_load_object"`)),

				fmt.Sprintf(
					"increase(%s[$interval])",
					c.getMetricWithFilter(`mo_task_execute_results_total`, `type="ickp_load_object"`)),
			},
			[]string{
				"GCheckpoint",
				"ICheckpoint",
			},
			timeseries.Span(3),
		),
		c.getPercentHist(
			"GCheckpoint Collecting Duration",
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="gckp_collect_uage"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(3),
		),
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
	)
}
