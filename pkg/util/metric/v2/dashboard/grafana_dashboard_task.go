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

func (c *DashboardCreator) initTaskDashboard() error {
	folder, err := c.createFolder(moFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Task Metrics",
		c.withRowOptions(
			c.initTaskFlushTableTailRow(),
			c.initTaskCkpEntryPendingRow(),
			c.initTaskMergeRow(),
			c.initTaskGCkpCollectUsageRow(),
			c.initTaskICkpCollectUsageRow(),
			c.initTaskCkpCollectUsageRow(),
		)...)

	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initTaskFlushTableTailRow() dashboard.Option {
	return dashboard.Row(
		"Flush Table Tail Duration",
		c.getHistogram(
			"Flush Table Tail Duration",
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="flush_table_tail"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTaskGCkpCollectUsageRow() dashboard.Option {
	return dashboard.Row(
		"Global Checkpoint Collects Storage Usage Duration",
		c.getHistogram(
			"Global Checkpoint Collects Storage Usage Duration",
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="gckp_collect_usage"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTaskICkpCollectUsageRow() dashboard.Option {
	return dashboard.Row(
		"Incremental Checkpoint Collects Storage Usage Duration",
		c.getHistogram(
			"Incremental Checkpoint Collects Storage Usage Duration",
			c.getMetricWithFilter(`mo_task_short_duration_seconds_bucket`, `type="ickp_collect_uage"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTaskCkpEntryPendingRow() dashboard.Option {
	return dashboard.Row(
		"Checkpoint Entry Pending Time",
		c.getHistogram(
			"Checkpoint Entry Pending Time",
			c.getMetricWithFilter(`mo_task_long_duration_seconds_bucket`, `type="ckp_entry_pending"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTaskMergeRow() dashboard.Option {
	return dashboard.Row(
		"Task Merge Related Status",
		c.withGraph(
			"Scheduled By Counting",
			4,
			`sum(increase(`+c.getMetricWithFilter("mo_task_scheduled_by_total", `type="merge"`)+`[$interval])) by (`+c.by+`, type)`,
			"{{"+c.by+"-type }}"),
		c.withGraph(
			"Merged Blocks Each Schedule",
			4,
			`sum(increase(`+c.getMetricWithFilter("mo_task_execute_results_total", `type="merged_block"`)+`[$interval])) by (`+c.by+`, type)`,
			"{{"+c.by+"-type }}"),
		c.withGraph(
			"Merged Size Each Schedule",
			4,
			`sum(increase(`+c.getMetricWithFilter("mo_task_execute_results_total", `type="merged_size"`)+`[$interval])) by (`+c.by+`, type)`,
			"{{ "+c.by+"-type }}"),
	)
}

func (c *DashboardCreator) initTaskCkpCollectUsageRow() dashboard.Option {
	return dashboard.Row(
		"Task Checkpoint Collects Usage Row",
		c.withGraph(
			"Global Checkpoint Load Object Count",
			6,
			`sum(increase(`+c.getMetricWithFilter("mo_task_execute_results_total", `type="gckp_load_object"`)+`[$interval])) by (`+c.by+`, type)`,
			"{{"+c.by+"-type }}"),
		c.withGraph(
			"Incremental Checkpoint Load Object Count",
			6,
			`sum(increase(`+c.getMetricWithFilter("mo_task_execute_results_total", `type="ickp_load_object"`)+`[$interval])) by (`+c.by+`, type)`,
			"{{"+c.by+"-type }}"),
	)
}
