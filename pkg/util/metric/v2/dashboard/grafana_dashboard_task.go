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

	"github.com/K-Phoen/grabana/dashboard"
)

func (c *DashboardCreator) initTaskDashboard() error {
	folder, err := c.createFolder(taskFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Task Metrics",
		c.withRowOptions(
			c.initTaskFlushTableTailRow(),
			c.initTaskCkpEntryPendingRow(),
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
			`mo_task_duration_seconds_bucket{type="flush_table_tail", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTaskCkpEntryPendingRow() dashboard.Option {
	return dashboard.Row(
		"Flush Table Tail Duration",
		c.getHistogram(
			`mo_task_duration_seconds_bucket{type="ckp_entry_pending", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}
