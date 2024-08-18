// Copyright 2021-2024 Matrix Origin
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

func (c *DashboardCreator) initShardingDashboard() error {
	folder, err := c.createFolder(c.folderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Sharding Metrics",
		c.withRowOptions(
			c.initShardingOverviewRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initShardingOverviewRow() dashboard.Option {
	return dashboard.Row(
		"Sharding overview",
		c.withMultiGraph(
			"Replica Count",
			4,
			[]string{
				`sum(` + c.getMetricWithFilter("mo_sharding_replica_count", ``) + `)`,
			},
			[]string{
				"replicas",
			}),

		c.withMultiGraph(
			"Replica Operators",
			4,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_schedule_replica_total", `type="add"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_schedule_replica_total", `type="delete"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_schedule_replica_total", `type="delete-all"`) + `[$interval]))`,
			},
			[]string{
				"add-replica",
				"delete-replica",
				"delete-all-replica",
			}),

		c.withMultiGraph(
			"Replica Reads",
			4,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_replica_read_total", `type="local"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_replica_read_total", `type="remote"`) + `[$interval]))`,
			},
			[]string{
				"local-read",
				"remote-read",
			}),
	)
}
