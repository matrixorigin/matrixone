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
			c.initShardingScheduleRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initShardingOverviewRow() dashboard.Option {
	return dashboard.Row(
		"Replica Overview",
		c.withMultiGraph(
			"Replica Count",
			6,
			[]string{
				`sum(` + c.getMetricWithFilter("mo_sharding_replica_count", ``) + `)`,
			},
			[]string{
				"replicas",
			}),

		c.withMultiGraph(
			"Replica Reads",
			6,
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

func (c *DashboardCreator) initShardingScheduleRow() dashboard.Option {
	return dashboard.Row(
		"Schedule Overview",
		c.withMultiGraph(
			"TN Schedule",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_schedule_replica_total", `type="allocate"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_schedule_replica_total", `type="re-allocate"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_schedule_replica_total", `type="balance"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_schedule_replica_total", `type="skip-no-cn"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_schedule_replica_total", `type="skip-freeze-cn"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_schedule_replica_total", `type="apply-add"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_schedule_replica_total", `type="apply-delete"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_schedule_replica_total", `type="apply-delete-all"`) + `[$interval]))`,
			},
			[]string{
				"allocate",
				"re-allocate",
				"balance",
				"skip-no-cn",
				"skip-freeze-cn",
				"apply-add",
				"apply-delete",
				"apply-delete-all",
			}),

		c.withMultiGraph(
			"Freeze CN",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_sharding_schedule_freeze_cn_count", ``) + `[$interval]))`,
			},
			[]string{
				"freeze-cn-count",
			}),
	)
}
