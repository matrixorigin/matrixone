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

func (c *DashboardCreator) initLogTailDashboard() error {
	folder, err := c.createFolder(moFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Logtail Metrics",
		c.withRowOptions(
			c.initLogtailOverviewRow(),
			c.initLogtailQueueRow(),
			c.initLogtailBytesRow(),
			c.initLogtailLoadCheckpointRow(),
			c.initLogtailCollectRow(),
			c.initLogtailSubscriptionRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initLogtailCollectRow() dashboard.Option {
	return dashboard.Row(
		"Logtail collect duration",
		c.getHistogram(
			"collect duration",
			c.getMetricWithFilter("mo_logtail_collect_duration_seconds_bucket", ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initLogtailSubscriptionRow() dashboard.Option {
	return dashboard.Row(
		"logtail subscription the tn have received",
		c.withGraph(
			"logtail subscription average increase",
			6,
			`sum(rate(`+c.getMetricWithFilter("mo_logtail_subscription_request_total", "")+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),
		c.withGraph(
			"logtail subscription average increase, sensitive",
			6,
			`sum(irate(`+c.getMetricWithFilter("mo_logtail_subscription_request_total", "")+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),
	)
}

func (c *DashboardCreator) initLogtailQueueRow() dashboard.Option {
	return dashboard.Row(
		"Logtail Queue Status",
		c.withMultiGraph(
			"Sending Queue",
			6,
			[]string{
				`sum(` + c.getMetricWithFilter("mo_logtail_queue_size", `type="send"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_logtail_queue_size", `type="receive"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_logtail_queue_size", `type="apply"`) + `)`,
			},
			[]string{
				"send",
				"receive",
				"apply",
			}),
		c.withGraph(
			"Checkpoint logtail",
			6,
			`sum(rate(`+c.getMetricWithFilter("mo_logtail_load_checkpoint_total", "")+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),
	)
}

func (c *DashboardCreator) initLogtailBytesRow() dashboard.Option {
	return dashboard.Row(
		"Logtail size",
		c.getHistogram(
			"Logtail size",
			c.getMetricWithFilter(`mo_logtail_bytes_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("bytes"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initLogtailOverviewRow() dashboard.Option {
	return dashboard.Row(
		"Logtail overview",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_logtail_append_duration_seconds_bucket`, ``),
				c.getMetricWithFilter(`mo_logtail_send_duration_seconds_bucket`, `step="total"`),
				c.getMetricWithFilter(`mo_logtail_send_duration_seconds_bucket`, `step="latency"`),
				c.getMetricWithFilter(`mo_logtail_send_duration_seconds_bucket`, `step="network"`),
				c.getMetricWithFilter(`mo_logtail_apply_duration_seconds_bucket`, `step="apply"`),
				c.getMetricWithFilter(`mo_logtail_apply_duration_seconds_bucket`, `step="apply-latency"`),
				c.getMetricWithFilter(`mo_logtail_apply_duration_seconds_bucket`, `step="apply-notify"`),
				c.getMetricWithFilter(`mo_logtail_apply_duration_seconds_bucket`, `step="apply-notify-latency"`),
				c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="cn-wait-logtail"`),
			},
			[]string{
				"append",
				"send",
				"send-latency",
				"send-network",
				"apply",
				"apply-latency",
				"apply-notify",
				"apply-notify-latency",
				"wait-commit-apply",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initLogtailLoadCheckpointRow() dashboard.Option {
	return dashboard.Row(
		"Logtail load checkpoint",
		c.getHistogram(
			"Logtail load checkpoint",
			c.getMetricWithFilter(`mo_logtail_load_checkpoint_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("s"),
			axis.Min(0)),
	)
}
