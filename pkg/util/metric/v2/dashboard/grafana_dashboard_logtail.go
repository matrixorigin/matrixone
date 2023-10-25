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
	"github.com/K-Phoen/grabana/variable/interval"
)

func (c *DashboardCreator) initLogTailDashboard() error {
	folder, err := c.createFolder(logtailFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Logtail Status",
		dashboard.AutoRefresh("5s"),
		dashboard.VariableAsInterval(
			"interval",
			interval.Values([]string{"30s", "1m", "5m", "10m", "30m", "1h", "6h", "12h"}),
		),
		c.initLogtailQueueRow(),
		c.initLogtailLoadCheckpointRow(),
		c.initLogtailBytesRow(),
		c.initLogtailAppendRow(),
		c.initLogtailApplyRow(),
		c.initLogtailSendRow(),
		c.initLogtailSendLatencyRow(),
		c.initLogtailSendNetworkRow(),
		c.initLogtailCollectRow(),
		c.initLogtailSubscriptionRow(),
	)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initLogtailCollectRow() dashboard.Option {
	return dashboard.Row(
		"Logtail collect duration",
		c.getBytesHistogram(
			`mo_logtail_collect_duration_seconds_bucket`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initLogtailSubscriptionRow() dashboard.Option {
	return dashboard.Row(
		"logtail subscription the tn have received",
		c.withGraph(
			"logtail subscription average increase",
			6,
			`rate(mo_logtail_subscription_request_total[$interval])`,
			""),
		c.withGraph(
			"logtail subscription average increase, sensitive",
			6,
			`irate(mo_logtail_subscription_request_total[$interval])`,
			""),
	)
}

func (c *DashboardCreator) initLogtailQueueRow() dashboard.Option {
	return dashboard.Row(
		"Logtail Queue Status",
		c.withGraph(
			"Sending Queue",
			4,
			`sum(mo_logtail_queue_size{type="send"})`,
			""),
		c.withGraph(
			"Receiving Queue",
			4,
			`sum(mo_logtail_queue_size{type="receive"})`,
			""),

		c.withGraph(
			"Checkpoint logtail",
			4,
			"sum(mo_logtail_load_checkpoint_total) by (instance)",
			"{{ instance }}"),
	)
}

func (c *DashboardCreator) initLogtailBytesRow() dashboard.Option {
	return dashboard.Row(
		"Logtail size",
		c.getBytesHistogram(
			`mo_logtail_bytes_bucket`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initLogtailAppendRow() dashboard.Option {
	return dashboard.Row(
		"Logtail append",
		c.getHistogram(
			`mo_logtail_append_duration_seconds_bucket`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initLogtailApplyRow() dashboard.Option {
	return dashboard.Row(
		"Logtail apply",
		c.getHistogram(
			`mo_logtail_apply_duration_seconds_bucket`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initLogtailSendRow() dashboard.Option {
	return dashboard.Row(
		"Logtail send total",
		c.getHistogram(
			`mo_logtail_send_duration_seconds_bucket{step="total"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initLogtailSendLatencyRow() dashboard.Option {
	return dashboard.Row(
		"Logtail send latency",
		c.getHistogram(
			`mo_logtail_send_duration_seconds_bucket{step="latency"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initLogtailSendNetworkRow() dashboard.Option {
	return dashboard.Row(
		"Logtail send network",
		c.getHistogram(
			`mo_logtail_send_duration_seconds_bucket{step="network"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initLogtailLoadCheckpointRow() dashboard.Option {
	return dashboard.Row(
		"Logtail load checkpoint",
		c.getHistogram(
			`mo_logtail_load_checkpoint_duration_seconds_bucket`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}
