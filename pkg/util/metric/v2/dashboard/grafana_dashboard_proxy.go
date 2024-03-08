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

func (c *DashboardCreator) initProxyDashboard() error {
	folder, err := c.createFolder(moFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Proxy Metrics",
		c.withRowOptions(
			c.initProxyConnectionRow(),
			c.initProxyTransferDurationRow(),
			c.initProxyTransferCounterRow(),
			c.initProxyOthersRow(),
		)...,
	)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initProxyConnectionRow() dashboard.Option {
	return dashboard.Row(
		"Proxy Connection",
		c.withMultiGraph(
			"Connect Counter",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_connect_counter", `type="accepted"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_connect_counter", `type="current"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_connect_counter", `type="success"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_connect_counter", `type="route-fail"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_connect_counter", `type="common-fail"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_connect_counter", `type="retry"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_connect_counter", `type="select"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_connect_counter", `type="reject"`) + `[$interval]))`,
			},
			[]string{
				"accepted",
				"current",
				"success",
				"route-fail",
				"common-fail",
				"retry",
				"select",
				"reject",
			},
		),
		c.withMultiGraph(
			"Disconnect Counter",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_disconnect_counter", `type="server"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_disconnect_counter", `type="client"`) + `[$interval]))`,
			},
			[]string{
				"server",
				"client",
			},
		),
	)
}

func (c *DashboardCreator) initProxyTransferDurationRow() dashboard.Option {
	return dashboard.Row(
		"Connection transfer duration",
		c.getHistogram(
			"connection transfer duration",
			c.getMetricWithFilter(`mo_proxy_connection_transfer_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initProxyTransferCounterRow() dashboard.Option {
	return dashboard.Row(
		"Proxy Transfer",
		c.withMultiGraph(
			"Connection Transfer Counter",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_connection_transfer_counter", `type="success"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_connection_transfer_counter", `type="fail"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_proxy_connection_transfer_counter", `type="abort"`) + `[$interval]))`,
			},
			[]string{
				"success",
				"fail",
				"abort",
			},
		),
		c.withGraph(
			"Backend Server Drain Counter",
			6,
			`sum(rate(`+c.getMetricWithFilter("mo_proxy_drain_counter", ``)+`[$interval]))`,
			""),
	)
}

func (c *DashboardCreator) initProxyOthersRow() dashboard.Option {
	return dashboard.Row(
		"Proxy Others State",
		c.withGraph(
			"Available Backend Server Num",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_proxy_available_backend_server_num", "")+`[$interval]))`,
			""),
		c.withGraph(
			"Transfer Queue Size",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_proxy_transfer_queue_size", "")+`[$interval]))`,
			""),
		c.withGraph(
			"Connections Need To Transfer",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_proxy_connections_need_to_transfer", "")+`[$interval]))`,
			""),
		c.withGraph(
			"Connections In Transfer Intent State",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_proxy_connections_transfer_intent", "")+`[$interval]))`,
			""),
	)
}
