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

func (c *DashboardCreator) initRPCDashboard() error {
	folder, err := c.createFolder(moFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"RPC Metrics",
		c.withRowOptions(
			c.initRPCOverviewRow(),
			c.initRPCConnectionRow(),
			c.initRPCConnectDurationRow(),
			c.initRPCWriteDurationRow(),
			c.initRPCRequestDoneDurationRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initRPCOverviewRow() dashboard.Option {
	return dashboard.Row(
		"RPC overview",
		c.withGraph(
			"RPC Network Input",
			6,
			`sum(irate(`+c.getMetricWithFilter("mo_rpc_network_bytes_total", `type="input"`)+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"RPC Network Output",
			6,
			`sum(irate(`+c.getMetricWithFilter("mo_rpc_network_bytes_total", `type="output"`)+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"RPC Client Create",
			6,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_client_create_total", "")+`[$interval])) by (name)`,
			"{{ name }}"),

		c.withGraph(
			"Connection pool",
			6,
			`sum(`+c.getMetricWithFilter("mo_rpc_backend_pool_size", ``)+`) by (name)`,
			"{{ name }}"),

		c.withGraph(
			"Sending queue",
			4,
			`sum(`+c.getMetricWithFilter("mo_rpc_sending_queue_size", ``)+`) by (name, side)`,
			"{{ name }}({{ side }})"),

		c.withGraph(
			"Write Batch Size",
			4,
			`sum(`+c.getMetricWithFilter("mo_rpc_sending_batch_size", ``)+`) by (name)`,
			"{{ name }}"),

		c.withGraph(
			"Server sessions",
			4,
			`sum(`+c.getMetricWithFilter("mo_rpc_server_session_size", ``)+`) by (name)`,
			"{{ name }}"),
	)
}

func (c *DashboardCreator) initRPCConnectionRow() dashboard.Option {
	return dashboard.Row(
		"Connection Status",
		c.withGraph(
			"Create",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_create_total", "")+`[$interval])) by (name)`,
			"{{ name }}"),

		c.withGraph(
			"Close",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_close_total", "")+`[$interval])) by (name)`,
			"{{ name }}"),

		c.withGraph(
			"Reconnect Total",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_connect_total", `type="total"`)+`[$interval])) by (name)`,
			"{{ name }}"),

		c.withGraph(
			"Reconnect Failed",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_connect_total", `type="failed"`)+`[$interval])) by (name)`,
			"{{ name }}"),
	)
}

func (c *DashboardCreator) initRPCConnectDurationRow() dashboard.Option {
	return dashboard.Row(
		"RPC connection duration",
		c.getHistogramWithExtraBy(
			"Connect duration",
			c.getMetricWithFilter(`mo_rpc_backend_connect_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			"name",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initRPCWriteDurationRow() dashboard.Option {
	return dashboard.Row(
		"RPC write duration",
		c.getHistogramWithExtraBy(
			"Client-side Write To Network Duration",
			c.getMetricWithFilter(`mo_rpc_write_duration_seconds_bucket`, `side="client"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			3,
			"name",
			axis.Unit("s"),
			axis.Min(0)),

		c.getHistogramWithExtraBy(
			"Client-side Write Latency Duration",
			c.getMetricWithFilter(`mo_rpc_write_latency_duration_seconds_bucket`, `side="client"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			3,
			"name",
			axis.Unit("s"),
			axis.Min(0)),

		c.getHistogramWithExtraBy(
			"Server-side Write To Network Duration",
			c.getMetricWithFilter(`mo_rpc_write_duration_seconds_bucket`, `side="server"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			3,
			"name",
			axis.Unit("s"),
			axis.Min(0)),

		c.getHistogramWithExtraBy(
			"Server-side Write Latency Duration",
			c.getMetricWithFilter(`mo_rpc_write_latency_duration_seconds_bucket`, `side="server"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			3,
			"name",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initRPCRequestDoneDurationRow() dashboard.Option {
	return dashboard.Row(
		"Request done Duration",
		c.getHistogramWithExtraBy(
			"Request done Duration",
			c.getMetricWithFilter(`mo_rpc_backend_done_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			"name",
			axis.Unit("s"),
			axis.Min(0)),
	)
}
