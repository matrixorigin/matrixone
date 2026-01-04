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
	folder, err := c.createFolder(c.folderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"RPC Metrics",
		c.withRowOptions(
			c.initRPCKeyMetricsRow(),
			c.initRPCRequestMetricsRow(),
			c.initRPCConnectionMetricsRow(),
			c.initRPCBackendHealthRow(),
			c.initRPCCircuitBreakerRow(),
			c.initRPCNetworkMetricsRow(),
			c.initRPCPerformanceMetricsRow(),
			c.initRPCGCRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

// initRPCKeyMetricsRow shows the most critical metrics at a glance (Four Golden Signals)
func (c *DashboardCreator) initRPCKeyMetricsRow() dashboard.Option {
	return dashboard.Row(
		"Key Metrics (Four Golden Signals)",
		c.withGraph(
			"Active Clients by Name",
			2,
			`sum(`+c.getMetricWithFilter("mo_rpc_client_active", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0)),

		c.withGraph(
			"Request Rate (QPS)",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_message_total", `type="send"`)+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("req/s"),
			axis.Min(0)),

		c.withGraph(
			"P95 Request Duration",
			2,
			`histogram_quantile(0.95, sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_done_duration_seconds_bucket", "")+`[$interval])) by (le, name))`,
			"{{ name }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"Connection Error Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_connect_total", `type="failed"`)+`[$interval])) by (name) / sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_connect_total", `type="total"`)+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("percent"),
			axis.Min(0),
			axis.Max(1)),

		c.withGraph(
			"Active Connections",
			2,
			`sum(`+c.getMetricWithFilter("mo_rpc_backend_pool_size", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0)),

		c.withGraph(
			"Connection Health",
			2,
			`(1 - sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_connect_total", `type="failed"`)+`[$interval])) by (name) / sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_connect_total", `type="total"`)+`[$interval])) by (name)) * 100`,
			"{{ name }}",
			axis.Unit("percent"),
			axis.Min(0),
			axis.Max(100)),
	)
}

// initRPCRequestMetricsRow shows request-level metrics (RED Method: Rate, Errors, Duration)
func (c *DashboardCreator) initRPCRequestMetricsRow() dashboard.Option {
	return dashboard.Row(
		"Request Metrics (RED Method)",
		c.withGraph(
			"Request Rate by Name",
			4,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_message_total", `type="send"`)+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("req/s"),
			axis.Min(0)),

		c.getHistogramWithExtraBy(
			"Request Duration (P50/P80/P90/P99)",
			c.getMetricWithFilter(`mo_rpc_backend_done_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			8,
			"name",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

// initRPCConnectionMetricsRow shows connection lifecycle metrics
func (c *DashboardCreator) initRPCConnectionMetricsRow() dashboard.Option {
	return dashboard.Row(
		"Connection Metrics",
		c.withGraph(
			"Connection Pool Size",
			3,
			`sum(`+c.getMetricWithFilter("mo_rpc_backend_pool_size", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0)),

		c.withGraph(
			"Connection Create Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_create_total", "")+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("conn/s"),
			axis.Min(0)),

		c.withGraph(
			"Connection Close Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_close_total", "")+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("conn/s"),
			axis.Min(0)),

		c.withGraph(
			"Connection Net Growth",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_create_total", "")+`[$interval])) by (name) - sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_close_total", "")+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("conn/s")),

		c.withGraph(
			"Connection Error Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_connect_total", `type="failed"`)+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("errors/s"),
			axis.Min(0)),

		c.withGraph(
			"Active Requests per Backend",
			2,
			`sum(`+c.getMetricWithFilter("mo_rpc_backend_active_requests", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0)),

		c.withGraph(
			"Backend Busy Status",
			2,
			`sum(`+c.getMetricWithFilter("mo_rpc_backend_busy", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0),
			axis.Max(1)),
	)
}

// initRPCBackendHealthRow shows backend health and failure metrics
func (c *DashboardCreator) initRPCBackendHealthRow() dashboard.Option {
	return dashboard.Row(
		"Backend Health & Failures",
		c.withGraph(
			"Auto-Create Timeout Rate",
			4,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_auto_create_timeout_total", "")+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("timeouts/s"),
			axis.Min(0)),

		c.withGraph(
			"Backend Unavailable Rate",
			4,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_unavailable_total", "")+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("errors/s"),
			axis.Min(0)),

		c.withMultiGraph(
			"Backend Creation vs Failures",
			4,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_rpc_backend_create_total", "") + `[$interval])) by (name)`,
				`sum(rate(` + c.getMetricWithFilter("mo_rpc_backend_connect_total", `type="failed"`) + `[$interval])) by (name)`,
			},
			[]string{"{{ name }} - create", "{{ name }} - failed"}),
	)
}

// initRPCCircuitBreakerRow shows circuit breaker metrics
func (c *DashboardCreator) initRPCCircuitBreakerRow() dashboard.Option {
	return dashboard.Row(
		"Circuit Breaker",
		c.withGraph(
			"Circuit Breaker State",
			4,
			`sum(`+c.getMetricWithFilter("mo_rpc_circuit_breaker_state", "")+`) by (name, backend)`,
			"{{ name }}/{{ backend }}",
			axis.Min(0),
			axis.Max(2)),

		c.withGraph(
			"Circuit Breaker Trip Rate",
			4,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_circuit_breaker_trips_total", "")+`[$interval])) by (name, backend)`,
			"{{ name }}/{{ backend }}",
			axis.Unit("trips/s"),
			axis.Min(0)),

		c.withGraph(
			"Open Circuit Breakers Count",
			4,
			`count(`+c.getMetricWithFilter("mo_rpc_circuit_breaker_state", "")+` == 2) by (name)`,
			"{{ name }}",
			axis.Min(0)),
	)
}

// initRPCNetworkMetricsRow shows network-level metrics
func (c *DashboardCreator) initRPCNetworkMetricsRow() dashboard.Option {
	return dashboard.Row(
		"Network Metrics",
		c.withGraph(
			"Network Input Throughput",
			4,
			`sum(irate(`+c.getMetricWithFilter("mo_rpc_network_bytes_total", `type="input"`)+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"Network Output Throughput",
			4,
			`sum(irate(`+c.getMetricWithFilter("mo_rpc_network_bytes_total", `type="output"`)+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"Message Send Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_message_total", `type="send"`)+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("msg/s"),
			axis.Min(0)),

		c.withGraph(
			"Message Receive Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_message_total", `type="receive"`)+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("msg/s"),
			axis.Min(0)),
	)
}

// initRPCPerformanceMetricsRow shows performance-related metrics
func (c *DashboardCreator) initRPCPerformanceMetricsRow() dashboard.Option {
	return dashboard.Row(
		"Performance Metrics",
		c.getHistogramWithExtraBy(
			"Request Duration (P50/P80/P90/P99)",
			c.getMetricWithFilter(`mo_rpc_backend_done_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			6,
			"name",
			axis.Unit("s"),
			axis.Min(0)),

		c.getHistogramWithExtraBy(
			"Connection Duration",
			c.getMetricWithFilter(`mo_rpc_backend_connect_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			3,
			"name",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"Sending Queue Size",
			3,
			`sum(`+c.getMetricWithFilter("mo_rpc_sending_queue_size", ``)+`) by (name, side)`,
			"{{ name }}({{ side }})",
			axis.Min(0)),

		c.withGraph(
			"Write Queue Length",
			3,
			`sum(`+c.getMetricWithFilter("mo_rpc_backend_write_queue_length", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0)),

		c.withGraph(
			"Active Requests",
			3,
			`sum(`+c.getMetricWithFilter("mo_rpc_backend_active_requests", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0)),

		c.withGraph(
			"Backend Busy",
			3,
			`sum(`+c.getMetricWithFilter("mo_rpc_backend_busy", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0),
			axis.Max(1)),
	)
}

// initRPCGCRow shows GC Manager internal metrics (for debugging, can be collapsed)
func (c *DashboardCreator) initRPCGCRow() dashboard.Option {
	return dashboard.Row(
		"GC Manager (Internal)",
		c.withGraph(
			"GC Channel Drop Rate",
			4,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_gc_channel_drop_total", "")+`[$interval])) by (type)`,
			"{{ type }}",
			axis.Unit("drops/s"),
			axis.Min(0)),

		c.withGraph(
			"GC Channel Queue Length",
			4,
			`sum(`+c.getMetricWithFilter("mo_rpc_gc_channel_queue_length", "")+`) by (type)`,
			"{{ type }}",
			axis.Min(0)),

		c.withGraph(
			"Registered Clients",
			2,
			`sum(`+c.getMetricWithFilter("mo_rpc_gc_registered_clients_total", "")+`)`,
			"Registered Clients",
			axis.Min(0)),

		c.withGraph(
			"Idle Backends Cleaned Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_gc_idle_backends_cleaned_total", "")+`[$interval]))`,
			"Idle Backends Cleaned",
			axis.Unit("ops/s"),
			axis.Min(0)),
	)
}
