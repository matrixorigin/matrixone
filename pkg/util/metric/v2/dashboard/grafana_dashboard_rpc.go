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

	build, err := dashboard.New("RPC Metrics", c.withRowOptions(c.rpcDashboardRows()...)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

// rpcDashboardRows is intentionally organized by troubleshooting question,
// rather than by metric type. Except for the request-health overview, keep each
// signal in one primary location so an operator can move from user impact to
// the responsible subsystem without comparing subtly different semantics.
func (c *DashboardCreator) rpcDashboardRows() []dashboard.Option {
	return []dashboard.Option{
		c.initRPCKeyRequestHealthRow(),
		c.initRPCRequestLifecycleRow(),
		c.initRPCBackpressureRow(),
		c.initRPCTransportErrorsRow(),
		c.initRPCTrafficRow(),
		c.initRPCClientPoolRow(),
		c.initRPCConnectionRecoveryRow(),
		c.initRPCBackendAvailabilityRow(),
		c.initRPCCircuitBreakerRow(),
		c.initRPCServerStreamRow(),
		c.initRPCGCRow(),
	}
}

func (c *DashboardCreator) initRPCKeyRequestHealthRow() dashboard.Option {
	return dashboard.Row(
		"Request Health",
		c.withGraph(
			"Client Request Start Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_client_request_started_total", "")+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("req/s"),
			axis.Min(0)),
		c.withGraph(
			"Client Request Completion Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_client_request_completed_total", "")+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("req/s"),
			axis.Min(0)),
		c.withGraph(
			"P95 Unary Duration (Backend Admission to Terminal)",
			3,
			`histogram_quantile(0.95, sum(rate(`+c.getMetricWithFilter("mo_rpc_client_request_duration_seconds_bucket", "")+`[$interval])) by (le, name))`,
			"{{ name }}",
			axis.Unit("s"),
			axis.Min(0)),
		c.withGraph(
			"Non-Success Request Ratio",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_client_request_completed_total", `outcome!="success"`)+`[$interval])) by (name) / clamp_min(sum(rate(`+c.getMetricWithFilter("mo_rpc_client_request_completed_total", "")+`[$interval])) by (name), 0.000000001)`,
			"{{ name }}",
			axis.Unit("percentunit"),
			axis.Min(0),
			axis.Max(1)),
		c.withGraph(
			"Active Backend Futures (Includes Internal)",
			2,
			`sum(`+c.getMetricWithFilter("mo_rpc_backend_active_requests", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initRPCRequestLifecycleRow() dashboard.Option {
	return dashboard.Row(
		"Request Lifecycle (RED)",
		c.withMultiGraph(
			"Started vs Completed",
			4,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_rpc_client_request_started_total", "") + `[$interval])) by (name)`,
				`sum(rate(` + c.getMetricWithFilter("mo_rpc_client_request_completed_total", "") + `[$interval])) by (name)`,
			},
			[]string{"{{ name }} started", "{{ name }} completed"},
			axis.Unit("req/s"),
			axis.Min(0)),
		c.withGraph(
			"Terminal Outcomes",
			4,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_client_request_completed_total", "")+`[$interval])) by (name, outcome)`,
			"{{ name }} {{ outcome }}",
			axis.Unit("req/s"),
			axis.Min(0)),
		c.getHistogramWithExtraBy(
			"Unary Duration, Admission to Terminal (P50/P80/P90/P99)",
			c.getMetricWithFilter("mo_rpc_client_request_duration_seconds_bucket", ""),
			[]float64{0.50, 0.80, 0.90, 0.99},
			4,
			"name",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initRPCBackpressureRow() dashboard.Option {
	return dashboard.Row(
		"Backpressure & Write Path",
		c.withGraph(
			"Queued Outbound Messages",
			3,
			`sum(`+c.getMetricWithFilter("mo_rpc_sending_queue_size", "")+`) by (name, side)`,
			"{{ name }} {{ side }}",
			axis.Min(0)),
		c.getHistogramWithExtraBy(
			"Queue Wait Duration",
			c.getMetricWithFilter("mo_rpc_write_latency_duration_seconds_bucket", ""),
			[]float64{0.50, 0.90, 0.99},
			3,
			"name, side",
			axis.Unit("s"),
			axis.Min(0)),
		c.getHistogramWithExtraBy(
			"Batch Encode & Socket Flush Duration",
			c.getMetricWithFilter("mo_rpc_write_duration_seconds_bucket", ""),
			[]float64{0.50, 0.90, 0.99},
			3,
			"name, side",
			axis.Unit("s"),
			axis.Min(0)),
		c.withGraph(
			"Busy Client Backend Count",
			3,
			`sum(`+c.getMetricWithFilter("mo_rpc_backend_busy", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0)),
		c.withGraph(
			"Most Recent Sending Batch Size",
			3,
			`sum(`+c.getMetricWithFilter("mo_rpc_sending_batch_size", "")+`) by (name, side)`,
			"{{ name }} {{ side }}",
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initRPCTransportErrorsRow() dashboard.Option {
	return dashboard.Row(
		"Transport Errors & Response Dispatch",
		c.withGraph(
			"MORPC Backend Errors",
			4,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_error_total", "")+`[$interval])) by (name, phase, error_type)`,
			"{{ name }} {{ phase }} {{ error_type }}",
			axis.Unit("errors/s"),
			axis.Min(0)),
		c.withGraph(
			"Lockservice Remote RPC Errors",
			4,
			`sum(rate(`+c.getMetricWithFilter("mo_lockservice_remote_rpc_error_total", "")+`[$interval])) by (method, error_type)`,
			"{{ method }} {{ error_type }}",
			axis.Unit("errors/s"),
			axis.Min(0)),
		c.getHistogramWithExtraBy(
			"Response Dispatch Overhead (Not RTT)",
			c.getMetricWithFilter("mo_rpc_backend_done_duration_seconds_bucket", ""),
			[]float64{0.50, 0.90, 0.99},
			4,
			"name",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initRPCTrafficRow() dashboard.Option {
	return dashboard.Row(
		"Transport Traffic (Messages, Not Requests)",
		c.withMultiGraph(
			"Message Send vs Receive Rate",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_rpc_message_total", `type="send"`) + `[$interval])) by (name)`,
				`sum(rate(` + c.getMetricWithFilter("mo_rpc_message_total", `type="receive"`) + `[$interval])) by (name)`,
			},
			[]string{"{{ name }} send", "{{ name }} receive"},
			axis.Unit("msg/s"),
			axis.Min(0)),
		c.withMultiGraph(
			"Network Throughput",
			6,
			[]string{
				`sum(irate(` + c.getMetricWithFilter("mo_rpc_network_bytes_total", `type="input"`) + `[$interval])) by (` + c.by + `)`,
				`sum(irate(` + c.getMetricWithFilter("mo_rpc_network_bytes_total", `type="output"`) + `[$interval])) by (` + c.by + `)`,
			},
			[]string{"{{ " + c.by + " }} input", "{{ " + c.by + " }} output"},
			axis.Unit("Bps"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initRPCClientPoolRow() dashboard.Option {
	return dashboard.Row(
		"Clients & Backend Pool",
		c.withGraph(
			"Active RPC Clients",
			3,
			`sum(`+c.getMetricWithFilter("mo_rpc_client_active", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0)),
		c.withGraph(
			"Client Creation Rate",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_client_create_total", "")+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("clients/s"),
			axis.Min(0)),
		c.withGraph(
			"Backend Pool Size",
			3,
			`sum(`+c.getMetricWithFilter("mo_rpc_backend_pool_size", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0)),
		c.withMultiGraph(
			"Backend Create vs Close Rate",
			3,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_rpc_backend_create_total", "") + `[$interval])) by (name)`,
				`sum(rate(` + c.getMetricWithFilter("mo_rpc_backend_close_total", "") + `[$interval])) by (name)`,
			},
			[]string{"{{ name }} create", "{{ name }} close"},
			axis.Unit("conn/s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initRPCConnectionRecoveryRow() dashboard.Option {
	return dashboard.Row(
		"Connection Recovery",
		c.withGraph(
			"Connect Attempt & Failure Rate",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_connect_total", "")+`[$interval])) by (name, type)`,
			"{{ name }} {{ type }}",
			axis.Unit("ops/s"),
			axis.Min(0)),
		c.withGraph(
			"Connect Failure Ratio",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_connect_total", `type="failed"`)+`[$interval])) by (name) / clamp_min(sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_connect_total", `type="total"`)+`[$interval])) by (name), 0.000000001)`,
			"{{ name }}",
			axis.Unit("percentunit"),
			axis.Min(0),
			axis.Max(1)),
		c.getHistogramWithExtraBy(
			"Connection Recovery Duration (Includes Retry/Backoff)",
			c.getMetricWithFilter("mo_rpc_backend_connect_duration_seconds_bucket", ""),
			[]float64{0.50, 0.90, 0.99},
			3,
			"name",
			axis.Unit("s"),
			axis.Min(0)),
		c.withGraph(
			"Backend Net Growth",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_create_total", "")+`[$interval])) by (name) - sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_close_total", "")+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("conn/s")),
	)
}

func (c *DashboardCreator) initRPCBackendAvailabilityRow() dashboard.Option {
	return dashboard.Row(
		"Backend Acquisition & Availability",
		c.withMultiGraph(
			"Auto-Create Timeout Impact vs Root Events",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_rpc_backend_auto_create_timeout_total", "") + `[$interval])) by (name)`,
				`sum(rate(` + c.getMetricWithFilter("mo_rpc_backend_auto_create_timeout_event_total", "") + `[$interval])) by (name)`,
			},
			[]string{"{{ name }} affected requests", "{{ name }} create states"},
			axis.Unit("timeouts/s"),
			axis.Min(0)),
		c.withGraph(
			"Backend Unavailable Rate",
			6,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_backend_unavailable_total", "")+`[$interval])) by (name)`,
			"{{ name }}",
			axis.Unit("errors/s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initRPCCircuitBreakerRow() dashboard.Option {
	return dashboard.Row(
		"Circuit Breaker",
		c.withGraph(
			"Circuit Breaker State (0 Closed, 1 Half-Open, 2 Open)",
			4,
			`max(`+c.getMetricWithFilter("mo_rpc_circuit_breaker_state", "")+`) by (name, backend)`,
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
			"Open Circuit Breaker Count",
			4,
			`count(`+c.getMetricWithFilter("mo_rpc_circuit_breaker_state", "")+` == 2) by (name)`,
			"{{ name }}",
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initRPCServerStreamRow() dashboard.Option {
	return dashboard.Row(
		"Server Sessions & Streams",
		c.withGraph(
			"Server Session Count",
			4,
			`sum(`+c.getMetricWithFilter("mo_rpc_server_session_size", "")+`) by (name)`,
			"{{ name }}",
			axis.Min(0)),
		c.withGraph(
			"Server Stream State Entries",
			8,
			`sum(`+c.getMetricWithFilter("mo_rpc_server_stream_state_size", "")+`) by (name, type)`,
			"{{ name }} {{ type }}",
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initRPCGCRow() dashboard.Option {
	return dashboard.Row(
		"GC Manager (Internal)",
		c.withGraph(
			"GC Channel Drop Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_gc_channel_drop_total", "")+`[$interval])) by (type)`,
			"{{ type }}",
			axis.Unit("drops/s"),
			axis.Min(0)),
		c.withGraph(
			"GC Channel Queue Length",
			2,
			`sum(`+c.getMetricWithFilter("mo_rpc_gc_channel_queue_length", "")+`) by (type)`,
			"{{ type }}",
			axis.Min(0)),
		c.withGraph(
			"Registered Clients",
			2,
			`sum(`+c.getMetricWithFilter("mo_rpc_gc_registered_clients_total", "")+`)`,
			"registered clients",
			axis.Min(0)),
		c.withGraph(
			"Idle Backends Cleaned Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_gc_idle_backends_cleaned_total", "")+`[$interval]))`,
			"idle cleaned",
			axis.Unit("ops/s"),
			axis.Min(0)),
		c.withGraph(
			"Inactive Cleanup Process Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_gc_inactive_processed_total", "")+`[$interval]))`,
			"inactive processed",
			axis.Unit("ops/s"),
			axis.Min(0)),
		c.withGraph(
			"Backend Create Process Rate",
			2,
			`sum(rate(`+c.getMetricWithFilter("mo_rpc_gc_create_processed_total", "")+`[$interval]))`,
			"create processed",
			axis.Unit("ops/s"),
			axis.Min(0)),
	)
}
