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
	"strings"
	"testing"

	grabanaDashboard "github.com/K-Phoen/grabana/dashboard"
	"github.com/stretchr/testify/require"
)

// TestCreateCloudDashboard creates a dashboard for cloud env.
func TestCreateCloudDashboard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	c := NewCloudDashboardCreator("http://127.0.0.1", "admin", "admin",
		"Prometheus", defaultMoFolderName)
	require.NoError(t, c.Create())
}

// TestCreateLocalDashboard creates a dashboard for local env.
func TestCreateLocalDashboard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	c := NewLocalDashboardCreator("http://127.0.0.1", "admin", "admin",
		localFolderName)
	require.NoError(t, c.Create())
}

// TestCreateK8SDashboard creates a dashboard for k8s env(EKS, TKE).
func TestCreateK8SDashboard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	c := NewK8SDashboardCreator("http://127.0.0.1", "admin", "admin",
		"Prometheus", defaultMoFolderName)
	require.NoError(t, c.Create())
}

// TestCreateCloudCtrlPlaneDashboard creates a dashboard for cloud env. (used in control-plane)
// diff TestCreateCloudDashboard, which is used in data-plane (unit).
func TestCreateCloudCtrlPlaneDashboard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	c := NewCloudCtrlPlaneDashboardCreator("http://127.0.0.1", "admin", "admin",
		"Prometheus", defaultMoFolderName)
	require.NoError(t, c.Create())
}

func Test_InitFrontendSQLLength(t *testing.T) {
	c := NewLocalDashboardCreator("http://127.0.0.1", "admin", "admin",
		localFolderName)
	c.initFrontendSQLLength()
}

func TestProxyConnectionDashboardSupportsRollingUpgrade(t *testing.T) {
	c := NewLocalDashboardCreator("http://127.0.0.1", "admin", "admin",
		localFolderName)
	build, err := grabanaDashboard.New("Proxy Metrics", c.initProxyConnectionRow())
	require.NoError(t, err)

	data, err := build.MarshalJSON()
	require.NoError(t, err)
	require.Contains(t, string(data), `type=~\"current|closed\"`)
}

func TestRPCDashboardCoversTroubleshootingSignalsWithCorrectSemantics(t *testing.T) {
	c := NewLocalDashboardCreator("http://127.0.0.1", "admin", "admin", localFolderName)
	build, err := grabanaDashboard.New("RPC Metrics", c.rpcDashboardRows()...)
	require.NoError(t, err)

	data, err := build.MarshalJSON()
	require.NoError(t, err)
	content := string(data)

	metrics := []string{
		"mo_rpc_client_request_started_total",
		"mo_rpc_client_request_completed_total",
		"mo_rpc_client_request_duration_seconds_bucket",
		"mo_rpc_backend_active_requests",
		"mo_rpc_sending_queue_size",
		"mo_rpc_write_latency_duration_seconds_bucket",
		"mo_rpc_write_duration_seconds_bucket",
		"mo_rpc_backend_busy",
		"mo_rpc_sending_batch_size",
		"mo_rpc_backend_error_total",
		"mo_lockservice_remote_rpc_error_total",
		"mo_rpc_backend_done_duration_seconds_bucket",
		"mo_rpc_message_total",
		"mo_rpc_network_bytes_total",
		"mo_rpc_client_active",
		"mo_rpc_client_create_total",
		"mo_rpc_backend_pool_size",
		"mo_rpc_backend_create_total",
		"mo_rpc_backend_close_total",
		"mo_rpc_backend_connect_total",
		"mo_rpc_backend_connect_duration_seconds_bucket",
		"mo_rpc_backend_auto_create_timeout_total",
		"mo_rpc_backend_auto_create_timeout_event_total",
		"mo_rpc_backend_unavailable_total",
		"mo_rpc_circuit_breaker_state",
		"mo_rpc_circuit_breaker_trips_total",
		"mo_rpc_server_session_size",
		"mo_rpc_server_stream_state_size",
		"mo_rpc_gc_channel_drop_total",
		"mo_rpc_gc_channel_queue_length",
		"mo_rpc_gc_registered_clients_total",
		"mo_rpc_gc_idle_backends_cleaned_total",
		"mo_rpc_gc_inactive_processed_total",
		"mo_rpc_gc_create_processed_total",
	}
	for _, metric := range metrics {
		require.Containsf(t, content, metric, "dashboard omitted troubleshooting signal %s", metric)
	}

	require.Contains(t, content, "Unary Duration (Backend Admission to Terminal)")
	require.Contains(t, content, "Response Dispatch Overhead (Not RTT)")
	require.NotContains(t, content, "Request Rate (QPS)",
		"message traffic must not be presented as unary request QPS")
	require.NotContains(t, content, "mo_rpc_backend_write_queue_length",
		"deprecated queue alias must not duplicate the canonical queue panel")
	require.Contains(t, content, "clamp_min(",
		"ratio panels must remain finite when there is no traffic")
	require.NotContains(t, content, "{{ name, side }}",
		"multi-label histogram legends must render each label independently")
	require.True(t, strings.Contains(content, "Messages, Not Requests"))
}
