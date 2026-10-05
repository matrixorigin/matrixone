// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issues

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

const (
	authenticatedClusterHeartbeatTimeout = 15 * time.Second
	authenticatedClusterBackendTimeout   = 20 * time.Second
	authenticatedClusterStoreTimeout     = 60 * time.Second
)

func runAuthenticatedClusterTest(t *testing.T, fn func(embed.Cluster)) {
	t.Helper()
	embed.RunBaseClusterTests(t, fn)
}

// Fence cross-CN assertions through the actual fixture services, not a possibly
// incomplete service-discovery view. Wait under the test deadline before calling
// the existing commit sync, whose own timeout is longer than these tests.
func syncAuthenticatedClusterCommit(t *testing.T, ctx context.Context, c embed.Cluster) {
	t.Helper()
	var services []cnservice.Service
	var committed timestamp.Timestamp
	c.ForeachServices(func(service embed.ServiceOperator) bool {
		if service.ServiceType() == metadata.ServiceType_CN {
			cn := service.RawService().(cnservice.Service)
			services = append(services, cn)
			if ts := cn.GetTxnClient().GetLatestCommitTS(); committed.Less(ts) {
				committed = ts
			}
		}
		return true
	})
	require.NotEmpty(t, services)
	require.False(t, committed.IsEmpty(), "commit fence requires a committed transaction")
	for _, cn := range services {
		_, err := cn.GetTimestampWaiter().GetTimestamp(ctx, committed)
		require.NoError(t, err, "wait for commit %s on CN %s", committed.DebugString(), cn.ID())
		cn.GetTxnClient().SyncLatestCommitTS(committed)
	}
}

func TestAuthenticatedTestsReuseBaseCluster(t *testing.T) {
	var baseCluster embed.Cluster
	var authenticatedCluster embed.Cluster

	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		baseCluster = c
	})
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		authenticatedCluster = c
	})

	require.Same(t, baseCluster, authenticatedCluster)

	var cnCount, tnCount, logCount int
	authenticatedCluster.ForeachServices(func(svc embed.ServiceOperator) bool {
		cfg := svc.GetServiceConfig()
		require.Equal(t, authenticatedClusterBackendTimeout,
			cfg.HAKeeperClient.BackendReadTimeout.Duration)
		switch svc.ServiceType() {
		case metadata.ServiceType_CN:
			cnCount++
			require.Equal(t, fmt.Sprintf("127.0.0.1:%d", cfg.CN.Frontend.Port), cfg.CN.SQLAddress)
			require.False(t, cfg.CN.Frontend.SkipCheckUser)
			require.Equal(t, authenticatedClusterHeartbeatTimeout,
				cfg.CN.HAKeeper.HeatbeatTimeout.Duration)
			require.Less(t, cfg.CN.HAKeeper.HeatbeatTimeout.Duration,
				cfg.HAKeeperClient.BackendReadTimeout.Duration)
		case metadata.ServiceType_TN:
			tnCount++
			require.NotNil(t, cfg.TN_please_use_getTNServiceConfig)
			require.Equal(t, authenticatedClusterHeartbeatTimeout,
				cfg.TN_please_use_getTNServiceConfig.HAKeeper.HeatbeatTimeout.Duration)
			require.Less(t,
				cfg.TN_please_use_getTNServiceConfig.HAKeeper.HeatbeatTimeout.Duration,
				cfg.HAKeeperClient.BackendReadTimeout.Duration)
		case metadata.ServiceType_LOG:
			logCount++
			require.Equal(
				t,
				authenticatedClusterStoreTimeout,
				cfg.LogService.HAKeeperConfig.TNStoreTimeout.Duration,
			)
			require.Equal(
				t,
				authenticatedClusterStoreTimeout,
				cfg.LogService.HAKeeperConfig.CNStoreTimeout.Duration,
			)
			require.Less(t, cfg.HAKeeperClient.BackendReadTimeout.Duration,
				cfg.LogService.HAKeeperConfig.TNStoreTimeout.Duration)
		}
		return true
	})
	require.Equal(t, 3, cnCount)
	require.Equal(t, 1, tnCount)
	require.Equal(t, 1, logCount)
}
