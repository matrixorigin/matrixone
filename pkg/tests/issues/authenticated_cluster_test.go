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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

const (
	authenticatedClusterHeartbeatTimeout = 30 * time.Second
	authenticatedClusterStoreTimeout     = 60 * time.Second
)

func runAuthenticatedClusterTest(t *testing.T, fn func(embed.Cluster)) {
	t.Helper()
	embed.RunBaseClusterTests(t, fn)
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
		switch svc.ServiceType() {
		case metadata.ServiceType_CN:
			cnCount++
			require.False(t, cfg.CN.Frontend.SkipCheckUser)
			require.Equal(
				t,
				authenticatedClusterHeartbeatTimeout,
				cfg.CN.HAKeeper.HeatbeatTimeout.Duration,
			)
		case metadata.ServiceType_TN:
			tnCount++
			require.NotNil(t, cfg.TN_please_use_getTNServiceConfig)
			require.Equal(
				t,
				authenticatedClusterHeartbeatTimeout,
				cfg.TN_please_use_getTNServiceConfig.HAKeeper.HeatbeatTimeout.Duration,
			)
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
			require.Less(
				t,
				authenticatedClusterHeartbeatTimeout,
				cfg.LogService.HAKeeperConfig.TNStoreTimeout.Duration,
			)
			require.Less(
				t,
				authenticatedClusterHeartbeatTimeout,
				cfg.LogService.HAKeeperConfig.CNStoreTimeout.Duration,
			)
		}
		return true
	})
	require.Equal(t, 3, cnCount)
	require.Equal(t, 1, tnCount)
	require.Equal(t, 1, logCount)
}
