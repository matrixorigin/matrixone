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
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/stretchr/testify/require"
)

const authenticatedClusterHeartbeatTimeout = 30 * time.Second

var authenticatedClusterSuite struct {
	once    sync.Once
	running sync.Mutex
	cluster embed.Cluster
	err     error
}

func runAuthenticatedClusterTest(t *testing.T, fn func(embed.Cluster)) {
	t.Helper()

	authenticatedClusterSuite.running.Lock()
	defer authenticatedClusterSuite.running.Unlock()

	authenticatedClusterSuite.once.Do(func() {
		authenticatedClusterSuite.cluster, authenticatedClusterSuite.err = embed.NewCluster(
			embed.WithCNCount(1),
			embed.WithTesting(),
			embed.WithPreStart(func(svc embed.ServiceOperator) {
				svc.Adjust(func(cfg *embed.ServiceConfig) {
					adjustAuthenticatedClusterServiceConfig(svc.ServiceType(), cfg)
				})
			}),
		)
		if authenticatedClusterSuite.err != nil {
			return
		}
		authenticatedClusterSuite.err = authenticatedClusterSuite.cluster.Start()
	})

	require.NoError(t, authenticatedClusterSuite.err)
	fn(authenticatedClusterSuite.cluster)
}

func adjustAuthenticatedClusterServiceConfig(serviceType metadata.ServiceType, cfg *embed.ServiceConfig) {
	switch serviceType {
	case metadata.ServiceType_CN:
		cfg.CN.LockService.MaxFixedSliceSize = 10001
		cfg.CN.LockService.MaxLockRowCount = 10000
		cfg.CN.Frontend.SkipCheckUser = false
		cfg.CN.HAKeeper.HeatbeatTimeout.Duration = authenticatedClusterHeartbeatTimeout
	case metadata.ServiceType_TN:
		if cfg.TN_please_use_getTNServiceConfig != nil {
			cfg.TN_please_use_getTNServiceConfig.HAKeeper.HeatbeatTimeout.Duration =
				authenticatedClusterHeartbeatTimeout
		}
		if cfg.TNCompatible != nil {
			cfg.TNCompatible.HAKeeper.HeatbeatTimeout.Duration = authenticatedClusterHeartbeatTimeout
		}
	}
}

func TestAdjustAuthenticatedClusterServiceConfig(t *testing.T) {
	t.Run("cn", func(t *testing.T) {
		cfg := &embed.ServiceConfig{}

		adjustAuthenticatedClusterServiceConfig(metadata.ServiceType_CN, cfg)

		require.EqualValues(t, 10001, cfg.CN.LockService.MaxFixedSliceSize)
		require.EqualValues(t, 10000, cfg.CN.LockService.MaxLockRowCount)
		require.False(t, cfg.CN.Frontend.SkipCheckUser)
		require.Equal(t, authenticatedClusterHeartbeatTimeout, cfg.CN.HAKeeper.HeatbeatTimeout.Duration)
	})

	t.Run("tn", func(t *testing.T) {
		cfg := &embed.ServiceConfig{
			TN_please_use_getTNServiceConfig: &tnservice.Config{},
			TNCompatible:                     &tnservice.Config{},
		}

		adjustAuthenticatedClusterServiceConfig(metadata.ServiceType_TN, cfg)

		require.Equal(t, authenticatedClusterHeartbeatTimeout,
			cfg.TN_please_use_getTNServiceConfig.HAKeeper.HeatbeatTimeout.Duration)
		require.Equal(t, authenticatedClusterHeartbeatTimeout,
			cfg.TNCompatible.HAKeeper.HeatbeatTimeout.Duration)
	})

	t.Run("unrelated service", func(t *testing.T) {
		cfg := &embed.ServiceConfig{
			TN_please_use_getTNServiceConfig: &tnservice.Config{},
		}

		adjustAuthenticatedClusterServiceConfig(metadata.ServiceType_LOG, cfg)

		require.Zero(t, cfg.CN.HAKeeper.HeatbeatTimeout.Duration)
		require.Zero(t, cfg.TN_please_use_getTNServiceConfig.HAKeeper.HeatbeatTimeout.Duration)
	})
}

func TestMain(m *testing.M) {
	code := m.Run()
	if authenticatedClusterSuite.cluster != nil {
		if err := authenticatedClusterSuite.cluster.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close authenticated test cluster: %v\n", err)
			if code == 0 {
				code = 1
			}
		}
	}
	os.Exit(code)
}
