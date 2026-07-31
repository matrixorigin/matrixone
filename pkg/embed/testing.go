// Copyright 2021-2024 Matrix Origin
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

package embed

import (
	"context"
	"fmt"
	"sync"
	"time"

	mruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
)

var (
	basicClusterState onceCluster
	basicRunningMutex sync.Mutex
)

type onceCluster struct {
	once    sync.Once
	cluster Cluster
	err     error
}

type testReporter interface {
	Helper()
	Fatalf(format string, args ...any)
}

func (c *onceCluster) run(
	t testReporter,
	init func() (Cluster, error),
	fn func(Cluster),
) {
	c.once.Do(func() {
		c.cluster, c.err = init()
	})
	if c.err != nil {
		t.Fatalf("failed to initialize base cluster: %v", c.err)
		return
	}
	fn(c.cluster)
}

func init() {
	stats.SkipPanicONDuplicate.Store(true)
}

const (
	basicClusterHAKeeperHeartbeatTimeout = 30 * time.Second
	basicClusterHAKeeperStoreTimeout     = 60 * time.Second
)

func startBasicCluster() (Cluster, error) {
	c, err := NewCluster(
		WithCNCount(3),
		WithTesting(),
		WithHAKeeperHeartbeatTimeout(basicClusterHAKeeperHeartbeatTimeout),
		WithPreStart(func(svc ServiceOperator) {
			switch svc.ServiceType() {
			case metadata.ServiceType_CN:
				svc.Adjust(
					func(config *ServiceConfig) {
						config.CN.LockService.MaxFixedSliceSize = 10001
						config.CN.LockService.MaxLockRowCount = 10000
						config.CN.Frontend.SkipCheckUser = false
						config.CN.Frontend.Iceberg.Enable = true
						config.CN.Frontend.Iceberg.EnableWrite = true
						config.CN.Frontend.Iceberg.EnableDelete = true
						config.CN.Frontend.Iceberg.EnableDML = true
						config.CN.Frontend.Iceberg.EnableMaintenance = true
					},
				)
			case metadata.ServiceType_LOG:
				svc.Adjust(
					func(config *ServiceConfig) {
						config.LogService.HAKeeperConfig.TNStoreTimeout.Duration =
							basicClusterHAKeeperStoreTimeout
						config.LogService.HAKeeperConfig.CNStoreTimeout.Duration =
							basicClusterHAKeeperStoreTimeout
					},
				)
			}
		}),
	)
	if err != nil {
		return nil, err
	}
	if err := c.Start(); err != nil {
		return nil, err
	}
	return c, nil
}

func prepareBasicCluster(c Cluster) {
	// Initialize essential frontend/session state using SQL executor
	svc, e := c.GetCNService(0)
	if e != nil {
		return
	}
	// Create and register a TaskService for embed cluster
	// Build a simple address factory using CN SQL address
	cfg := svc.GetServiceConfig()
	sqlAddr := fmt.Sprintf("%s:%d", cfg.CN.Frontend.Host, cfg.CN.Frontend.Port)
	addressFactory := func(ctx context.Context, random bool) (string, error) { return sqlAddr, nil }
	holder := taskservice.NewTaskServiceHolder(mruntime.ServiceRuntime(svc.ServiceID()), addressFactory)
	// register special user for task framework
	username := "task_user"
	password := "task_pass"
	frontend.SetSpecialUser(username, []byte(password))
	_ = holder.Create(logservicepb.CreateTaskService{
		User: logservicepb.TaskTableUser{
			Username: username,
			Password: password,
		},
		TaskDatabase: "mo_task",
	})
	if ts, ok := holder.Get(); ok {
		mruntime.ServiceRuntime(svc.ServiceID()).SetGlobalVariables("task-service", ts)
	}

	// Also prepare and register a ParameterUnit for compile path fallback
	pu := config.NewParameterUnit(&cfg.CN.Frontend, nil, nil, nil)
	mruntime.ServiceRuntime(svc.ServiceID()).SetGlobalVariables("parameter-unit", pu)
}

// RunBaseClusterTests starting an integration test for a 1 log, 1tn, 3cn base cluster is very slow
// due to the amount of time it takes to start a cluster (10-20s) when there are a very large number
// of test cases. So for some special cases that don't need to be restarted, a basicCluster can be
// reused to run the test cases. in summary, the basic cluster will only be started once!
func RunBaseClusterTests(
	t testReporter,
	fn func(Cluster),
) {
	t.Helper()
	// we must make all tests which use the basicCluster to be run in sequence
	basicRunningMutex.Lock()
	defer basicRunningMutex.Unlock()

	basicClusterState.run(t, startBasicCluster, func(c Cluster) {
		prepareBasicCluster(c)
		fn(c)
	})
}
