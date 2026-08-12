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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
)

var (
	basicClusterState SharedTestCluster
)

// SharedTestCluster serializes tests that reuse an expensive embedded cluster
// and preserves the first initialization result. Initialization callbacks must
// return errors instead of failing the current test from inside sync.Once, so a
// failed startup is reported consistently to every later caller.
type SharedTestCluster struct {
	mu      sync.Mutex
	once    sync.Once
	cluster Cluster
	err     error
	closed  bool
}

type testReporter interface {
	Helper()
	Fatalf(format string, args ...any)
}

func (c *SharedTestCluster) Run(
	t testReporter,
	init func() (Cluster, error),
	fn func(Cluster),
) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		t.Fatalf("shared cluster is closed")
		return
	}

	c.once.Do(func() {
		c.cluster, c.err = init()
		if c.err == nil && c.cluster == nil {
			c.err = moerr.NewInternalErrorNoCtx("cluster initializer returned nil without an error")
		}
	})
	if c.err != nil && c.cluster != nil {
		cleanupErr := c.cluster.Close()
		if cleanupErr == nil {
			c.cluster = nil
		} else {
			c.err = errors.Join(c.err, cleanupErr)
		}
	}
	if c.err != nil {
		t.Fatalf("failed to initialize shared cluster: %v", c.err)
		return
	}
	fn(c.cluster)
}

// Close releases the shared cluster or retries cleanup retained from a failed
// initialization. Ownership is cleared only after the underlying Close has
// completed successfully.
func (c *SharedTestCluster) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cluster == nil {
		c.closed = true
		return nil
	}
	if err := c.cluster.Close(); err != nil {
		return err
	}
	c.cluster = nil
	c.closed = true
	return nil
}

func init() {
	stats.SkipPanicONDuplicate.Store(true)
}

// StartTestCluster constructs and starts an embedded cluster with test mode
// enabled. If startup fails, it closes the partially started cluster before
// returning the original error. If rollback itself fails, the returned cluster
// is non-nil solely so the caller can retain it and retry Close.
func StartTestCluster(opts ...Option) (Cluster, error) {
	opts = append([]Option{WithTesting()}, opts...)
	c, err := NewCluster(opts...)
	if err != nil {
		return cleanupClusterOnError(c, err)
	}
	if err := c.Start(); err != nil {
		return cleanupClusterOnError(c, err)
	}
	return c, nil
}

const (
	basicClusterHAKeeperStoreTimeout           = 60 * time.Second
	basicClusterHAKeeperCheckInterval          = time.Second
	basicClusterHAKeeperBootstrapRetryInterval = 500 * time.Millisecond
	basicClusterServiceStartupRetryInterval    = 100 * time.Millisecond
)

func startBasicCluster() (Cluster, error) {
	return StartTestCluster(
		WithCNCount(3),
		WithPreStart(adjustBasicClusterService),
	)
}

func adjustBasicClusterService(svc ServiceOperator) {
	switch svc.ServiceType() {
	case metadata.ServiceType_CN:
		svc.Adjust(
			func(config *ServiceConfig) {
				config.TNShardReadyRetryInterval.Duration = basicClusterServiceStartupRetryInterval
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
				config.LogService.HAKeeperCheckInterval.Duration = basicClusterHAKeeperCheckInterval
				config.LogService.HAKeeperBootstrapRetryInterval.Duration = basicClusterHAKeeperBootstrapRetryInterval
				config.LogService.HAKeeperConfig.TNStoreTimeout.Duration =
					basicClusterHAKeeperStoreTimeout
				config.LogService.HAKeeperConfig.CNStoreTimeout.Duration =
					basicClusterHAKeeperStoreTimeout
			},
		)
	case metadata.ServiceType_TN:
		svc.Adjust(
			func(config *ServiceConfig) {
				config.HAKeeperRunningRetryInterval.Duration = basicClusterServiceStartupRetryInterval
			},
		)
	}
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
	basicClusterState.Run(t, startBasicCluster, func(c Cluster) {
		prepareBasicCluster(c)
		fn(c)
	})
}
