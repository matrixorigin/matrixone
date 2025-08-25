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
	mruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	"sync"
)

var (
	basicOnce         sync.Once
	basicCluster      Cluster
	basicRunningMutex sync.Mutex
)

func init() {
	stats.SkipPanicONDuplicate.Store(true)
}

// RunBaseClusterTests starting an integration test for a 1 log, 1tn, 3cn base cluster is very slow
// due to the amount of time it takes to start a cluster (10-20s) when there are a very large number
// of test cases. So for some special cases that don't need to be restarted, a basicCluster can be
// reused to run the test cases. in summary, the basic cluster will only be started once!
func RunBaseClusterTests(
	fn func(Cluster),
) error {
	// we must make all tests which use the basicCluster to be run in sequence
	basicRunningMutex.Lock()
	defer basicRunningMutex.Unlock()

	var err error
	var c Cluster
	basicOnce.Do(
		func() {
			c, err = NewCluster(
				WithCNCount(3),
				WithTesting(),
				WithPreStart(func(svc ServiceOperator) {
					if svc.ServiceType() == metadata.ServiceType_CN {
						svc.Adjust(
							func(config *ServiceConfig) {
								config.CN.LockService.MaxFixedSliceSize = 10001
								config.CN.LockService.MaxLockRowCount = 10000
								config.CN.Frontend.SkipCheckUser = true
							},
						)
					}
				}),
			)
			if err != nil {
				return
			}
			err = c.Start()
			if err != nil {
				return
			}
			basicCluster = c
		},
	)
	if err != nil {
		return err
	}
	// Initialize essential frontend/session state using SQL executor
	func() {
		svc, e := basicCluster.GetCNService(0)
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
	}()
	fn(basicCluster)
	return nil
}
