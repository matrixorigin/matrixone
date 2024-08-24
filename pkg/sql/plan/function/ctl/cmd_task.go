// Copyright 2021 - 2022 Matrix Origin
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

package ctl

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	taskpb "github.com/matrixorigin/matrixone/pkg/pb/task"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	disableTask = "disable"
	enableTask  = "enable"
	getUser     = "getuser"
	retention   = "retention"

	taskMap = map[string]int32{
		"storageusage": int32(taskpb.TaskCode_MetricStorageUsage),
	}
)

// handleTask handles task command
// parameter format:
// 1. enable
// 2. disable
// 3. getuser
// 4. retention:CronExpr
// 5. uuid:taskId
func handleTask(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	parameter = strings.ToLower(parameter)
	switch parameter {
	case disableTask:
		taskservice.DebugCtlTaskFramework(true)
		return Result{
			Method: TaskMethod,
			Data:   "OK",
		}, nil
	case enableTask:
		taskservice.DebugCtlTaskFramework(false)
		return Result{
			Method: TaskMethod,
			Data:   "OK",
		}, nil
	case getUser:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, err := proc.GetHaKeeper().GetClusterState(ctx)
		cancel()
		if err != nil {
			return Result{Method: TaskMethod, Data: "failed to get cluster state"}, err
		}
		user := state.GetTaskTableUser()
		return Result{Method: TaskMethod, Data: user}, nil
	case retention:
		cronExpr := strings.Split(strings.TrimSpace(parameter), ":")[1]
		_, err := proc.GetSessionInfo().SqlHelper.ExecSql(
			fmt.Sprintf(
				"update mo_task.sys_cron_task set cron_expr='%s' where task_metadata_id='retention'",
				cronExpr),
		)
		if err != nil {
			return Result{}, err
		}
		return Result{
			Method: TaskMethod,
			Data:   "OK",
		}, nil
	default:
	}

	args := strings.Split(strings.TrimSpace(parameter), ":")
	if args[0] == retention {
		if len(args) != 2 {
			return Result{Method: TaskMethod, Data: "retention task cron expr not specified"}, nil
		}
		_, err := proc.GetSessionInfo().SqlHelper.ExecSql(
			fmt.Sprintf(
				"update mo_task.sys_cron_task set cron_expr='%s' where task_metadata_id='retention'",
				args[1]),
		)
		if err != nil {
			return Result{}, err
		}
		return Result{
			Method: TaskMethod,
			Data:   "OK",
		}, nil
	}

	target, taskCode, err := checkRunTaskParameter(args)
	if err != nil {
		return Result{}, err
	}
	resp, err := transferTaskToCN(proc.GetQueryClient(), target, taskCode)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Method: TaskMethod,
		Data:   resp,
	}, nil
}

func checkRunTaskParameter(args []string) (string, int32, error) {
	// uuid:taskId
	if len(args) != 2 {
		return "", 0, moerr.NewInternalErrorNoCtx("cmd invalid, expected uuid:task")
	}
	taskCode, ok := taskMap[args[1]]
	if !ok {
		return "", 0, moerr.NewInternalErrorNoCtx("cmd invalid, task %s not found", args[1])
	}
	return args[0], taskCode, nil
}

func transferTaskToCN(qc qclient.QueryClient, target string, taskCode int32) (resp *querypb.Response, err error) {
	clusterservice.GetMOCluster(qc.ServiceID()).GetCNService(
		clusterservice.NewServiceIDSelector(target),
		func(cn metadata.CNService) bool {
			req := qc.NewRequest(querypb.CmdMethod_RunTask)
			req.RunTask = &querypb.RunTaskRequest{TaskCode: taskCode}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			resp, err = qc.SendMessage(ctx, cn.QueryAddress, req)
			return true
		})
	return
}
