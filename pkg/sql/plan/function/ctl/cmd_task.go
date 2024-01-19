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
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	taskpb "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	disableTask = "disable"
	enableTask  = "enable"

	taskMap = map[string]int32{
		"storageusage": int32(taskpb.TaskCode_MetricStorageUsage),
	}
)

// handleTask handles task command
// parameter format:
// 1. enable
// 2. disable
// 3. [uuid:]taskId
func handleTask(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	switch parameter {
	case disableTask:
		taskservice.DebugCtlTaskFramwork(true)
		return Result{
			Method: TaskMethod,
			Data:   "OK",
		}, nil
	case enableTask:
		taskservice.DebugCtlTaskFramwork(false)
		return Result{
			Method: TaskMethod,
			Data:   "OK",
		}, nil
	default:
	}

	target, taskCode, err := checkRunTaskParameter(parameter)
	if err != nil {
		return Result{}, err
	}
	resp, err := transferTaskToCN(proc.QueryService, target, taskCode)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Method: TaskMethod,
		Data:   resp,
	}, nil
}

func checkRunTaskParameter(param string) (string, int32, error) {
	param = strings.ToLower(param)
	// uuid:taskId
	args := strings.Split(param, ":")
	if len(args) != 2 {
		return "", 0, moerr.NewInternalErrorNoCtx("cmd invalid, expected uuid:task")
	}
	taskCode, ok := taskMap[args[1]]
	if !ok {
		return "", 0, moerr.NewInternalErrorNoCtx("cmd invalid, task %s not found", args[1])
	}
	return args[0], taskCode, nil
}

func transferTaskToCN(qs queryservice.QueryService, target string, taskCode int32) (resp *querypb.Response, err error) {
	clusterservice.GetMOCluster().GetCNService(
		clusterservice.NewServiceIDSelector(target),
		func(cn metadata.CNService) bool {
			req := qs.NewRequest(querypb.CmdMethod_RunTask)
			req.RunTask = &querypb.RunTaskRequest{TaskCode: taskCode}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			resp, err = qs.SendMessage(ctx, cn.QueryAddress, req)
			return true
		})
	return
}
