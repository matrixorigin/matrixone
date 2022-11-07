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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	disableTask = "disable"
	enableTask  = "enable"
)

func handleTask(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (pb.CtlResult, error) {
	switch parameter {
	case disableTask:
		taskservice.DebugCtlTaskFramwork(true)
	case enableTask:
		taskservice.DebugCtlTaskFramwork(false)
	default:
		return pb.CtlResult{},
			moerr.NewInvalidInput("task command only support %s and %s",
				enableTask,
				disableTask)
	}

	return pb.CtlResult{
		Method: pb.CmdMethod_Task.String(),
		Data:   "OK",
	}, nil
}
