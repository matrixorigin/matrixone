// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strconv"
	"strings"
	"time"
)

type Res struct {
	PodID           string `json:"pod_id,omitempty"`
	CommitThreshold uint64 `json:"commit_threshold,omitempty"`
	WriteThreshold  uint64 `json:"write_threshold,omitempty"`
	ErrorStr        string `json:"error,omitempty"`
}

func handleWorkspaceThreshold(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewWrongServiceNoCtx("expected CN", string(service))
	}

	thresholds := strings.Split(parameter, ":")
	if len(thresholds) != 2 {
		return Result{}, moerr.NewInvalidInput(proc.Ctx, "invalid parameter")
	}
	commit, err := strconv.ParseUint(thresholds[0], 10, 64)
	if err != nil {
		return Result{}, moerr.NewInvalidInput(proc.Ctx, "invalid commit threshold")
	}
	write, err := strconv.ParseUint(thresholds[1], 10, 64)
	if err != nil {
		return Result{}, moerr.NewInvalidInput(proc.Ctx, "invalid write threshold")
	}

	request := proc.GetQueryClient().NewRequest(query.CmdMethod_WorkspaceThreshold)
	request.WorkspaceThresholdRequest = &query.WorkspaceThresholdRequest{
		CommitThreshold: commit,
		WriteThreshold:  write,
	}

	results := make([]Res, 0)

	clusterservice.GetMOCluster(
		proc.GetService()).GetCNService(clusterservice.Selector{}, func(cn metadata.CNService) bool {
		ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second, moerr.CauseTransferRequest2OtherCNs)
		defer cancel()

		resp, err := proc.GetQueryClient().SendMessage(ctx, cn.QueryAddress, request)
		err = moerr.AttachCause(ctx, err)

		res := Res{
			PodID: cn.ServiceID,
		}

		if err != nil {
			res.ErrorStr = err.Error()
		} else {
			res.CommitThreshold = resp.WorkspaceThresholdResponse.CommitThreshold
			res.WriteThreshold = resp.WorkspaceThresholdResponse.WriteThreshold
		}
		results = append(results, res)
		return true
	})

	return Result{
		Method: WorkspaceThreshold,
		Data:   results,
	}, nil
}
