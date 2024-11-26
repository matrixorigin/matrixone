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
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

/*
	mo_ctl("cn", "MoTableStats", "use_old_impl:false|true");
	mo_ctl("cn", "MoTableStats", "force_update:false|true");
	mo_ctl("cn", "MoTableStats", "move_on:false|true");
	mo_ctl("cn", "MoTableStats", "restore_default_setting:true");
	mo_ctl("cn", "MoTableStats", "echo_current_setting:true");
*/

func handleMoTableStats(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {

	if service != cn {
		return Result{}, moerr.NewWrongServiceNoCtx("expected CN", string(service))
	}

	request := proc.GetQueryClient().NewRequest(query.CmdMethod_CtlMoTableStats)
	request.CtlMoTableStatsRequest.Cmd = strings.ToLower(parameter)

	results := make(map[string]string)

	clusterservice.GetMOCluster(
		proc.GetService()).GetCNService(clusterservice.Selector{}, func(cn metadata.CNService) bool {
		ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second, moerr.CauseTransferRequest2OtherCNs)
		defer cancel()

		resp, err := proc.GetQueryClient().SendMessage(ctx, cn.QueryAddress, request)
		err = moerr.AttachCause(ctx, err)

		if err != nil {
			results[cn.ServiceID] = err.Error()
			//result += fmt.Sprintf("%s:%s\n", cn.ServiceID, err.Error())
		} else {
			results[cn.ServiceID] = resp.CtlMoTableStatsResponse.Resp
			//result += fmt.Sprintf("%s:%s\n", cn.ServiceID, resp.CtlMoTableStatsResponse.Resp)
		}
		return true
	})

	return Result{
		Method: MoTableStats,
		Data:   results,
	}, nil
}
