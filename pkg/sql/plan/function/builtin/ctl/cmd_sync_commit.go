// Copyright 2023 Matrix Origin
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/ctlservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleSyncCommit(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (pb.CtlResult, error) {
	cs := ctlservice.GetCtlService()
	mc := clusterservice.GetMOCluster()
	var services []string
	mc.GetCNService(
		clusterservice.NewSelector(),
		func(c metadata.CNService) bool {
			services = append(services, c.ServiceID)
			return true
		})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	maxCommitTS := timestamp.Timestamp{}
	for _, id := range services {
		resp, err := cs.SendCtlMessage(
			ctx,
			metadata.ServiceType_CN,
			id,
			cs.NewRequest(pb.CmdMethod_GetCommit))
		if err != nil {
			return pb.CtlResult{}, err
		}
		if maxCommitTS.Less(resp.GetCommit.CurrentCommitTS) {
			maxCommitTS = resp.GetCommit.CurrentCommitTS
		}
		cs.Release(resp)
	}

	for _, id := range services {
		req := cs.NewRequest(pb.CmdMethod_SyncCommit)
		req.SycnCommit.LatestCommitTS = maxCommitTS
		resp, err := cs.SendCtlMessage(
			ctx,
			metadata.ServiceType_CN,
			id,
			req)
		if err != nil {
			return pb.CtlResult{}, err
		}
		cs.Release(resp)
	}

	return pb.CtlResult{
		Method: pb.CmdMethod_SyncCommit.String(),
		Data: fmt.Sprintf("sync %d cn services's commit ts to %s",
			len(services),
			maxCommitTS.DebugString()),
	}, nil
}
