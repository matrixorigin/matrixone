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

package compile

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Probe the selected workers as well as the coordinator's rollout gate.
// Capabilities are not cached across executions or sender checks.
func remoteWorkersSupportProtocol(proc *process.Process, workers engine.Nodes, minimum int64) (bool, error) {
	if proc == nil {
		return false, nil
	}
	parent := proc.Ctx
	if parent == nil {
		parent = context.Background()
	}
	if err := parent.Err(); err != nil {
		return false, err
	}
	version, known := remoteMORPCProtocolVersion(proc.GetService())
	if !known || version < minimum {
		return false, nil
	}
	ctx, cancel := context.WithTimeoutCause(parent, 5*time.Second, moerr.NewInternalError(parent, "remote expression capability probe timed out"))
	defer cancel()
	for _, worker := range workers {
		if worker.Addr == "" || proc.GetQueryClient() == nil {
			return false, nil
		}
		cluster, err := clusterservice.GetMOClusterWithContext(ctx, proc.GetService())
		if err != nil {
			return false, parent.Err()
		}
		var addr string
		var workerID string
		selector := clusterservice.NewSelector()
		if worker.Id != "" {
			selector = clusterservice.NewServiceIDSelector(worker.Id)
		}
		err = clusterservice.GetCNServiceWithoutWorkingStateWithContext(ctx, cluster,
			selector, func(cn metadata.CNService) bool {
				if cn.PipelineServiceAddress == worker.Addr && (worker.Id == "" || cn.ServiceID == worker.Id) {
					addr = cn.QueryAddress
					workerID = cn.ServiceID
					return false
				}
				return true
			})
		if err != nil {
			return false, parent.Err()
		}
		if addr == "" {
			return false, nil
		}
		if workerID != "" && workerID == proc.GetService() {
			continue
		}
		client := proc.GetQueryClient()
		req := client.NewRequest(querypb.CmdMethod_GetProtocolVersion)
		req.GetProtocolVersion = &querypb.GetProtocolVersionRequest{}
		resp, err := client.SendMessage(ctx, addr, req)
		if err != nil {
			if resp != nil {
				client.Release(resp)
			}
			return false, parent.Err()
		}
		if resp == nil {
			return false, nil
		}
		supported := resp.GetProtocolVersion != nil && resp.GetProtocolVersion.Version >= minimum
		client.Release(resp)
		if !supported {
			return false, nil
		}
	}
	return true, nil
}
