// Copyright 2021 - 2023 Matrix Origin
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

package queryservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
)

// handleRequest handles the requests from client.
func (s *queryService) handleRequest(ctx context.Context, req *pb.Request, resp *pb.Response) error {
	switch pb.CmdMethod(req.Method()) {
	case pb.CmdMethod_ShowProcessList:
		if req.ShowProcessListRequest == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		sessions, err := s.processList(req.ShowProcessListRequest.Tenant,
			req.ShowProcessListRequest.SysTenant)
		if err != nil {
			resp.WrapError(err)
			return nil
		}
		resp.ShowProcessListResponse = &pb.ShowProcessListResponse{
			Sessions: sessions,
		}
		return nil
	default:
		return moerr.NewInternalError(ctx, "Unsupported cmd: %s", req.CmdMethod)
	}
}

// processList returns all the sessions. For sys tenant, return all sessions; but for common
// tenant, just return the sessions belong to the tenant.
// It is called "processList" is because it is used in "SHOW PROCESSLIST" statement.
func (s *queryService) processList(tenant string, sysTenant bool) ([]*status.Session, error) {
	var ss []Session
	if sysTenant {
		ss = s.sessionMgr.GetAllSessions()
	} else {
		ss = s.sessionMgr.GetSessionsByTenant(tenant)
	}
	sessions := make([]*status.Session, 0, len(ss))
	for _, ses := range ss {
		sessions = append(sessions, ses.StatusSession())
	}
	return sessions, nil
}
