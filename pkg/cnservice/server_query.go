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

package cnservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
)

func (s *service) initQueryService() {
	svc, err := queryservice.NewQueryService(s.cfg.UUID,
		s.queryServiceListenAddr(), s.cfg.RPC, s.sessionMgr)
	if err != nil {
		panic(err)
	}
	s.queryService = svc
	s.initQueryCommandHandler()
}

func (s *service) initQueryCommandHandler() {
	s.queryService.AddHandleFunc(query.CmdMethod_KillConn, s.handleKillConn, false)
}

func (s *service) handleKillConn(ctx context.Context, req *query.Request, resp *query.Response) error {
	if req == nil || req.KillConnRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	rm := s.mo.GetRoutineManager()
	if rm == nil {
		return moerr.NewInternalError(ctx, "routine manager not initialized")
	}
	accountMgr := rm.GetAccountRoutineManager()
	if accountMgr == nil {
		return moerr.NewInternalError(ctx, "account routine manager not initialized")
	}

	accountMgr.EnKillQueue(req.KillConnRequest.AccountID, req.KillConnRequest.Version)
	return nil
}
