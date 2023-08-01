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

package cnservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/ctlservice"
	"github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

func (s *service) initCtlService() {
	cs, err := ctlservice.NewCtlService(
		s.cfg.UUID,
		s.ctlServiceListenAddr(),
		s.cfg.RPC)
	if err != nil {
		panic(err)
	}
	s.ctlservice = cs
	s.initCtlCommandHandler()
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.CtlService, s.ctlservice)
}

func (s *service) initCtlCommandHandler() {
	s.ctlservice.AddHandleFunc(
		ctl.CmdMethod_GetCommit,
		func(
			ctx context.Context,
			req *ctl.Request,
			resp *ctl.Response) error {
			resp.GetCommit.CurrentCommitTS = s._txnClient.(client.TxnClientWithCtl).GetLatestCommitTS()
			return nil
		},
		false)

	s.ctlservice.AddHandleFunc(
		ctl.CmdMethod_SyncCommit,
		func(
			ctx context.Context,
			req *ctl.Request,
			resp *ctl.Response) error {
			s._txnClient.(client.TxnClientWithCtl).SetLatestCommitTS(req.SycnCommit.LatestCommitTS)
			return nil
		},
		false)
}
