// Copyright 2026 Matrix Origin
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

package frontend

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	queryclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
)

const globalSysVarCommitSyncTimeout = 10 * time.Second

// syncGlobalSysVarCommit makes a committed SET GLOBAL visible to transactions
// created on every routable CN before the statement reports success.
func syncGlobalSysVarCommit(ctx context.Context, ses *Session) error {
	pu := getPuIfPresent(ses.GetService())
	if pu == nil || pu.QueryClient == nil {
		return nil
	}
	if pu.TxnClient == nil {
		return moerr.NewInternalError(ctx, "transaction client is not initialized")
	}
	commitTS := pu.TxnClient.GetLatestCommitTS()
	if commitTS.IsEmpty() {
		return moerr.NewInternalError(ctx, "global system variable commit timestamp is empty")
	}

	syncCtx, cancel := context.WithTimeoutCause(ctx, globalSysVarCommitSyncTimeout, moerr.CauseSyncLatestCommitT)
	defer cancel()

	cluster, err := clusterservice.GetMOClusterWithContext(syncCtx, pu.QueryClient.ServiceID())
	if err != nil {
		return moerr.AttachCause(syncCtx, err)
	}
	if err = syncCommitTimestampToCNs(syncCtx, pu.QueryClient, cluster, commitTS); err != nil {
		return moerr.AttachCause(syncCtx, err)
	}
	return nil
}

func syncCommitTimestampToCNs(
	ctx context.Context,
	qc queryclient.QueryClient,
	cluster clusterservice.MOCluster,
	commitTS timestamp.Timestamp,
) error {
	if commitTS.IsEmpty() || qc == nil || cluster == nil {
		return nil
	}

	nodes := make([]string, 0, 4)
	cluster.GetCNService(clusterservice.NewSelector(), func(cn metadata.CNService) bool {
		if cn.QueryAddress != "" {
			nodes = append(nodes, cn.QueryAddress)
		}
		return true
	})
	if len(nodes) == 0 {
		return nil
	}

	genRequest := func() *querypb.Request {
		req := qc.NewRequest(querypb.CmdMethod_SyncCommit)
		req.SycnCommit = &querypb.SyncCommitRequest{LatestCommitTS: commitTS}
		return req
	}
	return queryservice.RequestMultipleCn(
		ctx,
		nodes,
		qc,
		genRequest,
		func(string, *querypb.Response) {},
		nil,
	)
}
