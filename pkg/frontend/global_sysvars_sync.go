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
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	queryclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
)

const globalSysVarCommitSyncTimeout = 10 * time.Second

// syncGlobalSysVarCommit makes a committed SET GLOBAL visible to transactions
// created on every currently routable CN before the statement reports success.
func syncGlobalSysVarCommit(ctx context.Context, ses *Session) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if ses == nil {
		return moerr.NewInternalError(ctx, "session is not initialized")
	}
	pu := getPuIfPresent(ses.GetService())
	// Focused frontend tests and non-CN users do not install a query client.
	// A running CN always does, so the production path remains fail-closed.
	if pu == nil || pu.QueryClient == nil {
		return nil
	}
	if pu.HAKeeperClient == nil {
		return moerr.NewInternalError(ctx, "HAKeeper client is not initialized")
	}

	commitTS := ses.getLastCommitTS()
	if commitTS.IsEmpty() {
		return moerr.NewInternalError(ctx, "global system variable commit timestamp is empty")
	}
	syncCtx, cancel := context.WithTimeoutCause(
		ctx, globalSysVarCommitSyncTimeout, moerr.CauseSyncLatestCommitT)
	defer cancel()

	details, err := pu.HAKeeperClient.GetClusterDetails(syncCtx)
	if err != nil {
		return moerr.AttachCause(syncCtx, err)
	}
	if err = syncCommitTimestampToCNs(
		syncCtx, pu.QueryClient, details.CNStores, commitTS); err != nil {
		return moerr.AttachCause(syncCtx, err)
	}
	return nil
}

func syncCommitTimestampToCNs(
	ctx context.Context,
	qc queryclient.QueryClient,
	cnStores []logpb.CNStore,
	commitTS timestamp.Timestamp,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if commitTS.IsEmpty() {
		return moerr.NewInvalidInput(ctx, "empty sync commit timestamp")
	}
	if qc == nil {
		return moerr.NewInternalError(ctx, "query client is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	nodes := make([]string, 0, len(cnStores))
	seen := make(map[string]struct{}, len(cnStores))
	for _, cn := range cnStores {
		// Proxy routing drops CNStore.State when it builds metadata.CNService and
		// filters only by WorkState. Fence the same set: a TimeoutState CN that is
		// still Working remains routable and must acknowledge or make SET fail.
		if (cn.WorkState != metadata.WorkState_Working &&
			cn.WorkState != metadata.WorkState_Unknown) ||
			cn.QueryAddress == "" {
			continue
		}
		if _, ok := seen[cn.QueryAddress]; ok {
			continue
		}
		seen[cn.QueryAddress] = struct{}{}
		nodes = append(nodes, cn.QueryAddress)
	}
	if len(nodes) == 0 {
		return moerr.NewInternalError(ctx, "no CN query service is available for commit synchronization")
	}

	// QueryClient method-version checks use the caller's negotiated runtime
	// version, not the target binary's capability. Preflight every endpoint so a
	// mixed-version cluster never invokes the legacy fatal-on-timeout handler.
	if err := requireContextAwareSyncCommit(ctx, qc, nodes); err != nil {
		return err
	}

	genRequest := func() *querypb.Request {
		req := qc.NewRequest(querypb.CmdMethod_SyncCommit)
		req.SycnCommit = &querypb.SyncCommitRequest{LatestCommitTS: commitTS}
		return req
	}
	var responseErr error
	handleResponse := func(address string, resp *querypb.Response) {
		if resp == nil {
			responseErr = errors.Join(responseErr, moerr.NewInternalErrorf(
				ctx, "CN %s returned an empty sync commit response", address))
			return
		}
		if resp.SyncCommit == nil {
			responseErr = errors.Join(responseErr, moerr.NewInternalErrorf(
				ctx, "CN %s returned no applied commit timestamp", address))
			return
		}
		if resp.SyncCommit.CurrentCommitTS.Less(commitTS) {
			responseErr = errors.Join(responseErr, moerr.NewInternalErrorf(
				ctx,
				"CN %s applied commit timestamp %s before required %s",
				address,
				resp.SyncCommit.CurrentCommitTS.DebugString(),
				commitTS.DebugString(),
			))
		}
	}
	requestErr := queryservice.RequestMultipleCn(
		ctx,
		nodes,
		qc,
		genRequest,
		handleResponse,
		nil,
	)
	return errors.Join(requestErr, responseErr)
}

func requireContextAwareSyncCommit(
	ctx context.Context,
	qc queryclient.QueryClient,
	nodes []string,
) error {
	genRequest := func() *querypb.Request {
		req := qc.NewRequest(querypb.CmdMethod_GetProtocolVersion)
		req.GetProtocolVersion = &querypb.GetProtocolVersionRequest{}
		return req
	}
	var responseErr error
	handleResponse := func(address string, resp *querypb.Response) {
		if resp == nil || resp.GetProtocolVersion == nil {
			responseErr = errors.Join(responseErr, moerr.NewInternalErrorf(
				ctx, "CN %s returned no protocol version", address))
			return
		}
		if resp.GetProtocolVersion.Version < defines.MORPCVersion27 {
			responseErr = errors.Join(responseErr, moerr.NewInternalErrorf(
				ctx,
				"CN %s does not support context-aware SyncCommit: protocol version %d",
				address,
				resp.GetProtocolVersion.Version,
			))
		}
	}
	requestErr := queryservice.RequestMultipleCn(
		ctx,
		nodes,
		qc,
		genRequest,
		handleResponse,
		nil,
	)
	return errors.Join(requestErr, responseErr)
}
