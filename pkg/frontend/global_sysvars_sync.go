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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

const (
	globalSysVarCommitSyncTimeout = 10 * time.Second
	globalSysVarFencePollInterval = 20 * time.Millisecond
)

// validateGlobalSysVarSyncProtocol fails before the catalog mutation when a
// rolling deployment has not activated the HAKeeper routing-fence protocol.
func validateGlobalSysVarSyncProtocol(ctx context.Context, ses *Session) error {
	pu := getPuIfPresent(ses.GetService())
	if pu == nil || pu.HAKeeperClient == nil {
		return nil
	}
	rt := moruntime.ServiceRuntime(ses.GetService())
	if rt == nil {
		return moerr.NewInternalError(ctx, "service runtime is not initialized")
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	if !ok || !valid || version < defines.MORPCVersion14 {
		return moerr.NewInternalErrorf(ctx,
			"SET GLOBAL requires MORPC protocol version %d", defines.MORPCVersion14)
	}
	if _, ok := pu.HAKeeperClient.(logservice.GlobalSysVarHAKeeperClient); !ok {
		return moerr.NewInternalError(ctx,
			"HAKeeper client does not support global system variable fencing")
	}
	return nil
}

// syncGlobalSysVarCommit publishes the committed timestamp as a durable
// HAKeeper admission fence and waits until every CN routable at that
// linearization point has applied it.
func syncGlobalSysVarCommit(ctx context.Context, ses *Session) error {
	pu := getPuIfPresent(ses.GetService())
	if pu == nil || pu.HAKeeperClient == nil {
		return nil
	}
	if pu.TxnClient == nil {
		return moerr.NewInternalError(ctx, "transaction client is not initialized")
	}
	commitTS := pu.TxnClient.GetLatestCommitTS()
	if commitTS.IsEmpty() {
		return moerr.NewInternalError(ctx, "global system variable commit timestamp is empty")
	}
	fenceClient, ok := pu.HAKeeperClient.(logservice.GlobalSysVarHAKeeperClient)
	if !ok {
		return moerr.NewInternalError(ctx,
			"HAKeeper client does not support global system variable fencing")
	}

	syncCtx, cancel := context.WithTimeoutCause(
		ctx, globalSysVarCommitSyncTimeout, moerr.CauseSyncLatestCommitT)
	defer cancel()
	if err := fenceClient.UpdateGlobalSysVarCommitTS(syncCtx, commitTS); err != nil {
		return moerr.AttachCause(syncCtx, err)
	}
	if err := waitGlobalSysVarCommitFence(
		syncCtx, pu.HAKeeperClient.GetClusterDetails, commitTS); err != nil {
		return moerr.AttachCause(syncCtx, err)
	}
	return nil
}

func waitGlobalSysVarCommitFence(
	ctx context.Context,
	getDetails func(context.Context) (logpb.ClusterDetails, error),
	commitTS timestamp.Timestamp,
) error {
	if commitTS.IsEmpty() || getDetails == nil {
		return nil
	}
	ticker := time.NewTicker(globalSysVarFencePollInterval)
	defer ticker.Stop()
	for {
		details, err := getDetails(ctx)
		if err != nil {
			return err
		}
		if details.GlobalSysVarCommitTS.GreaterEq(commitTS) &&
			allRoutableCNsApplied(details.CNStores, commitTS) &&
			allProxiesApplied(details.ProxyStores, commitTS) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func allProxiesApplied(proxies []logpb.ProxyStore, commitTS timestamp.Timestamp) bool {
	for _, proxy := range proxies {
		if proxy.State != logpb.NormalState {
			continue
		}
		if proxy.GlobalSysVarCommitTS.Less(commitTS) {
			return false
		}
	}
	return true
}

func allRoutableCNsApplied(cns []logpb.CNStore, commitTS timestamp.Timestamp) bool {
	for _, cn := range cns {
		if cn.State != logpb.NormalState || cn.SQLAddress == "" ||
			(cn.WorkState != metadata.WorkState_Working &&
				cn.WorkState != metadata.WorkState_Unknown) {
			continue
		}
		if cn.GlobalSysVarCommitTS.Less(commitTS) {
			return false
		}
	}
	return true
}
