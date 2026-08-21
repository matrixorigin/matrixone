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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const (
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
	if !ok || !valid || version < defines.MORPCVersion24 {
		return moerr.NewInternalErrorf(ctx,
			"SET GLOBAL requires MORPC protocol version %d", defines.MORPCVersion24)
	}
	if _, ok := pu.HAKeeperClient.(logservice.GlobalSysVarHAKeeperClient); !ok {
		return moerr.NewInternalError(ctx,
			"HAKeeper client does not support global system variable fencing")
	}
	details, err := pu.HAKeeperClient.GetClusterDetails(ctx)
	if err != nil {
		return err
	}
	return validateGlobalSysVarSyncProtocolDetails(ctx, details)
}

func validateGlobalSysVarSyncProtocolDetails(
	ctx context.Context, details logpb.ClusterDetails,
) error {
	hasServingCN := false
	for _, cn := range details.CNStores {
		if cn.State != logpb.NormalState || cn.SQLAddress == "" {
			continue
		}
		hasServingCN = true
		if cn.ProtocolVersion < defines.MORPCVersion24 {
			return moerr.NewInternalErrorf(ctx,
				"CN %s protocol version %d does not support global system variable fencing",
				cn.UUID, cn.ProtocolVersion)
		}
	}
	if !hasServingCN {
		return moerr.NewInternalError(ctx,
			"HAKeeper has no protocol-capable SQL CN")
	}
	hasLiveLogStore := false
	for _, store := range details.LogStores {
		if store.State != logpb.NormalState {
			continue
		}
		hasLiveLogStore = true
		if store.ProtocolVersion < defines.MORPCVersion24 {
			return moerr.NewInternalErrorf(ctx,
				"LogStore %s protocol version %d does not support global system variable fencing",
				store.UUID, store.ProtocolVersion)
		}
	}
	if !hasLiveLogStore {
		return moerr.NewInternalError(ctx,
			"HAKeeper has no protocol-capable live LogStore")
	}
	for _, proxy := range details.ProxyStores {
		if proxy.State != logpb.NormalState {
			continue
		}
		if proxy.ProtocolVersion < defines.MORPCVersion24 {
			return moerr.NewInternalErrorf(ctx,
				"Proxy %s protocol version %d does not support global system variable fencing",
				proxy.UUID, proxy.ProtocolVersion)
		}
	}
	return nil
}

func beginGlobalSysVarUpdate(ctx context.Context, ses *Session) (uint64, error) {
	pu := getPuIfPresent(ses.GetService())
	if pu == nil || pu.HAKeeperClient == nil {
		return 0, nil
	}
	fenceClient, ok := pu.HAKeeperClient.(logservice.GlobalSysVarHAKeeperClient)
	if !ok {
		return 0, moerr.NewInternalError(ctx,
			"HAKeeper client does not support global system variable fencing")
	}
	for {
		if err := validateGlobalSysVarSyncProtocol(ctx, ses); err != nil {
			return 0, err
		}
		details, err := pu.HAKeeperClient.GetClusterDetails(ctx)
		if err != nil {
			return 0, err
		}
		if err = validateGlobalSysVarSyncProtocolDetails(ctx, details); err != nil {
			return 0, err
		}
		generation, err := fenceClient.BeginGlobalSysVarUpdate(
			ctx, details.GlobalSysVarMembershipRevision, defines.MORPCVersion24)
		if err != nil {
			return 0, err
		}
		if generation != 0 {
			return generation, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(globalSysVarFencePollInterval):
		}
	}
}

func completeAbortedGlobalSysVarUpdate(ctx context.Context, ses *Session, generation uint64) {
	pu := getPuIfPresent(ses.GetService())
	if generation == 0 || pu == nil || pu.HAKeeperClient == nil {
		return
	}
	fenceClient, ok := pu.HAKeeperClient.(logservice.GlobalSysVarHAKeeperClient)
	if !ok {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), logservice.GlobalSysVarFenceTimeoutFloor)
	defer cancel()
	_ = fenceClient.CompleteGlobalSysVarUpdate(
		cleanupCtx, generation, timestamp.Timestamp{})
}

func globalSysVarFenceTimeout(details logpb.ClusterDetails) time.Duration {
	maxProgress := time.Duration(0)
	for _, cn := range details.CNStores {
		if cn.State == logpb.NormalState {
			maxProgress = max(maxProgress, time.Duration(cn.GlobalSysVarProgressTimeoutNanos))
		}
	}
	for _, proxy := range details.ProxyStores {
		if proxy.State == logpb.NormalState {
			maxProgress = max(maxProgress, time.Duration(proxy.GlobalSysVarProgressTimeoutNanos))
		}
	}
	return max(logservice.GlobalSysVarFenceTimeoutFloor,
		2*maxProgress+logservice.GlobalSysVarFenceControlPlaneSlack)
}

// ReconcileGlobalSysVarOutbox consumes the catalog-backed outbox independently
// of the SQL session that committed SET GLOBAL.  HAKeeper's pending generation
// is only completed after the same generation is visible in the catalog, so a
// concurrent uncommitted mutation cannot be published prematurely.
var readGlobalSysVarOutboxGeneration = func(ctx context.Context, service string) (uint64, error) {
	executor := NewInternalExecutor(service)
	opts := ie.NewOptsBuilder().Database("mo_catalog").Internal(true).
		AccountId(sysAccountID).UserId(rootID).DefaultRoleId(moAdminRoleID).Finish()
	query := fmt.Sprintf(
		"select cast(variable_value as unsigned) from mo_catalog.mo_mysql_compatibility_mode "+
			"where system_variables = true and variable_name = '%s' "+
			"order by cast(variable_value as unsigned) desc limit 1",
		globalSystemVariableEpochName)
	result := executor.Query(ctx, query, opts)
	if err := result.Error(); err != nil {
		return 0, err
	}
	if result.RowCount() == 0 {
		return 0, nil
	}
	return result.GetUint64(ctx, 0, 0)
}

func ReconcileGlobalSysVarOutbox(ctx context.Context, service string) error {
	pu := getPuIfPresent(service)
	if pu == nil || pu.HAKeeperClient == nil || pu.TxnClient == nil {
		return nil
	}
	fenceClient, ok := pu.HAKeeperClient.(logservice.GlobalSysVarHAKeeperClient)
	if !ok {
		return nil
	}
	details, err := pu.HAKeeperClient.GetClusterDetails(ctx)
	if err != nil {
		return err
	}
	pending := details.GlobalSysVarPendingGeneration
	if pending == 0 || pending <= details.GlobalSysVarCompletedGeneration {
		return nil
	}
	durableGeneration, err := readGlobalSysVarOutboxGeneration(ctx, service)
	if err != nil {
		return err
	}
	if durableGeneration < pending {
		return nil
	}
	commitTS := pu.TxnClient.GetLatestCommitTS()
	if commitTS.IsEmpty() {
		return moerr.NewInternalError(ctx, "global system variable reconciliation timestamp is empty")
	}
	reconcileCtx, cancel := context.WithTimeoutCause(ctx,
		globalSysVarFenceTimeout(details), moerr.CauseSyncLatestCommitT)
	defer cancel()
	if err = fenceClient.CompleteGlobalSysVarUpdate(reconcileCtx, pending, commitTS); err != nil {
		return moerr.AttachCause(reconcileCtx, err)
	}
	if err = waitGlobalSysVarCommitFence(
		reconcileCtx, pu.HAKeeperClient.GetClusterDetails, commitTS); err != nil {
		return moerr.AttachCause(reconcileCtx, err)
	}
	return nil
}

// syncGlobalSysVarCommit publishes the committed timestamp as a durable
// HAKeeper admission fence and waits until every CN routable at that
// linearization point has applied it.
func syncGlobalSysVarCommit(ctx context.Context, ses *Session) error {
	return completeAndSyncGlobalSysVarCommit(ctx, ses, 0)
}

func completeAndSyncGlobalSysVarCommit(ctx context.Context, ses *Session, generation uint64) error {
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

	details, err := pu.HAKeeperClient.GetClusterDetails(ctx)
	if err != nil {
		return err
	}
	syncCtx, cancel := context.WithTimeoutCause(ctx,
		globalSysVarFenceTimeout(details), moerr.CauseSyncLatestCommitT)
	defer cancel()
	if generation == 0 {
		err = fenceClient.UpdateGlobalSysVarCommitTS(syncCtx, commitTS)
	} else {
		err = fenceClient.CompleteGlobalSysVarUpdate(syncCtx, generation, commitTS)
	}
	if err != nil {
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
