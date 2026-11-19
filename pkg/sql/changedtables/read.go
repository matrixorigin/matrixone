// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package changedtables

import (
	"context"
	"errors"
	"maps"
	"time"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type callerIdentity struct {
	accountID, userID, roleID uint32
}

type Row struct {
	Kind, Reason, AccountName, DatabaseName, TableName string
	AccountID                                          uint32
	DatabaseID, TableID                                uint64
}

type Result struct {
	Rows        []Row
	Marker      string
	From        *timestamp.Timestamp
	To          timestamp.Timestamp
	SourceCount uint64
}

const (
	readTimeout          = 30 * time.Second
	maxObserverNameRunes = 300
)

var errReadDeadline = errors.New("changed-table query deadline")

func Read(proc *process.Process, marker, observerName *string) (_ Result, err error) {
	if proc == nil || proc.Base == nil || proc.Ctx == nil {
		return Result{}, moerr.NewInternalErrorNoCtx("changed-table query process is unavailable")
	}
	ctx, cancel := context.WithTimeoutCause(proc.Ctx, readTimeout, errReadDeadline)
	defer cancel()
	defer func() {
		if context.Cause(ctx) == errReadDeadline &&
			(errors.Is(err, context.DeadlineExceeded) || moerr.IsMoErrCode(err, moerr.ErrRPCTimeout)) {
			err = moerr.NewChangeWatchRetry(ctx, "the CN changed-table query deadline expired")
		}
	}()
	if marker != nil && (len(*marker) == 0 || len(*marker) > MaxMarkerBytes) {
		return Result{}, moerr.NewInvalidInput(ctx, "invalid changed-table marker length")
	}
	if observerName != nil && (len(*observerName) == 0 || len(*observerName) > maxObserverNameRunes*utf8.UTFMax ||
		!utf8.ValidString(*observerName) || utf8.RuneCountInString(*observerName) > maxObserverNameRunes) {
		return Result{}, moerr.NewInvalidInput(ctx, "invalid changed-table observer name")
	}
	caller, err := loadCallerIdentity(ctx, proc.GetTopContext())
	if err != nil {
		return Result{}, err
	}
	if err := validateWatchPrincipal(ctx, caller, observerName); err != nil {
		return Result{}, err
	}
	service := proc.GetService()
	rt := runtime.ServiceRuntime(service)
	if rt == nil {
		return Result{}, moerr.NewNotSupported(ctx, "changed-table CN runtime is unavailable")
	}
	value, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	if version, ok := value.(int64); !ok || version < defines.MORPCVersion39 {
		return Result{}, moerr.NewNotSupported(ctx, "changed-table queries require MORPC 39")
	}
	value, _ = rt.GetGlobalVariables(runtime.InternalSQLExecutor)
	exec, ok := value.(executor.SQLExecutor)
	if !ok || exec == nil {
		return Result{}, moerr.NewNotSupported(ctx, "changed-table catalog executor is unavailable")
	}

	barrier, ok := proc.Base.SessionInfo.StorageEngine.(engine.LogtailReadBarrier)
	if !ok {
		barrier, ok = proc.Ctx.Value(defines.EngineKey{}).(engine.LogtailReadBarrier)
	}
	if !ok {
		return Result{}, moerr.NewNotSupported(ctx, "changed-table logtail read barrier is unavailable")
	}
	keeper := proc.GetHaKeeper()
	target, err := changedTableTN(ctx, keeper)
	if err != nil {
		return Result{}, err
	}
	barrierStarted := time.Now()
	to, err := barrier.AcquireLogtailReadBarrier(ctx)
	v2.ChangeWatchBarrierDurationHistogram.Observe(time.Since(barrierStarted).Seconds())
	if err != nil {
		return Result{}, err
	}
	if to.PhysicalTime <= 0 || to.NodeID != 0 {
		return Result{}, moerr.NewInternalError(ctx, "invalid changed-table publication frontier")
	}
	registry, err := readCatalog(ctx, exec, caller, observerName, to)
	if err != nil {
		return Result{}, err
	}

	var prior Marker
	var from *timestamp.Timestamp
	if marker != nil {
		prior, err = DecodeMarker(ctx, *marker, registry.binding, to)
		if err != nil {
			return Result{}, err
		}
		from = &prior.To
	}
	digests, baseline, incremental, err := ProjectGrants(ctx, registry.grants, prior.Sources)
	if err != nil {
		return Result{}, err
	}
	req := cmd_util.GetScopedChangedTablesReq{From: to, To: to}
	if from != nil && from.Less(to) && len(incremental) != 0 {
		req.From = *from
		req.Tables = make([]cmd_util.ScopedChangedTable, 0, len(incremental))
		for _, grant := range incremental {
			req.Tables = append(req.Tables, grantTableIdentity(grant))
		}
	}
	changed, err := requestScopedChanges(ctx, proc.Base.TxnClient, target, &req)
	if err != nil {
		return Result{}, err
	}
	changedTables := make(map[cmd_util.ScopedChangedTable]struct{}, len(changed))
	for _, table := range changed {
		changedTables[table] = struct{}{}
	}

	rows := make([]Row, 0, len(baseline)+len(changed))
	for _, grant := range baseline {
		reason := "source_added"
		if from == nil {
			reason = "first"
		} else if _, exists := prior.Sources[grant.AccountID]; exists {
			reason = "scope_changed"
		}
		rows = append(rows, changedTableRow(grant, registry.sourceNames, "BASELINE", reason))
	}
	for _, grant := range incremental {
		if _, ok := changedTables[grantTableIdentity(grant)]; ok {
			rows = append(rows, changedTableRow(grant, registry.sourceNames, "CHANGED", "dml"))
		}
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	next, err := EncodeMarker(ctx, registry.binding, Marker{To: to, Sources: digests})
	if err != nil {
		return Result{}, err
	}

	// Read the current authority again so a revoked grant cannot release an
	// already-buffered row or marker. A fresh cut also fences DDL and recovery.
	currentCaller, err := loadCallerIdentity(ctx, proc.GetTopContext())
	if err != nil {
		return Result{}, err
	}
	if currentCaller != caller {
		return Result{}, moerr.NewInvalidInput(ctx, "changed-table caller identity changed during query")
	}
	barrierStarted = time.Now()
	recheckTo, err := barrier.AcquireLogtailReadBarrier(ctx)
	v2.ChangeWatchRecheckBarrierDurationHistogram.Observe(time.Since(barrierStarted).Seconds())
	if err != nil {
		return Result{}, err
	}
	if recheckTo.Less(to) || recheckTo.NodeID != 0 {
		return Result{}, moerr.NewInternalError(ctx, "changed-table publication frontier regressed during query")
	}
	current, err := readCatalog(ctx, exec, caller, observerName, recheckTo)
	if err != nil {
		return Result{}, err
	}
	currentDigests, _, _, err := ProjectGrants(ctx, current.grants, digests)
	if err != nil {
		return Result{}, err
	}
	if current.binding != registry.binding {
		return Result{}, moerr.NewInvalidInput(ctx, "changed-table observer binding changed during query")
	}
	if !maps.Equal(digests, currentDigests) || !maps.Equal(registry.sourceNames, current.sourceNames) {
		if recheckTo.Equal(to) {
			return Result{}, moerr.NewInternalError(ctx, "changed-table catalog scope changed at the same publication frontier")
		}
		return Result{}, moerr.NewChangeWatchRetry(ctx, "changed-table authorization or catalog scope changed during query")
	}
	recheckTN, err := changedTableTN(ctx, keeper)
	if err != nil {
		return Result{}, err
	}
	if recheckTN.ShardID != target.ShardID || recheckTN.LogShardID != target.LogShardID ||
		recheckTN.ReplicaID != target.ReplicaID || recheckTN.Address != target.Address {
		return Result{}, moerr.NewChangeWatchRetry(ctx, "changed-table TN routing changed during query")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return Result{Rows: rows, Marker: next, From: from, To: to, SourceCount: uint64(len(digests))}, nil
}

func loadCallerIdentity(ctx, top context.Context) (callerIdentity, error) {
	if top == nil {
		return callerIdentity{}, moerr.NewInvalidInput(ctx, "changed-table caller identity is unavailable")
	}
	account, hasAccount := top.Value(defines.TenantIDKey{}).(uint32)
	user, hasUser := top.Value(defines.UserIDKey{}).(uint32)
	role, hasRole := top.Value(defines.RoleIDKey{}).(uint32)
	if !hasAccount || !hasUser || !hasRole {
		return callerIdentity{}, moerr.NewInvalidInput(ctx, "changed-table caller identity is unavailable")
	}
	return callerIdentity{accountID: account, userID: user, roleID: role}, nil
}

type changeWatchTopologyReader interface {
	GetClusterState(context.Context) (logpb.CheckerState, error)
}

func changedTableTN(ctx context.Context, keeper changeWatchTopologyReader) (metadata.TNShard, error) {
	if keeper == nil {
		return metadata.TNShard{}, moerr.NewNotSupported(ctx, "changed-table HAKeeper client is unavailable")
	}
	state, err := keeper.GetClusterState(ctx)
	if err != nil {
		return metadata.TNShard{}, err
	}
	if err := ctx.Err(); err != nil {
		return metadata.TNShard{}, err
	}

	shards := state.ClusterInfo.TNShards
	if len(shards) != 1 || shards[0].ShardID == 0 || shards[0].LogShardID == 0 {
		return metadata.TNShard{}, moerr.NewNotSupported(ctx, "changed-table query requires one configured TN shard")
	}
	configured := shards[0]
	var target metadata.TNShard
	var latestTick uint64
	var contenders int
	for _, store := range state.TNState.Stores {
		if len(store.Shards) == 0 {
			continue
		}
		if len(store.Shards) != 1 || store.Shards[0].ShardID != configured.ShardID {
			return metadata.TNShard{}, moerr.NewNotSupported(ctx, "changed-table TN advertisement does not match the configured shard")
		}

		// Use MatrixOne's latest-heartbeat routing rule after checking the
		// authoritative shard inventory. HAKeeper can retain an older store's
		// advertisement after a replica of the same shard is replaced.
		if contenders == 0 || store.Tick > latestTick {
			latestTick = store.Tick
			contenders = 1
			target = metadata.TNShard{
				TNShardRecord: configured,
				ReplicaID:     store.Shards[0].ReplicaID,
				Address:       store.ServiceAddress,
			}
		} else if store.Tick == latestTick {
			contenders++
		}
	}
	if contenders != 1 || target.ReplicaID == 0 || target.Address == "" {
		return metadata.TNShard{}, moerr.NewNotSupported(ctx, "changed-table query requires one unambiguous TN route")
	}
	return target, nil
}

func requestScopedChanges(
	ctx context.Context,
	txnClient client.TxnClient,
	target metadata.TNShard,
	req *cmd_util.GetScopedChangedTablesReq,
) ([]cmd_util.ScopedChangedTable, error) {
	if txnClient == nil {
		return nil, moerr.NewInternalError(ctx, "changed-table TN client is unavailable")
	}
	payload, err := req.MarshalBinary()
	if err != nil {
		return nil, err
	}
	snapshot := req.To.Next()
	if !snapshot.Greater(req.To) {
		return nil, moerr.NewInternalError(ctx, "changed-table publication frontier has no successor")
	}
	op, err := txnClient.New(ctx, timestamp.Timestamp{}, client.WithTxnReadyOnly(), client.WithSnapshotTS(snapshot))
	if err != nil {
		return nil, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		_ = op.Rollback(cleanup)
	}()
	rpcMetric := v2.ChangeWatchCapabilityRPCDurationHistogram
	if len(req.Tables) != 0 {
		rpcMetric = v2.ChangeWatchScanRPCDurationHistogram
	}
	started := time.Now()
	result, err := op.Debug(ctx, []txn.TxnRequest{{
		CNRequest: &txn.CNOpRequest{OpCode: uint32(api.OpCode_OpGetScopedChangedTables), Target: target, Payload: payload},
	}})
	rpcMetric.Observe(time.Since(started).Seconds())
	if result != nil {
		defer result.Release()
	}
	if err != nil {
		return nil, err
	}
	if result == nil || len(result.Responses) != 1 || result.Responses[0].CNOpResponse == nil ||
		result.Responses[0].TxnError != nil || result.Responses[0].Method != txn.TxnMethod_DEBUG {
		return nil, moerr.NewInternalError(ctx, "invalid scoped changed-table TN response")
	}
	var response cmd_util.GetScopedChangedTablesResp
	if err := response.UnmarshalBinary(result.Responses[0].CNOpResponse.Payload); err != nil {
		return nil, err
	}
	if err := cmd_util.ValidateScopedChangedTablesResponse(ctx, req, &response); err != nil {
		return nil, err
	}
	return response.Tables, nil
}

func grantTableIdentity(grant Grant) cmd_util.ScopedChangedTable {
	return cmd_util.ScopedChangedTable{AccountID: grant.AccountID, DatabaseID: grant.DatabaseID, TableID: grant.TableID}
}

func changedTableRow(grant Grant, names map[uint32]string, kind, reason string) Row {
	return Row{
		Kind: kind, Reason: reason, AccountName: names[grant.AccountID], AccountID: grant.AccountID,
		DatabaseName: grant.DatabaseName, DatabaseID: grant.DatabaseID, TableName: grant.TableName, TableID: grant.TableID,
	}
}
