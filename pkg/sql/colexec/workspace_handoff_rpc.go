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

package colexec

import (
	"context"
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const workspaceHandoffRPCTimeout = 5 * time.Second

// WorkspaceHandoffFromBatch validates the producing transaction before any
// records from a remote batch are installed. An absent envelope is valid only
// for paths that did not negotiate a remote workspace handoff.
func WorkspaceHandoffFromBatch(proc *process.Process, bat *batch.Batch) (ioutil.RemoteWorkspaceHandoffScope, bool, error) {
	var zero ioutil.RemoteWorkspaceHandoffScope
	if bat == nil {
		return zero, false, nil
	}
	scope, present, err := ioutil.ParseRemoteWorkspaceHandoffScope(bat.ExtraBuf)
	if err != nil || !present {
		return scope, present, err
	}
	if proc == nil || proc.GetTxnOperator() == nil || string(proc.GetTxnOperator().Txn().ID) != scope.TxnID {
		return zero, true, moerr.NewInvalidStateNoCtx("remote workspace batch belongs to another transaction")
	}
	return scope, true, nil
}

// ConfirmWorkspaceHandoffRecords is called only after the corresponding
// object-stats batch was successfully recorded in the coordinator workspace.
// Missing receipts or failed RPCs are ambiguous and must not be treated as a
// rejection or an instruction for the producer to delete the object.
func ConfirmWorkspaceHandoffRecords(proc *process.Process, scope ioutil.RemoteWorkspaceHandoffScope, recorded *batch.Batch, statsColumn int) error {
	if proc == nil || recorded == nil || statsColumn < 0 || statsColumn >= len(recorded.Vecs) || recorded.Vecs[statsColumn] == nil {
		return moerr.NewInvalidInputNoCtx("invalid recorded workspace object batch")
	}
	vec := recorded.Vecs[statsColumn]
	seen := make(map[string]struct{}, min(vec.Length(), ioutil.RemoteWorkspaceHandoffDecisionBatchLimit))
	names := make([]string, 0, min(vec.Length(), ioutil.RemoteWorkspaceHandoffDecisionBatchLimit))
	flush := func() error {
		if len(names) == 0 {
			return nil
		}
		if err := ConfirmRemoteWorkspaceHandoffs(proc.Ctx, proc, scope, names, ioutil.RemoteWorkspaceHandoffAdopted); err != nil {
			return err
		}
		clear(seen)
		names = names[:0]
		return nil
	}
	for i := 0; i < vec.Length(); i++ {
		raw := vec.GetBytesAt(i)
		if len(raw) != objectio.ObjectStatsLen {
			return moerr.NewInvalidInputNoCtx("invalid recorded workspace object statistics")
		}
		stats := objectio.ObjectStats(raw)
		name := stats.ObjectName().String()
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
		if len(names) == ioutil.RemoteWorkspaceHandoffDecisionBatchLimit {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// SettleFailedWorkspaceHandoff reports only decisions fenced by the local
// coordinator. A cancellation, missing provider or unrecorded name stays
// unknown; the original SQL failure remains the caller's returned error.
func SettleFailedWorkspaceHandoff(proc *process.Process, source any, scope ioutil.RemoteWorkspaceHandoffScope, bat *batch.Batch, statsColumn int) error {
	provider, ok := source.(interface {
		WorkspaceHandoffDecision(ioutil.RemoteWorkspaceHandoffID) (ioutil.RemoteWorkspaceHandoffDecision, error)
	})
	if !ok || proc == nil || proc.Ctx == nil || proc.Ctx.Err() != nil || bat == nil || statsColumn < 0 || statsColumn >= len(bat.Vecs) || bat.Vecs[statsColumn] == nil {
		return nil
	}
	groups := map[ioutil.RemoteWorkspaceHandoffDecision][]string{}
	seen := make(map[string]struct{})
	vec := bat.Vecs[statsColumn]
	for i := 0; i < vec.Length(); i++ {
		raw := vec.GetBytesAt(i)
		if len(raw) != objectio.ObjectStatsLen {
			return moerr.NewInvalidInputNoCtx("invalid remote workspace object statistics")
		}
		stats := objectio.ObjectStats(raw)
		name := stats.ObjectName().String()
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		decision, err := provider.WorkspaceHandoffDecision(scope.Object(name))
		if err != nil {
			return err
		}
		if decision == ioutil.RemoteWorkspaceHandoffAdopted || decision == ioutil.RemoteWorkspaceHandoffRejected {
			groups[decision] = append(groups[decision], name)
		}
	}
	for _, decision := range []ioutil.RemoteWorkspaceHandoffDecision{ioutil.RemoteWorkspaceHandoffAdopted, ioutil.RemoteWorkspaceHandoffRejected} {
		names := groups[decision]
		for len(names) > 0 {
			n := min(len(names), ioutil.RemoteWorkspaceHandoffDecisionBatchLimit)
			if err := ConfirmRemoteWorkspaceHandoffs(proc.Ctx, proc, scope, names[:n], decision); err != nil {
				return err
			}
			names = names[n:]
		}
	}
	return nil
}

// ProbeRemoteWorkspaceHandoff must succeed before the destination is sent a
// fragment that can persist workspace objects.
func ProbeRemoteWorkspaceHandoff(ctx context.Context, proc *process.Process, addr string) error {
	_, err := callWorkspaceHandoffRPC(ctx, proc, addr, nil)
	return err
}

func callWorkspaceHandoffRPC(ctx context.Context, proc *process.Process, addr string, data []byte) ([]byte, error) {
	if ctx == nil || proc == nil || proc.GetQueryClient() == nil || addr == "" {
		return nil, moerr.NewInvalidStateNoCtx("remote workspace capability endpoint is unavailable")
	}
	cluster, err := clusterservice.GetMOClusterWithContext(ctx, proc.GetService())
	if err != nil {
		return nil, err
	}
	var queryAddress string
	ambiguous := false
	err = clusterservice.GetCNServiceWithoutWorkingStateWithContext(ctx, cluster, clusterservice.NewSelector(), func(cn metadata.CNService) bool {
		if cn.PipelineServiceAddress == addr {
			if queryAddress != "" && queryAddress != cn.QueryAddress {
				ambiguous = true
			}
			queryAddress = cn.QueryAddress
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if ambiguous {
		return nil, moerr.NewInvalidStateNoCtx("remote workspace capability address is ambiguous")
	}
	if queryAddress == "" {
		return nil, moerr.NewRetryForCNRollingRestart()
	}
	client := proc.GetQueryClient()
	request := client.NewRequest(query.CmdMethod_GetProtocolVersion)
	request.GetProtocolVersion = &query.GetProtocolVersionRequest{WorkspaceHandoffRequest: data}
	rpcCtx, cancel := context.WithTimeout(ctx, workspaceHandoffRPCTimeout)
	defer cancel()
	response, err := client.SendMessage(rpcCtx, queryAddress, request)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, moerr.NewInvalidStateNoCtx("remote workspace capability response is empty")
	}
	defer client.Release(response)
	if response.GetGetProtocolVersion() == nil || response.GetGetProtocolVersion().GetWorkspaceHandoffVersion() != ioutil.RemoteWorkspaceHandoffVersion1 {
		return nil, moerr.NewRetryForCNRollingRestart()
	}
	return append([]byte(nil), response.GetGetProtocolVersion().GetWorkspaceHandoffResponse()...), nil
}

// ConfirmRemoteWorkspaceHandoff returns false on an unknown or evicted
// identity; callers must retain ownership if transport or confirmation fails.
func ConfirmRemoteWorkspaceHandoff(ctx context.Context, proc *process.Process, scope ioutil.RemoteWorkspaceHandoffScope, name string, decision ioutil.RemoteWorkspaceHandoffDecision) (bool, error) {
	data, err := ioutil.MarshalRemoteWorkspaceHandoffDecision(scope, name, decision)
	if err != nil {
		return false, err
	}
	response, err := callWorkspaceHandoffRPC(ctx, proc, scope.ProducerCN, data)
	if err != nil {
		return false, err
	}
	if len(response) != 1 || response[0] > 1 {
		return false, moerr.NewInvalidStateNoCtx("invalid remote workspace handoff receipt")
	}
	return response[0] == 1, nil
}

// ConfirmRemoteWorkspaceHandoffs uses one RPC for a bounded group while the
// producer still settles and reports every object independently.
func ConfirmRemoteWorkspaceHandoffs(ctx context.Context, proc *process.Process, scope ioutil.RemoteWorkspaceHandoffScope, names []string, decision ioutil.RemoteWorkspaceHandoffDecision) error {
	if ctx == nil {
		return moerr.NewInvalidInputNoCtx("remote workspace handoff context is nil")
	}
	data, err := ioutil.MarshalRemoteWorkspaceHandoffDecisionBatch(scope, names, decision)
	if err != nil {
		return err
	}
	var response []byte
	for attempt := 0; attempt < 3; attempt++ {
		response, err = callWorkspaceHandoffRPC(ctx, proc, scope.ProducerCN, data)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !errors.Is(err, context.DeadlineExceeded) && !moerr.IsMoErrCode(err, moerr.ErrStreamClosed) &&
			!moerr.IsMoErrCode(err, moerr.ErrBackendClosed) && !moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) {
			return err
		}
	}
	if err != nil {
		// The peer may have applied every decision despite lost responses.
		// An absent receipt remains unknown and never authorizes deletion.
		if ctx.Err() == nil {
			if states, queryErr := QueryRemoteWorkspaceHandoffs(ctx, proc, scope, names); queryErr == nil {
				confirmed := true
				for _, state := range states {
					if state != decision {
						confirmed = false
						break
					}
				}
				if confirmed {
					return nil
				}
			}
		}
		return err
	}
	if len(response) != len(names) {
		return moerr.NewInvalidStateNoCtx("invalid remote workspace handoff batch receipt")
	}
	for _, matched := range response {
		if matched != 1 {
			return moerr.NewInvalidStateNoCtx("remote workspace handoff confirmation is unknown")
		}
	}
	return nil
}

// QueryRemoteWorkspaceHandoffs performs a bounded read using the original
// identity. An unknown response is not proof that an object was rejected.
func QueryRemoteWorkspaceHandoffs(ctx context.Context, proc *process.Process, scope ioutil.RemoteWorkspaceHandoffScope, names []string) ([]ioutil.RemoteWorkspaceHandoffDecision, error) {
	return queryWorkspaceHandoffs(ctx, proc, scope.ProducerCN, 'Q', scope, names)
}

// QueryCoordinatorWorkspaceHandoffs asks the recording CN whether it adopted
// the objects. The producing CN remains part of the original identity scope.
func QueryCoordinatorWorkspaceHandoffs(ctx context.Context, proc *process.Process, coordinatorCN string, scope ioutil.RemoteWorkspaceHandoffScope, names []string) ([]ioutil.RemoteWorkspaceHandoffDecision, error) {
	return queryWorkspaceHandoffs(ctx, proc, coordinatorCN, 'C', scope, names)
}

func queryWorkspaceHandoffs(ctx context.Context, proc *process.Process, address string, kind byte, scope ioutil.RemoteWorkspaceHandoffScope, names []string) ([]ioutil.RemoteWorkspaceHandoffDecision, error) {
	data, err := ioutil.MarshalRemoteWorkspaceHandoffDecisionBatch(scope, names, ioutil.RemoteWorkspaceHandoffAdopted)
	if err != nil {
		return nil, err
	}
	data[0] = kind
	response, err := callWorkspaceHandoffRPC(ctx, proc, address, data)
	if err != nil {
		return nil, err
	}
	if len(response) != len(names) {
		return nil, moerr.NewInvalidStateNoCtx("invalid remote workspace handoff query response")
	}
	states := make([]ioutil.RemoteWorkspaceHandoffDecision, len(response))
	for i, state := range response {
		if state > byte(ioutil.RemoteWorkspaceHandoffRejected) {
			return nil, moerr.NewInvalidStateNoCtx("invalid remote workspace handoff query state")
		}
		states[i] = ioutil.RemoteWorkspaceHandoffDecision(state)
	}
	return states, nil
}
