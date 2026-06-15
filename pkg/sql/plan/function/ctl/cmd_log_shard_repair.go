// Copyright 2021 - 2022 Matrix Origin
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

package ctl

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type logShardRepairClient interface {
	RepairLogShard(context.Context, logpb.RepairLogShard) error
	UnblockLogShardStores(context.Context, logpb.UnblockLogShardStores) error
}

type logShardRepairCtlRequest struct {
	Op                     string                   `json:"op"`
	Shard                  logShardRepairShardInput `json:"shard"`
	ShardID                uint64                   `json:"shardID"`
	BlockedStores          []string                 `json:"blockedStores"`
	Stores                 []string                 `json:"stores"`
	Reason                 string                   `json:"reason"`
	CleanupReplicasByStore map[string][]uint64      `json:"cleanupReplicasByStore"`
	Force                  bool                     `json:"force"`
	DryRun                 bool                     `json:"dryRun"`
}

type logShardRepairShardInput struct {
	ShardID           uint64            `json:"shardID"`
	Replicas          map[uint64]string `json:"replicas"`
	NonVotingReplicas map[uint64]string `json:"nonVotingReplicas"`
	Epoch             uint64            `json:"epoch"`
	LeaderID          uint64            `json:"leaderID"`
	Term              uint64            `json:"term"`
}

type logShardRepairCtlResult struct {
	Op             string                               `json:"op"`
	DryRun         bool                                 `json:"dryRun"`
	Before         logpb.LogShardInfo                   `json:"before"`
	After          logpb.LogShardInfo                   `json:"after"`
	RepairState    logpb.LogShardRepairState            `json:"repairState"`
	RepairStateSet bool                                 `json:"repairStateSet"`
	BlockedStores  []string                             `json:"blockedStores"`
	Request        logShardRepairCtlRequest             `json:"request"`
	AllRepairs     map[uint64]logpb.LogShardRepairState `json:"allRepairs"`
}

const logShardRepairReasonPrefix = "__mo_log_shard_repair__:"

type logShardRepairReason struct {
	Reason                 string              `json:"reason,omitempty"`
	CleanupReplicasByStore map[string][]uint64 `json:"cleanupReplicasByStore,omitempty"`
}

func encodeLogShardRepairReason(reason string, cleanup map[string][]uint64) (string, error) {
	if len(cleanup) == 0 {
		return reason, nil
	}
	data, err := json.Marshal(logShardRepairReason{
		Reason:                 reason,
		CleanupReplicasByStore: cleanup,
	})
	if err != nil {
		return "", err
	}
	return logShardRepairReasonPrefix + string(data), nil
}

func handleLogShardRepair(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewNotSupportedf(proc.Ctx, "%s only supports CN service", LogShardRepairMethod)
	}
	var req logShardRepairCtlRequest
	if err := json.Unmarshal([]byte(parameter), &req); err != nil {
		return Result{}, moerr.NewInvalidInputf(proc.Ctx, "invalid LogShardRepair JSON: %v", err)
	}
	op := strings.ToLower(strings.TrimSpace(req.Op))
	if op == "" {
		op = "repair"
	}
	client := proc.GetHaKeeper()
	if client == nil {
		return Result{}, moerr.NewInternalError(proc.Ctx, "hakeeper client is nil")
	}
	ctx, cancel := context.WithTimeout(proc.Ctx, 10*time.Second)
	defer cancel()
	beforeState, err := client.GetClusterState(ctx)
	if err != nil {
		return Result{}, err
	}
	result := logShardRepairCtlResult{
		Op:         op,
		DryRun:     req.DryRun,
		Request:    req,
		AllRepairs: beforeState.LogShardRepairs,
	}
	switch op {
	case "repair":
		reason, err := encodeLogShardRepairReason(req.Reason, req.CleanupReplicasByStore)
		if err != nil {
			return Result{}, err
		}
		repair := logpb.RepairLogShard{
			Shard:         req.Shard.toLogShardInfo(),
			BlockedStores: req.BlockedStores,
			Reason:        reason,
			Force:         req.Force,
		}
		if repair.Shard.ShardID == 0 {
			return Result{}, moerr.NewInvalidInput(proc.Ctx, "repair shardID is required")
		}
		result.Before = beforeState.LogState.Shards[repair.Shard.ShardID]
		result.After = repair.Shard
		result.BlockedStores = sortedStrings(req.BlockedStores)
		if !req.DryRun {
			repairClient, ok := client.(logShardRepairClient)
			if !ok {
				return Result{}, moerr.NewNotSupported(proc.Ctx, "hakeeper client does not support log shard repair")
			}
			if err := repairClient.RepairLogShard(ctx, repair); err != nil {
				return Result{}, err
			}
			afterState, err := client.GetClusterState(ctx)
			if err != nil {
				return Result{}, err
			}
			result.After = afterState.LogState.Shards[repair.Shard.ShardID]
			result.RepairState, result.RepairStateSet = afterState.LogShardRepairs[repair.Shard.ShardID]
			result.AllRepairs = afterState.LogShardRepairs
		}
	case "unblock":
		unblock := logpb.UnblockLogShardStores{
			ShardID: req.ShardID,
			Stores:  req.Stores,
			Reason:  req.Reason,
		}
		if unblock.ShardID == 0 {
			return Result{}, moerr.NewInvalidInput(proc.Ctx, "unblock shardID is required")
		}
		result.Before = beforeState.LogState.Shards[unblock.ShardID]
		result.After = result.Before
		result.BlockedStores = sortedStrings(req.Stores)
		result.RepairState, result.RepairStateSet = beforeState.LogShardRepairs[unblock.ShardID]
		if !req.DryRun {
			repairClient, ok := client.(logShardRepairClient)
			if !ok {
				return Result{}, moerr.NewNotSupported(proc.Ctx, "hakeeper client does not support log shard repair")
			}
			if err := repairClient.UnblockLogShardStores(ctx, unblock); err != nil {
				return Result{}, err
			}
			afterState, err := client.GetClusterState(ctx)
			if err != nil {
				return Result{}, err
			}
			result.After = afterState.LogState.Shards[unblock.ShardID]
			result.RepairState, result.RepairStateSet = afterState.LogShardRepairs[unblock.ShardID]
			result.AllRepairs = afterState.LogShardRepairs
		}
	default:
		return Result{}, moerr.NewInvalidInputf(proc.Ctx, "unsupported LogShardRepair op %q", req.Op)
	}
	return Result{
		Method: LogShardRepairMethod,
		Data:   result,
	}, nil
}

func (s logShardRepairShardInput) toLogShardInfo() logpb.LogShardInfo {
	if s.Replicas == nil {
		s.Replicas = make(map[uint64]string)
	}
	if s.NonVotingReplicas == nil {
		s.NonVotingReplicas = make(map[uint64]string)
	}
	return logpb.LogShardInfo{
		ShardID:           s.ShardID,
		Replicas:          s.Replicas,
		NonVotingReplicas: s.NonVotingReplicas,
		Epoch:             s.Epoch,
		LeaderID:          s.LeaderID,
		Term:              s.Term,
	}
}

func sortedStrings(values []string) []string {
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	return sorted
}
