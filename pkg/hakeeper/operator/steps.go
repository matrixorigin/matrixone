// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Portions of this file are additionally subject to the following
// copyright.
//
// Copyright (C) 2021 Matrix Origin.
//
// Modified the behavior and the interface of the step.

package operator

import (
	"fmt"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type ClusterState struct {
	LogState   pb.LogState
	TNState    pb.TNState
	CNState    pb.CNState
	ProxyState pb.ProxyState
}

type OpStep interface {
	fmt.Stringer

	IsFinish(state ClusterState) bool
}

type Replica struct {
	UUID      string
	ShardID   uint64
	ReplicaID uint64
	Epoch     uint64
}

type AddLogService struct {
	Target string
	Replica
}

func (a AddLogService) String() string {
	return fmt.Sprintf("adding %v:%v(at epoch %v) to %s", a.ShardID, a.ReplicaID, a.Epoch, a.UUID)
}

func (a AddLogService) IsFinish(state ClusterState) bool {
	if _, ok := state.LogState.Shards[a.ShardID]; !ok {
		return true
	}
	if _, ok := state.LogState.Shards[a.ShardID].Replicas[a.ReplicaID]; ok {
		return true
	}

	return false
}

type AddNonVotingLogService struct {
	Target string
	Replica
}

func (a AddNonVotingLogService) String() string {
	return fmt.Sprintf("adding non-voting %v:%v(at epoch %v) to %s",
		a.ShardID, a.ReplicaID, a.Epoch, a.UUID)
}

func (a AddNonVotingLogService) IsFinish(state ClusterState) bool {
	if _, ok := state.LogState.Shards[a.ShardID]; !ok {
		return true
	}
	if _, ok := state.LogState.Shards[a.ShardID].NonVotingReplicas[a.ReplicaID]; ok {
		return true
	}
	return false
}

type RemoveLogService struct {
	Target string
	Replica
}

func (a RemoveLogService) String() string {
	return fmt.Sprintf("removing %v:%v(at epoch %v) on log store %s", a.ShardID, a.ReplicaID, a.Epoch, a.UUID)
}

func (a RemoveLogService) IsFinish(state ClusterState) bool {
	if shard, ok := state.LogState.Shards[a.ShardID]; ok {
		if _, ok := shard.Replicas[a.ReplicaID]; ok {
			return false
		}
	}

	return true
}

type RemoveNonVotingLogService struct {
	Target string
	Replica
}

func (a RemoveNonVotingLogService) String() string {
	return fmt.Sprintf("removing non-voting %v:%v(at epoch %v) on log store %s",
		a.ShardID, a.ReplicaID, a.Epoch, a.UUID)
}

func (a RemoveNonVotingLogService) IsFinish(state ClusterState) bool {
	if shard, ok := state.LogState.Shards[a.ShardID]; ok {
		if _, ok := shard.NonVotingReplicas[a.ReplicaID]; ok {
			return false
		}
	}
	return true
}

type StartLogService struct {
	Replica
}

func (a StartLogService) String() string {
	return fmt.Sprintf("starting %v:%v on %s", a.ShardID, a.ReplicaID, a.UUID)
}

func (a StartLogService) IsFinish(state ClusterState) bool {
	if _, ok := state.LogState.Stores[a.UUID]; !ok {
		return true
	}
	for _, replicaInfo := range state.LogState.Stores[a.UUID].Replicas {
		if replicaInfo.ShardID == a.ShardID {
			return true
		}
	}

	return false
}

type StartNonVotingLogService struct {
	Replica
}

func (a StartNonVotingLogService) String() string {
	return fmt.Sprintf("starting non-voting %v:%v on %s", a.ShardID, a.ReplicaID, a.UUID)
}

func (a StartNonVotingLogService) IsFinish(state ClusterState) bool {
	if _, ok := state.LogState.Stores[a.UUID]; !ok {
		return true
	}
	for _, replicaInfo := range state.LogState.Stores[a.UUID].Replicas {
		if replicaInfo.ShardID == a.ShardID {
			return true
		}
	}
	return false
}

type StopLogService struct {
	Replica
}

func (a StopLogService) String() string {
	return fmt.Sprintf("stopping %v on %s", a.ShardID, a.UUID)
}

func (a StopLogService) IsFinish(state ClusterState) bool {
	if store, ok := state.LogState.Stores[a.UUID]; ok {
		for _, replicaInfo := range store.Replicas {
			if replicaInfo.ShardID == a.ShardID {
				return false
			}
		}
	}

	return true
}

type StopNonVotingLogService struct {
	Replica
}

func (a StopNonVotingLogService) String() string {
	return fmt.Sprintf("stopping %v on %s", a.ShardID, a.UUID)
}

func (a StopNonVotingLogService) IsFinish(state ClusterState) bool {
	if store, ok := state.LogState.Stores[a.UUID]; ok {
		for _, replicaInfo := range store.Replicas {
			if replicaInfo.ShardID == a.ShardID {
				return false
			}
		}
	}
	return true
}

type KillLogZombie struct {
	Replica
}

func (a KillLogZombie) String() string {
	return fmt.Sprintf("killing zombie on %s", a.UUID)
}

func (a KillLogZombie) IsFinish(state ClusterState) bool {
	if store, ok := state.LogState.Stores[a.UUID]; ok {
		for _, replicaInfo := range store.Replicas {
			if replicaInfo.ShardID == a.ShardID {
				return false
			}
		}
	}

	return true
}

type AddTnReplica struct {
	StoreID            string
	ShardID, ReplicaID uint64
	LogShardID         uint64
}

func (a AddTnReplica) String() string {
	return fmt.Sprintf("adding %v:%v to tn store %s (log shard %d)",
		a.ShardID, a.ReplicaID, a.StoreID, a.LogShardID,
	)
}

func (a AddTnReplica) IsFinish(state ClusterState) bool {
	for _, info := range state.TNState.Stores[a.StoreID].Shards {
		if a.ShardID == info.GetShardID() && a.ReplicaID == info.GetReplicaID() {
			return true
		}
	}
	return false
}

type RemoveTnReplica struct {
	StoreID            string
	ShardID, ReplicaID uint64
	LogShardID         uint64
}

func (a RemoveTnReplica) String() string {
	return fmt.Sprintf("removing %v:%v to tn store %s (log shard %d)",
		a.ShardID, a.ReplicaID, a.StoreID, a.LogShardID,
	)
}

func (a RemoveTnReplica) IsFinish(state ClusterState) bool {
	for _, info := range state.TNState.Stores[a.StoreID].Shards {
		if a.ShardID == info.GetShardID() && a.ReplicaID == info.GetReplicaID() {
			return false
		}
	}
	return true
}

// StopTnStore corresponds to tn store shutdown command.
type StopTnStore struct {
	StoreID string
}

func (a StopTnStore) String() string {
	return fmt.Sprintf("stopping tn store %s", a.StoreID)
}

func (a StopTnStore) IsFinish(state ClusterState) bool {
	if _, ok := state.TNState.Stores[a.StoreID]; ok {
		return false
	}
	return true
}

// StopLogStore corresponds to log store shutdown command.
type StopLogStore struct {
	StoreID string
}

func (a StopLogStore) String() string {
	return fmt.Sprintf("stopping log store %s", a.StoreID)
}

func (a StopLogStore) IsFinish(state ClusterState) bool {
	if _, ok := state.LogState.Stores[a.StoreID]; ok {
		return false
	}
	return true
}

type CreateTaskService struct {
	StoreID   string
	StoreType pb.ServiceType
	TaskUser  pb.TaskTableUser
}

func (a CreateTaskService) String() string {
	return fmt.Sprintf("create task service on %s(%s)", a.StoreID, a.StoreType)
}

func (a CreateTaskService) IsFinish(state ClusterState) bool {
	if state, ok := state.LogState.Stores[a.StoreID]; ok {
		return state.GetTaskServiceCreated()
	}

	if state, ok := state.TNState.Stores[a.StoreID]; ok {
		return state.GetTaskServiceCreated()
	}

	if state, ok := state.CNState.Stores[a.StoreID]; ok {
		return state.GetTaskServiceCreated()
	}

	return true
}

type DeleteCNStore struct {
	StoreID string
}

func (a DeleteCNStore) String() string {
	return fmt.Sprintf("deleting cn store %s", a.StoreID)
}

func (a DeleteCNStore) IsFinish(state ClusterState) bool {
	if _, ok := state.CNState.Stores[a.StoreID]; ok {
		return false
	}
	return true
}

type JoinGossipCluster struct {
	StoreID  string
	Existing []string
}

func (a JoinGossipCluster) String() string {
	return fmt.Sprintf("join gossip cluster for %s", a.StoreID)
}

func (a JoinGossipCluster) IsFinish(state ClusterState) bool {
	if state, ok := state.CNState.Stores[a.StoreID]; ok {
		return state.GetGossipJoined()
	}
	return true
}

type DeleteProxyStore struct {
	StoreID string
}

func (a DeleteProxyStore) String() string {
	return fmt.Sprintf("deleting proxy store %s", a.StoreID)
}

func (a DeleteProxyStore) IsFinish(state ClusterState) bool {
	if _, ok := state.ProxyState.Stores[a.StoreID]; ok {
		return false
	}
	return true
}

type AddLogShard struct {
	UUID    string
	ShardID uint64
}

func (a AddLogShard) String() string {
	return fmt.Sprintf("add shard %d on %s", a.ShardID, a.UUID)
}

func (a AddLogShard) IsFinish(state ClusterState) bool {
	_, ok := state.LogState.Shards[a.ShardID]
	return ok
}

type BootstrapShard struct {
	UUID           string
	ShardID        uint64
	ReplicaID      uint64
	InitialMembers map[uint64]string
	Join           bool
}

func (a BootstrapShard) String() string {
	return fmt.Sprintf("boostraping shard %d", a.ShardID)
}

func (a BootstrapShard) IsFinish(state ClusterState) bool {
	if _, ok := state.LogState.Stores[a.UUID]; !ok {
		return true
	}
	for _, replicaInfo := range state.LogState.Stores[a.UUID].Replicas {
		if replicaInfo.ShardID == a.ShardID {
			return true
		}
	}
	return false
}
