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

type OpStep interface {
	fmt.Stringer

	IsFinish(state pb.LogState, dnState pb.DNState, cnState pb.CNState) bool
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

func (a AddLogService) IsFinish(state pb.LogState, _ pb.DNState, _ pb.CNState) bool {
	if _, ok := state.Shards[a.ShardID]; !ok {
		return true
	}
	if _, ok := state.Shards[a.ShardID].Replicas[a.ReplicaID]; ok {
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

func (a RemoveLogService) IsFinish(state pb.LogState, _ pb.DNState, _ pb.CNState) bool {
	if shard, ok := state.Shards[a.ShardID]; ok {
		if _, ok := shard.Replicas[a.ReplicaID]; ok {
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

func (a StartLogService) IsFinish(state pb.LogState, _ pb.DNState, _ pb.CNState) bool {
	if _, ok := state.Stores[a.UUID]; !ok {
		return true
	}
	for _, replicaInfo := range state.Stores[a.UUID].Replicas {
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

func (a StopLogService) IsFinish(state pb.LogState, _ pb.DNState, _ pb.CNState) bool {
	if store, ok := state.Stores[a.UUID]; ok {
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

func (a KillLogZombie) IsFinish(state pb.LogState, _ pb.DNState, _ pb.CNState) bool {
	if store, ok := state.Stores[a.UUID]; ok {
		for _, replicaInfo := range store.Replicas {
			if replicaInfo.ShardID == a.ShardID {
				return false
			}
		}
	}

	return true
}

type AddDnReplica struct {
	StoreID            string
	ShardID, ReplicaID uint64
	LogShardID         uint64
}

func (a AddDnReplica) String() string {
	return fmt.Sprintf("adding %v:%v to dn store %s (log shard %d)",
		a.ShardID, a.ReplicaID, a.StoreID, a.LogShardID,
	)
}

func (a AddDnReplica) IsFinish(_ pb.LogState, state pb.DNState, _ pb.CNState) bool {
	for _, info := range state.Stores[a.StoreID].Shards {
		if a.ShardID == info.GetShardID() && a.ReplicaID == info.GetReplicaID() {
			return true
		}
	}
	return false
}

type RemoveDnReplica struct {
	StoreID            string
	ShardID, ReplicaID uint64
	LogShardID         uint64
}

func (a RemoveDnReplica) String() string {
	return fmt.Sprintf("removing %v:%v to dn store %s (log shard %d)",
		a.ShardID, a.ReplicaID, a.StoreID, a.LogShardID,
	)
}

func (a RemoveDnReplica) IsFinish(_ pb.LogState, state pb.DNState, _ pb.CNState) bool {
	for _, info := range state.Stores[a.StoreID].Shards {
		if a.ShardID == info.GetShardID() && a.ReplicaID == info.GetReplicaID() {
			return false
		}
	}
	return true
}

// StopDnStore corresponds to dn store shutdown command.
type StopDnStore struct {
	StoreID string
}

func (a StopDnStore) String() string {
	return fmt.Sprintf("stopping dn store %s", a.StoreID)
}

func (a StopDnStore) IsFinish(_ pb.LogState, state pb.DNState, _ pb.CNState) bool {
	if _, ok := state.Stores[a.StoreID]; ok {
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

func (a StopLogStore) IsFinish(state pb.LogState, _ pb.DNState, _ pb.CNState) bool {
	if _, ok := state.Stores[a.StoreID]; ok {
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

func (a CreateTaskService) IsFinish(logState pb.LogState, dnState pb.DNState, cnState pb.CNState) bool {
	if state, ok := logState.Stores[a.StoreID]; ok {
		return state.GetTaskServiceCreated()
	}

	if state, ok := dnState.Stores[a.StoreID]; ok {
		return state.GetTaskServiceCreated()
	}

	if state, ok := cnState.Stores[a.StoreID]; ok {
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

func (a DeleteCNStore) IsFinish(_ pb.LogState, _ pb.DNState, state pb.CNState) bool {
	if _, ok := state.Stores[a.StoreID]; ok {
		return false
	}
	return true
}
