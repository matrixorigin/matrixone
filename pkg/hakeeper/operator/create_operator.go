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
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func CreateAddReplica(uuid string, shardInfo pb.LogShardInfo, replicaID uint64) (*Operator, error) {
	return NewBuilder("", shardInfo).AddPeer(uuid, replicaID).Build()
}

func CreateAddNonVotingReplica(uuid string, shardInfo pb.LogShardInfo, replicaID uint64) (*Operator, error) {
	return NewBuilder("", shardInfo).AddNonVotingPeer(uuid, replicaID).Build()
}

func CreateRemoveReplica(uuid string, shardInfo pb.LogShardInfo) (*Operator, error) {
	return NewBuilder("", shardInfo).RemovePeer(uuid).Build()
}

func CreateRemoveNonVotingReplica(uuid string, shardInfo pb.LogShardInfo) (*Operator, error) {
	return NewBuilder("", shardInfo).RemoveNonVotingPeer(uuid).Build()
}

func CreateStopReplica(brief string, uuid string, shardID uint64, epoch uint64) *Operator {
	return NewOperator(brief, shardID, 0,
		StopLogService{Replica{uuid, shardID, 0, epoch}})
}

func CreateKillZombie(brief, uuid string, shardID, replicaID uint64) *Operator {
	return NewOperator(brief, shardID, 0,
		KillLogZombie{Replica{UUID: uuid, ShardID: shardID, ReplicaID: replicaID}})
}

func CreateStartReplica(brief, uuid string, shardID, replicaID uint64) *Operator {
	return NewOperator(brief, shardID, 0,
		StartLogService{Replica{UUID: uuid, ShardID: shardID, ReplicaID: replicaID}})
}

func CreateBootstrapShard(
	brief, uuid string, shardID, replicaID uint64, initialMembers map[uint64]string,
) *Operator {
	return NewOperator(
		brief,
		shardID,
		0,
		BootstrapShard{
			UUID:           uuid,
			ShardID:        shardID,
			ReplicaID:      replicaID,
			InitialMembers: initialMembers,
			Join:           false,
		},
	)
}

func CreateStartNonVotingReplica(brief, uuid string, shardID, replicaID uint64) *Operator {
	return NewOperator(brief, shardID, 0,
		StartNonVotingLogService{Replica{UUID: uuid, ShardID: shardID, ReplicaID: replicaID}})
}

func CreateTaskServiceOp(brief, uuid string, serviceType pb.ServiceType, user pb.TaskTableUser) *Operator {
	return NewOperator(brief, 0, 0,
		CreateTaskService{StoreID: uuid, StoreType: serviceType, TaskUser: user},
	)
}

func CreateDeleteCNOp(brief, uuid string) *Operator {
	return NewOperator(brief, 0, 0, DeleteCNStore{StoreID: uuid})
}

func JoinGossipClusterOp(brief, uuid string, existing []string) *Operator {
	return NewOperator(brief, 0, 0,
		JoinGossipCluster{StoreID: uuid, Existing: existing},
	)
}

func CreateDeleteProxyOp(brief, uuid string) *Operator {
	return NewOperator(brief, 0, 0, DeleteProxyStore{StoreID: uuid})
}

func CreateAddShardOp(brief string, uuid string, shardID uint64) *Operator {
	return NewOperator(
		brief,
		shardID,
		0,
		AddLogShard{
			UUID:    uuid,
			ShardID: shardID,
		},
	)
}
