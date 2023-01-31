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
	return NewBuilder("", shardInfo).AddPeer(string(uuid), replicaID).Build()
}

func CreateRemoveReplica(uuid string, shardInfo pb.LogShardInfo) (*Operator, error) {
	return NewBuilder("", shardInfo).RemovePeer(string(uuid)).Build()
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

func CreateTaskServiceOp(brief, uuid string, serviceType pb.ServiceType, user pb.TaskTableUser) *Operator {
	return NewOperator(brief, 0, 0,
		CreateTaskService{StoreID: uuid, StoreType: serviceType, TaskUser: user},
	)
}

func CreateDeleteCNOp(brief, uuid string) *Operator {
	return NewOperator(brief, 0, 0, DeleteCNStore{StoreID: uuid})
}
