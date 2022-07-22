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
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func CreateAddReplica(uuid util.StoreID, shardInfo logservice.LogShardInfo, replicaID uint64) (*Operator, error) {
	return NewBuilder("", shardInfo).AddPeer(string(uuid), replicaID).Build()
}

func CreateRemoveReplica(uuid util.StoreID, shardInfo logservice.LogShardInfo) (*Operator, error) {
	return NewBuilder("", shardInfo).RemovePeer(string(uuid)).Build()
}

func CreateStopReplica(brief string, uuid util.StoreID, shardID uint64, epoch uint64) *Operator {
	return NewOperator(brief, shardID, 0,
		StopLogService{string(uuid), shardID, epoch})
}

func CreateStartReplica(brief string, uuid util.StoreID, shardID, replicaID uint64) *Operator {
	return NewOperator(brief, shardID, 0,
		StartLogService{StoreID: string(uuid), ShardID: shardID, ReplicaID: replicaID})
}
