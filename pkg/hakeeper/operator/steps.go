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
// Copyright (C) 2021 MatrixOrigin.
//
// Modified the behavior and the interface of the step.

package operator

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
)

type OpStep interface {
	fmt.Stringer

	IsFinish(state hakeeper.LogState, dnState hakeeper.DNState) bool
}

type AddLogService struct {
	UUID                      string
	ShardID, ReplicaID, Epoch uint64

	tick uint64
}

func (a AddLogService) String() string {
	return fmt.Sprintf("adding %v:%v(at epoch %v) to %s", a.ShardID, a.ReplicaID, a.Epoch, a.UUID)
}

func (a AddLogService) IsFinish(state hakeeper.LogState, _ hakeeper.DNState) bool {
	if _, ok := state.Shards[a.ShardID].Replicas[a.ReplicaID]; ok {
		return true
	}

	return false
}

type RemoveLogService struct {
	UUID               string
	ShardID, ReplicaID uint64
}

func (a RemoveLogService) String() string {
	return fmt.Sprintf("removing %v:%v to %s", a.ShardID, a.ReplicaID, a.UUID)
}

func (a RemoveLogService) IsFinish(state hakeeper.LogState, _ hakeeper.DNState) bool {
	if _, ok := state.Shards[a.ShardID].Replicas[a.ReplicaID]; !ok {
		return true
	}

	return false
}

type StartLogService struct {
	UUID               string
	ShardID, ReplicaID uint64
}

func (a StartLogService) String() string {
	return fmt.Sprintf("starting %v:%v on %s", a.ShardID, a.ReplicaID, a.UUID)
}

func (a StartLogService) IsFinish(state hakeeper.LogState, _ hakeeper.DNState) bool {
	// FIXME: state.Stores[a.UUID] is going to return nil when a.UUID is not a
	// key in state.Stores
	for _, replicaInfo := range state.Stores[a.UUID].Replicas {
		if replicaInfo.ShardID == a.ShardID {
			return true
		}
	}

	return false
}

type StopLogService struct {
	UUID    string
	ShardID uint64
}

func (a StopLogService) String() string {
	return fmt.Sprintf("starting %v on %s", a.ShardID, a.UUID)
}

func (a StopLogService) IsFinish(state hakeeper.LogState, _ hakeeper.DNState) bool {
	// FIXME: state.Stores[a.UUID] is going to return nil when a.UUID is not a
	// key in state.Stores
	for _, replicaInfo := range state.Stores[a.UUID].Replicas {
		if replicaInfo.ShardID == a.ShardID {
			return false
		}
	}

	return true
}
