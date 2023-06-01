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
// Modified the behavior of the operator controller.

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// Controller is used to manage operators.
type Controller struct {
	// operators is a map from shardID to its operators.
	operators map[uint64][]*Operator
}

func NewController() *Controller {
	return &Controller{
		operators: make(map[uint64][]*Operator),
	}
}

// RemoveOperator removes an operator from the operators.
func (c *Controller) RemoveOperator(op *Operator) bool {
	removed := c.removeOperator(op)
	if removed {
		_ = op.Cancel()
	}
	return removed
}

func (c *Controller) removeOperator(op *Operator) bool {
	for i, curOp := range c.operators[op.shardID] {
		if curOp == op {
			c.operators[op.shardID] = append(c.operators[op.shardID][:i], c.operators[op.shardID][i+1:]...)
			if len(c.operators[op.shardID]) == 0 {
				delete(c.operators, op.shardID)
			}
			return true
		}
	}
	return false
}

// GetOperators gets operators from the given shard.
func (c *Controller) GetOperators(shardID uint64) []*Operator {
	return c.operators[shardID]
}

type ExecutingReplicas struct {
	Adding   map[uint64][]uint64
	Removing map[uint64][]uint64
	Starting map[uint64][]uint64
}

func (c *Controller) GetExecutingReplicas() ExecutingReplicas {
	executing := ExecutingReplicas{
		Adding:   make(map[uint64][]uint64),
		Removing: make(map[uint64][]uint64),
		Starting: make(map[uint64][]uint64),
	}
	for shardID, operators := range c.operators {
		for _, op := range operators {
			for _, step := range op.steps {
				switch step := step.(type) {
				case RemoveLogService:
					executing.Removing[shardID] = append(executing.Removing[shardID], step.ReplicaID)
				case AddLogService:
					executing.Adding[shardID] = append(executing.Adding[shardID], step.ReplicaID)
				case StartLogService:
					executing.Starting[shardID] = append(executing.Starting[shardID], step.ReplicaID)
				}
			}
		}
	}
	return executing
}

func (c *Controller) RemoveFinishedOperator(logState pb.LogState, dnState pb.DNState, cnState pb.CNState) {
	for _, ops := range c.operators {
		for _, op := range ops {
			op.Check(logState, dnState, cnState)
			switch op.Status() {
			case SUCCESS, EXPIRED:
				c.RemoveOperator(op)
			}
		}
	}
}

func (c *Controller) Dispatch(ops []*Operator, logState pb.LogState,
	dnState pb.DNState, cnState pb.CNState) (commands []pb.ScheduleCommand) {
	for _, op := range ops {
		c.operators[op.shardID] = append(c.operators[op.shardID], op)
		if step := op.Check(logState, dnState, cnState); step != nil {
			commands = append(commands, generateScheduleCommand(step))
		}
	}
	return
}

func generateScheduleCommand(step OpStep) pb.ScheduleCommand {
	switch st := step.(type) {
	case AddLogService:
		return addLogService(st)
	case RemoveLogService:
		return removeLogService(st)
	case StartLogService:
		return startLogService(st)
	case StopLogService:
		return stopLogService(st)
	case KillLogZombie:
		return killLogZombie(st)
	case AddDnReplica:
		return addDnReplica(st)
	case RemoveDnReplica:
		return removeDnReplica(st)
	case StopDnStore:
		return stopDnStore(st)
	case StopLogStore:
		return stopLogStore(st)
	case CreateTaskService:
		return createTaskService(st)
	case DeleteCNStore:
		return deleteCNStore(st)
	}
	panic("invalid schedule command")
}

func addLogService(st AddLogService) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID: st.Target,
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				UUID:      st.UUID,
				ShardID:   st.ShardID,
				ReplicaID: st.ReplicaID,
				Epoch:     st.Epoch,
			},
			ChangeType: pb.AddReplica,
		},
		ServiceType: pb.LogService,
	}
}

func removeLogService(st RemoveLogService) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID: st.Target,
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				UUID:      st.UUID,
				ShardID:   st.ShardID,
				ReplicaID: st.ReplicaID,
				Epoch:     st.Epoch,
			},
			ChangeType: pb.RemoveReplica,
		},
		ServiceType: pb.LogService,
	}
}

func startLogService(st StartLogService) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID: st.UUID,
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				UUID:      st.UUID,
				ShardID:   st.ShardID,
				ReplicaID: st.ReplicaID,
			},
			ChangeType: pb.StartReplica,
		},
		ServiceType: pb.LogService,
	}
}

func stopLogService(st StopLogService) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID: st.UUID,
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				UUID:    st.UUID,
				ShardID: st.ShardID,
				Epoch:   st.Epoch,
			},
			ChangeType: pb.RemoveReplica,
		},
		ServiceType: pb.LogService,
	}
}

func killLogZombie(st KillLogZombie) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID:          st.UUID,
		Bootstrapping: false,
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				UUID:      st.UUID,
				ShardID:   st.ShardID,
				ReplicaID: st.ReplicaID,
			},
			ChangeType: pb.KillZombie,
		},
		ServiceType: pb.LogService,
	}
}

func addDnReplica(st AddDnReplica) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID: st.StoreID,
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				UUID:       st.StoreID,
				ShardID:    st.ShardID,
				ReplicaID:  st.ReplicaID,
				LogShardID: st.LogShardID,
			},
			ChangeType: pb.StartReplica,
		},
		ServiceType: pb.DNService,
	}
}

func removeDnReplica(st RemoveDnReplica) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID: st.StoreID,
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				UUID:       st.StoreID,
				ShardID:    st.ShardID,
				ReplicaID:  st.ReplicaID,
				LogShardID: st.LogShardID,
			},
			ChangeType: pb.StopReplica,
		},
		ServiceType: pb.DNService,
	}
}

func stopDnStore(st StopDnStore) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID: st.StoreID,
		ShutdownStore: &pb.ShutdownStore{
			StoreID: st.StoreID,
		},
		ServiceType: pb.DNService,
	}
}

func stopLogStore(st StopLogStore) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID: st.StoreID,
		ShutdownStore: &pb.ShutdownStore{
			StoreID: st.StoreID,
		},
		ServiceType: pb.LogService,
	}
}

func createTaskService(st CreateTaskService) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID:        st.StoreID,
		ServiceType: st.StoreType,
		CreateTaskService: &pb.CreateTaskService{
			User:         st.TaskUser,
			TaskDatabase: catalog.MOTaskDB,
		},
	}
}

func deleteCNStore(st DeleteCNStore) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID:        st.StoreID,
		ServiceType: pb.CNService,
		DeleteCNStore: &pb.DeleteCNStore{
			StoreID: st.StoreID,
		},
	}
}
