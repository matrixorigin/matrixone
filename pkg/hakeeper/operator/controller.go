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
// Modified the behavior of the operator controller.

package operator

import (
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// Controller is used to manage operators.
type Controller struct {
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

func (c *Controller) GetRemovingReplicas() map[uint64][]uint64 {
	removing := make(map[uint64][]uint64)
	for shardID, operators := range c.operators {
		for _, op := range operators {
			for _, step := range op.steps {
				switch step := step.(type) {
				case RemoveLogService:
					removing[shardID] = append(removing[shardID], step.ReplicaID)
				}
			}
		}
	}
	return removing
}

func (c *Controller) GetAddingReplicas() map[uint64][]uint64 {
	adding := make(map[uint64][]uint64)
	for shardID, operators := range c.operators {
		for _, op := range operators {
			for _, step := range op.steps {
				switch step := step.(type) {
				case AddLogService:
					adding[shardID] = append(adding[shardID], step.ReplicaID)
				}
			}
		}
	}
	return adding
}

func (c *Controller) RemoveFinishedOperator(logState pb.LogState, dnState pb.DNState) {
	for _, ops := range c.operators {
		for _, op := range ops {
			op.Check(logState, dnState)
			switch op.Status() {
			case SUCCESS, EXPIRED:
				c.RemoveOperator(op)
			}
		}
	}
}

func (c *Controller) Dispatch(ops []*Operator, logState pb.LogState, dnState pb.DNState) (commands []pb.ScheduleCommand) {
	for _, op := range ops {
		c.operators[op.shardID] = append(c.operators[op.shardID], op)
		step := op.Check(logState, dnState)
		var cmd pb.ScheduleCommand
		switch st := step.(type) {
		case AddLogService:
			cmd = pb.ScheduleCommand{
				UUID: st.Target,
				ConfigChange: &pb.ConfigChange{
					Replica: pb.Replica{
						UUID:      st.StoreID,
						ShardID:   st.ShardID,
						ReplicaID: st.ReplicaID,
						Epoch:     st.Epoch,
					},
					ChangeType: pb.AddReplica,
				},
				ServiceType: pb.LogService,
			}
		case RemoveLogService:
			cmd = pb.ScheduleCommand{
				UUID: st.Target,
				ConfigChange: &pb.ConfigChange{
					Replica: pb.Replica{
						UUID:      st.StoreID,
						ShardID:   st.ShardID,
						ReplicaID: st.ReplicaID,
						Epoch:     st.Epoch,
					},
					ChangeType: pb.RemoveReplica,
				},
				ServiceType: pb.LogService,
			}
		case StartLogService:
			cmd = pb.ScheduleCommand{
				UUID: st.StoreID,
				ConfigChange: &pb.ConfigChange{
					Replica: pb.Replica{
						UUID:      st.StoreID,
						ShardID:   st.ShardID,
						ReplicaID: st.ReplicaID,
					},
					ChangeType: pb.StartReplica,
				},
				ServiceType: pb.LogService,
			}
		case StopLogService:
			cmd = pb.ScheduleCommand{
				UUID: st.StoreID,
				ConfigChange: &pb.ConfigChange{
					Replica: pb.Replica{
						UUID:    st.StoreID,
						ShardID: st.ShardID,
					},
					ChangeType: pb.StopReplica,
				},
				ServiceType: pb.LogService,
			}
		case AddDnReplica:
			cmd = pb.ScheduleCommand{
				UUID: st.StoreID,
				ConfigChange: &pb.ConfigChange{
					Replica: pb.Replica{
						UUID:      st.StoreID,
						ShardID:   st.ShardID,
						ReplicaID: st.ReplicaID,
					},
					ChangeType: pb.AddReplica,
				},
				ServiceType: pb.DnService,
			}
		case RemoveDnReplica:
			cmd = pb.ScheduleCommand{
				UUID: st.StoreID,
				ConfigChange: &pb.ConfigChange{
					Replica: pb.Replica{
						UUID:      st.StoreID,
						ShardID:   st.ShardID,
						ReplicaID: st.ReplicaID,
					},
					ChangeType: pb.RemoveReplica,
				},
				ServiceType: pb.DnService,
			}
		case StopDnStore:
			cmd = pb.ScheduleCommand{
				UUID: st.StoreID,
				ShutdownStore: &pb.ShutdownStore{
					StoreID: st.StoreID,
				},
				ServiceType: pb.DnService,
			}
		case StopLogStore:
			cmd = pb.ScheduleCommand{
				UUID: st.StoreID,
				ShutdownStore: &pb.ShutdownStore{
					StoreID: st.StoreID,
				},
				ServiceType: pb.LogService,
			}
		}

		commands = append(commands, cmd)
	}

	return
}
