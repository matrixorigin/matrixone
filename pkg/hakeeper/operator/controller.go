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
	"sync"

	hapb "github.com/matrixorigin/matrixone/pkg/pb/hakeeper"
)

// Controller is used to manage operators.
type Controller struct {
	sync.RWMutex
	operators map[uint64][]*Operator
}

func NewController() *Controller {
	return &Controller{
		operators: make(map[uint64][]*Operator),
	}
}

// RemoveOperator removes an operator from the operators.
func (c *Controller) RemoveOperator(op *Operator) bool {
	c.Lock()
	removed := c.removeOperatorLocked(op)
	c.Unlock()
	if removed {
		_ = op.Cancel()
	}
	return removed
}

func (c *Controller) removeOperatorLocked(op *Operator) bool {
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
	c.RLock()
	defer c.RUnlock()

	return c.operators[shardID]
}

func (c *Controller) GetRemovingReplicas() (removing map[uint64][]uint64) {
	for shardID, operators := range c.operators {
		for _, op := range operators {
			for _, step := range op.steps {
				switch step.(type) {
				case RemoveLogService:
					removing[shardID] = append(removing[shardID], step.(RemoveLogService).ReplicaID)
				}
			}
		}
	}
	return
}

func (c *Controller) GetAddingReplicas() (adding map[uint64][]uint64) {
	for shardID, operators := range c.operators {
		for _, op := range operators {
			for _, step := range op.steps {
				switch step.(type) {
				case AddLogService:
					adding[shardID] = append(adding[shardID], step.(AddLogService).ReplicaID)
				}
			}
		}
	}
	return
}

func (c *Controller) RemoveFinishedOperator(dnState hapb.DNState, state hapb.LogState) {
	for _, ops := range c.operators {
		for _, op := range ops {
			op.Check(state, dnState)
			switch op.Status() {
			case SUCCESS, EXPIRED:
				c.RemoveOperator(op)
			}
		}
	}
}

func (c *Controller) Dispatch(ops []*Operator, logState hapb.LogState, dnState hapb.DNState) (commands []hapb.ScheduleCommand) {
	for _, op := range ops {
		c.operators[op.shardID] = append(c.operators[op.shardID], op)
		step := op.Check(logState, dnState)
		var cmd hapb.ScheduleCommand
		switch st := step.(type) {
		case AddLogService:
			cmd = hapb.ScheduleCommand{
				UUID: st.Target,
				ConfigChange: &hapb.ConfigChange{
					Replica: hapb.Replica{
						UUID:      st.StoreID,
						ShardID:   st.ShardID,
						ReplicaID: st.ReplicaID,
						Epoch:     st.Epoch,
					},
					ChangeType: hapb.AddReplica,
				},
				ServiceType: hapb.LogService,
			}
		case RemoveLogService:
			cmd = hapb.ScheduleCommand{
				UUID: st.Target,
				ConfigChange: &hapb.ConfigChange{
					Replica: hapb.Replica{
						UUID:      st.StoreID,
						ShardID:   st.ShardID,
						ReplicaID: st.ReplicaID,
					},
					ChangeType: hapb.RemoveReplica,
				},
				ServiceType: hapb.LogService,
			}
		case StartLogService:
			cmd = hapb.ScheduleCommand{
				UUID: st.StoreID,
				ConfigChange: &hapb.ConfigChange{
					Replica: hapb.Replica{
						UUID:      st.StoreID,
						ShardID:   st.ShardID,
						ReplicaID: st.ReplicaID,
					},
					ChangeType: hapb.StartReplica,
				},
				ServiceType: hapb.LogService,
			}
		case StopLogService:
			cmd = hapb.ScheduleCommand{
				UUID: st.StoreID,
				ConfigChange: &hapb.ConfigChange{
					Replica: hapb.Replica{
						UUID:    st.StoreID,
						ShardID: st.ShardID,
					},
					ChangeType: hapb.StopReplica,
				},
				ServiceType: hapb.LogService,
			}
		case AddDnReplica:
			cmd = hapb.ScheduleCommand{
				UUID: st.StoreID,
				ConfigChange: &hapb.ConfigChange{
					Replica: hapb.Replica{
						UUID:      st.StoreID,
						ShardID:   st.ShardID,
						ReplicaID: st.ReplicaID,
					},
					ChangeType: hapb.AddReplica,
				},
				ServiceType: hapb.DnService,
			}
		case RemoveDnReplica:
			cmd = hapb.ScheduleCommand{
				UUID: st.StoreID,
				ConfigChange: &hapb.ConfigChange{
					Replica: hapb.Replica{
						UUID:      st.StoreID,
						ShardID:   st.ShardID,
						ReplicaID: st.ReplicaID,
					},
					ChangeType: hapb.RemoveReplica,
				},
				ServiceType: hapb.DnService,
			}
		case StopDnStore:
			cmd = hapb.ScheduleCommand{
				UUID: st.StoreID,
				ShutdownStore: &hapb.ShutdownStore{
					StoreID: st.StoreID,
				},
				ServiceType: hapb.DnService,
			}
		case StopLogStore:
			cmd = hapb.ScheduleCommand{
				UUID: st.StoreID,
				ShutdownStore: &hapb.ShutdownStore{
					StoreID: st.StoreID,
				},
				ServiceType: hapb.LogService,
			}
		}

		commands = append(commands, cmd)
	}

	return
}
