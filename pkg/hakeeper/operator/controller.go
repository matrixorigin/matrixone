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
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"sync"
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
	curOps := c.operators[op.shardID]
	for _, curOp := range curOps {
		if curOp == op {
			delete(c.operators, op.shardID)
		}
		return true
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

func (c *Controller) RemoveFinishedOperator(dnState hakeeper.DNState, state hakeeper.LogState) {
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
