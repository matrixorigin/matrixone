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
// Modified the behavior of the operator.

package operator

import (
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
)

func TestHasStarted(t *testing.T) {
	op := NewOperator("", 1, 1)
	assert.Equal(t, true, op.HasStarted())
}

func TestCheckSuccess(t *testing.T) {
	op := NewOperator("", 1, 1, AddLogService{}, RemoveLogService{})
	assert.Equal(t, false, op.CheckSuccess())

	op.currentStep = 1
	assert.Equal(t, false, op.CheckSuccess())

	op.currentStep = 2
	assert.Equal(t, true, op.CheckSuccess())
}

func TestCheck(t *testing.T) {
	op := NewOperator("", 1, 1,
		AddLogService{"a", Replica{"d", 1, 4, 1}},
		RemoveLogService{"a", Replica{"c", 1, 3, 1}})

	logState := pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: {
			ShardID:  1,
			Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			Epoch:    0,
		}},
	}
	currentStep := op.Check(logState, pb.DNState{}, pb.CNState{})

	assert.Equal(t,
		AddLogService{"a", Replica{"d", 1, 4, 1}},
		currentStep)
	assert.NotEqual(t, SUCCESS, op.Status())

	logState = pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: {
			ShardID:  1,
			Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
			Epoch:    0,
		}},
	}
	currentStep = op.Check(logState, pb.DNState{}, pb.CNState{})

	assert.Equal(t,
		RemoveLogService{"a", Replica{"c", 1, 3, 1}},
		currentStep)
	assert.NotEqual(t, SUCCESS, op.Status())

	logState = pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: {
			ShardID:  1,
			Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
			Epoch:    0,
		}},
	}
	currentStep = op.Check(logState, pb.DNState{}, pb.CNState{})

	assert.Equal(t, nil, currentStep)
	assert.Equal(t, SUCCESS, op.Status())

	assert.Equal(t, nil, op.Check(pb.LogState{}, pb.DNState{}, pb.CNState{}))
}
