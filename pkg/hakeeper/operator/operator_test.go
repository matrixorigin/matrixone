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
// Modified the behavior of the operator.

package operator

import (
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
)

func TestOperatorStep(t *testing.T) {
	logState := pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: {
			ShardID:  1,
			Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			Epoch:    1,
		}},
		Stores: nil,
	}

	dnState := pb.DNState{}

	assert.False(t, AddLogService{StoreID: "d", ShardID: 1, ReplicaID: 4}.IsFinish(logState, dnState))
	assert.True(t, AddLogService{StoreID: "c", ShardID: 1, ReplicaID: 3}.IsFinish(logState, dnState))
	assert.False(t, RemoveLogService{StoreID: "c", ShardID: 1, ReplicaID: 3}.IsFinish(logState, dnState))
	assert.True(t, RemoveLogService{StoreID: "d", ShardID: 1, ReplicaID: 4}.IsFinish(logState, dnState))
}
