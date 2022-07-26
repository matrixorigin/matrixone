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
// Modified the behavior of the builder.

package operator

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
)

func TestAddReplica(t *testing.T) {
	logShard := logservice.LogShardInfo{ShardID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}, Epoch: 1}
	assert.NoError(t, NewBuilder("", logShard).AddPeer("d", 4).err)
	assert.Error(t, NewBuilder("", logShard).AddPeer("", 4).err)
	assert.Error(t, NewBuilder("", logShard).AddPeer("c", 4).err)
	assert.Error(t, NewBuilder("", logShard).AddPeer("d", 3).err)
}

func TestRemoveReplica(t *testing.T) {
	logShard := logservice.LogShardInfo{ShardID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}, Epoch: 1}
	assert.NoError(t, NewBuilder("", logShard).RemovePeer("c").err)
	assert.Error(t, NewBuilder("", logShard).RemovePeer("d").err)
}

func TestAddBuild(t *testing.T) {
	logShard := logservice.LogShardInfo{ShardID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}, Epoch: 1}

	_, err := NewBuilder("", logShard).Build()
	assert.Error(t, err)

	_, err = NewBuilder("", logShard).AddPeer("", 4).Build()
	assert.Error(t, err)

	build, err := NewBuilder("", logShard).AddPeer("d", 4).Build()
	assert.NoError(t, err)
	assert.Equal(t, "add peer: store [d]", build.brief)
	assert.Equal(t, uint64(1), build.shardID)
	assert.Equal(t, uint64(1), build.epoch)
	assert.Equal(t, AddLogService{
		Target: "a",
		Replica: Replica{
			UUID:      "d",
			ShardID:   1,
			ReplicaID: 4,
			Epoch:     1,
		},
	}, build.steps[0])
}

func TestRemoveBuild(t *testing.T) {
	logShard := logservice.LogShardInfo{ShardID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}, Epoch: 1}

	_, err := NewBuilder("", logShard).Build()
	assert.Error(t, err)

	_, err = NewBuilder("", logShard).RemovePeer("").Build()
	assert.Error(t, err)

	_, err = NewBuilder("", logShard).RemovePeer("d").Build()
	assert.Error(t, err)

	build, err := NewBuilder("", logShard).RemovePeer("c").Build()
	assert.NoError(t, err)
	assert.Equal(t, "rm peer: store [c]", build.brief)
	assert.Equal(t, uint64(1), build.shardID)
	assert.Equal(t, uint64(1), build.epoch)
	assert.Equal(t, RemoveLogService{
		Target: "a",
		Replica: Replica{
			UUID:      "c",
			ShardID:   1,
			ReplicaID: 3,
			Epoch:     1,
		},
	}, build.steps[0])
}
