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
	"github.com/stretchr/testify/require"
)

func TestAddReplica(t *testing.T) {
	logShard := logservice.LogShardInfo{ShardID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}, Epoch: 1}
	assert.NoError(t, NewBuilder("", logShard).AddPeer("d", 4).err)
	assert.Error(t, NewBuilder("", logShard).AddPeer("", 4).err)
	assert.Error(t, NewBuilder("", logShard).AddPeer("c", 4).err)
	assert.Error(t, NewBuilder("", logShard).AddPeer("d", 3).err)
}

func TestAddNonVotingReplica(t *testing.T) {
	logShard := logservice.LogShardInfo{
		ShardID: 1,
		Replicas: map[uint64]string{
			1: "a",
			2: "b",
			3: "c",
			4: "d",
			5: "e",
			6: "f",
		},
		NonVotingReplicas: map[uint64]string{
			4: "d",
			5: "e",
			6: "f",
		},
		Epoch: 1,
	}
	assert.NoError(t, NewBuilder("", logShard).AddNonVotingPeer("g", 7).err)
	assert.Error(t, NewBuilder("", logShard).AddNonVotingPeer("", 7).err)
	assert.Error(t, NewBuilder("", logShard).AddNonVotingPeer("f", 6).err)
	assert.Error(t, NewBuilder("", logShard).AddNonVotingPeer("g", 6).err)
}

func TestRemoveReplica(t *testing.T) {
	logShard := logservice.LogShardInfo{ShardID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}, Epoch: 1}
	assert.NoError(t, NewBuilder("", logShard).RemovePeer("c").err)
	assert.Error(t, NewBuilder("", logShard).RemovePeer("d").err)
}

func TestRemoveNonVotingReplica(t *testing.T) {
	logShard := logservice.LogShardInfo{
		ShardID: 1,
		Replicas: map[uint64]string{
			1: "a",
			2: "b",
			3: "c",
			4: "d",
			5: "e",
			6: "f",
		},
		NonVotingReplicas: map[uint64]string{
			4: "d",
			5: "e",
			6: "f",
		},
		Epoch: 1,
	}
	assert.NoError(t, NewBuilder("", logShard).RemoveNonVotingPeer("f").err)
	assert.Error(t, NewBuilder("", logShard).RemoveNonVotingPeer("g").err)
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

func TestAddNonVotingBuild(t *testing.T) {
	logShard := logservice.LogShardInfo{
		ShardID: 1,
		Replicas: map[uint64]string{
			1: "a",
			2: "b",
			3: "c",
			4: "d",
			5: "e",
			6: "f",
		},
		NonVotingReplicas: map[uint64]string{
			4: "d",
			5: "e",
			6: "f",
		},
		Epoch: 1,
	}
	_, err := NewBuilder("", logShard).Build()
	assert.Error(t, err)

	_, err = NewBuilder("", logShard).AddNonVotingPeer("", 7).Build()
	assert.Error(t, err)

	build, err := NewBuilder("", logShard).AddNonVotingPeer("g", 7).Build()
	assert.NoError(t, err)
	assert.Equal(t, "add non-voting peer: store [g]", build.brief)
	assert.Equal(t, uint64(1), build.shardID)
	assert.Equal(t, uint64(1), build.epoch)
	assert.Equal(t, AddNonVotingLogService{
		Target: "a",
		Replica: Replica{
			UUID:      "g",
			ShardID:   1,
			ReplicaID: 7,
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

func TestRemoveNonVotingBuild(t *testing.T) {
	logShard := logservice.LogShardInfo{
		ShardID: 1,
		Replicas: map[uint64]string{
			1: "a",
			2: "b",
			3: "c",
			4: "d",
			5: "e",
			6: "f",
		},
		NonVotingReplicas: map[uint64]string{
			4: "d",
			5: "e",
			6: "f",
		},
		Epoch: 1,
	}

	_, err := NewBuilder("", logShard).Build()
	assert.Error(t, err)

	_, err = NewBuilder("", logShard).RemoveNonVotingPeer("").Build()
	assert.Error(t, err)

	_, err = NewBuilder("", logShard).RemoveNonVotingPeer("g").Build()
	assert.Error(t, err)

	build, err := NewBuilder("", logShard).RemoveNonVotingPeer("f").Build()
	assert.NoError(t, err)
	assert.Equal(t, "rm non-voting peer: store [f]", build.brief)
	assert.Equal(t, uint64(1), build.shardID)
	assert.Equal(t, uint64(1), build.epoch)
	assert.Equal(t, RemoveNonVotingLogService{
		Target: "a",
		Replica: Replica{
			UUID:      "f",
			ShardID:   1,
			ReplicaID: 6,
			Epoch:     1,
		},
	}, build.steps[0])
}

func TestBuildTargetSelection(t *testing.T) {
	assertCommandTargets := func(t *testing.T, op *Operator, expected []string) {
		t.Helper()
		require.Len(t, op.steps, len(expected))
		for i, step := range op.steps {
			assert.Equal(t, expected[i], generateScheduleCommand(step).UUID)
		}
	}

	t.Run("last voting replica has no safe target", func(t *testing.T) {
		logShard := logservice.LogShardInfo{
			ShardID:  1,
			Replicas: map[uint64]string{1: "a"},
			Epoch:    1,
		}

		op, err := NewBuilder("", logShard).RemovePeer("a").Build()
		require.ErrorContains(t, err, "without a retained voting peer")
		assert.Nil(t, op)
	})

	t.Run("non-voting replica has no safe target without voting peers", func(t *testing.T) {
		logShard := logservice.LogShardInfo{
			ShardID:           1,
			NonVotingReplicas: map[uint64]string{2: "b"},
			Epoch:             1,
		}

		op, err := NewBuilder("", logShard).RemoveNonVotingPeer("b").Build()
		require.ErrorContains(t, err, "without a retained voting peer")
		assert.Nil(t, op)
	})

	t.Run("mixed voting change targets the retained peer for every step", func(t *testing.T) {
		logShard := logservice.LogShardInfo{
			ShardID:  1,
			Replicas: map[uint64]string{1: "a", 2: "c"},
			Epoch:    1,
		}

		op, err := NewBuilder("", logShard).
			RemovePeer("a").
			AddPeer("b", 3).
			Build()
		require.NoError(t, err)
		assert.Equal(t, []OpStep{
			RemoveLogService{
				Target:  "c",
				Replica: Replica{UUID: "a", ShardID: 1, ReplicaID: 1, Epoch: 1},
			},
			AddLogService{
				Target:  "c",
				Replica: Replica{UUID: "b", ShardID: 1, ReplicaID: 3, Epoch: 1},
			},
		}, op.steps)
		assertCommandTargets(t, op, []string{"c", "c"})
	})

	t.Run("mixed non-voting add targets the retained peer for every step", func(t *testing.T) {
		logShard := logservice.LogShardInfo{
			ShardID:  1,
			Replicas: map[uint64]string{1: "a", 2: "c"},
			Epoch:    1,
		}

		op, err := NewBuilder("", logShard).
			RemovePeer("a").
			AddNonVotingPeer("b", 3).
			Build()
		require.NoError(t, err)
		assert.Equal(t, []OpStep{
			RemoveLogService{
				Target:  "c",
				Replica: Replica{UUID: "a", ShardID: 1, ReplicaID: 1, Epoch: 1},
			},
			AddNonVotingLogService{
				Target:  "c",
				Replica: Replica{UUID: "b", ShardID: 1, ReplicaID: 3, Epoch: 1},
			},
		}, op.steps)
		assertCommandTargets(t, op, []string{"c", "c"})
	})

	t.Run("mixed change with only a newly added voting peer has no safe target", func(t *testing.T) {
		logShard := logservice.LogShardInfo{
			ShardID:  1,
			Replicas: map[uint64]string{1: "a"},
			Epoch:    1,
		}

		op, err := NewBuilder("", logShard).
			RemovePeer("a").
			AddPeer("b", 2).
			Build()
		require.ErrorContains(t, err, "without a retained voting peer")
		assert.Nil(t, op)
	})

	t.Run("voting add without an existing voting peer has no safe target", func(t *testing.T) {
		logShard := logservice.LogShardInfo{ShardID: 1, Epoch: 1}

		op, err := NewBuilder("", logShard).AddPeer("a", 1).Build()
		require.ErrorContains(t, err, "without a retained voting peer")
		assert.Nil(t, op)
	})

	t.Run("non-voting add without an existing voting peer has no safe target", func(t *testing.T) {
		logShard := logservice.LogShardInfo{ShardID: 1, Epoch: 1}

		op, err := NewBuilder("", logShard).AddNonVotingPeer("a", 1).Build()
		require.ErrorContains(t, err, "without a retained voting peer")
		assert.Nil(t, op)
	})
}
