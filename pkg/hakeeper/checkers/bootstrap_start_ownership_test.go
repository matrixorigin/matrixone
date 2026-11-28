// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkers

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestBootstrapOwnsUnreportedReplicaStart(t *testing.T) {
	c := NewCoordinator("", hakeeper.Config{})
	alloc := util.NewTestIDAllocator(100)
	state := pb.CheckerState{Tick: 1,
		ClusterInfo: pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}},
		LogState: pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: {ShardID: 1, Replicas: map[uint64]string{}}}, Stores: map[string]pb.LogStoreInfo{
			"a": {Tick: 1, TaskServiceCreated: true}, "b": {Tick: 1, TaskServiceCreated: true}, "c": {Tick: 1, TaskServiceCreated: true},
		}},
	}
	first := c.Check(alloc, state, false)
	var bootstrap []pb.ScheduleCommand
	for _, cmd := range first {
		if cmd.BootstrapShard != nil {
			bootstrap = append(bootstrap, cmd)
		}
	}
	require.Len(t, bootstrap, 3, "the first real scheduling round must bootstrap all replicas")
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, Replicas: bootstrap[0].BootstrapShard.InitialMembers}
	state.LogState.Shards[1] = shard
	var missing pb.ScheduleCommand
	for i, cmd := range bootstrap {
		if i == 0 {
			missing = cmd
			continue
		}
		store := state.LogState.Stores[cmd.UUID]
		store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: cmd.BootstrapShard.ReplicaID}}
		state.LogState.Stores[cmd.UUID] = store
	}
	second := c.Check(alloc, state, false)
	t.Logf("unreported bootstrap store=%s id=%d commands=%+v", missing.UUID, missing.BootstrapShard.ReplicaID, second)
	for _, cmd := range second {
		if cmd.ConfigChange != nil && cmd.ConfigChange.Replica.ReplicaID == missing.BootstrapShard.ReplicaID {
			require.NotEqual(t, pb.StartReplica, cmd.ConfigChange.ChangeType, "an in-flight bootstrap already owns this local start")
			require.NotEqual(t, pb.RemoveReplica, cmd.ConfigChange.ChangeType)
		}
	}
}

func TestFreshCoordinatorReplansMembershipAndIndependentTNWork(t *testing.T) {
	// Initialize and restore the TN checker's service runtime around this test.
	_, _, _ = tnStartScenario(t)
	cfg := hakeeper.Config{TickPerSecond: 1, LogStoreTimeout: 5 * time.Second}
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
	state := pb.CheckerState{
		Tick:          10,
		ClusterInfo:   pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}},
		TaskTableUser: pb.TaskTableUser{Username: "task"},
		LogState: pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{
			"a": {Tick: 10, TaskServiceCreated: true, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
			"b": {Tick: 1, TaskServiceCreated: true, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}},
			"c": {Tick: 10, TaskServiceCreated: true, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}},
		}},
		TNState: pb.TNState{Stores: map[string]pb.TNStoreInfo{"tn": {Tick: 10}}},
	}
	check := func() (bool, bool) {
		commands := NewCoordinator("", cfg).Check(util.NewTestIDAllocator(100), state, false)
		var remove, tnTask bool
		for _, cmd := range commands {
			if cmd.ConfigChange != nil && cmd.ConfigChange.ChangeType == pb.RemoveReplica && cmd.ConfigChange.Replica.UUID == "b" {
				remove = true
			}
			if cmd.UUID == "tn" && cmd.CreateTaskService != nil {
				tnTask = true
			}
		}
		return remove, tnTask
	}
	remove, tnTask := check()
	require.True(t, remove, "the current plan should remove the expired voter")
	require.True(t, tnTask, "unrelated TN work must remain in the new batch")
	b := state.LogState.Stores["b"]
	b.Tick = state.Tick
	state.LogState.Stores["b"] = b
	remove, tnTask = check()
	require.False(t, remove, "a recovered voter is no longer a removal candidate")
	require.True(t, tnTask)
}
