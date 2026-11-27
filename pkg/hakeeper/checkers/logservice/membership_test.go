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

package logservice

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestLogMembershipTargetsRequireReportedCurrentAndProjectedMajority(t *testing.T) {
	cfg := hakeeper.Config{}
	cfg.Fill()
	const tick = uint64(10000)
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 5, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}}
	stores := make(map[string]pb.LogStoreInfo)
	for id, uuid := range shard.Replicas {
		stores[uuid] = pb.LogStoreInfo{Tick: tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}}
	}
	stores["f"] = pb.LogStoreInfo{Tick: tick}
	state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: stores}
	expired := stores["a"]
	expired.Tick = 0
	stores["a"] = expired
	expired = stores["b"]
	expired.Tick = 0
	stores["b"] = expired
	remove := pb.Replica{ShardID: 1, ReplicaID: 1, UUID: "a", Epoch: 5}
	require.Equal(t, []string{"c", "d", "e"}, membershipTargets(cfg, state, tick, pb.RemoveReplica, remove),
		"the executor must be a reporting retained voter, not the first configured UUID")
	remove.ReplicaID, remove.UUID = 3, "c"
	require.Empty(t, membershipTargets(cfg, state, tick, pb.RemoveReplica, remove),
		"removing a live voter would leave less than the projected majority")
	add := pb.Replica{ShardID: 1, ReplicaID: 6, UUID: "f", Epoch: 5}
	require.Empty(t, membershipTargets(cfg, state, tick, pb.AddReplica, add),
		"an unstarted sixth voter raises the majority")
	require.Equal(t, []string{"c", "d", "e"}, membershipTargets(cfg, state, tick, pb.AddNonVotingReplica, add))

	// A live store reporting another replica, role, or disk generation cannot
	// be counted as an available voting member.
	c := stores["c"]
	c.Replicas[0].ReplicaID = 99
	stores["c"] = c
	remove.ReplicaID, remove.UUID = 1, "a"
	require.Empty(t, membershipTargets(cfg, state, tick, pb.RemoveReplica, remove))
	require.Empty(t, membershipTargets(cfg, state, tick, pb.AddNonVotingReplica, add))
	c.Replicas[0].ReplicaID = 3
	c.Replicas[0].IsNonVoting = true
	stores["c"] = c
	require.Empty(t, membershipTargets(cfg, state, tick, pb.RemoveReplica, remove))
	c.Replicas[0].IsNonVoting = false
	c.StoreIncarnation = "new"
	stores["c"] = c
	shard.ReplicaStoreIncarnations = map[uint64]string{3: "old"}
	state.Shards[1] = shard
	require.Empty(t, membershipTargets(cfg, state, tick, pb.RemoveReplica, remove))
	c.StoreIncarnation = "old"
	stores["c"] = c
	require.Equal(t, []string{"c", "d", "e"}, membershipTargets(cfg, state, tick, pb.RemoveReplica, remove))
	remove.Epoch--
	require.Empty(t, membershipTargets(cfg, state, tick, pb.RemoveReplica, remove))
}

func TestHAKeeperMembershipTargetsAllowStableReplacement(t *testing.T) {
	cfg := hakeeper.Config{}
	cfg.Fill()
	const tick = uint64(10000)
	shard := pb.LogShardInfo{ShardID: 0, Epoch: 5, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
	stores := map[string]pb.LogStoreInfo{
		"a": {Tick: tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
		"b": {Tick: tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}},
		"c": {Tick: 0, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}},
		"d": {Tick: tick},
	}
	state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{0: shard}, Stores: stores}
	remove := pb.Replica{ShardID: 0, ReplicaID: 3, UUID: "c", Epoch: 5}
	require.Equal(t, []string{"a", "b"}, membershipTargets(cfg, state, tick, pb.RemoveReplica, remove))

	shard.Epoch++
	delete(shard.Replicas, 3)
	state.Shards[0] = shard
	add := pb.Replica{ShardID: 0, ReplicaID: 4, UUID: "d", Epoch: 6}
	require.Equal(t, []string{"a", "b"}, membershipTargets(cfg, state, tick, pb.AddReplica, add))
	b := stores["b"]
	b.Tick = 0
	stores["b"] = b
	require.Empty(t, membershipTargets(cfg, state, tick, pb.AddReplica, add),
		"an old or projected majority cannot be inferred from a single reporter")
}
