// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

func TestParseLogShardsFencesChangedStoreIncarnation(t *testing.T) {
	cluster := pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{
		ShardID:          1,
		NumberOfReplicas: 3,
	}}}
	shard := pb.LogShardInfo{
		ShardID:                  1,
		Replicas:                 map[uint64]string{1: "log-a", 3: "log-c", 5: "log-d"},
		NonVotingReplicas:        map[uint64]string{2: "log-b"},
		ReplicaStoreIncarnations: map[uint64]string{1: "old-a", 2: "old-b", 3: "old-c", 5: "old-d"},
		Epoch:                    1,
	}
	state := pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: shard},
		Stores: map[string]pb.LogStoreInfo{
			"log-a": {StoreIncarnation: "new-a"},
			"log-b": {StoreIncarnation: "new-b"},
			"log-c": {
				StoreIncarnation: "old-c",
				Replicas:         []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}},
			},
			"log-d": {
				StoreIncarnation: "old-d",
				Replicas:         []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 5}},
			},
		},
	}

	normal, nonVoting := parseLogShards(cluster, state, nil, 1)
	assert.Equal(t, []replica{{uuid: "log-a", shardID: 1, replicaID: 1}}, normal.toRemove[1])
	assert.Empty(t, normal.toStart)
	assert.Equal(t, []replica{{uuid: "log-b", shardID: 1, replicaID: 2}}, nonVoting.toRemove[1])
	assert.Empty(t, nonVoting.toStart)

	// Once the store and replica generations agree, a missing local replica is
	// an ordinary StartReplica case rather than a lost-storage replacement.
	state.Stores["log-a"] = pb.LogStoreInfo{StoreIncarnation: "old-a"}
	state.Stores["log-b"] = pb.LogStoreInfo{StoreIncarnation: "old-b"}
	normal, nonVoting = parseLogShards(cluster, state, nil, 1)
	assert.Empty(t, normal.toRemove)
	assert.Equal(t, []replica{{uuid: "log-a", shardID: 1, replicaID: 1}}, normal.toStart)
	assert.Empty(t, nonVoting.toRemove)
	assert.Equal(t, []replica{{uuid: "log-b", shardID: 1, replicaID: 2}}, nonVoting.toStart)

	// Empty incarnation fields represent a mixed-version cluster and preserve
	// the legacy fail-open behavior.
	state.Shards[1] = pb.LogShardInfo{
		ShardID:           1,
		Replicas:          map[uint64]string{1: "log-a"},
		NonVotingReplicas: map[uint64]string{2: "log-b"},
		Epoch:             1,
	}
	normal, nonVoting = parseLogShards(cluster, state, nil, 1)
	assert.Empty(t, normal.toRemove)
	assert.Len(t, normal.toStart, 1)
	assert.Empty(t, nonVoting.toRemove)
	assert.Len(t, nonVoting.toStart, 1)
}

func TestChangedVotingStoreIncarnationCountsTowardSurplus(t *testing.T) {
	cluster := pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 4}}}
	shard := pb.LogShardInfo{
		ShardID:                  1,
		Replicas:                 map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"},
		ReplicaStoreIncarnations: map[uint64]string{5: "old"},
	}
	state := pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: shard},
		Stores: map[string]pb.LogStoreInfo{"d": {}, "e": {StoreIncarnation: "new"}},
	}
	for id := uint64(1); id <= 3; id++ {
		state.Stores[shard.Replicas[id]] = pb.LogStoreInfo{
			Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}},
		}
	}

	normal, _ := parseLogShards(cluster, state, nil, 0)
	assert.Equal(t, []replica{{uuid: "e", shardID: 1, replicaID: 5}}, normal.toRemove[1])
}

func TestFixedLogShardInfo(t *testing.T) {
	cases := []struct {
		desc string

		record metadata.LogShardRecord

		// nonVotingReplicaNum is the required non-voting replica number.
		nonVotingReplicaNum uint64

		info          pb.LogShardInfo
		expiredStores []string

		expectedNormal    *fixingShard
		expectedNonVoting *fixingShard
	}{
		{
			desc: "normal case",
			record: metadata.LogShardRecord{
				ShardID:          1,
				NumberOfReplicas: 3,
			},
			info: pb.LogShardInfo{
				ShardID:  1,
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			},
			expectedNormal: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
				toAdd:    0,
			},
			expectedNonVoting: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{},
			},
		},
		{
			desc: "shard 1 has 2 replicas, which expected to be 3",
			record: metadata.LogShardRecord{
				ShardID:          1,
				NumberOfReplicas: 3,
			},
			info: pb.LogShardInfo{
				ShardID:  1,
				Replicas: map[uint64]string{1: "a", 2: "b"},
			},
			expectedNormal: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{1: "a", 2: "b"},
				toAdd:    1,
			},
			expectedNonVoting: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{},
			},
		},
		{
			desc: "shard 1 has 3 replicas, which expected to be 1, and leader is the last to be removed",
			record: metadata.LogShardRecord{
				ShardID:          1,
				NumberOfReplicas: 1,
			},
			info: pb.LogShardInfo{
				ShardID:  1,
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
				LeaderID: 2,
			},
			expectedNormal: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{2: "b"},
				toAdd:    0,
			},
			expectedNonVoting: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{},
			},
		},
		{
			desc: "shard 1 has no non-voting replicas, and the expecting num is 3",
			record: metadata.LogShardRecord{
				ShardID:          1,
				NumberOfReplicas: 3,
			},
			info: pb.LogShardInfo{
				ShardID:  1,
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			},
			nonVotingReplicaNum: 3,
			expectedNormal: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
				toAdd:    0,
			},
			expectedNonVoting: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{},
				toAdd:    3,
			},
		},
		{
			desc: "shard 1 has 6 replicas, including 3 non-voting replicas, and the expecting num is 3",
			record: metadata.LogShardRecord{
				ShardID:          1,
				NumberOfReplicas: 3,
			},
			info: pb.LogShardInfo{
				ShardID:           1,
				Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
				NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
			},
			nonVotingReplicaNum: 3,
			expectedNormal: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
				toAdd:    0,
			},
			expectedNonVoting: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
				toAdd:    0,
			},
		},
		{
			desc: "shard 1 has 6 replicas, including 2 non-voting replicas, and the expecting num is 3",
			record: metadata.LogShardRecord{
				ShardID:          1,
				NumberOfReplicas: 3,
			},
			info: pb.LogShardInfo{
				ShardID:           1,
				Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
				NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
			},
			nonVotingReplicaNum: 3,
			expectedNormal: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
				toAdd:    0,
			},
			expectedNonVoting: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{4: "d", 5: "e"},
				toAdd:    1,
			},
		},
		{
			desc: "shard 1 has 6 replicas, including 2 non-voting replicas, and the expecting num is 3",
			record: metadata.LogShardRecord{
				ShardID:          1,
				NumberOfReplicas: 3,
			},
			info: pb.LogShardInfo{
				ShardID:           1,
				Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
				NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f", 7: "g"},
			},
			nonVotingReplicaNum: 3,
			expectedNormal: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
				toAdd:    0,
			},
			expectedNonVoting: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{5: "e", 6: "f", 7: "g"},
				toAdd:    0,
			},
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		normal, nonVoting := fixedLogShardInfo(c.record, c.nonVotingReplicaNum, c.info, c.expiredStores, nil)
		assert.Equal(t, c.expectedNormal, normal)
		assert.Equal(t, c.expectedNonVoting, nonVoting)
	}
}

func TestCollectStats(t *testing.T) {
	cases := []struct {
		desc              string
		cluster           pb.ClusterInfo
		infos             pb.LogState
		expired           []string
		expectedNormal    *stats
		expectedNonVoting *stats

		// nonVotingReplicaNum is the required non-voting replica number.
		nonVotingReplicaNum uint64
	}{
		{
			desc: "Normal case",
			cluster: pb.ClusterInfo{
				TNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3}}},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0,
					}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0,
							}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0,
							}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
				},
			},
			expired:           []string{},
			expectedNormal:    &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}},
			expectedNonVoting: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}, nonVoting: true},
		},
		{
			desc: "Shard 1 has only 2 replicas, which is expected as 3.",
			cluster: pb.ClusterInfo{
				TNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3}}},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b"},
					Epoch:    1,
					LeaderID: 0,
					Term:     0}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
				},
			},
			expired:           []string{},
			expectedNormal:    &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{1: 1}},
			expectedNonVoting: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}, nonVoting: true},
		},
		{
			desc: "replica on Store c is not started.",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3}}},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{}},
				},
			},
			expectedNormal: &stats{toStart: []replica{{uuid: "c", shardID: 1, replicaID: 3}},
				toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}},
			expectedNonVoting: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}, nonVoting: true},
		},
		{
			desc: "replica on Store d is a zombie.",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3}}},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"d": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
								Epoch:    0,
								LeaderID: 0,
								Term:     0},
						}}},
				},
			},
			expectedNormal: &stats{zombies: []replica{{uuid: "d", shardID: 1, replicaID: 0}},
				toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}},
			expectedNonVoting: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}, nonVoting: true},
		},
		{
			desc: "do not remove replica d if it is in LogShardInfo.Replicas, despite it's epoch is small.",
			cluster: pb.ClusterInfo{
				TNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 4}}},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
						Epoch:    1}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1},
						ReplicaID: 1,
					}}},
					"b": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1},
						ReplicaID: 2,
					}}},
					"c": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1},
						ReplicaID: 3,
					}}},
					"d": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"}},
						ReplicaID: 4,
					}}},
				},
			},
			expectedNormal: &stats{
				toRemove: map[uint64][]replica{},
				toAdd:    map[uint64]uint32{}},
			expectedNonVoting: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}, nonVoting: true},
		},
		{
			desc: "Shard 1 has 4 replicas, which is expected as 3.",
			cluster: pb.ClusterInfo{
				TNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3}}},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"d": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
				},
			},
			expectedNormal: &stats{toRemove: map[uint64][]replica{1: {{uuid: "a", shardID: 1, replicaID: 1}}},
				toAdd: map[uint64]uint32{},
			},
			expectedNonVoting: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}, nonVoting: true},
		},
		{
			desc: "Store a is expired",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3}}},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							ReplicaID: 1,
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 999999999, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							ReplicaID: 2,
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 999999999, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							ReplicaID: 3,
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}}},
			},
			expired: []string{"a"},
			expectedNormal: &stats{
				toRemove: map[uint64][]replica{1: {{uuid: "a", shardID: 1, replicaID: 1}}},
				toAdd:    map[uint64]uint32{},
			},
			expectedNonVoting: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}, nonVoting: true},
		},

		// for non-voting replicas
		{
			desc: "nothing happened",
			cluster: pb.ClusterInfo{
				TNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3}}},
			nonVotingReplicaNum: 3,
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:           1,
						Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:             1,
						LeaderID:          0,
						Term:              0,
						NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
					}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"d": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"e": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"f": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
				},
			},
			expired:           []string{},
			expectedNormal:    &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}},
			expectedNonVoting: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}, nonVoting: true},
		},
		{
			desc: "shard 1 has 3 normal replicas and 0 non-voting replicas, but expect 3 non-voting replica",
			cluster: pb.ClusterInfo{
				TNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3}}},
			nonVotingReplicaNum: 3,
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0,
					}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0,
							}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0,
							}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
				},
			},
			expired:           []string{},
			expectedNormal:    &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}},
			expectedNonVoting: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{1: 3}, nonVoting: true},
		},
		{
			desc: "shard 1 has 3 normal replicas and 4 non-voting replicas, but only 3 non-voting replica expected",
			cluster: pb.ClusterInfo{
				TNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3}}},
			nonVotingReplicaNum: 3,
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:           1,
						Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:             1,
						LeaderID:          0,
						Term:              0,
						NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f", 7: "g"},
					}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f", 7: "g"},
							}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f", 7: "g"},
							}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f", 7: "g"},
							}}}},
					"d": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f", 7: "g"},
							}}}},
					"e": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f", 7: "g"},
							}}}},
					"f": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f", 7: "g"},
							}}}},
					"g": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f", 7: "g"},
							}}}},
				},
			},
			expired:        []string{},
			expectedNormal: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}},
			expectedNonVoting: &stats{toRemove: map[uint64][]replica{
				1: {{uuid: "d", shardID: 1, replicaID: 4}},
			}, toAdd: map[uint64]uint32{}, nonVoting: true},
		},
		{
			desc: "non-voting replica on store f is not started.",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3}}},
			nonVotingReplicaNum: 3,
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:           1,
						Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:             1,
						LeaderID:          0,
						NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
						Term:              0}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"d": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"e": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"f": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{}},
				},
			},
			expectedNormal: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}},
			expectedNonVoting: &stats{toStart: []replica{{uuid: "f", shardID: 1, replicaID: 6}},
				toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}, nonVoting: true},
		},
		{
			desc: "non-voting replica on store d is a zombie.",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3}}},
			nonVotingReplicaNum: 3,
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:           1,
						Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:             1,
						LeaderID:          0,
						Term:              0,
						NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
					},
				},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"d": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"e": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"f": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"g": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             0,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
				},
			},
			expectedNormal: &stats{zombies: []replica{{uuid: "g", shardID: 1, replicaID: 0}},
				toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}},
			expectedNonVoting: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}, nonVoting: true},
		},
		{
			desc: "store f is expired.",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3}}},
			nonVotingReplicaNum: 3,
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:           1,
						Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:             1,
						LeaderID:          0,
						Term:              0,
						NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
					},
				},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"d": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"e": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
					"f": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:             1,
								LeaderID:          0,
								Term:              0,
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
							}}}},
				},
			},
			expired:        []string{"f"},
			expectedNormal: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}},
			expectedNonVoting: &stats{toRemove: map[uint64][]replica{
				1: {{uuid: "f", shardID: 1, replicaID: 6}},
			}, toAdd: map[uint64]uint32{}, nonVoting: true},
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		normal, nonVoting := parseLogShards(c.cluster, c.infos, c.expired, c.nonVotingReplicaNum)
		assert.Equal(t, c.expectedNormal, normal)
		assert.Equal(t, c.expectedNonVoting, nonVoting)
	}
}

func TestCollectStore(t *testing.T) {
	cases := []struct {
		desc     string
		cluster  pb.ClusterInfo
		infos    pb.LogState
		tick     uint64
		expected []string
	}{
		{
			desc: "no expired stores",
			cluster: pb.ClusterInfo{
				TNShards:  nil,
				LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0},
					"b": {Tick: 0},
					"c": {Tick: 0},
				},
			},
			tick:     0,
			expected: []string{},
		},
		{
			desc: "store b expired",
			cluster: pb.ClusterInfo{
				TNShards:  nil,
				LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: expiredTick + 1},
					"b": {Tick: 0},
					"c": {Tick: expiredTick + 1},
				},
			},
			tick:     expiredTick + 1,
			expected: []string{"b"},
		},
	}
	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		cfg := hakeeper.Config{}
		cfg.Fill()
		workingStores, expired := parseLogStores(cfg, c.infos, c.tick)
		var working []string
		for storeID := range workingStores {
			working = append(working, storeID)
		}
		sort.Slice(working, func(i, j int) bool {
			return working[i] < working[j]
		})
		sort.Slice(expired, func(i, j int) bool {
			return expired[i] < expired[j]
		})
		assert.Equal(t, c.expected, expired)
		cfg1 := hakeeper.Config{LogStoreTimeout: time.Hour}
		cfg1.Fill()
		_, expired = parseLogStores(cfg1, c.infos, c.tick)
		assert.Equal(t, []string{}, expired)
	}
}

func TestMembershipChangesWaitForVotingMajority(t *testing.T) {
	for _, tc := range []struct {
		name              string
		nonVotingReplicas map[uint64]string
		requiredNonVoters uint64
	}{
		{name: "non-voting add", nonVotingReplicas: map[uint64]string{4: "d"}, requiredNonVoters: 2},
		{name: "non-voting remove", nonVotingReplicas: map[uint64]string{4: "d", 5: "e"}, requiredNonVoters: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shard := pb.LogShardInfo{
				ShardID:           1,
				Epoch:             9,
				Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
				NonVotingReplicas: tc.nonVotingReplicas,
			}
			independentShard := pb.LogShardInfo{ShardID: 2, Epoch: 1, Replicas: map[uint64]string{1: "a"}}
			state := pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: shard, 2: independentShard},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 100, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
					"b": {Tick: 100},                                                                   // alive, but it has not reported this shard
					"c": {Tick: 0, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}}, // expired voter; its last report is not evidence
					"d": {Tick: 100},
					"e": {Tick: 100},
				},
			}
			cluster := pb.ClusterInfo{LogShards: []metadata.LogShardRecord{
				{ShardID: 1, NumberOfReplicas: 3}, {ShardID: 2, NumberOfReplicas: 1},
			}}
			normal, nonVoting := parseLogShards(cluster, state, []string{"c"}, tc.requiredNonVoters)

			assert.Empty(t, normal.toAdd[1])
			assert.Empty(t, normal.toRemove[1])
			assert.Empty(t, nonVoting.toAdd[1])
			assert.Empty(t, nonVoting.toRemove[1])
			assert.True(t, hasReplica(normal.toStart, "b", 1), "a configured live voter can still start")
			assert.True(t, hasReplica(nonVoting.toStart, "d", 1), "a configured non-voter can still start")
			assert.True(t, hasReplica(normal.toStart, "a", 2), "an unrelated shard can start on the reporting voter")

			lc := &logServiceChecker{
				CheckerCommonFields: hakeeper.CheckerCommonFields{Cluster: cluster},
				logState:            state,
				workingStores:       map[string]pb.Locality{"a": {}, "b": {}, "d": {}, "e": {}},
			}
			common := checker{lc: lc, ss: []*stats{normal, nonVoting}, membershipScheduled: make(map[uint64]struct{})}
			assert.Empty(t, (&checkToAdd{checker: common}).check())
			assert.Empty(t, (&checkToRemove{checker: common}).check())
			startOps := (&checkToStart{checker: common}).check()
			startedIndependentShard := false
			for _, op := range startOps {
				for _, step := range op.OpSteps() {
					if start, ok := step.(operator.StartLogService); ok &&
						start.Replica.UUID == "a" && start.Replica.ShardID == 2 {
						startedIndependentShard = true
					}
				}
			}
			assert.True(t, startedIndependentShard, "an unrelated start remains schedulable while this shard lacks quorum")
		})
	}
}

func hasReplica(replicas []replica, uuid string, shardID uint64) bool {
	for _, r := range replicas {
		if r.uuid == uuid && r.shardID == shardID {
			return true
		}
	}
	return false
}
