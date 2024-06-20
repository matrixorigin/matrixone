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
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

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
		normal, nonVoting := fixedLogShardInfo(c.record, c.nonVotingReplicaNum, c.info, c.expiredStores)
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
			expectedNormal: &stats{toStart: []replica{{"c", 1, 0, 3}},
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
			expectedNormal: &stats{zombies: []replica{{"d", 1, 0, 0}},
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
			expectedNormal: &stats{toRemove: map[uint64][]replica{1: {{"a", 1, 0, 1}}},
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
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 999999999, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 999999999, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
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
			expectedNonVoting: &stats{toStart: []replica{{"f", 1, 0, 6}},
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
			expectedNormal: &stats{zombies: []replica{{"g", 1, 0, 0}},
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
