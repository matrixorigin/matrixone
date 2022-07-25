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
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

func TestFixedLogShardInfo(t *testing.T) {
	cases := []struct {
		desc string

		record        metadata.LogShardRecord
		info          pb.LogShardInfo
		expiredStores util.StoreSlice

		expected *fixingShard
	}{
		{
			desc: "normal case",
			record: metadata.LogShardRecord{
				ShardID:          1,
				NumberOfReplicas: 3,
				Name:             "shard-1",
			},
			info: pb.LogShardInfo{
				ShardID:  1,
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			},
			expected: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
				toAdd:    0,
			},
		},
		{
			desc: "shard 1 has 2 replicas, which expected to be 3",
			record: metadata.LogShardRecord{
				ShardID:          1,
				NumberOfReplicas: 3,
				Name:             "shard-1",
			},
			info: pb.LogShardInfo{
				ShardID:  1,
				Replicas: map[uint64]string{1: "a", 2: "b"},
			},
			expected: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{1: "a", 2: "b"},
				toAdd:    1,
			},
		},
		{
			desc: "shard 1 has 2 replicas, which expected to be 3",
			record: metadata.LogShardRecord{
				ShardID:          1,
				NumberOfReplicas: 3,
				Name:             "shard-1",
			},
			info: pb.LogShardInfo{
				ShardID:  1,
				Replicas: map[uint64]string{1: "a", 2: "b"},
			},
			expected: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{1: "a", 2: "b"},
				toAdd:    1,
			},
		},
	}

	for _, c := range cases {
		output := fixedLogShardInfo(c.record, c.info, c.expiredStores)
		assert.Equal(t, c.expected, output)
	}
}

func TestCollectStats(t *testing.T) {
	cases := []struct {
		desc     string
		cluster  pb.ClusterInfo
		infos    pb.LogState
		expired  util.StoreSlice
		expected *stats
	}{
		{
			desc: "Normal case",
			cluster: pb.ClusterInfo{
				DNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             ""}}},
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
			expired:  []*util.Store{},
			expected: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}}},
		{
			desc: "Shard 1 has only 2 replicas, which is expected as 3.",
			cluster: pb.ClusterInfo{
				DNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             ""}}},
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
			expired:  []*util.Store{},
			expected: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{1: 1}}},
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
			expected: &stats{toStart: []replica{{"c", 1, 0, 3}},
				toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}}},
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
								Term:     0}}}},
				},
			},
			expected: &stats{toStop: []replica{{"d", 1, 0, 0}},
				toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}}},
		{
			desc: "do not remove replica d if it is in LogShardInfo.Replicas, despite it's epoch is small.",
			cluster: pb.ClusterInfo{
				DNShards: nil,
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
			expected: &stats{
				toRemove: map[uint64][]replica{},
				toAdd:    map[uint64]uint32{}}},
		{
			desc: "Shard 1 has 4 replicas, which is expected as 3.",
			cluster: pb.ClusterInfo{
				DNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             ""}}},
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
			expected: &stats{toRemove: map[uint64][]replica{1: {{"a", 1, 0, 1}}},
				toAdd: map[uint64]uint32{},
			},
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
			expired: []*util.Store{{ID: "a"}},
			expected: &stats{
				toRemove: map[uint64][]replica{1: {{uuid: "a", shardID: 1, replicaID: 1}}},
				toAdd:    map[uint64]uint32{},
			},
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		stat := parseLogShards(c.cluster, c.infos, c.expired)
		assert.Equal(t, c.expected, stat)
	}
}

func TestCollectStore(t *testing.T) {
	cases := []struct {
		desc     string
		cluster  pb.ClusterInfo
		infos    pb.LogState
		tick     uint64
		expected util.ClusterStores
	}{
		{
			desc: "no expired stores",
			cluster: pb.ClusterInfo{
				DNShards:  nil,
				LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: uint64(10 * hakeeper.DefaultTickPerSecond * 60),
					},
					"b": {
						Tick: uint64(13 * hakeeper.DefaultTickPerSecond * 60),
					},
					"c": {
						Tick: uint64(12 * hakeeper.DefaultTickPerSecond * 60),
					},
				},
			},
			tick: uint64(10 * hakeeper.DefaultTickPerSecond * 60),
			expected: util.ClusterStores{
				Working: []*util.Store{
					{ID: "a", Capacity: 32},
					{ID: "b", Capacity: 32},
					{ID: "c", Capacity: 32},
				},
			},
		},
		{
			desc: "store b expired",
			cluster: pb.ClusterInfo{
				DNShards:  nil,
				LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: uint64(10 * hakeeper.DefaultTickPerSecond * 60)},
					"b": {Tick: 0},
					"c": {Tick: uint64(12 * hakeeper.DefaultTickPerSecond * 60)},
				},
			},
			tick: uint64(15 * hakeeper.DefaultTickPerSecond * 60),
			expected: util.ClusterStores{
				Working: []*util.Store{
					{ID: "a", Capacity: 32},
					{ID: "c", Capacity: 32},
				},
				Expired: []*util.Store{{ID: "b", Capacity: 32}},
			},
		},
	}
	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		cfg := hakeeper.Config{}
		cfg.Fill()
		stores := parseLogStores(cfg, c.infos, c.tick)
		sort.Slice(stores.Working, func(i, j int) bool {
			return stores.Working[i].ID < stores.Working[j].ID
		})
		sort.Slice(stores.Expired, func(i, j int) bool {
			return stores.Expired[i].ID < stores.Expired[j].ID
		})
		assert.Equal(t, c.expected, *stores)
		cfg1 := hakeeper.Config{LogStoreTimeout: time.Hour}
		cfg1.Fill()
		stores = parseLogStores(cfg1, c.infos, c.tick)
		assert.Nil(t, stores.ExpiredStores())
	}
}
