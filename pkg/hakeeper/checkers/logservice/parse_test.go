// Copyright 2022 MatrixOrigin.
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

package logservice

import (
	"fmt"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

func minuteToTick(minute uint64) uint64 { return minute * util.TickPerSecond * 60 }

func TestFixedLogShardInfo(t *testing.T) {
	cases := []struct {
		desc string

		record        metadata.LogShardRecord
		info          pb.LogShardInfo
		expiredStores util.StoreSlice

		expected *fixingShard
	}{
		{
			desc:   "normal case",
			record: metadata.LogShardRecord{ShardID: 1, NumberOfReplicas: 3},
			info:   pb.LogShardInfo{ShardID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}},
			expected: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
				toAdd:    0,
			},
		},
		{
			desc:   "shard 1 has 2 replicas, which expected to be 3",
			record: metadata.LogShardRecord{ShardID: 1, NumberOfReplicas: 3},
			info:   pb.LogShardInfo{ShardID: 1, Replicas: map[uint64]string{1: "a", 2: "b"}},
			expected: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{1: "a", 2: "b"},
				toAdd:    1,
			},
		},
		{
			desc:   "shard 1 has 4 replicas, which expected to be 3",
			record: metadata.LogShardRecord{ShardID: 1, NumberOfReplicas: 3},
			info:   pb.LogShardInfo{ShardID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"}},
			expected: &fixingShard{
				shardID:  1,
				replicas: map[uint64]string{2: "b", 3: "c", 4: "d"},
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
				LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1},
				},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1,
						}}}},
					"b": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1,
						}}}},
					"c": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1,
						}}}},
				},
			},
			expired:  []*util.Store{},
			expected: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]uint32{}}},
		{
			desc: "Shard 1 has only 2 replicas, which is expected as 3.",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b"},
					Epoch:    1}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1}}}},
					"b": {Replicas: []pb.LogReplicaInfo{{
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
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1}}}},
					"b": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1}}}},
					"c": {},
				},
			},
			expected: &stats{
				toStart:  []replica{{"c", 1, 0, 3}},
				toRemove: map[uint64][]replica{},
				toAdd:    map[uint64]uint32{}}},
		{
			desc: "replica on Store d is a zombie.",
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
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
				toStop:   []replica{{"d", 1, 0, 0}},
				toRemove: map[uint64][]replica{},
				toAdd:    map[uint64]uint32{}}},
		{
			desc: "do not remove replica d if it is in LogShardInfo.Replicas, despite it's epoch is small.",
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
				DNShards:  nil,
				LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
						Epoch:    1}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
							Epoch:    1},
						ReplicaID: 1,
					}}},
					"b": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
							Epoch:    1},
						ReplicaID: 2,
					}}},
					"c": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
							Epoch:    1},
						ReplicaID: 3,
					}}},
					"d": {Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
							Epoch:    1},
						ReplicaID: 4,
					}}},
				},
			},
			expected: &stats{
				toRemove: map[uint64][]replica{1: {{"a", 1, 0, 1}}},
				toAdd:    map[uint64]uint32{},
			},
		},
		{
			desc: "Store a is expired",
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1}}}},
					"b": {Tick: 999999999,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1}}}},
					"c": {Tick: 999999999,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1}}}}},
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
				LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: minuteToTick(10)},
					"b": {Tick: minuteToTick(13)},
					"c": {Tick: minuteToTick(12)},
				},
			},
			tick: minuteToTick(10),
			expected: util.ClusterStores{
				Working: []*util.Store{
					{ID: "a", Length: 0, Capacity: 32},
					{ID: "b", Length: 0, Capacity: 32},
					{ID: "c", Length: 0, Capacity: 32},
				},
			},
		},
		{
			desc: "store b expired",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: minuteToTick(10)},
					"b": {Tick: 0},
					"c": {Tick: minuteToTick(12)},
				},
			},
			tick: minuteToTick(15),
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
		stores := parseLogStores(c.infos, c.tick)
		sort.Slice(stores.Working, func(i, j int) bool {
			return stores.Working[i].ID < stores.Working[j].ID
		})
		sort.Slice(stores.Expired, func(i, j int) bool {
			return stores.Expired[i].ID < stores.Expired[j].ID
		})
		assert.Equal(t, c.expected, *stores)
	}
}
