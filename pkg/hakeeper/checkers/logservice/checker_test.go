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
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"reflect"
	"testing"
)

func TestCollect(t *testing.T) {
	cases := []struct {
		desc     string
		cluster  hakeeper.ClusterInfo
		infos    hakeeper.LogState
		tick     uint64
		expected stats
	}{
		{
			desc: "Normal case",
			cluster: hakeeper.ClusterInfo{
				DNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             ""}}},
			infos: hakeeper.LogState{
				Shards: map[uint64]logservice.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0,
					}},
				Stores: map[string]hakeeper.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0,
							}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0,
							}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}}},
			}, tick: 10, expected: stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]int{}}},
		{
			desc: "Shard 1 has only 2 replicas, which is expected as 3.",
			cluster: hakeeper.ClusterInfo{
				DNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             ""}}},
			infos: hakeeper.LogState{
				Shards: map[uint64]logservice.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b"},
					Epoch:    1,
					LeaderID: 0,
					Term:     0}},
				Stores: map[string]hakeeper.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
				},
			}, tick: 10, expected: stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]int{1: 1}}},
		{
			desc: "replica on Store c is not started.",
			infos: hakeeper.LogState{
				Shards: map[uint64]logservice.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0}},
				Stores: map[string]hakeeper.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{}},
				},
			}, tick: 10,
			expected: stats{toStart: []replica{{"c", 1, 0, 3}},
				toRemove: map[uint64][]replica{}, toAdd: map[uint64]int{}}},
		{
			desc: "replica on Store d is a zombie.",
			infos: hakeeper.LogState{
				Shards: map[uint64]logservice.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0}},
				Stores: map[string]hakeeper.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"d": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
								Epoch:    0,
								LeaderID: 0,
								Term:     0}}}},
				},
			}, tick: 10,
			expected: stats{toStop: []replica{{"d", 1, 0, 0}},
				toRemove: map[uint64][]replica{}, toAdd: map[uint64]int{}}},
		{
			desc: "Shard 1 has 4 replicas, which is expected as 3.",
			cluster: hakeeper.ClusterInfo{
				DNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             ""}}},
			infos: hakeeper.LogState{
				Shards: map[uint64]logservice.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0}},
				Stores: map[string]hakeeper.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"d": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
				},
			}, tick: 10,
			expected: stats{toRemove: map[uint64][]replica{1: {{"a", 1, 0, 1}}},
				toAdd: map[uint64]int{}}},
		{
			desc: "Store a is expired",
			infos: hakeeper.LogState{
				Shards: map[uint64]logservice.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0}},
				Stores: map[string]hakeeper.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 999999999, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 999999999, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []logservice.LogReplicaInfo{{
							LogShardInfo: logservice.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}}},
			}, tick: 999999999, expected: stats{
				toRemove: map[uint64][]replica{1: {{"a", 1, 1, 1}}},
				toAdd:    map[uint64]int{}}},
	}

	for i, c := range cases {
		stat := collectStats(c.cluster, c.infos, c.tick)
		fmt.Printf("case %v: %s\n", i, c.desc)
		if !reflect.DeepEqual(stat, c.expected) {
			fmt.Println(stat)
			fmt.Println(c.expected)
			t.Fatalf("error")
		}
	}
}
