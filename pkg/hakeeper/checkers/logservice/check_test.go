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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

var expiredTick = uint64(hakeeper.DefaultLogStoreTimeout / time.Second * hakeeper.DefaultTickPerSecond)

func TestCheck(t *testing.T) {
	cases := []struct {
		desc        string
		cluster     pb.ClusterInfo
		infos       pb.LogState
		removing    map[uint64][]uint64
		adding      map[uint64][]uint64
		currentTick uint64
		expected    []*operator.Operator
	}{
		{
			desc: "normal case",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
					Term:     1,
				}},
				Stores: map[string]pb.LogStoreInfo{"a": {
					Tick: 0,
					Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1,
							LeaderID: 1,
							Term:     1,
						}, ReplicaID: 1}}},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 2}}},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 3}}},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected:    nil,
		},
		{
			desc: "store \"a\" expired",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
					Term:     1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 1}}},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 2}}},
					"c": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 3}}},
					"d": {
						Tick: expiredTick + 1,
					},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: expiredTick + 1,
			expected: []*operator.Operator{
				operator.NewOperator("", 1, 1,
					operator.RemoveLogService{
						Target: "b",
						Replica: operator.Replica{
							UUID:      "a",
							ShardID:   1,
							ReplicaID: 1,
							Epoch:     1},
					}),
			},
		},
		{
			desc: "shard 1 has only 2 replicas, which expected as 3",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b"},
					Epoch:    1,
					LeaderID: 1,
					Term:     1,
				}},
				Stores: map[string]pb.LogStoreInfo{"a": {
					Tick: 0,
					Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1,
							LeaderID: 1,
							Term:     1,
						}, ReplicaID: 1}}},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 2}}},
					"c": {Tick: 0, Replicas: []pb.LogReplicaInfo{}},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{operator.NewOperator("adding 1:4(at epoch 1) to c", 1,
				1, operator.AddLogService{
					Target: "a",
					Replica: operator.Replica{
						UUID:      "c",
						ShardID:   1,
						ReplicaID: 4,
						Epoch:     1,
					},
				})},
		},
		{
			desc: "replica 3 on store c is not started",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
					Term:     1,
				}},
				Stores: map[string]pb.LogStoreInfo{"a": {
					Tick: 0,
					Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1,
							LeaderID: 1,
							Term:     1,
						}, ReplicaID: 1}}},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 2}}},
					"c": {
						Tick:     0,
						Replicas: []pb.LogReplicaInfo{}},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{operator.NewOperator("", 1,
				1, operator.StartLogService{
					Replica: operator.Replica{
						UUID:      "c",
						ShardID:   1,
						ReplicaID: 3},
				})},
		},
		{
			desc: "store \"a\" expired and is processing",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
					Term:     1,
				}},
				Stores: map[string]pb.LogStoreInfo{"a": {
					Tick: 0,
					Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1,
							LeaderID: 1,
							Term:     1,
						}, ReplicaID: 1}}},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 2}}},
					"c": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 3}}},
				},
			},
			removing:    map[uint64][]uint64{1: {1}},
			adding:      nil,
			currentTick: expiredTick + 1,
			expected:    []*operator.Operator{},
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		alloc := util.NewTestIDAllocator(3)
		cfg := hakeeper.Config{}
		cfg.Fill()
		executing := operator.ExecutingReplicas{
			Adding:   c.adding,
			Removing: c.removing,
		}
		operators := Check(alloc, cfg, c.cluster, c.infos, executing, pb.TaskTableUser{}, c.currentTick)

		assert.Equal(t, len(c.expected), len(operators))
		for j, op := range operators {
			assert.Equal(t, len(c.expected[j].OpSteps()), len(op.OpSteps()))
			for k, step := range op.OpSteps() {
				assert.Equal(t, c.expected[j].OpSteps()[k], step)
			}
		}
	}
}
