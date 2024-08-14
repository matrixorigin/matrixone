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

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "debug",
		Format: "console",
	})

	runtime.SetupServiceBasedRuntime("", runtime.NewRuntime(metadata.ServiceType_LOG, "test", logutil.GetGlobalLogger()))
	m.Run()
}

var expiredTick = uint64(hakeeper.DefaultLogStoreTimeout / time.Second * hakeeper.DefaultTickPerSecond)

func TestCheck(t *testing.T) {
	cases := []struct {
		desc                string
		cluster             pb.ClusterInfo
		logState            pb.LogState
		users               pb.TaskTableUser
		removing            map[uint64][]uint64
		adding              map[uint64][]uint64
		currentTick         uint64
		nonVotingReplicaNum uint64
		idStartFrom         uint64
		expected            []*operator.Operator
		standbyEnabled      bool
	}{
		// for normal replicas
		{
			desc: "normal case",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			logState: pb.LogState{
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
			logState: pb.LogState{
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
			logState: pb.LogState{
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
			idStartFrom: 3,
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
			logState: pb.LogState{
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
			logState: pb.LogState{
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
		{
			desc: "no more stores but need to create task service",
			cluster: pb.ClusterInfo{
				TNShards: []metadata.TNShardRecord{{
					ShardID:    1,
					LogShardID: 1,
				}},
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID: 1,
					Replicas: map[uint64]string{
						1: "a",
						2: "b",
						3: "c",
					},
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
							},
							ReplicaID: 1,
						}},
						TaskServiceCreated: true,
					},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							},
						}},
						TaskServiceCreated: true,
					},
					"c": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							},
						}},
						TaskServiceCreated: false,
					},
				},
			},
			users:       pb.TaskTableUser{Username: "abc"},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{operator.NewOperator("", 1,
				1, operator.CreateTaskService{
					StoreID:  "c",
					TaskUser: pb.TaskTableUser{Username: "abc"},
				})},
		},
		{
			desc: "bootstrapping for the shard that has no replicas",
			cluster: pb.ClusterInfo{
				TNShards: []metadata.TNShardRecord{{
					ShardID:    1,
					LogShardID: 1,
				}},
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{},
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick:               expiredTick + 1,
						Replicas:           []pb.LogReplicaInfo{},
						TaskServiceCreated: true,
					},
					"b": {
						Tick:               expiredTick + 1,
						Replicas:           []pb.LogReplicaInfo{},
						TaskServiceCreated: true,
					},
					"c": {
						Tick:               expiredTick + 1,
						Replicas:           []pb.LogReplicaInfo{},
						TaskServiceCreated: true,
					},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{
				operator.NewOperator("", 1,
					1, operator.BootstrapShard{
						UUID:      "a",
						ShardID:   1,
						ReplicaID: 1,
						InitialMembers: map[uint64]string{
							1: "a",
							2: "b",
							3: "c",
						},
						Join: false,
					},
				),
				operator.NewOperator("", 1,
					1, operator.BootstrapShard{
						UUID:      "b",
						ShardID:   1,
						ReplicaID: 2,
						InitialMembers: map[uint64]string{
							1: "a",
							2: "b",
							3: "c",
						},
						Join: false,
					},
				),
				operator.NewOperator("", 1,
					1, operator.BootstrapShard{
						UUID:      "c",
						ShardID:   1,
						ReplicaID: 3,
						InitialMembers: map[uint64]string{
							1: "a",
							2: "b",
							3: "c",
						},
						Join: false,
					},
				),
			},
		},
		{
			desc:           "add new shard",
			standbyEnabled: true,
			cluster: pb.ClusterInfo{
				TNShards: []metadata.TNShardRecord{{
					ShardID:    2,
					LogShardID: 1,
				}},
				LogShards: []metadata.LogShardRecord{
					{
						ShardID:          0,
						NumberOfReplicas: 3,
					},
					{
						ShardID:          1,
						NumberOfReplicas: 3,
					},
				},
			},
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					0: {
						ShardID: 0,
						Replicas: map[uint64]string{
							1: "a",
							2: "b",
							3: "c",
						},
						Epoch:    1,
						LeaderID: 1,
						Term:     1,
					},
					1: {
						ShardID: 1,
						Replicas: map[uint64]string{
							1: "a",
							2: "b",
							3: "c",
						},
						Epoch:    1,
						LeaderID: 1,
						Term:     1,
					},
				},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: expiredTick + 2,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  0,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 1,
							},
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  1,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 1,
							},
						},
						TaskServiceCreated: true,
					},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  0,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 2,
							},
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  1,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 2,
							},
						},
						TaskServiceCreated: true,
					},
					"c": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  0,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 3,
							},
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  1,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 3,
							},
						},
						TaskServiceCreated: true,
					},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{
				operator.NewOperator("", 1,
					1, operator.AddLogShard{
						UUID:    "a",
						ShardID: 2,
					},
				),
			},
		},
		// for non-voting replicas
		{
			desc: "shard 1 has no non-voting replicas, which expected as 1",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 1,
			logState: pb.LogState{
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
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  1,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 1,
							},
						},
					},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  1,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 2,
							},
						},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  1,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 3,
							},
						},
					},
					"d": {
						Tick:     0,
						Replicas: []pb.LogReplicaInfo{},
					},
				},
			},
			idStartFrom: 3,
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{operator.NewOperator("adding 1:4(at epoch 1) to c", 1,
				1, operator.AddNonVotingLogService{
					Target: "a",
					Replica: operator.Replica{
						UUID:      "d",
						ShardID:   1,
						ReplicaID: 4,
						Epoch:     1,
					},
				}),
			},
		},
		{
			desc: "shard 1 has only 2 non-voting replicas, which expected as 3",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 3,
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:           1,
					Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:             1,
					LeaderID:          1,
					Term:              1,
					NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 1,
							},
						},
					},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 2,
							},
						},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 3,
							},
						},
					},
					"d": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 4,
							},
						},
					},
					"e": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 5,
							},
						},
					},
					"f": {
						Tick:     0,
						Replicas: []pb.LogReplicaInfo{},
					},
				},
			},
			idStartFrom: 5,
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{operator.NewOperator("adding 1:4(at epoch 1) to c", 1,
				1, operator.AddNonVotingLogService{
					Target: "a",
					Replica: operator.Replica{
						UUID:      "f",
						ShardID:   1,
						ReplicaID: 6,
						Epoch:     1,
					},
				}),
			},
		},
		{
			desc: "store d expired",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 3,
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:           1,
					Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:             1,
					LeaderID:          1,
					Term:              1,
					NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 1,
							},
						},
					},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 2,
							},
						},
					},
					"c": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 3,
							},
						},
					},
					"d": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 4,
							},
						},
					},
					"e": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 5,
							},
						},
					},
					"f": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 6,
							},
						},
					},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: expiredTick + 1,
			expected: []*operator.Operator{
				operator.NewOperator("", 1, 1,
					operator.RemoveNonVotingLogService{
						Target: "a",
						Replica: operator.Replica{
							UUID:      "d",
							ShardID:   1,
							ReplicaID: 4,
							Epoch:     1,
						},
					}),
			},
		},
		{
			desc: "replica 6 on store f is not started",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 3,
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:           1,
					Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:             1,
					LeaderID:          1,
					Term:              1,
					NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 1,
							},
						},
					},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 2,
							},
						},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 3,
							},
						},
					},
					"d": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 3,
							},
						},
					},
					"e": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 3,
							},
						},
					},
					"f": {
						Tick:     0,
						Replicas: []pb.LogReplicaInfo{},
					},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{operator.NewOperator("", 1,
				1, operator.StartNonVotingLogService{
					Replica: operator.Replica{
						UUID:      "f",
						ShardID:   1,
						ReplicaID: 6},
				}),
			},
		},
		{
			desc: "store f expired and is processing",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 3,
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:           1,
					Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:             1,
					LeaderID:          1,
					Term:              1,
					NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 1,
							},
						},
					},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 2,
							},
						},
					},
					"c": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 3,
							},
						},
					},
					"d": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 4,
							},
						},
					},
					"e": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 5,
							},
						},
					},
					"f": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 6,
							},
						},
					},
				},
			},
			removing:    map[uint64][]uint64{1: {6}},
			adding:      nil,
			currentTick: expiredTick + 1,
			expected:    []*operator.Operator{},
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		alloc := util.NewTestIDAllocator(c.idStartFrom)
		cfg := hakeeper.Config{}
		cfg.Fill()
		executing := operator.ExecutingReplicas{
			Adding:   c.adding,
			Removing: c.removing,
		}
		lc := NewLogServiceChecker(
			hakeeper.NewCheckerCommonFields(
				"",
				cfg,
				alloc,
				c.cluster,
				c.users,
				c.currentTick,
			),
			c.logState,
			pb.TNState{},
			executing,
			executing,
			c.nonVotingReplicaNum,
			pb.Locality{},
			c.standbyEnabled,
		)
		operators := lc.Check()

		assert.Equal(t, len(c.expected), len(operators))
		for j, op := range operators {
			assert.Equal(t, len(c.expected[j].OpSteps()), len(op.OpSteps()))
			for k, step := range op.OpSteps() {
				assert.Equal(t, c.expected[j].OpSteps()[k], step)
			}
		}
	}
}
