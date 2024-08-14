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

package checkers

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

func TestFixExpiredStore(t *testing.T) {
	cases := []struct {
		desc                string
		idAlloc             *util.TestIDAllocator
		cluster             pb.ClusterInfo
		tn                  pb.TNState
		log                 pb.LogState
		nonVotingReplicaNum uint64
		currentTick         uint64
		expected            []pb.ScheduleCommand
	}{
		{
			desc:    "normal case",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1, LeaderID: 1},
							ReplicaID: 1},
						}},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1},
							ReplicaID: 2,
						}},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1},
							ReplicaID: 3,
						}},
					},
				},
			},
			currentTick: 0,
			expected:    []pb.ScheduleCommand(nil),
		},
		{
			desc:    "store a is expired",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1, LeaderID: 1},
							ReplicaID: 1},
						}},
					"b": {
						Tick: expiredTick,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1},
							ReplicaID: 2,
						}},
					},
					"c": {
						Tick: expiredTick,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1},
							ReplicaID: 3,
						}},
					},
				},
			},
			currentTick: expiredTick + 1,
			expected: []pb.ScheduleCommand{{
				UUID: "b",
				ConfigChange: &pb.ConfigChange{
					Replica: pb.Replica{
						UUID:      "a",
						ShardID:   1,
						ReplicaID: 1,
						Epoch:     1,
					},
					ChangeType: pb.RemoveReplica,
				},
				ServiceType: pb.LogService,
			}},
		},
		{
			desc:    "shard 1 has 2 replicas, which expected to be 3",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, Replicas: []pb.LogReplicaInfo{}},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1},
							ReplicaID: 2,
						}},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1},
							ReplicaID: 3,
						}},
					},
				},
			},
			currentTick: 0,
			expected: []pb.ScheduleCommand{{
				UUID: "b",
				ConfigChange: &pb.ConfigChange{
					Replica: pb.Replica{
						UUID:      "a",
						ShardID:   1,
						ReplicaID: 4,
						Epoch:     1,
					},
					ChangeType: pb.AddReplica,
				},
				ServiceType: pb.LogService,
			}},
		},
		{
			desc:    "replica on store a is not started",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, Replicas: []pb.LogReplicaInfo{}},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1},
							ReplicaID: 2,
						}},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1},
							ReplicaID: 3,
						}},
					},
				},
			},
			currentTick: 0,
			expected: []pb.ScheduleCommand{
				{
					UUID: "a",
					ConfigChange: &pb.ConfigChange{
						Replica: pb.Replica{
							UUID:      "a",
							ShardID:   1,
							ReplicaID: 1,
						},
						ChangeType: pb.StartReplica,
					},
					ServiceType: pb.LogService,
				},
			},
		},
		{
			desc:    "non-voting normal case",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 3,
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:           1,
					Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
					NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
					Epoch:             1,
					LeaderID:          1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1, LeaderID: 1},
							ReplicaID: 1},
						}},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 2,
						}},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 3,
						}},
					},
					"d": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 4,
						}},
					},
					"e": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 5,
						}},
					},
					"f": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 6,
						}},
					},
				},
			},
			currentTick: 0,
			expected:    []pb.ScheduleCommand(nil),
		},
		{
			desc:    "start non-voting replica",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 3,
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:           1,
					Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
					NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
					Epoch:             1,
					LeaderID:          1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 2,
						}},
					},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 2,
						}},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 3,
						}},
					},
					"d": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 4,
						}},
					},
					"e": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 5,
						}},
					},
					"f": {Tick: 0, Replicas: []pb.LogReplicaInfo{}},
				},
			},
			currentTick: 0,
			expected: []pb.ScheduleCommand{
				{
					UUID: "f",
					ConfigChange: &pb.ConfigChange{
						Replica: pb.Replica{
							UUID:      "f",
							ShardID:   1,
							ReplicaID: 6,
						},
						ChangeType: pb.StartNonVotingReplica,
					},
					ServiceType: pb.LogService,
				},
			},
		},
		{
			desc:    "store f is expired",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 3,
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:           1,
					Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
					NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
					Epoch:             1,
					LeaderID:          1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: expiredTick,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1, LeaderID: 1},
							ReplicaID: 1},
						}},
					"b": {
						Tick: expiredTick,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 2,
						}},
					},
					"c": {
						Tick: expiredTick,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 3,
						}},
					},
					"d": {
						Tick: expiredTick,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 4,
						}},
					},
					"e": {
						Tick: expiredTick,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 5,
						}},
					},
					"f": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 6,
						}},
					},
				},
			},
			currentTick: expiredTick + 1,
			expected: []pb.ScheduleCommand{{
				UUID: "a",
				ConfigChange: &pb.ConfigChange{
					Replica: pb.Replica{
						UUID:      "f",
						ShardID:   1,
						ReplicaID: 6,
						Epoch:     1,
					},
					ChangeType: pb.RemoveNonVotingReplica,
				},
				ServiceType: pb.LogService,
			}},
		},
		{
			desc:    "shard 1 has 2 non-voting replicas, which expected to be 3",
			idAlloc: util.NewTestIDAllocator(5),
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 3,
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:           1,
					Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
					NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
					Epoch:             1,
					LeaderID:          1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 2,
						}},
					},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 2,
						}},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 3,
						}},
					},
					"d": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 3,
						}},
					},
					"e": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:           1,
								Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
								NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
								Epoch:             1,
								LeaderID:          1},
							ReplicaID: 3,
						}},
					},
					"f": {
						Tick:     0,
						Replicas: []pb.LogReplicaInfo{},
					},
				},
			},
			currentTick: 0,
			expected: []pb.ScheduleCommand{{
				UUID: "a",
				ConfigChange: &pb.ConfigChange{
					Replica: pb.Replica{
						UUID:      "f",
						ShardID:   1,
						ReplicaID: 6,
						Epoch:     1,
					},
					ChangeType: pb.AddNonVotingReplica,
				},
				ServiceType: pb.LogService,
			}},
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		coordinator := NewCoordinator("", hakeeper.Config{})
		output := coordinator.Check(
			c.idAlloc,
			pb.CheckerState{
				Tick:                c.currentTick,
				ClusterInfo:         c.cluster,
				TNState:             c.tn,
				LogState:            c.log,
				NonVotingReplicaNum: c.nonVotingReplicaNum,
			},
			false,
		)
		assert.Equal(t, c.expected, output)
	}
}

func TestFixZombie(t *testing.T) {
	cases := []struct {
		desc     string
		idAlloc  *util.TestIDAllocator
		cluster  pb.ClusterInfo
		tn       pb.TNState
		log      pb.LogState
		tick     uint64
		expected []pb.ScheduleCommand
	}{
		{
			desc:    "replica on store c is a zombie",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
					Epoch:    2,
					LeaderID: 1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
								Epoch:    2,
								LeaderID: 1},
							ReplicaID: 1},
						}},
					"b": {
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
								Epoch:    2,
								LeaderID: 1},
							ReplicaID: 2,
						}},
					},
					"c": {
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1},
							ReplicaID: 3,
						}},
					},
					"d": {
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
								Epoch:    2,
								LeaderID: 1},
							ReplicaID: 3,
						}},
					},
				},
			},
			expected: []pb.ScheduleCommand{
				{
					UUID: "c",
					ConfigChange: &pb.ConfigChange{
						Replica: pb.Replica{
							UUID:      "c",
							ShardID:   1,
							ReplicaID: 3,
						},
						ChangeType: pb.KillZombie,
					},
					ServiceType: pb.LogService,
				},
			},
		},
		{
			desc:    "store c is expired, thus replicas on it are not zombies.",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
					Epoch:    2,
					LeaderID: 1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
								Epoch:    2,
								LeaderID: 1},
							ReplicaID: 1},
						}},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
								Epoch:    2,
								LeaderID: 1},
							ReplicaID: 2,
						}},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1},
							ReplicaID: 3,
						}},
					},
					"d": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
								Epoch:    2,
								LeaderID: 1},
							ReplicaID: 3,
						}},
					},
				},
			},
			tick:     expiredTick + 1,
			expected: nil,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		coordinator := NewCoordinator("", hakeeper.Config{})
		output := coordinator.Check(
			c.idAlloc,
			pb.CheckerState{
				Tick:        c.tick,
				ClusterInfo: c.cluster,
				TNState:     c.tn,
				LogState:    c.log,
			},
			false,
		)
		assert.Equal(t, c.expected, output)
	}
}

func TestOpExpiredAndThenCompleted(t *testing.T) {
	cluster := pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}}
	idAlloc := util.NewTestIDAllocator(2)
	coordinator := NewCoordinator("", hakeeper.Config{})
	fn := func(time uint64) uint64 { return time * hakeeper.DefaultTickPerSecond }
	currentTick := fn(uint64(hakeeper.DefaultLogStoreTimeout / time.Second))

	replicas := map[uint64]string{1: "a", 2: "b"}
	logShardInfo := pb.LogShardInfo{ShardID: 1, Replicas: replicas, Epoch: 2, LeaderID: 1}
	logState := pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: {ShardID: 1, Replicas: replicas, Epoch: 1, LeaderID: 1}},
		Stores: map[string]pb.LogStoreInfo{
			"a": {Tick: 0, Replicas: []pb.LogReplicaInfo{{LogShardInfo: logShardInfo, ReplicaID: 1}}},
			"b": {Tick: 0, Replicas: []pb.LogReplicaInfo{{LogShardInfo: logShardInfo, ReplicaID: 2}}},
			"c": {Tick: 1},
		},
	}

	assert.NotNil(t, coordinator.Check(
		idAlloc,
		pb.CheckerState{
			Tick:        currentTick,
			ClusterInfo: cluster,
			LogState:    logState,
		},
		false,
	))
	assert.Nil(t, coordinator.Check(
		idAlloc,
		pb.CheckerState{
			Tick:        currentTick,
			ClusterInfo: cluster,
			LogState:    logState,
		},
		false,
	))

	ops := coordinator.OperatorController.GetOperators(1)
	assert.Equal(t, 1, len(ops))
	ops[0].SetStatus(operator.EXPIRED)

	assert.NotNil(t, coordinator.Check(
		idAlloc,
		pb.CheckerState{
			Tick:        currentTick,
			ClusterInfo: cluster,
			LogState:    logState,
		},
		false,
	))
	ops = coordinator.OperatorController.GetOperators(1)
	assert.Equal(t, 1, len(ops))

	replicas = map[uint64]string{1: "a", 2: "b", 4: "c"}
	logShardInfo = pb.LogShardInfo{ShardID: 1, Replicas: replicas, Epoch: 2, LeaderID: 1}
	logState = pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: {ShardID: 1, Replicas: replicas, Epoch: 1, LeaderID: 1}},
		Stores: map[string]pb.LogStoreInfo{
			"a": {Tick: 0, Replicas: []pb.LogReplicaInfo{{LogShardInfo: logShardInfo, ReplicaID: 1}}},
			"b": {Tick: 0, Replicas: []pb.LogReplicaInfo{{LogShardInfo: logShardInfo, ReplicaID: 2}}},
			"c": {Tick: 0, Replicas: []pb.LogReplicaInfo{{LogShardInfo: logShardInfo, ReplicaID: 4}}},
		},
	}

	assert.Nil(t, coordinator.Check(
		idAlloc,
		pb.CheckerState{
			Tick:        currentTick,
			ClusterInfo: cluster,
			LogState:    logState,
		},
		false,
	))
}
