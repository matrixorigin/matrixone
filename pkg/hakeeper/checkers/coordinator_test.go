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

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

func TestFixExpiredStore(t *testing.T) {
	cases := []struct {
		desc        string
		idAlloc     *util.TestIDAllocator
		cluster     pb.ClusterInfo
		dn          pb.DNState
		log         pb.LogState
		currentTick uint64
		expected    []pb.ScheduleCommand
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
						Tick: 12 * hakeeper.DefaultTickPerSecond * 60,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1, LeaderID: 1},
							ReplicaID: 1},
						}},
					"b": {
						Tick: 13 * hakeeper.DefaultTickPerSecond * 60,
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
						Tick: 14 * hakeeper.DefaultTickPerSecond * 60,
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
			currentTick: 15 * hakeeper.DefaultTickPerSecond * 60,
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
						Tick: 3 * hakeeper.DefaultTickPerSecond * 60,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1, LeaderID: 1},
							ReplicaID: 1},
						}},
					"b": {
						Tick: 13 * hakeeper.DefaultTickPerSecond * 60,
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
						Tick: 14 * hakeeper.DefaultTickPerSecond * 60,
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
			currentTick: 15 * hakeeper.DefaultTickPerSecond * 60,
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
					"a": {
						Tick:     12 * hakeeper.DefaultTickPerSecond * 60,
						Replicas: []pb.LogReplicaInfo{},
					},
					"b": {
						Tick: 13 * hakeeper.DefaultTickPerSecond * 60,
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
						Tick: 14 * hakeeper.DefaultTickPerSecond * 60,
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
			currentTick: 15 * hakeeper.DefaultTickPerSecond * 60,
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
					"a": {
						Tick:     12 * hakeeper.DefaultTickPerSecond * 60,
						Replicas: []pb.LogReplicaInfo{},
					},
					"b": {
						Tick: 13 * hakeeper.DefaultTickPerSecond * 60,
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
						Tick: 14 * hakeeper.DefaultTickPerSecond * 60,
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
			currentTick: 15 * hakeeper.DefaultTickPerSecond * 60,
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
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		coordinator := NewCoordinator(hakeeper.Config{})
		output := coordinator.Check(c.idAlloc, c.cluster, c.dn, c.log, c.currentTick)
		assert.Equal(t, c.expected, output)
	}
}

func TestFixZombie(t *testing.T) {
	cases := []struct {
		desc        string
		idAlloc     *util.TestIDAllocator
		cluster     pb.ClusterInfo
		dn          pb.DNState
		log         pb.LogState
		currentTick uint64
		expected    []pb.ScheduleCommand
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
						Tick: 12 * hakeeper.DefaultTickPerSecond * 60,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
								Epoch:    2,
								LeaderID: 1},
							ReplicaID: 1},
						}},
					"b": {
						Tick: 13 * hakeeper.DefaultTickPerSecond * 60,
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
						Tick: 14 * hakeeper.DefaultTickPerSecond * 60,
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
						Tick: 14 * hakeeper.DefaultTickPerSecond * 60,
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
			currentTick: 15 * hakeeper.DefaultTickPerSecond * 60,
			expected: []pb.ScheduleCommand{
				{
					UUID: "c",
					ConfigChange: &pb.ConfigChange{
						Replica: pb.Replica{
							UUID:    "c",
							ShardID: 1,
							Epoch:   1,
						},
						ChangeType: pb.RemoveReplica,
					},
					ServiceType: pb.LogService,
				},
			},
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		coordinator := NewCoordinator(hakeeper.Config{})
		output := coordinator.Check(c.idAlloc, c.cluster, c.dn, c.log, c.currentTick)
		assert.Equal(t, c.expected, output)
	}
}

func TestOpExpiredAndThenCompleted(t *testing.T) {
	cluster := pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}}
	idAlloc := util.NewTestIDAllocator(2)
	coordinator := NewCoordinator(hakeeper.Config{})
	fn := func(time uint64) uint64 { return time * hakeeper.DefaultTickPerSecond * 60 }

	replicas := map[uint64]string{1: "a", 2: "b"}
	logShardInfo := pb.LogShardInfo{ShardID: 1, Replicas: replicas, Epoch: 2, LeaderID: 1}
	logState := pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: {ShardID: 1, Replicas: replicas, Epoch: 1, LeaderID: 1}},
		Stores: map[string]pb.LogStoreInfo{
			"a": {Tick: fn(12), Replicas: []pb.LogReplicaInfo{{LogShardInfo: logShardInfo, ReplicaID: 1}}},
			"b": {Tick: fn(13), Replicas: []pb.LogReplicaInfo{{LogShardInfo: logShardInfo, ReplicaID: 2}}},
			"c": {Tick: fn(14) * hakeeper.DefaultTickPerSecond * 60},
		},
	}

	assert.NotNil(t, coordinator.Check(idAlloc, cluster, pb.DNState{}, logState, fn(15)))
	assert.Nil(t, coordinator.Check(idAlloc, cluster, pb.DNState{}, logState, fn(16)))

	ops := coordinator.OperatorController.GetOperators(1)
	assert.Equal(t, 1, len(ops))
	ops[0].SetStatus(operator.EXPIRED)

	assert.NotNil(t, coordinator.Check(idAlloc, cluster, pb.DNState{}, logState, fn(17)))
	ops = coordinator.OperatorController.GetOperators(1)
	assert.Equal(t, 1, len(ops))

	replicas = map[uint64]string{1: "a", 2: "b", 4: "c"}
	logShardInfo = pb.LogShardInfo{ShardID: 1, Replicas: replicas, Epoch: 2, LeaderID: 1}
	logState = pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: {ShardID: 1, Replicas: replicas, Epoch: 1, LeaderID: 1}},
		Stores: map[string]pb.LogStoreInfo{
			"a": {Tick: fn(16), Replicas: []pb.LogReplicaInfo{{LogShardInfo: logShardInfo, ReplicaID: 1}}},
			"b": {Tick: fn(17), Replicas: []pb.LogReplicaInfo{{LogShardInfo: logShardInfo, ReplicaID: 2}}},
			"c": {Tick: fn(14), Replicas: []pb.LogReplicaInfo{{LogShardInfo: logShardInfo, ReplicaID: 4}}},
		},
	}

	assert.Nil(t, coordinator.Check(idAlloc, cluster, pb.DNState{}, logState, fn(18)))
}
