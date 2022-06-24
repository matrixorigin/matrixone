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

package checkers

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	hapb "github.com/matrixorigin/matrixone/pkg/pb/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFixExpiredStore(t *testing.T) {
	cases := []struct {
		desc        string
		idAlloc     *util.TestIDAllocator
		cluster     hapb.ClusterInfo
		dn          hapb.DNState
		log         hapb.LogState
		currentTick uint64
		expected    []hapb.ScheduleCommand
	}{
		{
			desc:    "normal case",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: hapb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             "shard1",
				}},
			},
			log: hapb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
				}},
				Stores: map[string]hapb.LogStoreInfo{
					"a": {
						Tick: 12 * hakeeper.TickPerSecond * 60,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1, LeaderID: 1},
							ReplicaID: 1},
						}},
					"b": {
						Tick: 13 * hakeeper.TickPerSecond * 60,
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
						Tick: 14 * hakeeper.TickPerSecond * 60,
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
			currentTick: 15 * hakeeper.TickPerSecond * 60,
			expected:    []hapb.ScheduleCommand(nil),
		},
		{
			desc:    "store a is expired",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: hapb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             "shard1",
				}},
			},
			log: hapb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
				}},
				Stores: map[string]hapb.LogStoreInfo{
					"a": {
						Tick: 3 * hakeeper.TickPerSecond * 60,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1, LeaderID: 1},
							ReplicaID: 1},
						}},
					"b": {
						Tick: 13 * hakeeper.TickPerSecond * 60,
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
						Tick: 14 * hakeeper.TickPerSecond * 60,
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
			currentTick: 15 * hakeeper.TickPerSecond * 60,
			expected: []hapb.ScheduleCommand{{
				Target: "b",
				ConfigChange: hapb.ConfigChange{
					Replica: hapb.Replica{
						StoreID:   "a",
						ShardID:   1,
						ReplicaID: 1,
						Epoch:     0,
					},
					ChangeType: hapb.RemoveReplica,
				},
				ServiceType: hapb.LogService,
			}},
		},
		{
			desc:    "shard 1 has 2 replicas, which expected to be 3",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: hapb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             "shard1",
				}},
			},
			log: hapb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
				}},
				Stores: map[string]hapb.LogStoreInfo{
					"a": {
						Tick:     12 * hakeeper.TickPerSecond * 60,
						Replicas: []pb.LogReplicaInfo{},
					},
					"b": {
						Tick: 13 * hakeeper.TickPerSecond * 60,
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
						Tick: 14 * hakeeper.TickPerSecond * 60,
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
			currentTick: 15 * hakeeper.TickPerSecond * 60,
			expected: []hapb.ScheduleCommand{{
				Target: "b",
				ConfigChange: hapb.ConfigChange{
					Replica: hapb.Replica{
						StoreID:   "a",
						ShardID:   1,
						ReplicaID: 4,
						Epoch:     1,
					},
					ChangeType: hapb.AddReplica,
				},
				ServiceType: hapb.LogService,
			}},
		},
		{
			desc:    "replica on store a is not started",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: hapb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             "shard1",
				}},
			},
			log: hapb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
				}},
				Stores: map[string]hapb.LogStoreInfo{
					"a": {
						Tick:     12 * hakeeper.TickPerSecond * 60,
						Replicas: []pb.LogReplicaInfo{},
					},
					"b": {
						Tick: 13 * hakeeper.TickPerSecond * 60,
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
						Tick: 14 * hakeeper.TickPerSecond * 60,
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
			currentTick: 15 * hakeeper.TickPerSecond * 60,
			expected: []hapb.ScheduleCommand{
				{
					Target: "a",
					ConfigChange: hapb.ConfigChange{
						Replica: hapb.Replica{
							StoreID:   "a",
							ShardID:   1,
							ReplicaID: 1,
						},
						ChangeType: hapb.StartReplica,
					},
					ServiceType: hapb.LogService,
				},
			},
		},
	}

	for i, c := range cases {
		coordinator := NewCoordinator()
		output := coordinator.Check(c.idAlloc, c.cluster, c.dn, c.log, c.currentTick)
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, output)
	}
}

func TestFixZombie(t *testing.T) {
	cases := []struct {
		desc        string
		idAlloc     *util.TestIDAllocator
		cluster     hapb.ClusterInfo
		dn          hapb.DNState
		log         hapb.LogState
		currentTick uint64
		expected    []hapb.ScheduleCommand
	}{
		{
			desc:    "replica on store c is a zombie",
			idAlloc: util.NewTestIDAllocator(3),
			cluster: hapb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             "shard1",
				}},
			},
			log: hapb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
					Epoch:    2,
					LeaderID: 1,
				}},
				Stores: map[string]hapb.LogStoreInfo{
					"a": {
						Tick: 12 * hakeeper.TickPerSecond * 60,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
								Epoch:    2,
								LeaderID: 1},
							ReplicaID: 1},
						}},
					"b": {
						Tick: 13 * hakeeper.TickPerSecond * 60,
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
						Tick: 14 * hakeeper.TickPerSecond * 60,
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
						Tick: 14 * hakeeper.TickPerSecond * 60,
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
			currentTick: 15 * hakeeper.TickPerSecond * 60,
			expected: []hapb.ScheduleCommand{
				{
					Target: "c",
					ConfigChange: hapb.ConfigChange{
						Replica: hapb.Replica{
							StoreID: "c",
							ShardID: 1,
						},
						ChangeType: hapb.StopReplica,
					},
					ServiceType: hapb.LogService,
				},
			},
		},
	}

	for i, c := range cases {
		coordinator := NewCoordinator()
		output := coordinator.Check(c.idAlloc, c.cluster, c.dn, c.log, c.currentTick)
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, output)
	}
}
