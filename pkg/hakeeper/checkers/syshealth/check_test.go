// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package syshealth

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"

	"github.com/stretchr/testify/require"
)

func TestShutdownStores(t *testing.T) {
	stores := map[util.StoreID]struct{}{
		util.StoreID("11"): {},
		util.StoreID("12"): {},
		util.StoreID("13"): {},
	}

	// operator for log service
	{
		serviceType := pb.LogService
		ops := shutdownStores(serviceType, stores)
		require.Equal(t, len(stores), len(ops))

		for i := 0; i < len(ops); i++ {
			op := ops[i]
			steps := op.OpSteps()
			require.Equal(t, 1, len(steps))

			_, ok := steps[0].(operator.StopLogStore)
			require.True(t, ok)
		}
	}

	// operator for dn service
	{
		serviceType := pb.DnService
		ops := shutdownStores(serviceType, stores)
		require.Equal(t, len(stores), len(ops))

		for i := 0; i < len(ops); i++ {
			op := ops[i]
			steps := op.OpSteps()
			require.Equal(t, 1, len(steps))

			_, ok := steps[0].(operator.StopDnStore)
			require.True(t, ok)
		}
	}
}

func TestParseLogStores(t *testing.T) {
	expiredTick := uint64(10)
	// construct current tick in order to make hearbeat tick expired
	currTick := hakeeper.ExpiredTick(expiredTick, hakeeper.LogStoreTimeout) + 1

	logState := pb.LogState{
		Stores: map[string]pb.LogStoreInfo{
			"expired1": mockLogStoreInfo(
				expiredTick,
				mockLogReplicaInfo(10, 100),
				mockLogReplicaInfo(11, 101),
			),
			"working1": mockLogStoreInfo(
				currTick,
				mockLogReplicaInfo(10, 102),
				mockLogReplicaInfo(11, 103),
			),
			"working2": mockLogStoreInfo(
				currTick,
				mockLogReplicaInfo(10, 104),
				mockLogReplicaInfo(11, 105),
			),
		},
	}

	logStores := parseLogStores(logState, currTick)
	require.Equal(t, len(logState.Stores), logStores.length())
	require.Equal(t, pb.LogService, logStores.serviceType)
	require.Equal(t, 1, len(logStores.expired))
	require.Equal(t, 1, len(logStores.shutdownExpiredStores()))
	require.Equal(t, 2, len(logStores.working))
	require.Equal(t, 2, len(logStores.shutdownWorkingStores()))
}

func TestParseDnStores(t *testing.T) {
	expiredTick := uint64(10)
	// construct current tick in order to make hearbeat tick expired
	currTick := hakeeper.ExpiredTick(expiredTick, hakeeper.LogStoreTimeout) + 1

	dnState := pb.DNState{
		Stores: map[string]pb.DNStoreInfo{
			"expired1": {
				Tick: expiredTick,
				Shards: []pb.DNShardInfo{
					mockDnShardInfo(10, 100),
				},
			},
			"working1": {
				Tick: currTick,
				Shards: []pb.DNShardInfo{
					mockDnShardInfo(11, 101),
				},
			},
			"working2": {
				Tick: currTick,
				Shards: []pb.DNShardInfo{
					mockDnShardInfo(12, 102),
					mockDnShardInfo(13, 103),
				},
			},
		},
	}

	dnStores := parseDnStores(dnState, currTick)
	require.Equal(t, len(dnState.Stores), dnStores.length())
	require.Equal(t, pb.DnService, dnStores.serviceType)
	require.Equal(t, 1, len(dnStores.expired))
	require.Equal(t, 1, len(dnStores.shutdownExpiredStores()))
	require.Equal(t, 2, len(dnStores.working))
	require.Equal(t, 2, len(dnStores.shutdownWorkingStores()))
}

func TestLogShard(t *testing.T) {
	// odd shard size
	{
		logShard := newLogShard(10, defaultLogShardSize)
		require.True(t, logShard.healthy())

		logShard.registerExpiredReplica(100)
		require.True(t, logShard.healthy())

		logShard.registerExpiredReplica(101)
		require.False(t, logShard.healthy())
	}

	// even shard size
	{
		logShard := newLogShard(10, 2)
		require.True(t, logShard.healthy())

		logShard.registerExpiredReplica(100)
		require.False(t, logShard.healthy())
	}
}

func TestLogShardMap(t *testing.T) {
	expiredStores := map[util.StoreID]struct{}{
		"expired1": {},
		"expired2": {},
		"expired3": {},
	}

	tick := uint64(10)

	logState := pb.LogState{
		Stores: map[string]pb.LogStoreInfo{
			"expired1": mockLogStoreInfo(
				tick,
				mockLogReplicaInfo(10, 100),
				mockLogReplicaInfo(11, 101),
			),
			"expired2": mockLogStoreInfo(
				tick,
				mockLogReplicaInfo(10, 102),
				mockLogReplicaInfo(12, 103),
			),
			"expired3": mockLogStoreInfo(
				tick,
				mockLogReplicaInfo(13, 104),
				mockLogReplicaInfo(14, 105),
			),
		},
	}

	logShardMap := logShardsWithExpired(expiredStores, logState, pb.ClusterInfo{})
	require.Equal(t, 5, len(logShardMap))
	require.Equal(t, 2, len(logShardMap[10].expiredReplicas))
	require.Equal(t, 1, len(logShardMap[11].expiredReplicas))
	require.Equal(t, 1, len(logShardMap[12].expiredReplicas))
	require.Equal(t, 1, len(logShardMap[13].expiredReplicas))
	require.Equal(t, 1, len(logShardMap[14].expiredReplicas))
}

func TestCheck(t *testing.T) {
	expiredTick := uint64(10)
	// construct current tick in order to make hearbeat tick expired
	currTick := hakeeper.ExpiredTick(expiredTick, hakeeper.LogStoreTimeout) + 1

	// system healthy
	{
		logState := pb.LogState{
			Stores: map[string]pb.LogStoreInfo{
				"expired1": mockLogStoreInfo(
					expiredTick,
					mockLogReplicaInfo(10, 100),
					mockLogReplicaInfo(11, 101),
				),
				"working1": mockLogStoreInfo(
					currTick,
					mockLogReplicaInfo(10, 102),
					mockLogReplicaInfo(11, 103),
				),
				"working2": mockLogStoreInfo(
					currTick,
					mockLogReplicaInfo(10, 104),
					mockLogReplicaInfo(11, 105),
				),
			},
		}

		dnState := pb.DNState{
			Stores: map[string]pb.DNStoreInfo{
				"expired11": {
					Tick: expiredTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(10, 100),
					},
				},
				"working11": {
					Tick: currTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(11, 101),
					},
				},
				"working12": {
					Tick: currTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(12, 102),
						mockDnShardInfo(13, 103),
					},
				},
			},
		}

		ops, healthy := Check(pb.ClusterInfo{}, dnState, logState, currTick)
		require.True(t, healthy)
		require.Equal(t, 0, len(ops))
	}

	// system unhealthy
	{
		logState := pb.LogState{
			Stores: map[string]pb.LogStoreInfo{
				"expired1": mockLogStoreInfo(
					expiredTick,
					mockLogReplicaInfo(10, 100),
					mockLogReplicaInfo(11, 101),
				),
				"expired2": mockLogStoreInfo(
					expiredTick,
					mockLogReplicaInfo(10, 102),
					mockLogReplicaInfo(11, 103),
				),
				"working2": mockLogStoreInfo(
					currTick,
					mockLogReplicaInfo(10, 104),
					mockLogReplicaInfo(11, 105),
				),
			},
		}

		dnState := pb.DNState{
			Stores: map[string]pb.DNStoreInfo{
				"expired11": {
					Tick: expiredTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(10, 100),
					},
				},
				"working11": {
					Tick: currTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(11, 101),
					},
				},
				"working12": {
					Tick: currTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(12, 102),
						mockDnShardInfo(13, 103),
					},
				},
			},
		}

		ops, healthy := Check(pb.ClusterInfo{}, dnState, logState, currTick)
		require.False(t, healthy)
		require.Equal(t, 6, len(ops))
	}
}

func mockLogReplicaInfo(shardID, replicaID uint64) pb.LogReplicaInfo {
	info := pb.LogReplicaInfo{
		ReplicaID: replicaID,
	}
	info.ShardID = shardID
	return info
}

func mockLogStoreInfo(tick uint64, replicas ...pb.LogReplicaInfo) pb.LogStoreInfo {
	return pb.LogStoreInfo{
		Tick:     tick,
		Replicas: replicas,
	}
}

func mockDnShardInfo(shardID, replicaID uint64) pb.DNShardInfo {
	return pb.DNShardInfo{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
}
