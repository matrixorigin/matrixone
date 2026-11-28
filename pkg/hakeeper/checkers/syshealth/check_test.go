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

package syshealth

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestParseLogStores(t *testing.T) {
	expiredTick := uint64(10)
	// construct current tick in order to make heartbeat tick expired
	cfg := hakeeper.Config{}
	cfg.Fill()
	currTick := cfg.ExpiredTick(expiredTick, cfg.LogStoreTimeout) + 1

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

	logStores := parseLogState(cfg, logState, currTick)
	require.Equal(t, len(logState.Stores), len(logStores.working)+len(logStores.expired))
	require.Equal(t, 1, len(logStores.expired))
	require.Equal(t, 2, len(logStores.working))
}

func TestLogShard(t *testing.T) {
	// odd shard size
	{
		logShard := newLogShard(10, defaultLogShardSize)
		require.False(t, logShard.healthy())

		logShard.registerExpiredReplica(100)
		require.False(t, logShard.healthy())

		logShard.registerWorkingReplica(101)
		require.False(t, logShard.healthy())

		logShard.registerWorkingReplica(102)
		require.True(t, logShard.healthy())

		logShard.registerExpiredReplica(103)
		require.True(t, logShard.healthy())
	}

	// even shard size
	{
		logShard := newLogShard(10, 2)
		require.False(t, logShard.healthy())

		// register a working replica
		logShard.registerWorkingReplica(104)
		require.False(t, logShard.healthy())

		// repeated register
		logShard.registerWorkingReplica(104)
		require.False(t, logShard.healthy())

		// register another working replica
		logShard.registerWorkingReplica(105)
		require.True(t, logShard.healthy())

		// register a expired replica
		logShard.registerExpiredReplica(100)
		require.True(t, logShard.healthy())
	}
}

func TestLogShardMap(t *testing.T) {
	expiredStores := map[string]struct{}{
		"expired1": {},
		"expired2": {},
		"expired3": {},
	}

	workingStores := map[string]struct{}{
		"working1": {},
		"working2": {},
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
			),
			"working1": mockLogStoreInfo(
				tick,
				mockLogReplicaInfo(10, 106),
				mockLogReplicaInfo(11, 107),
			),
			"working2": mockLogStoreInfo(
				tick,
				mockLogReplicaInfo(10, 108),
				mockLogReplicaInfo(14, 109),
				mockLogReplicaInfo(11, 110),
			),
		},
	}

	shards := listExpiredShards(expiredStores, workingStores, logState, pb.ClusterInfo{})
	require.Equal(t, 4, len(shards))

	require.Equal(t, 2, len(shards[10].expiredReplicas))
	require.Equal(t, 2, len(shards[10].workingReplicas))

	require.Equal(t, 1, len(shards[11].expiredReplicas))
	require.Equal(t, 2, len(shards[11].workingReplicas))

	require.Equal(t, 1, len(shards[12].expiredReplicas))
	require.Equal(t, 0, len(shards[12].workingReplicas))

	require.Equal(t, 1, len(shards[13].expiredReplicas))
	require.Equal(t, 0, len(shards[13].workingReplicas))
}

func TestHealthy(t *testing.T) {
	expiredTick := uint64(10)
	// construct current tick in order to make heartbeat tick expired
	cfg := hakeeper.Config{}
	cfg.Fill()
	currTick := cfg.ExpiredTick(expiredTick, cfg.LogStoreTimeout) + 1

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

		healthy := Healthy(cfg, pb.ClusterInfo{}, logState, currTick)
		require.True(t, healthy)
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

		healthy := Healthy(cfg, pb.ClusterInfo{}, logState, currTick)
		require.False(t, healthy)
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
