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

package dnservice

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestCheckInitiatingShards(t *testing.T) {
	// clear all records, or other test would fail
	defer func() {
		waitingShards.clear()
	}()

	nextReplicaID := uint64(100)
	enough := true
	idAlloc := newMockIDAllocator(nextReplicaID, enough)

	workingStores := []*util.Store{
		util.NewStore("store1", 2, DnStoreCapacity),
		util.NewStore("store2", 3, DnStoreCapacity),
		util.NewStore("store3", 4, DnStoreCapacity),
	}

	reportedShard := uint64(10)
	reportedReplica := uint64(21)
	initialShard := uint64(11)

	rs := newReportedShards()
	// register a working replica => no need to operation
	rs.registerReplica(newReplica(reportedReplica, reportedShard, "store11"), false)

	// mock pb.ClusterInfo
	cluster := mockClusterInfo(reportedShard, initialShard)

	// mock hakeeper.Config
	config := hakeeper.Config{
		TickPerSecond:   10,
		LogStoreTimeout: 10 * time.Second,
		DNStoreTimeout:  10 * time.Second,
	}

	// mock ShardMapper
	mapper := mockShardMapper()

	earliestTick := uint64(10)
	expiredTick := config.ExpiredTick(earliestTick, config.DNStoreTimeout) + 1

	// discover an initial shard => no operators generated
	ops := checkInitiatingShards(rs, mapper, workingStores, idAlloc, cluster, config, earliestTick, logger)
	require.Equal(t, 0, len(ops))

	// waiting some time, but not long enough
	ops = checkInitiatingShards(rs, mapper, workingStores, idAlloc, cluster, config, expiredTick-1, logger)
	require.Equal(t, 0, len(ops))

	// waiting long enough
	ops = checkInitiatingShards(rs, mapper, workingStores, idAlloc, cluster, config, expiredTick, logger)
	require.Equal(t, 1, len(ops))
}

func TestCheckReportedState(t *testing.T) {
	nextReplicaID := uint64(100)
	enough := true
	idAlloc := newMockIDAllocator(nextReplicaID, enough)
	mapper := mockShardMapper()

	workingStores := []*util.Store{
		util.NewStore("store1", 2, DnStoreCapacity),
		util.NewStore("store2", 3, DnStoreCapacity),
		util.NewStore("store3", 4, DnStoreCapacity),
	}

	shardID := uint64(10)

	// register an expired replica => should add a new replica
	rs := newReportedShards()
	rs.registerReplica(newReplica(11, shardID, "store11"), true)
	ops := checkReportedState(rs, mapper, workingStores, idAlloc, logger)
	require.Equal(t, 1, len(ops))
	require.Equal(t, shardID, ops[0].ShardID())

	// register a working replica => no more step
	rs = newReportedShards()
	rs.registerReplica(newReplica(12, shardID, "store12"), false)
	ops = checkReportedState(rs, mapper, workingStores, idAlloc, logger)
	require.Equal(t, 0, len(ops))
}

func TestInitialShards(t *testing.T) {
	waitingShards := newInitialShards()

	// list all shard id
	ids := waitingShards.listEligibleShards(func(tick uint64) bool {
		return true
	})
	require.Equal(t, 0, len(ids))

	// test register
	shardID := uint64(1)
	tick := uint64(10)
	updated := waitingShards.register(shardID, tick)
	require.True(t, updated)
	updated = waitingShards.register(shardID, tick)
	require.False(t, updated)
	tick = 11
	updated = waitingShards.register(shardID, tick)
	require.False(t, updated)
	tick = 9
	updated = waitingShards.register(shardID, tick)
	require.True(t, updated)
	ids = waitingShards.listEligibleShards(func(tick uint64) bool {
		return true
	})
	require.Equal(t, 1, len(ids))

	// test delta
	updated = waitingShards.remove(shardID + 1)
	require.False(t, updated)
	updated = waitingShards.remove(shardID)
	require.True(t, updated)
	ids = waitingShards.listEligibleShards(func(tick uint64) bool {
		return true
	})
	require.Equal(t, 0, len(ids))
}

func TestParseDNState(t *testing.T) {
	expiredTick := uint64(10)
	// construct current tick in order to make heartbeat tick expired
	cfg := hakeeper.Config{}
	cfg.Fill()
	currTick := cfg.ExpiredTick(expiredTick, cfg.DNStoreTimeout) + 1

	// 1. no working dn stores
	{
		dnState := pb.DNState{
			Stores: map[string]pb.DNStoreInfo{
				"expired1": {
					Tick: expiredTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(10, 12),
					},
				},
				"expired2": {
					Tick: expiredTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(11, 13),
					},
				},
			},
		}

		stores, shards := parseDnState(cfg, dnState, currTick)

		// check stores
		require.Equal(t, len(stores.WorkingStores()), 0)
		require.Equal(t, len(stores.ExpiredStores()), 2)

		// check shards
		shardIDs := shards.listShards()
		require.Equal(t, len(shardIDs), 2)

		shard10, err := shards.getShard(10)
		require.NoError(t, err)
		require.NotNil(t, shard10)
		require.Equal(t, len(shard10.workingReplicas()), 0)
		require.Equal(t, len(shard10.expiredReplicas()), 1)

		_, err = shards.getShard(100)
		require.Error(t, err)
	}

	// 2. verbose running shard replica
	{
		dnState := pb.DNState{
			Stores: map[string]pb.DNStoreInfo{
				"expired1": {
					Tick: expiredTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(10, 11),
						mockDnShardInfo(14, 17),
					},
				},
				"working1": {
					Tick: currTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(12, 13),
					},
				},
				"working2": {
					Tick: currTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(14, 15),
						mockDnShardInfo(12, 16),
					},
				},
			},
		}

		stores, shards := parseDnState(cfg, dnState, currTick)

		// check stores
		require.Equal(t, len(stores.WorkingStores()), 2)
		require.Equal(t, len(stores.ExpiredStores()), 1)

		// check shards
		shardIDs := shards.listShards()
		require.Equal(t, len(shardIDs), 3)

		shard10, err := shards.getShard(10)
		require.NoError(t, err)
		require.NotNil(t, shard10)
		require.Equal(t, len(shard10.workingReplicas()), 0)
		require.Equal(t, len(shard10.expiredReplicas()), 1)

		shard12, err := shards.getShard(12)
		require.NoError(t, err)
		require.NotNil(t, shard12)
		require.Equal(t, len(shard12.workingReplicas()), 2)
		require.Equal(t, len(shard12.expiredReplicas()), 0)

		shard14, err := shards.getShard(14)
		require.NoError(t, err)
		require.NotNil(t, shard14)
		require.Equal(t, len(shard14.workingReplicas()), 1)
		require.Equal(t, len(shard14.expiredReplicas()), 1)
	}
}

func mockDnShardInfo(shardID, replicaID uint64) pb.DNShardInfo {
	return pb.DNShardInfo{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
}
