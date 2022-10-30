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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

var logger = logutil.GetGlobalLogger().Named("hakeeper-test")

func TestExpiredReplicas(t *testing.T) {
	replicaIDs := []uint64{11, 13, 12, 14, 15}
	retFirst := expiredReplicas(mockDnShard(10, nil, replicaIDs))
	retSecond := expiredReplicas(mockDnShard(10, nil, replicaIDs))

	require.Equal(t, len(retFirst), len(retSecond))
	for i := 0; i < len(retFirst); i++ {
		require.Equal(t, retFirst[i].replicaID, retSecond[i].replicaID)
	}
}

func TestExtraWorkingReplicas(t *testing.T) {
	workingIDs := []uint64{11, 13, 12, 14, 15}
	shard := mockDnShard(10, workingIDs, nil)

	extraFirst := extraWorkingReplicas(shard)
	require.Equal(t, 4, len(extraFirst))

	extraSecond := extraWorkingReplicas(shard)
	require.Equal(t, 4, len(extraSecond))

	// whether the order is deterministic or not
	for i := 0; i < len(extraFirst); i++ {
		require.Equal(t,
			extraFirst[i].replicaID,
			extraSecond[i].replicaID,
		)
	}

	// get max replica ID
	maxReplicaID := uint64(0)
	for _, id := range workingIDs {
		if id > maxReplicaID {
			maxReplicaID = id
		}
	}
	// max replica ID not in
	for i := 0; i < len(extraFirst); i++ {
		require.NotEqual(t,
			maxReplicaID,
			extraFirst[i].replicaID,
		)
	}
}

func TestConsumeLeastSpareStore(t *testing.T) {
	var working []*util.Store
	_, err := consumeLeastSpareStore(working)
	require.Error(t, err)

	working = []*util.Store{
		util.NewStore("store13", 1, DnStoreCapacity),
		util.NewStore("store12", 1, DnStoreCapacity),
		util.NewStore("store11", 2, DnStoreCapacity),
	}

	id, err := consumeLeastSpareStore(working)
	require.NoError(t, err)
	require.Equal(t, "store12", id)

	id, err = consumeLeastSpareStore(working)
	require.NoError(t, err)
	require.Equal(t, "store13", id)

	id, err = consumeLeastSpareStore(working)
	require.NoError(t, err)
	require.Equal(t, "store11", id)
}

func TestCheckShard(t *testing.T) {
	// normal running cluster
	{
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
		shard := newDnShard(10)

		// register an expired replica => should add a new replica
		shard.register(newReplica(11, shardID, "store11"), true)
		steps := checkShard(shard, mapper, workingStores, idAlloc, logger)
		require.Equal(t, 1, len(steps))
		add, ok := (steps[0]).(operator.AddDnReplica)
		require.True(t, ok)
		require.Equal(t, nextReplicaID, add.ReplicaID)
		require.Equal(t, shardID, add.ShardID)
		require.Equal(t, "store1", add.StoreID)

		// register a working replica => no more step
		shard.register(newReplica(12, shardID, "store12"), false)
		steps = checkShard(shard, mapper, workingStores, idAlloc, logger)
		require.Equal(t, 0, len(steps))

		// register another working replica => should remove extra replicas
		shard.register(newReplica(13, shardID, "store13"), false)
		steps = checkShard(shard, mapper, workingStores, idAlloc, logger)
		require.Equal(t, 1, len(steps))
		remove, ok := (steps[0]).(operator.RemoveDnReplica)
		require.True(t, ok)
		require.Equal(t, uint64(12), remove.ReplicaID)
		require.Equal(t, shardID, remove.ShardID)
		require.Equal(t, "store12", remove.StoreID)
	}

	{
		// ID exhausted temporarily
		enough := false
		idAlloc := newMockIDAllocator(0, enough)
		mapper := mockShardMapper()

		workingStores := []*util.Store{
			util.NewStore("store1", 2, DnStoreCapacity),
			util.NewStore("store2", 3, DnStoreCapacity),
			util.NewStore("store3", 4, DnStoreCapacity),
		}

		anotherShard := uint64(100)
		// register another expired replica, should add a new replica
		shard := mockDnShard(anotherShard, nil, []uint64{101})
		steps := checkShard(shard, mapper, workingStores, idAlloc, logger)
		require.Equal(t, 0, len(steps))
	}
}

func mockDnShard(
	shardID uint64, workingReplicas, expiredReplica []uint64,
) *dnShard {
	shard := newDnShard(shardID)

	// register working replicas
	for i, replicaID := range workingReplicas {
		replica := newReplica(
			replicaID, shardID,
			fmt.Sprintf("store%d", i),
		)
		shard.register(replica, false)
	}

	// register expired replicas
	for i, replicaID := range expiredReplica {
		replica := newReplica(
			replicaID, shardID,
			fmt.Sprintf("store%d", i+len(workingReplicas)),
		)
		shard.register(replica, true)
	}

	return shard
}

func TestCheck(t *testing.T) {
	// clear all records, or other test would fail
	defer func() {
		waitingShards.clear()
	}()

	staleTick := uint64(10)
	// construct current tick in order to make heartbeat tick expired
	config := hakeeper.Config{}
	config.Fill()
	currTick := config.ExpiredTick(staleTick, config.DNStoreTimeout) + 1

	enough := true
	newReplicaID := uint64(100)
	idAlloc := newMockIDAllocator(newReplicaID, enough)

	// 1. no working dn stores
	{
		dnState := pb.DNState{
			Stores: map[string]pb.DNStoreInfo{
				"expired1": {
					Tick: staleTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(10, 12),
					},
				},
				"expired2": {
					Tick: staleTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(11, 13),
					},
				},
			},
		}

		clusterInfo := mockClusterInfo(10, 11)

		steps := Check(idAlloc, config, clusterInfo, dnState, pb.TaskTableUser{}, currTick, logger)
		require.Equal(t, len(steps), 0)
	}

	// 2. running cluster
	{
		dnState := pb.DNState{
			Stores: map[string]pb.DNStoreInfo{
				"expired1": {
					Tick: staleTick,
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
				"working3": {
					Tick: currTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(12, 18),
					},
				},
			},
		}

		// all shards were reported
		clusterInfo := mockClusterInfo(10, 12, 14)

		// At current tick, shard 10, 12, 14:
		//  10 - add replica
		//  12 - remove two extra replica (16, 13)
		//  14 - no command
		operators := Check(idAlloc, config, clusterInfo, dnState, pb.TaskTableUser{}, currTick, logger)
		require.Equal(t, 2, len(operators))

		// shard 10 - single operator step
		op := operators[0]
		require.Equal(t, op.ShardID(), uint64(10))
		steps := op.OpSteps()
		require.Equal(t, len(steps), 1)
		add, ok := steps[0].(operator.AddDnReplica)
		require.True(t, ok)
		require.Equal(t, add.StoreID, "working1")

		// shard 12 - two operator steps
		op = operators[1]
		require.Equal(t, op.ShardID(), uint64(12))
		steps = op.OpSteps()
		require.Equal(t, len(steps), 2)
		remove, ok := steps[0].(operator.RemoveDnReplica)
		require.True(t, ok)
		require.Equal(t, remove.StoreID, "working1")
		require.Equal(t, remove.ReplicaID, uint64(13))
		remove, ok = steps[1].(operator.RemoveDnReplica)
		require.True(t, ok)
		require.Equal(t, remove.StoreID, "working2")
		require.Equal(t, remove.ReplicaID, uint64(16))
	}

	// 3. cluster running with initial shard
	{

		dnState := pb.DNState{
			Stores: map[string]pb.DNStoreInfo{
				"expired1": {
					Tick: staleTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(14, 17),
					},
				},
				"working1": {
					Tick: currTick,
					Shards: []pb.DNShardInfo{
						mockDnShardInfo(12, 16),
					},
				},
			},
		}

		// with un-reported shard ID
		unreported := uint64(20)
		cluster := mockClusterInfo(unreported, 12, 14)

		// at the tick of `staleTick`, shard 14, 20:
		//  14 - no command
		//  20 - add replica after a while
		bootstrapping = false
		operators := Check(idAlloc, config, cluster, dnState, pb.TaskTableUser{}, staleTick, logger)
		require.Equal(t, 0, len(operators))

		// at the tick of `currTick`, shard 14, 20:
		//  14 - add replica
		//  20 - add replica
		operators = Check(idAlloc, config, cluster, dnState, pb.TaskTableUser{}, currTick, logger)
		require.Equal(t, 2, len(operators))

		// shard 14 - single operator step
		op := operators[0]
		require.Equal(t, op.ShardID(), uint64(14))
		steps := op.OpSteps()
		require.Equal(t, len(steps), 1)
		add, ok := steps[0].(operator.AddDnReplica)
		require.True(t, ok)
		require.Equal(t, add.StoreID, "working1")

		// shard 20 - add replica after a while
		op = operators[1]
		require.Equal(t, op.ShardID(), uint64(20))
		steps = op.OpSteps()
		require.Equal(t, len(steps), 1)
		add, ok = steps[0].(operator.AddDnReplica)
		require.True(t, ok)
		require.Equal(t, add.StoreID, "working1")
	}
}

type mockIDAllocator struct {
	next   uint64
	enough bool
}

func newMockIDAllocator(next uint64, enough bool) *mockIDAllocator {
	return &mockIDAllocator{
		next:   next,
		enough: enough,
	}
}

func (idAlloc *mockIDAllocator) Next() (uint64, bool) {
	if !idAlloc.enough {
		return 0, false
	}

	id := idAlloc.next
	idAlloc.next += 1
	return id, true
}

func mockClusterInfo(ids ...uint64) pb.ClusterInfo {
	var c pb.ClusterInfo

	records := make([]metadata.DNShardRecord, 0, len(ids))
	for _, id := range ids {
		records = append(records, metadata.DNShardRecord{
			ShardID:    id,
			LogShardID: id,
		})
	}
	c.DNShards = records

	return c
}

type mapper struct{}

func (m mapper) getLogShardID(shardID uint64) (uint64, error) {
	return shardID, nil
}

func mockShardMapper() ShardMapper {
	return mapper{}
}
