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
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"

	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, len(extraFirst), 4)

	extraSecond := extraWorkingReplicas(shard)
	require.Equal(t, len(extraSecond), 4)

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
	var working []*dnStore
	_, err := consumeLeastSpareStore(working)
	require.Error(t, err)

	working = []*dnStore{
		newDnStore("store13", 1, DnStoreCapacity),
		newDnStore("store12", 1, DnStoreCapacity),
		newDnStore("store11", 2, DnStoreCapacity),
	}

	id, err := consumeLeastSpareStore(working)
	require.NoError(t, err)
	require.Equal(t, id, StoreID("store12"))

	id, err = consumeLeastSpareStore(working)
	require.NoError(t, err)
	require.Equal(t, id, StoreID("store13"))

	id, err = consumeLeastSpareStore(working)
	require.NoError(t, err)
	require.Equal(t, id, StoreID("store11"))
}

func TestCheckShard(t *testing.T) {
	// normal running cluster
	{
		newReplicaID := uint64(100)
		idGen := func() (uint64, bool) {
			return newReplicaID, true
		}

		workingStores := []*dnStore{
			newDnStore("store1", 2, DnStoreCapacity),
			newDnStore("store2", 3, DnStoreCapacity),
			newDnStore("store3", 4, DnStoreCapacity),
		}

		shardID := uint64(10)
		shard := newDnShard(10)

		// register a expired replica, should add a new replica
		shard.register(newReplica(11, shardID, "store11"), true)
		steps := checkShard(shard, workingStores, idGen)
		require.Equal(t, len(steps), 1)
		require.Equal(t, steps[0].Command(), AddReplica)
		require.Equal(t, steps[0].ReplicaID(), newReplicaID)
		require.Equal(t, steps[0].ShardID(), shardID)
		require.Equal(t, steps[0].Target(), StoreID("store1"))

		// register a working replica, no more step
		shard.register(newReplica(12, shardID, "store12"), false)
		steps = checkShard(shard, workingStores, idGen)
		require.Equal(t, len(steps), 0)

		// register another working replica, should remove extra replicas
		shard.register(newReplica(13, shardID, "store13"), false)
		steps = checkShard(shard, workingStores, idGen)
		require.Equal(t, len(steps), 1)
		require.Equal(t, steps[0].Command(), RemoveReplica)
		require.Equal(t, steps[0].ReplicaID(), uint64(12))
		require.Equal(t, steps[0].ShardID(), shardID)
		require.Equal(t, steps[0].Target(), StoreID("store12"))
	}

	{
		// id generator temporary failed
		idGen := func() (uint64, bool) {
			return 0, false
		}

		workingStores := []*dnStore{
			newDnStore("store1", 2, DnStoreCapacity),
			newDnStore("store2", 3, DnStoreCapacity),
			newDnStore("store3", 4, DnStoreCapacity),
		}

		anotherShard := uint64(100)
		// register another expired replica, should add a new replica
		shard := mockDnShard(anotherShard, nil, []uint64{101})
		steps := checkShard(shard, workingStores, idGen)
		require.Equal(t, len(steps), 0)
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
	expiredTick := uint64(10)
	// construct current tick in order to make hearbeat tick expired
	currTick := hakeeper.ExpiredTick(expiredTick, dnStoreTimeout) + 1

	newReplicaID := uint64(100)
	idGen := func() (uint64, bool) {
		return newReplicaID, true
	}

	// 1. no working dn stores
	{
		dnState := hakeeper.DNState{
			Stores: map[string]hakeeper.DNStoreInfo{
				"expired1": {
					Tick: expiredTick,
					Shards: []logservice.DNShardInfo{
						mockDnShardMeta(10, 12),
					},
				},
				"expired2": {
					Tick: expiredTick,
					Shards: []logservice.DNShardInfo{
						mockDnShardMeta(11, 13),
					},
				},
			},
		}

		ops := Check(mockClusterInfo(), dnState, currTick, idGen)
		require.Equal(t, len(ops), 0)
	}

	// 2. running cluster
	{
		dnState := hakeeper.DNState{
			Stores: map[string]hakeeper.DNStoreInfo{
				"expired1": {
					Tick: expiredTick,
					Shards: []logservice.DNShardInfo{
						mockDnShardMeta(10, 11),
						mockDnShardMeta(14, 17),
					},
				},
				"working1": {
					Tick: currTick,
					Shards: []logservice.DNShardInfo{
						mockDnShardMeta(12, 13),
					},
				},
				"working2": {
					Tick: currTick,
					Shards: []logservice.DNShardInfo{
						mockDnShardMeta(14, 15),
						mockDnShardMeta(12, 16),
					},
				},
				"working3": {
					Tick: currTick,
					Shards: []logservice.DNShardInfo{
						mockDnShardMeta(12, 18),
					},
				},
			},
		}

		// shard 10, 12, 14:
		//	10 - add replica
		//  12 - remote two extra replica (16, 13)
		//  14 - no command
		ops := Check(mockClusterInfo(), dnState, currTick, idGen)
		require.Equal(t, len(ops), 3)

		require.Equal(t, ops[0].Step.ShardID(), uint64(10))
		require.Equal(t, ops[0].Step.Command(), AddReplica)

		require.Equal(t, ops[1].Step.ShardID(), uint64(12))
		require.Equal(t, ops[1].Step.Command(), RemoveReplica)
		require.Equal(t, ops[1].Step.ReplicaID(), uint64(13))

		require.Equal(t, ops[2].Step.ShardID(), uint64(12))
		require.Equal(t, ops[2].Step.Command(), RemoveReplica)
		require.Equal(t, ops[2].Step.ReplicaID(), uint64(16))
	}
}

func mockClusterInfo() hakeeper.ClusterInfo {
	return hakeeper.ClusterInfo{}
}
